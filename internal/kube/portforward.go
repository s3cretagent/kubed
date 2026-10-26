package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog/v2"
)

// Forward is one running port-forward.
type Forward struct {
	Namespace, Pod string
	// PodUID tells the pod apart from a new one under the same name (a
	// StatefulSet's db-0 comes back as db-0); empty when not known.
	PodUID    types.UID
	LocalPort int
	PodPort   int
	// Target is what you forwarded to when it wasn't a pod ("service web");
	// Again, when set, starts it once more through another running pod.
	Target string
	Again  func() (*Forward, error)
	// Moves counts the times in a row it was started again after ending
	// within moments of starting: one that keeps ending stops for good.
	Moves int
	// Dropped: you stopped it while kubed was finding another pod for it.
	Dropped atomic.Bool
	Started time.Time

	stop chan struct{}
	done chan struct{}
	err  error

	client *Client // the connection it runs on

	mu      sync.Mutex
	lost    bool      // its pod went away: ended by itself, not by you
	missed  int       // ticks in a row its pod was missing from the cache
	lastErr string    // the last connection that failed, while the forward stays up
	lastAt  time.Time // when it failed
}

// Failing is why the last connection through the forward failed, if that
// was in the last minute ("connection refused": nothing listens on the port).
func (f *Forward) Failing() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastErr == "" || time.Since(f.lastAt) > time.Minute {
		return ""
	}
	return f.lastErr
}

// Write takes client-go's error output, one failed connection at a time.
func (f *Forward) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if v4, _, ok := strings.Cut(line, " IPv6 "); ok && strings.Contains(v4, "IPv4") {
		line = v4 // the kubelet tries both; the IPv4 half is the one that counts
	}
	switch i := strings.LastIndex(line, ": "); {
	case strings.Contains(strings.ToLower(line), "connection refused"):
		line = "connection refused" // nothing listens on the port
	case i >= 0 && strings.Contains(line, "connect"):
		line = line[i+2:] // "…dial tcp4 127.0.0.1:80: connect: no route to host" → "no route to host"
	}
	if line != "" {
		f.mu.Lock()
		f.lastErr, f.lastAt = line, time.Now()
		f.mu.Unlock()
	}
	return len(p), nil
}

func (f *Forward) String() string {
	if f.Target != "" {
		return fmt.Sprintf("localhost:%d → %s (through pod %s, port %d)", f.LocalPort, f.Target, f.Pod, f.PodPort)
	}
	return fmt.Sprintf("localhost:%d → pod %s in %s, port %d", f.LocalPort, f.Pod, f.Namespace, f.PodPort)
}

// Reaches is what the forward reaches: its target ("service web"), or
// its pod when it reached one directly ("pod web-1").
func (f *Forward) Reaches() string {
	if f.Target != "" {
		return f.Target
	}
	return "pod " + f.Pod
}

// Short says what the forward reaches in a few words, for messages:
// "localhost:8080 now reaches service web, port 80".
func (f *Forward) Short() string {
	what := f.Reaches()
	if f.Target != "" {
		// Through a service or deployment, the number is the pod's port,
		// which can differ from the service's own.
		return fmt.Sprintf("localhost:%d now reaches %s (pod port %d)", f.LocalPort, what, f.PodPort)
	}
	return fmt.Sprintf("localhost:%d now reaches %s, port %d", f.LocalPort, what, f.PodPort)
}

// OnClosed reports that the connection it ran on is closed (you switched cluster or
// reconnected), so nothing about it concerns the cluster you're on now.
func (f *Forward) OnClosed() bool { return f.client != nil && f.client.Closed() }

// Err returns why the forward ended, if it has.
func (f *Forward) Err() error {
	select {
	case <-f.done:
		return f.err
	default:
		return nil
	}
}

// client-go tells why a connection through a forward failed (nothing
// listening, "connection refused") only to these handlers. Each forward
// runs with its own logger (forwardSink), so the error reaches the very
// forward it came from, not a newer one on the same local port.
func init() {
	utilruntime.ErrorHandlers = append(utilruntime.ErrorHandlers,
		func(ctx context.Context, err error, msg string, _ ...any) {
			if err == nil || !strings.Contains(msg, "forwarding") && !strings.Contains(msg, "error stream") {
				return
			}
			if s, ok := klog.FromContext(ctx).GetSink().(forwardSink); ok {
				_, _ = s.f.Write([]byte(err.Error()))
			}
		})
}

// forwardSink is a forward's logger: it logs nothing, it only says which
// forward an error belongs to.
type forwardSink struct{ f *Forward }

func (forwardSink) Init(logr.RuntimeInfo)            {}
func (forwardSink) Enabled(int) bool                 { return false }
func (forwardSink) Info(int, string, ...any)         {}
func (forwardSink) Error(error, string, ...any)      {}
func (s forwardSink) WithValues(...any) logr.LogSink { return s }
func (s forwardSink) WithName(string) logr.LogSink   { return s }

// ErrLeftCluster is a forward asked for on a cluster you have since left.
var ErrLeftCluster = errors.New("the connection it was asked on has closed (you switched cluster or reconnected)")

// Forwards tracks every port-forward started in this session.
type Forwards struct {
	mu    sync.Mutex
	list  []*Forward
	ended []*Forward // stopped by themselves, not by you; reported once
}

// Start opens localPort and forwards it to podPort. It returns once the
// listener is ready or failed.
func (fs *Forwards) Start(c *Client, ns, pod string, localPort, podPort int) (*Forward, error) {
	if c == nil || c.Closed() {
		return nil, ErrLeftCluster
	}
	transport, upgrader, err := spdy.RoundTripperFor(c.Config)
	if err != nil {
		return nil, err
	}
	url := c.Typed.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(ns).Name(pod).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)

	f := &Forward{Started: time.Now(), Namespace: ns, Pod: pod, LocalPort: localPort, PodPort: podPort,
		stop: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan struct{})
	ctx := klog.NewContext(utilwait.ContextForChannel(f.stop), logr.New(forwardSink{f}))
	pf, err := portforward.NewOnAddressesWithContext(ctx, dialer, []string{"localhost"},
		[]string{fmt.Sprintf("%d:%d", localPort, podPort)}, ready, io.Discard, f)
	if err != nil {
		return nil, err
	}
	go func() {
		f.err = pf.ForwardPorts()
		close(f.done)
	}()

	select {
	case <-ready:
	case <-f.done:
		return nil, f.err
	}
	// The pod it reaches now, by its UID: a StatefulSet's db-0 replaced
	// while you typed the ports is this one, not the one you picked.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if p, err := c.Typed.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{}); err == nil {
		f.PodUID = p.UID
	}
	cancel()
	f.client = c
	fs.mu.Lock()
	if c.Closed() { // you switched cluster (or reconnected) while it connected: not wanted
		fs.mu.Unlock()
		close(f.stop)
		return nil, ErrLeftCluster
	}
	fs.list = append(fs.list, f)
	fs.mu.Unlock()
	return f, nil
}

// List returns the active forwards, dropping ones that have ended.
func (fs *Forwards) List() []*Forward {
	if fs == nil {
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	alive := fs.list[:0]
	for _, f := range fs.list {
		select {
		case <-f.done:
			f.mu.Lock()
			lost := f.lost
			f.mu.Unlock()
			select {
			case <-f.stop:
				if lost { // stopped because its pod went away
					fs.ended = append(fs.ended, f)
				}
			default:
				fs.ended = append(fs.ended, f)
			}
		default:
			alive = append(alive, f)
		}
	}
	fs.list = alive
	return append([]*Forward(nil), alive...)
}

// TakeEnded returns forwards that stopped by themselves since the last call.
func (fs *Forwards) TakeEnded() []*Forward {
	if fs == nil {
		return nil
	}
	fs.List()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := fs.ended
	fs.ended = nil
	return out
}

// Missing notes whether the forward's pod was missing this tick and
// reports whether it has been for long enough (two ticks) to call it
// gone: a list being reloaded can drop a pod for a moment.
func (f *Forward) Missing(now bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !now {
		f.missed = 0
		return false
	}
	f.missed++
	return f.missed >= 2
}

// Lost ends a forward whose pod is gone. Unlike Stop, it counts as ending
// by itself, so it is reported (and a service forward starts again).
func (fs *Forwards) Lost(f *Forward) {
	f.mu.Lock()
	f.lost = true
	f.mu.Unlock()
	fs.Stop(f)
}

// Stop ends a forward.
func (fs *Forwards) Stop(f *Forward) {
	if f.stop == nil { // never started
		return
	}
	select {
	case <-f.stop:
	default:
		close(f.stop)
	}
}

// StopAll ends every forward (on quit or context switch).
func (fs *Forwards) StopAll() {
	for _, f := range fs.List() {
		fs.Stop(f)
	}
}
