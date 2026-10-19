package kube

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// stallProxy passes TCP through to a server. stall makes every connection
// open so far go quiet without closing (bytes are swallowed both ways), as
// a network that dropped it does; connections opened after work.
type stallProxy struct {
	ln      net.Listener
	target  string
	stalled atomic.Int64 // connections numbered up to this are quiet
	next    atomic.Int64
	mu      sync.Mutex
	conns   []net.Conn
}

func newStallProxy(t *testing.T, target string) *stallProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			n := p.next.Add(1)
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go p.pipe(n, up, c)
			go p.pipe(n, c, up)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			_ = c.Close()
		}
	})
	return p
}

func (p *stallProxy) pipe(n int64, dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		k, err := src.Read(buf)
		if err != nil {
			return
		}
		if n <= p.stalled.Load() {
			continue // gone quiet: nothing gets through, nothing closes
		}
		if _, err := dst.Write(buf[:k]); err != nil {
			return
		}
	}
}

func (p *stallProxy) stall() { p.stalled.Store(p.next.Load()) }

// quietConnection makes a client whose one HTTP/2 connection, carrying an
// open watch, then goes quiet; kubed's connection check is in place (made
// quicker for the test). Requests sent next start out on that connection.
func quietConnection(t *testing.T) kubernetes.Interface {
	for _, name := range []string{"HTTP2_READ_IDLE_TIMEOUT_SECONDS", "HTTP2_PING_TIMEOUT_SECONDS"} {
		t.Setenv(name, "") // restored after the test
		_ = os.Unsetenv(name)
	}
	oldQuiet, oldWait := connQuiet, connPingWait
	connQuiet, connPingWait = time.Second, time.Second // the same check, quicker for the test
	t.Cleanup(func() { connQuiet, connPingWait = oldQuiet, oldWait })
	checkConnectionsSooner()

	done := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/watch" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select { // a watch: open, nothing new to say
			case <-r.Context().Done():
			case <-done:
			}
			return
		}
		_, _ = io.WriteString(w, `{"major":"1","minor":"35","gitVersion":"v1.35.0"}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(done) }) // runs first: Close waits for the watch to end
	proxy := newStallProxy(t, srv.Listener.Addr().String())

	cfg := &rest.Config{
		Host:            "https://" + proxy.ln.Addr().String(),
		TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})},
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, cfg.Host+"/watch", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.ProtoMajor != 2 {
		t.Fatalf("the test needs HTTP/2, got %s", resp.Proto)
	}
	cs, err := kubernetes.NewForConfigAndClient(cfg, hc)
	if err != nil {
		t.Fatal(err)
	}
	proxy.stall()
	return cs
}

// Rounds 111, 133, 135: now and then the screen lagged the cluster by 20
// to 35s: the one connection every client shares had gone quiet, and
// client-go only drops such a connection after up to 45s. kubed's own
// setting drops it within seconds, and a read (a watch, a list) then
// goes on on a new one.
func TestQuietConnectionIsDroppedSoon(t *testing.T) {
	cs := quietConnection(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cs.Discovery().RESTClient().Get().AbsPath("/version").Do(ctx).Error(); err != nil {
		t.Fatalf("after %s: %v", time.Since(start).Round(time.Second), err)
	}
	if took := time.Since(start); took > 6*time.Second {
		t.Errorf("a read on a connection gone quiet took %s, want a few seconds", took.Round(time.Second))
	}
}

// Rounds 131, 136: a change sent on a connection gone quiet waited there
// until kubed's own 30s limit, or reached the cluster late. Now it ends
// within seconds, and when it fails the error says the connection was lost
// (never sent again on its own: a change is not repeated).
func TestChangeOnQuietConnectionEndsSoon(t *testing.T) {
	cs := quietConnection(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := cs.CoreV1().RESTClient().Patch(types.MergePatchType).AbsPath("/apis/apps/v1/namespaces/ns/deployments/web/scale").
		Body([]byte(`{"spec":{"replicas":2}}`)).Do(ctx).Error()
	took := time.Since(start)
	if took > 6*time.Second {
		t.Errorf("a change on a connection gone quiet took %s (%v), want a few seconds", took.Round(time.Second), err)
	}
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("it timed out instead of noticing the lost connection: %v", err)
	}
}

// A value set in the environment is the user's: kept.
func TestConnectionCheckKeepsYourSetting(t *testing.T) {
	t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", "60")
	t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "")
	_ = os.Unsetenv("HTTP2_PING_TIMEOUT_SECONDS")
	checkConnectionsSooner()
	if got := os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"); got != "60" {
		t.Errorf("your setting became %q", got)
	}
	if got := os.Getenv("HTTP2_PING_TIMEOUT_SECONDS"); got != "10" {
		t.Errorf("ping wait = %q, want 10", got)
	}
}

// Connect puts the check in place before it makes any client.
func TestConnectChecksConnectionsSooner(t *testing.T) {
	for _, name := range []string{"HTTP2_READ_IDLE_TIMEOUT_SECONDS", "HTTP2_PING_TIMEOUT_SECONDS"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	t.Setenv("HOME", t.TempDir()) // the discovery cache goes there, not into yours
	path := t.TempDir() + "/config"
	kubeconfig := `apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "https://127.0.0.1:1"}}]
users: [{name: u, user: {token: t}}]
contexts: [{name: x, context: {cluster: c, user: u}}]
current-context: x
`
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _ = LoadKubeconfig(path).Connect("x") // nothing listens there: it fails, after the check is set
	if got := os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"); got != "15" {
		t.Errorf("quiet check = %q, want 15", got)
	}
	if got := os.Getenv("HTTP2_PING_TIMEOUT_SECONDS"); got != "10" {
		t.Errorf("ping wait = %q, want 10", got)
	}
}
