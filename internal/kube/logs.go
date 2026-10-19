package kube

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// LogOptions selects which log stream to read.
type LogOptions struct {
	Container  string
	Previous   bool
	Follow     bool
	TailLines  int64
	Timestamps bool
	// Since, when set, reads everything from that long ago instead of the
	// last TailLines lines (capped at sinceLimit bytes).
	Since time.Duration
	// LimitBytes, when set, replaces SinceLimit for this stream.
	LimitBytes int64
	// Restarted: the container has restarted (its restartCount), so with
	// Previous an earlier run the API can't find had its logs removed, and
	// live logs of one waiting to start say it ran before.
	Restarted bool
}

// SinceLimit bounds how much a "since" read pulls from the cluster.
const SinceLimit = 20 << 20

// StreamLogs sends log lines to out until the stream ends or ctx is
// cancelled, then closes out. A non-nil error is sent as the last line.
func (c *Client) StreamLogs(ctx context.Context, ns, pod string, opts LogOptions, out chan<- string) {
	defer close(out)
	if !opts.Follow { // a fixed read: if the cluster doesn't answer, say so rather than wait on
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		c.streamOnce(ctx, ns, pod, opts, out, nil)
		return
	}
	// Following: a stream the cluster stops answering gives no error, and
	// reads just like an app with nothing to say. When nothing came within
	// followProbe but a plain read finds lines, it is stuck: open it again.
	// Read once here, not in the check below: that may run after out is
	// closed, when a test has put the setting back.
	probe := followProbe
	for try := 0; ; try++ {
		sctx, cancel := context.WithCancel(ctx)
		var got, stuck atomic.Bool
		go func() {
			select {
			case <-sctx.Done():
				return
			case <-time.After(probe):
			}
			if !got.Load() && (opts.TailLines > 0 || opts.Since > 0) && c.hasLogs(sctx, ns, pod, opts) && !got.Load() { // only new lines asked for: silence says nothing
				stuck.Store(true)
				cancel()
			}
		}()
		c.streamOnce(sctx, ns, pod, opts, out, &got)
		cancel()
		if !stuck.Load() || ctx.Err() != nil {
			return
		}
		if try == 2 {
			out <- "kubed: the cluster stopped sending these logs; r reads them again"
			return
		}
		if got.Load() { // lines shown after all: only new ones from here
			opts.TailLines, opts.Since = 0, 0
		}
	}
}

// followProbe is how long a followed stream may stay silent before kubed
// checks, with a plain read, that there is really nothing to show.
var followProbe = 8 * time.Second

// hasLogs: a plain read finds a line the followed stream should have
// sent (its last one, within Since when set).
func (c *Client) hasLogs(ctx context.Context, ns, pod string, opts LogOptions) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	one, limit := int64(1), int64(maxLine)
	po := &corev1.PodLogOptions{Container: opts.Container, TailLines: &one, LimitBytes: &limit}
	if opts.Since > 0 {
		secs := int64(opts.Since.Seconds())
		po.SinceSeconds = &secs
	}
	b, err := c.Typed.CoreV1().Pods(ns).GetLogs(pod, po).DoRaw(ctx)
	// A whole line only: one still being written (a prompt, no newline yet)
	// the stream holds back until it ends, rightly.
	line, _, whole := strings.Cut(string(b), "\n")
	return err == nil && whole && strings.TrimSpace(line) != ""
}

// streamOnce reads one log stream into out; got, when given, is set once
// a line has been sent.
func (c *Client) streamOnce(ctx context.Context, ns, pod string, opts LogOptions, out chan<- string, got *atomic.Bool) {
	tail := opts.TailLines
	po := &corev1.PodLogOptions{
		Container:  opts.Container,
		Previous:   opts.Previous,
		Follow:     opts.Follow,
		TailLines:  &tail,
		Timestamps: opts.Timestamps,
	}
	if opts.Since > 0 {
		secs, limit := int64(opts.Since.Seconds()), int64(SinceLimit)
		if opts.LimitBytes > 0 {
			limit = opts.LimitBytes
		}
		po.TailLines, po.SinceSeconds, po.LimitBytes = nil, &secs, &limit
	}
	req := c.Typed.CoreV1().Pods(ns).GetLogs(pod, po)
	stream, err := req.Stream(ctx)
	if err != nil {
		if got == nil || ctx.Err() == nil { // not one kubed stopped itself
			select {
			case out <- "kubed: " + logErrorOf(err.Error(), opts):
			case <-ctx.Done():
			}
		}
		return
	}
	defer stream.Close()

	// A reader, not a Scanner: one huge line (a stack dump, a big JSON
	// object) must not stop the whole stream. Lines are cut at maxLine.
	rd := bufio.NewReaderSize(stream, 64*1024)
	first := true
	for {
		line, err := readLine(rd)
		if line == "" && err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				out <- "kubed: " + logErrorOf(err.Error(), opts)
			}
			return
		}
		// The cluster sends "logs are gone" as an ordinary log line.
		if first && opts.Previous && strings.HasPrefix(line, "unable to retrieve container logs") {
			line = "kubed: " + logError(line, true)
		}
		first = false
		if got != nil {
			got.Store(true)
		}
		select {
		case out <- line:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// maxLine is how much of one log line kubed keeps.
const maxLine = 16 * 1024

// readLine reads one line, keeping at most maxLine of it.
func readLine(rd *bufio.Reader) (string, error) {
	var b strings.Builder
	cut := false
	for {
		part, isPrefix, err := rd.ReadLine()
		if b.Len() < maxLine {
			b.Write(part[:min(len(part), maxLine-b.Len())])
		} else {
			cut = true
		}
		if err != nil || !isPrefix {
			if cut || b.Len() >= maxLine && isPrefix {
				b.WriteString(" … (line cut: it was very long)")
			}
			return b.String(), err
		}
	}
}

// logErrorOf is logError for one stream: a container that restarted had
// an earlier run, even when the API no longer finds it.
func logErrorOf(err string, opts LogOptions) string {
	if opts.Previous && opts.Restarted && strings.Contains(err, "previous terminated container") && strings.Contains(err, "not found") {
		return "this container restarted, but Kubernetes no longer keeps the logs of its earlier run. l shows the live ones"
	}
	if !opts.Previous && opts.Restarted && strings.Contains(err, "is waiting to start") && !strings.Contains(err, "PodInitializing") {
		return "the container isn't running right now (it ran before; p shows its last run's logs)" + waitingWords(err)
	}
	return logError(err, opts.Previous)
}

// waitingWords is why a container waits to start, from the API's "is
// waiting to start: <reason>", in words: " (it can't download its image)".
// The reason as is when kubed has no words for it.
func waitingWords(err string) string {
	_, reason, _ := strings.Cut(err, "is waiting to start: ")
	reason = strings.TrimSpace(reason)
	switch {
	case reason == "ErrImageNeverPull":
		return " (its image isn't on its node, and its pull policy is Never, so it is never downloaded)"
	case reason == "ImagePullBackOff" || reason == "ErrImagePull" || reason == "InvalidImageName" || strings.Contains(reason, "can't be pulled"):
		return " (it can't download its image)"
	case reason == "ContainerCreating":
		return " (it is still being set up on its node: its volumes mount and its image downloads first)"
	case reason == "":
		return ""
	}
	return " (" + reason + ")"
}

// logError says why logs couldn't be read, in words.
func logError(err string, previous bool) string {
	switch {
	case previous && strings.Contains(err, "previous terminated container") && strings.Contains(err, "not found"):
		return "this container hasn't restarted, so there are no earlier logs. l shows the live ones"
	case previous && (strings.Contains(err, "unable to retrieve container logs") || strings.Contains(err, "previous terminated container")):
		return "the logs from before the last restart are gone (the cluster keeps them only until the next restart). Details: " + err
	case strings.Contains(err, "is waiting to start: PodInitializing"):
		return "the container hasn't started: its setup steps (init containers) run first, and it starts once they finish; if one fails, it never starts. Their own logs show how they are going. Details: " + err
	case strings.Contains(err, "is waiting to start"):
		return "the container hasn't started yet, so there are no logs" + waitingWords(err)
	case strings.Contains(err, "connection lost") || strings.Contains(err, "connection reset") || strings.Contains(err, "EOF") ||
		strings.Contains(err, "timeout") || strings.Contains(err, "timed out") || strings.Contains(err, "deadline exceeded"):
		return "lost the connection to the cluster while reading these logs; r reads them again"
	case strings.Contains(err, "no route to host") || strings.Contains(err, "connection refused") ||
		strings.Contains(err, "network is unreachable") || strings.Contains(err, "no such host"):
		// From you to the cluster, or from the cluster to the pod's node:
		// the error says which, so it goes along.
		return "couldn't reach the logs (the network between here, the cluster and the pod's node dropped); r tries again. Details: " + err
	}
	return "can't read the logs: " + err
}
