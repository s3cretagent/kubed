package kube

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// One huge line is cut, and the stream goes on.
func TestReadLineCutsLongLines(t *testing.T) {
	long := strings.Repeat("x", 3*maxLine)
	rd := bufio.NewReaderSize(strings.NewReader(long+"\nnext\n"), 1024)
	first, err := readLine(rd)
	if err != nil || len(first) > maxLine+64 || !strings.Contains(first, "line cut") {
		t.Fatalf("first line: %d bytes, %v", len(first), err)
	}
	if second, _ := readLine(rd); second != "next" {
		t.Errorf("second line = %q", second)
	}
}

// Round 118: a followed stream the cluster stops answering read as an app
// with nothing logged, for good. When a plain read finds lines, it opens
// again; a log really empty (or quiet for the window asked) waits on.
func TestStuckFollowOpensAgain(t *testing.T) {
	old := followProbe
	followProbe = 100 * time.Millisecond
	defer func() { followProbe = old }()
	serve := func(h http.HandlerFunc) *Client {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		return &Client{Typed: cs}
	}
	read := func(c *Client, opts LogOptions, wait time.Duration) []string {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		out := make(chan string, 10)
		go c.StreamLogs(ctx, "ns", "p", opts, out)
		var lines []string
		for l := range out {
			lines = append(lines, l)
		}
		return lines
	}
	var follows atomic.Int32
	hangsOnce := func(empty bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			if q.Get("follow") == "true" {
				if follows.Add(1) == 1 || empty {
					<-r.Context().Done() // no answer, no error
					return
				}
				_, _ = fmt.Fprintln(w, "hello")
				return
			}
			if !empty && q.Get("sinceSeconds") == "" {
				_, _ = fmt.Fprintln(w, "hello")
			}
		}
	}
	if got := read(serve(hangsOnce(false)), LogOptions{Follow: true, TailLines: 100}, 2*time.Second); !slices.Contains(got, "hello") {
		t.Errorf("stuck stream: %q", got)
	}
	follows.Store(0)
	if got := read(serve(hangsOnce(true)), LogOptions{Follow: true, TailLines: 100}, 600*time.Millisecond); len(got) != 0 {
		t.Errorf("empty log: %q", got)
	}
	// h: lines from before the hour only; the hour itself was quiet.
	quietHour := serve(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("follow") == "true" {
			<-r.Context().Done()
			return
		}
		if q.Get("sinceSeconds") == "" {
			_, _ = fmt.Fprintln(w, "hello")
		}
	})
	if got := read(quietHour, LogOptions{Follow: true, Since: time.Hour}, 600*time.Millisecond); len(got) != 0 {
		t.Errorf("quiet hour: %q", got)
	}
	// Its last line still being written (a prompt, no newline yet): the
	// stream rightly holds it back, so that silence isn't a stuck stream.
	prompt := serve(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("follow") == "true" {
			<-r.Context().Done()
			return
		}
		_, _ = fmt.Fprint(w, "Password: ")
	})
	if got := read(prompt, LogOptions{Follow: true, TailLines: 100}, 600*time.Millisecond); len(got) != 0 {
		t.Errorf("half a line: %q", got)
	}
	// Stuck every time: said, once, after a few tries.
	always := serve(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("follow") == "true" {
			<-r.Context().Done()
			return
		}
		_, _ = fmt.Fprintln(w, "hello")
	})
	if got := read(always, LogOptions{Follow: true, TailLines: 100}, 3*time.Second); len(got) != 1 || !strings.Contains(got[0], "stopped sending") {
		t.Errorf("always stuck: %q", got)
	}
}

// A container held behind its setup steps (PodInitializing) waits on
// them, and never starts if one failed: say so, not only "not yet".
func TestLogErrorSetupStepsFirst(t *testing.T) {
	got := logError(`container "main" in pod "initfail-bcdfg" is waiting to start: PodInitializing`, false)
	if !strings.Contains(got, "setup step") || !strings.Contains(got, "never starts") {
		t.Errorf("%q", got)
	}
}

// QA round 128: setup that is simply still running isn't a failure: the
// note says it starts once they finish, and only if one fails it never
// does.
func TestLogErrorSetupStillRunning(t *testing.T) {
	got := logError(`container "main" in pod "slow-bcdfg" is waiting to start: PodInitializing`, false)
	if !strings.Contains(got, "starts once they finish") || strings.Contains(got, "if one failed") || strings.Contains(got, "say why") {
		t.Errorf("%q", got)
	}
}

// Round 150: a container that never started gets one plain line with
// the reason, not the API's raw text.
func TestLogErrorNeverStartedInWords(t *testing.T) {
	for reason, want := range map[string]string{
		"ErrImageNeverPull":     "pull policy is Never",
		"ImagePullBackOff":      "can't download its image",
		"ErrImagePull":          "can't download its image",
		"image can't be pulled": "can't download its image",
		"ContainerCreating":     "being set up on its node",
	} {
		got := logError(`container "main" in pod "p-bcdfg" is waiting to start: `+reason, false)
		if !strings.Contains(got, "hasn't started yet") || !strings.Contains(got, want) || strings.Contains(got, "Details") || strings.Contains(got, "waiting to start") {
			t.Errorf("%s: %q", reason, got)
		}
	}
}

// A container that ran before and now waits (a crash loop) hasn't
// "not started yet": its live logs say it ran, and p shows that run.
func TestLogErrorWaitingAfterRestart(t *testing.T) {
	err := `container "app" in pod "web-1" is waiting to start: CrashLoopBackOff`
	got := logErrorOf(err, LogOptions{Restarted: true})
	if strings.Contains(got, "hasn't started") || !strings.Contains(got, "ran before") || !strings.Contains(got, "CrashLoopBackOff") {
		t.Errorf("restarted, waiting: %q", got)
	}
	if got := logErrorOf(err, LogOptions{}); !strings.Contains(got, "hasn't started yet") {
		t.Errorf("never ran, waiting: %q", got)
	}
	init := `container "app" in pod "web-1" is waiting to start: PodInitializing`
	if got := logErrorOf(init, LogOptions{Restarted: true}); !strings.Contains(got, "init containers") {
		t.Errorf("restarted, init running: %q", got)
	}
}
