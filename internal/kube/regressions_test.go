package kube

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Round 118: a followed stream that sent its lines and then went quiet (an
// app with nothing more to say) is not stuck: it isn't opened again, which
// would show its lines twice and then claim the cluster stopped sending.
func TestQuietFollowAfterLinesIsNotStuck(t *testing.T) {
	old := followProbe
	followProbe = 100 * time.Millisecond
	defer func() { followProbe = old }()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, "hello")
		if r.URL.Query().Get("follow") == "true" {
			w.(http.Flusher).Flush()
			<-r.Context().Done() // nothing more to say
		}
	}))
	defer srv.Close()
	cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	out := make(chan string, 10)
	go (&Client{Typed: cs}).StreamLogs(ctx, "ns", "p", LogOptions{Follow: true, TailLines: 100}, out)
	var lines []string
	for l := range out {
		lines = append(lines, l)
	}
	if len(lines) != 1 || lines[0] != "hello" {
		t.Errorf("got %q, want the one line once", lines)
	}
}
