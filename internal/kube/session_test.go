package kube

import (
	"context"
	"io"
	"testing"
	"time"

	"k8s.io/client-go/tools/remotecommand"
)

// echoExec pretends to be a remote shell: it echoes stdin to stdout until
// stdin closes, and records the first terminal size.
type echoExec struct {
	size chan remotecommand.TerminalSize
}

func (e echoExec) Stream(remotecommand.StreamOptions) error { return nil }

func (e echoExec) StreamWithContext(_ context.Context, o remotecommand.StreamOptions) error {
	e.size <- *o.TerminalSizeQueue.Next()
	_, err := io.Copy(o.Stdout, o.Stdin)
	return err
}

func TestSessionRoundTrip(t *testing.T) {
	ex := echoExec{size: make(chan remotecommand.TerminalSize, 1)}
	s := startSession(ex, 80, 24)
	if got := <-ex.size; got.Width != 80 || got.Height != 24 {
		t.Fatalf("first size = %+v", got)
	}
	if _, err := s.Write([]byte("ls\r")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(s.Output(), buf); err != nil || string(buf) != "ls\r" {
		t.Fatalf("echo = %q, %v", buf, err)
	}
	s.Close()
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not end after Close")
	}
}
