package kube

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/muesli/cancelreader"
	"golang.org/x/term"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// shellCmd prefers bash and falls back to sh.
var shellCmd = []string{"/bin/sh", "-c", "command -v bash >/dev/null && exec bash || exec sh"}

// Shell is an interactive shell in a container. It implements Bubble Tea's
// ExecCommand interface, so the UI can hand the terminal over to it.
type Shell struct {
	client    *Client
	ns, pod   string
	container string

	stdin          io.Reader
	stdout, stderr io.Writer
}

// NewShell prepares (but does not start) a shell session.
func (c *Client) NewShell(ns, pod, container string) *Shell {
	return &Shell{client: c, ns: ns, pod: pod, container: container, stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr}
}

// SetStdin is called by Bubble Tea before Run.
func (s *Shell) SetStdin(r io.Reader) { s.stdin = r }

// SetStdout is called by Bubble Tea before Run.
func (s *Shell) SetStdout(w io.Writer) { s.stdout = w }

// SetStderr is called by Bubble Tea before Run.
func (s *Shell) SetStderr(w io.Writer) { s.stderr = w }

// shellExecutor prepares an interactive TTY exec of shellCmd.
func (c *Client) shellExecutor(ns, pod, container string) (remotecommand.Executor, error) {
	req := c.Typed.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(ns).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   shellCmd,
			Stdin:     true,
			Stdout:    true,
			TTY:       true,
		}, scheme.ParameterCodec)
	return remotecommand.NewSPDYExecutor(c.Config, "POST", req.URL())
}

// Run opens the shell and blocks until it exits.
func (s *Shell) Run() error {
	exec, err := s.client.shellExecutor(s.ns, s.pod, s.container)
	if err != nil {
		return err
	}
	out := bannerFirst(s.stdout, fmt.Sprintf("kubed: a shell in %s/%s (%s). Type exit to go back to kubed.\r\n", s.ns, s.pod, s.container))

	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		state, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		defer term.Restore(fd, state)
	}

	// client-go keeps reading stdin after the shell exits; without a
	// cancellable reader that read swallows the next key pressed in kubed.
	stdin, err := cancelreader.NewReader(s.stdin)
	if err != nil {
		return err
	}
	defer stdin.Close()
	defer stdin.Cancel()

	fmt.Fprintf(s.stderr, "connecting to %s/%s (%s)…\r\n", s.ns, s.pod, s.container)
	sizes := newResizeQueue(fd)
	defer sizes.stop()
	return exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            out,
		Tty:               true,
		TerminalSizeQueue: sizes,
	})
}

// resizeQueue forwards terminal size changes (SIGWINCH) to the container.
type resizeQueue struct {
	fd    int
	ch    chan os.Signal
	first bool
}

func newResizeQueue(fd int) *resizeQueue {
	q := &resizeQueue{fd: fd, ch: make(chan os.Signal, 1), first: true}
	signal.Notify(q.ch, syscall.SIGWINCH)
	return q
}

func (q *resizeQueue) stop() {
	signal.Stop(q.ch)
	close(q.ch)
}

func (q *resizeQueue) Next() *remotecommand.TerminalSize {
	if !q.first {
		if _, ok := <-q.ch; !ok {
			return nil
		}
	}
	q.first = false
	w, h, err := term.GetSize(q.fd)
	if err != nil {
		return nil
	}
	return &remotecommand.TerminalSize{Width: uint16(w), Height: uint16(h)}
}

// bannerFirst writes banner just before the shell's first output, so a
// shell that fails at once (no /bin/sh in the image) never promises one.
func bannerFirst(w io.Writer, banner string) io.Writer {
	return &bannerWriter{w: w, banner: banner}
}

type bannerWriter struct {
	w      io.Writer
	banner string
	said   bool
}

func (b *bannerWriter) Write(p []byte) (int, error) {
	if !b.said && len(p) > 0 {
		b.said = true
		if _, err := io.WriteString(b.w, b.banner); err != nil {
			return 0, err
		}
	}
	return b.w.Write(p)
}
