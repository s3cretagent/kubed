package kube

import (
	"context"
	"io"
	"sync"
	"time"

	"k8s.io/client-go/tools/remotecommand"
)

// Session is a shell in a container whose input and output are plain
// streams, so the UI can draw it inside its own panel instead of handing
// over the terminal (that is what Shell does).
type Session struct {
	stdin  *io.PipeWriter
	output *io.PipeReader

	sizes  chan remotecommand.TerminalSize
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	once   sync.Once
}

// StartSession opens a TTY shell sized cols x rows.
func (c *Client) StartSession(ns, pod, container string, cols, rows int) (*Session, error) {
	exec, err := c.shellExecutor(ns, pod, container)
	if err != nil {
		return nil, err
	}
	return startSession(exec, cols, rows), nil
}

func startSession(exec remotecommand.Executor, cols, rows int) *Session {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		stdin:  inW,
		output: outR,
		sizes:  make(chan remotecommand.TerminalSize, 4),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.sizes <- remotecommand.TerminalSize{Width: uint16(cols), Height: uint16(rows)}

	go func() {
		s.err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:             inR,
			Stdout:            outW,
			Tty:               true,
			TerminalSizeQueue: sizeQueue(s.sizes),
		})
		_ = outW.CloseWithError(io.EOF)
		_ = inR.Close()
		close(s.done)
	}()
	return s
}

// Output is the shell's screen output (stdout and stderr, merged by the TTY).
func (s *Session) Output() io.Reader { return s.output }

// Write sends keystrokes to the shell.
func (s *Session) Write(p []byte) (int, error) { return s.stdin.Write(p) }

// Resize tells the container the new terminal size. It never blocks; if
// sizes pile up only the latest few are kept.
func (s *Session) Resize(cols, rows int) {
	select {
	case s.sizes <- remotecommand.TerminalSize{Width: uint16(cols), Height: uint16(rows)}:
	default:
	}
}

// Done closes when the shell has exited.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err is why the shell ended (nil for a normal exit). Valid after Done.
func (s *Session) Err() error { return s.err }

// Close ends the shell. It first sends end-of-input so the shell in the
// pod exits (cutting the connection alone leaves it running there), and
// only cuts the connection if the shell hasn't gone within a second.
func (s *Session) Close() {
	s.once.Do(func() {
		go func() {
			// ctrl-c stops a running command and clears the line; once the
			// shell is back at its prompt, ctrl-d (or exit) ends it. Sent
			// together, the ctrl-d is lost. A write can block if nothing
			// reads the output any more; the other goroutine cuts it off.
			for _, keys := range []string{"\x03", "\x04", "exit\r"} {
				select {
				case <-s.done:
					return
				default:
				}
				_, _ = s.stdin.Write([]byte(keys))
				time.Sleep(250 * time.Millisecond)
			}
			_ = s.stdin.Close()
		}()
		go func() {
			select {
			case <-s.done:
			case <-time.After(1500 * time.Millisecond):
			}
			s.cancel()
			_ = s.stdin.Close()
			_ = s.output.Close() // nobody reads it now; unblocks a stuck write
		}()
	})
}

// sizeQueue adapts a channel to remotecommand.TerminalSizeQueue.
type sizeQueue chan remotecommand.TerminalSize

func (q sizeQueue) Next() *remotecommand.TerminalSize {
	size, ok := <-q
	if !ok {
		return nil
	}
	return &size
}
