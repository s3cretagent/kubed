package ui

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/vt"

	"github.com/s3cretagent/kubed/internal/kube"
)

// leaveShellKey leaves an in-panel shell (the telnet / docker detach key).
const leaveShellKey = tea.KeyCtrlCloseBracket

// terminal is a shell drawn inside kubed's body box: the container's output
// feeds a VT emulator sized to the box, and keys are turned back into bytes.
//
//	keys ─► keyBytes ─► session stdin
//	box  ◄─ render   ◄─ emulator ◄─ session output
type terminal struct {
	title string
	sess  *kube.Session

	mu     sync.Mutex // guards emu and broken
	emu    *vt.Emulator
	broken error // set if the emulator panicked; the shell is closed

	// updates gets a value whenever the screen changed; closed when the
	// shell ends.
	updates chan struct{}
}

func newTerminal(title string, sess *kube.Session, cols, rows int) *terminal {
	t := &terminal{title: title, sess: sess, emu: vt.NewEmulator(cols, rows), updates: make(chan struct{}, 1)}
	go t.pumpOutput()
	go t.pumpReplies()
	return t
}

// pumpOutput copies container output into the emulator.
func (t *terminal) pumpOutput() {
	defer close(t.updates)
	buf := make([]byte, 32*1024)
	for {
		n, err := t.sess.Output().Read(buf)
		if n > 0 {
			if !t.write(buf[:n]) {
				return
			}
			select {
			case t.updates <- struct{}{}:
			default: // a redraw is already pending
			}
		}
		if err != nil {
			return
		}
	}
}

// write feeds the emulator. The emulator library is experimental: if it
// panics, the shell is closed with an error instead of crashing kubed.
func (t *terminal) write(p []byte) (ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			t.broken = fmt.Errorf("terminal display failed (%v). Use S for a full-screen shell", r)
			t.sess.Close()
			ok = false
		}
	}()
	_, _ = t.emu.Write(p)
	return true
}

// brokenErr is why the emulator stopped, if it panicked.
func (t *terminal) brokenErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.broken
}

// pumpReplies forwards what the emulator answers to terminal queries (cursor
// position reports and the like) back to the container.
func (t *terminal) pumpReplies() {
	buf := make([]byte, 1024)
	for {
		n, err := t.emu.Read(buf)
		if n > 0 {
			if _, werr := t.sess.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (t *terminal) resize(cols, rows int) {
	t.mu.Lock()
	t.emu.Resize(cols, rows)
	t.mu.Unlock()
	t.sess.Resize(cols, rows)
}

func (t *terminal) close() {
	t.sess.Close()
	t.mu.Lock()
	_ = t.emu.Close()
	t.mu.Unlock()
}

// send writes a key to the shell.
func (t *terminal) send(msg tea.KeyMsg) {
	if b := keyBytes(msg); len(b) > 0 {
		_, _ = t.sess.Write(b)
	}
}

// lines renders the emulator screen, with the cursor as a reversed cell.
func (t *terminal) lines() (out []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.broken != nil {
		return []string{t.broken.Error()}
	}
	defer func() {
		if r := recover(); r != nil {
			t.broken = fmt.Errorf("terminal display failed (%v). Use S for a full-screen shell", r)
			t.sess.Close()
			out = []string{t.broken.Error()}
		}
	}()
	w, h := t.emu.Width(), t.emu.Height()
	cur := t.emu.CursorPosition()
	out = make([]string, h)
	for y := range h {
		var b strings.Builder
		for x := 0; x < w; {
			cell := t.emu.CellAt(x, y)
			content, style, width := " ", uv.Style{}, 1
			if cell != nil && cell.Width > 0 {
				content, style, width = cell.Content, cell.Style, cell.Width
				if content == "" {
					content = " "
				}
			}
			if x == cur.X && y == cur.Y {
				style.Attrs ^= uv.AttrReverse
			}
			b.WriteString(style.Styled(content))
			x += width
		}
		out[y] = b.String()
	}
	return out
}

// terminalMsg means the shell screen changed; done means the shell ended.
type terminalMsg struct {
	t    *terminal
	done bool
}

func waitTerminal(t *terminal) tea.Cmd {
	return func() tea.Msg {
		if _, ok := <-t.updates; !ok {
			<-t.sess.Done()
			return terminalMsg{t: t, done: true}
		}
		return terminalMsg{t: t}
	}
}

// keyBytes turns a Bubble Tea key into what a terminal would send.
func keyBytes(msg tea.KeyMsg) []byte {
	var b []byte
	switch msg.Type {
	case tea.KeyRunes:
		b = []byte(string(msg.Runes))
	case tea.KeySpace:
		b = []byte(" ")
	case tea.KeyUp:
		b = []byte("\x1b[A")
	case tea.KeyDown:
		b = []byte("\x1b[B")
	case tea.KeyRight:
		b = []byte("\x1b[C")
	case tea.KeyLeft:
		b = []byte("\x1b[D")
	case tea.KeyHome:
		b = []byte("\x1b[H")
	case tea.KeyEnd:
		b = []byte("\x1b[F")
	case tea.KeyPgUp:
		b = []byte("\x1b[5~")
	case tea.KeyPgDown:
		b = []byte("\x1b[6~")
	case tea.KeyDelete:
		b = []byte("\x1b[3~")
	case tea.KeyInsert:
		b = []byte("\x1b[2~")
	case tea.KeyShiftTab:
		b = []byte("\x1b[Z")
	default:
		// Control keys (ctrl-a…ctrl-z, enter, tab, esc, backspace) are the
		// ASCII control codes themselves.
		if t := int(msg.Type); t >= 0 && t <= 127 {
			b = []byte{byte(t)}
		}
	}
	if msg.Alt && len(b) > 0 {
		b = append([]byte{0x1b}, b...)
	}
	return b
}

// shellEnded explains how a shell finished, for the footer.
func shellEnded(err error) resultMsg {
	if err == nil || errors.Is(err, io.EOF) {
		return resultMsg{text: "shell closed", shellEnd: true}
	}
	return resultMsg{err: err, shellEnd: true}
}
