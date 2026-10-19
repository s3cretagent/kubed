package ui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

// helpLines opens ? on a w by h screen and returns all its lines, plain.
func helpLines(t *testing.T, w, h int) []string {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	mxClusters()
	m := mxModel(mxFull, w, h)
	m.openHelp()
	var rows []string // as the screen wraps them
	for _, l := range m.pager.lines {
		rows = append(rows, strings.Split(ansi.Strip(hangWrapAt(l, max(m.pager.vp.Width, 1), m.pager.hang)), "\n")...)
	}
	return rows
}

// lineWith is the index of the first line holding s (-1 for none).
func lineWith(lines []string, s string) int {
	for i, l := range lines {
		if strings.Contains(l, s) {
			return i
		}
	}
	return -1
}

// QA round 140: ? at 40x15 broke its two columns: keys landed in the
// description column, and separate key groups shared a line. Narrow, it
// is one column: each key on its own line, its words indented under it.
// At 80 columns the two columns stay; a line too long for them is split
// into its groups, never wrapped into the key column.
func TestHelpNarrowIsOneColumn(t *testing.T) {
	lines := helpLines(t, 40, 15)
	for _, l := range lines {
		if len([]rune(l)) > 36 {
			t.Errorf("wider than the screen: %q", l)
		}
	}
	for _, c := range []struct{ key, words string }{{"↑ ↓", "move"}, {"enter", "open, or"}, {"esc", "go back"}, {"q", "quit"}, {"Y", "copy its name"}} {
		i := -1
		for j, l := range lines {
			if strings.TrimSpace(l) == c.key {
				i = j
				break
			}
		}
		if i < 0 || i+1 >= len(lines) || !strings.HasPrefix(strings.TrimSpace(lines[i+1]), c.words) ||
			len(lines[i+1])-len(strings.TrimLeft(lines[i+1], " ")) <= len(lines[i])-len(strings.TrimLeft(lines[i], " ")) {
			t.Errorf("key %q not on its own line with %q indented under it:\n%s", c.key, c.words, strings.Join(lines, "\n"))
		}
	}
	for _, pair := range [][2]string{{"move", "enter"}, {"gh apps", "gp pods"}, {"details", "its YAML"}} {
		if i := lineWith(lines, pair[0]); i >= 0 && strings.Contains(lines[i], pair[1]) {
			t.Errorf("groups merged: %q", lines[i])
		}
	}

	lines = helpLines(t, 80, 24)
	if lineWith(lines, "↑ ↓     move                 enter   open") < 0 {
		t.Errorf("80 columns lost its two columns:\n%s", strings.Join(lines, "\n"))
	}
	c, d := lineWith(lines, "c  stop new pods landing on a node (cordon)"), lineWith(lines, "D  move a node's pods off (drain)")
	if c < 0 || d < 0 || c == d || !strings.HasPrefix(lines[d], "  D") {
		t.Errorf("cordon and drain: lines %d %d:\n%s", c, d, strings.Join(lines, "\n"))
	}
}
