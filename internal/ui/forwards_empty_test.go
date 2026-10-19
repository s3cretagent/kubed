package ui

import (
	"strings"
	"testing"
)

// The port list with nothing open says how to open one, in both modes.
func TestNoPortsOpenSaysHow(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 30
	m.navigate(location{kind: locForwards}, false)
	if v := m.View(); !strings.Contains(v, "No ports open. Select") {
		t.Errorf("empty port list:\n%s", v)
	}
	m.opts.ReadOnly = true
	if v := m.View(); !strings.Contains(v, "so F can't open any") {
		t.Errorf("empty port list, read-only:\n%s", v)
	}
}
