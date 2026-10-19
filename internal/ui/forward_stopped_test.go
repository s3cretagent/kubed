package ui

import (
	"strings"
	"testing"
	"time"
)

// A forward that stopped stays in :pf with why, until ctrl-d clears it.
func TestStoppedForwardStaysListed(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.noteStopped(stoppedForward{port: 18553, podPort: 80, target: "deployment web", why: "couldn't start again: no pod of deployment web is running", at: time.Now()})
	rows := m.forwardRows()
	if len(rows) != 1 || !strings.Contains(rows[0].Cells[4], "Stopped at") || !strings.Contains(rows[0].Cells[4], "no pod of deployment web") {
		t.Fatalf("rows %+v", rows)
	}
	m.stopForwards(rows)
	if len(m.forwardRows()) != 0 || !strings.Contains(m.flash, "cleared 1 stopped port") {
		t.Errorf("after ctrl-d: rows %v, flash %q", m.forwardRows(), m.flash)
	}
}

// On a narrow screen the box under :pf says why a forward stopped in full.
func TestStoppedForwardReasonReadableWhenNarrow(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 80, 24
	m.noteStopped(stoppedForward{port: 18081, podPort: 81, target: "deployment web", why: "lost connection to pod (is something listening on port 81?)", at: time.Now()})
	m.navigate(location{kind: locForwards}, false)
	v := m.View()
	if !strings.Contains(v, "something listening on") || !strings.Contains(v, "port 81?") || !strings.Contains(v, "press F on deployment web") {
		t.Errorf("the reason isn't on screen:\n%s", v)
	}
}
