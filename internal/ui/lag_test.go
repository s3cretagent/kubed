package ui

import (
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// QA round 136: a scale confirmed with y never reached the cluster and
// no message was seen. The change waited on a connection gone quiet, and
// its failure, coming many seconds later, went away at the next key
// pressed (any key 2s after an error cleared it). A command's failure now
// stays through keys for failureKeep, and in the footer.
func TestCommandFailureStaysThroughKeys(t *testing.T) {
	m := testModel()
	m.width, m.height = 120, 40
	m.Update(resultMsg{err: errors.New(`Patch "https://x/apis/apps/v1/namespaces/shop/deployments/web/scale": http2: client connection lost`)})
	if !m.flashErr || !strings.Contains(m.flash, "lost the connection") {
		t.Fatalf("flash = %q (error %v), want the lost connection said", m.flash, m.flashErr)
	}
	m.flashAt = time.Now().Add(-3 * time.Second) // shown a moment ago; you were typing as it came
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	if m.flash == "" || !m.flashShown() {
		t.Fatalf("a key cleared the failure (flash %q)", m.flash)
	}
	if foot := strings.Join(m.footerLines(), "\n"); !strings.Contains(foot, "lost the connection") {
		t.Errorf("footer doesn't say it failed:\n%s", foot)
	}

	// Once it had its time, a key clears it as before.
	m.flashAt, m.flashKeep = time.Now().Add(-failureKeep-time.Second), time.Now().Add(-time.Second)
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.flash != "" {
		t.Errorf("still shown after its time: %q", m.flash)
	}
}

// Other errors (a key that doesn't apply here) still go at the next key
// after 2s, so the footer's keys come back; a new message isn't held by an
// older failure's time.
func TestOtherErrorsStillClearOnKeys(t *testing.T) {
	m := testModel()
	m.width, m.height = 120, 40
	m.Update(resultMsg{err: errors.New("first failed")})
	m.setFlash("", errors.New("that works on deployments"))
	m.flashAt = time.Now().Add(-3 * time.Second)
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if m.flash != "" {
		t.Errorf("flash %q kept, want it cleared by the key", m.flash)
	}
}
