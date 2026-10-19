package ui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// On a light terminal the adaptive colours must switch to their darker
// variants (readable on white).
func TestLightTerminalPalette(t *testing.T) {
	lipgloss.SetColorProfile(termenv.TrueColor)
	defer lipgloss.SetHasDarkBackground(true)

	lipgloss.SetHasDarkBackground(false)
	light := styleInfoLabel.Render("Context:") + renderLogo(false)
	lipgloss.SetHasDarkBackground(true)
	dark := styleInfoLabel.Render("Context:") + renderLogo(false)

	for _, want := range []string{"192;86;0" /* label #C05600 */, "31;79;216" /* logo K #1F4FD8 */} {
		if !strings.Contains(light, want) {
			t.Errorf("light palette missing %s", want)
		}
	}
	if !strings.Contains(dark, "255;163;72") /* label #FFA348 */ {
		t.Error("dark palette missing label colour")
	}
}
