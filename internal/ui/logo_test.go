package ui

import (
	"testing"

	"github.com/charmbracelet/lipgloss"
)

func TestLogoRowsAlign(t *testing.T) {
	for i, letter := range logoLetters {
		if len(letter) != logoHeight {
			t.Fatalf("letter %d has %d rows, want %d", i, len(letter), logoHeight)
		}
		w := lipgloss.Width(letter[0])
		for r, row := range letter {
			if lipgloss.Width(row) != w {
				t.Errorf("letter %d row %d is %d wide, want %d", i, r, lipgloss.Width(row), w)
			}
		}
	}
	if len(logoColors) != len(logoLetters) {
		t.Errorf("%d colours for %d letters", len(logoColors), len(logoLetters))
	}
}
