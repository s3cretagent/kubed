package ui

import (
	"github.com/charmbracelet/lipgloss"

	"github.com/s3cretagent/kubed/internal/view"
)

// Adaptive colours work on light and dark terminals. The palette follows
// k9s: orange labels, blue keys and borders, cyan titles.
var (
	colAccent = lipgloss.AdaptiveColor{Light: "#005FAF", Dark: "#5FAFFF"}
	colMuted  = lipgloss.AdaptiveColor{Light: "#6C6C6C", Dark: "#9E9E9E"}
	colOK     = lipgloss.AdaptiveColor{Light: "#00875F", Dark: "#5FD787"}
	colWarn   = lipgloss.AdaptiveColor{Light: "#8A6D00", Dark: "#E6C85A"} // yellow: clearly not the orange of labels
	colBad    = lipgloss.AdaptiveColor{Light: "#D70000", Dark: "#FF5F5F"}
	colLabel  = lipgloss.AdaptiveColor{Light: "#C05600", Dark: "#FFA348"}
	colTitle  = lipgloss.AdaptiveColor{Light: "#007C91", Dark: "#5FD7FF"}
	colNS     = lipgloss.AdaptiveColor{Light: "#8700AF", Dark: "#D787FF"}
	colBorder = lipgloss.AdaptiveColor{Light: "#5F87D7", Dark: "#3A6EA5"}

	styleLabel      = lipgloss.NewStyle().Foreground(colMuted)
	styleInfoLabel  = lipgloss.NewStyle().Foreground(colLabel).Bold(true)
	styleValue      = lipgloss.NewStyle().Bold(true)
	styleProd       = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FFFFFF")).Background(lipgloss.Color("#B71C1C")).Padding(0, 1) // dark red: white text reads on it
	styleRO         = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#000000")).Background(colOK).Padding(0, 1)                     // dark on green reads better
	styleWarnBadge  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#000000")).Background(colWarn).Padding(0, 1)
	styleBorderProd = lipgloss.NewStyle().Foreground(colBad)
	styleHeader     = lipgloss.NewStyle().Bold(true).Foreground(colLabel)
	styleCursor     = lipgloss.NewStyle().Reverse(true)
	styleMuted      = lipgloss.NewStyle().Foreground(colMuted)
	styleErr        = lipgloss.NewStyle().Foreground(colBad).Bold(true)
	styleWarn       = lipgloss.NewStyle().Foreground(colWarn)
	styleOK         = lipgloss.NewStyle().Foreground(colOK)
	styleKey        = lipgloss.NewStyle().Foreground(colAccent).Bold(true)
	styleBorder     = lipgloss.NewStyle().Foreground(colBorder)
	styleBoxTitle   = lipgloss.NewStyle().Foreground(colTitle).Bold(true)
	styleTitleNS    = lipgloss.NewStyle().Foreground(colNS).Bold(true)
	stylePrompt     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colWarn).Padding(0, 1)
	styleMenu       = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colTitle).Padding(0, 1)
	styleTitle      = lipgloss.NewStyle().Bold(true)
	styleFilter     = lipgloss.NewStyle().Foreground(colWarn)
	styleMatch      = lipgloss.NewStyle().Foreground(lipgloss.Color("#000000")).Background(colWarn)
)

// logTagColors tell pods apart in a workload's merged logs.
var logTagColors = []lipgloss.AdaptiveColor{colAccent, colLabel, colOK, colNS, colTitle, colWarn}

func logTagStyle(i int) lipgloss.Style {
	return lipgloss.NewStyle().Foreground(logTagColors[i%len(logTagColors)])
}

func toneStyle(t view.Tone) lipgloss.Style {
	switch t {
	case view.ToneOK:
		return styleOK
	case view.ToneWarn:
		return styleWarn
	case view.ToneBad:
		return styleErr
	case view.ToneDim:
		return styleMuted
	}
	return lipgloss.NewStyle()
}
