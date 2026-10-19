package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Letters of "KUBED" in the ANSI Shadow figlet font. Every row of a letter
// has the same width so the letters line up when joined.
var logoLetters = [][]string{
	{ // K
		"██╗  ██╗",
		"██║ ██╔╝",
		"█████╔╝ ",
		"██╔═██╗ ",
		"██║  ██╗",
		"╚═╝  ╚═╝",
	},
	{ // U
		"██╗   ██╗",
		"██║   ██║",
		"██║   ██║",
		"██║   ██║",
		"╚██████╔╝",
		" ╚═════╝ ",
	},
	{ // B
		"██████╗ ",
		"██╔══██╗",
		"██████╔╝",
		"██╔══██╗",
		"██████╔╝",
		"╚═════╝ ",
	},
	{ // E
		"███████╗",
		"██╔════╝",
		"█████╗  ",
		"██╔══╝  ",
		"███████╗",
		"╚══════╝",
	},
	{ // D
		"██████╗ ",
		"██╔══██╗",
		"██║  ██║",
		"██║  ██║",
		"██████╔╝",
		"╚═════╝ ",
	},
}

// logoColors run blue → cyan → green, one per letter. Light-terminal shades
// are darker so the letters stay readable on white.
var logoColors = []lipgloss.AdaptiveColor{
	{Light: "#1F4FD8", Dark: "#5F87FF"},
	{Light: "#0070C0", Dark: "#5FAFFF"},
	{Light: "#008B9E", Dark: "#5FD7FF"},
	{Light: "#00896B", Dark: "#5FD7AF"},
	{Light: "#2E8B2E", Dark: "#5FD787"},
}

// prodLogoColors run red → orange: on a production cluster the logo itself
// says where you are.
var prodLogoColors = []lipgloss.AdaptiveColor{
	{Light: "#B71C1C", Dark: "#FF5F5F"},
	{Light: "#C62828", Dark: "#FF6E5A"},
	{Light: "#D84315", Dark: "#FF8052"},
	{Light: "#E65100", Dark: "#FF934A"},
	{Light: "#EF6C00", Dark: "#FFA742"},
}

const logoHeight = 6

// renderLogo draws "KUBED", each letter in its own colour.
func renderLogo(prod bool) string {
	colors := logoColors
	if prod {
		colors = prodLogoColors
	}
	rows := make([]string, logoHeight)
	for r := range rows {
		var b strings.Builder
		for i, letter := range logoLetters {
			b.WriteString(lipgloss.NewStyle().Foreground(colors[i]).Bold(true).Render(letter[r]))
		}
		rows[r] = b.String()
	}
	return strings.Join(rows, "\n")
}
