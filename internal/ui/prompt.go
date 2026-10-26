package ui

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type promptKind int

const (
	promptConfirm promptKind = iota
	promptInput
	promptChoose
	promptMenu
)

// menuItem is one entry of the Enter menu.
type menuItem struct {
	label    string
	key      string // shortcut shown on the right; pressing it picks the item
	about    string // what it does, shown under the menu while highlighted
	disabled bool   // shown greyed out (read-only); picking it says why
	off      string // why, in a word or two, after its label ("read-only", "no pods")
	hidden   bool   // can never apply to this row: left out of the menu
	run      func() tea.Cmd
	// also is a second key that acts on this item while it is highlighted
	// (b on a version: go back to it), without opening it first.
	also    string
	alsoRun func() tea.Cmd
}

// prompt is a modal question: confirm an action, type a value, or pick one
// option from a list.
type prompt struct {
	kind      promptKind
	title     string
	warnings  []string
	foundMenu bool // the menu a search result opens with: esc goes back to the results
	// backTo, when set, reopens the screen this menu was opened from (the
	// rollout view, for the versions list it opens): esc goes back there.
	backTo func() tea.Cmd

	// confirm: when typeToConfirm is set (production contexts) the user must
	// type it instead of pressing y.
	typeToConfirm string
	onYes         func() tea.Cmd

	// input
	input    textinput.Model
	onSubmit func(string) tea.Cmd
	validate func(string) error // optional: a bad value keeps the prompt open

	problem string // why the last enter didn't go through, shown in red

	// fresh: the input still holds its suggested value, untouched. Typing
	// replaces it (like selected text); arrows or backspace edit it.
	fresh bool

	// choose
	options []string
	choice  int
	onPick  func(string) tea.Cmd

	// menu
	items    []menuItem
	maxItems int    // how many fit on screen; more scroll
	tick     func() // runs every tick while open (ages that move on)

	// Where the menu was drawn, for mouse clicks: the item on each line
	// of the box's text, and the box's place on the screen.
	itemAt                    map[int]int
	clicked                   int // the item clicked last (-1: none): a second click on it picks it
	boxTop, boxLeft, boxWidth int
}

func menuPrompt(title string, items []menuItem) *prompt {
	return &prompt{kind: promptMenu, title: title, items: items, clicked: -1}
}

func confirmPrompt(title string, warnings []string, typeToConfirm string, onYes func() tea.Cmd) *prompt {
	p := &prompt{kind: promptConfirm, title: title, warnings: warnings, typeToConfirm: typeToConfirm, onYes: onYes}
	if typeToConfirm != "" {
		p.input = newInput("")
	}
	return p
}

func inputPrompt(title, value string, onSubmit func(string) tea.Cmd) *prompt {
	return &prompt{kind: promptInput, title: title, input: newInput(value), onSubmit: onSubmit, fresh: value != ""}
}

func newInput(value string) textinput.Model {
	ti := textinput.New()
	ti.Prompt = "> "
	ti.SetValue(value)
	ti.CursorEnd()
	ti.Focus()
	return ti
}

// update returns (cmd, done). done closes the prompt.
func (p *prompt) update(msg tea.KeyMsg) (tea.Cmd, bool) {
	if msg.String() == "esc" || msg.String() == "ctrl+c" {
		return nil, true
	}
	switch p.kind {
	case promptConfirm:
		if p.typeToConfirm == "" {
			switch msg.String() {
			case "y", "Y":
				return p.onYes(), true
			case "n", "N", "q":
				return nil, true
			}
			return nil, false
		}
		if msg.String() == "enter" {
			if strings.TrimSpace(p.input.Value()) == p.typeToConfirm {
				return p.onYes(), true
			}
			p.problem = "That doesn't match. Type " + p.typeToConfirm + " exactly, or esc to cancel."
			return nil, false
		}
	case promptInput:
		if p.fresh && msg.String() != "enter" {
			p.fresh = false
			if msg.Type == tea.KeyRunes || msg.Type == tea.KeySpace {
				p.input.SetValue("")
			}
		}
		if msg.String() == "enter" {
			v := strings.TrimSpace(p.input.Value())
			if p.validate != nil {
				if err := p.validate(v); err != nil {
					p.problem = err.Error()
					return nil, false
				}
			}
			return p.onSubmit(v), true
		}
	case promptChoose:
		switch msg.String() {
		case "up", "k":
			p.choice = max(p.choice-1, 0)
		case "down", "j":
			p.choice = min(p.choice+1, len(p.options)-1)
		case "enter":
			return p.onPick(p.options[p.choice]), true
		}
		return nil, false
	case promptMenu:
		switch key := msg.String(); key {
		case "up", "k": // wraps around, so the last item is one key away
			p.choice, p.problem = (p.choice-1+len(p.items))%len(p.items), ""
		case "down", "j":
			p.choice, p.problem = (p.choice+1)%len(p.items), ""
		case "enter":
			it := p.items[p.choice]
			if it.disabled { // say why, and stay: esc then closes just the menu
				p.problem = it.about
				return nil, false
			}
			if it.run == nil { // a line that only informs
				return nil, false
			}
			return it.run(), true
		default:
			if it := p.items[p.choice]; it.also != "" && it.also == key && it.alsoRun != nil {
				return it.alsoRun(), true
			}
			for i, it := range p.items {
				if it.key != "" && it.key == key && it.run != nil {
					if it.disabled {
						p.choice, p.problem = i, it.about
						return nil, false
					}
					return it.run(), true
				}
			}
		}
		return nil, false
	}
	var cmd tea.Cmd
	p.input, cmd = p.input.Update(msg)
	return cmd, false
}

func (p *prompt) render(width int) string {
	var b strings.Builder
	title := p.title
	if width > 0 { // a long title must not push the box past the screen
		// A long line wraps, keeping its indent: what a confirm says is
		// read in full, never cut with "…".
		var cut []string
		w := max(width-6, 20) // the box's border and padding take the rest
		for _, l := range strings.Split(title, "\n") {
			if len([]rune(l)) <= w {
				cut = append(cut, l)
				continue
			}
			indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
			for _, part := range wrapLines(l, w-len(indent), 1<<20) {
				cut = append(cut, indent+part)
			}
		}
		title = strings.Join(cut, "\n")
	}
	b.WriteString(styleTitle.Render(title))
	for _, w := range p.warnings {
		b.WriteString("\n" + styleWarn.Render("⚠ "+w))
	}
	switch p.kind {
	case promptConfirm:
		if p.typeToConfirm != "" {
			b.WriteString("\n" + styleErr.Render("This is a production cluster.") + " Type " + styleValue.Render(p.typeToConfirm) + " and press enter:")
			b.WriteString("\n" + p.input.View())
			if p.problem != "" {
				b.WriteString("\n" + styleErr.Render(p.problem))
			}
		} else {
			b.WriteString("\n" + styleKey.Render("[y]") + " yes   " + styleKey.Render("[n]") + " no")
		}
	case promptInput:
		if p.fresh {
			// Drawn like selected text: what you type replaces it.
			b.WriteString("\n" + p.input.Prompt + styleCursor.Render(p.input.Value()))
		} else {
			b.WriteString("\n" + p.input.View())
		}
		if p.problem != "" {
			b.WriteString("\n" + styleErr.Render(p.problem))
		}
		hint := "enter to apply, esc to cancel"
		if p.fresh {
			hint = "type to replace it, ← → to edit it, enter to apply, esc to cancel"
		}
		b.WriteString("\n" + styleMuted.Render(hint))
	case promptMenu:
		label := func(it menuItem) string {
			if it.disabled { // so it can't be mistaken for one that works
				return it.label + " · " + cmp.Or(it.off, "off")
			}
			return it.label
		}
		labelW := 0
		for _, it := range p.items {
			labelW = max(labelW, len(label(it)))
		}
		labelW = min(labelW, max(width-20, 20)) // inside the screen (with the box's frame and key): long labels end in …
		// As wide as the longest description needs, up to menuWidth, and
		// as many lines as the longest one takes (so the box doesn't jump).
		aboutW := 0
		for _, it := range p.items {
			aboutW = max(aboutW, len([]rune(it.about)))
		}
		aboutW = min(max(aboutW, 30), menuWidth(width))
		rows := 1
		for _, it := range p.items {
			rows = max(rows, len(wrapLines(it.about, aboutW, 3)))
		}
		// A menu taller than the screen scrolls around the highlighted item.
		// The room was worked out for three lines of description: shorter
		// ones leave room for more items.
		first, last := 0, len(p.items)
		if show := p.maxItems + 3 - rows; p.maxItems > 0 && len(p.items) > show {
			first = min(max(p.choice-show/2, 0), len(p.items)-show)
			last = first + show
		}
		if first > 0 {
			b.WriteString("\n" + styleMuted.Render(fmt.Sprintf("  ↑ %d more", first)))
		}
		p.itemAt = map[int]int{}
		for i := first; i < last; i++ {
			p.itemAt[strings.Count(b.String(), "\n")+1] = i
			it := p.items[i]
			key := styleKey.Render(displayKey(it.key))
			text := truncate(label(it), labelW)
			line := "  " + padRight(text, labelW+3) + key
			if it.disabled {
				line = styleMuted.Render("  " + padRight(text, labelW+3) + displayKey(it.key))
			}
			if i == p.choice {
				line = styleCursor.Render("▶ "+padRight(text, labelW+3)) + key
			}
			b.WriteString("\n" + line)
		}
		if last < len(p.items) {
			b.WriteString("\n" + styleMuted.Render(fmt.Sprintf("  ↓ %d more", len(p.items)-last)))
		}
		// What the highlighted item does, in two lines that are always
		// there, so the box doesn't jump as you move.
		about := ""
		if p.choice < len(p.items) {
			about = p.items[p.choice].about
		}
		if p.problem != "" && p.problem == about {
			// You picked an item that is off: say so, not just describe it.
			about = "✗ " + about
		}
		lines := wrapLines(about, aboutW, 3)
		for len(lines) < rows {
			lines = append(lines, "")
		}
		b.WriteString("\n")
		for _, l := range lines {
			style := styleValue
			if p.problem != "" && strings.HasPrefix(about, "✗ ") { // you picked an item that is off: its reason, in red
				style = styleErr
			}
			b.WriteString("\n" + style.Render(padRight(l, aboutW))) // same width whichever item is highlighted
		}
		if p.problem != "" && !strings.HasPrefix(about, "✗ ") { // an off item's reason is its description: not twice
			for _, l := range wrapLines(p.problem, aboutW, 2) {
				b.WriteString("\n" + styleErr.Render(l))
			}
		}
	case promptChoose:
		for i, o := range p.options {
			line := "  " + o
			if i == p.choice {
				line = styleCursor.Render("▶ " + o)
			}
			b.WriteString("\n" + line)
		}
	}
	style := stylePrompt
	if p.kind == promptMenu {
		// One width for the whole menu, so it doesn't jump as you move.
		w := 30
		for _, l := range strings.Split(b.String(), "\n") {
			w = max(w, lipgloss.Width(l))
		}
		return styleMenu.Width(w + 2).Render(b.String())
	}
	return style.Width(min(width-4, 100)).Render(b.String())
}

// menuWidth is how wide the description under a menu wraps: wider on
// wide screens, so it needs fewer lines.
func menuWidth(screen int) int {
	switch {
	case screen >= 120:
		return 70
	case screen >= 96:
		return 58
	}
	return 46
}

// wrapLines word-wraps s to width, at most n lines (the last one cut with …).
func wrapLines(s string, width, n int) []string {
	var lines []string
	cur := ""
	var words []string
	for _, w := range strings.Fields(s) {
		// A word longer than a line (a path, an ARN) is broken into pieces.
		for width > 0 && len([]rune(w)) > width {
			r := []rune(w)
			words = append(words, string(r[:width]))
			w = string(r[width:])
		}
		words = append(words, w)
	}
	for _, w := range words {
		switch {
		case cur == "":
			cur = w
		case len(cur)+1+len(w) <= width:
			cur += " " + w
		default:
			lines = append(lines, cur)
			cur = w
		}
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	if len(lines) > n {
		lines = lines[:n]
		lines[n-1] = truncate(lines[n-1]+" …", width)
	}
	return lines
}
