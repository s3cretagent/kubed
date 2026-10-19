package ui

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"github.com/s3cretagent/kubed/internal/view"
)

// maxLogLines bounds memory for long-running log follows.
const maxLogLines = 5000

// pager shows describe / YAML / logs / help text full screen.
type pager struct {
	title  string
	lines  []string
	vp     viewport.Model
	wrap   bool
	follow bool // logs: stick to the bottom as lines arrive

	// Set while a log stream is open.
	logs   chan string
	cancel context.CancelFunc
	isLogs bool
	source logSource // which stream, so it can be reopened

	live     func() string  // re-renders the content every tick (rollout view)
	undo     func() tea.Cmd // u: go back to the previous version (rollout view)
	versions func() tea.Cmd // v: every kept version (rollout view)

	errorsOnly bool              // logs: show only lines that look like errors
	editRow    *view.Row         // YAML: what e edits
	dropped    bool              // logs: more than maxLogLines came, the oldest were left out
	short      string            // a shorter title for narrow screens, so the notes after it still fit
	tagNames   map[string]string // logs of several pods: the short tag on each line → "pod/container"
	plain      string            // when set, what saving and copying all give (whole names)
	tree       bool              // a tree (X): wrapped lines keep its │ lines
	back       func() tea.Cmd    // esc reopens what this was opened from
	secret     bool              // secret values: no saving, no copying everything
	csv        bool              // what it saves is CSV (:shipped)
	gotLines   bool              // logs: the stream has sent something (else: still reading)
	other      []string          // logs: each line in the form not shown (with or without its time); t swaps
	notes      map[string]bool   // lines kubed wrote among the logs ("no more lines…"): not the app's
	hang       int               // where wrapped lines continue (0: under the line's own text)
	noticed    map[string]bool   // new pods of a followed workload already said
	raw        bool              // logs: show JSON lines as they are, not tidied
	ended      bool              // logs: the stream closed (the pod stopped or was replaced)
	notStarted bool              // logs: ended with no line, no container having started
	openedAt   time.Time         // logs: when the stream was opened
	plainFrom  int               // > 0: line i of the screen is line i-plainFrom of plain
	diff       bool              // a diff: wrapped rows keep their line's - or +
	lastLineAt time.Time         // logs: when lines last arrived, and how many came then
	lastBatch  int

	readDropped bool // logs: the line being read was just dropped (the app says so, once)
	saidDropped bool // logs: that was said already

	search  string // highlighted text; n / N jump between matches
	matches []int  // line numbers containing search
	match   int    // index into matches

	rowOf []int // with wrap on: the screen row each line starts on
}

func newPager(title, content string, width, height int) *pager {
	p := &pager{title: title, lines: strings.Split(strings.TrimRight(content, "\n"), "\n"), vp: viewport.New(width, height)}
	p.refresh()
	return p
}

func (p *pager) resize(width, height int) {
	p.vp.Width, p.vp.Height = width, height
	p.refresh()
}

// note adds kubed's own lines to the logs: shown, but not copied, saved
// or counted as lines the app wrote.
func (p *pager) note(lines ...string) {
	if p.notes == nil {
		p.notes = map[string]bool{}
	}
	for _, l := range lines {
		p.notes[l] = true
	}
	p.append(lines)
}

// isNote: a line kubed wrote among the logs, added as a note or sent
// down the stream ("kubed: couldn't reach the logs…", after a pod's tag).
func (p *pager) isNote(l string) bool {
	if p.notes[l] {
		return true
	}
	plain := ansi.Strip(l)
	if i := strings.Index(plain, " │ "); i >= 0 && i < 60 {
		plain = plain[i+len(" │ "):]
	}
	return strings.HasPrefix(plain, "kubed: ")
}

// onlyNotes: every line of the logs is one kubed wrote (none at all too).
func (p *pager) onlyNotes() bool {
	for _, l := range p.lines {
		if !p.isNote(l) {
			return false
		}
	}
	return true
}

// logLines leaves kubed's notes out of lines.
func (p *pager) logLines(lines []string) []string {
	if !p.isLogs && len(p.notes) == 0 {
		return lines
	}
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if !p.isNote(l) {
			out = append(out, l)
		}
	}
	return out
}

func (p *pager) append(lines []string) {
	tidy := func(l string) string {
		if j := strings.LastIndex(l, "\r"); j >= 0 && j < len(l)-1 {
			// A progress line rewrites itself: keep what a terminal shows,
			// after the pod tag and timestamp kubed put in front.
			l = linePrefix.FindString(l) + l[j+1:]
		}
		// A tab is one character but several columns: cutting at the edge
		// counted it as one and the line spilled onto a second row.
		return expandTabs(l)
	}
	// Logs come with Kubernetes' time on each line; both forms are kept,
	// so t only switches which is shown (and searched, copied, saved).
	// The message is tidied on its own (tabs from its start, as without
	// a time), so both forms line up the same.
	other := make([]string, len(lines))
	for i, l := range lines {
		bare, timed := tidy(l), tidy(l)
		if p.isLogs {
			prefix, ts, rest := splitStamp(l, len(p.tagNames) > 0)
			msg := tidyMessage(rest)
			bare, timed = prefix+msg, prefix+msg
			if ts != "" {
				timed = prefix + ts + " " + msg
			}
		}
		if p.isLogs && !p.source.timestamps {
			lines[i], other[i] = bare, timed
		} else {
			lines[i], other[i] = timed, bare
		}
	}
	p.gotLines = true
	p.lastLineAt, p.lastBatch = time.Now(), len(lines)
	p.lines = append(p.lines, lines...)
	if p.isLogs {
		p.other = append(p.other, other...)
	}
	top := -1 // paused: the line on top, so dropping old lines doesn't scroll it away
	if !p.follow && len(p.lines) > maxLogLines {
		if shown, i := p.shown(), p.lineAt(p.vp.YOffset); i < len(shown) {
			top = shown[i]
		}
	}
	over := 0
	if over = len(p.lines) - maxLogLines; over > 0 {
		p.dropped = true // older lines left out: the title says so
		p.lines = p.lines[over:]
		if len(p.other) >= over {
			p.other = p.other[over:]
		}
	}
	if p.search != "" {
		p.rematch(max(over, 0))
	}
	p.refresh()
	if top >= 0 && over > 0 {
		if top -= over; top < 0 && !p.saidDropped {
			p.readDropped, p.saidDropped = true, true
		}
		shown := p.shown()
		p.vp.SetYOffset(p.row(sort.SearchInts(shown, max(top, 0))))
	}
}

// linePrefix is what kubed puts before a log line: a pod tag ("web-1 │ ")
// and a timestamp, when shown.
var linePrefix = regexp.MustCompile(`^(?:[^\r│]{1,40} │ )?(?:\d{4}-\d\d-\d\dT[\d:.]+Z )?`)

// expandTabs turns tabs into spaces up to the next multiple of 8 columns,
// as a terminal shows them.
func expandTabs(l string) string {
	if !strings.Contains(l, "\t") {
		return l
	}
	var b strings.Builder
	col := 0
	for _, r := range l {
		if r == '\t' {
			n := 8 - col%8
			b.WriteString(strings.Repeat(" ", n))
			col += n
			continue
		}
		b.WriteRune(r)
		col++
	}
	return b.String()
}

// readingBack: older lines are still arriving (h reads back in big
// batches before the live ones trickle in), so "none found" isn't final.
func (p *pager) readingBack() bool {
	if !p.ended && !p.follow {
		return true
	}
	return p.source.since > 0 && p.lastBatch >= 20 && time.Since(p.lastLineAt) < 2*time.Second
}

// visible are the lines on screen: all of them, or only errors.
func (p *pager) visible() []string {
	if !p.errorsOnly {
		return p.lines
	}
	idx := p.shown()
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = p.lines[j]
	}
	return out
}

// shown are the indexes in lines of what is on screen (with errors only
// on, just the errors and their stack traces).
func (p *pager) shown() []int {
	if !p.errorsOnly {
		out := make([]int, len(p.lines))
		for i := range out {
			out[i] = i
		}
		return out
	}
	var out []int
	// Merged logs interleave pods: a stack trace goes on in its own pod's
	// lines, whatever other pods print in between.
	inError := map[string]bool{}
	for i, l := range p.lines {
		plain := ansi.Strip(l)
		tag := ""
		if bar := strings.Index(plain, "│"); len(p.tagNames) > 0 && bar >= 0 && bar < 200 && logTagEnd(plain) > 0 {
			// "web·x │ " is the pod, not the message. Only the one space
			// after the bar goes: a stack frame's indent is how it's known.
			tag, plain = plain[:bar], strings.TrimPrefix(plain[bar+len("│"):], " ")
		}
		if ts, rest, ok := strings.Cut(plain, " "); ok { // Kubernetes' time first: a frame's indent comes after it
			if _, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				plain = rest
			}
		}
		plain = loggerName.ReplaceAllString(plain, "") // "gunicorn.error" names a logger, not an error
		if strings.HasPrefix(plain, "kubed:") {
			inError[tag] = false
			out = append(out, i) // kubed's own notes (a lost connection) always show
			continue
		}
		switch {
		case errorLine.MatchString(plain) && (!infoLine.MatchString(plain) || serverError.MatchString(plain)):
			inError[tag] = true
		case inError[tag] && isContinuation(plain):
			// The indented lines of a stack trace belong to its error.
		default:
			inError[tag] = false
			continue
		}
		out = append(out, i)
	}
	return out
}

// keepPlace applies a change to how lines show (wrap, tidy, errors only)
// and keeps the line at the top of the screen at the top, or the nearest
// one after it, instead of jumping.
func (p *pager) keepPlace(change func()) {
	if p.follow {
		change()
		p.refresh()
		return
	}
	before := p.shown()
	top := -1
	if i := p.lineAt(p.vp.YOffset); i < len(before) {
		top = before[i]
	}
	change()
	p.refresh()
	if top < 0 {
		return
	}
	after := p.shown()
	i := sort.SearchInts(after, top)
	if i >= len(after) {
		p.vp.GotoBottom()
		return
	}
	p.vp.SetYOffset(p.row(i))
}

// isContinuation: a line that carries on the one before (stack frames,
// "Caused by:", wrapped text), not a new log entry.
func isContinuation(s string) bool {
	return strings.HasPrefix(s, " ") || strings.HasPrefix(s, "\t") || strings.HasPrefix(s, "Caused by") ||
		strings.HasPrefix(s, "During handling") || strings.HasPrefix(s, "The above exception")
}

// errorLine matches log lines that look like errors in common formats:
// plain words, Python tracebacks, JSON / logfmt levels, and HTTP 5xx
// where it is the status (right after the request line, or a status
// field), not a response size that happens to be 500.
var loggerName = regexp.MustCompile(`\b[\w-]+\.(error|exception)\b`)

// infoLine: a line that says outright it is not an error (level=info,
// "level":"debug", INFO at the start), whatever words it carries
// ("Trigger 'on-sync-failed' FAILED" from a notifier deciding not to send).
var infoLine = regexp.MustCompile(`(?i:\blevel=(info|debug|trace)\b|"(level|levelname|severity|lvl)"\s*:\s*"(info|debug|trace|warn|warning|notice)")|^.{0,80}?[\s\[](INFO|DEBUG|TRACE)[\]\s:]|^\[?(INFO|DEBUG|TRACE)\b`)

// serverError: an HTTP 5xx answer, an error whatever level the line says.
var serverError = regexp.MustCompile(`HTTP/[\d.]+"\s+5\d\d\b|\bstatus["=:\s]+5\d\d\b`)

var errorLine = regexp.MustCompile(`(?i)(error|exception)\b|\b(fatal|panic|traceback|critical|failed|failure)\b|"(level|levelname|severity|lvl)"\s*:\s*"(error|fatal|critical|alert|emergency)"|\blevel=(error|fatal|critical)\b|HTTP/[\d.]+"\s+5\d\d\b|\bstatus["=:\s]+5\d\d\b`)

func (p *pager) refresh() {
	lines := p.visible()
	vis := lines // as stored: what tells kubed's notes apart
	if p.isLogs && !p.raw {
		tidy := make([]string, len(lines))
		for i, l := range lines {
			tag := bracketTagEnd(l)  // one pod's "│" is its own; a "[name] " label is read past
			if len(p.tagNames) > 0 { // several pods: kubed's tag first, then maybe the app's own label
				tag = logTagEnd(l)
				tag += bracketTagEnd(l[tag:])
			}
			tidy[i] = prettyLine(l, tag)
		}
		lines = tidy
	}
	if p.search != "" {
		marked := highlight(lines, p.search)
		for i, l := range vis {
			if i < len(marked) && p.isNote(l) {
				marked[i] = lines[i] // kubed's own notes aren't searched: nothing marked in them
			}
		}
		lines = marked
		// Without colour the matches look like any text: mark the one n/N
		// is on at the start of its line.
		if noColor && p.match < len(p.matches) && p.matches[p.match] < len(lines) {
			lines = append([]string(nil), lines...)
			lines[p.matches[p.match]] = "▶ " + lines[p.matches[p.match]]
		}
	}
	p.rowOf = nil
	if p.wrap {
		// Wrap line by line, remembering where each starts on screen, so
		// a search can scroll to a line that wraps over several rows.
		width := max(p.vp.Width, 1)
		wrapped := make([]string, len(lines))
		p.rowOf = make([]int, len(lines))
		row := 0
		for i, l := range lines {
			if p.tree {
				wrapped[i] = treeWrap(l, width)
			} else {
				wrapped[i] = carryStyle(l, hangWrapAt(l, width, p.hang))
				if p.diff {
					wrapped[i] = carryMark(l, wrapped[i])
				}
			}
			p.rowOf[i] = row
			row += strings.Count(wrapped[i], "\n") + 1
		}
		lines = wrapped
	}
	p.vp.SetContent(strings.Join(lines, "\n"))
	if p.follow {
		p.vp.GotoBottom()
	}
}

func (p *pager) close() {
	if p.cancel != nil {
		p.cancel()
	}
}

// update handles scrolling keys.
func (p *pager) update(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "g", "home":
		p.follow = false
		p.vp.GotoTop()
	case "G", "end":
		p.vp.GotoBottom()
		if p.isLogs && !p.ended { // at the end of live logs: keep following
			p.follow = true
		}
	case "w":
		p.keepPlace(func() { p.wrap = !p.wrap })
	case "f":
		if p.isLogs {
			p.follow = !p.follow
			if p.follow {
				p.vp.GotoBottom()
			}
		}
	default:
		var cmd tea.Cmd
		p.vp, cmd = p.vp.Update(msg)
		if !p.vp.AtBottom() {
			p.follow = false
		}
		return cmd
	}
	return nil
}

// logLinesMsg carries lines read from a log stream.
type logLinesMsg struct {
	ch    chan string
	lines []string
	done  bool
}

// waitLogs blocks for the next line, then drains whatever else is ready so
// a burst of logs costs one redraw.
func waitLogs(ch chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-ch
		if !ok {
			return logLinesMsg{ch: ch, done: true}
		}
		lines := []string{line}
		for len(lines) < 500 {
			select {
			case l, ok := <-ch:
				if !ok {
					return logLinesMsg{ch: ch, lines: lines, done: true}
				}
				lines = append(lines, l)
			default:
				return logLinesMsg{ch: ch, lines: lines}
			}
		}
		return logLinesMsg{ch: ch, lines: lines}
	}
}

// rematch finds the search's matches again after lines arrive (and old
// ones are dropped): saved positions would point at the wrong lines. The
// current match stays the same line where it still exists.
func (p *pager) rematch(dropped int) {
	cur := -1
	if p.match < len(p.matches) {
		cur = p.matches[p.match] - dropped
	}
	needle := strings.ToLower(p.search)
	p.matches = p.matches[:0]
	for i, l := range p.visible() {
		if !p.isNote(l) && lineMatches(l, needle) { // kubed's own notes aren't the app's lines
			p.matches = append(p.matches, i)
		}
	}
	p.match = 0
	for i, line := range p.matches {
		if line >= cur {
			p.match = i
			break
		}
	}
	if p.follow && len(p.matches) > 0 {
		p.match = len(p.matches) - 1 // following the end: the newest match is the one on screen
	}
}

// lineMatches: a line holds what you searched for. "warnings" (or
// "problems") also finds the lines flagged as warnings, as / does on lists.
func lineMatches(l, needle string) bool {
	if needle == "warnings" || needle == "problems" {
		if mark := warnMark(); mark != "" && strings.HasPrefix(l, mark) || strings.HasSuffix(ansi.Strip(l), " !") {
			return true
		}
	}
	return strings.Contains(strings.ToLower(ansi.Strip(l)), needle)
}

// warnMark is how a line drawn in the warning colour starts ("" without
// colour, when such lines end in " !" instead). Asked each time: colour
// is decided once the terminal is known.
func warnMark() string {
	mark, _, _ := strings.Cut(styleWarn.Render("x"), "x")
	return mark
}

// setSearch highlights term and jumps to its first match at or below the
// current position.
func (p *pager) setSearch(term string) {
	p.search = term
	p.matches = nil
	if term != "" {
		needle := strings.ToLower(term)
		for i, l := range p.visible() {
			if !p.isNote(l) && lineMatches(l, needle) {
				p.matches = append(p.matches, i)
			}
		}
	}
	// A match past the right edge can't be seen: wrap long lines.
	if !p.wrap {
		vis := p.visible() // once: it filters every line when errors only is on
		for _, i := range p.matches {
			if lipgloss.Width(vis[i]) > p.vp.Width {
				p.wrap = true
				break
			}
		}
	}
	p.match = 0
	for i, line := range p.matches {
		if line >= p.lineAt(p.vp.YOffset) {
			p.match = i
			break
		}
	}
	p.follow = false
	p.refresh()
	p.showMatch()
}

// nextMatch moves to the next (+1) or previous (-1) match, wrapping.
func (p *pager) nextMatch(dir int) {
	if len(p.matches) == 0 {
		return
	}
	p.match = (p.match + dir + len(p.matches)) % len(p.matches)
	p.follow = false
	p.showMatch()
}

func (p *pager) showMatch() {
	if noColor && len(p.matches) > 0 {
		p.refresh() // the "▶" moves to the match n/N is on now
	}
	if len(p.matches) > 0 {
		p.vp.SetYOffset(max(p.row(p.matches[p.match])-2, 0))
	}
}

// row is the screen row line i starts on.
func (p *pager) row(i int) int {
	if p.rowOf != nil && i < len(p.rowOf) {
		return p.rowOf[i]
	}
	return i
}

// lineAt is the line shown on screen row r.
func (p *pager) lineAt(r int) int {
	if p.rowOf == nil {
		return r
	}
	i := sort.SearchInts(p.rowOf, r+1) - 1
	return max(i, 0)
}

// searchStatus is "3 of 12" for the title, or "".
func (p *pager) searchStatus() string {
	switch {
	case p.search == "":
		return ""
	case len(p.matches) == 0:
		return "no match for “" + p.search + "”" // as typed: %q would show \" for a quote
	}
	return fmt.Sprintf("%q %d of %d", p.search, p.match+1, len(p.matches))
}

// highlight marks every case-insensitive occurrence of term. A coloured
// line with a match loses its colours, so the match shows.
func highlight(lines []string, term string) []string {
	out := make([]string, len(lines))
	needle := strings.ToLower(term)
	for i, l := range lines {
		out[i] = l
		if strings.Contains(l, "\x1b") {
			// A coloured line (tidied logs) that matches is shown plain,
			// so the match itself can stand out.
			if plain := ansi.Strip(l); strings.Contains(strings.ToLower(plain), needle) {
				l = plain
			} else {
				continue
			}
		}
		lower := strings.ToLower(l)
		var b strings.Builder
		for {
			j := strings.Index(lower, needle)
			if j < 0 {
				b.WriteString(l)
				break
			}
			b.WriteString(l[:j])
			b.WriteString(styleMatch.Render(l[j : j+len(needle)]))
			l, lower = l[j+len(needle):], lower[j+len(needle):]
		}
		out[i] = b.String()
	}
	return out
}

// plainText is the pager content without colours, for saving.
func (p *pager) plainText() string {
	if p.plain != "" {
		return p.plain
	}
	var b strings.Builder
	for _, l := range p.logLines(p.visible()) { // with errors only on: just the errors
		b.WriteString(p.lineText(l)) // pod logs as "pod/container: line"
		b.WriteByte('\n')
	}
	return b.String()
}

// hangWrapAt wraps at a fixed column when one is set (the last column of
// a table), else like hangWrap.
// leadingStyle is the colour codes a line starts with.
var leadingStyle = regexp.MustCompile(`^(?:\x1b\[[0-9;]*m)+`)

// carryStyle gives each wrapped row of a line the colour the line starts
// with: a removed diff line stays red on every row, even when its first
// row has scrolled off.
func carryStyle(orig, wrapped string) string {
	style := leadingStyle.FindString(orig)
	if style == "" || !strings.Contains(wrapped, "\n") {
		return wrapped
	}
	rows := strings.Split(wrapped, "\n")
	for i := range rows {
		if i > 0 {
			rows[i] = style + rows[i]
		}
		rows[i] += "\x1b[0m"
	}
	return strings.Join(rows, "\n")
}

// carryMark repeats a diff line's - or + at the start of its wrapped rows,
// so without colour a continued line still says which side it is.
func carryMark(orig, wrapped string) string {
	plain := ansi.Strip(orig)
	if plain == "" || (plain[0] != '-' && plain[0] != '+') || !strings.Contains(wrapped, "\n") {
		return wrapped
	}
	style := leadingStyle.FindString(orig)
	rows := strings.Split(wrapped, "\n")
	for i := 1; i < len(rows); i++ {
		rest := strings.TrimPrefix(rows[i], style)
		if strings.HasPrefix(rest, " ") {
			rows[i] = style + string(plain[0]) + rest[1:]
		}
	}
	return strings.Join(rows, "\n")
}

func hangWrapAt(l string, width, at int) string {
	if at <= 0 || ansi.StringWidth(l) <= width {
		return hangWrap(l, width)
	}
	at = min(at, width-30) // too far right to read under: as far right as reads well
	head := ansi.Truncate(l, at, "")
	tail := ansi.TruncateLeft(l, at, "")
	parts := strings.Split(wrapWords(tail, width-at), "\n")
	pad := strings.Repeat(" ", at)
	for i := 1; i < len(parts); i++ {
		parts[i] = pad + strings.TrimLeft(parts[i], " ")
	}
	return head + strings.Join(parts, "\n")
}

// hangWrap wraps a line to width, starting the extra lines under the
// line's text instead of at the left edge: under a help entry's words, a
// log line's message after its pod tag, an indented line's indent.
func hangWrap(l string, width int) string {
	if ansi.StringWidth(l) <= width {
		return l
	}
	indent := hangIndent(ansi.Strip(l))
	if indent == 0 || indent > width/2 {
		return wrapWords(l, width)
	}
	parts := strings.Split(wrapWords(l, width-indent), "\n")
	pad := strings.Repeat(" ", indent)
	for i := 1; i < len(parts); i++ {
		parts[i] = pad + strings.TrimLeft(parts[i], " ")
	}
	return strings.Join(parts, "\n")
}

// hangIndent is the column a line's text starts at: after its leading
// spaces, then after a short label followed by two or more spaces
// ("  :waste   what…"), or after a log line's "pod │ " tag.
func hangIndent(plain string) int {
	if i := strings.Index(plain, "│ "); i >= 0 && i < 40 {
		return ansi.StringWidth(plain[:i]) + 2
	}
	n := len(plain) - len(strings.TrimLeft(plain, " "))
	rest := plain[n:]
	if j := strings.Index(rest, "  "); j > 0 && j <= 20 {
		k := j
		for k < len(rest) && rest[k] == ' ' {
			k++
		}
		if k < len(rest) {
			return n + ansi.StringWidth(rest[:k])
		}
	}
	return n
}

// lineText is one line as it would be shared: without colours, and for
// merged pod logs "pod: message" instead of kubed's "pod │ message".
func (p *pager) lineText(l string) string {
	plain := ansi.Strip(l)
	if p.isLogs && len(p.tagNames) > 0 { // tagged by kubed: several pods
		if i := strings.Index(plain, " │ "); i >= 0 && i < 60 {
			tag := strings.TrimSpace(plain[:i])
			if full, ok := p.tagNames[tag]; ok { // the whole name: something you can grep for
				tag = full
			}
			return tag + ": " + plain[i+len(" │ "):]
		}
	}
	return strings.TrimRight(plain, " ")
}

// plainLine is the plain (whole-name, real-time) form of screen line i,
// when the screen has one; ok is false otherwise.
func (p *pager) plainLine(i int) (string, bool) {
	if p.plainFrom <= 0 || p.errorsOnly || p.plain == "" {
		return "", false
	}
	lines := strings.Split(strings.TrimRight(p.plain, "\n"), "\n")
	if j := i - p.plainFrom; j >= 0 && j < len(lines) {
		return lines[j], true
	}
	return "", false
}

// noEarlier: logs from before a restart that kubed found aren't there.
func (p *pager) noEarlier() bool {
	return len(p.lines) > 0 && strings.HasPrefix(ansi.Strip(p.lines[0]), "kubed: ")
}

// copyText is every line, ready to paste elsewhere.
func (p *pager) copyText() string {
	if p.plain != "" {
		return p.plain
	}
	var b strings.Builder
	for _, l := range p.logLines(p.visible()) { // with errors only on: just the errors
		b.WriteString(p.lineText(l) + "\n")
	}
	return b.String()
}

// wrapWords wraps at spaces, never inside hyphenated names
// ("system-critical-spot-arm64", "dest-server="): the wrapping library
// breaks at every hyphen, so they are swapped for a look-alike while it
// works. Words longer than a line are still cut.
func wrapWords(s string, width int) string {
	const keep = "\u2011" // non-breaking hyphen
	return strings.ReplaceAll(ansi.Wrap(strings.ReplaceAll(s, "-", keep), width, ""), keep, "-")
}

// treeWrap wraps one line of a tree ("│  ├─ ✓ Secret db-creds  note…") so
// that continuation lines carry the tree's │ lines and start under the
// text, not at the left edge.
func treeWrap(l string, width int) string {
	if ansi.StringWidth(l) <= width {
		return l
	}
	plain := []rune(ansi.Strip(l))
	n := 0 // runes of tree drawing and mark before the text
	for n < len(plain) && strings.ContainsRune("│├└─ ✓⚠✗", plain[n]) {
		n++
	}
	if n == 0 || n > width/2 {
		return wrapWords(l, width)
	}
	cont := make([]rune, n) // what continues the tree on the next lines
	for i, r := range plain[:n] {
		switch r {
		case '│':
			cont[i] = '│'
		case '├':
			cont[i] = '│'
		default:
			cont[i] = ' '
		}
	}
	head := ansi.Truncate(l, n, "")
	text := ansi.TruncateLeft(l, n, "")
	parts := strings.Split(wrapWords(text, width-n), "\n")
	for i := 1; i < len(parts); i++ {
		parts[i] = styleMuted.Render(string(cont)) + strings.TrimLeft(parts[i], " ")
	}
	return head + strings.Join(parts, "\n")
}

// splitStamp parts a log line into kubed's pod tag (when tagged), the time
// Kubernetes puts in front ("" when none, as in kubed's own notes) and
// the message.
func splitStamp(l string, tagged bool) (prefix, ts, rest string) {
	body := l
	if tagged {
		end := logTagEnd(l) // the tag kubed put first, before anything the app wrote
		prefix, body = l[:end], l[end:]
	}
	t, after, ok := strings.Cut(body, " ")
	if !ok {
		return prefix, "", body
	}
	if _, err := time.Parse(time.RFC3339Nano, t); err != nil {
		return prefix, "", body
	}
	return prefix, t, after
}

// tidyMessage is what a terminal shows of a log message: after its last
// carriage return (a progress line rewriting itself), tabs to the next
// multiple of 8 columns from the message's start.
func tidyMessage(msg string) string {
	if j := strings.LastIndex(msg, "\r"); j >= 0 && j < len(msg)-1 {
		msg = msg[j+1:]
	}
	return expandTabs(msg)
}

// toggleTimes shows or hides Kubernetes' time on each log line, keeping
// the line you are reading where it is.
func (p *pager) toggleTimes() {
	p.keepPlace(func() {
		if len(p.other) == len(p.lines) {
			p.lines, p.other = p.other, p.lines
		}
		p.source.timestamps = !p.source.timestamps
		if p.search != "" {
			p.rematch(0)
		}
	})
}
