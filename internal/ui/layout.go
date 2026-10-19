package ui

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// Screen, top to bottom:
//
//	header      info panel (6 rows) or one line on small terminals
//	input box   3 rows, only while typing ":" or "/"
//	body box    table + "eks" section, or the pager
//	footer      breadcrumbs + messages (1 row)
const (
	maxFooterLines = 3
	inputBoxLines  = 3
	boxBorderLines = 2
	maxDetailLines = 8
	flashFor       = 6 * time.Second
	errorFlashFor  = 20 * time.Second // a failure stays long enough to be read after a slow wait
	// failureKeep: a command that failed (a change that didn't go through)
	// keeps its message this long even as you press keys: after a slow
	// wait you may be typing when it comes.
	failureKeep = 10 * time.Second
)

func (m *Model) inputActive() bool {
	return m.mode == modeCommand || m.mode == modeFilter || m.mode == modeSearch || m.mode == modeFind
}

func (m *Model) inputBoxHeight() int {
	if !m.inputActive() {
		return 0
	}
	return inputBoxLines + len(m.sugs)
}

// bodyHeight is the height of the body box including its borders.
func (m *Model) bodyHeight() int {
	h := m.height - m.headerHeight() - m.footerHeight() - m.inputBoxHeight()
	return max(h, boxBorderLines+1)
}

func (m *Model) pagerWidth() int  { return max(m.width-4, 1) }
func (m *Model) pagerHeight() int { return max(m.bodyHeight()-boxBorderLines, 1) }

// View renders the screen.
func (m *Model) View() string {
	if m.width == 0 {
		return ""
	}
	if m.mode == modePrompt && m.prompt != nil {
		// The prompt floats over the screen it was opened from, so you
		// still see the row it is about.
		p := m.prompt
		m.mode = m.modeBehindPrompt()
		screen := m.View()
		m.mode = modePrompt
		// The footer is the prompt's own keys, not the screen's behind it.
		if i := strings.LastIndex(screen, "\n"); i >= 0 {
			screen = screen[:i+1] + m.renderFooter()
		}
		from, to := m.overlayRows()
		box := p.render(m.width)
		if m.isProd() { // production's red reaches the boxes on top too
			box = strings.ReplaceAll(box, ansiColor(colTitle), ansiColor(colBad))
			box = strings.ReplaceAll(box, ansiColor(colWarn), ansiColor(colBad))
		}
		p.boxTop, p.boxLeft = overlayPlace(box, m.width, m.height, from, to)
		p.boxWidth = lipgloss.Width(box)
		return overlay(screen, box, m.width, m.height, from, to)
	}
	parts := []string{m.renderHeader()}
	if m.inputActive() {
		lines := []string{m.input.View()}
		for i, s := range m.sugs {
			name := s.name
			if s.show != "" {
				name = s.show // a shorter way to show it (cluster names)
			}
			name = padRight(name, 36) + "  "
			line := "  " + name + styleMuted.Render(s.detail)
			if i == m.sugSel {
				line = styleCursor.Render(padRight("▶ "+name+s.detail, innerWidth(m.width)))
			}
			lines = append(lines, line)
		}
		parts = append(parts, m.box(m.width, m.inputBoxHeight(), panel{lines: lines}))
	}
	parts = append(parts, m.renderBody(), m.renderFooter())
	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

// overlay draws box centred on top of screen.
// The box is centred between rows from and to, so it stays inside the
// list's frame instead of covering the header.
func overlay(screen, box string, width, height, from, to int) string {
	bg := strings.Split(screen, "\n")
	for len(bg) < height {
		bg = append(bg, "")
	}
	fg := strings.Split(box, "\n")
	top, left := overlayPlace(box, width, height, from, to)
	for i, line := range fg {
		row := top + i
		if row >= len(bg) {
			break
		}
		under := bg[row]
		bg[row] = ansi.Truncate(padRight(under, left), left, "") + line +
			ansi.TruncateLeft(under, left+lipgloss.Width(line), "")
	}
	return strings.Join(bg[:height], "\n")
}

// overlayPlace is the screen row and column where overlay puts box's
// top left corner.
func overlayPlace(box string, width, height, from, to int) (top, left int) {
	lines := strings.Count(box, "\n") + 1
	if to-from < lines { // too tall for the frame: use the whole screen
		from, to = 0, height
	}
	return from + max((to-from-lines)/2, 0), max((width-lipgloss.Width(box))/2, 0)
}

// overlayRows are the rows a prompt may cover: inside the list's frame,
// one row in from its border.
func (m *Model) overlayRows() (from, to int) {
	return m.headerHeight() + 1, m.height - m.footerHeight() - 1
}

// menuRoom is how many menu items fit inside the frame: the rest of the
// box is its title, the description and borders.
func (m *Model) menuRoom() int {
	from, to := m.overlayRows()
	return max(to-from-8, 5)
}

// emptyLogLines says why a logs view is empty instead of showing nothing.
func (m *Model) emptyLogLines() []string {
	further := "Press h to look further back (the last hour, 6 hours, a day)"
	switch m.pager.source.since { // only the windows still ahead
	case time.Hour:
		further = "Press h to look further back (6 hours, a day)"
	case 6 * time.Hour:
		further = "Press h to look further back (a day)"
	}
	if since := m.pager.source.since; since > 0 && nextSince(since) == 0 {
		// Already a day back: say so, and that a pod's logs start with it.
		further = "That's as far back as kubed reads (a day); a pod's logs only go back to when it started"
	}
	switch {
	case m.pager.errorsOnly && len(m.pager.lines) > 0 && m.pager.readingBack():
		// Still reading: "none" now could be wrong in a second.
		return []string{styleMuted.Render(fmt.Sprintf("No error lines in the %d lines read so far; still reading…", len(m.pager.logLines(m.pager.lines))))}
	case m.pager.errorsOnly && len(m.pager.lines) > 0 && m.pager.ended && m.pager.source.since == 0 && len(m.pager.lines) < 500:
		// A run that ended, read whole: its log is all here.
		return []string{styleOK.Render(fmt.Sprintf("No error lines in this log (%d lines, all of it).", len(m.pager.logLines(m.pager.lines)))),
			styleMuted.Render("e shows every line again.")}
	case m.pager.errorsOnly && len(m.pager.lines) > 0:
		return []string{styleOK.Render(fmt.Sprintf("No error lines in the last %d lines.", len(m.pager.logLines(m.pager.lines)))),
			styleMuted.Render(further + ", or e to see every line again.")}
	case !m.pager.gotLines && !m.pager.ended && time.Since(m.pager.openedAt) < 3*time.Second:
		// A time window with nothing in it sends nothing at all, and a
		// followed stream never "ends": after a few quiet seconds it's
		// empty (pods that print nothing, finished ones among them).
		return []string{styleMuted.Render("Reading the logs…")}
	case m.pager.source.since > 0:
		return []string{styleMuted.Render("Nothing logged in the last " + view.Age(time.Now().Add(-m.pager.source.since)) + "."),
			styleMuted.Render(further + ".")}
	case m.pager.follow && m.pager.source.workload == "" && m.restarted(m.pager.source):
		return []string{styleMuted.Render("Nothing logged yet. New lines show up here as soon as they are written."),
			styleMuted.Render("p shows the logs from before its last restart.")}
	case m.pager.follow:
		return []string{styleMuted.Render("Nothing logged yet. New lines show up here as soon as they are written.")}
	}
	return []string{styleMuted.Render("These logs are empty.")}
}

// emptyColour is a colour switched on and straight off again: no text.
var emptyColour = regexp.MustCompile(`\x1b\[[0-9;]*m\x1b\[0?m`)

// suggestWords is the Suggest line of :waste: what to reserve, or "keep"
// where there is nothing to give back.
// usedWords is the most a workload used. One killed for running out of
// memory used more than any reading caught: it died between them.
func usedWords(w enrich.Waste, since time.Time) string {
	if !w.RanOutOfMemory.IsZero() {
		return fmt.Sprintf("Used     at most %s CPU in the %s kubed has watched; memory: all it had (it was killed for running out, between readings)", view.Cores(w.PeakCPU), view.Age(since))
	}
	return fmt.Sprintf("Used     at most %s and %s in the %s kubed has watched", view.Cores(w.PeakCPU), view.Mem(w.PeakMem), view.Age(since))
}

func suggestWords(w enrich.Waste) string {
	if !w.RanOutOfMemory.IsZero() { // the 30% is about CPU here: memory is kept
		return "Suggest  " + view.WasteAdvice(w) + " (CPU: the most it used, plus 30%)"
	}
	return "Suggest  " + view.WasteAdvice(w) + " (the most it used, plus 30%)"
}

func (m *Model) renderBody() string {
	h := m.bodyHeight()
	m.tbl.onScreen = 0 // set again when the list is drawn
	switch {
	case m.mode == modeShell && m.term != nil:
		title := styleBoxTitle.Render(m.term.title)
		return m.box(m.width, h, panel{title: title, lines: m.term.lines()})
	case (m.mode == modePager || m.mode == modeSearch) && m.pager != nil:
		title := styleBoxTitle.Render(m.pager.title)
		if m.pager.short != "" && lipgloss.Width(m.pager.title) > m.width-50 { // leave room for the notes after it
			title = styleBoxTitle.Render(m.pager.short)
		}
		switch {
		case m.pager.isLogs && m.pager.notStarted:
			title += " " + styleMuted.Render("· not started")
		case m.pager.isLogs && m.pager.ended && !m.pager.source.previous:
			title += " " + styleMuted.Render("· ended")
		case m.pager.isLogs && m.pager.follow && !m.pager.source.previous:
			title += " " + styleOK.Render("· live")
		}
		if m.pager.isLogs && m.pager.errorsOnly {
			title += styleMuted.Render(" · ") + styleErr.Render(fmt.Sprintf("errors only: %d of %d lines", len(m.pager.logLines(m.pager.visible())), len(m.pager.logLines(m.pager.lines))))
		}
		if m.pager.isLogs && m.pager.dropped {
			title += styleWarn.Render(fmt.Sprintf(" · newest %d lines only", maxLogLines))
		}
		if m.pager.isLogs && m.pager.source.timestamps {
			title += styleMuted.Render(" · timestamps")
		}
		if s := m.pager.searchStatus(); s != "" {
			title += styleMuted.Render(" · ") + styleFilter.Render(s)
		}
		// The viewport leaves an empty colour pair for every coloured word
		// it cut off; drop them so wide log lines stay short.
		lines := strings.Split(emptyColour.ReplaceAllString(m.pager.vp.View(), ""), "\n")
		if m.pager.isLogs && len(m.pager.visible()) == 0 {
			lines = m.emptyLogLines()
		}
		foot := ""
		if !m.pager.vp.AtBottom() && !m.pager.follow {
			foot = styleMuted.Render(fmt.Sprintf("↓ more below · line %d of %d", min(m.pager.vp.YOffset+m.pager.vp.Height, m.pager.vp.TotalLineCount()), m.pager.vp.TotalLineCount()))
		}
		return m.box(m.width, h, panel{title: title, lines: lines, foot: foot})
	case m.connecting && m.loc.kind != locContexts:
		return m.box(m.width, h, panel{lines: m.connectingLines()})
	}
	if p, bad := m.problem(); bad && m.loc.kind != locContexts {
		return m.box(m.width, h, panel{lines: problemLines(p, innerWidth(m.width))})
	}

	title := m.boxTitle()
	if m.loc.kind == locHelm && len(m.helm) == 0 {
		msg := styleMuted.Render("loading helm releases…")
		if m.helmErr != nil {
			msg = styleErr.Render("can't read the Helm releases: " + friendlyError(m.helmErr.Error()))
		} else if !m.helmLoading {
			where := "namespace " + m.namespace
			if m.namespace == "" {
				where = "any namespace"
			}
			msg = styleMuted.Render("No Helm releases in " + where + ".")
		}
		return m.box(m.width, h, panel{title: title, lines: []string{msg}})
	}
	if m.loc.kind == locHome {
		if pending := m.homeLoading(); pending != "" {
			return m.box(m.width, h, panel{title: title, lines: []string{styleMuted.Render(pending)}})
		}
	}
	if m.loc.kind == locResource {
		if err := m.client.Cache.Err(m.loc.res.GVR); err != nil {
			return m.box(m.width, h, panel{title: title, lines: []string{styleErr.Render(err.Error())}})
		}
		if !m.client.Cache.Synced(m.loc.res.GVR) {
			return m.box(m.width, h, panel{title: title, lines: []string{styleMuted.Render("loading " + m.loc.res.GVR.Resource + "…")}})
		}
	}

	var top string

	detail := m.detailLines()
	// Keep at least a few rows of the list; say when the box has more.
	if room := h - boxBorderLines - 1 - minTableRows; len(detail) > room && room > 1 {
		detail = fitDetail(detail, room)
	}
	tableHeight := h - boxBorderLines
	if len(detail) > 0 {
		tableHeight -= len(detail) + 1 // + divider
	}
	tableHeight = max(tableHeight, 2)
	panels := []panel{{title: title, lines: strings.Split(m.tbl.render(innerWidth(m.width), tableHeight), "\n")}}
	if len(m.tbl.rows) == 0 {
		if msg := m.emptyMessage(); msg != "" {
			panels[0].lines = append(panels[0].lines[:1], "", styleMuted.Render(msg))
		}
	}
	if len(detail) > 0 {
		// The box under the table is always about the selected row.
		section := "about this row"
		if row, ok := m.tbl.selected(); ok && len(row.Cells) > 0 {
			section = "about " + strings.TrimPrefix(row.Cells[0], markPrefix)
		}
		panels = append(panels, panel{title: styleBoxTitle.Render(section), lines: detail})
	}
	if top != "" {
		return lipgloss.JoinVertical(lipgloss.Left, top, m.box(m.width, h, panels...))
	}
	return m.box(m.width, h, panels...)
}

// topExtra is how many rows sit above the table box (home's attention block).
func (m *Model) topExtra() int { return 0 }

// boxTitle reads like k9s: resource(namespace)[count], plus the drill-down
// and filter when set.
func (m *Model) boxTitle() string {
	var name, scope string
	switch m.loc.kind {
	case locContexts:
		name = "clusters"
		if m.contextNote != "" {
			name += " · " + m.contextNote
		}
	case locForwards:
		name = "ports you opened"
	case locResources:
		name = "resources"
	case locContainers:
		name = "containers"
		scope = m.loc.podName
	case locHome:
		name = "apps"
		if m.namespace != "" {
			scope = m.namespace
		}
	case locFind:
		name = "find"
		if m.mode != modeFind && m.loc.query != "" {
			name = "found for \"" + m.loc.query + "\"" // the search, not a filter typed on it
		}
	case locWaste:
		name = "unused capacity"
		scope = nsLabel(m.namespace)
	case locHelm:
		name = "Helm releases"
		scope = nsLabel(m.namespace)
	default:
		name = m.loc.res.GVR.Resource
		if friendly, ok := listTitles[name]; ok {
			name = friendly
		}
		if m.loc.res.Kind == "Application" && m.loc.res.GVR.Group == "argoproj.io" {
			name = "ArgoCD apps"
		}
		if m.loc.res.Namespaced {
			scope = m.namespace
			if scope == "" {
				scope = "all"
			}
		}
	}
	count := fmt.Sprint(len(m.tbl.rows))
	if m.tbl.filter != "" {
		count = fmt.Sprintf("%d of %d", len(m.tbl.rows), m.tbl.total)
	}
	t := styleBoxTitle.Render(name) // the trail goes in front at the end, shortened to fit
	switch d := m.loc.drill; {
	case d != nil: // "pods of app auth", "pods on node ip-10-0-1-2"
		t += " " + styleTitleNS.Render(d.Title)
	case scope == "all":
		t += " " + styleMuted.Render("in all namespaces")
	case scope != "":
		t += " " + styleMuted.Render("in") + " " + styleTitleNS.Render(scope)
	}
	switch {
	case m.loc.kind == locFind && strings.TrimSpace(m.loc.query) == "":
		// Nothing searched yet: no count, it would look like results.
	case m.loc.kind == locFind && m.foundAll > len(m.tbl.rows):
		t += " " + styleMuted.Render(fmt.Sprintf("(first %d of %d: keep typing)", len(m.tbl.rows), m.foundAll))
	case len(m.tbl.rows) == 0 && m.listLoading():
		t += " " + styleMuted.Render("(loading)")
	default:
		t += " " + styleMuted.Render("(") + styleValue.Render(count) + styleMuted.Render(")")
	}
	if m.loc.kind == locWaste { // apps that use more than they reserve: a risk, counted first
		short := 0
		for _, w := range m.wastes {
			if w.ReqMem > 0 && w.PeakMem > w.ReqMem || !w.RanOutOfMemory.IsZero() {
				short++
			}
		}
		if short > 0 {
			t += " " + styleWarn.Render(fmt.Sprintf("· %d short of memory", short))
		}
	}
	switch {
	case m.loc.kind == locWaste && m.wasteEarly():
		t += " " + styleWarn.Render("· only "+m.wasteSeen()+" watched, too short to act on")
	case (m.loc.kind == locHome || m.loc.kind == locResource && m.loc.res.Namespaced) && m.nsTrouble(m.namespace) != "":
		t += " " + styleErr.Render("· namespace "+m.nsTrouble(m.namespace))
	case m.loc.kind == locHome && m.tbl.filter == "":
		t += m.homeSummary()
	case m.loc.kind == locResource && m.tbl.filter == "":
		t += m.listSummary()
	}
	if words := sortWords(m.tbl.sort, m.tbl.desc); words != "" { // the column with the arrow may be off screen
		t += " " + styleMuted.Render("· "+words)
	}
	if m.tbl.filter != "" {
		t += " " + styleFilter.Render("· filter: "+m.tbl.filter)
	}
	if n := len(m.tbl.marked()); n > 0 {
		t += " " + styleTitleNS.Render(fmt.Sprintf("· %d picked", n))
	}
	// The trail gives way first: counts, problems, sort and filter matter
	// more than how you got here.
	return m.trailWithin(m.width-8-lipgloss.Width(t)) + t
}

// minTableRows is how much of the list stays visible above the details box.
const minTableRows = 6

// emptyMessage says why a list is empty and what to do.
func (m *Model) emptyMessage() string {
	if m.loc.kind == locResource && m.loc.drill == nil && m.tbl.filter == "" && m.client != nil &&
		len(m.client.Cache.List(m.loc.res.GVR, m.namespace)) > 0 && time.Since(m.tbl.openedAt) < 2*time.Second {
		// Loaded a moment ago; the rows follow on the next refresh. Later,
		// an empty list is empty (kubed hides some, like kube-root-ca.crt).
		return "loading…"
	}
	switch {
	case m.loc.drill != nil && strings.HasPrefix(m.loc.drill.Title, "of app ") && m.loc.res.GVR.Resource != "pods":
		return "This app uses no " + m.loc.res.GVR.Resource + "."
	case m.loc.drill != nil && strings.HasPrefix(m.loc.drill.Title, "of app "):
		for _, a := range m.apps { // say why, as home's box does
			if a.Name == m.loc.app {
				if why := view.AppWhy(a, view.Lookups{Secret: m.secretError, Argo: m.argoLookup, ReadOnly: m.opts.ReadOnly}); why != "" {
					return "This app has no pods running right now: " + why + "."
				}
				if a.Status.Tone == view.ToneDim {
					return "This app has no pods running right now: " + a.Status.Why + "."
				}
			}
		}
		return "This app has no pods running right now."
	case m.loc.drill != nil:
		if name, ok := strings.CutPrefix(m.loc.drill.Title, "started by cron job "); ok {
			if gvr, served := m.client.Resources.Served(kube.CronJobs); served && m.enr != nil {
				for _, cj := range m.client.Cache.List(gvr, m.namespace) {
					if done, _, _ := unstructured.NestedString(cj.Object, "status", "lastSuccessfulTime"); cj.GetName() == name &&
						done == "" && m.enr.CronLastRun(cj).IsZero() {
						if sched, _, _ := unstructured.NestedString(cj.Object, "spec", "schedule"); strings.HasPrefix(view.CronWords(sched), "never") {
							return "It hasn't run yet, and its schedule never comes round (" + view.CronWords(sched) + "). t runs it now."
						}
						return "It hasn't run yet. The cron job's box says when the next run is; t runs it now."
					}
				}
			}
			return "No runs kept: Kubernetes cleans up finished runs after a while. The cron job's box says when it last worked."
		}
		return "Nothing running " + m.loc.drill.Title + " right now."
	case m.tbl.filter != "":
		return "Nothing matches \"" + m.tbl.filter + "\". esc clears the filter."
	case m.loc.kind == locForwards && m.writesLocked():
		return "No ports open. kubed is read-only (started with --readonly), so F can't open any."
	case m.loc.kind == locForwards:
		return "No ports open. Select an app, pod, service or deployment and press F."
	case m.loc.kind == locFind && m.loc.query == "":
		return ""
	case m.loc.kind == locFind:
		return "Nothing found by that name."
	case m.namespace != "" && (m.loc.kind == locHome || m.loc.kind == locResource && m.loc.res.Namespaced):
		switch m.nsTrouble(m.namespace) {
		case "no longer exists":
			return "Namespace " + m.namespace + " no longer exists. :ns picks another."
		case "being deleted":
			return "Nothing here in " + m.namespace + ": it is being deleted. :ns picks another."
		}
		return "Nothing here in " + m.namespace + ". :ns all shows every namespace."
	}
	return "Nothing here."
}

// sortWords says a list's order in words: "newest first", "most cost first".
func sortWords(mode sortMode, desc bool) string {
	pick := func(first, other string) string {
		if desc {
			return other
		}
		return first
	}
	switch mode {
	case sortName:
		return pick("by name, A to Z", "by name, Z to A")
	case sortAge:
		return pick("newest first", "oldest first")
	case sortCost:
		return pick("costliest first", "cheapest first")
	case sortRestarts:
		return pick("most restarts first", "fewest restarts first")
	case sortCPU:
		return pick("most CPU first", "least CPU first")
	case sortMem:
		return pick("most memory first", "least memory first")
	}
	return ""
}

// listSummary counts a list's problems for its title (nothing when none).
func (m *Model) listSummary() string {
	bad, warn := 0, 0
	for _, r := range m.tbl.rows {
		switch r.Tone {
		case view.ToneBad:
			bad++
		case view.ToneWarn:
			warn++
		}
	}
	sep := styleMuted.Render(" · ")
	out := ""
	failing := "failing"
	if onResource(m) && m.loc.res.GVR.Resource == kube.ExternalSecrets.Resource {
		failing = "not syncing" // the Watch line's words
	}
	if bad > 0 {
		out += sep + styleErr.Render(fmt.Sprintf("%d %s", bad, failing))
	}
	if warn > 0 {
		out += sep + styleWarn.Render(fmt.Sprintf("%d to check", warn))
	}
	if out != "" && m.tbl.sort == sortDefault {
		out += sep + styleMuted.Render("problems first")
	}
	return out
}

// homeSummary is how the apps are doing, for the home title.
func (m *Model) homeSummary() string {
	bad, warn, idle := 0, 0, 0
	for _, a := range m.apps {
		switch a.Status.Tone {
		case view.ToneBad:
			bad++
		case view.ToneWarn:
			warn++
		case view.ToneDim:
			idle++
		}
	}
	sep := styleMuted.Render(" · ")
	stopped := ""
	if idle > 0 { // stopped and scheduled apps: listed last
		stopped = sep + styleMuted.Render(fmt.Sprintf("%d idle", idle))
	}
	switch {
	case len(m.apps) == 0:
		return ""
	case bad == 0 && warn == 0:
		return sep + styleOK.Render("all healthy") + stopped
	}
	var out string
	if bad > 0 {
		out += sep + styleErr.Render(fmt.Sprintf("%d failing", bad))
	}
	if warn > 0 {
		out += sep + styleWarn.Render(fmt.Sprintf("%d to check", warn))
	}
	if m.tbl.sort != sortDefault {
		return out + stopped // you sorted: problems aren't on top
	}
	return out + sep + styleMuted.Render("problems first") + stopped
}

func (m *Model) detailLines() []string {
	if m.enr == nil {
		return nil
	}
	switch m.loc.kind {
	case locHome:
		var lines []string
		if row, ok := m.tbl.selected(); ok {
			if a, ok := m.appFor(row); ok {
				lines = append(lines, view.AppLines(a, view.Lookups{Secret: m.secretError, Argo: m.argoLookup, Crash: m.crashLookup, ReadOnly: m.opts.ReadOnly, Web: m.appWebNoted})...)
			}
		}
		if hidden := m.homeBlindSpots(); hidden != "" {
			// Can't see everything, so don't let an empty list read as "all is well".
			lines = append([]string{"Hidden   ? " + hidden}, lines...)
		}
		return m.styleDetail(lines)
	case locWaste:
		return m.wasteLines()
	case locForwards:
		return m.forwardLines()
	case locContainers:
		if pod, ok := m.containersPod(); ok {
			if row, ok := m.tbl.selected(); ok {
				if lines := view.ContainerDetail(m.enr, pod, row.Obj.GetName()); lines != nil {
					return m.styleDetail(lines)
				}
			}
			return m.styleDetail(view.For(kube.Resource{GVR: kube.Pods}).Detail(m.enr, pod))
		}
		return nil
	case locResource:
	default:
		return nil
	}
	row, ok := m.tbl.selected()
	if !ok {
		return nil
	}
	v := view.For(m.loc.res)
	if v.Detail == nil {
		return nil
	}
	return m.styleDetail(v.Detail(m.enr, row.Obj))
}

// styleDetail caps the detail lines and colours their leading label.
func (m *Model) styleDetail(lines []string) []string {
	// Each detail is one or more rows; only whole details are shown, so a
	// sentence is never cut in the middle.
	var groups [][]string
	var tails []string // each detail's last words, as many as fit a row after "…"
	fix := -1          // the group that says what to do
	for _, l := range lines {
		if l == "" { // a detail with nothing to say (not managed by ArgoCD)
			continue
		}
		var out []string
		label, rest, found := strings.Cut(l, " ")
		if !found {
			groups = append(groups, []string{l})
			tails = append(tails, "")
			continue
		}
		if label == "Fix" && fix < 0 {
			fix = len(groups)
		}
		// Long sentences wrap under their text instead of being cut off at
		// the screen edge.
		width := max(innerWidth(m.width)-detailIndent, 20)
		rows := wrapLines(strings.TrimLeft(rest, " "), width, 4)
		tails = append(tails, lastWords(rest, width-1))
		if len(rows) == 0 {
			rows = []string{""}
		}
		out = append(out, styleInfoLabel.Render(padRight(label, detailIndent-1))+" "+rows[0])
		for _, r := range rows[1:] {
			out = append(out, strings.Repeat(" ", detailIndent)+r)
		}
		groups = append(groups, out)
	}
	limit := m.detailCap()
	if keep, ok := keepFix(groups, tails, fix, limit); ok {
		return keep
	}
	var out []string
	for i, g := range groups {
		room := limit
		if i < len(groups)-1 {
			room-- // keep a row for "… more"
		}
		if len(out)+len(g) > room {
			if len(out) == 0 { // the first detail alone is too long: show what fits
				out = g[:max(room, 1)]
			}
			return append(out, styleMuted.Render("… more: d shows everything"))
		}
		out = append(out, g...)
	}
	return out
}

// keepFix: when the details don't all fit and the one saying what to do
// (Fix) would be cut, the details above it keep their first row only ("…")
// so it stays. ok is false when everything fits, there is no Fix, or even
// that doesn't fit.
func keepFix(groups [][]string, tails []string, fix, limit int) ([]string, bool) {
	total := 0
	for _, g := range groups {
		total += len(g)
	}
	if fix < 0 || total <= limit {
		return nil, false
	}
	upTo := 0 // rows through the Fix group, as they are
	for _, g := range groups[:fix+1] {
		upTo += len(g)
	}
	if upTo <= limit-1 { // it fits as it is: the usual cut keeps it
		return nil, false
	}
	var out []string
	for _, g := range groups[:fix] {
		row := g[0]
		if len(g) > 1 {
			row += "…"
		}
		out = append(out, row)
	}
	out = append(out, groups[fix]...)
	if len(out) > limit-1 {
		return nil, false
	}
	// Room left: the problem keeps its last row too, where the cause
	// often is ("…: that image or tag doesn't exist").
	for i, g := range groups[:fix] {
		if !strings.HasPrefix(ansi.Strip(g[0]), "Problem") || len(g) < 2 || len(out) >= limit-1 {
			continue
		}
		// The text's own end: its last row may be one cut short ("… ").
		last := strings.Repeat(" ", detailIndent) + "…" + tails[i]
		if len(g) == 2 { // two rows: both, as they are
			out[i], last = g[0], g[1]
		}
		out = append(out[:i+1], append([]string{last}, out[i+1:]...)...)
		break
	}
	return append(out, styleMuted.Render("… more: d shows everything")), true
}

// detailCap is how many rows the box under a list may take: fewer on
// short terminals, so the list keeps most of the screen.
func (m *Model) detailCap() int {
	if m.height < 30 {
		return 5 // enough for a problem and its fix
	}
	return maxDetailLines
}

// lastWords is the end of s, whole words, at most width runes.
func lastWords(s string, width int) string {
	words := strings.Fields(s)
	out := ""
	for i := len(words) - 1; i >= 0; i-- {
		next := words[i]
		if out != "" {
			next += " " + out
		}
		if len([]rune(next)) > width {
			break
		}
		out = next
	}
	if out == "" && len(words) > 0 { // one word longer than a row: its end
		r := []rune(words[len(words)-1])
		out = string(r[max(len(r)-width, 0):])
	}
	return out
}

// detailIndent is where detail text starts ("Why      …").
const detailIndent = 9

// --- footer: breadcrumbs + messages ---

func (m *Model) renderFooter() string {
	return strings.Join(m.footerLines(), "\n")
}

// footerLines is the bottom of the screen: one line of keys, or a message.
// A message or the g list that doesn't fit wraps onto a few lines rather
// than being cut off.
func (m *Model) footerLines() []string {
	width := max(m.width-2, 10) // the line starts with a space
	one := func(s string) []string { return []string{truncate(" "+s, m.width)} }
	switch {
	case m.mode == modePrompt && m.prompt != nil:
		return one(fitHints(m.hints(), width)) // the prompt's keys, not an older message
	case m.problemShown():
		return one(fitHints(m.hints(), width)) // the screen already says what went wrong
	case m.gKey:
		return gLines(width)
	case m.busy != "":
		return one(styleWarn.Render("… " + m.busy))
	case m.inputActive() && !m.flashErr:
		return one(fitHints(m.hints(), width)) // what the waiting prompt takes, over news from elsewhere
	case m.flashShown():
		mark, style := "✓ ", styleOK
		switch {
		case m.flashErr:
			mark, style = "✗ ", styleErr
		case m.flashTip:
			mark, style = "› ", styleValue
		}
		var out []string
		for i, l := range wrapLines(m.flash, width-2, maxFooterLines) {
			if i == 0 {
				l = mark + l
			} else {
				l = "  " + l
			}
			out = append(out, " "+style.Render(l))
		}
		return out
	}
	return one(fitHints(m.hints(), width))
}

// footerHeight is how many lines footerLines takes.
func (m *Model) footerHeight() int {
	width := max(m.width-2, 10) // as footerLines
	switch {
	case m.mode == modePrompt && m.prompt != nil, m.problemShown():
		return 1
	case m.gKey:
		return len(gLines(width))
	case m.busy != "":
		return 1
	case m.inputActive() && !m.flashErr:
		return 1
	case m.flashShown():
		return max(len(wrapLines(m.flash, width-2, maxFooterLines)), 1)
	}
	return 1
}

// gPlaces are the places g jumps to, in the order the hint lists them.
var gPlaces = [][2]string{{"g", "top"}, {"h", "apps"}, {"p", "pods"}, {"d", "deployments"}, {"s", "services"}, {"n", "nodes"}, {"e", "events"},
	{"a", "ArgoCD"}, {"x", "ExternalSecrets"}, {"N", "node pools"}, {"c", "config maps"}, {"S", "secrets"}, {"i", "ingresses"}, {"j", "jobs"}, {"C", "cron jobs"}, {"H", "Helm"}}

// gLines lists every g place, wrapping between places, never inside one.
func gLines(width int) []string {
	var lines []string
	cur, used := styleKey.Render("g…"), 2
	for _, p := range gPlaces {
		item := styleKey.Render(p[0]) + " " + styleMuted.Render(p[1])
		w := len(p[0]) + 1 + len(p[1])
		if used+3+w > width {
			lines = append(lines, " "+cur)
			cur, used = "  "+item, 2+w
			continue
		}
		cur += styleMuted.Render(" · ") + item
		used += 3 + w
	}
	return append(lines, " "+cur)
}

// fitHints joins hints into one line no wider than width, dropping the
// ones before the last (help) that don't fit, never cutting one in half.
func fitHints(hints []hint, width int) string {
	sep := styleMuted.Render(" · ")
	render := func(h hint) string {
		return styleKey.Render(h.key) + " " + styleMuted.Render(lowerFirst(h.label))
	}
	if len(hints) == 0 {
		return ""
	}
	// The last two (help and quit, or esc) always stay.
	keep := min(2, len(hints))
	tail := make([]string, 0, keep)
	for _, h := range hints[len(hints)-keep:] {
		tail = append(tail, render(h))
	}
	last := strings.Join(tail, sep)
	used := lipgloss.Width(last)
	var parts []string
	for _, h := range hints[:len(hints)-keep] {
		r := render(h)
		if used+lipgloss.Width(r)+lipgloss.Width(sep) > width {
			continue
		}
		parts = append(parts, r)
		used += lipgloss.Width(r) + lipgloss.Width(sep)
	}
	return strings.Join(append(parts, last), sep)
}

// lowerFirst lowercases a label's first letter, keeping names such as
// YAML or ArgoCD as they are: "Show YAML" becomes "show YAML".
func lowerFirst(s string) string {
	r := []rune(s)
	if len(r) > 1 && unicode.IsUpper(r[0]) && unicode.IsUpper(r[1]) {
		return s
	}
	if len(r) > 0 {
		r[0] = unicode.ToLower(r[0])
	}
	return string(r)
}

// trail is where you are: "apps › pods of deployment/auth".

// trailWithin is the trail in at most room columns: its last steps after
// "…" when all of it doesn't fit, or nothing.
func (m *Model) trailWithin(room int) string {
	if m.loc.kind == locContexts || m.loc.kind == locFind {
		return "" // screens for the whole kubed, not part of where you were
	}
	var names []string
	for _, loc := range m.history {
		if loc.fromFind {
			continue // opened from a search result: esc goes back past it, to the results
		}
		n := locName(loc)
		if loc.kind == locFind { // esc comes back through the results: a step, named by what you searched
			q := []rune(loc.query)
			if len(q) > 16 {
				q = append(q[:15], '…')
			}
			n = fmt.Sprintf("found %q", string(q))
		}
		// The same place twice in a row is one step.
		if len(names) == 0 || names[len(names)-1] != n {
			names = append(names, n)
		}
	}
	if len(names) > 0 && names[len(names)-1] == locName(m.loc) { // the title names it already
		names = names[:len(names)-1]
	}
	if len(names) == 0 {
		return ""
	}
	for i := 0; i < len(names); i++ { // drop the oldest steps until it fits
		text := strings.Join(names[i:], " › ") + " › "
		if i > 0 {
			text = "… › " + text
		}
		if lipgloss.Width(text) <= room {
			return styleMuted.Render(text)
		}
	}
	return ""
}

func locName(loc location) string {
	switch {
	case loc.kind == locContexts:
		return "clusters"
	case loc.kind == locForwards:
		return "ports you opened"
	case loc.kind == locResources:
		return "resources"
	case loc.kind == locContainers:
		return "containers of " + loc.podName
	case loc.kind == locHome:
		return "apps"
	case loc.kind == locFind:
		return "find"
	case loc.kind == locWaste:
		return "unused capacity" // as its title says
	case loc.kind == locHelm:
		return "Helm releases"
	case loc.drill != nil:
		return loc.res.GVR.Resource + " " + loc.drill.Title // "pods of app auth"
	case loc.res.GVR.Resource == "":
		return "kubed"
	}
	if loc.res.Kind == "Application" && loc.res.GVR.Group == "argoproj.io" {
		return "ArgoCD apps" // as its list calls it
	}
	if t, ok := listTitles[loc.res.GVR.Resource]; ok {
		return t
	}
	return loc.res.GVR.Resource
}

// --- boxes ---

// panel is one section of a box; sections after the first are separated by
// a divider carrying their title.
type panel struct {
	title string
	lines []string
	foot  string // on the bottom border, for the last panel ("↓ more")
}

func innerWidth(width int) int { return max(width-4, 1) }

// drawBox draws panels in a single rounded frame of width x height. The
// first panel takes whatever height the others leave.
// box draws a frame in the context's colour: red on production.
func (m *Model) box(width, height int, panels ...panel) string {
	border := styleBorder
	if m.isProd() {
		border = styleBorderProd
	}
	return drawBoxWith(border, width, height, panels...)
}

func drawBox(width, height int, panels ...panel) string {
	return drawBoxWith(styleBorder, width, height, panels...)
}

func drawBoxWith(border lipgloss.Style, width, height int, panels ...panel) string {
	inner := innerWidth(width)
	for i := range panels {
		panels[i].lines = splitRows(panels[i].lines)
	}
	fixed := boxBorderLines
	for _, p := range panels[1:] {
		fixed += len(p.lines) + 1
	}
	flex := max(height-fixed, 0)

	var out []string
	out = append(out, borderLine(border, "╭", "╮", panels[0].title, width))
	out = append(out, frameLines(border, panels[0].lines, inner, flex)...)
	for _, p := range panels[1:] {
		out = append(out, borderLine(border, "├", "┤", p.title, width))
		out = append(out, frameLines(border, p.lines, inner, len(p.lines))...)
	}
	out = append(out, borderLine(border, "╰", "╯", panels[len(panels)-1].foot, width))
	return strings.Join(out, "\n")
}

// frameLines pads or cuts lines to exactly n rows of the inner width.
// Text with embedded newlines (e.g. multi-line API errors) becomes several
// rows, so the frame never grows taller than planned.
func frameLines(border lipgloss.Style, lines []string, inner, n int) []string {
	lines = splitRows(lines)
	out := make([]string, n)
	for i := range out {
		l := ""
		if i < len(lines) {
			l = lines[i]
		}
		out[i] = border.Render("│") + " " + padRight(truncate(l, inner), inner) + " " + border.Render("│")
	}
	return out
}

// borderLine draws "╭──── title ────╮" with the title centred.
func borderLine(border lipgloss.Style, left, right, title string, width int) string {
	span := max(width-2, 0)
	if title == "" {
		return border.Render(left + strings.Repeat("─", span) + right)
	}
	title = " " + title + " "
	tw := lipgloss.Width(title)
	if tw > span-2 {
		title = truncate(title, span-2)
		tw = lipgloss.Width(title)
	}
	l := (span - tw) / 2
	r := span - tw - l
	return border.Render(left+strings.Repeat("─", l)) + title + border.Render(strings.Repeat("─", r)+right)
}

func splitRows(lines []string) []string {
	var out []string
	for _, l := range lines {
		out = append(out, strings.Split(strings.ReplaceAll(l, "\r", ""), "\n")...)
	}
	return out
}

// homeBlindSpots names what the dashboard cannot see (RBAC), so an empty
// list is not mistaken for "nothing is wrong".
func (m *Model) homeBlindSpots() string {
	var hidden []string
	for _, g := range enrich.HomeNeeds {
		gvr, ok := m.client.Resources.Served(g)
		if !ok {
			continue
		}
		if err := m.client.Cache.Err(gvr); err != nil {
			hidden = append(hidden, gvr.Resource)
		}
	}
	if len(hidden) == 0 {
		return ""
	}
	msg := "you can't list " + strings.Join(hidden, ", ")
	if m.namespace == "" {
		msg += " in all namespaces. Pick one with :ns <name>"
	}
	return msg
}

// wasteSeen is how long kubed has watched usage for :waste.
func (m *Model) wasteSeen() string {
	for _, w := range m.wastes {
		if !w.Since.IsZero() {
			return view.Age(w.Since)
		}
	}
	return "a moment"
}

// wasteEarly: under an hour of usage seen, too little to act on. Nothing
// listed: nothing to act on, so nothing to warn about.
func (m *Model) wasteEarly() bool {
	if len(m.wastes) == 0 {
		return false
	}
	for _, w := range m.wastes {
		if !w.Since.IsZero() {
			return time.Since(w.Since) < time.Hour
		}
	}
	return true
}

// forwardLines explain the selected port in full: a narrow screen cuts
// the STATUS column, and why a forward stopped is what you need to read.
func (m *Model) forwardLines() []string {
	row, ok := m.tbl.selected()
	if !ok || len(row.Cells) < 5 {
		return nil
	}
	port := " (pod port " + row.Cells[2] + ")" // through a service, the pod's port, which can differ from the service's
	if strings.HasPrefix(row.Cells[1], "pod ") {
		port = " (port " + row.Cells[2] + ")"
	}
	lines := []string{"Address  " + row.Cells[0] + " reaches " + row.Cells[1] + port}
	if row.Cells[3] != "-" {
		lines = append(lines, "Through  pod "+row.Cells[3])
	}
	return m.styleDetail(append(lines, "Status   "+row.Cells[4]))
}

// wasteLines explain the selected workload's numbers, then the total.
func (m *Model) wasteLines() []string {
	total := 0.0
	var since time.Time
	for _, w := range m.wastes {
		total += w.Monthly
		since = w.Since
	}
	switch {
	case since.IsZero() && m.client != nil && m.client.Metrics.Polled():
		// Read, and nothing here reserves much more than it uses.
		return m.styleDetail([]string{"Usage    nothing in " + nsWords(m.namespace) + " reserves much more than it uses"})
	case since.IsZero():
		return m.styleDetail([]string{"Usage    waiting for the first metrics reading…"})
	}
	early := time.Since(since) < time.Hour
	var lines []string
	if row, ok := m.tbl.selected(); ok {
		for _, w := range m.wastes {
			if w.Workload.GetName() != row.Obj.GetName() || w.Workload.GetNamespace() != row.Obj.GetNamespace() || w.Workload.GetKind() != row.Obj.GetKind() {
				continue
			}
			lines = append(lines,
				fmt.Sprintf("Reserves %s and %s per pod (%s)", view.Cores(w.ReqCPU), view.Mem(w.ReqMem), view.Plural(w.Pods, "pod")),
				usedWords(w, since),
				suggestWords(w))
			if !w.RanOutOfMemory.IsZero() {
				lines = append(lines, "Risk     it ran out of memory "+view.Age(w.RanOutOfMemory)+" ago and was killed: keep its memory, or raise its limit")
			} else if w.ReqMem > 0 && w.PeakMem > w.ReqMem {
				lines = append(lines, "Risk     it used more memory than it reserves: on a busy node it is the first to be pushed off. Reserve more, not less")
			}
			if m.enr.Costs() && w.Monthly >= 1 && (w.ReqMem == 0 || w.PeakMem <= w.ReqMem) { // a risk row saves nothing worth having
				saves := fmt.Sprintf("Saves    about %s a month", dollars(w.Monthly))
				if early {
					saves += " (rough: from a short watch)"
				}
				lines = append(lines, saves)
			}
		}
	}
	switch {
	case early:
		lines = append(lines, "Wait     only "+view.Age(since)+" watched: that misses daily peaks. Leave kubed open for a day, or check your metrics history, before changing what they reserve")
	case m.enr.Costs():
		lines = append(lines, fmt.Sprintf("In all   about %s a month if every workload here reserved what is suggested", dollars(total)))
	}
	return m.styleDetail(lines)
}

// homeLoading says what home is still waiting for ("" when ready). Apps
// are only drawn once pods and their owners are in, so nothing shows up
// half-attributed (e.g. as "No pods") while lists load.
func (m *Model) homeLoading() string {
	if m.client == nil {
		return ""
	}
	var waiting, done []string
	for _, g := range view.AppCore {
		gvr, ok := m.client.Resources.Served(g)
		if !ok {
			continue
		}
		if m.client.Cache.Synced(gvr) || m.client.Cache.Err(gvr) != nil {
			done = append(done, gvr.Resource+" ✓")
		} else {
			waiting = append(waiting, gvr.Resource+" …")
		}
	}
	if len(waiting) == 0 {
		waiting = m.ownersLoading()
	}
	if len(waiting) == 0 {
		return ""
	}
	return "loading apps: " + strings.Join(append(done, waiting...), "  ")
}

// ownersLoading are the kinds that own pods here (a Job, a StatefulSet, a
// DaemonSet) whose lists haven't loaded: until they have, those pods'
// apps can't be told right (a failed Job's pod read "Running"). Only for
// a few seconds after connecting: a big list of old jobs doesn't hold
// home back longer. The pods are looked through at most twice a second.
func (m *Model) ownersLoading() []string {
	if m.enr == nil || time.Since(m.connectedAt) > ownersWait {
		return nil
	}
	if time.Since(m.ownersAt) < 500*time.Millisecond {
		return m.owners
	}
	owners := map[string]schema.GroupVersionResource{"batch/Job": kube.Jobs, "apps/StatefulSet": kube.StatefulSets, "apps/DaemonSet": kube.DaemonSets}
	pending := map[string]schema.GroupVersionResource{}
	for kind, g := range owners {
		if gvr, ok := m.client.Resources.Served(g); ok && !m.client.Cache.Synced(gvr) && m.client.Cache.Err(gvr) == nil {
			pending[kind] = gvr
		}
	}
	var out []string
	for _, p := range m.enr.List(kube.Pods, "") {
		if len(pending) == 0 {
			break
		}
		for _, ref := range p.GetOwnerReferences() {
			group, _, _ := strings.Cut(ref.APIVersion, "/")
			key := group + "/" + ref.Kind
			if gvr, ok := pending[key]; ok && ref.Controller != nil && *ref.Controller {
				out = append(out, gvr.Resource+" …")
				delete(pending, key)
			}
		}
	}
	slices.Sort(out)
	m.owners, m.ownersAt = out, time.Now()
	return out
}

// ownersWait is the longest home waits for the lists of what owns pods.
const ownersWait = 5 * time.Second

// listTitles are list titles in words where the kind name runs together.
var listTitles = map[string]string{
	"nodepools": "node pools", "nodeclaims": "node claims", "externalsecrets": "ExternalSecrets",
	"cronjobs": "cron jobs", "configmaps": "config maps", "statefulsets": "statefulsets", "daemonsets": "daemonsets",
	"persistentvolumeclaims": "volume claims", "horizontalpodautoscalers": "autoscalers", "httproutes": "web addresses (gateway routes)",
	"ingresses": "web addresses (ingresses)", "serviceaccounts": "service accounts",
}

// ansiColor is the escape sequence lipgloss uses for a colour as a
// foreground, so a rendered box's border can be recoloured.
func ansiColor(c lipgloss.AdaptiveColor) string {
	out := lipgloss.NewStyle().Foreground(c).Render("x")
	if i := strings.Index(out, "x"); i > 0 {
		return out[:i]
	}
	return "\x1b[none"
}

// flashShown: the last message is still on screen. Errors stay longer:
// one that ends a long wait mustn't be gone before you look back.
func (m *Model) flashShown() bool {
	if m.flash == "" {
		return false
	}
	if m.flashErr {
		return time.Since(m.flashAt) < errorFlashFor
	}
	return time.Since(m.flashAt) < flashFor
}

// fitDetail cuts a box to room lines. A Fix line (what to do) stays: the
// entries above it first lose their wrapped tails ("…"), then the rest is
// cut with a pointer to d.
func fitDetail(detail []string, room int) []string {
	more := styleMuted.Render("… more: d shows everything")
	fix := -1
	for i, l := range detail {
		if strings.HasPrefix(ansi.Strip(l), "Fix ") {
			fix = i
			break
		}
	}
	fixEnd := fix + 1
	for fix >= 0 && fixEnd < len(detail) && strings.HasPrefix(ansi.Strip(detail[fixEnd]), " ") {
		fixEnd++
	}
	if fix < 0 || fixEnd <= room-1 { // no Fix line, or it fits whole as it is
		return append(detail[:room-1], more)
	}
	var short []string // the lines above Fix, each entry on its first line only
	for i := 0; i < fix; i++ {
		if strings.HasPrefix(ansi.Strip(detail[i]), " ") { // a wrapped tail
			if n := len(short); n > 0 && !strings.HasSuffix(short[n-1], "…") {
				short[n-1] += "…"
			}
			continue
		}
		short = append(short, detail[i])
	}
	short = append(short, detail[fix:fixEnd]...)
	if len(short) > room-1 { // still too long: as before
		return append(detail[:room-1], more)
	}
	return append(short, more)
}
