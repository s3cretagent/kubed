package ui

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// The header is the colour KUBED logo with the cluster's facts beside it.
// On small terminals it shrinks to one line.
const (
	headerFullMinHeight = 30
	headerFullMinWidth  = 100
)

func (m *Model) fullHeader() bool {
	return m.height >= headerFullMinHeight && m.width >= headerFullMinWidth
}

func (m *Model) headerHeight() int {
	if m.fullHeader() {
		return logoHeight
	}
	return 1
}

func (m *Model) renderHeader() string {
	if !m.fullHeader() {
		return m.compactHeader()
	}
	logo := renderLogo(m.isProd())
	room := m.width - lipgloss.Width(logo) - 3
	lines := m.headerFacts()
	for i, l := range lines {
		lines[i] = ansi.Truncate(l, room, "…")
	}
	facts := strings.Join(lines, "\n")
	pad := max(m.width-lipgloss.Width(facts)-lipgloss.Width(logo)-1, 2)
	return lipgloss.JoinHorizontal(lipgloss.Top, facts, strings.Repeat(" ", pad), logo)
}

// headerFacts are the six lines beside the logo: where you are and how
// the cluster is doing. No key lists or shortcuts: those live in the
// footer and the Enter menu.
func (m *Model) headerFacts() []string {
	row := func(label, value string) string {
		return styleInfoLabel.Render(fmt.Sprintf(" %-9s", label)) + value
	}
	room := m.width - lipgloss.Width(renderLogo(false)) - 3 - 10 // beside the logo, after the label

	// The badges (PRODUCTION, READ-ONLY) are never cut: the name gives way.
	badges := strings.Join(m.badges(), "  ")
	name := shortContext(m.context)
	if b := lipgloss.Width(badges); b > 0 {
		name = truncateMiddle(name, max(room-b-2, 12))
	}
	cluster := styleValue.Render(name)
	if badges != "" {
		cluster += "  " + badges
	}

	ns := styleValue.Render(nsWords(m.namespace)) + styleMuted.Render("  (:ns another namespace · :ctx another cluster)")
	if m.namespace != m.chosenNS {
		ns = styleValue.Render(nsWords(m.namespace)) + styleMuted.Render("  (this screen only; esc: back to "+nsWords(m.chosenNS)+")")
	}
	if trouble := m.nsTrouble(m.namespace); trouble != "" {
		ns = styleValue.Render(m.namespace) + "  " + styleErr.Render(trouble) + styleMuted.Render("  (:ns another namespace)")
	}
	dash := styleMuted.Render("-")
	usage, nodes, where := dash, dash, dash
	cost := styleMuted.Render("? every key  ·  q quit")
	costLabel := "Help"
	if m.client != nil && m.enr != nil {
		s := m.enr.ClusterSummary()
		if s.UsageKnown {
			usage = styleMuted.Render("CPU ") + pctStyle(s.CPUPct) + styleMuted.Render(" · memory ") + pctStyle(s.MemPct) +
				styleMuted.Render(" of what the nodes have")
		}
		if s.Nodes > 0 {
			nodes = fmt.Sprint(s.Nodes) // the :waste hint follows the region below
		}
		version, _, _ := strings.Cut(m.client.Version, "-")
		region := s.Region
		if region == "" && !m.client.Cache.Synced(kube.Nodes) {
			region = "…" // still loading, not unknown
		}
		where = view.OrDash(region) + styleMuted.Render(" · Kubernetes ") + view.OrDash(version)
		if m.costsComing() { // the row it will fill, from the start: nothing jumps when prices arrive
			costLabel, cost = "Cost", styleMuted.Render("working it out…")
		}
		if m.enr.Costs() && s.Nodes > 0 {
			costLabel = "Cost"
			cost = styleMuted.Render("Nodes ") + styleValue.Render(dollars(s.Hourly*hoursPerMonth)+"/mo")
			if monthly, _, ok := m.allIn(); ok {
				cost += styleMuted.Render(" · all-in ") + styleValue.Render(dollars(monthly)+"/mo")
				if t := m.rates.cluster.TaxRate; t > 0 {
					cost += styleMuted.Render(" · ") + styleValue.Render(dollars(monthly*(1+t))) + styleMuted.Render(" with tax")
				}
			} else if m.rates.viaAWS {
				// Say what's missing, so this isn't read as the cluster's
				// whole cost next to another cluster's all-in figure.
				if !m.rates.clusterDone && m.rates.resolveErr == nil {
					cost += styleMuted.Render(" · all-in …")
				} else {
					cost += styleMuted.Render(" · nodes only, no all-in (? says why)")
				}
			}
			if s.Unpriced > 0 {
				cost += styleWarn.Render(fmt.Sprintf("  %d nodes unpriced", s.Unpriced))
			}

		}
	}
	if m.problemShown() || (m.client == nil && !m.connecting) {
		// Nothing below is known yet: say so once instead of a row of dashes.
		return []string{row("Cluster", cluster), row("Status", styleErr.Render("not connected")), "", "", "", row("Help", styleMuted.Render("? every key  ·  q quit"))}
	}
	// Region goes up beside the nodes; its row tells what else needs a look.
	return []string{
		row("Cluster", cluster),
		row("Showing", ns),
		row("In use", usage),
		row("Nodes", nodes+styleMuted.Render(" · ")+where+styleMuted.Render("  (:waste shows unused capacity)")),
		row("Watch", m.watchLine()),
		row(costLabel, cost),
	}
}

// watchItem is one thing on the Watch line and the key that opens it.
type watchItem struct{ text, key string }

// watchItems sum up what needs a look beyond the apps list: failing cron
// jobs, secrets that don't sync and nodes nearly full. loading is true
// while some of it is still unknown.
func (m *Model) watchItems() (items []watchItem, loading bool) {
	if m.client == nil || m.enr == nil {
		return nil, false
	}
	list := func(g schema.GroupVersionResource) ([]*unstructured.Unstructured, bool) {
		gvr, ok := m.client.Resources.Served(g)
		if !ok {
			return nil, true // not installed: nothing to count
		}
		if m.client.Cache.Err(gvr) != nil {
			return nil, true // can't read it: nothing kubed can count
		}
		if !m.client.Cache.Synced(gvr) {
			m.client.Cache.Watch(gvr) // started on another screen than home: load what Watch needs
			return nil, false
		}
		if g == kube.Nodes {
			return m.client.Cache.List(gvr, ""), true // nodes belong to no namespace: a picked one would hide them all
		}
		return m.client.Cache.List(gvr, m.namespace), true
	}
	if cjs, ok := list(kube.CronJobs); !ok || !jobsOK(list) || !podsListed(list) {
		loading = true
	} else {
		paused := func(o *unstructured.Unstructured) bool {
			p, _, _ := unstructured.NestedBool(o.Object, "spec", "suspend")
			return p // paused: the cron jobs list doesn't count it either
		}
		if n := countWhere(cjs, func(o *unstructured.Unstructured) bool {
			if paused(o) {
				return false
			}
			f, kept := m.enr.CronLastFinished(o)
			stuck, _ := m.enr.CronRunsStuck(o) // runs that can't start never finish
			return kept && f || stuck > 0
		}); n > 0 {
			items = append(items, watchItem{view.Plural(n, "cron job") + " failing", "gC"})
		}
		// Its last run didn't succeed and isn't kept (failed runs removed):
		// gC flags it to check, and home says it.
		if n := countWhere(cjs, func(o *unstructured.Unstructured) bool { return !paused(o) && m.enr.CronLastRunGone(o) }); n > 0 {
			items = append(items, watchItem{view.Plural(n, "cron job") + " not succeeding", "gC"})
		}
	}
	if ess, ok := list(kube.ExternalSecrets); !ok {
		loading = true
	} else if n := countWhere(ess, func(o *unstructured.Unstructured) bool { return enrich.ExternalSecret(o).Ready == "False" }); n > 0 {
		items = append(items, watchItem{view.Plural(n, "secret") + " not syncing", "gx"})
	}
	// Node room needs pods (what they reserve, how many) and a first
	// usage reading; until then a quiet line would be a false all clear.
	_, podsOK := list(kube.Pods)
	if nodes, ok := list(kube.Nodes); !ok || !podsOK || !m.client.Metrics.Polled() {
		loading = true
	} else {
		// A node whose pods really use it up is worth a look. Nodes out of
		// pod places are normal when an autoscaler packs them; that only
		// matters when no node has room left.
		full, nearly, noRoom := 0, 0, 0
		if m.nearlyFullSince == nil {
			m.nearlyFullSince = map[string]time.Time{}
		}
		for _, o := range nodes {
			// Memory pushes pods off (CPU only slows them). A node that
			// hovers around 90% would make the line blink: it counts once
			// it has stayed there 2 minutes (at once from 95%), and until
			// it drops below 87%.
			before := full + nearly
			pct := -1
			if u := m.enr.NodeUsage(o); u.Known && u.AllocMem > 0 {
				pct = enrich.Pct(u.Use.MemBytes, u.AllocMem)
			}
			since, seen := m.nearlyFullSince[o.GetName()]
			switch {
			case pct >= 100:
				full++ // the node list says "Full"
			case pct >= 90 && !seen:
				m.nearlyFullSince[o.GetName()] = time.Now()
				if pct >= 95 {
					nearly++
				}
			case pct >= 95, pct >= 87 && seen && time.Since(since) >= 2*time.Minute:
				nearly++
			case pct >= 87 && seen: // still settling
			default:
				delete(m.nearlyFullSince, o.GetName())
			}
			if full+nearly == before && strings.Contains(view.NodeBusy(m.enr, o), "pod places") {
				noRoom++ // counted once: a full node has no room either
			}
		}
		switch {
		case full > 0 && nearly > 0:
			items = append(items, watchItem{fmt.Sprintf("%s full, %d nearly", view.Plural(full, "node"), nearly), "gn"})
		case full > 0:
			items = append(items, watchItem{view.Plural(full, "node") + " full", "gn"})
		case nearly > 0:
			items = append(items, watchItem{view.Plural(nearly, "node") + " nearly full", "gn"})
		}
		if len(nodes) > 0 && noRoom > 0 && full+nearly+noRoom == len(nodes) {
			items = append(items, watchItem{"no node has room for new pods", "gn"})
		}
	}
	return items, loading
}

// jobsOK: a cron job's last run is read from its Jobs, so they must be in.
func jobsOK(list func(schema.GroupVersionResource) ([]*unstructured.Unstructured, bool)) bool {
	_, ok := list(kube.Jobs)
	return ok
}

// watchLine is the header's Watch row.
func (m *Model) watchLine() string {
	if m.client == nil || m.enr == nil {
		return styleMuted.Render("-")
	}
	items, loading := m.watchItems()
	var parts []string
	for _, it := range items {
		parts = append(parts, it.text+" ("+it.key+")")
	}
	switch {
	case len(parts) > 0 && loading:
		return styleWarn.Render(strings.Join(parts, " · ")) + styleMuted.Render(" · checking the rest…")
	case len(parts) > 0:
		return styleWarn.Render(strings.Join(parts, " · "))
	case loading:
		return styleMuted.Render("checking…")
	}
	return styleOK.Render("cron jobs, secrets and nodes look fine")
}

// watchShort is the Watch line for the one-line header: just the keys.
func (m *Model) watchShort() string {
	items, _ := m.watchItems()
	var keys []string
	for _, it := range items {
		if !slices.Contains(keys, it.key) {
			keys = append(keys, it.key)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	return styleWarn.Render("look: " + strings.Join(keys, " "))
}

func countWhere(objs []*unstructured.Unstructured, f func(*unstructured.Unstructured) bool) int {
	n := 0
	for _, o := range objs {
		if f(o) {
			n++
		}
	}
	return n
}

// dollars formats a whole-dollar amount with thousands separators: $3,167.
func dollars(v float64) string {
	n := int64(math.Round(v))
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return "$" + s
}

// compactHeader is the one-line header for small terminals.
func (m *Model) compactHeader() string {
	sep := styleMuted.Render(" · ")
	// Parts in screen order, each with a priority: when the line is too
	// narrow the least needed go first, whole, instead of cutting the line.
	type part struct {
		text string
		prio int // lower stays longer
	}
	parts := []part{{wordmark(m.isProd()), 0}}
	// What the cluster is (production, read-only) sits by its name; ports
	// you opened are a part of their own.
	var marks []string
	if badge := m.envBadge(); badge != "" {
		marks = append(marks, badge)
	}
	if m.opts.ReadOnly {
		marks = append(marks, styleRO.Render("READ-ONLY"))
	}
	badges := strings.Join(marks, " ")
	name := shortContext(m.context)
	parts = append(parts, part{styleValue.Render(name), 1})
	parts = append(parts, part{styleValue.Render(nsWords(m.namespace)), 1}) // where you are: kept; the name shortens
	if n := len(m.forwards.List()); n > 0 {
		parts = append(parts, part{styleLabel.Render(view.Plural(n, "port") + " open"), 1})
	}
	if w := m.watchShort(); w != "" {
		parts = append(parts, part{w, 2}) // what needs a look outlasts the numbers
	}
	if m.client != nil && m.enr != nil {
		s := m.enr.ClusterSummary()
		if s.Nodes > 0 && m.enr.Costs() {
			parts = append(parts, part{styleMuted.Render("nodes ") + styleValue.Render(dollars(s.Hourly*hoursPerMonth)+"/mo"), 3})
		}
		if s.UsageKnown {
			parts = append(parts, part{styleMuted.Render("CPU ") + pctStyle(s.CPUPct) + styleMuted.Render(" mem ") + pctStyle(s.MemPct), 4})
		}
		if s.Nodes > 0 {
			parts = append(parts, part{fmt.Sprintf("%d nodes", s.Nodes), 5})
		}
	}
	width := func(ps []part) int {
		w := 1
		for i, p := range ps {
			if i > 0 {
				w += 3
			}
			w += lipgloss.Width(p.text)
		}
		return w
	}
	badgeW := 0
	if badges != "" {
		badgeW = lipgloss.Width(badges) + 1
	}
	// The numbers go first; then the name shortens; "look:" goes last of all.
	for prio := 5; prio >= 3 && width(parts)+badgeW > m.width; prio-- {
		kept := parts[:0:0]
		for _, p := range parts {
			if p.prio != prio {
				kept = append(kept, p)
			}
		}
		parts = kept
	}
	if over := width(parts) + badgeW - m.width; over > 0 { // still too wide: the cluster name gives way
		name = strings.TrimPrefix(name, "cluster-") // the word every name starts with goes first
		over = width(parts) + badgeW - m.width - (len(shortContext(m.context)) - len(name))
		short := name
		if over > 0 {
			short = truncateMiddle(name, max(len(name)-over, 8))
		}
		parts[1].text = styleValue.Render(short)
		if width(parts)+badgeW > m.width { // and then the "look:" keys
			kept := parts[:0:0]
			for _, p := range parts {
				if p.prio != 2 {
					kept = append(kept, p)
				}
			}
			parts = kept
			if over := width(parts) + badgeW - m.width; over > 0 {
				parts[1].text = styleValue.Render(truncateMiddle(name, max(len(name)-over, 8)))
			}
		}
	}
	if badges != "" { // badges sit right after the name, never cut
		parts[1].text += " " + badges
	}
	texts := make([]string, len(parts))
	for i, p := range parts {
		texts[i] = p.text
	}
	return truncate(" "+strings.Join(texts, sep), m.width)
}

// wordmark is "KUBED" in the logo's colours, for the one-line header.
func wordmark(prod bool) string {
	colors := logoColors
	if prod {
		colors = prodLogoColors
	}
	var b strings.Builder
	for i, r := range "KUBED" {
		b.WriteString(lipgloss.NewStyle().Bold(true).Foreground(colors[i]).Render(string(r)))
	}
	return b.String()
}

// helpContent is the ? screen: the logo, facts about this cluster, keys.
func (m *Model) helpContent() string {
	var b strings.Builder
	help := helpTextFor(m.enr.Costs())
	if m.opts.Mouse {
		help = strings.Replace(help, "  mouse   select text to copy it as usual; :mouse lets you click rows",
			"  mouse   click a row to pick it; hold Option (or Shift) to select text; :mouse turns clicking off", 1)
	}
	b.WriteString(helpLayout(help, m.pagerWidth()))
	b.WriteString("\nTHIS CLUSTER\n")
	row := func(label, value string) {
		fmt.Fprintf(&b, "  %s %s\n", styleInfoLabel.Render(fmt.Sprintf("%-10s", label)), value)
	}
	row("Cluster", shortContext(m.context))
	if m.client != nil {
		row("Kubernetes", view.OrDash(m.client.Version))
		if m.enr != nil {
			row("Region", view.OrDash(m.enr.ClusterSummary().Region))
		}
		env := "not sure yet: every change asks you to type its name, to be safe"
		switch {
		case m.env.known && m.env.prod:
			env = "yes " + sourceWords(m.env.source) + ": every change asks you to type its name"
		case m.env.known:
			env = "no " + sourceWords(m.env.source) + ": changes ask y/n"
		}
		if m.opts.ReadOnly {
			env = strings.SplitN(env, ":", 2)[0] + "; changes are off anyway: kubed was started with --readonly"
		}
		row("Production", env)
	}
	switch {
	case m.enr.Costs():
		m.priceHelp(&b, row)
	case m.rates.account != "" && m.rates.resolveErr != nil:
		// The cluster's AWS account is known but kubed can't log in to it:
		// no prices anywhere, but say why here.
		row("Costs", "not shown: "+m.rates.resolveErr.Error())
	}
	return b.String()
}

// priceHelp is the ? screen's account of where prices come from.
func (m *Model) priceHelp(b *strings.Builder, row func(label, value string)) {
	row("Prices", m.priceLabel())
	if _, why := m.priceSource(); why != "" {
		for _, l := range strings.Split(why, "\n") {
			style := styleValue
			if strings.Contains(l, "not read") || strings.Contains(l, "can't") {
				style = styleWarn
			}
			b.WriteString(style.Render("             "+l) + "\n") // the pager wraps it under its text
		}
	}
	b.WriteString(styleMuted.Render(priceHelp) + "\n")
}

// priceHelp says where the dollar figures come from and how to make them
// match the bill.
const priceHelp = `             Spot nodes: the live price in their zone.
             On-demand nodes: what your account paid per hour last week, so Savings Plans and Reserved Instances count.
             All-in: everything AWS bills under this cluster's tags (disks, network, load balancers, logs, EKS fee).
             COST/MO of an app: its pods' share of their nodes, by what they reserve.
             The header's all-in figure also shows the total with tax.`

// badges are PROD / READ-ONLY / port-forward markers.
func (m *Model) badges() []string {
	var b []string
	if badge := m.envBadge(); badge != "" {
		b = append(b, badge)
	}
	if m.opts.ReadOnly {
		b = append(b, styleRO.Render("READ-ONLY"))
	}
	if n := len(m.forwards.List()); n > 0 {
		b = append(b, styleLabel.Render(view.Plural(n, "port")+" open"))
	}
	return b
}

func pctStyle(p int) string {
	s := fmt.Sprintf("%d%%", p)
	switch {
	case p >= 90:
		return styleErr.Render(s)
	case p >= 75:
		return styleWarn.Render(s)
	}
	return styleOK.Render(s)
}

type hint struct{ key, label string }

// hints are the keys that work on the current screen: a few to get
// around, then the actions of this view (from the action table).
func (m *Model) hints() []hint {
	if m.mode == modePrompt && m.prompt != nil {
		return promptHints(m.prompt)
	}
	switch m.mode {
	case modeShell:
		return []hint{{"ctrl-]", "Close the shell and go back"}}
	case modeFilter:
		return []hint{{"type", "To narrow the list"}, {"enter", "Keep it"}, {"esc", "Clear"}}
	case modeCommand:
		return []hint{{"type", "A command or a kind"}, {"tab", "Complete"}, {"↑↓", "History"}, {"esc", "Cancel"}}
	case modeFind:
		return []hint{{"type", "A name"}, {"↑↓", "Choose"}, {"enter", "Go there and see what you can do"}, {"esc", "Back"}}
	case modeSearch:
		return []hint{{"type", "What to look for"}, {"enter", "Find it"}, {"esc", "Cancel"}}
	case modePager:
		if m.pager == nil || !m.pager.isLogs {
			h := []hint{{"/", "Search"}}
			if m.pager != nil && m.pager.undo != nil {
				if !m.writesLocked() {
					h = append([]hint{{"u", "Go back a version"}}, h...)
				}
				h = append([]hint{{"v", "Versions"}}, h...)
			}
			if m.pager != nil && m.pager.search != "" {
				h = append(h, hint{"n/N", "Next/prev"})
			}
			wrap := "Wrap"
			if m.pager != nil && m.pager.wrap {
				wrap = "Unwrap"
			}
			switch {
			case m.pager != nil && m.pager.secret: // values: one at a time, never to a file
				h = append(h, hint{"w", wrap}, hint{"c", "Copy the value you found"})
			default:
				h = append(h, hint{"w", wrap}, hint{"g", "Jump to a list"}, hint{"c", "Copy"}, hint{"ctrl-s", "Save"})
			}
			if m.pager != nil && m.pager.editRow != nil && !m.writesLocked() {
				h = append([]hint{{"e", "Edit"}}, h...)
			}
			return append(h, hint{"esc", "Back"})
		}
		p := m.pager
		errLabel, followLabel := "Errors only", "Follow"
		if p.errorsOnly {
			errLabel = "All lines"
		}
		if p.follow {
			followLabel = "Stop following"
		}
		copyLabel := "Copy screen"
		if p.search != "" {
			copyLabel = "Copy line"
		}
		// Most used first: the footer drops from the end of what doesn't fit.
		h := []hint{{"e", errLabel}, {"/", "Search"}}
		if p.search != "" {
			h = append(h, hint{"n/N", "Next/prev"})
		}
		wrapLabel := "Wrap"
		if p.wrap {
			wrapLabel = "Unwrap"
		}
		h = append(h, hint{"w", wrapLabel}, hint{"c", copyLabel}, hint{"C", "Copy all"})
		switch {
		case p.source.previous || p.source.stoppedRun: // a run that ended: back to the live one
			h = append(h, hint{"l", "Live logs"})
		case p.source.workload == "" && m.restarted(p.source):
			h = append(h, hint{"p", "Before last restart"})
		}
		jLabel := "As written"
		if p.raw {
			jLabel = "Tidy"
		}
		switch {
		case p.ended:
			h = append(h, hint{"r", "Read again"})
		case !p.source.previous: // previous logs don't follow anything
			h = append(h, hint{"f", followLabel})
		}
		if !p.source.previous {
			back := "Last " + view.Age(time.Now().Add(-nextSince(p.source.since)))
			if nextSince(p.source.since) == 0 {
				back = "Newest only"
			}
			h = append(h, hint{"h", back})
		}
		h = append(h, hint{"j", jLabel}, hint{"t", "Timestamps"}, hint{"ctrl-s", "Save"})
		if p.search != "" {
			return append(h, hint{"esc", "Clear search"})
		}
		return append(h, hint{"esc", "Back"})
	}
	switch {
	case m.connecting && m.loc.kind != locContexts:
		return []hint{{"c", "Pick another cluster"}, {"q", "Quit"}}
	case m.loc.kind != locContexts:
		if _, bad := m.problem(); bad {
			return []hint{{"r", "Try again"}, {"c", "Pick another cluster"}, {"q", "Quit"}}
		}
	}
	switch m.loc.kind {
	case locFind:
		esc := "Back"
		if m.tbl.filter != "" { // as esc does: the filter typed on the results goes first
			esc = "Clear filter"
		}
		h := []hint{{"f", "Change the search"}, {"esc", esc}, {"?", "Help"}}
		if _, ok := m.tbl.selected(); ok { // no results: enter has nothing to open
			h = append([]hint{{"enter", "Go there and see what you can do"}}, h...)
		}
		return h
	case locContexts:
		h := []hint{{"/", "Filter"}}
		if _, ok := m.tbl.selected(); ok {
			h = append([]hint{{"enter", "Switch to it"}}, h...)
		}
		if m.prevContext != "" {
			h = append(h, hint{":ctx -", "The one before"})
		}
		if len(m.history) > 0 {
			h = append(h, hint{"esc", "Back"})
		}
		return append(h, hint{"q", "Quit"})
	}
	// Getting around first (enter, find, filter), then this screen's most
	// used actions; the footer keeps what fits, and always help and quit.
	var h []hint
	if _, ok := m.tbl.selected(); ok { // an empty list: enter has nothing to open
		h = append(h, hint{"enter", "Open / actions"})
	}
	switch { // in the order esc does them
	case m.loc.fromFind && len(m.history) > 0:
		h = append(h, hint{"esc", "Back to the results"})
	case m.tbl.filter != "":
		h = append(h, hint{"esc", "Clear filter"})
	case len(m.tbl.marks) > 0:
		h = append(h, hint{"esc", "Clear picks"})
	case len(m.history) == 0 && m.loc.kind == locHome && m.client != nil && m.namespace != m.chosenNS:
		h = append(h, hint{"esc", "Back to " + nsWords(m.chosenNS)}) // as the header says
	case len(m.history) > 0:
		h = append(h, hint{"esc", "Back"})
	case m.loc.kind != locHome && m.client != nil:
		h = append(h, hint{"esc", "Home"})
	}
	if m.client != nil {
		h = append(h, hint{"f", "Find"})
	}
	h = append(h, hint{"/", "Filter"})
	seen := map[string]bool{}
	n := 0
	// This screen's favourites first (changes included, unless read-only),
	// then the other actions that only look.
	_, anyRow := m.tbl.selected()
	// Rows picked other than the cursor's: a one-row key refuses (as the
	// menu greys it), so the footer doesn't offer it.
	othersPicked := anyRow && m.othersPicked()
	oneRowOff := func(a action) bool { return !a.bulk && othersPicked && m.usesRow(a) }
	for _, key := range m.footerFavourites() {
		if !anyRow {
			break // an empty list: nothing to act on
		}
		a, ok := m.actionFor(key)
		if !ok || seen[key] || ((a.write || a.sensitive) && m.writesLocked()) || oneRowOff(a) {
			continue
		}
		label := a.label
		if row, ok := m.tbl.selected(); ok && a.item != nil { // worded for this row, like the menu
			it := menuItem{label: a.label}
			a.item(m, row, &it)
			if it.disabled || it.hidden {
				continue
			}
			label = it.label
		}
		seen[key] = true
		h = append(h, hint{displayKey(key), shortLabel(label)})
		n++
	}
	for _, a := range actions {
		if n >= footerActions {
			break
		}
		if a.key == "" || seen[a.key] || a.write || (a.sensitive && m.writesLocked()) || !a.when(m) || oneRowOff(a) {
			continue
		}
		row, ok := m.tbl.selected()
		if !ok { // an empty list: nothing to act on
			break
		}
		label := a.label
		if a.item != nil {
			it := menuItem{label: a.label}
			a.item(m, row, &it)
			if it.disabled || it.hidden { // offers only what works on this row
				continue
			}
			label = it.label
		}
		seen[a.key] = true
		h = append(h, hint{displayKey(a.key), shortLabel(label)})
		n++
	}
	return append(h, hint{"?", "Help"}, hint{"q", "Quit"})
}

// promptHints are the keys of the prompt in front.
func promptHints(p *prompt) []hint {
	switch p.kind {
	case promptMenu:
		if p.foundMenu { // a search result's menu: esc goes back to the results, as keys.go does
			return []hint{{"↑↓", "Choose"}, {"enter", "Do it"}, {"esc", "Back to the results"}}
		}
		if p.backTo != nil { // as keys.go does
			return []hint{{"↑↓", "Choose"}, {"enter", "Do it"}, {"esc", "Back"}}
		}
		return []hint{{"↑↓", "Choose"}, {"enter", "Do it"}, {"esc", "Close"}}
	case promptChoose:
		return []hint{{"↑↓", "Choose"}, {"enter", "Pick"}, {"esc", "Cancel"}}
	case promptInput:
		return []hint{{"enter", "Apply"}, {"esc", "Cancel"}}
	case promptConfirm:
		if p.typeToConfirm != "" {
			return []hint{{"type", p.typeToConfirm}, {"enter", "Confirm"}, {"esc", "Cancel"}}
		}
		return []hint{{"y", "Yes"}, {"n", "No"}}
	}
	return nil
}

// footerActions is how many of the screen's actions the footer names.
const footerActions = 4

// footerFavourites are the keys people use most on this screen.
func (m *Model) footerFavourites() []string {
	if m.loc.kind == locHome {
		return []string{"w", "l", "X", "r"}
	}
	if m.loc.kind == locContainers {
		return []string{"l", "s", "p"}
	}
	if !onResource(m) {
		return nil
	}
	v := view.For(m.loc.res)
	switch {
	case m.loc.res.GVR.Resource == kube.Pods.Resource:
		return []string{"l", "s", "p", "r"}
	case isDeployments(m):
		return []string{"l", "r", "+", "o", "v"}
	case v.Has(view.CapScale):
		return []string{"l", "r", "+"}
	case v.Has(view.CapRestart):
		return []string{"l", "r"}
	case isSecrets(m), isConfigMaps(m):
		return []string{"x"}
	case v.Has(view.CapTrigger):
		return []string{"t"}
	}
	return nil
}

// restarted: the logs' container has restarted, so p has earlier logs.
func (m *Model) restarted(src logSource) bool {
	if m.client == nil {
		return false
	}
	pod, ok := m.client.Cache.Get(kube.Pods, src.ns, src.pod)
	if !ok { // gone from a full list: nothing to read; else can't tell, so the key stays
		return !m.client.Cache.SyncedIn(kube.Pods, src.ns)
	}
	n, found := view.ContainerRestarts(pod)[src.container]
	return !found || n > 0
}

// shortLabel drops the bracketed part: "Logs (all pods)" → "Logs".
func shortLabel(s string) string {
	if i := strings.Index(s, " ("); i > 0 {
		return s[:i]
	}
	return s
}

// nsTrouble is "being deleted" or "no longer exists" for a namespace
// that is, or "" when it is fine or kubed can't tell (it can't read the
// namespaces, or hasn't read them all yet).
func (m *Model) nsTrouble(ns string) string {
	if ns == "" || m.client == nil {
		return ""
	}
	gvr, ok := m.client.Resources.Served(kube.Namespaces)
	if !ok || !m.client.Cache.Synced(gvr) {
		return ""
	}
	o, found := m.client.Cache.Get(gvr, "", ns)
	switch {
	case !found:
		return "no longer exists"
	case o.GetDeletionTimestamp() != nil:
		return "being deleted"
	}
	return ""
}

// nsWords is a namespace in a sentence: "payments", "all namespaces".
func nsWords(ns string) string {
	if ns == "" {
		return "all namespaces"
	}
	return ns
}

func nsLabel(ns string) string {
	if ns == "" {
		return "all"
	}
	return ns
}

// shortContext turns an EKS ARN context
// ("arn:aws:eks:<region>:<account>:cluster/<name>") into "<name>".
func shortContext(ctx string) string {
	if strings.HasPrefix(ctx, "arn:aws:eks:") {
		if _, name, ok := strings.Cut(ctx, ":cluster/"); ok {
			return name
		}
	}
	return strings.TrimSuffix(ctx, "-context") // "-context" says nothing
}

func (m *Model) priceLabel() string {
	if m.priceMsg != "" {
		return styleMuted.Render(m.priceMsg)
	}
	if m.priceTbl == nil {
		return styleMuted.Render("none")
	}
	short, _ := m.priceSource()
	return short
}

// fmtPct prints a percent with at most one decimal: 27.7%, 30%.
func fmtPct(p float64) string {
	return strconv.FormatFloat(math.Round(p*10)/10, 'f', -1, 64) + "%"
}

// podsListed: the pods list is loaded (a cron job's stuck runs are read
// from its pods).
func podsListed(list func(schema.GroupVersionResource) ([]*unstructured.Unstructured, bool)) bool {
	_, ok := list(kube.Pods)
	return ok
}

// othersPicked: rows other than the cursor's are picked, so a key (or
// menu item) for one row can't tell which is meant.
func (m *Model) othersPicked() bool {
	cur, ok := m.tbl.selected()
	if !ok || len(m.tbl.marks) == 0 { // marked() builds a list: only when something is picked
		return false
	}
	picks := m.tbl.marked()
	return len(picks) > 1 || len(picks) == 1 && rowKey(picks[0]) != rowKey(cur)
}
