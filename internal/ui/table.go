package ui

import (
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/view"
)

// noColor: the terminal asked for no colour (NO_COLOR), which lipgloss
// takes to mean no styles at all.
var noColor = os.Getenv("NO_COLOR") != ""

type sortMode int

const (
	sortDefault sortMode = iota // namespace/name, as the cache returns it
	sortName
	sortAge
	sortCost     // the money column (COST/MO, SAVES/MO), most first
	sortRestarts // the RESTARTS column, most first
	sortCPU      // CPU used (pods: cores, nodes: %), most first
	sortMem      // memory used, most first
)

// sortColumn is the column a number sort uses, or -1.
func sortColumn(header []string, mode sortMode) int {
	switch mode {
	case sortCost:
		return costColumn(header)
	case sortRestarts:
		return columnNamed(header, "RESTARTS")
	case sortCPU:
		if i := columnNamed(header, "CPU %"); i >= 0 {
			return i
		}
		return columnNamed(header, "CPU")
	case sortMem:
		if i := columnNamed(header, "MEM %"); i >= 0 {
			return i
		}
		return columnNamed(header, "MEM")
	}
	return -1
}

func columnNamed(header []string, name string) int {
	for i, h := range header {
		if strings.TrimRight(h, "↑↓") == name {
			return i
		}
	}
	return -1
}

// costColumn is where a list keeps its money, or -1.
func costColumn(header []string) int {
	for i, h := range header {
		switch strings.TrimRight(h, "↑↓") {
		case "COST/MO", "SAVES/MO":
			return i
		}
	}
	return -1
}

// amountIn reads a cell's number in a comparable unit: "$1,234", "45%",
// "0.25" cores, "512Mi" and "1.2Gi" (as Mi). 0 when there is none.
func amountIn(cell string) float64 {
	c := strings.TrimSpace(cell)
	switch {
	case strings.HasSuffix(c, "Gi"):
		return dollarsIn(strings.TrimSuffix(c, "Gi")) * 1024
	case strings.HasSuffix(c, "Mi"):
		return dollarsIn(strings.TrimSuffix(c, "Mi"))
	}
	return dollarsIn(strings.TrimSuffix(c, "%"))
}

// dollarsIn reads "$1,234" or "<$1" from a cell; 0 when there is none.
func dollarsIn(cell string) float64 {
	cell = strings.TrimSpace(cell)
	under := strings.HasPrefix(cell, "<") // "<$1": less than that, so below it when sorting
	cell = strings.NewReplacer("$", "", ",", "", "<", "").Replace(cell)
	v, _ := strconv.ParseFloat(strings.Fields(cell + " x")[0], 64)
	if under {
		v /= 2
	}
	return v
}

// table is the scrollable list of rows for the current view.
type table struct {
	want     string // the row to come back to (rowID), until it is listed or you move
	back     goneRow
	header   []string
	optional []string        // hide order when narrow
	hidden   map[string]bool // columns left out altogether (money, when costs are off)
	// problemsFirst: failing rows, then ones to check, then the rest (the
	// list's own order within each), unless you chose a sort.
	problemsFirst bool
	main          string     // the column that keeps its width after the name (a message, a status)
	rows          []view.Row // after filter + sort
	unfiltered    []view.Row // before the filter: column widths come from these, so typing a filter doesn't move them
	total         int        // before filter

	cursor, offset int
	onScreen       int // how many rows were drawn last time: the ones a click can reach
	filter         string
	shown          map[string]bool      // the columns drawn last time (narrow screens drop some)
	timeOf         func(view.Row) int64 // the time each row's time column shows, when the list has one
	sort           sortMode
	desc           bool

	marks map[string]bool // rowKey → marked with space

	// touched: you moved or acted on a row. From then on the cursor
	// follows that row as the list re-sorts, even from the top.
	touched bool
	// openedAt: when this list opened. While it loads (a few seconds) a
	// cursor on the top row stays on top; after that it follows its item.
	openedAt time.Time
}

// dropColumns leaves out the hidden columns from the header and every row.
func dropColumns(header []string, rows []view.Row, hidden map[string]bool) ([]string, []view.Row) {
	var keep []int
	for i, h := range header {
		if !hidden[h] {
			keep = append(keep, i)
		}
	}
	if len(keep) == len(header) {
		return header, rows
	}
	pick := func(cells []string) []string {
		out := make([]string, 0, len(keep))
		for _, i := range keep {
			if i < len(cells) {
				out = append(out, cells[i])
			}
		}
		return out
	}
	out := make([]view.Row, len(rows))
	for i, r := range rows {
		r.Cells = pick(r.Cells)
		out[i] = r
	}
	return pick(header), out
}

func rowKey(r view.Row) string { return r.Obj.GetNamespace() + "/" + r.Obj.GetName() }

// markPrefix goes in front of a marked row's first cell.
const markPrefix = "✓ "

func (t *table) toggleMark() {
	r, ok := t.selected()
	if !ok {
		return
	}
	if t.marks == nil {
		t.marks = map[string]bool{}
	}
	k := rowKey(r)
	if t.marks[k] {
		delete(t.marks, k)
	} else {
		t.marks[k] = true
	}
}

// markAll marks every shown row, or clears marks when all are marked.
func (t *table) markAll() {
	if len(t.marked()) == len(t.rows) {
		t.clearMarks()
		return
	}
	t.marks = map[string]bool{}
	for _, r := range t.rows {
		t.marks[rowKey(r)] = true
	}
}

func (t *table) clearMarks() { t.marks = nil }

// marked returns the marked rows that are still shown.
func (t *table) marked() []view.Row {
	var out []view.Row
	for _, r := range t.rows {
		if t.marks[rowKey(r)] {
			out = append(out, r)
		}
	}
	return out
}

// setRows replaces the rows, keeping the cursor on the same object when it
// still exists.
func (t *table) setRows(header []string, rows []view.Row, timeOf func(view.Row) int64) {
	t.timeOf = timeOf // saving a list writes each row's real time
	// Keep the cursor on the same object as rows change. Before you touch
	// the list, a cursor on the first row stays on top (the list re-sorts
	// while loading, and "first" is what you are looking at).
	var selected string
	settled := !t.openedAt.IsZero() && time.Since(t.openedAt) > 3*time.Second
	if r, ok := t.selected(); ok && r.Obj != nil && (t.cursor > 0 || t.touched || settled) {
		selected = string(r.Obj.GetUID()) + r.Obj.GetKind() + r.Obj.GetNamespace() + "/" + r.Obj.GetName()
	}
	if t.want != "" { // coming back: the row you left, not whatever is at its place
		selected = t.want
	}

	header, rows = dropColumns(header, rows, t.hidden)
	// "argocd" on every row says nothing, except in find, where it says
	// where each match lives.
	if i := slices.Index(header, "WHY"); i >= 0 && !slices.ContainsFunc(rows, func(r view.Row) bool { return i < len(r.Cells) && r.Cells[i] != "" }) {
		header, rows = dropColumns(header, rows, map[string]bool{"WHY": true}) // all healthy: nothing to say
	}
	if (len(header) == 0 || header[0] != findHeader[0]) && sameEverywhere(header, rows, "NAMESPACE") {
		header, rows = dropColumns(header, rows, map[string]bool{"NAMESPACE": true})
	}
	t.header, t.total = header, len(rows)
	for i, r := range rows { // marks first: column widths then count the ✓
		if t.marks[rowKey(r)] && len(r.Cells) > 0 {
			cells := append([]string(nil), r.Cells...)
			cells[0] = markPrefix + cells[0]
			rows[i].Cells = cells
		}
	}
	t.unfiltered = rows
	var cols []int // the columns on screen last time: a filter matches what you see
	if len(t.shown) > 0 {
		for i, h := range header {
			if t.shown[h] {
				cols = append(cols, i)
			}
		}
	}
	rows = filterRowsIn(rows, t.filter, cols)
	sortRows(rows, t.sort, t.desc, timeOf)
	if c := sortColumn(header, t.sort); c >= 0 {
		blank := func(cell string) bool { // "-": nothing to sort by, so last either way
			cell = strings.TrimSpace(ansi.Strip(cell))
			return cell == "" || cell == "-" || cell == "n/a"
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if bi, bj := blank(rows[i].Cells[c]), blank(rows[j].Cells[c]); bi != bj {
				return bj
			}
			a, b := amountIn(rows[i].Cells[c]), amountIn(rows[j].Cells[c])
			if t.desc {
				return a < b
			}
			return a > b
		})
	}
	if t.problemsFirst && t.sort == sortDefault {
		sort.SliceStable(rows, func(i, j int) bool { return toneRank(rows[i].Tone) > toneRank(rows[j].Tone) })
	}
	if f := strings.ToLower(strings.TrimSpace(t.filter)); f != "" && !isLabelFilter(f) && !strings.HasPrefix(f, "!") {
		// What you typed exactly comes first (/auth finds auth before
		// authz), then names that start with it, then the rest.
		rank := func(r view.Row) int {
			if r.Obj == nil {
				return 2
			}
			switch name := strings.ToLower(r.Obj.GetName()); {
			case name == f:
				return 0
			case strings.HasPrefix(name, f):
				return 1
			}
			return 2
		}
		sort.SliceStable(rows, func(i, j int) bool { return rank(rows[i]) < rank(rows[j]) })
	}
	t.rows = rows

	// A row gone for a moment (a port-forward moving to another pod) gets
	// the cursor back when it is listed again within a few seconds, unless
	// the cursor, filter or sort changed meanwhile. Until then the cursor
	// follows the row it is on, as for any other change.
	b := t.back
	if b.key != "" && (time.Now().After(b.until) || b.cursor != t.cursor || b.filter != t.filter || b.sort != t.sort || b.desc != t.desc) {
		t.back = goneRow{}
	}
	rowID := func(r view.Row) string {
		return string(r.Obj.GetUID()) + r.Obj.GetKind() + r.Obj.GetNamespace() + "/" + r.Obj.GetName()
	}
	found := false
	for i, r := range rows {
		if r.Obj != nil && t.back.key != "" && t.want == "" && rowID(r) == t.back.key {
			t.cursor, t.back, found = i, goneRow{}, true
			break
		}
	}
	if !found && selected != "" {
		for i, r := range rows {
			if r.Obj != nil && rowID(r) == selected {
				t.cursor, t.want, found = i, "", true
				break
			}
		}
		if !found && t.want == "" && t.back.key == "" && len(rows) > 0 && time.Since(t.openedAt) > time.Second {
			t.back = goneRow{key: selected, until: time.Now().Add(5 * time.Second)}
		}
	}
	t.clamp()
	if t.back.key != "" { // what counts as unchanged at the next refresh
		t.back.cursor, t.back.filter, t.back.sort, t.back.desc = t.cursor, t.filter, t.sort, t.desc
	}
}

// goneRow is a row that left the list, the cursor's last place: see setRows.
type goneRow struct {
	key    string
	until  time.Time
	cursor int
	filter string
	sort   sortMode
	desc   bool
}

func (t *table) selected() (view.Row, bool) {
	if t.cursor < 0 || t.cursor >= len(t.rows) {
		return view.Row{}, false
	}
	return t.rows[t.cursor], true
}

func (t *table) move(delta int) {
	t.touched = len(t.rows) > 0 // moving through an empty, loading list picks nothing
	t.want, t.back = "", goneRow{}
	t.cursor += delta
	t.clamp()
}

// jump puts the cursor on row i (a key, a click, a search): a row that
// was gone a moment no longer calls it back. Never above the first row:
// G or end on an empty (still loading) list asks for row -1, and the
// rows that arrive next must still draw from the top.
func (t *table) jump(i int) {
	t.cursor, t.back = max(i, 0), goneRow{}
}

func (t *table) clamp() {
	if t.cursor >= len(t.rows) {
		t.cursor = len(t.rows) - 1
	}
	if t.cursor < 0 {
		t.cursor = 0
	}
}

func (t *table) reset() {
	t.cursor, t.offset, t.filter, t.touched, t.shown, t.want, t.back = 0, 0, "", false, nil, "", goneRow{}
	t.openedAt = time.Now()
}

// filterRows keeps rows whose cells contain the filter (case-insensitive).
// A leading "!" inverts the match.
func filterRows(rows []view.Row, filter string) []view.Row { return filterRowsIn(rows, filter, nil) }

// filterRowsIn matches only the cells in cols (the columns on screen), so a
// row never matches for text you can't see; nil means every cell.
func filterRowsIn(rows []view.Row, filter string, cols []int) []view.Row {
	if filter == "" || isLabelFilter(filter) { // label filters apply to objects, earlier
		return rows
	}
	invert := strings.HasPrefix(filter, "!")
	needle := strings.ToLower(strings.TrimPrefix(filter, "!"))
	if needle == "" {
		return rows
	}
	switch needle { // "/warnings" or "/problems": only the rows flagged in colour
	case "warning", "warnings", "problem", "problems":
		out := rows[:0:0]
		for _, r := range rows {
			if (r.Tone == view.ToneWarn || r.Tone == view.ToneBad) != invert {
				out = append(out, r)
			}
		}
		return out
	}
	// A filter with regex characters (^web, api$, web|auth) is a regex.
	var re *regexp.Regexp
	if strings.ContainsAny(needle, "^$|*+?[]()") {
		// As typed, not lowercased: \D or \S lowercased flip their meaning;
		// (?i) already ignores case.
		re, _ = regexp.Compile("(?i)" + strings.TrimPrefix(filter, "!"))
	}
	out := rows[:0:0]
	for _, r := range rows {
		cells := r.Cells
		if cols != nil {
			cells = pick(r.Cells, cols)
		}
		text := strings.Join(cells, " ")
		if r.Obj != nil {
			text += " " + r.Obj.GetName() // cells may show a shortened name
			// A pod's phase, as kubectl says it: STATUS may say why it's
			// worth a look ("Hit memory limit") while it is Running.
			if phase, _, _ := unstructured.NestedString(r.Obj.Object, "status", "phase"); phase != "" && r.Obj.GetKind() == "Pod" {
				text += " " + phase
			}
		}
		hit := strings.Contains(strings.ToLower(text), needle)
		if re != nil {
			hit = false
			for _, c := range append(cells, nameOf(r)) { // ^ means the start of a cell
				if re.MatchString(strings.TrimPrefix(c, markPrefix)) {
					hit = true
					break
				}
			}
		}
		if hit != invert {
			out = append(out, r)
		}
	}
	return out
}

// isLabelFilter: "/-l app=x" filters by label selector instead of text.
func isLabelFilter(f string) bool { return strings.HasPrefix(f, "-l ") || f == "-l" }

// toneRank orders rows for problems first.
func toneRank(t view.Tone) int {
	switch t {
	case view.ToneBad:
		return 2
	case view.ToneWarn:
		return 1
	case view.ToneDim: // finished or idle: after what is running
		return -1
	}
	return 0
}

func sortRows(rows []view.Row, mode sortMode, desc bool, timeOf func(view.Row) int64) {
	var less func(a, b view.Row) bool
	switch mode {
	case sortName:
		less = func(a, b view.Row) bool { return a.Obj.GetName() < b.Obj.GetName() }
	case sortAge:
		if timeOf == nil { // this list has no times: keep its own order
			return
		}
		// Newest first reads naturally for "what just happened". No time
		// (never deployed) goes last either way.
		sort.SliceStable(rows, func(i, j int) bool {
			ti, tj := timeOf(rows[i]), timeOf(rows[j])
			if (ti <= 0) != (tj <= 0) {
				return tj <= 0
			}
			if desc {
				return ti < tj
			}
			return ti > tj
		})
		return
	default:
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if desc {
			return less(rows[j], rows[i])
		}
		return less(rows[i], rows[j])
	})
}

// headerWithArrow marks the sorted column like k9s: NAME↑, AGE↓.
func (t *table) headerWithArrow() []string {
	if len(t.header) == 0 {
		return nil
	}
	out := append([]string(nil), t.header...)
	if t.sort == sortDefault {
		return out // the list's own order (problems first, best match…); no arrow
	}
	arrow := "↑"
	if t.desc {
		arrow = "↓"
	}
	col := 0 // by name: the first column, or the first that isn't a time (events start with AGO)
	for i, h := range t.header {
		if !isTimeColumn(h) {
			col = i
			break
		}
	}
	for i, h := range t.header {
		if t.sort == sortAge && isTimeColumn(h) {
			col = i
			arrow = map[bool]string{false: "↓", true: "↑"}[t.desc] // newest first is "most first", like the others
		}
	}
	if c := sortColumn(t.header, t.sort); c >= 0 {
		col = c
		arrow = map[bool]string{false: "↓", true: "↑"}[t.desc] // most first reads as going down
	}
	out[col] += arrow
	return out
}

// visibleColumns drops optional columns (in order) until every heading
// fits in width.
func (t *table) visibleColumns(width int) []int {
	hidden := map[string]bool{}
	cols := func() []int {
		var idx []int
		for i, h := range t.header {
			if !hidden[h] {
				idx = append(idx, i)
			}
		}
		return idx
	}
	// A column needs its heading, or its natural width up to
	// readableWidth ("Running", "CrashLoopBackOff"…); NAME needs nameKeep.
	natural := make([]int, len(t.header))
	all := t.rows
	if len(t.unfiltered) > 0 { // the whole list: typing a filter doesn't add or drop columns
		all = t.unfiltered
	}
	for _, r := range all {
		for i, c := range r.Cells {
			if i < len(natural) {
				natural[i] = max(natural[i], lipgloss.Width(c))
			}
		}
	}
	fits := func(idx []int) bool {
		need := colGap * (len(idx) - 1)
		for _, i := range idx {
			want := min(natural[i], readableWidth)
			switch {
			case i == nameColumn(t.header):
				want = min(natural[i], nameKeep)
			case t.header[i] == t.main:
				// The main column takes whatever is left when drawn; asking
				// for less here keeps NAMESPACE and friends on screen.
				want = min(natural[i], 24)
			case keepWhole(t.header[i]):
				want = natural[i] // whole, or the column goes
			}
			need += max(lipgloss.Width(t.header[i]), want, colMinWidth)
		}
		return need <= width
	}
	idx := cols()
	last := ""
	needNS := namesRepeatAcross(t.header, all) // two "cm1" rows: the namespace tells them apart
	for _, name := range t.optional {
		if fits(idx) {
			break
		}
		if name == "NAMESPACE" && needNS {
			continue // the others go first; the widths then cut NAME to make room
		}
		hidden[name], last = true, name
		idx = cols()
	}
	// A whole column hidden last (a node pool) may leave room for ones
	// hidden before it: bring back each that still fits. Only then: elsewhere
	// the hide order is what matters (WHY outlasts restarts and dates).
	for i := len(t.optional) - 1; i >= 0 && keepWhole(last); i-- {
		if name := t.optional[i]; hidden[name] {
			hidden[name] = false
			if !fits(cols()) {
				hidden[name] = true
			}
		}
	}
	return cols()
}

func pick(cells []string, idx []int) []string {
	out := make([]string, len(idx))
	for j, i := range idx {
		if i < len(cells) {
			out[j] = cells[i]
		}
	}
	return out
}

// render draws the table into width x height cells.
func (t *table) render(width, height int) string {
	if height < 2 {
		t.onScreen = 0
		return ""
	}
	idx := t.visibleColumns(width)
	t.shown = map[string]bool{}
	for _, i := range idx {
		t.shown[t.header[i]] = true
	}
	header := pick(t.headerWithArrow(), idx)
	visible := make([]view.Row, len(t.rows))
	for i, r := range t.rows {
		visible[i] = view.Row{Obj: r.Obj, Cells: pick(r.Cells, idx), Tone: r.Tone}
	}
	mainIdx := -1
	for j, i := range idx {
		if t.header[i] == t.main {
			mainIdx = j
		}
	}
	sized := visible
	if t.filter != "" && len(t.unfiltered) > 0 {
		sized = make([]view.Row, len(t.unfiltered))
		for i, r := range t.unfiltered {
			sized[i] = view.Row{Cells: pick(r.Cells, idx)}
		}
	}
	colsWidth := width
	if noColor { // room for the row marks ("! ◀") at the end, so they never cut the last column
		colsWidth = max(width-4, 10)
	}
	widths := columnWidths(header, sized, colsWidth, mainIdx) // header includes the sort arrow

	bodyHeight := height - 1
	t.onScreen = bodyHeight
	if t.cursor < t.offset {
		t.offset = t.cursor
	}
	if t.cursor >= t.offset+bodyHeight {
		t.offset = t.cursor - bodyHeight + 1
	}

	var b strings.Builder
	b.WriteString(styleHeader.Render(formatRow(header, widths)))
	for i := t.offset; i < len(visible) && i < t.offset+bodyHeight; i++ {
		b.WriteByte('\n')
		r := visible[i]
		line := formatRow(r.Cells, widths)
		// Without colour (NO_COLOR) a row flagged in colour says so with
		// "!" at the end, so "N to check" can still be found.
		flag := " "
		if noColor && (r.Tone == view.ToneWarn || r.Tone == view.ToneBad) {
			flag = "!"
		}
		if noColor && i != t.cursor && flag == "!" && width > 2 {
			line = padRight(ansi.Truncate(line, width-2, "…"), width-2) + " !"
		}
		if i == t.cursor {
			// Reversed in the row's own colour, so a failing row stays red
			// while selected. Without colour (NO_COLOR) there is no reverse
			// either: a mark at the end says which row it is.
			if noColor && width > 3 {
				line = padRight(ansi.Truncate(line, width-3, "…"), width-3) + flag + " ◀"
			}
			line = toneStyle(r.Tone).Inherit(styleCursor).Render(padRight(line, width))
		} else {
			line = toneStyle(r.Tone).Render(line)
		}
		b.WriteString(line)
	}
	return b.String()
}

const (
	colGap      = 2
	colMaxWidth = 60
	colMinWidth = 4
	// nameKeep is how much of NAME survives before other columns stop
	// shrinking: the name is what you read first.
	nameKeep = 36
	// mainKeep is how much of the main column survives before others stop
	// shrinking.
	mainKeep = 60
	nameMin  = 20
	// readableWidth is how wide a non-name column must stay to be read
	// before optional columns start hiding.
	readableWidth = 16
)

// keepWhole: a column whose values differ anywhere (node pools:
// "nodepool-system-arm64", "nodepool-general-arm64"), so a
// cut one can't be told apart: it shows whole, or drops out (the box below
// names it).
func keepWhole(header string) bool { return header == "NODEPOOL" }

// columnWidths fits columns into width: each gets its natural size capped at
// colMaxWidth (the last column may use whatever is left), then columns
// shrink in stages until everything fits:
//
//  1. wide cells shrink, keeping NAME ≤ nameKeep and others ≤ readableWidth
//  2. NAME gives up space down to nameMin
//  3. everything shrinks down to its heading
//  4. headings get cut too
func columnWidths(header []string, rows []view.Row, width, mainIdx int) []int {
	n := len(header)
	w, natural := make([]int, n), make([]int, n)
	for i, h := range header {
		natural[i] = lipgloss.Width(h)
	}
	for _, r := range rows {
		for i, c := range r.Cells {
			if i < n {
				natural[i] = max(natural[i], lipgloss.Width(c))
			}
		}
	}
	nameIdx := nameColumn(header)
	for i := range w {
		w[i] = natural[i]
		if i != nameIdx && i != mainIdx && i < n-1 && !keepWhole(header[i]) { // the name and the main column may use what they need
			w[i] = min(w[i], colMaxWidth)
		}
	}

	needNS := namesRepeatAcross(header, rows) // the namespace is what tells two rows apart: keep it whole
	floor := func(nameWidth int) []int {
		f := make([]int, n)
		for i, h := range header {
			f[i] = max(lipgloss.Width(h), min(natural[i], readableWidth))
			if keepWhole(h) {
				f[i] = natural[i]
			}
			if needNS && strings.TrimRight(h, "↑↓") == "NAMESPACE" {
				f[i] = max(f[i], min(natural[i], 32))
			}
			if i == nameIdx {
				f[i] = min(natural[i], nameWidth)
			}
		}
		return f
	}
	headings := make([]int, n)
	for i, h := range header {
		headings[i] = lipgloss.Width(h)
	}

	avail := width - colGap*(n-1)
	// Whole names first: the main column (WHY, a message) gives up its
	// extra room before a name or namespace is cut, so a wider screen never
	// shows less of a name than a narrow one.
	whole := floor(nameKeep)
	for i, h := range header {
		if mainIdx < 0 || mainIdx >= n || header[mainIdx] != "WHY" {
			break // elsewhere the main column is the message itself: it keeps its room
		}
		switch {
		case i == nameIdx:
			whole[i] = min(natural[i], 60)
		case h == "NAMESPACE":
			whole[i] = min(natural[i], 32)
		case i == mainIdx:
			whole[i] = min(w[i], 24)
		}
	}
	if mainIdx >= 0 && mainIdx < n && header[mainIdx] == "WHY" {
		shrink(w, whole, avail)
	}
	// Then the name and the main column give way last: first the others
	// shrink to readable, keeping those two.
	keep := floor(nameKeep)
	if nameIdx >= 0 {
		keep[nameIdx] = min(w[nameIdx], nameKeep)
	}
	if mainIdx >= 0 && mainIdx < n {
		keep[mainIdx] = min(w[mainIdx], mainKeep)
	}
	shrink(w, keep, avail)
	shrink(w, floor(nameKeep), avail)
	shrink(w, floor(nameMin), avail)
	shrink(w, headings, avail)
	shrink(w, nil, avail)
	return w
}

// nameColumn is where a list's names are: the NAME/APP/CLUSTER column, or
// the first column unless that is a time.
func nameColumn(header []string) int {
	for i, h := range header {
		switch strings.TrimRight(h, "↑↓") {
		case "NAME", "APP", "CLUSTER", "ABOUT":
			return i
		}
	}
	if len(header) > 0 && !isTimeColumn(header[0]) {
		return 0
	}
	return -1
}

func isTimeColumn(h string) bool {
	switch strings.TrimRight(h, "↑↓") {
	case "AGE", "AGO", "LAST", "LAST RUN", "DEPLOYED":
		return true
	}
	return false
}

// shrink narrows the widest column one cell at a time until sum(w) <= avail
// or no column can shrink below its floor (colMinWidth when floor is nil).
func shrink(w, floor []int, avail int) {
	for sum(w) > avail {
		widest := -1
		for i := range w {
			limit := colMinWidth
			if floor != nil {
				limit = max(floor[i], colMinWidth)
			}
			if w[i] > limit && (widest < 0 || w[i] > w[widest]) {
				widest = i
			}
		}
		if widest < 0 {
			return
		}
		w[widest]--
	}
}

func formatRow(cells []string, widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		c := ""
		if i < len(cells) {
			c = cells[i]
		}
		// Names: drop the generated hash first, then cut the middle. Node
		// addresses differ at the end too. Everything else is cut at the end.
		switch {
		case i == 0:
			c = shortenName(c, w)
		case strings.HasPrefix(c, "ip-"):
			c = cutListName(c, w)
		default:
			c = truncate(c, w)
		}
		parts[i] = padRight(c, w)
	}
	return strings.TrimRight(strings.Join(parts, strings.Repeat(" ", colGap)), " ")
}

func truncate(s string, w int) string {
	// Much longer than it can show, and plain: cut first, then measure.
	// Coloured text is never cut by bytes: a cut inside a colour code eats
	// the "…" and leaves a cell of old screen showing (a stray "│"), and
	// many colour codes make a short line look long.
	if len(s) > 4*w+256 && !strings.Contains(s, "\x1b") {
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			s = s[:i]
		}
		if len(s) > 4*w+256 {
			cut := 4*w + 256
			for cut > 0 && !utf8.RuneStart(s[cut]) { // not inside a letter either
				cut--
			}
			s = s[:cut]
		}
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	if w <= 1 {
		return "…"
	}
	// One pass, colour-aware (trimming a rune at a time and measuring again
	// takes forever on a big text, such as copied logs).
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return ansi.Truncate(s, w-1, "") + "…"
}

// podName splits "<workload>-<template hash>-<5 chars>" (a Deployment's
// pod) and "<workload>-<template hash>" (its ReplicaSet).
var podName = regexp.MustCompile(`^(.+)-([a-z0-9]{6,10})(-[a-z0-9]{5})?$`)

// shortenName fits a name into w: the generated hash goes first
// ("shop-backend-celery-…-nsxxx"), then the middle of what is left.
func shortenName(s string, w int) string {
	if lipgloss.Width(s) <= w || strings.Contains(s, "\x1b") {
		return truncateMiddle(s, w)
	}
	prefix := ""
	if strings.HasPrefix(s, markPrefix) {
		prefix, s = markPrefix, strings.TrimPrefix(s, markPrefix)
		w -= lipgloss.Width(markPrefix)
	}
	if m := podName.FindStringSubmatch(s); m != nil && strings.ContainsAny(m[2], "0123456789") {
		tail := "-…" + m[3]
		if m[3] == "" {
			tail = "-…"
		}
		if room := w - len([]rune(tail)); room >= 8 {
			base := []rune(m[1])
			if len(base) > room { // one cut only: the end of the base goes with the hash
				return prefix + string(base[:room-1]) + "…" + strings.TrimPrefix(tail, "-…")
			}
			return prefix + m[1] + tail
		}
	}
	return prefix + cutListName(s, w)
}

// cutListName is truncateMiddle for a list's names, also when narrow:
// names often share their start ("ip-10-0-"), so the end, which tells
// them apart, gets most of the room ("ip-…145-239").
func cutListName(s string, w int) string {
	if w >= 12 || w < 5 || lipgloss.Width(s) <= w || strings.Contains(s, "\x1b") {
		return truncateMiddle(s, w)
	}
	r := []rune(s)
	head := (w - 1) / 3
	return string(r[:head]) + "…" + string(r[len(r)-(w-1-head):])
}

// truncateMiddle keeps a name's start and end: "auth-app-ser…7f6bcc-22v96".
func truncateMiddle(s string, w int) string {
	if lipgloss.Width(s) <= w || w < 12 || strings.Contains(s, "\x1b") {
		return truncate(s, w)
	}
	r := []rune(s)
	tail := (w - 1) / 2
	head := w - 1 - tail
	return string(r[:head]) + "…" + string(r[len(r)-tail:])
}

func padRight(s string, w int) string {
	if gap := w - lipgloss.Width(s); gap > 0 {
		return s + strings.Repeat(" ", gap)
	}
	return s
}

func sum(xs []int) int {
	n := 0
	for _, x := range xs {
		n += x
	}
	return n
}

// sameEverywhere: the column holds one value on every row (and there are
// rows enough for that to mean something).
// namesRepeatAcross: two rows share a name but not a namespace, so only
// the NAMESPACE column tells them apart.
func namesRepeatAcross(header []string, rows []view.Row) bool {
	n, ns := nameColumn(header), slices.IndexFunc(header, func(h string) bool { return strings.TrimRight(h, "↑↓") == "NAMESPACE" })
	if n < 0 || ns < 0 {
		return false
	}
	seen := map[string]string{} // name: a namespace it was seen in
	for _, r := range rows {
		if n >= len(r.Cells) || ns >= len(r.Cells) {
			continue
		}
		name := ansi.Strip(r.Cells[n])
		if first, ok := seen[name]; ok && first != r.Cells[ns] {
			return true
		}
		seen[name] = r.Cells[ns]
	}
	return false
}

func sameEverywhere(header []string, rows []view.Row, col string) bool {
	i := slices.Index(header, col)
	if i < 0 || len(rows) < 3 {
		return false
	}
	for _, r := range rows[1:] {
		if i >= len(r.Cells) || r.Cells[i] != rows[0].Cells[i] {
			return false
		}
	}
	return true
}

func nameOf(r view.Row) string {
	if r.Obj == nil {
		return ""
	}
	return r.Obj.GetName()
}
