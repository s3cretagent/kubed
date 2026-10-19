package ui

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

func TestJumpsDoNotStack(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.openResource("pods", true) // somewhere you drilled to
	start := len(m.history)
	m.jump("p")
	m.jump("n")
	m.jump("N")
	if len(m.history) != start+3 {
		t.Fatalf("each jump should be one step back, got %d (from %d)", len(m.history), start)
	}
	// Enter-style drill keeps the trail; esc goes back one.
	m.openResource("pods", true)
	if locName(m.history[len(m.history)-1]) != "node pools" {
		t.Fatalf("drill trail = %v", m.history)
	}
	m.back()
	if m.loc.res.GVR != kube.NodePools {
		t.Fatalf("back went to %v", m.loc.res.GVR)
	}
	m.back() // back through the jumps, one at a time
	m.back()
	m.back()
	if m.loc.res.GVR != kube.Pods {
		t.Fatalf("esc back through the jumps went to %v, want the pods you started from", m.loc.res.GVR)
	}
}

// Jumping to a list already on the trail goes back to it there: the
// trail never repeats a list, and esc doesn't walk through it twice.
func TestJumpBackOntoTheTrail(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.openResource("pods", true)
	m.jump("n") // pods › nodes
	m.jump("p") // pods again: back to it, not pods › nodes › pods
	if m.loc.res.GVR != kube.Pods || len(m.history) != 0 && m.history[len(m.history)-1].res.GVR == kube.Nodes {
		t.Fatalf("on %v, trail %v", m.loc.res.GVR, m.history)
	}
	for _, h := range m.history {
		if h.res.GVR == kube.Pods {
			t.Fatalf("pods twice on the trail: %v", m.history)
		}
	}
}

// Every view must give each key a single meaning.
func TestOneMeaningPerKey(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	views := []location{{kind: locContainers}, {kind: locHelm}, {kind: locForwards}, {kind: locHome}, {kind: locFind}}
	for _, gvr := range []schema.GroupVersionResource{kube.Pods, kube.Deployments, kube.StatefulSets, kube.DaemonSets,
		kube.ReplicaSets, kube.Jobs, kube.CronJobs, kube.Nodes, kube.Secrets, kube.Services, kube.ServiceAccounts} {
		views = append(views, location{kind: locResource, res: kube.Resource{GVR: gvr}})
	}
	for _, loc := range views {
		m.loc = loc
		seen := map[string]string{}
		for _, a := range actions {
			if !a.when(m) || a.key == "" { // menu-only items have no key
				continue
			}
			if prev, dup := seen[a.key]; dup {
				t.Errorf("%s: key %q means both %q and %q", locName(loc), a.key, prev, a.label)
			}
			seen[a.key] = a.label
		}
	}
}

func TestScaleIsPlusAndShellIsS(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Deployments}}
	if a, ok := m.actionFor("+"); !ok || a.label != "Scale" {
		t.Errorf("+ on deployments = %+v", a)
	}
	if _, ok := m.actionFor("s"); ok {
		t.Error("s must not scale deployments any more")
	}
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	if a, ok := m.actionFor("s"); !ok || a.label != "Shell" {
		t.Errorf("s on pods = %+v", a)
	}
}

func TestMarksAndBulkRows(t *testing.T) {
	var tb table
	tb.setRows([]string{"NAME"}, rows("a", "b", "c"), nil)
	tb.toggleMark()
	tb.move(2)
	tb.toggleMark()
	tb.setRows([]string{"NAME"}, rows("a", "b", "c"), nil)
	got := tb.marked()
	if len(got) != 2 || !strings.HasPrefix(got[0].Cells[0], markPrefix) {
		t.Fatalf("marked = %+v", got)
	}
	tb.markAll()
	tb.markAll() // second time clears
	if len(tb.marked()) != 0 {
		t.Error("markAll twice should clear")
	}
}

func TestLabelFilterNotTextFiltered(t *testing.T) {
	in := rows("a", "b")
	if got := filterRows(in, "-l app=x"); len(got) != 2 {
		t.Errorf("label filter must not text-filter rows: %d", len(got))
	}
}

func TestPagerSearch(t *testing.T) {
	p := newPager("t", "one\nTwo error here\nthree\nerror again", 40, 3)
	p.setSearch("ERROR")
	if len(p.matches) != 2 || p.searchStatus() != `"ERROR" 1 of 2` {
		t.Fatalf("matches %v status %q", p.matches, p.searchStatus())
	}
	p.nextMatch(1)
	p.nextMatch(1) // wraps
	if p.match != 0 {
		t.Errorf("match = %d after wrapping", p.match)
	}
	if !strings.Contains(p.plainText(), "Two error here") {
		t.Error("plainText lost content")
	}
}

func TestMouseSelectsThenOpensMenu(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 40
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	m.tbl.setRows([]string{"NAME"}, rows("a", "b", "c"), nil)
	m.tbl.render(110, 20)         // drawn, as the screen is before any click
	first := m.headerHeight() + 2 // header, frame top border, column headings
	click := func(y int) {
		m.onMouse(tea.MouseMsg{Button: tea.MouseButtonLeft, Action: tea.MouseActionPress, Y: y})
	}
	click(first + 2)
	if m.tbl.cursor != 2 {
		t.Fatalf("click selected row %d, want 2", m.tbl.cursor)
	}
	click(first + 2) // second click on the selected row opens its menu
	if m.mode != modePrompt || m.prompt == nil || m.prompt.kind != promptMenu {
		t.Fatalf("second click: mode %v prompt %+v", m.mode, m.prompt)
	}
	m.mode, m.prompt = modeTable, nil
	m.onMouse(tea.MouseMsg{Button: tea.MouseButtonWheelUp})
	if m.tbl.cursor != 0 {
		t.Errorf("wheel up: cursor %d", m.tbl.cursor)
	}
}

func TestProdAsksForTheNameNotAnUnlock(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.opts.ProdPattern = regexp.MustCompile("prod")
	m.context = "acme-production"
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Deployments}}
	m.tbl.setRows([]string{"NAME"}, rows("api"), nil)
	if m.writesLocked() {
		t.Fatal("production must not be locked; it asks for the name instead")
	}
	m.runAction("r")
	if m.mode != modePrompt || m.prompt.typeToConfirm != "api" {
		t.Fatalf("restart on prod should ask to type %q; prompt %+v", "api", m.prompt)
	}
}

func TestDoubleEscIsTwoEscapes(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	m.tbl.setRows([]string{"NAME"}, rows("a"), nil)
	m.tbl.filter = "a"
	m.openPager(newPager("x", "text", 40, 5))
	m.onKey(tea.KeyMsg{Type: tea.KeyEsc, Alt: true})
	if m.mode != modeTable || m.tbl.filter != "" {
		t.Fatalf("alt+esc should close the pager and clear the filter: mode %v filter %q", m.mode, m.tbl.filter)
	}
}

// Opening something in another namespace and pressing esc puts you back in
// the namespace you came from (it used to stay switched).
func TestBackRestoresNamespace(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.jumpTo(location{kind: locHome})
	m.enterNamespace("auth")
	m.openResource("pods", true)
	if m.namespace != "auth" {
		t.Fatalf("drilled into %q", m.namespace)
	}
	m.back()
	if m.namespace != "" || m.loc.kind != locHome {
		t.Errorf("back: namespace %q, loc %v; want all namespaces on home", m.namespace, m.loc.kind)
	}

	// A namespace you chose yourself stays with the screen you chose it on.
	m.setNamespace("payments")
	m.openResource("pods", true)
	m.back()
	if m.namespace != "payments" {
		t.Errorf("chosen namespace lost: %q", m.namespace)
	}
}

func TestQQuits(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.forwards = &kube.Forwards{}
	m.jumpTo(location{kind: locHome})
	cmd := m.onKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	if cmd == nil {
		t.Fatal("q did nothing")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Error("q must quit")
	}
}

// Opening a search result and pressing esc brings back the search, typed in.
func TestEscReturnsToSearch(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.jumpTo(location{kind: locHome})
	m.startFind()
	m.input.SetValue("auth")
	m.mode = modeTable // enter on a result leaves the find box
	m.openResource("pods", true)
	m.back()
	if m.loc.kind != locFind || m.mode != modeTable || m.input.Value() != "auth" {
		t.Errorf("back to %v, mode %v, search %q; want the results for \"auth\", not typing", m.loc.kind, m.mode, m.input.Value())
	}
	if m.tbl.filter != "" {
		t.Errorf("results must come back whole, not filtered by %q", m.tbl.filter)
	}
	m.startFind() // f types again
	if m.mode != modeFind || m.loc.kind != locFind || len(m.history) != 1 {
		t.Errorf("f on the results should type again in place: mode %v, history %d", m.mode, len(m.history))
	}
}

// A shell can change a pod, so read-only mode refuses it.
func TestReadOnlyRefusesShell(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	for _, key := range []string{"s", "S"} {
		if a, ok := m.actionFor(key); !ok || !a.write {
			t.Errorf("%s must count as a change (blocked by --readonly)", key)
		}
	}
}

// On production, acting on several rows needs the action and the count typed.
func TestProductionBulkConfirm(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.env = environment{known: true, prod: true, source: "name"}
	rows := []view.Row{{Obj: view.Named("ns", "a"), Cells: []string{"a"}}, {Obj: view.Named("ns", "b"), Cells: []string{"b"}}}
	m.confirmTitled("Delete these 2 pods?", rows, nil, func() tea.Cmd { return nil })
	if m.prompt == nil || m.prompt.typeToConfirm != "delete 2" {
		t.Fatalf("typeToConfirm = %q, want \"delete 2\"", m.prompt.typeToConfirm)
	}
	m.prompt.input.SetValue("2")
	if _, done := m.prompt.update(tea.KeyMsg{Type: tea.KeyEnter}); done || m.prompt.problem == "" {
		t.Error("a bare count must not confirm, and the prompt should say why")
	}
}

// A filter on a node's full name finds it, though the list shows it short.
func TestFilterMatchesFullName(t *testing.T) {
	full := "ip-10-0-136-25.ap-south-1.compute.internal"
	rows := []view.Row{{Obj: view.Named("", full), Cells: []string{view.ShortNode(full)}}}
	if got := filterRows(rows, full); len(got) != 1 {
		t.Errorf("filter by full name found %d rows", len(got))
	}
}

// p (logs from before a restart) offers the container that restarted first.
func TestPreviousLogsPickRestartedContainer(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "multi-1", "namespace": "qa", "uid": "u1", "resourceVersion": "1"},
		"spec": map[string]any{
			"containers":     []any{map[string]any{"name": "app"}, map[string]any{"name": "side"}},
			"initContainers": []any{map[string]any{"name": "setup"}},
		},
		"status": map[string]any{"containerStatuses": []any{
			map[string]any{"name": "app", "restartCount": int64(1)},
			map[string]any{"name": "side", "restartCount": int64(6)},
		}},
	}}
	m.logs(view.Row{Obj: pod, Cells: []string{"multi-1"}}, true)
	if m.prompt == nil || m.prompt.items[0].label != "side" || len(m.prompt.items) != 2 {
		t.Fatalf("choices = %+v, want side then app (setup never restarted)", m.prompt.items)
	}
	m.prompt = nil
	m.logs(view.Row{Obj: pod, Cells: []string{"multi-1"}}, false)
	if m.prompt.items[0].label != "app" || m.prompt.items[len(m.prompt.items)-1].label != "setup (runs first)" {
		t.Errorf("for live logs the app's own container first, setup last: %+v", m.prompt.items)
	}
}

// Text typed fast arrives as one burst; "delete" must be text, not the key.
func TestBurstTypingIntoPrompt(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.env = environment{known: true, prod: true}
	rows := []view.Row{{Obj: view.Named("ns", "a"), Cells: []string{"a"}}, {Obj: view.Named("ns", "b"), Cells: []string{"b"}}}
	m.confirmTitled("Delete these 2 pods?", rows, nil, func() tea.Cmd { return nil })
	m.onKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("delete 2")})
	if got := m.prompt.input.Value(); got != "delete 2" {
		t.Errorf("typed %q, want \"delete 2\"", got)
	}
}

// Two results with the same name and namespace but different kinds (a
// deployment and its service): moving down must stay on the second after
// the list refreshes.
func TestCursorKeepsSameNamedRowsApart(t *testing.T) {
	mk := func(kind string) view.Row {
		o := view.Named("auth", "auth-app-server")
		o.SetKind(kind)
		return view.Row{Obj: o, Cells: []string{strings.ToLower(kind), "auth-app-server"}}
	}
	rows := func() []view.Row { return []view.Row{mk("Deployment"), mk("Service"), mk("Pod")} }
	var tb table
	tb.setRows([]string{"WHAT", "NAME"}, rows(), nil)
	tb.move(1)
	tb.setRows([]string{"WHAT", "NAME"}, rows(), nil) // the refresh every second
	if tb.cursor != 1 {
		t.Errorf("cursor jumped to %d after a refresh, want 1", tb.cursor)
	}
}

// A key pressed before the list has loaded says so instead of doing nothing.
func TestActionWhileLoadingSaysSo(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	m.runAction("d")
	if !strings.Contains(m.flash, "still loading") {
		t.Errorf("flash = %q", m.flash)
	}
}

// While a list re-sorts (data still arriving), a cursor on the first row
// stays on the first row.
func TestCursorOnTopStaysOnTop(t *testing.T) {
	mk := func(names ...string) []view.Row {
		var rows []view.Row
		for _, n := range names {
			rows = append(rows, view.Row{Obj: view.Named("ns", n), Cells: []string{n}})
		}
		return rows
	}
	var tb table
	tb.setRows([]string{"APP"}, mk("crowdsec", "argocd"), nil)
	tb.setRows([]string{"APP"}, mk("argocd", "crowdsec"), nil) // argocd became the worst
	if tb.cursor != 0 {
		t.Errorf("cursor followed the old first row to %d", tb.cursor)
	}
}

// Sorting by age on a list without times must not crash.
func TestSortByAgeWithoutTimes(t *testing.T) {
	tb := table{sort: sortAge}
	tb.setRows([]string{"APP"}, []view.Row{{Obj: view.Named("a", "x"), Cells: []string{"x"}}, {Obj: view.Named("a", "y"), Cells: []string{"y"}}}, nil)
	if len(tb.rows) != 2 {
		t.Fatal("rows lost")
	}
}

// Restart is findable: on pods (its deployment), and in read-only mode it
// shows greyed out instead of disappearing.
func TestRestartIsFindable(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	if a, ok := m.actionFor("r"); !ok || a.label != "Restart" {
		t.Errorf("r on pods = %+v", a)
	}
	m.opts.ReadOnly = true
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Obj: view.Named("ns", "web-1"), Cells: []string{"web-1"}}}, nil)
	m.menu()
	found := false
	for _, it := range m.prompt.items {
		if it.label == "Restart" {
			// Round 126: the reason r itself gives first (read-only), as
			// runAction checks it before looking at the pod.
			found = it.disabled && strings.Contains(it.about, "--readonly")
		}
	}
	if !found {
		t.Error("restart on a pod nothing runs should be shown, greyed, saying why")
	}
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Deployments}}
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Obj: view.Named("ns", "web"), Cells: []string{"web"}}}, nil)
	m.menu()
	found = false
	for _, it := range m.prompt.items {
		if strings.HasPrefix(it.label, "Restart") {
			found = it.disabled && strings.Contains(it.about, "--readonly")
		}
	}
	if !found {
		t.Error("in read-only the restart item should be shown, greyed, saying why")
	}
}

// Typing into a prompt with a suggested value replaces it: scale 1, type 3,
// gets 3 (not 13). Editing keys keep it.
func TestPrefilledPromptReplaces(t *testing.T) {
	var got string
	p := inputPrompt("Scale", "1", func(v string) tea.Cmd { got = v; return nil })
	p.update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("3")})
	p.update(tea.KeyMsg{Type: tea.KeyEnter})
	if got != "3" {
		t.Errorf("typed 3 over 1, got %q", got)
	}
	p = inputPrompt("Port", "8080:80", func(v string) tea.Cmd { got = v; return nil })
	p.update(tea.KeyMsg{Type: tea.KeyBackspace})
	p.update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("1")})
	p.update(tea.KeyMsg{Type: tea.KeyEnter})
	if got != "8080:81" {
		t.Errorf("edited 8080:80, got %q", got)
	}
}

// Errors only keeps named errors (ConfigurationError) and the stack trace
// lines under an error, and drops the rest.
func TestErrorsOnlyKeepsTraces(t *testing.T) {
	p := &pager{errorsOnly: true, lines: []string{
		"starting",
		"Error in worker",
		"  File \"x.py\", line 3",
		"ConfigurationError: missing key",
		"all good",
		"    indented, but after a normal line",
	}}
	got := strings.Join(p.visible(), "|")
	want := "Error in worker|  File \"x.py\", line 3|ConfigurationError: missing key"
	if got != want {
		t.Errorf("got %q", got)
	}
}

// Find: a name containing the words is a match; letters in order are only
// a close match.
func TestFindScoreLoose(t *testing.T) {
	exact := findScore([]string{"argocd-ser"}, "argocd-server", "argocd", "pod")
	near := findScore([]string{"argocd-ser"}, "argocd-notifications-controller", "argocd", "pod")
	if exact >= looseMark || near < looseMark {
		t.Errorf("match %d, close match %d", exact, near)
	}
}

// A Service port goes to its targetPort, by number or by the name of a
// container port.
func TestServicePortToPod(t *testing.T) {
	svc := &unstructured.Unstructured{Object: map[string]any{"kind": "Service", "metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"ports": []any{
			map[string]any{"port": int64(80), "targetPort": "http"},
			map[string]any{"port": int64(9090), "targetPort": int64(9091)},
			map[string]any{"port": int64(5000)},
		}}}}
	pod := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "web-1"},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "app",
			"ports": []any{map[string]any{"name": "http", "containerPort": int64(8080)}}}}}}}
	for port, want := range map[int]int{80: 8080, 9090: 9091, 5000: 5000} {
		if got, err := servicePortToPod(svc, pod, port); err != nil || got != want {
			t.Errorf("port %d: got %d, %v; want %d", port, got, err, want)
		}
	}
	if _, err := servicePortToPod(svc, pod, 1234); err == nil {
		t.Error("a port the service doesn't have should say so")
	}
}

// Once you've opened a menu or acted on the top row, it stays selected
// when the list re-sorts, so a key never lands on a row you didn't pick.
func TestTouchedRowFollowsResort(t *testing.T) {
	mk := func(names ...string) []view.Row {
		var rows []view.Row
		for _, n := range names {
			rows = append(rows, view.Row{Obj: view.Named("ns", n), Cells: []string{n}})
		}
		return rows
	}
	var tb table
	tb.setRows([]string{"APP"}, mk("airflow", "cron"), nil)
	tb.touched = true
	tb.setRows([]string{"APP"}, mk("cron", "airflow"), nil)
	if r, _ := tb.selected(); r.Obj.GetName() != "airflow" {
		t.Errorf("selection moved to %s", r.Obj.GetName())
	}
}

// A response size of 500 is not a server error; a 500 status is.
func TestErrorLineHTTPStatus(t *testing.T) {
	ok := `INFO "GET /auth/token/validate HTTP/1.1" 200 500`
	bad := `INFO "GET /auth/token/validate HTTP/1.1" 502 13`
	if errorLine.MatchString(ok) || !errorLine.MatchString(bad) || !errorLine.MatchString(`{"status": 503}`) {
		t.Error("5xx should match only as a status")
	}
}

// Not connected: opening the cluster list and going back must not crash
// (there is no client to load anything with).
func TestBackFromClustersWhileNotConnected(_ *testing.T) {
	m := testModel()
	m.client.Cache.Stop()
	m.client = nil
	m.loc = location{kind: locHome}
	m.navigate(location{kind: locContexts}, true)
	m.onKey(tea.KeyMsg{Type: tea.KeyEsc}) // panicked before
	m.Update(tickMsg{})                   // the clock ticks on the cluster picker too
}

// Toggling errors only or wrap keeps the line you were reading on top.
func TestLogTogglesKeepPlace(t *testing.T) {
	var lines []string
	for i := range 100 {
		if i%10 == 0 {
			lines = append(lines, fmt.Sprintf("line %d error happened", i))
		} else {
			lines = append(lines, fmt.Sprintf("line %d fine", i))
		}
	}
	p := newPager("logs", strings.Join(lines, "\n"), 40, 5)
	p.isLogs = true
	p.vp.SetYOffset(25)
	p.keepPlace(func() { p.errorsOnly = true })
	if got := p.visible()[p.lineAt(p.vp.YOffset)]; got != "line 30 error happened" {
		t.Fatalf("errors only: top line %q, want line 30", got)
	}
	p.keepPlace(func() { p.errorsOnly = false })
	if got := p.lines[p.lineAt(p.vp.YOffset)]; got != "line 30 error happened" {
		t.Fatalf("all lines again: top line %q, want line 30", got)
	}
	p.keepPlace(func() { p.wrap = !p.wrap })
	if got := p.lines[p.lineAt(p.vp.YOffset)]; got != "line 30 error happened" {
		t.Fatalf("wrap: top line %q, want line 30", got)
	}
}

// ":pods -l app=web" is a label filter, as in kubectl, never a namespace.
func TestCommandLabelFlag(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.runCommand("pods -l app=web")
	if m.namespace == "-l" || m.tbl.filter != "-l app=web" {
		t.Fatalf("namespace %q, filter %q; want the label filter and no namespace change", m.namespace, m.tbl.filter)
	}
	m.runCommand("pods -A")
	if m.namespace != "" {
		t.Errorf("-A should show every namespace, got %q", m.namespace)
	}
}

// JSON logs say their level in a field: that decides, whatever words the
// message carries.
func TestErrorLineJSONLevel(t *testing.T) {
	isErr := func(l string) bool {
		return errorLine.MatchString(l) && (!infoLine.MatchString(l) || serverError.MatchString(l))
	}
	for line, want := range map[string]bool{
		`{"levelname": "ERROR", "message": "No valid payment gateway found"}`:           true,
		`{"levelname": "INFO", "message": "Ignoring razorpay payment.failed callback"}`: false,
		`{"severity": "WARNING", "message": "retry failed, trying again"}`:              false,
		`{"level": "error", "msg": "db down"}`:                                          true,
	} {
		if got := isErr(line); got != want {
			t.Errorf("%s: error = %v, want %v", line, got, want)
		}
	}
}

// Keys typed as a shell closes ("exit" into a pod that just went away)
// must not act in kubed: e would open an edit.
func TestKeysQuietAfterShellCloses(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	m.Update(resultMsg{text: "shell closed", shellEnd: true})
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	if m.mode == modeCommand {
		t.Error("a key right after the shell closed reached kubed")
	}
	m.keysQuietUntil = time.Time{}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	if m.mode != modeCommand {
		t.Error("keys should work again once the quiet second is over")
	}
}

// Read-only leaves out what Kubernetes' "view" role does: secret values
// and port-forwards, with the reason said.
func TestReadOnlyLeavesOutSecretsAndPorts(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.opts.ReadOnly = true
	for _, gvr := range []schema.GroupVersionResource{kube.Pods, kube.Secrets} {
		m.loc = location{kind: locResource, res: kube.Resource{GVR: gvr}}
		key := "F"
		if gvr == kube.Secrets {
			key = "x"
		}
		a, ok := m.actionFor(key)
		if !ok || !a.sensitive {
			t.Fatalf("%s on %s should be a sensitive action", key, gvr.Resource)
		}
		m.flash = ""
		m.runAction(key)
		if !strings.Contains(m.flash, "secret values and port-forwards") {
			t.Errorf("%s on %s in read-only: flash %q", key, gvr.Resource, m.flash)
		}
	}
}

// Errors only keeps a stack trace whole in merged logs: its frames are
// indented after the pod tag, and other pods' lines may come between.
func TestErrorsOnlyKeepsFramesAcrossPods(t *testing.T) {
	p := &pager{errorsOnly: true, tagNames: map[string]string{"a·x": "a/x", "b·y": "b/y"}, lines: []string{
		"a·x │ Traceback (most recent call last):",
		"b·y │ GET /health 200",
		`a·x │   File "/app/amqp/transport.py", line 467, in _read`,
		"a·x │     raise IOError('Server unexpectedly closed connection')",
		"a·x │ OSError: Server unexpectedly closed connection",
		"a·x │ done",
	}}
	got := p.shown()
	want := []int{0, 2, 3, 4}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("shown %v, want %v", got, want)
	}
}

// /Running finds a running pod whose STATUS names something else to see.
func TestFilterMatchesPodPhase(t *testing.T) {
	pod := &unstructured.Unstructured{Object: map[string]any{"kind": "Pod",
		"metadata": map[string]any{"name": "celery-1"}, "status": map[string]any{"phase": "Running"}}}
	rows := []view.Row{{Obj: pod, Cells: []string{"celery-1", "Hit memory limit"}}}
	if got := filterRows(rows, "Running"); len(got) != 1 {
		t.Error("a running pod should match /Running")
	}
	if got := filterRows(rows, "!Running"); len(got) != 0 {
		t.Error("and not /!Running")
	}
}

// Each wrapped row of a coloured line keeps its colour.
func TestCarryStyle(t *testing.T) {
	red := "\x1b[31m"
	got := carryStyle(red+"- value: a-long-image-name", red+"- value:\na-long-image-name")
	rows := strings.Split(got, "\n")
	if len(rows) != 2 || !strings.HasPrefix(rows[1], red) {
		t.Errorf("second row lost its colour: %q", got)
	}
}

// A wrapped diff line repeats its - or + on its next rows.
func TestCarryMark(t *testing.T) {
	got := carryMark("-     checksum: 23f4", "-     checksum:\n  23f4")
	if rows := strings.Split(got, "\n"); rows[1] != "- 23f4" {
		t.Errorf("second row: %q", rows[1])
	}
}

// Merged logs of an app's workloads tag each with the part that differs.
func TestLogTagsDropSharedStart(t *testing.T) {
	tags := logTags([][2]string{
		{"agent-data-gateway-tableau-7d9f8c6b5d-9jhlq", "app"},
		{"agent-data-gateway-server-5f6d7c8b9a-qtpj7", "app"},
	})
	if tags[0] != "tableau·9jhlq" || tags[1] != "server·qtpj7" {
		t.Errorf("got %q", tags)
	}
}

// Log tags tell apart workloads whose names differ only at the end, and
// name a DaemonSet's pods next to a Deployment's.
func TestLogTagsDistinctEnds(t *testing.T) {
	tags := logTags([][2]string{
		{"shop-backend-catalogue-consumer-inventory-6c9d8f7b5a-tnrk9", "app"},
		{"shop-backend-catalogue-consumer-carts-7d8e9f6a5b-rtvb9", "app"},
	})
	if tags[0] == tags[1] || !strings.Contains(tags[0], "inventory") || !strings.Contains(tags[1], "carts") {
		t.Errorf("consumers: %q", tags)
	}
	tags = logTags([][2]string{
		{"ebs-csi-controller-5d6f7b8c9d-tz2sx", "csi-attacher"},
		{"ebs-csi-controller-5d6f7b8c9d-tz2sx", "csi-provisioner"},
		{"ebs-csi-node-76v62", "ebs-plugin"},
	})
	if !strings.Contains(tags[0], "controller") || !strings.Contains(tags[2], "node") {
		t.Errorf("controller and node pods: %q", tags)
	}
}

// Search keeps pointing at the right lines as lines arrive and old ones go.
func TestRematchAfterTrim(t *testing.T) {
	p := &pager{search: "hit", lines: []string{"a", "hit 1", "b", "hit 2", "c"}}
	p.rematch(0)
	p.match = 1           // on "hit 2"
	p.lines = p.lines[2:] // two old lines dropped
	p.lines = append(p.lines, "hit 3")
	p.rematch(2)
	if len(p.matches) != 2 || p.lines[p.matches[p.match]] != "hit 2" {
		t.Errorf("matches %v, current %d", p.matches, p.match)
	}
}

// Coming back to a list that is still loading lands on the row you left
// once it is listed, not on whatever row is now at its place.
func TestBackWaitsForTheRowYouLeft(t *testing.T) {
	named := func(names ...string) []view.Row {
		var rows []view.Row
		for _, n := range names {
			rows = append(rows, view.Row{Obj: view.Named("argocd", n), Cells: []string{n}})
		}
		return rows
	}
	tb := table{}
	tb.reset()
	tb.cursor, tb.want = 1, rowID(named("argocd")[0])
	tb.setRows([]string{"APP"}, named("a", "argocd-dashboard"), nil) // argocd not listed yet
	tb.setRows([]string{"APP"}, named("a", "argocd", "argocd-dashboard"), nil)
	if r, _ := tb.selected(); r.Obj.GetName() != "argocd" {
		t.Errorf("cursor on %s, want argocd", r.Obj.GetName())
	}
	tb.move(1) // once found, moving works as usual
	if r, _ := tb.selected(); r.Obj.GetName() != "argocd-dashboard" || tb.want != "" {
		t.Errorf("after moving: %s, want %q", r.Obj.GetName(), tb.want)
	}
}

// Round 100: the menu's p agrees with p on the list: a failed job's app
// has no restarts but its box quotes the failed run, which p opens.
func TestMenuPreviousLogsForFailedRun(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locHome}
	m.apps = []*view.App{{Name: "r100fail", Namespace: "ns", CrashNS: "ns", CrashPod: "r100fail-x"}}
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Obj: view.Named("ns", "r100fail"), Cells: []string{"r100fail"}}}, nil)
	m.menu()
	for _, it := range m.prompt.items {
		if it.label == "Logs before the last restart" && it.disabled {
			t.Errorf("greyed: %q", it.about)
		}
	}
}

// Review of round 100: p on an app whose run never started says so, as l
// does, instead of opening an empty log.
func TestPreviousLogsNeverStarted(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locHome}
	m.apps = []*view.App{{Name: "r100ns", Namespace: "ns", CrashNS: "ns", CrashPod: "r100ns-x",
		Status: view.Status{Short: "Job failed", Why: "container \"main\" it never started: /x isn't in the image"}}}
	if cmd := m.appPreviousLogs(view.Row{Obj: view.Named("ns", "r100ns")}); cmd != nil || !strings.Contains(m.flash, "never started its program") {
		t.Errorf("flash %q", m.flash)
	}
}

// Round 101: node pools differ anywhere in their names, so the column
// shows each whole or drops out; it never shows a cut one.
func TestNodePoolWholeOrHidden(t *testing.T) {
	pools := []string{"nodepool-system-arm64", "nodepool-general-arm64", "nodepool-batch-arm64"}
	var rows []view.Row
	for i, p := range pools {
		rows = append(rows, view.Row{Cells: []string{fmt.Sprintf("ip-10-0-0-%d", i), "Ready", p, "t4g.xlarge"}})
	}
	header := []string{"NAME", "STATUS", "NODEPOOL", "TYPE"}
	tb := &table{header: header, optional: []string{"NODEPOOL"}, rows: rows}
	for _, width := range []int{60, 80, 100, 120, 200} {
		idx := tb.visibleColumns(width)
		shown := false
		for _, i := range idx {
			shown = shown || header[i] == "NODEPOOL"
		}
		if !shown {
			continue
		}
		var picked []view.Row
		for _, r := range rows {
			picked = append(picked, view.Row{Cells: pick(r.Cells, idx)})
		}
		ws := columnWidths(pick(header, idx), picked, width, -1)
		for j, i := range idx {
			if header[i] == "NODEPOOL" && ws[j] < len(pools[1]) {
				t.Errorf("width %d: NODEPOOL %d wide, cuts %q", width, ws[j], pools[1])
			}
		}
	}
}

// Review of round 100: when the box is about a program that never
// started, p still reaches another pod's earlier logs, but not the
// never-started pod's own restarts (they have none).
func TestRanBeforeRestart(t *testing.T) {
	cs := func(name, reason string, code, restarts int64) any {
		return map[string]any{"name": name, "restartCount": restarts,
			"lastState": map[string]any{"terminated": map[string]any{"reason": reason, "exitCode": code}}}
	}
	pod := func(name string, statuses ...any) view.Row {
		return view.Row{Obj: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": name, "namespace": "ns"},
			"status": map[string]any{"containerStatuses": statuses}}}}
	}
	// a: its app container never started (8 tries), its proxy crashed once; b crashed 3 times.
	src, ok := ranBeforeRestart([]view.Row{pod("a", cs("app", "StartError", 128, 8), cs("proxy", "Error", 1, 1)), pod("b", cs("web", "Error", 1, 3))})
	if !ok || src.pod != "b" || src.container != "web" {
		t.Errorf("%+v", src)
	}
	if _, ok := ranBeforeRestart([]view.Row{pod("a", cs("app", "StartError", 128, 8))}); ok {
		t.Error("only a never-started container")
	}
	if !neverStarted(&view.App{Status: view.Status{Short: "Can't start"}}) {
		t.Error("can't start")
	}
}

// Review of round 101: once a wide column (a whole node pool) is hidden,
// narrower ones hidden before it come back when they fit.
func TestHiddenColumnsComeBack(t *testing.T) {
	header := []string{"NAME", "STATUS", "NODEPOOL", "TYPE", "PRICING", "ZONE", "PODS", "CPU", "CPU %", "MEM", "MEM %", "COST/MO", "AGE"}
	row := view.Row{Cells: []string{"ip-10-0-12-34.ap-south-1.compute.internal", "Ready", "nodepool-general-arm64", "t4g.xlarge", "spot", "ap-south-1a", "23", "0.71", "37%", "1.1Gi", "79%", "$52", "3d"}}
	tb := &table{header: header, optional: []string{"AGE", "ZONE", "PRICING", "NODEPOOL", "CPU", "MEM", "PODS"}, rows: []view.Row{row}}
	got := strings.Join(pick(header, tb.visibleColumns(120)), " ")
	if !strings.Contains(got, "PRICING") || !strings.Contains(got, "ZONE") || strings.Contains(got, "NODEPOOL") {
		t.Errorf("120 wide: %s", got)
	}
}

// Review of round 101: on home, columns hidden before WHY don't come back
// while WHY is hidden (what is wrong outlasts restarts and dates).
func TestHomeHideOrderKept(t *testing.T) {
	header := []string{"APP", "NAMESPACE", "STATUS", "READY", "RESTARTS", "DEPLOYED", "COST/MO", "WHY"}
	rows := []view.Row{
		{Cells: []string{"payments", "payments", "Crashing", "1/3", "12", "2h ago", "$41", "container \"api\" keeps crashing: 12 restarts, the last 1m ago"}},
		{Cells: []string{"web", "web", "Running", "2/2", "0", "3d ago", "$12", "healthy"}},
	}
	tb := &table{header: header, optional: []string{"RESTARTS", "DEPLOYED", "WHY", "COST/MO", "NAMESPACE"}, main: "WHY", rows: rows}
	for w := 60; w <= 120; w++ {
		got := " " + strings.Join(pick(header, tb.visibleColumns(w)), " ") + " "
		if !strings.Contains(got, " WHY ") && (strings.Contains(got, " RESTARTS ") || strings.Contains(got, " DEPLOYED ")) {
			t.Fatalf("width %d: %s", w, got)
		}
	}
}

// Round 106: while a prompt waits for typed text, news from elsewhere (a
// port-forward moving on) doesn't hide what the prompt takes.
func TestTypingKeepsPromptHints(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 30
	m.mode = modeCommand
	m.setFlash("pod web-1 went away; forwarding again", nil)
	if got := strings.Join(m.footerLines(), "\n"); !strings.Contains(got, "tab complete") || strings.Contains(got, "went away") {
		t.Errorf("%q", got)
	}
}

// Round 111: a small screen keeps the Fix line (what to do) by cutting
// the wrapped tails of the lines above it first.
func TestFitDetailKeepsFix(t *testing.T) {
	detail := []string{
		"Problem  Can't pull image: c can't download 111122223333.dkr.ecr.ap-south-1",
		"         .amazonaws.com/docker-hub/library/nginx:no-such-tag: that image or",
		"         tag doesn't exist",
		"         (and more)",
		"Fix      enter → Its workloads, then i to fix the image name or tag",
		"Pods     0 of 1 ready",
	}
	got := strings.Join(fitDetail(detail, 4), "\n")
	if !strings.Contains(got, "Fix      enter") || !strings.Contains(got, "Problem  Can't pull image") || !strings.Contains(got, "d shows everything") {
		t.Errorf("%s", got)
	}
	short := fitDetail([]string{"Status   Running", "Images   web", "Pods     2 of 2 ready", "Cost     $3"}, 3) // no Fix: cut as before
	if len(short) != 3 || !strings.Contains(short[2], "d shows everything") {
		t.Errorf("%q", short)
	}
}

// Review of round 111: a Fix line whose first row fits but whose wrapped
// rest doesn't is kept whole too.
func TestFitDetailKeepsWrappedFix(t *testing.T) {
	detail := []string{"Problem  a", "         b", "Fix      do this", "         and that", "Pods     1"}
	got := strings.Join(fitDetail(detail, 4), "\n")
	if !strings.Contains(got, "and that") {
		t.Errorf("%s", got)
	}
}

// Round 112: on a short screen the box (as styled for the screen) keeps
// the Fix line when a long Problem would push it out.
func TestStyledBoxKeepsFix(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 80, 24
	long := "Problem  Can't pull image: badimg can't download 123456789012.dkr.ecr.ap-south-1.amazonaws.com/some-very-long-team-name/another-long-path-segment/service-image-that-does-not-exist:v9.9.9-nonexistent-tag: that image or tag doesn't exist"
	got := strings.Join(m.styleDetail([]string{long, "Fix      enter → Its workloads, then i to fix the image name or tag", "Pods     0 of 1 ready"}), "\n")
	if !strings.Contains(got, "Fix") || !strings.Contains(got, "then i to fix") || !strings.Contains(got, "d shows everything") {
		t.Errorf("%s", got)
	}
	if !strings.Contains(got, "doesn't exist") { // round 116: the cause, at the problem's end, stays when there is room
		t.Errorf("cause lost: %s", got)
	}
}

// Round 117: a problem longer than its four rows (a registry's whole 403
// reply) still ends with its cause, not with the row cut short.
func TestStyledBoxKeepsCauseOfLongProblem(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 80, 24
	long := "Problem  Can't pull image: app can't download 123456789012.dkr.ecr.ap-south-1.amazonaws.com/team/service:v1: " +
		strings.Repeat("failed to resolve reference with a long registry reply that goes on and on ", 5) + "; access denied by the registry"
	got := strings.Join(m.styleDetail([]string{long, "Fix      enter → Its workloads, then i to fix the image name or tag", "Pods     0 of 1 ready"}), "\n")
	if !strings.Contains(got, "access denied by the registry") || !strings.Contains(got, "then i to fix") {
		t.Errorf("cause lost: %s", got)
	}
	if n := strings.Count(got, "\n") + 1; n > 5 {
		t.Errorf("%d rows: %s", n, got)
	}
	for _, l := range strings.Split(got, "\n") {
		if w := len([]rune(ansi.Strip(l))); w > innerWidth(80) {
			t.Errorf("row %d wide: %q", w, l)
		}
	}
}

// Round 121: a group in :shipped read "(3 more) stuck"; it says it as
// :changes does, and a cut never hides the count.
func TestShippedGroupWording(t *testing.T) {
	if got := shippedWhat("deployment/stuck and 3 more", 44); got != "stuck (and 3 more)" {
		t.Errorf("%q", got)
	}
	long := shippedWhat("deployment/"+strings.Repeat("a-very-long-name-", 5)+" and 12 more", 44)
	if !strings.HasSuffix(long, " (and 12 more)") || len([]rune(long)) > 44 {
		t.Errorf("%q", long)
	}
	if got := shippedWhat("deployment/web", 44); got != "web" {
		t.Errorf("%q", got)
	}
	if full := shippedWhat("deployment/"+strings.Repeat("a-very-long-name-", 5)+" and 12 more", 0); !strings.HasPrefix(full, strings.Repeat("a-very-long-name-", 5)) {
		t.Errorf("export cut: %q", full) // the export keeps names whole
	}
}

// Round 123: a pod list opened from a search result, filtered to it: esc
// goes back to the results, and the footer says so (it said "Clear filter").
func TestFoundListEscHint(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.navigate(location{kind: locFind, query: "store-1"}, false)
	m.navigate(location{kind: locHome, fromFind: true}, true)
	m.tbl.filter = "store-1"
	esc := ""
	for _, h := range m.hints() {
		if h.key == "esc" {
			esc = h.label
		}
	}
	if esc != "Back to the results" {
		t.Errorf("esc says %q", esc)
	}
}

// Round 123: r on a paused deployment said a new pod starts before each old
// one stops; nothing restarts until it is resumed, and the prompt says so.
func TestRestartWhyPaused(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "spec": map[string]any{"replicas": int64(2), "paused": true}}}
	if why := restartWhy(dep); !strings.HasPrefix(why, "It is paused") {
		t.Errorf("paused: %q", why)
	}
	if !pausedDeploy(dep) {
		t.Error("paused deployment not seen as paused")
	}
	delete(dep.Object["spec"].(map[string]any), "paused")
	if why := restartWhy(dep); strings.Contains(why, "paused") {
		t.Errorf("not paused: %q", why)
	}
}

// Review of round 123: r on a paused deployment in the list said
// "restarted"; it restarts only once resumed.
func TestPausedRestartText(t *testing.T) {
	if got := pausedRestartText("restarted deployment web in shop", []string{"web"}, 1, "deployment web in shop"); strings.HasPrefix(got, "restarted") || !strings.Contains(got, "once it is resumed") {
		t.Errorf("one: %q", got)
	}
	if got := pausedRestartText("restarted 3 deployments", []string{"web"}, 3, ""); !strings.HasPrefix(got, "restarted 2 deployments; restart set for web") {
		t.Errorf("several: %q", got) // round 124: it said "restarted 2" when 1 of 2 was paused
	}
	if got := pausedRestartText("restarted 2 deployments", []string{"paused"}, 2, ""); !strings.HasPrefix(got, "restarted 1 deployment;") {
		t.Errorf("one of two: %q", got)
	}
	if got := pausedRestartText("restarted deployment web", nil, 1, "deployment web in shop"); got != "restarted deployment web" {
		t.Errorf("none: %q", got)
	}
	if got := pausedRestartText("restarted 2 deployments", []string{"a", "b"}, 2, ""); strings.HasPrefix(got, "restarted") {
		t.Errorf("all paused: %q", got)
	}
	if got := pausedRestartText("x", []string{"web"}, 1, "deployment web in shop"); !strings.Contains(got, "in shop") {
		t.Errorf("namespace: %q", got)
	}
}

// Round 124: / on search results ran a new search with the filter's text
// (the box is shared) and esc lost the results; it filters them instead.
func TestFilterOnFoundKeepsTheSearch(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.startFind()
	for _, r := range "oomonce-" {
		m.onFindKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m.showFound("oomonce-", 0) // the results, not typing (as after enter and esc back)
	m.startInput(modeFilter, "/", "")
	m.input.SetValue("web")
	m.refreshRows()
	if title := ansi.Strip(m.boxTitle()); !strings.Contains(title, `found for "oomonce-"`) {
		t.Errorf("title %q", title)
	}
	if m.loc.query != "oomonce-" {
		t.Errorf("query %q", m.loc.query)
	}
	// Review: filter kept with enter, a result opened, then esc back.
	m.mode = modeTable
	m.tbl.filter = "web"
	m.navigate(location{kind: locHome, fromFind: true}, true)
	m.back()
	if m.loc.kind != locFind || m.loc.query != "oomonce-" {
		t.Errorf("back on the results: %v %q", m.loc.kind, m.loc.query)
	}
}

// Round 125: a filter kept on search results hid every result of the next
// search (f again); a new search starts unfiltered.
func TestNewSearchDropsTheResultsFilter(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.startFind()
	for _, r := range "web" {
		m.onFindKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m.showFound("web", 0)
	m.tbl.filter = "qa125a"
	m.startFind() // f types a new search
	if m.tbl.filter != "" {
		t.Errorf("filter %q carried into the new search", m.tbl.filter)
	}
}

// Round 125: with rows picked, a one-row key (versions, logs, a port) used
// the row under the cursor without a word; it says it works on one row.
func TestOneRowKeysRefuseWithPicks(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.navigate(location{kind: locResource, res: kube.Resource{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}, false)
	a := action{key: "zz", label: "Look", run: func(*Model, []view.Row) tea.Cmd { t.Error("ran with picks"); return nil }}
	rows := []view.Row{{Obj: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "a", "uid": "a"}}}},
		{Obj: &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "b", "uid": "b"}}}}}
	m.tbl.setRows([]string{"NAME"}, rows, nil)
	m.tbl.markAll()
	m.runAction("zz", a)
	if !strings.Contains(m.flash, "one row at a time") {
		t.Errorf("flash %q", m.flash)
	}
}

// findTestModel is a testModel with pod web in kubed-qa125a, listed.
func findTestModel(t *testing.T) (*Model, dynamic.Interface) {
	t.Helper()
	var cluster dynamic.Interface
	m := testModelOn(func(d dynamic.Interface) dynamic.Interface { cluster = d; return d })
	m.width, m.height = 120, 40
	m.client.Cache.Watch(kube.Pods)
	addTestPod(t, m, cluster, "web", nil)
	return m, cluster
}

// addTestPod adds a running pod to the fake cluster and waits until the
// cache lists it.
func addTestPod(t *testing.T, m *Model, cluster dynamic.Interface, name string, labels map[string]any) {
	t.Helper()
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": name, "namespace": "kubed-qa125a", "uid": name, "labels": labels},
		"status":   map[string]any{"phase": "Running"}}}
	if _, err := cluster.Resource(kube.Pods).Namespace("kubed-qa125a").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, ok := m.client.Cache.Get(kube.Pods, "kubed-qa125a", name); ok && m.homeLoading() == "" {
			return
		}
	}
	t.Fatalf("pod %s never listed", name)
}

func typeFind(m *Model, s string) {
	for _, r := range s {
		m.onFindKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
}

// Round 125: after typing for a few seconds, each letter kept the cursor
// on the last search's top row (app web-frontend for "we"), so enter on
// "web" opened web-frontend, not the top row. A new search starts at its top.
func TestNewSearchStartsAtItsTop(t *testing.T) {
	m, _ := findTestModel(t)
	defer m.client.Cache.Stop()
	m.startFind()
	m.apps = []*view.App{{Name: "web-frontend", Namespace: "shop"}} // ranks above pod web for "we"
	m.tbl.openedAt = time.Now().Add(-4 * time.Second)               // typed slowly: the list has settled
	typeFind(m, "we")
	if r, _ := m.tbl.selected(); r.Obj.GetName() != "web-frontend" {
		t.Fatalf("for \"we\" the top row is %v", r.Cells)
	}
	m.tbl.openedAt = time.Now().Add(-4 * time.Second)
	typeFind(m, "b")
	if r, _ := m.tbl.selected(); m.tbl.cursor != 0 || r.Obj.GetName() != "web" {
		t.Errorf("cursor %d on %v, want the top row (pod web)", m.tbl.cursor, r.Cells)
	}
}

// Round 125: find showed the apps home last listed, so an app whose pods
// came since was missing until you went home. It looks again when a search
// starts and every second while on the results.
func TestFindSeesCurrentApps(t *testing.T) {
	m, cluster := findTestModel(t)
	defer m.client.Cache.Stop()
	addTestPod(t, m, cluster, "web-abc12", map[string]any{"app.kubernetes.io/name": "api"})
	hasApp := func(name string) bool {
		for _, r := range m.tbl.rows {
			if r.Cells[0] == "app" && r.Cells[1] == name {
				return true
			}
		}
		return false
	}
	m.startFind()
	typeFind(m, "api")
	if !hasApp("api") {
		t.Errorf("a search started without app api: %v", m.tbl.rows)
	}
	addTestPod(t, m, cluster, "shop-xyz34", map[string]any{"app.kubernetes.io/name": "apishop"})
	m.Update(tickMsg(time.Now()))
	if !hasApp("apishop") {
		t.Errorf("a second later, still no app apishop: %v", m.tbl.rows)
	}
}

// Round 125: esc back to the results put the cursor at the same place in
// the list, so when a row came above it meanwhile (app web, once home had
// looked), enter opened its neighbour. It goes back to the row you opened.
func TestBackToResultsReselectsTheRowOpened(t *testing.T) {
	m, _ := findTestModel(t)
	defer m.client.Cache.Stop()
	m.startFind()
	typeFind(m, "web")
	m.onFindKey(tea.KeyMsg{Type: tea.KeyEnter}) // opens pod web
	m.mode, m.prompt = modeTable, nil
	m.apps = []*view.App{{Name: "web", Namespace: "kubed-qa125a"}} // home looked meanwhile: app web ranks first
	m.back()
	if r, _ := m.tbl.selected(); m.loc.kind != locFind || r.Cells[0] != "pod" {
		t.Errorf("back on %v, not the pod opened", r.Cells)
	}
}

// Round 125: with rows picked, the menu offered one-row items that then
// refused; they show greyed, saying they work on one row.
func TestMenuGreysOneRowItemsWithPicks(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc = location{kind: locHome}
	m.apps = []*view.App{{Name: "a", Namespace: "ns", Pods: 1}, {Name: "b", Namespace: "ns", Pods: 1}}
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Obj: view.Named("ns", "a"), Cells: []string{"a"}}, {Obj: view.Named("ns", "b"), Cells: []string{"b"}}}, nil)
	off := func() map[string]bool {
		m.menu()
		got := map[string]bool{}
		for _, it := range m.prompt.items {
			got[it.label] = it.disabled && it.off == "one row"
		}
		return got
	}
	m.tbl.toggleMark() // only the row under the cursor: one-row items still work
	if got := off(); got["Logs (all pods)"] {
		t.Error("greyed with only the cursor row picked")
	}
	m.tbl.markAll()
	got := off()
	if !got["Logs (all pods)"] || !got["Events"] {
		t.Errorf("one-row items not greyed with picks: %v", got)
	}
	if !got["Open app"] { // round 126: it opens the cursor's row, not the picks
		t.Errorf("open not greyed with picks: %v", got)
	}
	if got["Copy name"] {
		t.Errorf("a key for every pick greyed: %v", got)
	}
}

// Review of round 125: w on a pods list shows the namespace whatever row, so
// picks don't block it; on home it is the app's, so they do.
func TestWhatChangedWithPicks(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	w := action{key: "w"}
	m.navigate(location{kind: locResource, res: kube.Resource{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}, false)
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Obj: &unstructured.Unstructured{Object: map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "a", "uid": "a"}}}}}, nil)
	if m.usesRow(w) {
		t.Error("w on pods is the namespace's: picks shouldn't block it")
	}
	m.loc.kind = locHome
	if !m.usesRow(w) {
		t.Error("w on home is the app's: picks should block it")
	}
}

// Round 126: a search result's menu said "esc Close" though esc goes back
// to the results.
func TestFoundMenuEscHint(t *testing.T) {
	for _, found := range []bool{true, false} {
		esc := ""
		for _, h := range promptHints(&prompt{kind: promptMenu, foundMenu: found}) {
			if h.key == "esc" {
				esc = h.label
			}
		}
		if want := map[bool]string{true: "Back to the results", false: "Close"}[found]; esc != want {
			t.Errorf("found %v: esc %q, want %q", found, esc, want)
		}
	}
}
