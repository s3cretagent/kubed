package ui

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/muesli/termenv"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"sigs.k8s.io/yaml"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// Round 65: the line a broken edit is sent back with names the bad line
// in the file as shown, two note lines and all.
func TestYAMLProblemNamesTheBadLine(t *testing.T) {
	for _, c := range []struct {
		text string
		bad  int // in the text as edited
	}{
		{"a: 1\nb:\n  c: 2\n  extra: world\n bad: x\nd: 3\n", 5},
		{"a: 1\nb: 2\nc d\ne: 4\n", 3},
		{"a: 1\nb:\n  - x\n  y: 2\n", 4},
		{"a: 1\nb: 2\n  c: 3\nd: 4\n", 3},
		{"apiVersion: v1\ndata:\n  greeting: [hello\nkind: ConfigMap\n", 3}, // round 67: an unclosed bracket
	} {
		_, err := yaml.YAMLToJSON([]byte(c.text))
		if err == nil {
			t.Fatalf("%q parsed", c.text)
		}
		got := yamlProblem(err.Error(), 2)
		if want := fmt.Sprintf("around line %d", c.bad+2); !strings.Contains(got, want) {
			t.Errorf("%q: %q, want %q", c.text, got, want)
		}
	}
}

// Round 65: a cron job's logs end with its run; the next run's pod is
// still noticed, so you know r has something to read.
func TestNewPodNoticedAfterLogsEnded(t *testing.T) {
	m := testModel()
	m.mode = modePager
	m.pager = &pager{isLogs: true, ended: true, source: logSource{
		streams: [][2]string{{"slow-1", "c"}},
		pods:    func() ([][2]string, int) { return [][2]string{{"slow-2", "c"}}, 1 },
	}}
	m.noticeNewPods()
	if got := strings.Join(m.pager.lines, "\n"); !strings.Contains(got, "new pod slow-2 started. r reads its logs") {
		t.Errorf("not noticed: %q", got)
	}
}

// Round 67: kubed's own notes in logs ("no more lines…") aren't copied,
// saved or counted as the app's lines.
func TestLogNotesAreNotLogLines(t *testing.T) {
	p := &pager{isLogs: true}
	p.append([]string{"starting", "ERROR boom"})
	p.note("(no more lines: the log stream ended. r reads it again)")
	if got := p.copyText(); got != "starting\nERROR boom\n" {
		t.Errorf("copied %q", got)
	}
	if got := p.plainText(); strings.Contains(got, "no more lines") {
		t.Errorf("saved %q", got)
	}
	if n := len(p.logLines(p.lines)); n != 2 {
		t.Errorf("counted %d lines, want 2", n)
	}
}

// Round 68: the stream's own "kubed: couldn't reach the logs" line, after
// a pod's tag, is kubed's too.
func TestStreamedKubedLineIsANote(t *testing.T) {
	p := &pager{isLogs: true}
	p.append([]string{"znx65 │ GET / 200", "znx65 │ kubed: couldn't reach the logs (the network dropped); r tries again"})
	if n := len(p.logLines(p.lines)); n != 1 {
		t.Errorf("counted %d lines, want 1", n)
	}
	if got := p.copyText(); strings.Contains(got, "couldn't reach") {
		t.Errorf("copied %q", got)
	}
}

// Round 68: a search result opens with its menu; one esc goes back to the
// results, as the guide says.
func TestEscOnFoundMenuGoesBackToResults(t *testing.T) {
	m := testModel()
	m.width, m.height = 120, 40
	res, _ := m.client.Resources.ByGVR(kube.Pods)
	m.navigate(location{kind: locHome}, false)
	m.navigate(location{kind: locResource, res: res}, true)
	m.navigate(location{kind: locFind}, true)
	m.navigate(location{kind: locHome, fromFind: true}, true)
	m.prompt = menuPrompt("app crasher", []menuItem{{label: "Its pods"}})
	m.prompt.foundMenu = true
	m.mode = modePrompt
	m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if m.loc.kind != locFind {
		t.Errorf("one esc left us on %v, want the results", m.loc.kind)
	}
}

// Round 78: copied and saved logs say which part of the logs they hold.
func TestLogsSayWhereTheyStart(t *testing.T) {
	for src, want := range map[*logSource]string{
		{}: "the last 500 lines, and newer",
		{workload: "app/web", streams: [][2]string{{"a", "c"}, {"b", "c"}}}: "the last 100 lines of each container, and newer",
		{previous: true}: "the last 500 lines of the run before the restart",
	} {
		if got := src.readFrom(); got != want {
			t.Errorf("%+v: %q, want %q", *src, got, want)
		}
	}
	p := &pager{isLogs: true}
	p.append([]string{"x"})
	m := testModel()
	m.mode, m.pager = modePager, p
	m.width, m.height = 100, 30
	m.pager.resize(100, 20)
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'C'}})
	if !strings.Contains(m.flash, "the last 500 lines") || copied != "x\n" {
		t.Errorf("C said %q and copied %q", m.flash, copied)
	}
}

// Round 80: an open menu fits the window again when it is resized.
func TestMenuRefitsOnResize(t *testing.T) {
	m := testModel()
	m.width, m.height = 200, 50
	m.prompt = menuPrompt("app web", []menuItem{{label: "a"}, {label: "b"}})
	m.prompt.maxItems = m.menuRoom()
	big := m.prompt.maxItems
	m.mode = modePrompt
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	if m.prompt.maxItems >= big || m.prompt.maxItems != m.menuRoom() {
		t.Errorf("room %d after shrinking (was %d, fits %d)", m.prompt.maxItems, big, m.menuRoom())
	}
}

// Review of round 80: an action waiting for a list (v, u on a stateful
// set) is dropped when you leave or a text screen takes over; it never
// opens over the next screen, and a text screen on its way is still
// dropped when you leave. Something unrelated changing the footer
// doesn't drop it.
func TestWaitedActionDroppedWhenYouLeave(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	ran := false
	run := func() tea.Cmd { ran = true; return nil }
	wait := func(id int) {
		m.busy, m.loadSeq, m.loadWaiting, m.loadBusy, m.loadOwnsBusy = "loading db's versions", id, id, "loading db's versions", true
		m.loadWhere = m.whereNow()
	}
	wait(1)
	m.navigate(location{kind: locFind}, true) // you go elsewhere
	m.Update(loadedMsg{1, run})
	if ran || m.dropPager || m.busy != "" {
		t.Errorf("left: ran %v, dropPager %v, busy %q", ran, m.dropPager, m.busy)
	}
	wait(2) // you stay: it runs, even if a scale's result cleared the footer meanwhile
	m.busy = ""
	m.Update(loadedMsg{2, run})
	if !ran {
		t.Error("didn't run when you stayed")
	}
	// A describe started while it waited, then esc: the describe is
	// dropped too, and the versions never open.
	ran = false
	wait(3)
	m.busy = "describing web"
	m.navigate(location{kind: locHome}, true)
	if !m.dropPager {
		t.Error("the describe on its way wasn't dropped")
	}
	m.Update(loadedMsg{3, run})
	if ran {
		t.Error("the versions opened after you left")
	}
	// A prompt opened meanwhile (s on another row, : or /): the versions
	// never replace it.
	ran = false
	wait(5)
	m.prompt = inputPrompt("How many copies of db should run? (now 3)", "3", func(string) tea.Cmd { return nil })
	m.mode = modePrompt
	m.Update(loadedMsg{5, run})
	if ran {
		t.Error("the versions replaced a prompt you opened meanwhile")
	}
	m.prompt, m.mode = nil, modeTable
	// The describe arrives first: the versions don't open over it.
	m.dropPager = false
	wait(4)
	m.openPager(newPager("describe web", "x", 80, 20))
	m.Update(loadedMsg{4, run})
	if ran {
		t.Error("the versions opened over the describe")
	}
}

// Round 81: a version brought back, its exact time no longer kept, is
// dated by the version made before it, as w does, not left undated.
func TestVersionWhenBoundsAGoBack(t *testing.T) {
	now := time.Now()
	v3 := &enrich.Version{Revision: 3, Reused: true, Created: now.Add(-8 * time.Minute)}
	v2 := &enrich.Version{Revision: 2, Created: now.Add(-4 * time.Minute)}
	made, ago, unknown := versionWhen(v3, []*enrich.Version{v3, v2})
	if made != "back" || ago != "≤4m ago" || unknown {
		t.Errorf("%q %q %v", made, ago, unknown)
	}
	v3.LiveSince = now.Add(-2 * time.Minute)
	if made, ago, _ := versionWhen(v3, []*enrich.Version{v3, v2}); made != "back" || ago != "2m ago" {
		t.Errorf("known time: %q %q", made, ago)
	}
}

// Round 124: the bound is the one enrich works out (BackAfter), the same
// what changed and Deployed say, not one guessed again from the numbers.
func TestVersionWhenSaysEnrichsBound(t *testing.T) {
	now := time.Now()
	v6 := &enrich.Version{Revision: 6, Reused: true, Created: now.Add(-6 * time.Minute), BackAfter: now.Add(-5 * time.Minute)}
	v5 := &enrich.Version{Revision: 5, Created: now.Add(-3 * time.Minute)}
	if made, ago, unknown := versionWhen(v6, []*enrich.Version{v6, v5}); made != "back" || ago != "≤5m ago" || unknown {
		t.Errorf("%q %q %v", made, ago, unknown)
	}
}

// Review of round 81 (fourth pass): a text screen taking over from a
// wait takes its "loading …" line too, so it can't later drop an
// unrelated describe.
func TestOpenPagerClearsTheWaitsLine(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.busy, m.loadSeq, m.loadWaiting, m.loadBusy, m.loadOwnsBusy = "loading web's versions", 1, 1, "loading web's versions", true
	m.openPager(newPager("logs of web", "x", 80, 20))
	if m.busy != "" || m.loadWaiting != 0 {
		t.Errorf("busy %q, waiting %d", m.busy, m.loadWaiting)
	}
	m.navigate(location{kind: locHome}, true)
	if m.dropPager {
		t.Error("the next describe would be dropped")
	}
}

// Fifth pass: a second v while the first waits keeps the footer its own
// (no "loading" stuck after), and help doesn't cancel what waits.
func TestSecondWaitAndHelp(t *testing.T) {
	m := waitModel()
	m.width, m.height = 100, 30
	m.busy, m.loadSeq, m.loadWaiting, m.loadBusy, m.loadOwnsBusy = "loading web's versions", 1, 1, "loading web's versions", true
	m.client.Cache.Watch(kube.Pods)
	m.whenLoaded(kube.Nodes, "web's versions", func() tea.Cmd { return nil }) // a second wait (Nodes isn't loaded in tests)
	if !m.loadOwnsBusy || m.loadWaiting != 2 {
		t.Fatalf("second wait: owns %v, waiting %d", m.loadOwnsBusy, m.loadWaiting)
	}
	m.Update(loadedMsg{2, func() tea.Cmd { return nil }})
	if m.busy != "" {
		t.Errorf("stuck footer %q", m.busy)
	}
	m.busy, m.loadSeq, m.loadWaiting, m.loadBusy, m.loadOwnsBusy = "loading web's versions", 3, 3, "loading web's versions", true
	m.openPager(newPager("help", "x", 80, 20))
	if m.loadWaiting != 3 || m.pager == nil || m.pager.title != "help" {
		t.Errorf("help cancelled the wait (%d) or didn't open", m.loadWaiting)
	}
}

// Review of round 81: t shows or hides Kubernetes' times without reading
// the logs again, so your search and the match you are on stay; copies
// and saves follow what is shown.
func TestTimestampsShownOrHiddenInPlace(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	p := newPager("logs", "", 80, 10)
	p.isLogs, p.lines = true, nil // as openLogs starts them
	p.append([]string{"2026-10-14T00:20:01.5Z x refused", "2026-10-14T00:20:02.5Z y", "2026-10-14T00:20:03.5Z x refused"})
	p.setSearch("refused")
	p.nextMatch(1)
	m.mode, m.pager = modePager, p
	if got := p.copyText(); strings.Contains(got, "2026-10-14T") {
		t.Errorf("times copied while hidden: %q", got)
	}
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	if m.pager != p || p.match != 1 || p.search != "refused" || !p.source.timestamps {
		t.Errorf("t: same pager %v, match %d, search %q, times %v", m.pager == p, p.match, p.search, p.source.timestamps)
	}
	if got := p.copyText(); !strings.Contains(got, "2026-10-14T00:20:03.5Z x refused") {
		t.Errorf("times not copied while shown: %q", got)
	}
}

// Errors only keeps a stack trace with its error when lines carry times.
func TestErrorsOnlyWithTimes(t *testing.T) {
	p := newPager("logs", "", 80, 10)
	p.isLogs = true
	p.append([]string{"2026-10-14T00:20:01.5Z ERROR boom", "2026-10-14T00:20:01.5Z \tat Foo.bar", "2026-10-14T00:20:02.5Z fine"})
	p.errorsOnly = true
	if n := len(p.shown()); n != 2 {
		t.Errorf("errors only kept %d lines, want the error and its frame", n)
	}
}

// Final review: once older lines were left out, copies and saves say
// they hold only the newest ones.
func TestHeldWordsAfterDropping(t *testing.T) {
	p := &pager{isLogs: true}
	if got := p.heldWords(); got != "the last 500 lines, and newer" {
		t.Errorf("%q", got)
	}
	p.dropped = true
	if got := p.heldWords(); got != fmt.Sprintf("the last 500 lines, and newer; only the newest %d lines are kept", maxLogLines) {
		t.Errorf("%q", got)
	}
}

// Review of the simplification: with times hidden, search sees only what
// is shown (no match in a hidden time), tabs line up as without times,
// and t keeps the line you read at the top.
func TestHiddenTimesAreNotSearchedAndTabsLineUp(t *testing.T) {
	p := newPager("logs", "", 80, 5)
	p.isLogs, p.lines = true, nil // as openLogs starts them
	p.append([]string{"2026-10-14T00:20:01.5004321Z GET / 200", "2026-10-14T00:20:02.1Z GET /x 500", "2026-10-14T00:20:03.12Z \tat Foo.bar"})
	p.setSearch("500")
	if len(p.matches) != 1 {
		t.Errorf("searched the hidden time: %d matches for 500", len(p.matches))
	}
	if got := p.lines[2]; got != "        at Foo.bar" {
		t.Errorf("tab measured with the hidden time: %q", got)
	}
	p.toggleTimes()
	if !strings.HasPrefix(p.lines[0], "2026-10-14T00:20:01") || p.search != "500" {
		t.Errorf("t on: %q", p.lines[0])
	}
	p.toggleTimes()
	if strings.HasPrefix(p.lines[0], "2026") {
		t.Errorf("t off again: %q", p.lines[0])
	}
}

// A v picked from the Enter menu that waits for its list still runs once
// the menu has closed.
func TestWaitFromMenuStillRuns(t *testing.T) {
	m := waitModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	m.prompt = menuPrompt("db", []menuItem{{label: "Versions"}})
	m.mode = modePrompt
	ran := false
	m.whenLoaded(kube.Nodes, "db's versions", func() tea.Cmd { ran = true; return nil }) // Nodes never loads in tests
	m.prompt, m.mode = nil, modeTable                                                    // the menu closes
	m.Update(loadedMsg{m.loadWaiting, func() tea.Cmd { ran = true; return nil }})
	if !ran {
		t.Error("the waited action picked from the menu was dropped")
	}
}

// Round 82: following the end with a search on, the current match is the
// newest (n doesn't jump to the oldest).
func TestFollowingKeepsTheNewestMatch(t *testing.T) {
	p := newPager("logs", "", 80, 5)
	p.isLogs, p.lines, p.search, p.follow = true, nil, "err", true
	p.append([]string{"err 1", "ok", "err 2"})
	p.append([]string{"err 3"})
	if p.match != 2 {
		t.Errorf("on match %d of %d", p.match+1, len(p.matches))
	}
}

// Round 82: a describe asked for while v waits comes; the versions don't.
// And a list loaded while help is open runs when help closes.
func TestWaitGivesWayAndWaitsForHelp(t *testing.T) {
	m := waitModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	ran := false
	run := func() tea.Cmd { ran = true; return nil }
	m.whenLoaded(kube.Nodes, "db's versions", run)
	m.busy = "describing db" // d meanwhile
	m.Update(loadedMsg{m.loadWaiting, run})
	if ran {
		t.Error("the versions opened instead of the describe")
	}
	m.busy = ""
	m.whenLoaded(kube.Nodes, "db's versions", run)
	m.openPager(newPager("help", "x", 80, 20)) // ? meanwhile
	m.Update(loadedMsg{m.loadWaiting, run})
	if ran {
		t.Fatal("the versions opened over help")
	}
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyEsc}) // close help
	if !ran {
		t.Error("the versions didn't open once help closed")
	}
}

// Review: leaving help for elsewhere (:) drops what waited behind it; it
// never pops up later.
func TestHeldActionDroppedWhenHelpLeft(t *testing.T) {
	m := waitModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	ran := false
	run := func() tea.Cmd { ran = true; return nil }
	m.whenLoaded(kube.Nodes, "db's versions", run)
	m.openPager(newPager("help", "x", 80, 20))
	m.Update(loadedMsg{m.loadWaiting, run})
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	if m.loadPending != nil {
		t.Error("still held after leaving help with :")
	}
	m.mode = modeTable
	m.openPager(newPager("help", "x", 80, 20))
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyEsc})
	if ran {
		t.Error("it popped up later")
	}
}

// Review: the "loaded after you moved on" note names what was waited for,
// even when another action's line was in the footer.
func TestMovedOnNoteNamesTheWait(t *testing.T) {
	m := waitModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	m.busy = "applying your edit to statefulset web"
	m.whenLoaded(kube.Nodes, "web's versions", func() tea.Cmd { return nil })
	m.busy = ""
	m.mode = modeFilter // / meanwhile
	m.Update(loadedMsg{m.loadWaiting, func() tea.Cmd { return nil }})
	if !strings.Contains(m.flash, "web's versions loaded after you moved on") {
		t.Errorf("flash %q", m.flash)
	}
}

// Round 83: one pod's lines with "│" in them are the app's own: copies
// keep them, times show as times, and a stack frame lines up the same
// with times on or off.
func TestOnePodPipesAndTabs(t *testing.T) {
	p := newPager("logs", "", 80, 10)
	p.isLogs, p.lines = true, nil
	p.source.streams = [][2]string{{"solo", "main"}} // one pod: no tags
	p.append([]string{"2026-10-14T04:11:24.616994423Z solo line 1 pipe │ here │ ok", "2026-10-14T04:11:25.1Z \tat com.example.Solo.run"})
	if got := p.copyText(); !strings.Contains(got, "solo line 1 pipe │ here │ ok") {
		t.Errorf("copied %q", got)
	}
	p.toggleTimes()
	if got := prettyLine(p.lines[0], 0); strings.Contains(got, "2026-10-14T") {
		t.Errorf("raw time shown: %q", got)
	}
	if !strings.HasSuffix(p.lines[1], "Z         at com.example.Solo.run") {
		t.Errorf("frame with times on: %q", p.lines[1])
	}
}

// Review: one pod's "[name] " label is still read past, so logfmt after
// it is tidied.
func TestOnePodBracketLabelTidied(t *testing.T) {
	if got := ansi.Strip(prettyLine("[worker-1] level=info msg=hello", bracketTagEnd("[worker-1] level=info msg=hello"))); !strings.Contains(got, "hello") || strings.Contains(got, "msg=") {
		t.Errorf("%q", got)
	}
}

// Round 84: a "[name]" label after kubed's pod tag is read past too, with
// times off, so logfmt after it is tidied.
func TestTaggedBracketLabelTidied(t *testing.T) {
	p := newPager("logs", "", 120, 5)
	p.isLogs, p.lines, p.tagNames = true, nil, map[string]string{"czrft": "ld-1/main"}
	p.append([]string{"czrft │ 2026-10-14T04:37:25.1Z [worker-1] level=info msg=hello user=bob"})
	if got := ansi.Strip(p.vp.View()); strings.Contains(got, "msg=hello") || !strings.Contains(got, "hello") {
		t.Errorf("%q", got)
	}
}

// Round 84: a key that works on a found row says to open it first.
func TestKeyOnFoundRowSaysOpenFirst(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locFind}, false)
	m.mode = modeTable
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	if !strings.Contains(m.flash, "enter opens the result") {
		t.Errorf("flash %q", m.flash)
	}
}

// Round 84: :pf lists forwards by local port whatever each is doing, so a
// forward moving to another pod keeps its row (and the cursor on it).
func TestForwardRowsKeepTheirPlace(t *testing.T) {
	m := testModel()
	m.stoppedForwards = []stoppedForward{{port: 18485, podPort: 80, target: "deployment web", why: "x", at: time.Now()},
		{port: 18484, podPort: 80, target: "deployment web", why: "y", at: time.Now()}}
	rows := m.forwardRows()
	if len(rows) != 2 || rows[0].Obj.GetName() != "18484" {
		t.Errorf("order %v", []string{rows[0].Obj.GetName(), rows[1].Obj.GetName()})
	}
}

// Round 85: the row you chose, gone for a moment (a forward moving to
// another pod), gets the cursor back when it is listed again.
func TestCursorComesBackToARowGoneForAMoment(t *testing.T) {
	tb := &table{}
	rows := func(names ...string) []view.Row {
		var out []view.Row
		for _, n := range names {
			out = append(out, view.Row{Obj: view.Named("", n), Cells: []string{n}})
		}
		return out
	}
	hdr := []string{"LOCAL"}
	tb.setRows(hdr, rows("18530", "18531"), nil)
	tb.move(1)                          // on 18531
	tb.setRows(hdr, rows("18530"), nil) // 18531 moving
	tb.setRows(hdr, rows("18530", "18531"), nil)
	if r, _ := tb.selected(); r.Obj.GetName() != "18531" {
		t.Errorf("cursor on %s", r.Obj.GetName())
	}
}

// Round 85: a forward whose connections failed for some reason other than
// the pod refusing them (a dropped network) connects again, and when it
// can't, the message doesn't blame the port.
func TestForwardDropNotBlamedOnThePort(t *testing.T) {
	f := &kube.Forward{Started: time.Now().Add(-time.Minute), Again: func() (*kube.Forward, error) { return nil, nil }}
	_, _ = f.Write([]byte("an error occurred forwarding 18530 -> 80: error forwarding port 80 to pod x: read: i/o timeout"))
	if !follows(f, false) {
		t.Error("a timed-out connection should go on through a pod")
	}
	if hint, note := forwardStopWords(f.Failing(), 80); strings.Contains(hint+note, "listening") {
		t.Errorf("blames the port: %q %q", hint, note)
	}
	_, _ = f.Write([]byte("error forwarding port 80 to pod x, uid : dial tcp4 127.0.0.1:80: connect: connection refused"))
	if follows(f, false) {
		t.Error("a refused connection shouldn't start again")
	}
	if hint, _ := forwardStopWords(f.Failing(), 80); !strings.Contains(hint, "listening on port 80") {
		t.Errorf("hint %q", hint)
	}
}

type versionStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s versionStore) List(g schema.GroupVersionResource, _ string) []*unstructured.Unstructured {
	return s[g]
}
func (s versionStore) Get(g schema.GroupVersionResource, _, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s[g] {
		if o.GetName() == name {
			return o, true
		}
	}
	return nil, false
}
func (versionStore) Synced(schema.GroupVersionResource) bool { return true }

// Round 85: the ages in an open versions list kept the time it opened.
func TestVersionAgesMoveOnWhileOpen(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d1"}, "spec": map[string]any{"replicas": int64(1)}}}
	rs := func(name string, rev int, made time.Time, image string) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": fmt.Sprint(rev)},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "d1", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}}}}
		o.SetCreationTimestamp(metav1.NewTime(made))
		return o
	}
	start := time.Now().Truncate(time.Second)
	store := versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		rs("web-2", 2, start.Add(-10*time.Second), "nginx:2"), rs("web-1", 1, start.Add(-20*time.Second), "nginx:1")}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(store, res, nil)
	m.versionsNow(view.Row{Obj: dep})
	if m.prompt == nil || len(m.prompt.items) != 2 {
		t.Fatalf("prompt %+v", m.prompt)
	}
	before := m.prompt.items[1].label
	time.Sleep(time.Until(start.Add(2 * time.Second)))
	m.prompt.tick()
	after := m.prompt.items[1].label
	if !strings.Contains(before, "made  20s ago") || after != strings.Replace(before, "20s ago", "22s ago", 1) {
		t.Errorf("before %q after %q", before, after)
	}
}

// Review of round 85: a row gone for a moment pulls the cursor back only
// while nothing else changed; meanwhile the cursor follows its row.
func TestGoneRowGivesWay(t *testing.T) {
	rows := func(names ...string) []view.Row {
		var out []view.Row
		for _, n := range names {
			out = append(out, view.Row{Obj: view.Named("", n), Cells: []string{n}})
		}
		return out
	}
	hdr := []string{"NAME"}
	on := func(tb *table) string { r, _ := tb.selected(); return r.Obj.GetName() }
	start := func() *table {
		tb := &table{}
		tb.setRows(hdr, rows("a", "b", "c"), nil)
		tb.move(2)                           // on c
		tb.setRows(hdr, rows("a", "b"), nil) // c gone: the cursor is on b
		return tb
	}

	tb := start()
	tb.setRows(hdr, rows("x", "a", "b"), nil) // a new row sorts in: still on b
	if on(tb) != "b" {
		t.Errorf("followed to %s, not b", on(tb))
	}

	tb = start()
	tb.move(-1) // you moved to a
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	if on(tb) != "a" {
		t.Errorf("after a move, pulled to %s", on(tb))
	}

	tb = start()
	tb.filter = "b" // you filtered meanwhile
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	tb.filter = ""
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	if on(tb) != "b" {
		t.Errorf("after a filter, pulled to %s", on(tb))
	}

	tb = start()
	tb.back.until = time.Now().Add(-time.Second) // long gone
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	if on(tb) != "b" {
		t.Errorf("after it expired, pulled to %s", on(tb))
	}
}

// Review: G (or any jump) after a row went away keeps the cursor where you
// put it, even when that is the same place.
func TestJumpForgetsGoneRow(t *testing.T) {
	rows := func(names ...string) []view.Row {
		var out []view.Row
		for _, n := range names {
			out = append(out, view.Row{Obj: view.Named("", n), Cells: []string{n}})
		}
		return out
	}
	hdr := []string{"NAME"}
	tb := &table{}
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	tb.move(2)
	tb.setRows(hdr, rows("a", "b"), nil) // c gone: on b
	tb.jump(len(tb.rows) - 1)            // G: still b
	tb.setRows(hdr, rows("a", "b", "c"), nil)
	if r, _ := tb.selected(); r.Obj.GetName() != "b" {
		t.Errorf("pulled to %s", r.Obj.GetName())
	}
}

// Round 86: a StatefulSet brings its pod back under the same name; the
// forward's pod is still gone, so it moves on instead of blaming the network.
func TestForwardPodReplacedUnderSameName(t *testing.T) {
	f := &kube.Forward{Pod: "db-0", PodUID: "old"}
	p := &unstructured.Unstructured{}
	p.SetName("db-0")
	p.SetUID("new")
	if !podGoneFor(f, p, true) {
		t.Error("a new db-0 counted as the forward's pod")
	}
	p.SetUID("old")
	if podGoneFor(f, p, true) {
		t.Error("the same pod counted as gone")
	}
	if !podGoneFor(f, nil, false) {
		t.Error("an unlisted pod counted as there")
	}
}

// Round 86: searching logs skips kubed's own notes ("── end of the run: it
// exited with an error"), so the count and c are about the app's lines.
func TestLogSearchSkipsKubedNotes(t *testing.T) {
	p := newPager("logs", "", 80, 10)
	p.isLogs, p.lines = true, nil
	p.append([]string{"error: bad config value"})
	p.note("── end of the run: it exited with an error (exit 3)")
	p.setSearch("error")
	if len(p.matches) != 1 {
		t.Errorf("%d matches, want 1", len(p.matches))
	}
	p.append([]string{"another error"})
	p.rematch(0)
	if len(p.matches) != 2 {
		t.Errorf("after more lines: %d matches, want 2", len(p.matches))
	}
}

// Review of round 86: kubed's notes, no longer searched, aren't marked
// as matches either.
func TestLogSearchDoesNotMarkNotes(t *testing.T) {
	was := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.TrueColor)
	defer lipgloss.SetColorProfile(was)
	p := newPager("logs", "", 100, 10)
	p.isLogs, p.raw, p.lines = true, true, nil
	p.append([]string{"error: bad config value"})
	p.note("── end of the run: it exited with an error (exit 3)")
	p.setSearch("error")
	p.refresh()
	mark := styleMatch.Render("error")
	got := strings.Split(p.vp.View(), "\n")
	if len(got) < 2 || !strings.Contains(got[0], mark) || strings.Contains(got[1], mark) {
		t.Errorf("lines %q", got[:min(2, len(got))])
	}
}

// Round 87: :waste listing nothing said "only a moment watched, too short
// to act on" after ten minutes; with nothing listed there is nothing to act on.
func TestWasteEmptyNotEarly(t *testing.T) {
	m := testModel()
	if m.wasteEarly() {
		t.Error("nothing listed, still warned")
	}
	m.wastes = []enrich.Waste{{}}
	if !m.wasteEarly() {
		t.Error("a row not yet timed should still warn")
	}
}

// Round 87: a failed Job's pod read "Running" until jobs loaded. Home
// waits for the list of a kind that owns pods here.
func TestHomeWaitsForPodOwners(t *testing.T) {
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true, Singular: "pod"},
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true, Singular: "job"},
	})
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "failjob-x", "namespace": "ns", "ownerReferences": []any{
			map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "failjob", "uid": "j1", "controller": true}}}}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Jobs: "JobList"}, pod)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(slowList{dyn, kube.Jobs})}, connectedAt: time.Now()}
	m.enr = enrich.New(m.client.Cache, res, nil)
	m.client.Cache.Watch(kube.Pods)
	m.client.Cache.Watch(kube.Jobs)
	for deadline := time.Now().Add(5 * time.Second); !m.client.Cache.Synced(kube.Pods) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.ownersLoading(); len(got) != 1 || got[0] != "jobs …" {
		t.Errorf("ownersLoading() = %v", got)
	}
}

// Round 87: a forward's connection can drop before kubed hears its pod is
// being deleted. It waits a moment to tell the two apart (except when the
// pod refused the connection), so it moves instead of blaming the network.
func TestEndedForwardWaitsToLearnWhy(t *testing.T) {
	if !holdEnded(false, "", time.Second) {
		t.Error("pod looks up a second after: should wait")
	}
	if holdEnded(true, "", 0) {
		t.Error("pod gone: no need to wait")
	}
	if holdEnded(false, "connection refused", 0) {
		t.Error("refused: no need to wait")
	}
	if holdEnded(false, "", 4*time.Second) {
		t.Error("waited long enough")
	}
}

// Review of round 87: forwards waiting to learn why they ended belong to
// the cluster you leave; switching drops them, so none starts in the new one.
func TestSwitchDropsHeldForwards(t *testing.T) {
	m := testModel()
	m.forwards = &kube.Forwards{}
	m.kc = kube.LoadKubeconfig(filepath.Join(t.TempDir(), "none")) // no clusters: nothing real is reached
	m.endedHeld = []endedForward{{f: &kube.Forward{LocalPort: 18986}, at: time.Now()}}
	m.switchContext("other")
	if len(m.endedHeld) != 0 {
		t.Error("held forward kept across a cluster switch")
	}
}

// Round 88: a forward to a service goes on through a pod of it whenever
// its connection ends (a pod deleted, two in a row, the only pod
// replaced), except one that keeps ending at once.
func TestForwardFollowsUnlessItKeepsEnding(t *testing.T) {
	f := &kube.Forward{Again: func() (*kube.Forward, error) { return nil, nil }}
	if !follows(f, false) {
		t.Error("should follow")
	}
	f.Moves = 3
	if follows(f, false) {
		t.Error("ended at once three times in a row: should stop")
	}
	if follows(&kube.Forward{}, true) {
		t.Error("a forward to one pod has nothing to follow")
	}
}

// Round 88: while a forward finds another pod (up to 30s for a single
// pod's replacement), :pf says so instead of losing the row.
func TestMovingForwardListed(t *testing.T) {
	m := testModel()
	m.forwards = &kube.Forwards{}
	m.moving = map[int]*kube.Forward{18081: {LocalPort: 18081, PodPort: 80, Target: "service web"}}
	rows := m.forwardRows()
	if len(rows) != 1 || !strings.Contains(strings.Join(rows[0].Cells, " "), "Connecting again through a pod of service web") {
		t.Errorf("rows %v", rows)
	}
	if !m.portTaken(18081) {
		t.Error("its port counts as taken while it moves")
	}
}

// Review of round 88: a pod going away in a rollout often refuses before
// it goes; its forward still follows. One you stopped while it moved
// doesn't come back as "stopped", and F can't take its port meanwhile.
func TestForwardMoveEdges(t *testing.T) {
	f := &kube.Forward{LocalPort: 18081, Target: "service web", Again: func() (*kube.Forward, error) { return nil, nil }}
	_, _ = f.Write([]byte("dial tcp4 127.0.0.1:80: connect: connection refused"))
	if !follows(f, true) {
		t.Error("its pod went away: should follow even after a refusal")
	}
	if follows(f, false) {
		t.Error("its pod is up and refused: nothing listens")
	}

	m := testModel()
	m.forwards = &kube.Forwards{}
	m.moving = map[int]*kube.Forward{18081: f}
	if err := m.portMoving(18081); err == nil || !strings.Contains(err.Error(), "connecting again through a pod") {
		t.Errorf("F on a moving port: %v", err)
	}
	delete(m.moving, 18081) // ctrl-d
	m.Update(resultMsg{moved: f, err: fmt.Errorf("no pod ran"), stopped: &stoppedForward{port: 18081}})
	if len(m.stoppedForwards) != 0 || m.flashErr {
		t.Errorf("a forward you stopped came back: %v", m.stoppedForwards)
	}
}

// Review of round 88: a move you stopped stops looking; F on its port
// waits until it has; what it found is never followed again.
func TestStoppedMoveStaysStopped(t *testing.T) {
	m := testModel()
	m.forwards = &kube.Forwards{}
	f := &kube.Forward{LocalPort: 18081, Target: "service web"}
	m.moving = map[int]*kube.Forward{18081: f}
	m.stopping = map[int]*kube.Forward{}
	// ctrl-d's part for a moving row
	f.Dropped.Store(true)
	delete(m.moving, 18081)
	m.stopping[18081] = f
	if err := m.portMoving(18081); err == nil || !strings.Contains(err.Error(), "still stopping") {
		t.Errorf("F while it stops: %v", err)
	}
	next := &kube.Forward{LocalPort: 18081, Again: func() (*kube.Forward, error) { return nil, nil }}
	m.Update(resultMsg{moved: f, next: next})
	if next.Again != nil {
		t.Error("what it found could be followed again")
	}
	if err := m.portMoving(18081); err != nil {
		t.Errorf("after the search ended: %v", err)
	}
}

// Round 89: ctrl-k on an app said it works on "its pods or workloads";
// force stop is for pods only.
func TestAppKeyRefusalNamesPods(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.navigate(location{kind: locHome}, false)
	m.Update(tea.KeyMsg{Type: tea.KeyCtrlK})
	if !strings.Contains(m.flash, "works on its pods, not on a whole app") {
		t.Errorf("flash %q", m.flash)
	}
}

// Round 90: from an all-namespaces screen, an app home grouped in one
// namespace (found by f) opened in all namespaces, where it isn't an app:
// "has no pods yet" on another app. It opens in its own namespace.
func TestFoundHomeAppOpensInItsNamespace(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.namespace = ""
	found := view.Named("kubed-qa90b", "oomer")
	found.SetKind("Application")
	m.openFound(view.Row{Obj: found})
	if m.namespace != "kubed-qa90b" {
		t.Errorf("opened in namespace %q", m.namespace)
	}
}

// Review of round 90: an ArgoCD app found by f (its row carries its
// apiVersion) still opens across all namespaces, not in ArgoCD's own.
func TestFoundArgoAppOpensAcrossNamespaces(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	m.namespace = "kubed-qa90b"
	found := view.Named("argocd", "payments")
	found.SetAPIVersion("argoproj.io/v1alpha1")
	found.SetKind("Application")
	m.openFound(view.Row{Obj: found})
	if m.namespace != "" {
		t.Errorf("opened in namespace %q", m.namespace)
	}
}

// Round 94: :apps -n NS said nothing and stayed on all namespaces.
func TestAppsCommandTakesANamespace(t *testing.T) {
	if ns, given := listNamespaceArgs([]string{"-n", "kubed-qa94b"}); !given || ns != "kubed-qa94b" {
		t.Errorf("-n: %q %v", ns, given)
	}
	if ns, given := listNamespaceArgs([]string{"-A"}); !given || ns != "" {
		t.Errorf("-A: %q %v", ns, given)
	}
	if _, given := listNamespaceArgs(nil); given {
		t.Error("none typed")
	}
	m := testModel()
	m.width, m.height = 100, 30
	m.namespace = ""
	m.client.Cache.Watch(kube.Namespaces) // the namespaces list, loaded (empty)
	for deadline := time.Now().Add(5 * time.Second); !m.client.Cache.Synced(kube.Namespaces) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	m.runCommand("apps -n nosuch-ns")
	if !strings.Contains(m.flash, "no namespace called") {
		t.Errorf("flash %q", m.flash)
	}
}

// Round 95: e hung 25s on a stale connection, then said the network
// dropped. Reads give up on a hung try sooner and try once more; which
// errors count as the network's (worth one more try) is this.
func TestReadsRetryOnlyNetworkFailures(t *testing.T) {
	for _, msg := range []string{"context deadline exceeded", "read tcp: i/o timeout", "unexpected EOF", "http2: client connection lost"} {
		if !networkError(fmt.Errorf("get deployments: %s", msg)) {
			t.Errorf("%q should be tried again", msg)
		}
	}
	for _, msg := range []string{`deployments.apps "web" not found`, "deployments.apps is forbidden: User cannot get"} {
		if networkError(fmt.Errorf("%s", msg)) {
			t.Errorf("%q isn't the network", msg)
		}
	}
	if readTry >= apiTimeout {
		t.Error("one read try should give up well before the overall limit")
	}
}

// Round 96: deleting a failed job's pod promised "starts a new one" with
// no tries left (backoffLimit 0), and didn't say its logs go with it.
func TestJobPodDeleteSaysWhatHappens(t *testing.T) {
	job := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "sidejob"},
		"spec": map[string]any{"backoffLimit": int64(0)}, "status": map[string]any{}}}
	if w := jobPodDeleteWords(job); !strings.Contains(w, "no tries left") || !strings.Contains(w, "logs go with it") {
		t.Errorf("no tries left: %q", w)
	}
	job.Object["spec"] = map[string]any{"backoffLimit": int64(3)}
	if w := jobPodDeleteWords(job); !strings.Contains(w, "starts another") {
		t.Errorf("tries left: %q", w)
	}
}

// Round 96: esc came back through the find results, but the trail left
// that step out ("apps › pods").
func TestTrailShowsTheFindStep(t *testing.T) {
	m := testModel()
	m.width, m.height = 160, 30
	m.navigate(location{kind: locHome}, false)
	m.history = []location{{kind: locHome}, {kind: locFind, query: "web-674"}}
	if tr := m.trailWithin(120); !strings.Contains(tr, `found "web-674"`) {
		t.Errorf("trail %q", tr)
	}
}

// Review of round 96: a finished or paused job starts no new try; a long
// search is cut short in the trail; steps esc skips aren't shown.
func TestJobDeleteEndedAndTrailEdges(t *testing.T) {
	job := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "j"},
		"spec":   map[string]any{"backoffLimit": int64(3)},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True"}}}}}
	if w := jobPodDeleteWords(job); !strings.Contains(w, "already ended") {
		t.Errorf("ended: %q", w)
	}
	job.Object["status"] = map[string]any{}
	job.Object["spec"] = map[string]any{"backoffLimit": int64(3), "suspend": true}
	if w := jobPodDeleteWords(job); !strings.Contains(w, "paused") {
		t.Errorf("paused: %q", w)
	}
	m := testModel()
	m.width, m.height = 80, 24
	m.navigate(location{kind: locHome}, false)
	m.history = []location{{kind: locHome}, {kind: locFind, query: "a-very-long-search-for-something"}, {kind: locHome, fromFind: true}}
	tr := m.trailWithin(70)
	if !strings.Contains(tr, "…") || strings.Count(tr, "apps") != 1 {
		t.Errorf("trail %q", tr)
	}
}

// QA round 128: v calls a version made by a restart "restarted", as w
// does, not "made".
func TestVersionsSayRestarted(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d-restarted"}, "spec": map[string]any{"replicas": int64(1)}}}
	rs := func(name string, rev int, made time.Time, stamp string) *unstructured.Unstructured {
		tmpl := map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nginx:1"}}}}
		if stamp != "" {
			tmpl["metadata"] = map[string]any{"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}}
		}
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": "restarted-" + name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": fmt.Sprint(rev)},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "d-restarted", "controller": true}}},
			"spec": map[string]any{"template": tmpl}}}
		o.SetCreationTimestamp(metav1.NewTime(made))
		return o
	}
	now := time.Now()
	store := versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		rs("web-2", 2, now.Add(-time.Minute), now.Add(-time.Minute).UTC().Format(time.RFC3339)), rs("web-1", 1, now.Add(-time.Hour), "")}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(store, res, nil)
	m.versionsNow(view.Row{Obj: dep})
	if m.prompt == nil || len(m.prompt.items) != 2 {
		t.Fatalf("prompt %+v", m.prompt)
	}
	if l := m.prompt.items[0].label; !strings.Contains(l, "restarted") || strings.Contains(l, "made") {
		t.Errorf("the restart: %q", l)
	}
	if l := m.prompt.items[1].label; !strings.Contains(l, "made") {
		t.Errorf("the first version: %q", l)
	}
}

// QA round 128: an open versions list keeps its ready counts current, as
// its ages are: right after a go-back, "newest, 2 of 3 ready" and "1
// serving" give way to "running now" once every pod runs the new one, with
// the cursor kept on its version.
func TestVersionCountsMoveOnWhileOpen(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d-counts"}, "spec": map[string]any{"replicas": int64(3)}}}
	rs := func(name string, rev int, made time.Time, image string, ready int64) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": "counts-" + name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": fmt.Sprint(rev)},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "d-counts", "controller": true}}},
			"spec":   map[string]any{"replicas": ready, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}},
			"status": map[string]any{"replicas": int64(3), "readyReplicas": ready}}}
		o.SetCreationTimestamp(metav1.NewTime(made))
		return o
	}
	now := time.Now()
	newest, older, oldest := rs("web-6", 6, now.Add(-20*time.Second), "nginx:6", 2), rs("web-5", 5, now.Add(-time.Hour), "nginx:5", 1), rs("web-4", 4, now.Add(-2*time.Hour), "nginx:4", 0)
	newest.Object["status"] = map[string]any{"replicas": int64(3), "readyReplicas": int64(2)}
	store := versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {newest, older, oldest}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(store, res, nil)
	m.versionsNow(view.Row{Obj: dep})
	if m.prompt == nil || len(m.prompt.items) != 3 {
		t.Fatalf("prompt %+v", m.prompt)
	}
	if l := m.prompt.items[0].label; !strings.Contains(l, "2 of 3 ready") || !strings.Contains(m.prompt.items[1].label, "1 serving") {
		t.Fatalf("before: %q / %q", l, m.prompt.items[1].label)
	}
	m.prompt.choice = 2
	newest.Object["status"] = map[string]any{"replicas": int64(3), "readyReplicas": int64(3)}
	older.Object["status"] = map[string]any{"replicas": int64(0), "readyReplicas": int64(0)}
	m.prompt.tick()
	if l := m.prompt.items[0].label; !strings.Contains(l, "running now") || strings.Contains(l, "ready)") {
		t.Errorf("newest after: %q", l)
	}
	if l := m.prompt.items[1].label; strings.Contains(l, "serving") {
		t.Errorf("older after: %q", l)
	}
	if m.prompt.choice != 2 {
		t.Errorf("cursor moved to %d", m.prompt.choice)
	}
}

// Review of round 128: an open versions list stops following a workload
// deleted and made again under the same name (another one's versions).
func TestVersionListStopsOnANewWorkload(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d-first"}, "spec": map[string]any{"replicas": int64(1)}}}
	rs := func(name, depUID string, rev int, image string) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": "uid-" + name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": fmt.Sprint(rev)},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": depUID, "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}}}}
		o.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Duration(10-rev) * time.Minute)))
		return o
	}
	store := versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs("web-2", "d-first", 2, "nginx:2"), rs("web-1", "d-first", 1, "nginx:1")}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(store, res, nil)
	m.versionsNow(view.Row{Obj: dep})
	if m.prompt == nil || len(m.prompt.items) != 2 {
		t.Fatalf("prompt %+v", m.prompt)
	}
	again := dep.DeepCopy()
	again.SetUID("d-second")
	store[kube.Deployments] = []*unstructured.Unstructured{again}
	store[kube.ReplicaSets] = []*unstructured.Unstructured{rs("web-x", "d-second", 1, "redis:7")}
	m.prompt.tick()
	if len(m.prompt.items) != 2 || !strings.Contains(m.prompt.items[0].label, "revision 2") {
		t.Errorf("followed the new workload: %+v", m.prompt.items)
	}
}
