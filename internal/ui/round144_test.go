package ui

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 144: six containers of one pod all read "gbx6p/container…":
// the end of the container's name, which tells them apart, was cut off.
func TestLogTagsKeepContainerEnd(t *testing.T) {
	for _, prefix := range []string{"container-", "platform-sidecar-container-"} {
		var streams [][2]string
		for i := 1; i <= 6; i++ {
			streams = append(streams, [2]string{"web-5b76dd6597-gbx6p", fmt.Sprintf("%s%d", prefix, i)})
		}
		tags := logTags(streams)
		seen := map[string]bool{}
		for i, tag := range tags {
			if seen[tag] || !strings.HasSuffix(tag, fmt.Sprintf("-%d", i+1)) || !strings.HasPrefix(tag, "gbx6p/") {
				t.Errorf("%s: tags %q", prefix, tags)
				break
			}
			seen[tag] = true
		}
		if prefix == "container-" && tags[2] != "gbx6p/container-3" { // room for the whole name
			t.Errorf("tag %q, want gbx6p/container-3", tags[2])
		}
	}
}

// QA round 144: a click in the box under the list picked a row scrolled
// out of sight; only the rows on screen can be clicked.
func TestMouseClickOnlyOnVisibleRows(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 40
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	var names []string
	for i := range 40 {
		names = append(names, fmt.Sprintf("pod-%02d", i))
	}
	m.tbl.setRows([]string{"NAME"}, rows(names...), nil)
	m.tbl.render(110, 10) // a heading and 9 rows on screen
	first := m.headerHeight() + 2
	click := func(y int) {
		m.onMouse(tea.MouseMsg{Button: tea.MouseButtonLeft, Action: tea.MouseActionPress, Y: y})
	}
	click(first + 12) // below the list, in the box under it
	if m.tbl.cursor != 0 || m.tbl.offset != 0 {
		t.Fatalf("click below the list selected row %d (offset %d)", m.tbl.cursor, m.tbl.offset)
	}
	click(first + 8) // the last row on screen
	if m.tbl.cursor != 8 {
		t.Errorf("click on the last row shown selected %d", m.tbl.cursor)
	}
}

// QA round 144: clicks in the Enter menu did nothing. A click highlights
// an item, a click on the highlighted one runs it; an item that is off
// says why.
func TestMouseInMenu(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 40
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods}}
	ran := ""
	item := func(label string) menuItem {
		return menuItem{label: label, about: "does " + label, run: func() tea.Cmd { ran = label; return nil }}
	}
	items := []menuItem{item("logs"), item("shell"), {label: "delete", about: "changes are off (read-only)", disabled: true, off: "read-only"}}
	m.prompt, m.mode = menuPrompt("pod web", items), modePrompt
	screen := strings.Split(m.View(), "\n")
	rowOf := func(label string) int {
		for y, l := range screen {
			if strings.Contains(l, " "+label+" ") || strings.Contains(l, "▶ "+label) {
				return y
			}
		}
		t.Fatalf("%q not on screen:\n%s", label, strings.Join(screen, "\n"))
		return -1
	}
	click := func(y int) {
		m.onMouse(tea.MouseMsg{Button: tea.MouseButtonLeft, Action: tea.MouseActionPress, X: m.width / 2, Y: y})
	}
	shell, del := rowOf("shell"), rowOf("delete")
	click(rowOf("logs")) // highlighted as the menu opens: one click only picks it
	if m.prompt == nil || ran != "" {
		t.Fatalf("one click on the item highlighted at first ran %q", ran)
	}
	click(shell)
	if m.prompt == nil || m.prompt.choice != 1 || ran != "" {
		t.Fatalf("first click: open %v, choice %d, ran %q", m.prompt != nil, m.prompt.choice, ran)
	}
	click(del)
	if m.prompt == nil || m.prompt.choice != 2 || m.prompt.problem != items[2].about || ran != "" {
		t.Fatalf("click on an item that is off: choice %d, problem %q, ran %q", m.prompt.choice, m.prompt.problem, ran)
	}
	click(0) // the header, outside the menu
	if m.prompt == nil || m.prompt.choice != 2 {
		t.Fatalf("click outside the menu changed it: choice %d", m.prompt.choice)
	}
	click(shell)
	click(shell)
	if ran != "shell" || m.prompt != nil || m.mode == modePrompt {
		t.Errorf("second click: ran %q, still open %v, mode %v", ran, m.prompt != nil, m.mode)
	}
	ran = ""
	m.prompt, m.mode = menuPrompt("pod web", items), modePrompt
	m.View()
	click(rowOf("logs"))
	click(rowOf("logs"))
	if ran != "logs" {
		t.Errorf("two clicks on the item highlighted at first ran %q", ran)
	}
}

// QA round 144: at 60x20 every node read "ip-10-0-…": a narrow name
// column cut off the end that tells nodes apart. It keeps the end.
func TestNarrowNamesKeepTheirEnd(t *testing.T) {
	nodes := []string{"ip-10-0-145-239", "ip-10-0-37-12", "ip-10-0-131-8"}
	for w := 8; w <= 11; w++ {
		seen := map[string]bool{}
		for _, n := range nodes {
			got := shortenName(n, w)
			if seen[got] || len([]rune(got)) > w || !strings.HasSuffix(got, n[len(n)-4:]) {
				t.Errorf("width %d: %q -> %q", w, n, got)
			}
			seen[got] = true
			if row := strings.TrimSpace(formatRow([]string{n}, []int{w})); row != got {
				t.Errorf("width %d: row %q, name %q", w, row, got)
			}
		}
	}
}

// QA round 144: the logs of a pod that was deleted ended "the connection
// to the cluster dropped", and r then replaced the lines with "pods
// "victim" not found". The end says the pod was deleted; r keeps them.
func TestLogsOfDeletedPodSayDeleted(t *testing.T) {
	m := testModel()
	m.width, m.height = 200, 40
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{kube.Pods: "PodList"})
	var gets atomic.Int32
	dyn.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) { gets.Add(1); return false, nil, nil })
	m.client.Dynamic = dyn
	p := newPager("logs of pod victim · app container · in shop", "", 100, 20)
	p.isLogs, p.lines, p.follow = true, nil, true
	p.source = logSource{ns: "shop", pod: "victim", container: "app"}
	p.logs = make(chan string)
	p.append([]string{"hello", "world"})
	m.pager, m.mode = p, modePager
	_, cmd := m.Update(logLinesMsg{ch: p.logs, lines: []string{"kubed: lost the connection to the cluster while reading these logs; r reads them again"}, done: true})
	// The stream's end doesn't wait for the cluster: the check runs after.
	if n := gets.Load(); n != 0 || cmd == nil || len(p.lines) != 2 {
		t.Fatalf("at the stream's end: %d gets, cmd %v, lines %q", n, cmd != nil, p.lines)
	}
	m.Update(cmd())
	got := ansi.Strip(strings.Join(p.lines, "\n"))
	if strings.Contains(got, "connection") || !strings.HasSuffix(got, "── the pod was deleted; these are its last lines") || !strings.HasPrefix(got, "hello\nworld") {
		t.Errorf("end of a deleted pod's logs:\n%s", got)
	}
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	if m.pager != p || ansi.Strip(strings.Join(m.pager.lines, "\n")) != got {
		t.Errorf("r replaced the lines: %q", strings.Join(m.pager.lines, "\n"))
	}
	if want := "pod victim no longer exists, so its logs can't be read again"; m.flash != want {
		t.Errorf("flash %q, want %q", m.flash, want)
	}
}

// QA round 144: a refused pod edit said "change what makes it (its
// deployment)" for a pod nothing makes. It names what does, or says how
// to make it again.
func TestPodEditRefusalNamesOwner(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	pod := func(kind string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "victim", "namespace": "shop"}}}
		if kind != "" {
			yes := true
			u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: "x", UID: "u-x", Controller: &yes}})
		}
		return u
	}
	for kind, want := range map[string]string{
		"":            "create it again with the change (delete it, then apply the new spec)",
		"StatefulSet": "change what makes it (its stateful set) instead, or delete the pod",
		"Job":         "change what makes it (its job) instead, or delete the pod",
	} {
		if got := m.podEditFix(pod(kind)); got != want {
			t.Errorf("owner %q: %q, want %q", kind, got, want)
		}
	}
	if got := plainEditError("pod updates may not change fields other than `spec.containers[*].image`"); strings.Contains(got, "deployment") {
		t.Errorf("without knowing the pod: %q", got)
	}
}
