package ui

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// listStore is a cluster as enrich reads it, from fixed lists.
type listStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s listStore) List(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, o := range s[gvr] {
		if ns == "" || o.GetNamespace() == ns {
			out = append(out, o)
		}
	}
	return out
}

func (s listStore) Get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s[gvr] {
		if o.GetNamespace() == ns && o.GetName() == name {
			return o, true
		}
	}
	return nil, false
}

func (listStore) Synced(schema.GroupVersionResource) bool { return true }

// doubleUndoModel: web in qa, A (revision 1) made 5h ago, B (revision 2)
// 4h ago, u back to A (3), u back to B (4), every event long gone. Nothing
// dates B's go-back: it came back 4h ago or later, never exactly then.
func doubleUndoModel(t *testing.T) (*Model, time.Time) {
	t.Helper()
	now := time.Now()
	madeB := now.Add(-4 * time.Hour)
	tmpl := func(env string) map[string]any {
		return map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx",
			"env": []any{map[string]any{"name": "FOO", "value": env}}}}}}
	}
	rs := func(name, rev, history string, made time.Time, env string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": "rs-" + name, "creationTimestamp": made.Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev, "deployment.kubernetes.io/revision-history": history},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}},
			"spec": map[string]any{"template": tmpl(env)}}}
	}
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-web",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}},
		"spec": map[string]any{"template": tmpl("b")}}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
	})
	m := testModel()
	t.Cleanup(m.client.Cache.Stop)
	m.enr = enrich.New(listStore{
		kube.Deployments: {dep},
		kube.ReplicaSets: {rs("web-a", "3", "1", now.Add(-5*time.Hour), ""), rs("web-b", "4", "2", madeB, "b")},
	}, res, nil)
	m.namespace = "qa"
	m.width, m.height = 120, 40
	return m, madeB
}

// Round 124: when nothing dates a go-back, :changes showed the earliest it
// can be as if exact ("4h"); it marks it as a bound (≤4h), on screen and in
// what is saved or copied ("at or after").
func TestChangesSayAnUndatedGoBackAsABound(t *testing.T) {
	m, madeB := doubleUndoModel(t)
	msg, ok := m.changesIn("qa", "", "qa", nil)().(pagerMsg)
	if !ok {
		t.Fatal("no pager")
	}
	var row string
	order := map[string]int{}
	for i, l := range strings.Split(msg.content, "\n") {
		if strings.Contains(l, "revision 4") {
			row = l
		}
		for _, r := range []string{"revision 4:", "revision 3:"} {
			if strings.Contains(l, r) {
				order[r] = i
			}
		}
	}
	if order["revision 4:"] > order["revision 3:"] { // the same bound: the later revision first
		t.Errorf("revision 3 above revision 4:\n%s", msg.content)
	}
	if !strings.Contains(row, "≤4h") {
		t.Errorf("go-back row not a bound: %q\n%s", row, msg.content)
	}
	want := "at or after " + madeB.Local().Format("2 Jan 15:04 MST")
	if !strings.Contains(msg.plain, want) {
		t.Errorf("saved text lost the bound, want %q:\n%s", want, msg.plain)
	}
}

// Round 124: :shipped dated the same go-back as if exact; it says "after"
// (and "at or after" in the export), as Deployed and the versions list do.
func TestShippedSaysAnUndatedGoBackAsABound(t *testing.T) {
	m, madeB := doubleUndoModel(t)
	msg, ok := m.shipped()().(pagerMsg)
	if !ok {
		t.Fatal("no pager")
	}
	if want := "after " + madeB.Local().Format("Mon 2 Jan 15:04"); !strings.Contains(msg.content, want) {
		t.Errorf("want %q:\n%s", want, msg.content)
	}
	if want := "at or after " + madeB.Local().Format("2006-01-02 15:04 MST"); !strings.Contains(msg.plain, want) {
		t.Errorf("export lost the bound, want %q:\n%s", want, msg.plain)
	}
}

// Round 123: restarting an app with a paused deployment said "restarted";
// round 124 fixed the count for picked rows, but the app's own restart
// still counted the paused one among those restarted. Only the ones that
// restarted are counted; the paused ones are said apart.
func TestAppRestartCountsOnlyWhatRestarted(t *testing.T) {
	deploy := func(name string, paused bool) view.Row {
		o := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": name},
			"spec": map[string]any{"replicas": int64(2), "paused": paused}}}
		return view.Row{Obj: o, Cells: []string{name}}
	}
	if got := appRestartText("shop", []view.Row{deploy("web", false), deploy("api", true)}); !strings.HasPrefix(got, "restarted app shop (1 deployment); restart set for api") {
		t.Errorf("one of two paused: %q", got)
	}
	if got := appRestartText("shop", []view.Row{deploy("web", true), deploy("api", true)}); strings.Contains(got, "restarted") || !strings.Contains(got, "once web, api are resumed") {
		t.Errorf("all paused: %q", got)
	}
	if got := appRestartText("shop", []view.Row{deploy("web", false), deploy("api", false)}); got != "restarted app shop (2 deployments)" {
		t.Errorf("none paused: %q", got)
	}
}

// Round 123: l on something with no pods said "no pods yet", as if some
// were coming; a failed job's pods are gone with their logs, and anything
// else has no pod right now.
func TestLogsWithNoPodsSayWhy(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc.res = kube.Resource{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}
	dep := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "web", "namespace": "qa"},
		"spec": map[string]any{"replicas": int64(0), "selector": map[string]any{"matchLabels": map[string]any{"app": "web"}}}}}
	m.workloadLogs(view.Row{Obj: dep})
	if strings.Contains(m.flash, "yet") || !strings.Contains(m.flash, "no pod of web exists right now") {
		t.Errorf("scaled to 0: %q", m.flash)
	}
	m.loc.res = kube.Resource{GVR: kube.Jobs, Kind: "Job", Namespaced: true}
	job := &unstructured.Unstructured{Object: map[string]any{"kind": "Job", "metadata": map[string]any{"name": "migrate", "namespace": "qa"},
		"spec":   map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"job-name": "migrate"}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True"}}}}}
	m.workloadLogs(view.Row{Obj: job})
	if !strings.Contains(m.flash, "failed and its pods are gone") {
		t.Errorf("failed job: %q", m.flash)
	}
}

// Round 124: / on search results shares the box with the search, so the
// results were searched again with the filter's text (a pod shop showed
// among the results for "web"), and an empty filter box took the results'
// "Nothing found" and count away. The results stay the search's.
func TestFilterOnFoundKeepsItsResults(t *testing.T) {
	m, cluster := findTestModel(t)
	defer m.client.Cache.Stop()
	addTestPod(t, m, cluster, "shop", nil)
	m.startFind()
	typeFind(m, "web")
	m.showFound("web", 0)
	m.startInput(modeFilter, "/", "")
	m.input.SetValue("shop")
	m.refreshRows()
	for _, r := range m.tbl.rows {
		if len(r.Cells) > 1 && r.Cells[1] == "shop" {
			t.Errorf("filter searched again: %v", m.tbl.rows)
		}
	}
	m.mode = modeTable
	m.startFind()
	typeFind(m, "zzz")
	m.showFound("zzz", 0)
	m.startInput(modeFilter, "/", "")
	m.refreshRows()
	if got := m.emptyMessage(); got != "Nothing found by that name." {
		t.Errorf("empty results with / pressed: %q", got)
	}
}

// Round 124: esc back to the find box from a result typed the search again
// but kept the box's query unset, so / on the results lost it.
func TestResumedFindKeepsItsQuery(t *testing.T) {
	m, _ := findTestModel(t)
	defer m.client.Cache.Stop()
	m.navigate(location{kind: locFind}, false)
	m.resumeFind("web", 0)
	if m.loc.query != "web" {
		t.Errorf("query %q", m.loc.query)
	}
}

// Round 123: restarting several picked deployments, one paused, said they
// all get new pods one at a time; the prompt names the paused one.
func TestRestartRowsPromptNamesThePaused(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.loc.res = kube.Resource{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}
	deploy := func(name string, paused bool) view.Row {
		return view.Row{Obj: &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": name, "namespace": "qa"},
			"spec": map[string]any{"replicas": int64(2), "paused": paused}}}, Cells: []string{name}}
	}
	m.restartRows([]view.Row{deploy("web", false), deploy("api", true)})
	if m.prompt == nil || !strings.Contains(m.prompt.title, "Paused, so not until resumed: api") {
		t.Errorf("prompt: %v", m.prompt != nil && m.prompt.title != "")
	}
}

// Round 117: a long problem keeps its cause on an 80x24 screen. Each line
// keeps its own ending, also below a line with no label (one without a
// space), which shifted every ending onto the line above.
func TestLongProblemKeepsItsCauseBelowAnUnlabeledLine(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 80, 24
	long := "Problem  Can't pull image: app can't download 123456789012.dkr.ecr.ap-south-1.amazonaws.com/team/service:v1: " +
		strings.Repeat("failed to resolve reference with a long registry reply that goes on and on ", 5) + "; access denied by the registry"
	got := ansi.Strip(strings.Join(m.styleDetail([]string{"web", long, "Fix      enter → Its workloads, then i to fix the image name or tag", "Pods     0 of 1 ready"}), "\n"))
	if !strings.Contains(got, "access denied by the registry") || !strings.Contains(got, "then i to fix") {
		t.Errorf("cause lost: %s", got)
	}
}

// Round 122: p on a failed job whose pods were removed said none of its
// pods had restarted (and that l shows the live logs); no pod exists.
func TestPreviousLogsWithNoPodsSayNoneExists(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.previousLogsOf("migrate", nil)
	if !strings.Contains(m.flash, "no pod of migrate exists right now") || strings.Contains(m.flash, "restarted") {
		t.Errorf("%q", m.flash)
	}
}

// Round 106: while you type, a long note (not an error) took more footer
// rows than the one the hints show in, leaving blank rows above them.
func TestTypingKeepsTheFooterOneRow(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.width, m.height = 60, 30
	m.mode = modeCommand
	m.setFlash(strings.Repeat("pod web-1 went away; forwarding again through web-2 ", 4), nil)
	if h := m.footerHeight(); h != 1 {
		t.Errorf("footer %d rows while typing", h)
	}
	m.setFlash("", errors.New(strings.Repeat("no such namespace: kubed-qa-something ", 4)))
	if h := m.footerHeight(); h < 2 {
		t.Errorf("an error while typing still shows whole: %d rows", h)
	}
}

// Round 101: node pools differ at their end, so NODEPOOL shows whole or not
// at all, and comes back when there is room for it whole (the check that
// it is never cut passed when it was never shown).
func TestNodePoolShownWholeWhenItFits(t *testing.T) {
	pools := []string{"nodepool-system-arm64", "nodepool-general-arm64"}
	var rows []view.Row
	for i, p := range pools {
		rows = append(rows, view.Row{Cells: []string{fmt.Sprintf("ip-10-0-0-%d", i), "Ready", p, "t4g.xlarge"}})
	}
	header := []string{"NAME", "STATUS", "NODEPOOL", "TYPE"}
	tb := &table{header: header, optional: []string{"NODEPOOL"}, rows: rows}
	for _, width := range []int{60, 80, 100, 120} {
		idx := tb.visibleColumns(width)
		var picked []view.Row
		for _, r := range rows {
			picked = append(picked, view.Row{Cells: pick(r.Cells, idx)})
		}
		ws := columnWidths(pick(header, idx), picked, width, -1)
		shown := false
		for j, i := range idx {
			if header[i] == "NODEPOOL" {
				shown = true
				if ws[j] < len(pools[1]) {
					t.Errorf("width %d: NODEPOOL %d wide, cut", width, ws[j])
				}
			}
		}
		if width >= 100 && !shown {
			t.Errorf("width %d: room for NODEPOOL whole, but hidden", width)
		}
	}
}
