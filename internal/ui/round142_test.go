package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// fakeLogBatch is n numbered log lines, from line start.
func fakeLogBatch(start, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("line %d", start+i)
		if (start+i)%100 == 96 {
			out[i] += " needle96"
		}
	}
	return out
}

// QA round 142: paused logs past the 5,000-line cap drifted: the top line
// slid as old lines were dropped, and the match moved off. The line you
// read stays on top; once it is dropped, kubed says so once.
func TestPausedLogsKeepPlacePastCap(t *testing.T) {
	p := newPager("logs", "", 80, 10)
	p.isLogs, p.lines, p.follow = true, nil, true
	p.append(fakeLogBatch(0, maxLogLines))
	p.follow = false
	p.vp.SetYOffset(4000)
	top := func() string { return ansi.Strip(strings.Split(p.vp.View(), "\n")[0]) }
	p.setSearch("needle96")
	p.nextMatch(+1)
	before, matchLine := top(), p.lines[p.matches[p.match]]
	p.append(fakeLogBatch(maxLogLines, 500))
	if got := top(); got != before {
		t.Errorf("top line drifted: %q, was %q", got, before)
	}
	if got := p.lines[p.matches[p.match]]; got != matchLine {
		t.Errorf("match moved: %q, was %q", got, matchLine)
	}
	if p.readDropped {
		t.Errorf("said the lines were dropped while they are still there")
	}
	p.append(fakeLogBatch(maxLogLines+500, 4000)) // the line on top is dropped now
	if !p.readDropped {
		t.Errorf("the line being read was dropped without a word")
	}
	if p.vp.YOffset != 0 {
		t.Errorf("offset %d, want the oldest kept line on top", p.vp.YOffset)
	}
	p.readDropped = false
	p.append(fakeLogBatch(maxLogLines+4500, 4000))
	if p.readDropped {
		t.Errorf("said it twice")
	}
}

// The app says it when the line being read is dropped.
func TestPausedLogsDroppedIsSaid(t *testing.T) {
	m := testModel()
	m.width, m.height = 100, 30
	p := newPager("logs", "", 80, 10)
	p.isLogs, p.lines = true, nil
	p.logs = make(chan string)
	p.append(fakeLogBatch(0, maxLogLines))
	p.follow = false
	p.vp.SetYOffset(10)
	m.pager, m.mode = p, modePager
	m.Update(logLinesMsg{ch: p.logs, lines: fakeLogBatch(maxLogLines, 100)})
	if want := "the lines you were reading were dropped: only the newest 5000 are kept"; m.flash != want {
		t.Errorf("flash %q, want %q", m.flash, want)
	}
}

// nsModel is a cluster with namespaces shop and going (being deleted),
// its namespaces synced unless unsynced.
func nsModel(t *testing.T, synced bool) *Model {
	t.Helper()
	kinds := []kube.Resource{
		{GVR: kube.Nodes, Kind: "Node", ShortNames: []string{"no"}, Singular: "node"},
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true, ShortNames: []string{"po"}, Singular: "pod"},
		{GVR: kube.Namespaces, Kind: "Namespace", ShortNames: []string{"ns"}, Singular: "namespace"},
	}
	lists := map[schema.GroupVersionResource]string{}
	for _, k := range kinds {
		lists[k.GVR] = k.Kind + "List"
	}
	shop := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "shop"}}}
	going := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{"name": "going", "deletionTimestamp": "2026-10-16T00:00:00Z"}, "status": map[string]any{"phase": "Terminating"}}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, shop, going)
	res := kube.NewResources(kinds)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}, width: 140, height: 40,
		forwards: &kube.Forwards{}, env: environment{known: true}}
	m.enr = enrich.New(m.client.Cache, res, nil)
	t.Cleanup(m.client.Cache.Stop)
	if synced {
		m.client.Cache.Watch(kube.Namespaces)
		for end := time.Now().Add(5 * time.Second); !m.client.Cache.Synced(kube.Namespaces) && time.Now().Before(end); {
			time.Sleep(5 * time.Millisecond)
		}
	}
	return m
}

// QA round 142: a namespace being deleted, or gone, showed as if fine:
// "Showing kubed-qa142a2", "all healthy", "Nothing here in kubed-qa142a2".
func TestShownNamespaceBeingDeletedOrGone(t *testing.T) {
	showing := func(m *Model) string {
		for _, l := range m.headerFacts() {
			if strings.Contains(l, "Showing") {
				return ansi.Strip(l)
			}
		}
		return ""
	}
	for _, c := range []struct {
		ns     string
		synced bool
		header string // "" : nothing said
		empty  string
	}{
		{"shop", true, "", "Nothing here in shop. :ns all shows every namespace."},
		{"going", true, "being deleted", "Nothing here in going: it is being deleted. :ns picks another."},
		{"gone", true, "no longer exists", "Namespace gone no longer exists. :ns picks another."},
		{"gone", false, "", "Nothing here in gone. :ns all shows every namespace."},
	} {
		m := nsModel(t, c.synced)
		m.namespace, m.chosenNS = c.ns, c.ns
		m.loc = location{kind: locHome}
		h := showing(m)
		for _, w := range []string{"being deleted", "no longer exists"} {
			if strings.Contains(h, w) != (w == c.header) {
				t.Errorf("%s (synced %v): header %q, want it to say %q", c.ns, c.synced, h, c.header)
			}
		}
		if c.header != "" {
			if title := ansi.Strip(m.boxTitle()); !strings.Contains(title, c.header) {
				t.Errorf("%s: title %q doesn't say %q", c.ns, title, c.header)
			}
		}
		if got := m.emptyMessage(); got != c.empty {
			t.Errorf("%s (synced %v): empty %q, want %q", c.ns, c.synced, got, c.empty)
		}
	}
}

// QA round 142: s or S on a container without a shell showed the
// runtime's own error. Every shell path goes through setFlash.
func TestNoShellInImage(t *testing.T) {
	want := "this container has no shell (/bin/sh isn't in its image)"
	for _, e := range []string{
		`shell: OCI runtime exec failed: exec failed: unable to start container process: exec: "/bin/sh": stat /bin/sh: no such file or directory: unknown`,
		`OCI runtime exec failed: exec failed: container_linux.go:380: starting container process caused: exec: "/bin/sh": executable file not found in $PATH: unknown`,
	} {
		m := testModel()
		m.setFlash("", fmt.Errorf("%s", e))
		if m.flash != want {
			t.Errorf("%q\n  shown as %q", e, m.flash)
		}
		r := shellEnded(fmt.Errorf("%s", e))
		m.setFlash(r.text, r.err)
		if m.flash != want {
			t.Errorf("shell ended with %q\n  shown as %q", e, m.flash)
		}
	}
}

// QA round 142: after the pod was deleted, the logs footer offered "p
// before last restart" for a pod that never restarted, and p replaced the
// lines with 'can't read the logs: pods "chatty" not found'.
func TestLogsOfDeletedPodOfferNoP(t *testing.T) {
	m := testModel()
	m.width, m.height = 200, 40
	m.client.Dynamic = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{kube.Pods: "PodList"})
	m.client.Cache.Watch(kube.Pods)
	t.Cleanup(m.client.Cache.Stop)
	for end := time.Now().Add(5 * time.Second); !m.client.Cache.SyncedIn(kube.Pods, "shop") && time.Now().Before(end); {
		time.Sleep(5 * time.Millisecond)
	}
	p := newPager("logs of pod chatty · app container · in shop", "", 100, 20)
	p.isLogs, p.lines = true, nil
	p.source = logSource{ns: "shop", pod: "chatty", container: "app"}
	p.append([]string{"hello", "world"})
	m.pager, m.mode = p, modePager
	for _, h := range m.hints() {
		if h.key == "p" {
			t.Errorf("footer offers p %q for a pod that is gone", h.label)
		}
	}
	m.onPagerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("p")})
	if m.pager != p || strings.Join(p.lines, "\n") != "hello\nworld" {
		t.Errorf("p replaced the lines: %q", strings.Join(m.pager.lines, "\n"))
	}
	if want := "pod chatty no longer exists, so there are no earlier logs to read"; m.flash != want {
		t.Errorf("flash %q, want %q", m.flash, want)
	}
}

// QA round 142: a refused pod edit showed the API's dump, diff and all.
func TestPodEditRefusalInWords(t *testing.T) {
	msg := `Pod "chatty" is invalid: spec: Forbidden: pod updates may not change fields other than ` + "`spec.containers[*].image`" +
		`,` + "`spec.initContainers[*].image`" + `,` + "`spec.activeDeadlineSeconds`" + `,` + "`spec.tolerations`" + ` (only additions to existing tolerations),` +
		"`spec.terminationGracePeriodSeconds`" + ` (allow it to be set to 1 if it was previously negative)
  core.PodSpec{
  	Volumes:        {{Name: "kube-api-access-x", VolumeSource: {Projected: &{Sources: {{ServiceAccountToken: &{ExpirationSeconds: 3607, Path: "token"}}}}}}},
  	InitContainers: nil,
@@ -101,7 +101,7 @@
- 			Requests: core.ResourceList{s"cpu": {i: resource.int64Amount{value: 10, scale: -3}, s: "10m", Format: "DecimalSI"}},
+ 			Requests: core.ResourceList{s"cpu": {i: resource.int64Amount{value: 20, scale: -3}, s: "20m", Format: "DecimalSI"}},
  }`
	want := "a pod's spec can't be changed once it runs, except its images and a few fields; change what makes it instead, or delete the pod"
	if got := plainEditError(msg); got != want {
		t.Errorf("edit refusal:\n%s", got)
	}
	if got := friendlyError(msg); got != want {
		t.Errorf("flash:\n%s", got)
	}
}

// QA round 142: the container picker called every row "The app's own
// container". Only the one main container is; others say what they are,
// or nothing.
func TestContainerPickerHints(t *testing.T) {
	pod := func(names ...string) *unstructured.Unstructured {
		var cs []any
		for _, n := range names {
			cs = append(cs, map[string]any{"name": n, "image": "x"})
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "p", "namespace": "shop"}, "spec": map[string]any{"containers": cs}}}
	}
	hints := func(o *unstructured.Unstructured) map[string]string {
		m := testModel()
		m.withContainer(view.Row{Obj: o}, func(*unstructured.Unstructured, string) tea.Cmd { return nil })
		out := map[string]string{}
		for _, it := range m.prompt.items {
			out[it.label] = it.about
		}
		return out
	}
	got := hints(pod("alpha", "beta", "gamma"))
	for _, c := range []string{"alpha", "beta", "gamma"} {
		if got[c] != "" {
			t.Errorf("%s: %q, want nothing (three main containers)", c, got[c])
		}
	}
	got = hints(pod("web", "istio-proxy"))
	if got["web"] != "The app's own container" || got["istio-proxy"] == "The app's own container" {
		t.Errorf("web and its proxy: %q", got)
	}
}

// QA round 142 (8): help opened wide and then narrowed kept its two
// columns, wrapped and mixed up; opened narrow and then widened it stayed
// one column. Help lays itself out again on every resize.
func TestHelpRelaysOnResize(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mxClusters()
	plain := func(m *Model) string { return ansi.Strip(strings.Join(m.pager.lines, "\n")) }
	fresh := func(w, h int) string {
		m := mxModel(mxFull, w, h)
		m.openHelp()
		return plain(m)
	}
	for _, c := range [][4]int{{200, 50, 40, 15}, {80, 24, 40, 15}, {40, 15, 80, 24}, {40, 15, 200, 50}} {
		m := mxModel(mxFull, c[0], c[1])
		m.openHelp()
		m.pager.vp.SetYOffset(20)
		m.Update(tea.WindowSizeMsg{Width: c[2], Height: c[3]})
		if got, want := plain(m), fresh(c[2], c[3]); got != want {
			t.Errorf("%dx%d resized to %dx%d: not laid out as help opened at that size", c[0], c[1], c[2], c[3])
		}
		if m.pager.vp.YOffset == 0 {
			t.Errorf("%dx%d resized to %dx%d: went back to the top", c[0], c[1], c[2], c[3])
		}
	}
}

// QA round 142 (11): a recent-namespace digit past the list did nothing
// and said nothing.
func TestRecentDigitPastTheList(t *testing.T) {
	m := testModel()
	m.recentNS = []string{"shop", "pay"}
	m.onKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("9")})
	if want := "no namespace on key 9 yet: 1-2 are your recent ones (0 is all)"; m.flash != want {
		t.Errorf("flash %q, want %q", m.flash, want)
	}
}
