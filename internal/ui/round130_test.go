package ui

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// kindsModel is a cluster serving every kind the command words lead to,
// with one namespace, "shop".
func kindsModel(t *testing.T) *Model {
	t.Helper()
	kinds := []kube.Resource{
		{GVR: kube.Nodes, Kind: "Node", ShortNames: []string{"no"}, Singular: "node"},
		{GVR: kube.NodePools, Kind: "NodePool", Singular: "nodepool"},
		{GVR: kube.NodeClaims, Kind: "NodeClaim", Singular: "nodeclaim"},
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true, ShortNames: []string{"po"}, Singular: "pod"},
		{GVR: kube.Namespaces, Kind: "Namespace", ShortNames: []string{"ns"}, Singular: "namespace"},
		{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true, ShortNames: []string{"cj"}, Singular: "cronjob"},
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true, Singular: "job"},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, ShortNames: []string{"deploy"}, Singular: "deployment"},
		{GVR: kube.Ingresses, Kind: "Ingress", Namespaced: true, ShortNames: []string{"ing"}, Singular: "ingress"},
		{GVR: kube.HTTPRoutes, Kind: "HTTPRoute", Namespaced: true, Singular: "httproute"},
		{GVR: kube.ExternalSecrets, Kind: "ExternalSecret", Namespaced: true, ShortNames: []string{"es"}, Singular: "externalsecret"},
		{GVR: kube.Applications, Kind: "Application", Namespaced: true, ShortNames: []string{"app", "apps"}, Singular: "application"},
		{GVR: kube.Events, Kind: "Event", Namespaced: true, ShortNames: []string{"ev"}, Singular: "event"},
	}
	lists := map[schema.GroupVersionResource]string{}
	for _, k := range kinds {
		lists[k.GVR] = k.Kind + "List"
	}
	ns := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "shop"}}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, ns)
	res := kube.NewResources(kinds)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	m.enr = enrich.New(m.client.Cache, res, nil)
	m.client.Cache.Watch(kube.Namespaces)
	for end := time.Now().Add(5 * time.Second); !m.client.Cache.Synced(kube.Namespaces) && time.Now().Before(end); {
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(m.client.Cache.Stop)
	return m
}

// typeCommand types line at the ":" prompt and presses enter.
func typeCommand(m *Model, line string) {
	m.mode = modeCommand
	m.input.SetValue(line)
	m.sugs, m.sugSel = m.suggest(line), -1
	m.flash, m.flashErr = "", false
	m.onKey(tea.KeyMsg{Type: tea.KeyEnter})
}

// Round 130: ":cron -A" said there's nothing called "cron" while ":cron"
// and ":cronjobs -A" worked. Every word the guide and the kind table give
// takes -A, -n and a namespace, in any order, as it opens alone.
func TestCommandWordsTakeNamespaceFlags(t *testing.T) {
	var words []string
	for w := range synonyms {
		words = append(words, w)
	}
	for w := range aliases {
		words = append(words, w)
	}
	words = append(words, "cj", "deploy", "ing", "po", "no", "es", "cronjob", "deployments")
	forms := []struct{ suffix, ns string }{
		{" -A", ""}, {" --all-namespaces", ""}, {" -n shop", "shop"}, {" --namespace shop", "shop"},
		{" shop", "shop"}, {" -l app=web -A", ""}, {" -A -l app=web", ""}, {" -l app=web -n shop", "shop"},
	}
	for _, w := range words {
		if w == "releases" || w == "history" || w == "cost" || w == "costs" {
			continue // kubed's own screens, checked below
		}
		alone := kindsModel(t)
		alone.namespace, alone.chosenNS = "default", "default"
		typeCommand(alone, w)
		if alone.flashErr {
			t.Errorf(":%s alone: %s", w, alone.flash)
			continue
		}
		for _, f := range forms {
			m := kindsModel(t)
			m.namespace, m.chosenNS = "default", "default"
			typeCommand(m, w+f.suffix)
			if m.flashErr {
				t.Errorf(":%s%s: %s", w, f.suffix, m.flash)
				continue
			}
			if m.loc.kind != alone.loc.kind || m.loc.res.GVR != alone.loc.res.GVR {
				t.Errorf(":%s%s opened %v %v, alone it opens %v %v", w, f.suffix, m.loc.kind, m.loc.res.GVR, alone.loc.kind, alone.loc.res.GVR)
			}
			if m.loc.kind == locResource && m.namespace != f.ns {
				t.Errorf(":%s%s: namespace %q, want %q", w, f.suffix, m.namespace, f.ns)
			}
		}
	}
	// kubed's own screens, reached by another word, open as their word does.
	for _, w := range []string{"history", "cost", "costs"} {
		m := kindsModel(t)
		typeCommand(m, w+" -A")
		if m.flashErr && strings.Contains(m.flash, "nothing called") {
			t.Errorf(":%s -A: %s", w, m.flash)
		}
	}
}

// Every word runCommand acts on itself is kept from being completed to
// another word when it has arguments.
func TestOwnWordsMatchRunCommand(t *testing.T) {
	src, err := os.ReadFile("keys.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func (m *Model) runCommand(")
	end := strings.Index(body[start:], "\n}\n")
	for _, line := range strings.Split(body[start:start+end], "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "case \"") {
			continue
		}
		words := line[len("case "):strings.Index(line, ":")]
		for _, w := range strings.Split(words, ",") {
			if w = strings.Trim(strings.TrimSpace(w), `"`); !ownWords[w] {
				t.Errorf("runCommand acts on %q but ownWords doesn't have it", w)
			}
		}
	}
}

// Round 130: l on a pod that no node has taken (a job's only pod, Pending)
// said the log stream ended and r reads it again. The container hasn't
// started, so there are no logs yet; that's what it says.
func TestLogsOfAPodWaitingForANode(t *testing.T) {
	var cluster dynamic.Interface
	m := testModelOn(func(d dynamic.Interface) dynamic.Interface { cluster = d; return d })
	defer m.client.Cache.Stop()
	m.width, m.height = 120, 30
	m.client.Cache.Watch(kube.Pods)
	pending := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "neverran-x7k2p", "namespace": "kubed-qa130", "uid": "nr"},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "c"}}},
		"status": map[string]any{"phase": "Pending", "conditions": []any{map[string]any{"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
			"message": "0/3 nodes are available: 3 Insufficient cpu."}}}}}
	if _, err := cluster.Resource(kube.Pods).Namespace("kubed-qa130").Create(context.Background(), pending, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, ok := m.client.Cache.Get(kube.Pods, "kubed-qa130", "neverran-x7k2p"); ok {
			break
		}
	}
	for _, src := range []logSource{
		{ns: "kubed-qa130", pod: "neverran-x7k2p", container: "c"},                                   // l on the pod
		{ns: "kubed-qa130", workload: "job/neverran", streams: [][2]string{{"neverran-x7k2p", "c"}}}, // Logs (all pods)
	} {
		ch := make(chan string)
		m.pager = newPager("logs", "", 100, 20)
		m.pager.lines, m.pager.isLogs, m.pager.follow, m.pager.source, m.pager.logs = nil, true, true, src, ch // as openLogs
		m.mode = modePager
		m.Update(logLinesMsg{ch: ch, done: true})
		got := ansi.Strip(strings.Join(m.pager.lines, "\n") + "\n" + m.renderBody())
		if strings.Contains(got, "stream ended") || strings.Contains(got, "r reads") || strings.Contains(got, "· ended") ||
			!strings.Contains(got, "hasn't started yet") || !strings.Contains(got, "waiting for a node") {
			t.Errorf("%s: %s", src.workload+src.pod, got)
		}
	}
}

// Round 130: u's line said "(ArgoCD may put Git's version back)" on a
// deployment ArgoCD doesn't manage. It says so only when ArgoCD manages it
// and how it syncs is unknown; that it will, when it heals; else nothing.
func TestUndoLineSaysArgoOnlyWhenItManages(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	var u action
	for _, a := range actions {
		if a.key == "u" && strings.HasPrefix(a.about, "Go back to the previous version") {
			u = a
		}
	}
	dep := func(annotations map[string]any) view.Row {
		return view.Row{Obj: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "goodold", "namespace": "kubed-qa130", "uid": "g", "annotations": annotations}}}}
	}
	for _, c := range []struct {
		row  view.Row
		want string
	}{
		{dep(nil), "Go back to the previous version"},
		{dep(map[string]any{"argocd.argoproj.io/tracking-id": "shop:apps/Deployment:kubed-qa130/goodold"}), "Go back to the previous version (ArgoCD may put Git's version back)"},
	} {
		it := menuItem{key: "u", about: u.about}
		u.item(m, c.row, &it)
		if it.about != c.want {
			t.Errorf("%v: %q, want %q", c.row.Obj.GetAnnotations(), it.about, c.want)
		}
	}
	if strings.Contains(u.about, "ArgoCD") {
		t.Errorf("u's default line, shown when it is off, says ArgoCD: %q", u.about)
	}
	plain := "Put its deployment back on the version before"
	home := plain + " (ArgoCD may put Git's version back)"
	for _, c := range []struct {
		argo *enrich.ArgoApp
		want string
	}{
		{nil, "Put its deployment back on the version before"},
		{&enrich.ArgoApp{Name: "shop"}, home},
		{&enrich.ArgoApp{Name: "shop", Found: true, AutoSync: true}, "Put its deployment back on the version before"},
		{&enrich.ArgoApp{Name: "shop", Found: true, AutoSync: true, SelfHeal: true}, selfHealUndo(plain)},
	} {
		if got := argoUndo(plain, c.argo); got != c.want {
			t.Errorf("%+v: %q, want %q", c.argo, got, c.want)
		}
	}
}

// Review of round 130: with arguments, a word is completed only to a kind
// or to what it is a synonym of, never by its first letters to one of
// kubed's own commands. ":c prod" must not become ":ctx prod" (another
// cluster), nor ":n shop" ":ns shop" (the session's namespace).
func TestArgsNeverCompleteToOwnCommands(t *testing.T) {
	for _, line := range []string{"c prod", "n shop", "p web", "m x", "s x", "h x", "w x"} { // each refused, or a kind's list in a namespace that isn't there
		m := kindsModel(t)
		m.namespace, m.chosenNS = "default", "default"
		before := m.loc.kind
		typeCommand(m, line)
		if !m.flashErr || m.loc.kind != before {
			t.Errorf(":%s ran something: flash %q, screen %v (was %v)", line, m.flash, m.loc.kind, before)
		}
		if m.chosenNS != "default" {
			t.Errorf(":%s changed the namespace to %q", line, m.chosenNS)
		}
	}
}
