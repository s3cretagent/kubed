package ui

import (
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// findKinds are what "f" searches, most useful first, with the word shown.
var findKinds = []struct {
	gvr  schema.GroupVersionResource
	word string
}{
	{kube.Applications, "app"},
	{kube.Deployments, "deployment"},
	{kube.StatefulSets, "statefulset"},
	{kube.DaemonSets, "daemonset"},
	{kube.CronJobs, "cronjob"},
	{kube.Services, "service"},
	{kube.Ingresses, "ingress"},
	{kube.HTTPRoutes, "route"},
	{kube.ExternalSecrets, "externalsecret"},
	{kube.Jobs, "job"},
	{kube.Namespaces, "namespace"},
	// Pods and their settings come after what made them: "promtail" should
	// show the daemonset before its 30 pods.
	{kube.Pods, "pod"},
	{kube.ConfigMaps, "configmap"},
	{kube.Secrets, "secret"},
	{kube.Nodes, "node"},
	{kube.NodePools, "nodepool"},
}

// maxFound bounds the result list.
const maxFound = 200

var findHeader = []string{"WHAT", "NAME", "NAMESPACE"}

// startFind opens the find view; typing narrows it live.
func (m *Model) startFind() {
	if m.client == nil {
		m.setFlash("", fmt.Errorf("not connected to a cluster, so there is nothing to find yet: pick a cluster first"))
		return
	}
	m.refreshFindApps()
	if m.loc.kind == locFind { // back on the results: type again
		m.tbl.filter = ""                             // a new search starts whole: a filter on the old results would hide it
		m.startInput(modeFind, "find: ", m.loc.query) // the search, not a filter typed since
		m.input.Placeholder = "type a name: apps, pods, deployments, services, secrets, nodes, namespaces…"
		m.refreshRows() // the whole results at once, not a second later
		return
	}
	for _, k := range findKinds {
		if gvr, ok := m.client.Resources.Served(k.gvr); ok {
			m.client.Cache.Watch(gvr)
		}
	}
	// A search started from a search result replaces that search, so the
	// trail doesn't grow: esc still goes back to where you started.
	push := true
	for i := len(m.history) - 1; i >= 0; i-- {
		if m.history[i].kind == locFind {
			m.loc, m.history, push = m.history[i], m.history[:i], false // the new search takes its place
			break
		}
	}
	m.navigate(location{kind: locFind}, push)
	m.resumeFind("", 0)
}

// refreshFindApps looks at the apps again for find, which lists them as
// home does: an app whose pods came since home last looked is found too.
// Like home, it lists the apps of the namespace you are in (m.namespace).
func (m *Model) refreshFindApps() {
	if m.client != nil && m.enr != nil && m.homeLoading() == "" {
		m.refreshApps()
	}
}

// findRows ranks everything against the query. Every word of the query
// must match the name, namespace or kind; name matches rank highest.
func (m *Model) findRows(query string) []view.Row {
	words := strings.Fields(strings.ToLower(query))
	type hit struct {
		row   view.Row
		score int
	}
	var hits []hit
	appNS := map[string]string{} // where home says each app runs
	for _, a := range m.apps {
		appNS[a.Name] = a.Namespace
	}
	// Apps home lists that aren't ArgoCD apps (grouped by label or
	// namespace): found as apps too, first.
	for _, a := range m.apps {
		if a.Argo != nil {
			continue // found below, from its Application
		}
		score := findScore(words, strings.ToLower(a.Name), strings.ToLower(a.Namespace), "app")
		if score < 0 {
			continue
		}
		tone := view.ToneNormal
		if score >= looseMark {
			score, tone = score%looseMark-looseMark, view.ToneDim
		}
		found := view.Named(a.Namespace, a.Name)
		found.SetKind("Application") // opens like one: on the apps screen
		hits = append(hits, hit{view.Row{Obj: found, Tone: tone, Cells: []string{"app", a.Name, a.Namespace}}, score*100 + 1})
	}
	for rank, k := range findKinds {
		gvr, ok := m.client.Resources.Served(k.gvr)
		if !ok {
			continue
		}
		res, _ := m.client.Resources.ByGVR(gvr)
		for _, o := range m.client.Cache.List(gvr, "") {
			ns := o.GetNamespace()
			if res.Kind == "Application" { // where the app runs, not where ArgoCD keeps its record
				ns = appNamespace(o, appNS[o.GetName()])
			}
			name := strings.ToLower(o.GetName())
			switch res.Kind { // its web addresses count as its name: find a host, get its ingress or route
			case "Ingress":
				for _, h := range hostsOf(o) {
					name += " " + strings.ToLower(h)
				}
			case "HTTPRoute":
				hosts, _, _ := unstructured.NestedStringSlice(o.Object, "spec", "hostnames")
				for _, h := range hosts {
					name += " " + strings.ToLower(h)
				}
			}
			score := findScore(words, name, strings.ToLower(ns), k.word)
			if score < 0 {
				continue
			}
			loose := score >= looseMark // a letter-by-letter match: shown after, greyed
			if loose {
				score = score%looseMark - looseMark // below every real match
			}
			found := view.Named(o.GetNamespace(), o.GetName())
			found.SetAPIVersion(res.GVR.GroupVersion().String())
			found.SetKind(res.Kind)
			tone := view.ToneNormal
			if loose {
				tone = view.ToneDim
			}
			hits = append(hits, hit{view.Row{Obj: found, Tone: tone, Cells: []string{k.word, o.GetName(), ns}},
				score*100 - rank})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		if len(words) == 0 { // nothing typed yet: alphabetical within each kind
			return hits[i].row.Cells[1] < hits[j].row.Cells[1]
		}
		return len(hits[i].row.Cells[1]) < len(hits[j].row.Cells[1])
	})
	// Close matches (letters in order) only help when real ones are few.
	named := 0 // matches by name, not by letters in order
	for _, h := range hits {
		if h.row.Tone != view.ToneDim {
			named++
		}
	}
	rows := make([]view.Row, 0, min(len(hits), maxFound))
	for i := 0; i < len(hits) && len(rows) < maxFound; i++ {
		if named >= 5 && hits[i].row.Tone == view.ToneDim {
			break
		}
		rows = append(rows, hits[i].row)
	}
	m.foundAll = len(hits)
	if named >= 5 { // the close matches left out aren't "more results"
		m.foundAll = named
	}
	return rows
}

// looseMark is added for a letter-by-letter match, so a score at or above
// it was only a close match.
const looseMark = 1000

// findScore: -1 when a word matches nothing; otherwise higher is better.
func findScore(words []string, name, ns, kind string) int {
	if len(words) == 0 {
		return 0
	}
	total := 0
	for _, w := range words {
		switch {
		case name == w:
			total += 10
		case strings.HasPrefix(name, w):
			total += 7
		case strings.Contains(name, w):
			total += 5
		case strings.Contains(ns, w):
			total += 3
		case kind == w || strings.HasPrefix(kind, w):
			total += 2
		case subsequence(name, w):
			total += looseMark + 1
		default:
			return -1
		}
	}
	return total
}

// subsequence: every letter of w appears in s, in order ("pmtapi" ~ "payment-api").
func subsequence(s, w string) bool {
	i := 0
	for _, r := range s {
		if i < len(w) && rune(w[i]) == r {
			i++
		}
	}
	return i == len(w)
}

// openFound jumps to the found object's list, selects it and opens its menu.
func (m *Model) openFound(row view.Row) tea.Cmd {
	o := row.Obj
	if o.GetKind() == "Application" { // as on home: the app selected, with its menu
		homeApp := false // grouped by home as it is now, not an ArgoCD app
		for _, a := range m.apps {
			if a.Name == o.GetName() && a.Namespace == o.GetNamespace() && a.Argo == nil {
				homeApp = true
			}
		}
		if !homeApp {
			// An ArgoCD app (kept in ArgoCD's own namespace): look across
			// all. One home grouped in a namespace: in that namespace, as it
			// was found (across all namespaces home groups apps differently).
			if homeGrouped := o.GetAPIVersion() == ""; !homeGrouped || o.GetNamespace() == "" { // an ArgoCD Application carries its apiVersion; home's own app rows don't
				m.enterNamespace("")
			} else {
				m.enterNamespace(o.GetNamespace())
			}
		}
		m.navigate(location{kind: locHome, fromFind: true}, true)
		m.refreshRows()
		m.selectNamed(o.GetName())
		m.tbl.touched = true // the cursor stays on this app as the list refreshes
		if row, ok := m.tbl.selected(); ok && row.Obj.GetName() == o.GetName() {
			return m.foundMenu()
		}
		if o.GetAPIVersion() == "" && o.GetNamespace() != "" { // home's own app, not listed here (yet): say so where it lives
			m.setFlash("", fmt.Errorf("app %s isn't listed in %s now (its pods may be gone)", o.GetName(), o.GetNamespace()))
			return nil
		}
		return m.openAppNamed(o.GetName())
	}
	gv, _ := schema.ParseGroupVersion(o.GetAPIVersion())
	res, ok := m.client.Resources.ByKind(gv.Group, o.GetKind())
	if !ok {
		return nil
	}
	if o.GetKind() == "Namespace" { // its apps, on this screen only; esc comes back here
		m.enterNamespace(o.GetName())
		m.navigate(location{kind: locHome, fromFind: true}, true)
		return nil
	}
	if res.Namespaced {
		m.enterNamespace(o.GetNamespace())
	}
	// esc comes back to these search results.
	m.navigate(location{kind: locResource, res: res, filter: o.GetName(), fromFind: true}, true)
	m.selectNamed(o.GetName())
	return m.foundMenu()
}

// foundMenu opens a search result's menu; esc on it goes back to the results.
func (m *Model) foundMenu() tea.Cmd {
	cmd := m.menu()
	if m.mode == modePrompt && m.prompt != nil {
		m.prompt.foundMenu = true
	}
	return cmd
}

// resumeFind shows the find box with a search typed in and its results.
func (m *Model) resumeFind(query string, cursor int) {
	m.startInput(modeFind, "find: ", query)
	m.loc.query = query
	m.input.Placeholder = "type a name: apps, pods, deployments, services, secrets, nodes, namespaces…"
	m.refreshRows()
	m.tbl.jump(min(cursor, max(len(m.tbl.rows)-1, 0)))
}

// showFound brings back earlier search results to pick from, not typing:
// keys work as on any list, and f types again.
func (m *Model) showFound(query string, cursor int) {
	m.resumeFind(query, cursor)
	m.mode = modeTable
	m.input.Blur()
}

// selectNamed moves the cursor to the row with exactly this name.
func (m *Model) selectNamed(name string) {
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == name {
			m.tbl.jump(i)
			return
		}
	}
}

// openWorkload jumps to an object's own list, selected, with its menu open.
func (m *Model) openWorkload(o *unstructured.Unstructured) tea.Cmd {
	gv, _ := schema.ParseGroupVersion(o.GetAPIVersion())
	res, ok := m.client.Resources.ByKind(gv.Group, o.GetKind())
	if !ok {
		return nil
	}
	if res.Namespaced {
		m.enterNamespace(o.GetNamespace())
	}
	m.navigate(location{kind: locResource, res: res, filter: o.GetName()}, true)
	m.selectNamed(o.GetName())
	return m.menu()
}

// appNamespace is where an ArgoCD app's things run: the namespace most of
// its resources are in (its destination is often just "default"), else
// where home found its pods, else its destination.
func appNamespace(app *unstructured.Unstructured, home string) string {
	count := map[string]int{}
	best := ""
	res, _, _ := unstructured.NestedSlice(app.Object, "status", "resources")
	for _, r := range res {
		rm, _ := r.(map[string]any)
		ns, _ := rm["namespace"].(string)
		if ns == "" {
			continue
		}
		count[ns]++
		if count[ns] > count[best] {
			best = ns
		}
	}
	switch {
	case best != "":
		return best
	case home != "":
		return home
	}
	dest, _, _ := unstructured.NestedString(app.Object, "spec", "destination", "namespace")
	return dest
}

// hostsOf lists an Ingress's hosts.
func hostsOf(o *unstructured.Unstructured) []string {
	var hosts []string
	rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "rules")
	for _, r := range rules {
		if h, ok := r.(map[string]any)["host"].(string); ok && h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}
