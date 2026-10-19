package ui

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// refreshApps recomputes home's app list from the cache.
func (m *Model) refreshApps() {
	list := func(g schema.GroupVersionResource) []*unstructured.Unstructured {
		if gvr, ok := m.client.Resources.Served(g); ok {
			return m.client.Cache.List(gvr, m.namespace)
		}
		return nil
	}
	var workloads []*unstructured.Unstructured
	for _, g := range []schema.GroupVersionResource{kube.Deployments, kube.StatefulSets, kube.CronJobs} {
		workloads = append(workloads, list(g)...)
	}
	for _, ds := range list(kube.DaemonSets) { // one on no node has no pods to list it by
		if n, _, _ := unstructured.NestedInt64(ds.Object, "status", "desiredNumberScheduled"); n == 0 {
			workloads = append(workloads, ds)
		}
	}
	m.apps = view.Apps(m.enr, list(kube.Pods), list(kube.Applications), list(kube.ExternalSecrets), list(kube.Jobs), workloads)
	if m.namespace != "" { // Argo apps live in "argocd"; keep the ones deploying here
		m.apps = filterApps(m.apps, m.namespace)
	}
	m.home = m.enr.Home(time.Now())
}

func filterApps(apps []*view.App, ns string) []*view.App {
	var out []*view.App
	for _, a := range apps {
		if a.Namespace == ns {
			out = append(out, a)
		}
	}
	return out
}

func (m *Model) appFor(row view.Row) (*view.App, bool) {
	for _, a := range m.apps {
		if a.Name == row.Obj.GetName() && a.Namespace == row.Obj.GetNamespace() {
			return a, true
		}
	}
	return nil, false
}

// openApp shows the app's pods; their detail pane says why each is unwell.
func (m *Model) openApp(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	return m.openAppPage(a)
}

// openAppNamed opens an app found with f (an ArgoCD Application).
func (m *Model) openAppNamed(name string) tea.Cmd {
	m.enterNamespace("")
	m.refreshApps()
	for _, a := range m.apps {
		if a.Name == name {
			return m.openAppPage(a)
		}
	}
	m.setFlash("", fmt.Errorf("app %s has no pods yet", name))
	return nil
}

func (m *Model) openAppPage(a *view.App) tea.Cmd {
	res, ok := m.client.Resources.ByGVR(kube.Pods)
	if !ok {
		return nil
	}
	m.enterNamespace(a.Namespace)
	m.navigate(location{kind: locResource, res: res, app: a.Name, drill: &view.Drill{
		GVR: kube.Pods, Title: "of app " + a.Name, Match: view.AppPods(m.enr, a),
	}}, true)
	return nil
}

// appPodRows are an app's pods as rows of the pods view.
func (m *Model) appPodRows(a *view.App) []view.Row {
	res, _ := m.client.Resources.ByGVR(kube.Pods)
	match := view.AppPods(m.enr, a)
	var rows []view.Row
	for _, p := range m.client.Cache.List(res.GVR, a.Namespace) {
		if match(p) {
			rows = append(rows, view.Row{Obj: p, Cells: []string{p.GetName()}})
		}
	}
	return rows
}

// appLogs follows every pod of the app.
func (m *Model) appLogs(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	if m.appLogsNeverRan(a, false) {
		m.flashNeverRan(a.Name)
		return nil
	}
	podsNow := func() ([][2]string, int) {
		var streams [][2]string
		app := a
		if fresh, ok := m.appNamed(a.Namespace, a.Name); ok { // the app as listed now
			app = fresh
		}
		pods := m.appPodRows(app)
		for _, r := range pods {
			for _, c := range view.Containers(r.Obj) {
				streams = append(streams, [2]string{r.Obj.GetName(), c})
			}
			if podCount(streams) >= maxLogPods || len(streams) >= maxLogStreams { // pods, not containers: several per pod is usual
				break
			}
		}
		return streams, len(pods)
	}
	streams, all := podsNow()
	if len(streams) == 0 && neverStarted(a) { // its job's pods gone: they never ran (other pods' logs still open)
		m.flashNeverRan(a.Name)
		return nil
	}
	if len(streams) == 0 {
		m.setFlash("", fmt.Errorf("%s has no pods", a.Name))
		return nil
	}
	return m.openLogs(logSource{ns: a.Namespace, streams: streams, workload: "app/" + a.Name, allPods: all, pods: podsNow})
}

// appEvents lists the events in the app's namespace, newest first.
// appNamed is an app as the apps list has it now.
func (m *Model) appNamed(ns, name string) (*view.App, bool) {
	for _, a := range m.apps {
		if a.Namespace == ns && a.Name == name {
			return a, true
		}
	}
	return nil, false
}

func (m *Model) appEvents(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	own := m.appObjects(a)
	// Pods already gone (a failed rollout's, replaced since) still say why
	// it failed: theirs are named after what made them ("web-7f9c-xk2p").
	var makers []string
	for k := range own {
		for _, kind := range []string{"replicaset/", "statefulset/", "daemonset/", "job/"} {
			if name, ok := strings.CutPrefix(k, kind); ok {
				makers = append(makers, name+"-")
			}
		}
	}
	return m.eventsWhere(a.Namespace, "events of app "+a.Name, func(ev *unstructured.Unstructured) bool {
		kind, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "kind")
		name, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
		if own[strings.ToLower(kind)+"/"+name] {
			return true
		}
		if kind == "Pod" {
			for _, p := range makers {
				if strings.HasPrefix(name, p) {
					return true
				}
			}
		}
		return false
	})
}

// appObjects are what belongs to an app, as "kind/name": its pods, what
// runs them (ReplicaSet, Deployment, Job, CronJob…), their autoscalers, and
// the ConfigMaps and Secrets the pods use. App events and changes show
// only these, not the rest of a shared namespace.
func (m *Model) appObjects(a *view.App) map[string]bool {
	own := map[string]bool{}
	for _, r := range m.appPodRows(a) {
		cur := r.Obj
		for range 4 {
			own[strings.ToLower(cur.GetKind())+"/"+cur.GetName()] = true
			for _, ref := range cur.GetOwnerReferences() {
				own[strings.ToLower(ref.Kind)+"/"+ref.Name] = true
			}
			next := m.enr.TopOwner(cur)
			if next == cur {
				break
			}
			cur = next
		}
		own["horizontalpodautoscaler/"+cur.GetName()] = true
		for _, ref := range enrich.PodConfigRefs(r.Obj) {
			own[ref] = true
		}
	}
	// Its workloads and their ReplicaSets, pods or not: "can't create pods"
	// is said on the ReplicaSet when no pod exists to say it.
	workloads := map[string]bool{}
	for _, w := range m.appWorkloads(a) {
		key := strings.ToLower(w.Obj.GetKind()) + "/" + w.Obj.GetName()
		own[key], workloads[w.Obj.GetNamespace()+"/"+key] = true, true
		own["horizontalpodautoscaler/"+w.Obj.GetName()] = true
	}
	if gvr, ok := m.client.Resources.Served(kube.ReplicaSets); ok && len(workloads) > 0 {
		for _, rs := range m.client.Cache.List(gvr, a.Namespace) {
			for _, ref := range rs.GetOwnerReferences() {
				if workloads[rs.GetNamespace()+"/"+strings.ToLower(ref.Kind)+"/"+ref.Name] {
					own["replicaset/"+rs.GetName()] = true
				}
			}
		}
	}
	if a.Argo != nil { // what ArgoCD manages for it belongs to it, pods or not
		for _, r := range a.Argo.Resources {
			own[strings.ToLower(r.Kind)+"/"+r.Name] = true
		}
	} else {
		// Its cron jobs and jobs, their pods gone too (between runs, or a
		// failed job's pods cleaned up): no pods to find them by.
		for _, k := range []struct {
			gvr  schema.GroupVersionResource
			kind string
		}{{kube.CronJobs, "cronjob/"}, {kube.Jobs, "job/"}} {
			gvr, ok := m.client.Resources.Served(k.gvr)
			if !ok {
				continue
			}
			for _, o := range m.client.Cache.List(gvr, a.Namespace) {
				if a.OwnsWorkload(m.enr, o) {
					own[k.kind+o.GetName()] = true
				}
			}
		}
	}
	return own
}

// runningWorkloads leaves out deployments and statefulsets scaled to 0.
func runningWorkloads(wl []view.Row) []view.Row {
	var out []view.Row
	for _, w := range wl {
		if r, found, _ := unstructured.NestedInt64(w.Obj.Object, "spec", "replicas"); found && r == 0 {
			continue
		}
		out = append(out, w)
	}
	return out
}

// neverStarted: the app's box is about a program that never started, so
// that pod has no logs.
func neverStarted(a *view.App) bool {
	return a.Status.Short == "Can't start" || view.NeverRan(a.Status.Why)
}

// appLogsNeverRan: l on the app has no logs to show because its program
// never ran: its box says so outright and no other pod of it ever started
// a container, or it has no pods left and the box's cause says it never
// ran. appLogs and its menu line both ask it.
func (m *Model) appLogsNeverRan(a *view.App, noPods bool) bool {
	if a.Status.Short == "Can't start" || strings.Contains(a.Status.Why, "it never started") {
		return !slices.ContainsFunc(m.appPodRows(a), func(r view.Row) bool { return view.PodRan(r.Obj) }) // the box is about one pod: the others' logs still open
	}
	return noPods && neverStarted(a)
}

// neverRanErr is why there are no logs of a program that never ran: the
// menu line's words, and the flash's before "The box says why".
func neverRanErr(name string) error {
	return fmt.Errorf("%s never started its program, so it has no logs", name)
}

// flashNeverRan is what l and p say, on every screen, about a program
// that never ran.
func (m *Model) flashNeverRan(name string) {
	m.setFlash("", fmt.Errorf("%w. The box says why", neverRanErr(name)))
}

// ranBeforeRestart is the most restarted container, among pods, that ran
// before it restarted, so has earlier logs (not one that never started:
// exit 128); ok is false when there is none.
func ranBeforeRestart(rows []view.Row) (src logSource, ok bool) {
	most := int64(0)
	for _, r := range rows {
		statuses, _, _ := unstructured.NestedSlice(r.Obj.Object, "status", "containerStatuses")
		inits, _, _ := unstructured.NestedSlice(r.Obj.Object, "status", "initContainerStatuses")
		for _, s := range append(statuses, inits...) {
			m, _ := s.(map[string]any)
			n, _, _ := unstructured.NestedInt64(m, "restartCount")
			reason, _, _ := unstructured.NestedString(m, "lastState", "terminated", "reason")
			code, _, _ := unstructured.NestedInt64(m, "lastState", "terminated", "exitCode")
			if n > most && reason != "StartError" && reason != "ContainerCannotRun" && code != 128 {
				name, _, _ := unstructured.NestedString(m, "name")
				src, ok, most = logSource{ns: r.Obj.GetNamespace(), pod: r.Obj.GetName(), container: name}, true, n
			}
		}
	}
	return src, ok
}

// appPreviousLogs shows the logs from before the last restart of the app's
// most restarted pod.
func (m *Model) appPreviousLogs(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	if neverStarted(a) {
		// The pod the box is about has no logs: another pod's restart may.
		if src, ok := ranBeforeRestart(m.appPodRows(a)); ok {
			return m.openLogs(m.endedRun(src)) // that container, not the one that never started
		}
		m.flashNeverRan(a.Name)
		return nil
	}
	if a.PrevLogsOfHead() { // the pod a crash headline (and its Fix line) is about, when it has earlier logs
		for _, r := range m.appPodRows(a) {
			if r.Obj.GetName() == a.HeadPod && r.Obj.GetNamespace() == a.HeadNS && podRestarted(r.Obj) {
				return m.logs(r, true)
			}
		}
	}
	if a.CrashPod != "" { // the crash its box quotes, so the two agree
		for _, r := range m.appPodRows(a) {
			if r.Obj.GetName() == a.CrashPod && r.Obj.GetNamespace() == a.CrashNS {
				return m.logs(r, true)
			}
		}
	}
	return m.previousLogsOf(a.Name, m.appPodRows(a))
}

// workloadPreviousLogs is p on a deployment, stateful set…: the logs from
// before the last restart of its most restarted pod, as on an app.
func (m *Model) workloadPreviousLogs(row view.Row) tea.Cmd {
	var rows []view.Row
	restarted := false
	for _, p := range m.enr.PodsOf(row.Obj) {
		rows = append(rows, view.Row{Obj: p})
		restarted = restarted || podRestarted(p)
	}
	if !restarted && m.neverStarted(row.Obj) { // as l says, its pods gone or not
		return nil
	}
	policy, _, _ := unstructured.NestedString(row.Obj.Object, "spec", "template", "spec", "restartPolicy")
	if row.Obj.GetKind() == "Job" && policy == "Never" && len(rows) > 0 && !restarted { // OnFailure restarts in place, as any pod
		// Each try is a new pod (restartPolicy Never): a failed try's logs
		// are its own pod's, ended, not "live", and nothing came before.
		m.setFlash("", fmt.Errorf("%s runs each try in a new pod, so no pod has logs from before a restart. l shows each pod's logs, failed tries too", row.Obj.GetName()))
		return nil
	}
	return m.previousLogsOf(row.Obj.GetName(), rows)
}

// previousLogsOf opens the earlier logs of the most restarted of pods.
func (m *Model) previousLogsOf(name string, pods []view.Row) tea.Cmd {
	var best view.Row
	most := int64(0)
	for _, r := range pods {
		statuses, _, _ := unstructured.NestedSlice(r.Obj.Object, "status", "containerStatuses")
		inits, _, _ := unstructured.NestedSlice(r.Obj.Object, "status", "initContainerStatuses")
		for _, s := range append(statuses, inits...) { // a setup step that keeps failing has earlier logs too
			n, _, _ := unstructured.NestedInt64(s.(map[string]any), "restartCount")
			if n > most {
				best, most = r, n
			}
		}
	}
	if len(pods) == 0 { // none now (a job's removed, scaled to 0, never made): nothing to read, live or earlier
		m.setFlash("", fmt.Errorf("no pod of %s exists right now, so there are no logs to show", name))
		return nil
	}
	if most == 0 {
		m.setFlash("", fmt.Errorf("no pod of %s has restarted, so there are no earlier logs. l shows the live ones", name))
		return nil
	}
	return m.logs(best, true)
}

// appWorkloads are the Deployments / StatefulSets / DaemonSets running the
// app's pods.
func (m *Model) appWorkloads(a *view.App) []view.Row {
	seen := map[string]bool{}
	var rows []view.Row
	for _, r := range m.appPodRows(a) {
		top := m.enr.TopOwner(r.Obj)
		if top == r.Obj || seen[top.GetKind()+"/"+top.GetName()] {
			continue
		}
		switch top.GetKind() {
		case "Deployment", "StatefulSet", "DaemonSet":
			seen[top.GetKind()+"/"+top.GetName()] = true
			rows = append(rows, view.Row{Obj: top, Cells: []string{strings.ToLower(top.GetKind()) + " " + top.GetName()}})
		}
	}
	// An app with no pods has no pods to follow: its workloads are the ones
	// it was listed for (by label or namespace), or that ArgoCD manages.
	if a.Argo == nil { // also the ones scaled to 0, which have no pods to find them by
		for _, g := range []schema.GroupVersionResource{kube.Deployments, kube.StatefulSets, kube.DaemonSets} {
			gvr, ok := m.client.Resources.Served(g)
			if !ok {
				continue
			}
			for _, o := range m.client.Cache.List(gvr, a.Namespace) {
				if !seen[o.GetKind()+"/"+o.GetName()] && a.OwnsWorkload(m.enr, o) {
					seen[o.GetKind()+"/"+o.GetName()] = true
					rows = append(rows, view.Row{Obj: o, Cells: []string{strings.ToLower(o.GetKind()) + " " + o.GetName()}})
				}
			}
		}
	}
	if a.Argo != nil {
		gvrs := map[string]schema.GroupVersionResource{"Deployment": kube.Deployments, "StatefulSet": kube.StatefulSets, "DaemonSet": kube.DaemonSets}
		for _, r := range a.Argo.Resources {
			gvr, ok := gvrs[r.Kind]
			if !ok || seen[r.Kind+"/"+r.Name] {
				continue
			}
			if o, found := m.client.Cache.Get(gvr, r.Namespace, r.Name); found {
				seen[r.Kind+"/"+r.Name] = true
				rows = append(rows, view.Row{Obj: o, Cells: []string{strings.ToLower(r.Kind) + " " + r.Name}})
			}
		}
	}
	return rows
}

// appRestart rolls every workload of the app, after one confirm.
func (m *Model) appRestart(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	all := m.appWorkloads(a)
	if len(all) == 0 {
		m.setFlash("", fmt.Errorf("%s has nothing to restart", a.Name))
		return nil
	}
	wl := runningWorkloads(all) // one scaled to 0 has no pods to replace
	if len(wl) == 0 {
		m.setFlash("", fmt.Errorf("%s runs no copies, so there is nothing to restart. + scales it up", a.Name))
		return nil
	}
	var names []string
	for _, w := range wl {
		names = append(names, "  "+w.Cells[0])
	}
	var warnings []string
	for _, w := range wl {
		warnings = append(warnings, m.enr.Warnings(w.Obj, enrich.ActionRestart)...)
	}
	verb := "get"
	if len(wl) == 1 {
		verb = "gets"
	}
	// One that replaces no pod now (OnDelete, a partition holding back
	// every pod, paused) makes "get new pods" untrue: then the headline
	// only says they are marked to restart, and each line says what that does.
	allReplace := !slices.ContainsFunc(wl, func(w view.Row) bool { return !replacesNow(w.Obj) })
	// What a restart does to uptime, as r on the workload says: once, or
	// for each workload that goes down for a moment.
	var downtime []string
	for _, w := range wl {
		why := restartWhy(w.Obj)
		switch {
		case len(wl) == 1:
			downtime = append(downtime, why)
		case !allReplace, ownLine(w.Obj), strings.HasPrefix(why, "It is paused"):
			downtime = append(downtime, w.Cells[0]+": "+why)
		}
	}
	pace := " one pod at a time"
	for _, w := range wl {
		if ownLine(w.Obj) { // its own line says how
			pace = ""
		}
	}
	head := fmt.Sprintf("%s new pods, replacing the old ones%s", verb, pace)
	if !allReplace {
		head = map[bool]string{true: "is", false: "are"}[len(wl) == 1] + " marked to restart"
	}
	title := fmt.Sprintf("Restart app %s?\nIts %s %s:\n%s", a.Name, kindCounts(wl), head, strings.Join(names, "\n"))
	if len(downtime) > 0 {
		title += "\n" + strings.Join(downtime, "\n")
	}
	m.confirmNamed(title, a.Name, warnings, func() tea.Cmd {
		return func() tea.Msg {
			var failed []string
			for _, w := range wl {
				res, _ := m.client.Resources.ByKind("apps", w.Obj.GetKind())
				ctx, cancel := apiCtx()
				if err := m.client.RolloutRestart(ctx, kube.RefOf(res, w.Obj)); err != nil {
					failed = append(failed, w.Cells[0]+": "+err.Error())
				}
				cancel()
			}
			if len(failed) > 0 {
				return resultMsg{err: fmt.Errorf("restart: %s", strings.Join(failed, "; "))}
			}
			return resultMsg{text: appRestartText(a.Name, wl)}
		}
	})
	return nil
}

// appRestartText is what a restart of an app's workloads says once sent.
// A paused Deployment restarts only once resumed, so it is not counted as
// restarted (the count would claim it did).
func appRestartText(app string, wl []view.Row) string {
	var restarted []view.Row
	var paused []string
	for _, w := range wl {
		if pausedDeploy(w.Obj) {
			paused = append(paused, w.Cells[0])
		} else {
			restarted = append(restarted, w)
		}
	}
	text := fmt.Sprintf("restarted app %s (%s)", app, kindCounts(restarted))
	switch {
	case len(paused) == len(wl): // nothing restarts yet
		text = fmt.Sprintf("restart set for app %s: it happens once %s %s resumed", app, strings.Join(paused, ", "), map[bool]string{true: "is", false: "are"}[len(paused) == 1])
	case len(paused) > 0:
		text += fmt.Sprintf("; restart set for %s: %s once resumed", strings.Join(paused, ", "), map[bool]string{true: "it happens", false: "each happens"}[len(paused) == 1])
	}
	return text
}

// appDetails describes the ArgoCD Application, or the namespace.
func (m *Model) appDetails(row view.Row) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	if a.Argo != nil {
		if res, ok := m.client.Resources.ByGVR(kube.Applications); ok {
			ref := kube.Ref{Resource: res, Namespace: a.Argo.Namespace, Name: a.Name}
			about := aboutBlock(view.AppLines(a, view.Lookups{Secret: m.secretError, Argo: m.argoLookup, Crash: m.crashLookup, ReadOnly: m.opts.ReadOnly, Web: m.appWebNoted, Full: true}), m.pagerWidth())
			return func() tea.Msg {
				out, err := m.client.Describe(ref)
				return pagerMsg{title: "details of app " + a.Name, content: about + out, err: err}
			}
		}
	}
	// No ArgoCD record: describe what runs the app (its workloads), or
	// its pods when nothing does.
	about := aboutBlock(view.AppLines(a, view.Lookups{Secret: m.secretError, Argo: m.argoLookup, Crash: m.crashLookup, ReadOnly: m.opts.ReadOnly, Web: m.appWebNoted, Full: true}), m.pagerWidth())
	targets := m.appWorkloads(a)
	if len(targets) == 0 {
		targets = m.appPodRows(a)
	}
	var refs []kube.Ref
	for _, t := range targets {
		gv, _ := schema.ParseGroupVersion(t.Obj.GetAPIVersion())
		res, ok := m.client.Resources.ByKind(gv.Group, t.Obj.GetKind())
		if !ok {
			continue
		}
		refs = append(refs, kube.RefOf(res, t.Obj))
	}
	return func() tea.Msg {
		var b strings.Builder
		for i, ref := range refs {
			if i == 5 {
				fmt.Fprintf(&b, "\n… and %d more\n", len(refs)-5)
				break
			}
			out, err := m.client.Describe(ref)
			if err != nil {
				return pagerMsg{err: err}
			}
			if len(refs) > 1 {
				b.WriteString("── " + refWords(ref) + " ──\n\n")
			}
			b.WriteString(out + "\n")
		}
		return pagerMsg{title: "details of app " + a.Name, content: about + b.String()}
	}
}

// appOpenKind opens one kind (deployments, configmaps…) in the app's namespace.
func (m *Model) appOpenKind(row view.Row, kind string) tea.Cmd {
	a, ok := m.appFor(row)
	if !ok {
		return nil
	}
	own := m.appObjects(a)
	prefix := map[string]string{"deployments": "deployment/", "statefulsets": "statefulset/", "daemonsets": "daemonset/",
		"configmaps": "configmap/", "secrets": "secret/", "cronjobs": "cronjob/"}
	if kind == "deployments" {
		// The kind that runs the app; a mix opens the first and says so.
		var kinds []string
		for _, w := range m.appWorkloads(a) {
			k := strings.ToLower(w.Obj.GetKind()) + "s"
			if !slices.Contains(kinds, k) {
				kinds = append(kinds, k)
			}
		}
		if len(kinds) > 0 {
			kind = kinds[0]
		}
		if len(kinds) > 1 {
			defer m.setTip("this app also runs " + strings.Join(kinds[1:], " and ") + ", not shown in this list")
		}
	}
	res, ok := m.client.Resources.Lookup(kind)
	if !ok {
		return nil
	}
	p := prefix[kind]
	match := func(o *unstructured.Unstructured) bool { return own[p+o.GetName()] }
	m.enterNamespace(a.Namespace)
	m.navigate(location{kind: locResource, res: res, drill: &view.Drill{GVR: res.GVR, Title: "of app " + a.Name, Match: match}}, true)
	return nil
}

// kindCounts says what rows are, in words: "2 deployments and 1 statefulset".
func kindCounts(rows []view.Row) string {
	count := map[string]int{}
	var order []string
	for _, r := range rows {
		k := strings.ToLower(r.Obj.GetKind())
		if count[k] == 0 {
			order = append(order, k)
		}
		count[k]++
	}
	var parts []string
	for _, k := range order {
		if count[k] == 1 {
			parts = append(parts, "1 "+k)
		} else {
			parts = append(parts, fmt.Sprintf("%d %ss", count[k], k))
		}
	}
	if len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
	}
	return strings.Join(parts, "")
}

// appWeb lists the web addresses that reach an app: the hosts (and paths)
// of Ingresses whose backends are Services selecting its pods.
func (m *Model) appWeb(a *view.App) []string {
	svcGVR, ok1 := m.client.Resources.Served(kube.Services)
	ingGVR, ok2 := m.client.Resources.Served(kube.Ingresses)
	if !ok1 || !ok2 || a.Namespace == "" {
		return nil
	}
	m.client.Cache.Watch(svcGVR)
	m.client.Cache.Watch(ingGVR)
	pods := m.appPodRows(a)
	if len(pods) == 0 {
		return m.ownRouteHosts(a) // it may only hold routes to other apps
	}
	services := map[string]bool{}
	for _, s := range m.client.Cache.List(svcGVR, a.Namespace) {
		sel, _, _ := unstructured.NestedStringMap(s.Object, "spec", "selector")
		if len(sel) == 0 {
			continue
		}
		match := labels.SelectorFromSet(sel)
		for _, p := range pods {
			if match.Matches(labels.Set(p.Obj.GetLabels())) {
				services[s.GetName()] = true
				break
			}
		}
	}
	var hosts []string
	seen := map[string]bool{}
	add := func(addr string) {
		if addr != "" && !seen[addr] {
			seen[addr] = true
			hosts = append(hosts, addr)
		}
	}
	// Gateway API routes: hostnames for the services they send to, from
	// any namespace (a shared site's route often lives elsewhere), with
	// the path when a rule sends only part of the site here.
	if rtGVR, ok := m.client.Resources.Served(kube.HTTPRoutes); ok {
		m.client.Cache.Watch(rtGVR)
		// When two rules match the same host and path, the gateway sends it
		// to the older route (then by namespace/name), and within a route
		// to the earlier rule: go through them in that order.
		routes := m.client.Cache.List(rtGVR, "")
		sort.SliceStable(routes, func(i, j int) bool {
			ti, tj := routes[i].GetCreationTimestamp().Time, routes[j].GetCreationTimestamp().Time
			if !ti.Equal(tj) {
				return ti.Before(tj)
			}
			return routes[i].GetNamespace()+"/"+routes[i].GetName() < routes[j].GetNamespace()+"/"+routes[j].GetName()
		})
		claimed := map[string]bool{} // host + path, taken by an earlier route or rule
		for _, rt := range routes {
			names, _, _ := unstructured.NestedStringSlice(rt.Object, "spec", "hostnames")
			rules, _, _ := unstructured.NestedSlice(rt.Object, "spec", "rules")
			for _, r := range rules {
				rm, _ := r.(map[string]any)
				type hostPath struct{ host, path string }
				var mine []hostPath
				for _, h := range names {
					for _, p := range routePaths(rm) {
						if !claimed[h+p] {
							mine = append(mine, hostPath{h, p})
						}
						claimed[h+p] = true
					}
				}
				refs, _, _ := unstructured.NestedSlice(rm, "backendRefs")
				hits := false
				for _, ref := range refs {
					fm, _ := ref.(map[string]any)
					ns, _ := fm["namespace"].(string)
					if ns == "" {
						ns = rt.GetNamespace()
					}
					kind, _ := fm["kind"].(string)
					if name, _ := fm["name"].(string); ns == a.Namespace && (kind == "" || kind == "Service") && services[name] {
						hits = true
					}
				}
				if !hits {
					continue
				}
				for _, hp := range mine {
					add("https://" + hp.host + hp.path)
				}
			}
		}
	}
	for _, ing := range m.client.Cache.List(ingGVR, a.Namespace) {
		tls := map[string]bool{}
		tlsList, _, _ := unstructured.NestedSlice(ing.Object, "spec", "tls")
		for _, t := range tlsList {
			hs, _, _ := unstructured.NestedStringSlice(t.(map[string]any), "hosts")
			for _, h := range hs {
				tls[h] = true
			}
		}
		rules, _, _ := unstructured.NestedSlice(ing.Object, "spec", "rules")
		for _, r := range rules {
			rm, _ := r.(map[string]any)
			host, _ := rm["host"].(string)
			paths, _, _ := unstructured.NestedSlice(rm, "http", "paths")
			for _, p := range paths {
				pm, _ := p.(map[string]any)
				svc, _, _ := unstructured.NestedString(pm, "backend", "service", "name")
				if !services[svc] {
					continue
				}
				if host == "" {
					continue
				}
				scheme := "http://"
				if tls[host] {
					scheme = "https://"
				}
				add(scheme + host + plainPath(pm["path"]))
			}
		}
	}
	// Each site's own address first, then its paths: the box shows a few.
	sort.Slice(hosts, func(i, j int) bool {
		pi, pj := strings.Count(hosts[i], "/") > 2, strings.Count(hosts[j], "/") > 2
		if pi != pj {
			return !pi
		}
		return hosts[i] < hosts[j]
	})
	return hosts
}

// ownRouteHosts are the web addresses of the routes an app holds itself
// (an app of routes only, sending a site on to other apps).
func (m *Model) ownRouteHosts(a *view.App) []string {
	rtGVR, ok := m.client.Resources.Served(kube.HTTPRoutes)
	if !ok || a.Argo == nil {
		return nil
	}
	m.client.Cache.Watch(rtGVR)
	var hosts []string
	seen := map[string]bool{}
	for _, r := range a.Argo.Resources {
		if r.Kind != "HTTPRoute" {
			continue
		}
		rt, ok := m.client.Cache.Get(rtGVR, r.Namespace, r.Name)
		if !ok {
			continue
		}
		names, _, _ := unstructured.NestedStringSlice(rt.Object, "spec", "hostnames")
		for _, h := range names {
			if !seen[h] {
				seen[h] = true
				hosts = append(hosts, "https://"+h)
			}
		}
	}
	sort.Strings(hosts)
	return hosts
}

// routePaths are the paths a route rule matches: "" for the whole site
// ("/" or no match), "/api" for part of it.
func routePaths(rule map[string]any) []string {
	matches, _, _ := unstructured.NestedSlice(rule, "matches")
	var out []string
	seen := map[string]bool{}
	for _, mt := range matches {
		v, _, _ := unstructured.NestedString(mt.(map[string]any), "path", "value")
		p := ""
		if v != "" && v != "/" {
			p = plainPath(v)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		out = []string{""}
	}
	return out
}

// plainPath turns an ingress path into one you could type: "/api/.*$"
// becomes "/api/", "/" disappears.
func plainPath(v any) string {
	p, _ := v.(string)
	if i := strings.IndexAny(p, ".*$()[]^?+|\\"); i >= 0 {
		p = p[:i]
	}
	if p == "/" {
		return ""
	}
	return p
}

// podRestarted: one of the pod's containers has restarted, so it has logs
// from before.
func podRestarted(p *unstructured.Unstructured) bool {
	statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
	inits, _, _ := unstructured.NestedSlice(p.Object, "status", "initContainerStatuses")
	for _, s := range append(statuses, inits...) {
		if n, _, _ := unstructured.NestedInt64(s.(map[string]any), "restartCount"); n > 0 {
			return true
		}
	}
	return false
}
