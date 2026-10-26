package ui

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// action is one key that does something to the selected row(s). The same
// table drives key handling, the header hints and help, so they always
// agree: one key, one meaning.
type action struct {
	key   string
	label string
	about string // one plain line on what it does, shown in the menu
	write bool   // needs write access; hidden and blocked with --readonly
	// sensitive: reads what Kubernetes' own read-only role ("view") leaves
	// out, secret values and port-forwards. Blocked with --readonly too.
	sensitive bool
	bulk      bool // acts on every marked row when rows are marked
	when      func(m *Model) bool
	run       func(m *Model, rows []view.Row) tea.Cmd
	// item, when set, words the menu item for the selected row.
	item func(m *Model, row view.Row, it *menuItem)
}

// usesRow: the action is about the row it is pressed on, so picks make it
// unclear. w on a list that isn't an app's or a workload's shows the whole
// namespace, whichever row: picks don't change it.
func (m *Model) usesRow(a action) bool {
	if a.key != "w" || m.loc.kind == locHome || m.loc.app != "" {
		return true
	}
	if row, ok := m.tbl.selected(); ok && row.Obj != nil {
		switch row.Obj.GetKind() {
		case "Deployment", "StatefulSet", "DaemonSet", "CronJob", "Job":
			return true
		}
	}
	return false
}

// Conditions: which view an action belongs to.
func onResource(m *Model) bool { return m.loc.kind == locResource }

func has(c view.Cap) func(*Model) bool {
	return func(m *Model) bool { return onResource(m) && view.For(m.loc.res).Has(c) }
}

// podAction: works on a pod row and on a row of a pod's containers.
func podAction(c view.Cap) func(*Model) bool {
	return func(m *Model) bool { return m.loc.kind == locContainers || has(c)(m) }
}

func onKind(k locKind) func(*Model) bool { return func(m *Model) bool { return m.loc.kind == k } }

func isNamespaces(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Resource == kube.Namespaces.Resource && m.loc.res.GVR.Group == ""
}

// forwardable: a Service, or a workload whose pods can be reached.
func forwardable(m *Model) bool {
	if !onResource(m) {
		return false
	}
	if m.loc.res.GVR.Group == "" && m.loc.res.GVR.Resource == kube.Services.Resource {
		return true
	}
	switch m.loc.res.GVR.Resource {
	case kube.Jobs.Resource, kube.CronJobs.Resource: // runs that finish: nothing to connect to
		return false
	}
	return view.For(m.loc.res).Has(view.CapPodLogs)
}

func isArgoApps(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Resource == kube.Applications.Resource && m.loc.res.GVR.Group == kube.Applications.Group
}

func isExternalSecrets(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Resource == kube.ExternalSecrets.Resource
}

// isWebEntry: a list of ingresses or routes (where web addresses lead in).
func isWebEntry(m *Model) bool {
	if !onResource(m) {
		return false
	}
	r := m.loc.res.GVR
	return (r.Resource == kube.Ingresses.Resource && r.Group == kube.Ingresses.Group) ||
		(r.Resource == kube.HTTPRoutes.Resource && r.Group == kube.HTTPRoutes.Group)
}

// backendServices are the services an ingress or route sends traffic to.
func backendServices(o *unstructured.Unstructured) []backend {
	seen := map[string]bool{}
	var out []backend
	add := func(b backend) {
		if b.name != "" && !seen[b.kind+"/"+b.ns+"/"+b.name] {
			seen[b.kind+"/"+b.ns+"/"+b.name] = true
			out = append(out, b)
		}
	}
	rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "rules")
	for _, r := range rules {
		rm, _ := r.(map[string]any)
		refs, _, _ := unstructured.NestedSlice(rm, "backendRefs") // a route
		for _, ref := range refs {
			if b, ok := ref.(map[string]any); ok {
				kind, _ := b["kind"].(string)
				if kind == "" {
					kind = "Service"
				}
				ns, _ := b["namespace"].(string) // a route may send to another namespace
				if ns == "" {
					ns = o.GetNamespace()
				}
				name, _ := b["name"].(string)
				add(backend{kind: kind, ns: ns, name: name})
			}
		}
		paths, _, _ := unstructured.NestedSlice(rm, "http", "paths") // an ingress
		for _, p := range paths {
			if pm, ok := p.(map[string]any); ok {
				name, _, _ := unstructured.NestedString(pm, "backend", "service", "name")
				add(backend{kind: "Service", ns: o.GetNamespace(), name: name})
			}
		}
	}
	if name, _, _ := unstructured.NestedString(o.Object, "spec", "defaultBackend", "service", "name"); name != "" {
		add(backend{kind: "Service", ns: o.GetNamespace(), name: name})
	}
	return out
}

// backend is where a route or ingress sends traffic: a Service, or another
// kind (an Envoy Backend, outside the cluster).
type backend struct{ kind, ns, name string }

func isConfigMaps(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Resource == kube.ConfigMaps.Resource && m.loc.res.GVR.Group == ""
}

func isSecrets(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Resource == kube.Secrets.Resource && m.loc.res.GVR.Group == ""
}

func one(f func(m *Model, r view.Row) tea.Cmd) func(*Model, []view.Row) tea.Cmd {
	return func(m *Model, rows []view.Row) tea.Cmd { return f(m, rows[0]) }
}

// actions, most used first (the order of the Enter menu).
var actions = []action{
	{key: "w", label: "What changed", about: "The last 24 hours: new versions, ArgoCD syncs, crashes, scaling, nodes, secrets", when: func(m *Model) bool { return onResource(m) || m.loc.kind == locHome || m.loc.kind == locContainers },
		run: one((*Model).whatChanged)},
	// home: one row per app
	{key: "l", label: "Logs (all pods)", about: "Logs of every pod in this app, merged; e keeps only errors", when: onKind(locHome), run: one((*Model).appLogs),
		item: func(m *Model, row view.Row, it *menuItem) {
			a, ok := m.appFor(row)
			switch {
			case !ok:
			case m.appLogsNeverRan(a, a.Pods == 0 && a.Leftovers == 0): // as appLogs refuses first
				it.disabled, it.off, it.about = true, "never ran", "Can't: "+neverRanErr(a.Name).Error()
			case a.Pods == 0 && a.Leftovers == 0: // finished pods keep their logs
				it.disabled, it.off, it.about = true, "no pods", "Can't: no pod of this app is running"
			}
		}},
	{key: "E", label: "Events", about: "What Kubernetes itself said about this app: placing pods, health checks, image downloads", when: onKind(locHome), run: one((*Model).appEvents)},
	{key: "r", label: "Restart", about: "Rollout restart of every deployment, statefulset and daemonset of the app: new pods replace old ones one pod at a time, so it stays up", write: true, when: onKind(locHome), run: one((*Model).appRestart), item: appRestartItem},
	{key: "p", label: "Logs before the last restart", about: "The logs of its most recently restarted pod, from before that restart (a crash, running out of memory)", when: onKind(locHome), run: one((*Model).appPreviousLogs),
		item: func(m *Model, row view.Row, it *menuItem) {
			a, ok := m.appFor(row)
			switch {
			case !ok:
			case neverStarted(a): // as appPreviousLogs: only another pod's restart has earlier logs
				if _, ran := ranBeforeRestart(m.appPodRows(a)); !ran {
					it.disabled, it.off, it.about = true, "never ran", "Can't: "+neverRanErr(a.Name).Error()
				}
			case a.Restarts == 0 && a.CrashPod == "": // a failed run's pod has its logs too, as p on the list
				it.disabled, it.off, it.about = true, "no restarts", "Can't: no pod of this app has restarted, so there are no earlier logs"
			}
		}},
	{key: "+", label: "Scale", about: "Change how many copies of its deployment run", write: true, when: onKind(locHome), run: one(appDo("+")), item: appWorkloadItem("+")},
	{key: "u", label: "Go back to the previous version", about: "Put its deployment back on the version before", write: true, when: onKind(locHome), run: one(appDo("u")), item: appWorkloadItem("u")},
	{key: "o", label: "Watch the rollout", about: "Its deployment's old vs new version side by side, live", when: onKind(locHome), run: one(appDo("o")), item: appWorkloadItem("o")},
	{key: "v", label: "Versions", about: "The versions its deployment keeps, with their images; pick one to go back to it", when: onKind(locHome), run: one(appDo("v")), item: appWorkloadItem("v")},
	{key: "X", label: "What it uses", about: "Everything its deployment uses: web address, services, config, secrets, AWS role, volumes", when: onKind(locHome), run: one(appDo("X")), item: appWorkloadItem("X")},
	{sensitive: true, key: "F", label: "Reach on a local port (port-forward)", about: "Reach its deployment on a local port (localhost), through one of its running pods", when: onKind(locHome), run: one(appDo("F")), item: appWorkloadItem("F")},
	{key: "i", label: "Change image", about: "Run a different image or tag in its deployment", write: true, when: onKind(locHome), run: one(appDo("i")), item: appWorkloadItem("i")},
	{key: "y", label: "Show YAML", about: "Its deployment's full definition", when: onKind(locHome), run: one(appDo("y")), item: appWorkloadItem("y")},
	{key: "e", label: "Edit", about: "Change its deployment's YAML in your editor; checked by the API before it applies", write: true, when: onKind(locHome), run: one(appDo("e")), item: appWorkloadItem("e")},
	{key: "d", label: "Details", about: "ArgoCD's record of the app (where it comes from in Git, sync, health), or its namespace", when: onKind(locHome), run: one((*Model).appDetails)},
	{label: "Its workloads", about: "The deployments (or statefulsets, daemonsets) that run this app: scale, go back, restart one", when: onKind(locHome), run: one(func(m *Model, r view.Row) tea.Cmd {
		if a, ok := m.appFor(r); ok && !a.Runs("Deployment") && !a.Runs("StatefulSet") && !a.Runs("DaemonSet") && a.Runs("CronJob") {
			return m.appOpenKind(r, "cronjobs") // it only runs on a schedule: that's its workloads
		}
		return m.appOpenKind(r, "deployments")
	})},
	{label: "Its config", about: "The ConfigMaps this app's pods read", when: onKind(locHome), run: one(func(m *Model, r view.Row) tea.Cmd { return m.appOpenKind(r, "configmaps") })},
	{label: "Its cron jobs", about: "The jobs this app runs on a schedule: when they run next, and whether the last run worked", when: onKind(locHome),
		run: one(func(m *Model, r view.Row) tea.Cmd { return m.appOpenKind(r, "cronjobs") }),
		item: func(m *Model, row view.Row, it *menuItem) {
			if a, ok := m.appFor(row); !ok || !a.Runs("CronJob") {
				it.hidden = true
			}
		}},
	{label: "Its secrets", about: "The Secrets this app's pods read (values hidden until you show them)", when: onKind(locHome), run: one(func(m *Model, r view.Row) tea.Cmd { return m.appOpenKind(r, "secrets") })},

	{key: "l", label: "Logs", about: "This pod's logs, live; e keeps only errors, / searches", when: podAction(view.CapLogs), run: one(func(m *Model, r view.Row) tea.Cmd { return m.logs(r, false) })},
	{key: "l", label: "Logs (all pods)", about: "Logs of all its pods, merged; e keeps only errors", when: has(view.CapPodLogs), run: one((*Model).workloadLogs),
		item: func(m *Model, row view.Row, it *menuItem) { // as workloadLogs refuses
			if err := m.noLogPods(row.Obj); err != nil {
				it.disabled, it.off, it.about = true, "no pods", "Can't: "+err.Error()
				if m.jobNeverRan(row.Obj) { // pods or not, there is nothing to read
					it.off = "never ran"
				}
			}
		}},
	{key: "p", label: "Logs before the last restart", about: "The logs of its most restarted pod, from before that restart (a crash, running out of memory)", when: has(view.CapPodLogs), run: one((*Model).workloadPreviousLogs),
		item: func(m *Model, row view.Row, it *menuItem) { // as previousLogsOf refuses
			pods := m.enr.PodsOf(row.Obj)
			switch {
			case !slices.ContainsFunc(pods, restartedIn) && m.jobNeverRan(row.Obj): // as workloadPreviousLogs refuses first
				it.disabled, it.off, it.about = true, "never ran", "Can't: "+neverRanErr(row.Obj.GetName()).Error()
			case len(pods) == 0:
				it.disabled, it.off, it.about = true, "no pods", "Can't: no pod of it exists right now, so there are no logs to show"
			case !slices.ContainsFunc(pods, restartedIn):
				it.disabled, it.off, it.about = true, "no restarts", "Can't: no pod of it has restarted, so there are no earlier logs"
			}
		}},
	{key: "s", label: "Shell", about: "A shell inside the container, in this panel (ctrl-] closes it)", write: true, when: podAction(view.CapShell),
		run: one(func(m *Model, r view.Row) tea.Cmd {
			if err := m.noShell(r); err != nil {
				m.setFlash("", err)
				return nil
			}
			return m.askOnProd("Open a shell in "+r.Obj.GetName()+"? Commands you run there can change it.", func() tea.Cmd { return m.shell(r) })
		}), item: shellItem},
	{key: "d", label: "Details", about: "Everything Kubernetes knows about it, as kubectl describe shows it", when: func(m *Model) bool { return onResource(m) || m.loc.kind == locContainers },
		run: one(func(m *Model, r view.Row) tea.Cmd { return m.describe(m.podRowIfContainer(r)) })},
	{key: "E", label: "Events", about: "What Kubernetes itself said about it: placing it, health checks, image downloads", when: func(m *Model) bool { return onResource(m) || m.loc.kind == locContainers },
		run: one(func(m *Model, r view.Row) tea.Cmd { return m.objectEvents(m.podRowIfContainer(r)) })},
	{key: "o", label: "Watch the rollout", about: "Old vs new version side by side, pods ready, live", when: isDeployments, run: one((*Model).rolloutView)},
	{key: "+", label: "Scale", about: "Change how many copies (replicas) run", write: true, when: has(view.CapScale), run: one((*Model).scale)},
	{key: "u", label: "Go back to the previous version", about: "Go back to the previous version", write: true, when: isVersioned, run: one((*Model).undoRollout),
		item: func(m *Model, row view.Row, it *menuItem) {
			var managed *enrich.ArgoApp
			if argo, ok := m.enr.ManagedBy(row.Obj); ok {
				managed = &argo
			}
			it.about = argoUndo(it.about, managed)
			// As undoRolloutNow refuses, once its versions have loaded
			// (before, u waits for them).
			if gvr, ok := m.versionsList(m.ref(row)); !ok || !m.client.Cache.Synced(gvr) {
				return
			}
			if off, about, _ := m.undoOff(row.Obj); off != "" {
				it.disabled, it.off, it.about = true, off, about
			} else if _, fails := m.undoTargetFails(row.Obj); fails {
				it.about += undoFailsAbout
			} else {
				it.about += m.undoFailedAbout(row.Obj)
			}
		}},
	{key: "v", label: "Versions", about: "The versions it keeps, newest first, with their images; pick one to go back to it", when: isVersioned, run: one((*Model).versions),
		item: func(m *Model, row view.Row, it *menuItem) { // as versionsNow refuses, once they have loaded
			if gvr, ok := m.versionsList(m.ref(row)); ok && m.client.Cache.Synced(gvr) && len(m.enr.RolloutOf(row.Obj).All) == 0 {
				it.disabled, it.off, it.about = true, "none listed", "Can't: "+m.noVersionsWhy(m.ref(row)).Error()
			}
		}},
	{key: "r", label: "Restart", about: "Rollout restart: new pods replace the old ones one pod at a time, so it stays up (like kubectl rollout restart)", write: true, bulk: true, when: has(view.CapRestart), run: (*Model).restartRows, item: restartItem},
	{key: "r", label: "Restart", about: "Rollout restart of what runs this pod: new pods replace the old ones one pod at a time", write: true, when: podAction(view.CapLogs), run: one((*Model).restartOwner), item: ownerRestartItem},
	{key: "c", label: "Cordon: stop or allow new pods", about: "Stop new pods landing on this node (cordon), or allow them again (uncordon)", write: true, bulk: true, when: has(view.CapCordon), run: (*Model).cordonRows,
		item: func(m *Model, row view.Row, it *menuItem) {
			if rows := m.actedOn(row); len(rows) > 1 || rowKey(rows[0]) != rowKey(row) {
				it.about = "Stop new pods landing on " + pickedNames(rows, "node") + " (cordon), or allow them again (uncordon)"
			}
		}},
	{key: "D", label: "Drain", about: "Move every pod off this node safely, e.g. before removing it", write: true, when: has(view.CapCordon), run: one((*Model).drain)},
	{key: "l", label: "Logs of the last run", about: "The logs of the newest job it started", when: has(view.CapTrigger), run: one((*Model).lastRunLogs),
		item: func(m *Model, r view.Row, it *menuItem) {
			if gvr, ok := m.client.Resources.Served(kube.Jobs); ok {
				for _, j := range m.client.Cache.List(gvr, r.Obj.GetNamespace()) {
					for _, ref := range j.GetOwnerReferences() {
						if ref.Kind == "CronJob" && ref.Name == r.Obj.GetName() {
							return
						}
					}
				}
			}
			it.disabled, it.off, it.about = true, "no runs kept", "Can't: no run of it is kept (Kubernetes cleans up finished runs); the box says when it last worked"
		}},
	{key: "t", label: "Run now", about: "Start a job from this CronJob right away", write: true, when: has(view.CapTrigger), run: one((*Model).triggerCronJob)},
	{key: "z", label: "Pause", about: "Stop new runs from starting on its schedule (suspend), until you resume it", write: true, when: has(view.CapTrigger), run: one((*Model).suspendCronJob),
		item: func(_ *Model, row view.Row, it *menuItem) {
			if paused, _, _ := unstructured.NestedBool(row.Obj.Object, "spec", "suspend"); paused {
				it.label, it.about = "Resume", "Let runs start on its schedule again"
			}
		}},
	{key: "X", label: "What it uses", about: "Everything it uses: node, service account, AWS role, config, secrets, volumes", when: podAction(view.CapXray), run: one((*Model).xray)},
	{label: "Open its node", about: "The machine this pod runs on: how busy it is, its cost, any pressure", when: podAction(view.CapLogs),
		run: one(func(m *Model, r view.Row) tea.Cmd {
			pod := m.podRowIfContainer(r).Obj
			node, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName")
			if node == "" {
				m.setFlash("", fmt.Errorf("%s isn't on a node yet", pod.GetName()))
				return nil
			}
			return m.openNamed(kube.Nodes, "", node)
		})},
	{key: "x", label: "Show contents", about: "Its keys and values", when: isConfigMaps, run: one((*Model).showConfig)},
	{sensitive: true, key: "x", label: "Show values", about: "Show the secret's values in plain text", when: isSecrets,
		run: one(func(m *Model, r view.Row) tea.Cmd {
			// Always asked: values show in plain text, on any cluster.
			q := "Show the values of secret " + r.Obj.GetName() + " in plain text on screen?"
			typed := ""
			if m.typedConfirm() { // production: type its name, as for a change
				typed = r.Obj.GetName()
				q += "\n⚠ This is a production cluster."
			}
			m.prompt = confirmPrompt(q, nil, typed, func() tea.Cmd { return m.decodeSecret(r) })
			m.mode = modePrompt
			return nil
		})},
	{key: "o", label: "What it manages", about: "Everything this ArgoCD app manages, the parts that differ from Git first, with their health", when: isArgoApps, run: one((*Model).argoDiff),
		item: func(_ *Model, row view.Row, it *menuItem) { // as argoDiff refuses
			if res, _, _ := unstructured.NestedSlice(row.Obj.Object, "status", "resources"); len(res) == 0 {
				it.disabled, it.off, it.about = true, "none listed", "Can't: ArgoCD hasn't listed what it manages yet"
			}
		}},
	{label: "Open its app", about: "Its pods on the apps screen, and why each is or isn't healthy", when: isArgoApps,
		run: one(func(m *Model, r view.Row) tea.Cmd { return m.openAppNamed(r.Obj.GetName()) })},
	{label: "Open the service it sends to", about: "The service behind this web address; enter on it shows the pods that answer", when: isWebEntry,
		item: func(_ *Model, r view.Row, it *menuItem) {
			if len(backendServices(r.Obj)) == 0 {
				it.disabled, it.off, it.about = true, "none", "Can't: it names no service to send traffic to"
			}
		},
		run: one(func(m *Model, r view.Row) tea.Cmd {
			all := backendServices(r.Obj)
			if len(all) == 1 && all[0].kind == "Service" {
				return m.openNamed(kube.Services, all[0].ns, all[0].name)
			}
			items := make([]menuItem, 0, len(all))
			for _, b := range all {
				label := strings.ToLower(b.kind) + " " + b.name
				if b.ns != r.Obj.GetNamespace() {
					label += " (in " + b.ns + ")"
				}
				if b.kind != "Service" { // listed so the count adds up; kubed opens services only
					items = append(items, menuItem{label: label, about: "Not a service: " + b.kind + " " + b.name + " sends this traffic on (often outside the cluster); d on the route shows it"})
					continue
				}
				items = append(items, menuItem{label: label, about: "Its service in " + b.ns, run: func() tea.Cmd { return m.openNamed(kube.Services, b.ns, b.name) }})
			}
			m.prompt = menuPrompt("It sends traffic to "+view.Plural(len(all), "place")+". Which one?", items)
			m.mode = modePrompt
			return nil
		})},
	{label: "Open the Secret it fills", about: "The Secret this ExternalSecret writes, with its keys", when: isExternalSecrets,
		run: one(func(m *Model, r view.Row) tea.Cmd {
			return m.openNamed(kube.Secrets, r.Obj.GetNamespace(), enrich.ExternalSecret(r.Obj).Target)
		})},
	{label: "Open the ExternalSecret that fills it", about: "Where this Secret's values come from, and whether it syncs", when: isSecrets,
		item: func(m *Model, r view.Row, it *menuItem) {
			if _, ok := m.enr.ExternalSecretOwner(r.Obj); !ok {
				it.disabled, it.off, it.about = true, "none", "Can't: no ExternalSecret fills this Secret; it was made some other way"
			}
		},
		run: one(func(m *Model, r view.Row) tea.Cmd {
			es, ok := m.enr.ExternalSecretOwner(r.Obj)
			if !ok {
				m.setFlash("", fmt.Errorf("no ExternalSecret fills %s; it was made some other way", r.Obj.GetName()))
				return nil
			}
			return m.openNamed(kube.ExternalSecrets, r.Obj.GetNamespace(), es.Name)
		})},
	{key: "y", label: "Show YAML", about: "The full definition as Kubernetes stores it", when: onResource, run: one((*Model).showYAML)},
	{key: "y", label: "What it installed", about: "What this Helm release installed", when: onKind(locHelm), run: one(func(m *Model, r view.Row) tea.Cmd { return m.helmShow(r, "manifest") })},
	{key: "v", label: "Versions", about: "This Helm release's past versions: chart, app version, what happened", when: onKind(locHelm), run: one((*Model).helmHistory)},
	{key: "p", label: "Logs before the last restart", about: "The logs from before its last restart (a crash, running out of memory)", when: podAction(view.CapLogs),
		run: one(func(m *Model, r view.Row) tea.Cmd {
			pod := m.podRowIfContainer(r).Obj
			var fresh *unstructured.Unstructured // asked now, as endedRun would: a container may have died a second ago
			if gvr, ok := m.client.Resources.Served(kube.Pods); ok && !m.hasEarlierRun(r, pod) {
				if f, found := m.freshPod(gvr, pod.GetNamespace(), pod.GetName()); found {
					pod, fresh = f, f
				}
			}
			if !m.hasEarlierRun(r, pod) { // as the menu greys it
				why := "nothing in " + pod.GetName() + " has restarted or stopped"
				if m.loc.kind == locContainers {
					why = r.Obj.GetName() + " hasn't restarted or stopped"
				}
				m.setFlash("", fmt.Errorf("%s, so there are no earlier logs. l shows the live ones", why))
				return nil
			}
			if fresh != nil && m.loc.kind != locContainers {
				r.Obj = fresh // its containers, as just read
			}
			return m.logsWith(r, true, fresh)
		}),
		item: func(m *Model, r view.Row, it *menuItem) {
			// A container stopped right now (crashed, waiting to restart;
			// a finished run) has its run that ended to show, as p shows it.
			if pod := m.podRowIfContainer(r).Obj; !m.hasEarlierRun(r, pod) {
				about := "Can't: nothing in this pod has restarted, so there are no earlier logs"
				if m.loc.kind == locContainers {
					about = "Can't: this container hasn't restarted, so there are no earlier logs"
				}
				it.disabled, it.off, it.about = true, "no restarts", about
			}
		}},
	{key: "S", label: "Full-screen shell", about: "A shell that takes the whole terminal", write: true, when: podAction(view.CapShell),
		run: one(func(m *Model, r view.Row) tea.Cmd {
			if err := m.noShell(r); err != nil {
				m.setFlash("", err)
				return nil
			}
			return m.askOnProd("Open a shell in "+r.Obj.GetName()+"? Commands you run there can change it.", func() tea.Cmd { return m.fullScreenShell(r) })
		}), item: shellItem},
	{sensitive: true, key: "F", label: "Reach on a local port (port-forward)", about: "Reach one of its ports on a local port (localhost)", when: podAction(view.CapPortForward), run: one((*Model).portForward),
		item: func(m *Model, row view.Row, it *menuItem) { // as portForward refuses
			pod := m.podRowIfContainer(row).Obj
			if phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase"); phase != "Running" || pod.GetDeletionTimestamp() != nil {
				it.disabled, it.off, it.about = true, "not running", "Can't: the pod isn't running, so there is nothing to reach"
			}
		}},
	{sensitive: true, key: "F", label: "Reach on a local port (port-forward)", about: "Reach it on a local port (localhost), through one of its running pods", when: forwardable, run: one((*Model).portForwardTo),
		item: func(m *Model, row view.Row, it *menuItem) { // as portForwardTo refuses
			pods, ok := m.podsBehind(row.Obj)
			if !ok {
				return // still loading: F says so
			}
			switch pod := pickRunningPod(pods); {
			case pod == nil:
				it.disabled, it.off, it.about = true, "no pod running", "Can't: no pod of it is running, so there is nothing to connect to"
			case !containersRunning(pod):
				it.disabled, it.off, it.about = true, "not running", "Can't: no pod of it has its containers running (pod "+pod.GetName()+" keeps crashing or is starting), so there is nothing to connect to"
			}
		}},
	{key: "i", label: "Change image", about: "Run a different image or tag", write: true, when: has(view.CapImage), run: one((*Model).setImage)},
	{key: "e", label: "Edit", about: "Change its YAML in your editor; checked by the API before it applies", write: true, when: onResource, run: one((*Model).edit)},
	{key: "ctrl+k", label: "Force stop", about: "Stop the pod at once, without time to finish its work (Delete gives it time); whatever runs it starts a new one", write: true, bulk: true, when: has(view.CapKill), run: (*Model).killRows,
		item: func(m *Model, row view.Row, it *menuItem) {
			rows := m.actedOn(row)
			if len(rows) > 1 || rowKey(rows[0]) != rowKey(row) {
				it.about = "Stop " + pickedNames(rows, "pod") + " at once, without time to finish their work (Delete gives it); a pod something runs gets a new one (a job's only while its job has tries left), one nothing runs is gone for good"
				return
			}
			switch {
			case podEnded(row.Obj) && goneForGood(row.Obj): // as the confirm says: no work left to cut short
				it.about = "Remove the pod at once (it has already ended); nothing starts another, so it is gone for good"
			case podEnded(row.Obj): // whether its owner starts another depends on the owner: no claim
				it.about = "Remove the pod at once (it has already ended)"
			case goneForGood(row.Obj): // as the confirm says
				it.about = "Stop the pod at once, without time to finish its work (Delete gives it time); nothing starts another, so it is gone for good"
			case m.enr.Controller(row.Obj) != nil && m.enr.Controller(row.Obj).GetKind() == "Job": // as the confirm says
				it.about = "Stop the pod at once, without time to finish its work (Delete gives it time); " + jobNext(m.enr.Controller(row.Obj))
			}
		}},
	{key: "ctrl+d", label: "Delete", about: "Delete it from the cluster (a pod gets time to finish its work first)", write: true, bulk: true, when: onResource, run: (*Model).deleteRows,
		item: func(m *Model, row view.Row, it *menuItem) {
			if rows := m.actedOn(row); len(rows) > 1 || rowKey(rows[0]) != rowKey(row) {
				it.about = "Delete " + pickedNames(rows, "row") + " from the cluster (a pod gets time to finish its work first)"
			}
		}},
	{key: "ctrl+d", label: "Stop this port", about: "Stop reaching this port: the local port closes (with rows picked: all of them)", bulk: true, when: onKind(locForwards), run: (*Model).stopForwards,
		item: func(m *Model, row view.Row, it *menuItem) {
			if n := len(m.tbl.marked()); n > 0 {
				it.label = fmt.Sprintf("Stop %d picked", n) // the picks, not the row under the cursor
				return
			}
			if m.stoppedPort(row) { // already stopped: its line goes
				it.label, it.about = "Clear this line", "This port already stopped: remove its line from the list"
			}
		}},
	{label: "Pods in this namespace", about: "Every pod in this namespace", when: isNamespaces,
		run: one(func(m *Model, r view.Row) tea.Cmd {
			m.enterNamespace(r.Obj.GetName())
			m.openResource("pods", true)
			return nil
		})},
	// menu only (no key)
	{key: "Y", label: "Copy name", about: "Copy the name to your clipboard (with rows picked: all their names, one per line)", bulk: true, when: func(m *Model) bool { return m.loc.kind != locContexts && m.loc.kind != locFind }, run: func(m *Model, rows []view.Row) tea.Cmd {
		if len(rows) == 1 {
			return m.copyName(rows[0])
		}
		names := make([]string, len(rows))
		for i, r := range rows {
			names[i] = strings.TrimPrefix(r.Obj.GetName(), markPrefix)
		}
		return m.copy(strings.Join(names, "\n"), fmt.Sprintf("%d names", len(rows)))
	}, item: func(m *Model, _ view.Row, it *menuItem) {
		if m.loc.kind == locForwards {
			it.label, it.about = "Copy address", "Copy its local address, like localhost:8080, to your clipboard"
		}
	}},
	{label: "Copy YAML", about: "Copy the full YAML to your clipboard", when: onResource, run: one((*Model).copyYAML)},
}

// listLoading: the current list hasn't arrived yet, so there is no row to
// act on.
func (m *Model) listLoading() bool {
	switch {
	case m.client == nil:
		return m.connecting
	case m.loc.kind == locHome:
		return m.homeLoading() != ""
	case m.loc.kind == locResource:
		return !m.client.Cache.Synced(m.loc.res.GVR)
	}
	return false
}

// actionFor finds the action a key triggers on the current view.
func (m *Model) actionFor(key string) (action, bool) {
	for _, a := range actions {
		if a.key == key && a.when(m) {
			return a, true
		}
	}
	return action{}, false
}

// runAction runs an action from the table: the one picked in the menu, or
// the one a key triggers here.
func (m *Model) runAction(key string, picked ...action) tea.Cmd {
	a, ok := action{}, false
	if len(picked) > 0 {
		a, ok = picked[0], true
	} else if key != "" {
		a, ok = m.actionFor(key)
	}
	if !ok {
		return nil
	}
	if a.write && m.writesLocked() && a.key == "D" {
		if row, ok := m.tbl.selected(); ok && row.Obj.GetKind() == "Node" {
			return m.drainPreview(row) // what it would do is worth knowing read-only too
		}
	}
	if a.write && m.writesLocked() {
		err := m.lockedReason()
		if a.key == "s" || a.key == "S" { // not a change itself, but what you type in it can be
			err = fmt.Errorf("can't: kubed is read-only (started with --readonly), which, like Kubernetes' own view role, leaves out shells: commands typed in one can change things")
		}
		if row, ok := m.tbl.selected(); ok && a.key == "u" && m.selfHeals(row) {
			// The advice that matters first: kubed's going back wouldn't last.
			err = fmt.Errorf("ArgoCD heals this app, so going back here wouldn't last: change the version in Git (kubed is read-only anyway)")
		}
		m.setFlash("", err)
		return nil
	}
	if a.sensitive && m.writesLocked() {
		m.setFlash("", errSensitiveLocked)
		return nil
	}
	rows := m.tbl.marked()
	if !a.bulk && len(rows) > 0 && m.usesRow(a) {
		// One row's key while others are picked (a change, or a view, a
		// port, logs): which one you mean isn't clear, so don't guess.
		if cur, ok := m.tbl.selected(); !ok || len(rows) > 1 || rowKey(rows[0]) != rowKey(cur) { // by key: app and port rows have no UID
			picked := fmt.Sprintf("%d rows are", len(rows))
			if len(rows) == 1 {
				picked = "another row is"
			}
			m.setFlash("", fmt.Errorf("%s works on one row at a time, and %s picked. esc clears the picks",
				strings.ToLower(shortLabel(a.label)), picked))
			return nil
		}
	}
	if !a.bulk || len(rows) == 0 {
		row, ok := m.tbl.selected()
		if !ok && !m.listLoading() { // loaded since the last refresh (once a second): look again
			m.refreshRows()
			row, ok = m.tbl.selected()
		}
		if !ok {
			if m.listLoading() {
				m.setFlash("", fmt.Errorf("still loading this list; try again in a moment"))
			} else {
				m.setFlash("", fmt.Errorf("this list is empty, so there is nothing for %s to act on", displayKey(key)))
			}
			return nil
		}
		rows = []view.Row{row}
	}
	m.tbl.touched = true // you acted on this row: it stays selected
	return a.run(m, rows)
}

func displayKey(k string) string { return strings.ReplaceAll(k, "ctrl+", "ctrl-") }

// podRowIfContainer: in the containers view, describe the pod itself.
func (m *Model) podRowIfContainer(r view.Row) view.Row {
	if m.loc.kind != locContainers {
		return r
	}
	if pod, ok := m.containersPod(); ok {
		return view.Row{Obj: pod}
	}
	return r
}

// askOnProd asks y/n first on production (and clusters kubed can't place),
// for things that aren't changes but deserve a pause: a shell, secret
// values.
func (m *Model) askOnProd(question string, then func() tea.Cmd) tea.Cmd {
	if !m.typedConfirm() {
		return then()
	}
	m.prompt = confirmPrompt(question, []string{"This is a production cluster."}, "", then)
	m.mode = modePrompt
	return nil
}

// writesLocked: kubed was started with --readonly. (Production is not
// locked: every write there asks you to type the name instead.)
func (m *Model) writesLocked() bool { return m.opts.ReadOnly }

// sensitiveWhy: read-only follows Kubernetes' built-in "view" role, which
// can't read secret values or reach pods through port-forwards.
const sensitiveWhy = "kubed is read-only (started with --readonly), which, like Kubernetes' own view role, leaves out secret values and port-forwards"

var errSensitiveLocked = fmt.Errorf("can't: %s", sensitiveWhy)

func (m *Model) lockedReason() error {
	return fmt.Errorf("can't: kubed is read-only (started with --readonly). Start it without --readonly to change things")
}

// menu is the Enter menu for the selected row: the default "open" first,
// then everything that works here, each with its shortcut key.
func (m *Model) menu() tea.Cmd {
	row, ok := m.tbl.selected()
	if !ok {
		return nil
	}
	m.tbl.touched = true // the row you opened the menu on stays selected
	var items []menuItem
	// Rows picked other than (or as well as) this one: one-row items show
	// greyed, as runAction refuses them.
	picks := m.tbl.marked()
	othersPicked := len(picks) > 1 || len(picks) == 1 && rowKey(picks[0]) != rowKey(row)
	pickedWords := fmt.Sprintf("%d rows are", len(picks))
	if len(picks) == 1 {
		pickedWords = "another row is"
	}
	if label, run, ok := m.defaultOpen(row); ok {
		it := menuItem{label: label, key: "", about: openAbout(m.loc.kind, label), run: run}
		if othersPicked { // it opens the cursor's row, which isn't what's picked (and leaving drops the picks)
			it.disabled, it.off = true, "one row"
			it.about = "Can't: it opens one row, and " + pickedWords + " picked. esc clears the picks"
		}
		items = append(items, it)
	}
	for _, a := range actions {
		if !a.when(m) {
			continue
		}
		it := menuItem{label: a.label, key: a.key, about: a.about, run: func() tea.Cmd { return m.runAction(a.key, a) }}
		if a.item != nil {
			a.item(m, row, &it)
		}
		if it.hidden {
			continue
		}
		// The item's own reason comes last, as runAction checks read-only
		// and picks before the action itself looks at the row: the reason
		// shown is the one pressing the key gives.
		ownOff, ownWhy, ownAbout := it.disabled, it.off, it.about
		if ownOff {
			it.disabled, it.off, it.about = false, "", a.about
		}
		if row.Obj.GetKind() == "Event" {
			// An event's menu is about what it describes: changing the
			// event record itself is never what you mean.
			switch a.key {
			case "e", "ctrl+d", "E", "w", "Y":
				continue
			}
		}
		preview := a.write && m.writesLocked() && a.key == "D" && row.Obj.GetKind() == "Node"
		if preview {
			// As runAction: read-only, D shows what draining would move
			// (before it looks at picks or the row).
			it.label, it.about = "Drain: what it would move", "Read-only, so nothing moves: the pods draining this node would move, and where"
			ownOff = false
		} else if a.write && m.writesLocked() && !it.disabled {
			// Shown, so you know it exists, but off.
			it.disabled, it.off = true, "read-only"
			if a.key == "u" && m.selfHeals(row) { // as runAction words it
				// The advice that matters fits first: kubed's going back won't last anyway.
				it.about = "ArgoCD heals this app, so going back here wouldn't last: change the version in Git (kubed is read-only anyway)"
			} else {
				it.about = "Can't: kubed is read-only (started with --readonly). Otherwise: " + lowerFirstWord(it.about)
			}
		}
		if a.sensitive && m.writesLocked() && !it.disabled {
			it.disabled, it.off = true, "read-only"
			it.about = "Can't: " + sensitiveWhy + ". Otherwise: " + lowerFirstWord(it.about)
		}
		// After read-only, as runAction checks them: the first reason it
		// would give is the one shown.
		if !a.bulk && othersPicked && !it.disabled && !preview && m.usesRow(a) {
			// A one-row item while other rows are picked: which one is
			// meant isn't clear, so runAction would refuse it.
			it.disabled, it.off = true, "one row"
			it.about = "Can't: it works on one row at a time, and " + pickedWords + " picked. esc clears the picks"
		}
		if ownOff && !it.disabled {
			it.disabled, it.off, it.about = true, ownWhy, ownAbout
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		return nil
	}
	// What you can do first; what's off (read-only, nothing to act on) last.
	sort.SliceStable(items, func(i, j int) bool { return !items[i].disabled && items[j].disabled })
	title := strings.TrimPrefix(row.Cells[0], markPrefix)
	if row.Obj.GetKind() == "Event" { // the thing it is about, not its age
		kind, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "kind")
		name, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "name")
		title = "event about " + view.KindWords(strings.ToLower(kind)+" "+name)
	}
	if n := len(m.tbl.marked()); n > 1 {
		title = fmt.Sprintf("%d picked rows", n)
	}
	m.prompt = menuPrompt(title, items)
	m.prompt.maxItems = m.menuRoom()
	m.mode = modePrompt
	return nil
}

// restartedIn: a container of the pod has restarted (a setup step that
// keeps failing has earlier logs too).
func restartedIn(pod *unstructured.Unstructured) bool {
	statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
	inits, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
	for _, s := range append(statuses, inits...) {
		if c, ok := s.(map[string]any); ok {
			if n, _, _ := unstructured.NestedInt64(c, "restartCount"); n > 0 {
				return true
			}
		}
	}
	return false
}

// hasEarlierRun: p has a run that ended to show here: on a container's
// row that container's, else any of the pod's.
func (m *Model) hasEarlierRun(r view.Row, pod *unstructured.Unstructured) bool {
	if m.loc.kind == locContainers {
		return earlierRun(pod, r.Obj.GetName())
	}
	return slices.ContainsFunc(view.Containers(pod), func(c string) bool { return earlierRun(pod, c) })
}

// earlierRun: a container has a run that ended to show: the one before
// its last restart, or, stopped right now, its current one.
func earlierRun(pod *unstructured.Unstructured, container string) bool {
	return view.ContainerRestarts(pod)[container] > 0 || stoppedRunNow(pod, container)
}

// stoppedRunNow: the container is stopped right now (crashed and waiting
// to restart, or its pod has finished), so its current logs are a run
// that ended. A setup step that finished as it should (istio-init, a
// migration) isn't: it did its work and the API keeps no run before it.
func stoppedRunNow(pod *unstructured.Unstructured, container string) bool {
	if !view.StoppedNow(pod, container) {
		return false
	}
	inits, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
	for _, s := range inits {
		if c, ok := s.(map[string]any); ok && c["name"] == container {
			code, found, _ := unstructured.NestedInt64(c, "state", "terminated", "exitCode")
			return !found || code != 0
		}
	}
	return true
}

// shellItem greys a shell in a pod that has finished, as noShell refuses.
func shellItem(m *Model, row view.Row, it *menuItem) {
	if err := m.noShell(row); err != nil {
		it.disabled, it.off, it.about = true, "finished", "Can't: "+err.Error()
	}
}

// noShell says why a pod has nothing to open a shell in: it has finished,
// or (on its containers) that container isn't running.
func (m *Model) noShell(r view.Row) error {
	pod := m.podRowIfContainer(r).Obj
	phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
	switch phase {
	case "Succeeded":
		return fmt.Errorf("%s has finished its work and nothing runs in it now, so there's no shell. l shows its logs", pod.GetName())
	case "Failed":
		return fmt.Errorf("%s has stopped and nothing runs in it now, so there's no shell. l shows its logs", pod.GetName())
	}
	if m.loc.kind != locContainers {
		return nil
	}
	name := r.Obj.GetName()
	if view.RunningContainers(pod)[name] {
		return nil
	}
	switch {
	case view.StoppedNow(pod, name) && !stoppedRunNow(pod, name): // a setup step that did its work
		return fmt.Errorf("%s has finished its work and nothing runs in it now, so there's no shell. l shows its logs", name)
	case earlierRun(pod, name):
		return fmt.Errorf("%s isn't running right now (it may be crashing), so there's no shell. p shows the logs from before it stopped", name)
	}
	return fmt.Errorf("%s isn't running right now, so there's no shell", name)
}

// appDo runs a workload's action (scale, go back, YAML…) from an app's
// row: on its one workload directly, or, when it has several, in the list
// of them, so you pick which.
func appDo(key string) func(m *Model, r view.Row) tea.Cmd {
	return func(m *Model, r view.Row) tea.Cmd {
		a, ok := m.appFor(r)
		if !ok {
			return nil
		}
		wl := m.appWorkloads(a)
		if key == "F" { // only what listens on a port can be reached
			kept := wl[:0:0]
			for _, w := range runningWorkloads(wl) { // and has a pod running to reach
				if templateHasPorts(w.Obj) {
					kept = append(kept, w)
				}
			}
			switch {
			case len(wl) > 0 && len(runningWorkloads(wl)) == 0:
				m.setFlash("", fmt.Errorf("%s runs no copies, so there is nothing to reach. + scales it up", a.Name))
				return nil
			case len(kept) == 0:
				// None declares a port (it isn't required): F on the workload
				// asks which port, as F on it from its own list does.
				kept = runningWorkloads(wl)
			}
			wl = kept
		}
		runOn := func(o *unstructured.Unstructured) tea.Cmd {
			if !keepsVersionsFor(key, o.GetKind()) { // say so here, before leaving home
				m.setFlash("", fmt.Errorf("that works on %s, and %s is a %s", keyKinds(key), o.GetName(), strings.ToLower(o.GetKind())))
				return nil
			}
			gv, _ := schema.ParseGroupVersion(o.GetAPIVersion())
			res, ok := m.client.Resources.ByKind(gv.Group, o.GetKind())
			if !ok {
				return nil
			}
			if _, _, err := m.undoOff(o); key == "u" && err != nil { // say so here, before leaving home
				m.setFlash("", err)
				return nil
			}
			m.enterNamespace(o.GetNamespace())
			m.navigate(location{kind: locResource, res: res, filter: o.GetName(), via: true}, true)
			m.selectNamed(o.GetName())
			if cmd, ok := runKey(m, key); ok {
				return cmd
			}
			m.setFlash("", fmt.Errorf("that doesn't work on a %s", strings.ToLower(o.GetKind())))
			return nil
		}
		switch len(wl) {
		case 0:
			if a.Runs("CronJob") && (key == "i" || key == "e" || key == "y") {
				// Only cron jobs: the key works on them, one step away.
				m.setFlash("", fmt.Errorf("%s runs only cron jobs: open one (enter, then \"Its cron jobs\", or gC) and press %s there", a.Name, key))
				return nil
			}
			m.setFlash("", fmt.Errorf("%s runs nothing this works on (no deployment, statefulset or daemonset)", a.Name))
			return nil
		case 1:
			return runOn(wl[0].Obj)
		}
		// Several: ask which, every kind in one list, the unwell first.
		sort.SliceStable(wl, func(i, j int) bool { return !workloadReady(wl[i].Obj) && workloadReady(wl[j].Obj) })
		items := make([]menuItem, 0, len(wl))
		for _, w := range wl {
			o := w.Obj
			items = append(items, menuItem{label: strings.ToLower(o.GetKind()) + " " + o.GetName(),
				about: "In namespace " + o.GetNamespace(), run: func() tea.Cmd { return runOn(o) }})
		}
		what := "Which one?"
		if words, ok := appDoWords[key]; ok { // say what picking does
			what = words + ": which one?"
		}
		m.prompt = menuPrompt(fmt.Sprintf("%s runs %d workloads. %s", a.Name, len(wl), what), items)
		m.mode = modePrompt
		return nil
	}
}

// workloadReady: every copy it asks for is ready.
func workloadReady(o *unstructured.Unstructured) bool {
	want, _, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")
	ready, _, _ := unstructured.NestedInt64(o.Object, "status", "readyReplicas")
	if o.GetKind() == "DaemonSet" {
		want, _, _ = unstructured.NestedInt64(o.Object, "status", "desiredNumberScheduled")
		ready, _, _ = unstructured.NestedInt64(o.Object, "status", "numberReady")
	}
	return ready >= want
}

// lowerFirstWord lowercases the first letter of a sentence that follows
// another ("Otherwise: change its YAML…").
func lowerFirstWord(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// templateHasPorts: a workload's pods declare a container port.
func templateHasPorts(o *unstructured.Unstructured) bool {
	containers, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "containers")
	for _, c := range containers {
		if m, ok := c.(map[string]any); ok {
			if ports, _ := m["ports"].([]any); len(ports) > 0 {
				return true
			}
		}
	}
	return false
}

// appDoWords name what appDo's keys do, for its "which one?" picker.
var appDoWords = map[string]string{
	"X": "See what it uses", "y": "Show its YAML", "e": "Edit it", "+": "Scale it", "i": "Change its image",
	"u": "Go back a version", "o": "Watch its rollout", "v": "See its versions", "F": "Reach a port",
}

// runKey runs the action a key has where you are now. It is set in init
// because the actions table refers to appDo, which needs it.
var runKey func(m *Model, key string) (tea.Cmd, bool)

func init() {
	runKey = func(m *Model, key string) (tea.Cmd, bool) {
		if _, ok := m.actionFor(key); !ok {
			return nil, false
		}
		return m.runAction(key), true
	}
}

// appWorkloadItem says which workload a home menu item acts on, and greys
// it out when the app has none it applies to.
// keepsVersionsFor: whether the version keys (o, u, v) work on a kind.
// The rollout view (o) is for Deployments; going back and the versions
// list also work on StatefulSets and DaemonSets. Other keys always pass.
func keepsVersionsFor(key, kind string) bool {
	switch key {
	case "o":
		return kind == "Deployment"
	case "u", "v":
		return kind == "Deployment" || kind == "StatefulSet" || kind == "DaemonSet"
	}
	return true
}

// keyKinds names the kinds a version key works on.
func keyKinds(key string) string {
	if key == "o" {
		return "deployments"
	}
	return "deployments, stateful sets and daemon sets"
}

func appWorkloadItem(key string) func(m *Model, row view.Row, it *menuItem) {
	return func(m *Model, row view.Row, it *menuItem) {
		a, ok := m.appFor(row)
		if !ok {
			return
		}
		wl := m.appWorkloads(a)
		switch {
		case len(wl) == 0:
			it.hidden = true // nothing for it to act on, ever: don't offer it
		case len(wl) == 1:
			kind := strings.ToLower(wl[0].Obj.GetKind())
			if !keepsVersionsFor(key, wl[0].Obj.GetKind()) {
				it.disabled, it.off = true, keyKinds(key)+" only"
				it.about = "Can't: this works on " + keyKinds(key) + "; this app runs a " + kind
				return
			}
			it.about = strings.Replace(it.about, "its deployment", kind+" "+wl[0].Obj.GetName(), 1)
		default:
			it.about = "Pick which of its " + fmt.Sprint(len(wl)) + " workloads, then this. " + it.about
		}
		if key == "u" {
			it.about = argoUndo(it.about, a.Argo)
		}
		if key == "u" && len(wl) == 1 {
			if off, about, _ := m.undoOff(wl[0].Obj); off != "" { // as u on it refuses
				it.disabled, it.off, it.about = true, off, about
				return
			}
		}
		if gvr, ok := m.client.Resources.Served(kube.ReplicaSets); key == "u" && len(wl) == 1 && ok && m.client.Cache.Synced(gvr) {
			if _, fails := m.undoTargetFails(wl[0].Obj); fails { // as u on it says
				it.about += undoFailsAbout
			} else {
				it.about += m.undoFailedAbout(wl[0].Obj)
			}
		}
	}
}

// selfHeals: ArgoCD puts Git's version back on this row's app or workload.
func (m *Model) selfHeals(row view.Row) bool {
	if a, ok := m.appFor(row); ok && m.loc.kind == locHome {
		return a.Argo != nil && a.Argo.SelfHeal
	}
	argo, ok := m.enr.ManagedBy(row.Obj)
	return ok && argo.SelfHeal
}

// argoUndo words u's line for what kubed knows of ArgoCD (argo nil: it
// doesn't manage it): that it may put Git's version back only when it
// manages it and how it syncs can't be read; that it will, when it heals.
// Its default line, shown when the item is off, says neither.
func argoUndo(about string, argo *enrich.ArgoApp) string {
	switch {
	case argo != nil && argo.SelfHeal:
		return selfHealUndo(about)
	case argo != nil && !argo.Found:
		return about + " (ArgoCD may put Git's version back)"
	}
	return about
}

// selfHealUndo words a go-back where ArgoCD heals: it won't last.
func selfHealUndo(about string) string {
	return about + ". ArgoCD heals this app, so it puts Git's version straight back: to go back for good, change the version in Git"
}

// actedOn are the rows a bulk key acts on: the picks, else the row.
func (m *Model) actedOn(row view.Row) []view.Row {
	if picks := m.tbl.marked(); len(picks) > 0 {
		return picks
	}
	return []view.Row{row}
}

// pickedNames names the picked rows a bulk item acts on: "the picked
// node, a" or "the 3 picked nodes".
func pickedNames(rows []view.Row, what string) string {
	if len(rows) == 1 {
		return "the picked " + what + ", " + strings.TrimPrefix(rows[0].Obj.GetName(), markPrefix)
	}
	return fmt.Sprintf("the %d picked %ss", len(rows), what)
}

// restartItem words r's menu item from what a restart does to the rows it
// acts on, as its confirm does: "stays up" only when it does.
func restartItem(m *Model, row view.Row, it *menuItem) {
	rows := m.actedOn(row)
	if len(rows) == 1 && rowKey(rows[0]) == rowKey(row) {
		it.about = "Rollout restart (like kubectl rollout restart). " + restartWhy(row.Obj)
		return
	}
	it.about = "Rollout restart of " + pickedNames(rows, "row") + ": " + replaceWords(rows)
	var paused []string
	for _, r := range rows {
		if pausedDeploy(r.Obj) {
			paused = append(paused, r.Obj.GetName())
		}
	}
	if len(paused) > 0 {
		it.about += " (paused, so not until resumed: " + strings.Join(paused, ", ") + ")"
	}
}

// appRestartItem greys out restart for an app with nothing to restart.
func appRestartItem(m *Model, row view.Row, it *menuItem) {
	a, ok := m.appFor(row)
	if !ok {
		return
	}
	// What r restarts: one scaled to 0 is left out, so it isn't counted.
	wl := m.appWorkloads(a)
	run := runningWorkloads(wl)
	switch {
	case len(wl) == 0:
		it.hidden = true // nothing to restart
	case len(run) == 0:
		it.disabled, it.off, it.about = true, "stopped", "Can't: it runs no copies, so there is nothing to restart. + scales it up"
	case len(run) == 1: // what it does to uptime, as the confirm says
		it.about = "Rollout restart of its " + run[0].Cells[0] + ". " + restartWhy(run[0].Obj)
	default: // each says in the confirm whether it stays up
		it.about = "Rollout restart of its " + kindCounts(run) + ": " + replaceWords(run)
	}
}

// ownerRestartItem names what restarting a pod really restarts.
func ownerRestartItem(m *Model, row view.Row, it *menuItem) {
	pod := m.podRowIfContainer(row).Obj
	switch kind := m.enr.TopOwner(pod).GetKind(); kind {
	case "Deployment", "StatefulSet", "DaemonSet":
		it.label = "Restart"
		it.about = "Rollout restart of its " + strings.ToLower(kind) + ", " + m.enr.TopOwner(pod).GetName() + ". " + restartWhy(m.enr.TopOwner(pod))
	default:
		it.label = "Restart"
		it.disabled, it.off = true, "nothing runs it"
		it.about = "Can't: nothing runs this pod (no deployment, statefulset or daemonset), so nothing can restart it"
	}
}

// openAbout explains the menu's first item.
func openAbout(k locKind, label string) string {
	switch {
	case k == locHome:
		return "Its pods, and why each one is or isn't healthy"
	case k == locContexts:
		return "Point kubed at this cluster"
	case label == "Containers":
		return "The containers in this pod: status, restarts, image"
	case label == "Show only this namespace":
		return "Every screen shows just this namespace until you change it (:ns all for every namespace)"
	case label == "Pods" || label == "Nodes":
		return "The " + strings.ToLower(label) + " it runs"
	case label == "Pods behind it":
		return "The pods it sends traffic to"
	case label == "Runs":
		return "The jobs it started, newest first: did they finish, how long they took, their logs"
	}
	return ""
}

func (m *Model) copyName(r view.Row) tea.Cmd {
	if m.loc.kind == locForwards && len(r.Cells) > 0 { // a port: its address, not its place in the list
		return m.copy(r.Cells[0], "address")
	}
	return m.copy(strings.TrimPrefix(r.Obj.GetName(), markPrefix), "name")
}

func (m *Model) copyYAML(r view.Row) tea.Cmd {
	ref := m.ref(r)
	return func() tea.Msg {
		out, err := m.readYAML(ref)
		if err != nil {
			return resultMsg{err: err}
		}
		if err := copyText(hideSecretValues(ref, out, m.opts.ReadOnly)); err != nil {
			return resultMsg{err: err}
		}
		return resultMsg{text: "copied YAML of " + ref.String()}
	}
}

func (m *Model) copy(text, what string) tea.Cmd {
	if err := copyText(text); err != nil {
		m.setFlash("", err)
		return nil
	}
	if n := strings.Count(strings.TrimRight(text, "\n"), "\n") + 1; n > 1 { // a last newline ends a line, not starts one
		// "copied 26 lines: what's on screen · C copies all"
		m.setFlash(fmt.Sprintf("copied %s: %s", view.Plural(n, "line"), what), nil)
		return nil
	}
	m.setFlash(fmt.Sprintf("copied %s: %s", what, truncate(text, 60)), nil)
	return nil
}
