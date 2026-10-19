package ui

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// r138Budgets: node n1 runs pods db-a and db-b (app=db, ready), held by
// budget db-pdb that allows none down; budget other covers other pods,
// budget stale's status is from before its last change.
func r138Budgets() *Model {
	pod := func(name string) runtime.Object {
		return mxObj("v1", "Pod", "shop", name, map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "db"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "db-1", "uid": "ReplicaSet-db-1", "controller": true}}},
			"spec":   map[string]any{"nodeName": "n1", "containers": []any{map[string]any{"name": "app", "image": "db:1"}}},
			"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
	}
	pdb := func(name, app string, gen, seen int64) runtime.Object {
		return mxObj("policy/v1", "PodDisruptionBudget", "shop", name, map[string]any{"metadata": map[string]any{"generation": gen},
			"spec":   map[string]any{"minAvailable": int64(2), "selector": map[string]any{"matchLabels": map[string]any{"app": app}}},
			"status": map[string]any{"observedGeneration": seen, "disruptionsAllowed": int64(0), "currentHealthy": int64(2), "desiredHealthy": int64(2)}})
	}
	c := mxNewCluster([]runtime.Object{
		mxObj("v1", "Node", "", "n1", nil),
		pod("db-a"), pod("db-b"),
		pdb("db-pdb", "db", 1, 1), pdb("other", "other", 1, 1), pdb("stale", "db", 2, 1),
	}, false)
	return mxModel(c, 200, 50)
}

// QA round 138: a budget allowed no pod down. D on the node didn't name
// the budget that will hold the drain, and ctrl-d on one of its pods
// didn't say a delete isn't held by it. Only a budget whose selector
// matches the pod, its status current, is named.
func TestBudgetThatHoldsDrainNamed(t *testing.T) {
	m := r138Budgets()
	why := m.drainWhy("n1")
	if !strings.Contains(why, "The drain will wait on budget db-pdb: no pod of it may be taken down now.") {
		t.Errorf("drain says %q", why)
	}
	if strings.Contains(why, "budget other") || strings.Contains(why, "budget stale") {
		t.Errorf("drain names a budget that doesn't hold it: %q", why)
	}
	pod, _ := m.enr.Get(kube.Pods, "shop", "db-a")
	m.deleteRows([]view.Row{{Obj: pod, Cells: []string{"db-a"}}})
	if m.prompt == nil {
		t.Fatal("no confirm")
	}
	said := m.prompt.title + "\n" + strings.Join(m.prompt.warnings, "\n")
	if !strings.Contains(said, "Its disruption budget db-pdb allows none down now; a delete isn't held by it (only drains are), so this takes it below its minimum.") {
		t.Errorf("delete says %q", said)
	}
	if strings.Contains(said, "budget other") || strings.Contains(said, "budget stale") {
		t.Errorf("delete names a budget that doesn't cover it: %q", said)
	}
}

// QA round 138: the deployments box of one whose pod can't be placed says
// "E shows why no node fits", but E there listed only the deployment's
// own events: the scheduler's is on the pod. E on a workload lists its
// pods' and its ReplicaSets' events too, as E on home does.
func TestWorkloadEventsIncludeItsPods(t *testing.T) {
	ev := func(name, kind, about, reason, msg string) runtime.Object {
		return mxObj("v1", "Event", "shop", name, map[string]any{"involvedObject": map[string]any{"kind": kind, "name": about, "namespace": "shop"},
			"reason": reason, "message": msg, "type": "Warning", "lastTimestamp": "2026-10-16T10:00:00Z"})
	}
	c := mxNewCluster([]runtime.Object{
		mxObj("apps/v1", "Deployment", "shop", "pending", map[string]any{"spec": map[string]any{"replicas": int64(1)}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "pending-1", map[string]any{"metadata": mxOwner("apps/v1", "Deployment", "pending")}),
		mxObj("v1", "Pod", "shop", "pending-1-a", map[string]any{"metadata": mxOwner("apps/v1", "ReplicaSet", "pending-1"), "status": map[string]any{"phase": "Pending"}}),
		mxObj("v1", "Pod", "shop", "other-1-a", map[string]any{"status": map[string]any{"phase": "Running"}}),
		ev("e1", "Pod", "pending-1-a", "FailedScheduling", "0/3 nodes are available: 3 Insufficient cpu."),
		ev("e2", "ReplicaSet", "pending-1", "FailedCreate", "Error creating: forbidden"),
		ev("e3", "Deployment", "pending", "ScalingReplicaSet", "Scaled up replica set pending-1 from 0 to 1"),
		ev("e4", "Pod", "other-1-a", "BackOff", "someone else's pod"),
	}, false)
	m := mxModel(c, 200, 50)
	dep, _ := m.enr.Get(kube.Deployments, "shop", "pending")
	msg, _ := m.objectEvents(view.Row{Obj: dep, Cells: []string{"pending"}})().(pagerMsg)
	for _, want := range []string{"pod pending-1-a", "not enough free CPU", "Error creating: forbidden"} {
		if !strings.Contains(msg.content, want) {
			t.Errorf("E lacks %q:\n%s", want, msg.content)
		}
	}
	if strings.Contains(msg.content, "someone else") {
		t.Errorf("E lists another's pod:\n%s", msg.content)
	}
}

// QA round 138: a deployment with no earlier version (a first version
// that can't pull its image): home's Enter menu offered "Go back to the
// previous version" with no note, and u on home moved to the deployments
// list before saying it has none. The menu greys it out with the reason,
// as the list's menu does, and u says so where you are. A stateful set
// with one kept version the same.
func TestHomeUndoWithNoEarlierVersion(t *testing.T) {
	labels := map[string]any{"app": "badimage"}
	tmpl := map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nope:1"}}}}
	pod := func(owner, ownerKind string) runtime.Object {
		return mxObj("v1", "Pod", "mx", owner+"-x", map[string]any{"metadata": map[string]any{"labels": labels,
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": ownerKind, "name": owner, "uid": ownerKind + "-" + owner, "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nope:1"}}},
			"status": map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "app", "image": "nope:1", "ready": false,
				"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff", "message": `Back-off pulling image "nope:1"`}}}}}})
	}
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		t.Run(kind, func(t *testing.T) {
			objs := []runtime.Object{mxObj("v1", "Namespace", "", "mx", nil),
				mxObj("apps/v1", kind, "mx", "badimage", map[string]any{"metadata": map[string]any{"labels": labels},
					"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": labels}, "template": tmpl}})}
			if kind == "Deployment" {
				objs = append(objs, mxObj("apps/v1", "ReplicaSet", "mx", "badimage-1", map[string]any{"metadata": map[string]any{"labels": labels,
					"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}, "ownerReferences": mxOwner("apps/v1", "Deployment", "badimage")["ownerReferences"]},
					"spec": map[string]any{"replicas": int64(1), "template": tmpl}}), pod("badimage-1", "ReplicaSet"))
			} else {
				objs = append(objs, mxObj("apps/v1", "ControllerRevision", "mx", "badimage-h1", map[string]any{"metadata": mxOwner("apps/v1", "StatefulSet", "badimage"),
					"revision": int64(1), "data": map[string]any{"spec": map[string]any{"template": tmpl}}}), pod("badimage", "StatefulSet"))
			}
			cl := mxNewCluster(objs, false)
			defer cl.client.Cache.Stop()
			m := mxModel(cl, 160, 50)
			m.jumpTo(location{kind: locHome})
			m.refreshRows()
			found := false
			for i, r := range m.tbl.rows {
				if a, ok := m.appFor(r); ok && a.Name == "badimage" {
					m.tbl.jump(i)
					found = true
				}
			}
			if !found {
				t.Fatal("no app badimage on home")
			}
			m.menu()
			for _, it := range m.prompt.items {
				if it.key == "u" && (!it.disabled || it.off != "no earlier version") {
					t.Errorf("menu: %q · %q: %q", it.label, it.off, it.about)
				}
			}
			m.prompt, m.mode = nil, modeTable
			before := mxSnapOf(m)
			if _, err := mxPress(m, "u"); err != nil {
				t.Fatal(err)
			}
			if !m.flashErr || !strings.Contains(m.flash, "badimage has no earlier version to go back to") {
				t.Errorf("u says %q", m.flash)
			}
			after := mxSnapOf(m)
			after.flash = before.flash
			if after != before {
				t.Errorf("u moved from %+v to %+v", before, after)
			}
		})
	}
}

// QA round 137: a rollout whose new version Kubernetes can't make pods
// for (an admission webhook says no) while revision 1's pods serve. o
// said "rolling out: 0 of 2 updated" with no reason, v marked revision 2
// "(running now)", and shipped listed it as gone out. o says it is stuck
// and why, v says the newest has no pods and which revision serves, and
// shipped says no pod of it ran.
func TestRolloutViewsWhenNewPodsCantBeMade(t *testing.T) {
	cl := r138RolloutCluster()
	defer cl.client.Cache.Stop()
	m, row := r138OnDeployment(t, cl)
	m.rolloutView(row)
	if m.pager == nil {
		t.Fatal("no rollout view")
	}
	o := m.pager.plainText()
	if !strings.Contains(o, "stuck") || !strings.Contains(o, `Error creating: admission webhook "mutate.kyverno.svc-fail" denied the request: no`) || strings.Contains(o, "combined") {
		t.Errorf("o says:\n%s", o)
	}
	m.closePager()
	items, _ := m.versionItems(row)
	if len(items) < 2 || strings.Contains(items[0].label, "running now") || !strings.Contains(items[0].label, "(newest, no pods; revision 1 serves)") {
		t.Errorf("v: %q", items[0].label)
	}
	msg, _ := m.shipped()().(pagerMsg)
	if !strings.Contains(msg.content, "web:2") || !strings.Contains(msg.content, "(no pod of it ran: Kubernetes can't make its pods)") {
		t.Errorf("shipped:\n%s", msg.content)
	}
}

// r138OnDeployment: the deployments list, on deployment web.
func r138OnDeployment(t *testing.T, cl *mxCluster) (*Model, view.Row) {
	t.Helper()
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("deployments", true)
	m.refreshRows()
	row, ok := m.tbl.selected()
	if !ok || row.Obj.GetName() != "web" {
		t.Fatal("no deployment web")
	}
	return m, row
}

// r138RolloutCluster: deployment web rolling out to revision 2, whose
// ReplicaSet can't make pods (an admission webhook says no), while
// revision 1's 2 pods serve.
func r138RolloutCluster() *mxCluster {
	made := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	labels := map[string]any{"app": "web"}
	tmpl := func(img string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": img}}}}
	}
	rs := func(name, rev, img string, n int64, ago time.Duration) runtime.Object {
		return mxObj("apps/v1", "ReplicaSet", "mx", name, map[string]any{"metadata": map[string]any{"labels": labels, "creationTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339),
			"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}, "ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
			"spec": map[string]any{"replicas": n, "template": tmpl(img)}, "status": map[string]any{"replicas": n, "readyReplicas": n}})
	}
	pod := func(name string) runtime.Object {
		return mxObj("v1", "Pod", "mx", name, map[string]any{"metadata": map[string]any{"labels": labels, "ownerReferences": mxOwner("apps/v1", "ReplicaSet", "web-1")["ownerReferences"]},
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "app", "image": "web:1"}}},
			"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}})
	}
	return mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "mx", nil),
		mxObj("apps/v1", "Deployment", "mx", "web", map[string]any{"metadata": map[string]any{"labels": labels, "generation": int64(2), "creationTimestamp": made},
			"spec": map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": labels}, "template": tmpl("web:2")},
			"status": map[string]any{"observedGeneration": int64(2), "replicas": int64(2), "updatedReplicas": int64(0), "readyReplicas": int64(2), "availableReplicas": int64(2),
				"conditions": []any{map[string]any{"type": "Progressing", "status": "True", "reason": "ReplicaSetUpdated"},
					map[string]any{"type": "ReplicaFailure", "status": "True", "reason": "FailedCreate", "message": `admission webhook "mutate.kyverno.svc-fail" denied the request: no`}}}}),
		rs("web-1", "1", "web:1", 2, 2*time.Hour), rs("web-2", "2", "web:2", 0, 5*time.Minute), pod("web-1-a"), pod("web-1-b"),
		mxObj("v1", "Event", "mx", "web-2.1", map[string]any{"involvedObject": map[string]any{"kind": "ReplicaSet", "name": "web-2", "namespace": "mx"},
			"type": "Warning", "reason": "FailedCreate", "count": int64(25), "lastTimestamp": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
			"message": `(combined from similar events): Error creating: admission webhook "mutate.kyverno.svc-fail" denied the request: no`}),
	}, false)
}

// QA round 137: esc from the versions list opened from the rollout view
// (o, then v) skipped the rollout view, back to the list or home. It goes
// back to the rollout view it came from.
func TestEscFromVersionsBackToRollout(t *testing.T) {
	cl := r138RolloutCluster()
	defer cl.client.Cache.Stop()
	m, row := r138OnDeployment(t, cl)
	m.rolloutView(row)
	if m.pager == nil || m.pager.versions == nil {
		t.Fatal("no rollout view")
	}
	m.pager.versions()
	if m.prompt == nil {
		t.Fatal("no versions list")
	}
	if _, err := mxPress(m, "esc"); err != nil {
		t.Fatal(err)
	}
	if m.pager == nil || m.pager.title != "rollout of deployment web" || m.prompt != nil {
		t.Errorf("esc: pager %v, prompt %v, at %+v", m.pager != nil, m.prompt != nil, m.loc)
	}
}

// QA round 138: a refused Job edit (its pod template can't change) wrote
// the API's whole answer, the pod spec dumped as Go values, above the
// edit. It says in one plain line which field can't change, never its
// value.
func TestImmutableFieldInPlainWords(t *testing.T) {
	job := `dry run: Job.batch "pi" is invalid: spec.template: Invalid value: core.PodTemplateSpec{ObjectMeta:v1.ObjectMeta{Name:"", Labels:map[string]string{"a":"b"}}, Spec:core.PodSpec{Containers:[]core.Container{core.Container{Name:"pi", Image:"perl:5"}}}}: field is immutable`
	if got := plainEditError(job); got != "a job's pod template can't be changed once it exists; create the job again (or edit its cron job)" {
		t.Errorf("job: %q", got)
	}
	dep := `dry run: Deployment.apps "web" is invalid: spec.selector: Invalid value: v1.LabelSelector{MatchLabels:map[string]string{"app":"x"}, MatchExpressions:[]v1.LabelSelectorRequirement(nil)}: field is immutable`
	if got := plainEditError(dep); got != "spec.selector can't be changed once it exists. Nothing was changed" {
		t.Errorf("deployment: %q", got)
	}
}

// QA round 137: u on a stateful set whose only pod is failing (one pod at
// a time) puts the spec back, but the pod stays as it is until deleted.
// The confirm says so.
func TestStatefulSetUndoSaysDeleteStuckPod(t *testing.T) {
	labels := map[string]any{"app": "db"}
	tmpl := func(img string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": img}}}}
	}
	cr := func(name, img string, rev int64, ago time.Duration) runtime.Object {
		return mxObj("apps/v1", "ControllerRevision", "mx", name, map[string]any{"metadata": map[string]any{"creationTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339),
			"ownerReferences": mxOwner("apps/v1", "StatefulSet", "db")["ownerReferences"]}, "revision": rev, "data": map[string]any{"spec": map[string]any{"template": tmpl(img)}}})
	}
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "mx", nil),
		mxObj("apps/v1", "StatefulSet", "mx", "db", map[string]any{"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": labels}, "template": tmpl("db:bad")},
			"status": map[string]any{"replicas": int64(1), "currentRevision": "db-h1", "updateRevision": "db-h1"}}),
		cr("db-h0", "db:1", 1, 2*time.Hour), cr("db-h1", "db:bad", 2, 10*time.Minute),
		mxObj("v1", "Pod", "mx", "db-0", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "db", "controller-revision-hash": "db-h1"},
			"ownerReferences": mxOwner("apps/v1", "StatefulSet", "db")["ownerReferences"]},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "db:bad"}}},
			"status": map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "app", "image": "db:bad", "ready": false,
				"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff", "message": `Back-off pulling image "db:bad"`}}}}}}),
	}, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("statefulsets", true)
	m.refreshRows()
	row, ok := m.tbl.selected()
	if !ok || row.Obj.GetName() != "db" {
		t.Fatal("no stateful set db")
	}
	m.undoRolloutNow(row)
	if m.prompt == nil || !strings.Contains(m.prompt.title, "Delete it (ctrl-d on db-0) for this to take effect") {
		t.Errorf("u confirm: %+v", m.prompt)
	}
	// QA round 159: it also said "it is down for a moment", though nothing
	// stops: the failing pod stays until deleted, and it is down already.
	if m.prompt != nil && strings.Contains(m.prompt.title, "down for a moment") {
		t.Errorf("u confirm says it goes down: %q", m.prompt.title)
	}
}

// QA round 137: u on a daemon set went back to bad-one, whose pull had
// failed (the event kept), with no warning. The confirm and the menu say
// that version failed when it last ran.
func TestUndoWarnsVersionFailedBefore(t *testing.T) {
	labels := map[string]any{"app": "agent"}
	tmpl := func(img string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": img}}}}
	}
	cr := func(name, img string, rev int64, ago time.Duration) runtime.Object {
		return mxObj("apps/v1", "ControllerRevision", "mx", name, map[string]any{"metadata": map[string]any{"creationTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339),
			"ownerReferences": mxOwner("apps/v1", "DaemonSet", "agent")["ownerReferences"]}, "revision": rev, "data": map[string]any{"spec": map[string]any{"template": tmpl(img)}}})
	}
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "mx", nil),
		mxObj("apps/v1", "DaemonSet", "mx", "agent", map[string]any{"spec": map[string]any{"selector": map[string]any{"matchLabels": labels}, "template": tmpl("agent:bad-two")}}),
		cr("agent-h0", "agent:bad-one", 1, time.Hour), cr("agent-h1", "agent:bad-two", 2, 10*time.Minute),
		mxObj("v1", "Event", "mx", "agent-old.1", map[string]any{"involvedObject": map[string]any{"kind": "Pod", "name": "agent-q7x2k", "namespace": "mx"},
			"type": "Warning", "reason": "Failed", "lastTimestamp": time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
			"message": `Failed to pull image "agent:bad-one": not found`}),
	}, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("daemonsets", true)
	m.refreshRows()
	row, ok := m.tbl.selected()
	if !ok || row.Obj.GetName() != "agent" {
		t.Fatal("no daemon set agent")
	}
	m.menu()
	for _, it := range m.prompt.items {
		if it.key == "u" && !strings.HasSuffix(it.about, ". Going back won't help: that version failed when it last ran") {
			t.Errorf("menu: %q", it.about)
		}
	}
	m.prompt, m.mode = nil, modeTable
	m.undoRolloutNow(row)
	if m.prompt == nil || !strings.Contains(m.prompt.title, "That version failed when it last ran, so going back won't help: its image agent:bad-one couldn't be downloaded (that image or tag doesn't exist)") {
		t.Errorf("u confirm: %q", m.prompt.title)
	}
}

// Review of round 138: a pull that timed out often passes: the menu and
// the confirm say going back may not help, not that it won't.
func TestUndoWarnsVersionMayHaveFailed(t *testing.T) {
	labels := map[string]any{"app": "agent"}
	tmpl := func(img string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": img}}}}
	}
	cr := func(name, img string, rev int64, ago time.Duration) runtime.Object {
		return mxObj("apps/v1", "ControllerRevision", "mx", name, map[string]any{"metadata": map[string]any{"creationTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339),
			"ownerReferences": mxOwner("apps/v1", "DaemonSet", "agent")["ownerReferences"]}, "revision": rev, "data": map[string]any{"spec": map[string]any{"template": tmpl(img)}}})
	}
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "mx", nil),
		mxObj("apps/v1", "DaemonSet", "mx", "agent", map[string]any{"spec": map[string]any{"selector": map[string]any{"matchLabels": labels}, "template": tmpl("agent:bad-two")}}),
		cr("agent-h0", "agent:bad-one", 1, time.Hour), cr("agent-h1", "agent:bad-two", 2, 10*time.Minute),
		mxObj("v1", "Event", "mx", "agent-old.1", map[string]any{"involvedObject": map[string]any{"kind": "Pod", "name": "agent-q7x2k", "namespace": "mx"},
			"type": "Warning", "reason": "Failed", "lastTimestamp": time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
			"message": `Failed to pull image "agent:bad-one": dial tcp 1.2.3.4:443: i/o timeout`}),
	}, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("daemonsets", true)
	m.refreshRows()
	row, ok := m.tbl.selected()
	if !ok || row.Obj.GetName() != "agent" {
		t.Fatal("no daemon set agent")
	}
	m.menu()
	for _, it := range m.prompt.items {
		if it.key == "u" && !strings.HasSuffix(it.about, ". Going back may not help: that version failed when it last ran") {
			t.Errorf("menu: %q", it.about)
		}
	}
	m.prompt, m.mode = nil, modeTable
	m.undoRolloutNow(row)
	if m.prompt == nil || !strings.Contains(m.prompt.title, "That version failed when it last ran, so going back may not help: its image agent:bad-one couldn't be downloaded") {
		t.Errorf("u confirm: %q", m.prompt.title)
	}
}
