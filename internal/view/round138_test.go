package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

const r138Quota = `Error creating: pods "web-2-x" is forbidden: exceeded quota: two-pods, requested: pods=1, used: pods=2, limited: pods=2`

// r138Workload: a deployment (with its ReplicaSet web-2), a stateful set
// or a daemon set "web" asking for want copies, made copies made and ready, and a
// FailedCreate warning (merged, as Kubernetes repeats it) on what makes
// its pods when quota is set.
func r138Workload(kind string, want, made int64, quota bool) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	status := map[string]any{"observedGeneration": int64(1), "replicas": made, "updatedReplicas": made, "readyReplicas": made, "availableReplicas": made}
	if quota && kind == "Deployment" {
		status["conditions"] = []any{map[string]any{"type": "ReplicaFailure", "status": "True", "reason": "FailedCreate",
			"message": `pods "web-2-x" is forbidden: exceeded quota: two-pods, requested: pods=1, used: pods=2, limited: pods=2`}}
	}
	w := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": kind,
		"metadata": map[string]any{"name": "web", "namespace": "mxa", "uid": "w-web", "generation": int64(1), "labels": map[string]any{"app": "mx"}},
		"spec": map[string]any{"replicas": want, "selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}},
		"status": status}}
	owner, ownerUID := "web", "w-web"
	s := listStore{}
	res := []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}}
	if kind == "Deployment" {
		owner, ownerUID = "web-2", "uid-web-2"
		s[kube.Deployments] = []*unstructured.Unstructured{w}
		s[kube.ReplicaSets] = []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": "web-2", "namespace": "mxa", "uid": "uid-web-2",
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "w-web", "controller": true}}},
			"spec": map[string]any{"replicas": want}, "status": map[string]any{"replicas": made, "readyReplicas": made}}}}
		res = append(res, kube.Resource{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, kube.Resource{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true})
	} else {
		gvr := map[string]kube.Resource{"DaemonSet": {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true}, "StatefulSet": {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true}}[kind]
		s[gvr.GVR] = []*unstructured.Unstructured{w}
		res = append(res, gvr)
	}
	if kind == "DaemonSet" { // one on each of want nodes
		status["desiredNumberScheduled"], status["currentNumberScheduled"], status["numberReady"] = want, made, made
		delete(w.Object["spec"].(map[string]any), "replicas")
	}
	var pods []*unstructured.Unstructured
	for i := range made {
		p := mxPod("web-"+string(rune('a'+i)), "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
		p.OwnerReferences[0].Kind, p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = map[bool]string{true: "ReplicaSet", false: kind}[kind == "Deployment"], owner, types.UID(ownerUID)
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		pods = append(pods, mxObj(p))
	}
	s[kube.Pods] = pods
	if quota {
		ev := mxEvent("web.1", owner, "", "FailedCreate", "(combined from similar events): "+r138Quota, time.Minute)
		ev.Object["involvedObject"] = map[string]any{"kind": map[bool]string{true: "ReplicaSet", false: kind}[kind == "Deployment"], "name": owner}
		s[kube.Events] = []*unstructured.Unstructured{ev}
	}
	return enrich.New(mxStore{s}, kube.NewResources(res), nil), pods, w
}

// QA round 138: a quota let a deployment scaled 2 -> 3 make only 2 copies.
// Home said "Running: healthy", READY 2/2, and its box had no Problem
// line. It says Kubernetes can make only 2 of 3, quoting why (without the
// "(combined from similar events): " Kubernetes puts in front), READY
// counts out of 3 as kubectl does, and the Fix is the one for no pods. A
// stateful set and a daemon set the same.
func TestHomeSaysCopiesKubernetesCantMake(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		t.Run(kind, func(t *testing.T) {
			e, pods, w := r138Workload(kind, 3, 2, true)
			a := r136App(t, e, pods, w)
			if a.Status.Tone != ToneWarn || a.Status.Why != "Kubernetes can make only 2 of 3 copies: "+r138Quota {
				t.Errorf("status %+v", a.Status)
			}
			if got := readyCell(a); got != "2/3" {
				t.Errorf("READY %s, want 2/3 as kubectl counts", got)
			}
			lines := strings.Join(AppLines(a, Lookups{}), "\n")
			for _, want := range []string{"Problem  " + a.Status.Short + ": Kubernetes can make only 2 of 3 copies: " + r138Quota,
				"Fix      E shows why Kubernetes can't make its pods (often a missing service account, a quota or a policy)", "Pods     2 of 3 ready"} {
				if !strings.Contains(lines, want) {
					t.Errorf("box lacks %q:\n%s", want, lines)
				}
			}
			box := strings.Join(workloadDetail(e, w), "\n")
			for _, want := range []string{"Problem  " + a.Status.Short + ": Kubernetes can make only 2 of 3 copies: " + r138Quota,
				"Fix      E shows why Kubernetes can't make its pods (often a missing service account, a quota or a policy)"} {
				if !strings.Contains(box, want) {
					t.Errorf("%s box lacks %q:\n%s", kind, want, box)
				}
			}
			// All made: nothing to say, whatever an old warning says.
			e, pods, w = r138Workload(kind, 2, 2, true)
			if a := r136App(t, e, pods, w); a.Status != (Status{"Running", "healthy", ToneNormal}) || readyCell(a) != "2/2" {
				t.Errorf("all made: status %+v, READY %s", a.Status, readyCell(a))
			}
			if box := strings.Join(workloadDetail(e, w), "\n"); strings.Contains(box, "Problem") {
				t.Errorf("all made: box %s", box)
			}
			// Short, but Kubernetes says nothing of why (still making them): no claim.
			e, _, w = r138Workload(kind, 3, 2, false)
			if box := strings.Join(workloadDetail(e, w), "\n"); strings.Contains(box, "Problem") {
				t.Errorf("no reason: box %s", box)
			}
		})
	}
}

// QA round 138: a deployment that has no pods because Kubernetes can't
// make them (a missing service account, a LimitRange) had no Problem line
// in the deployments list's box, though home explains it. It says it as
// home does, with the same Fix.
func TestDeploymentsBoxSaysWhyNoPods(t *testing.T) {
	e, pods, w := r138Workload("Deployment", 1, 0, true)
	a := r136App(t, e, pods, w)
	if a.Status.Short != "No pods" || a.Status.Why != "asks for 1 copy but Kubernetes can't make them: "+r138Quota {
		t.Fatalf("home status %+v", a.Status)
	}
	box := strings.Join(workloadDetail(e, w), "\n")
	for _, want := range []string{"Problem  No pods: asks for 1 copy but Kubernetes can't make them: " + r138Quota,
		"Fix      E shows why Kubernetes can't make its pods (often a missing service account, a quota or a policy)"} {
		if !strings.Contains(box, want) {
			t.Errorf("box lacks %q:\n%s", want, box)
		}
	}
}

// An event message Kubernetes merged from repeats starts with
// "(combined from similar events): ": kubed quotes what follows.
func TestMessageWordsDropsCombinedPrefix(t *testing.T) {
	if got := MessageWords("(combined from similar events): " + r138Quota); got != r138Quota {
		t.Errorf("got %q", got)
	}
}

// A deployment says it can't make its pods in its ReplicaFailure
// condition while that lasts: a warning left from before, the condition
// gone, is no claim; the condition alone (its warnings expired) is quoted.
func TestCopiesKubernetesCantMakeNeedsCondition(t *testing.T) {
	e, pods, w := r138Workload("Deployment", 3, 2, true)
	unstructured.RemoveNestedField(w.Object, "status", "conditions")
	if a := r136App(t, e, pods, w); a.Status.Short == "Missing pods" {
		t.Errorf("put right: status %+v", a.Status)
	}
	_, pods, w = r138Workload("Deployment", 3, 2, true)
	_ = unstructured.SetNestedSlice(w.Object, []any{map[string]any{"type": "ReplicaFailure", "status": "True", "reason": "FailedCreate",
		"message": `pods "web-2-x" is forbidden: exceeded quota: two-pods`}}, "status", "conditions")
	e = enrich.New(mxStore{listStore{kube.Pods: pods, kube.Deployments: {w}}}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}}), nil)
	if a := r136App(t, e, pods, w); a.Status.Why != `Kubernetes can make only 2 of 3 copies: pods "web-2-x" is forbidden: exceeded quota: two-pods` {
		t.Errorf("condition only: status %+v", a.Status)
	}
}

// r138HPA: an autoscaler of deployment ghost with these conditions.
func r138HPA(conds ...map[string]any) *unstructured.Unstructured {
	var cs []any
	for _, c := range conds {
		cs = append(cs, c)
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]any{"name": "ghost-hpa", "namespace": "mxa"},
		"spec":     map[string]any{"scaleTargetRef": map[string]any{"kind": "Deployment", "name": "ghost"}, "minReplicas": int64(1), "maxReplicas": int64(3)},
		"status":   map[string]any{"currentReplicas": int64(0), "conditions": cs}}}
}

// QA round 138: an autoscaler whose deployment doesn't exist was a normal
// row in :hpa, not counted to check, its box only "AbleToScale is false".
// It is flagged, and says plainly that what it scales isn't there; one
// that can't read its metrics says that.
func TestHPAFlagsWhatItCantScale(t *testing.T) {
	row := For(kube.Resource{GVR: kube.HPAs}).Row
	detail := For(kube.Resource{GVR: kube.HPAs}).Detail
	for _, c := range []struct {
		name string
		cond map[string]any
		want string
	}{
		{"target gone", map[string]any{"type": "AbleToScale", "status": "False", "reason": "FailedGetScale",
			"message": `the HPA controller was unable to get the target's current scale: deployments.apps "ghost" not found`},
			"Problem  deployment ghost, which it scales, doesn't exist, so it scales nothing"},
		{"can't read it", map[string]any{"type": "AbleToScale", "status": "False", "reason": "FailedGetScale",
			"message": `the HPA controller was unable to get the target's current scale: deployments.apps "ghost" is forbidden`},
			`Problem  it can't read what it scales: the HPA controller was unable to get the target's current scale: deployments.apps "ghost" is forbidden`},
		{"no metrics", map[string]any{"type": "ScalingActive", "status": "False", "reason": "FailedGetResourceMetric",
			"message": "the HPA was unable to compute the replica count: failed to get cpu utilization: unable to get metrics for resource cpu: no metrics returned from resource metrics API"},
			"Problem  it isn't scaling: it can't read its metrics: the HPA was unable to compute the replica count: failed to get cpu utilization: unable to get metrics for resource cpu: no metrics returned from resource metrics API"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := r138HPA(c.cond)
			if _, tone := row(nil, h); tone != ToneWarn {
				t.Errorf("tone %v, want to check", tone)
			}
			if got := strings.Join(detail(nil, h), "\n"); got != c.want {
				t.Errorf("box %q, want %q", got, c.want)
			}
		})
	}
	// Scaled to 0 on purpose: the autoscaler waits, nothing to check.
	h := r138HPA(map[string]any{"type": "ScalingActive", "status": "False", "reason": "ScalingDisabled", "message": "scaling is disabled since the replica count of the target is zero"})
	if _, tone := row(nil, h); tone != ToneNormal {
		t.Errorf("scaled to 0: tone %v", tone)
	}
}

const r138Webhook = `Error creating: admission webhook "mutate.kyverno.svc-fail" denied the request: no`

// r138StuckRollout: deployment web rolling out to version 2, whose
// ReplicaSet can't make pods (an admission webhook says no), while version
// 1's 2 pods still serve.
func r138StuckRollout() (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	e, pods, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(2), "updatedReplicas": int64(0),
		"readyReplicas": int64(2), "availableReplicas": int64(2), "conditions": []any{
			map[string]any{"type": "Available", "status": "True"},
			map[string]any{"type": "Progressing", "status": "True", "reason": "ReplicaSetUpdated"},
			map[string]any{"type": "ReplicaFailure", "status": "True", "reason": "FailedCreate", "message": `admission webhook "mutate.kyverno.svc-fail" denied the request: no`}}}, 0, 2)
	ev := mxEvent("web-2.1", "web-2", "", "FailedCreate", "(combined from similar events): "+r138Webhook, time.Minute)
	ev.Object["involvedObject"] = map[string]any{"kind": "ReplicaSet", "name": "web-2"}
	ev.Object["count"] = int64(25)
	s := listStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: e.List(kube.ReplicaSets, "mxa"), kube.Events: {ev}}
	e = enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	return e, pods, dep
}

// QA round 137: a rollout whose new version can't make pods (an admission
// webhook, a quota) while the old pods serve: home said "Running 2/2
// healthy", and the deployments list didn't flag it, its box saying
// nothing. Home says the rollout is stuck and why; the list flags it, its
// box says why, with the Fix for pods Kubernetes can't make.
func TestRolloutThatCantMakeItsPods(t *testing.T) {
	e, pods, dep := r138StuckRollout()
	a := r136App(t, e, pods, dep)
	want := "new version of deployment/web can't make its pods: " + r138Webhook
	if a.Status.Short != "Rollout stuck" || a.Status.Why != want || a.Status.Tone != ToneBad {
		t.Errorf("status %+v", a.Status)
	}
	lines := strings.Join(AppLines(a, Lookups{}), "\n")
	if !strings.Contains(lines, "Fix      "+cantMakeFix) {
		t.Errorf("box:\n%s", lines)
	}
	if _, tone := For(kube.Resource{GVR: kube.Deployments}).Row(e, dep); tone != ToneBad {
		t.Errorf("row tone %v", tone)
	}
	box := strings.Join(workloadDetail(e, dep), "\n")
	for _, w := range []string{"Problem  rollout stuck (can't make its new pods): " + r138Webhook, "Fix      " + cantMakeFix} {
		if !strings.Contains(box, w) {
			t.Errorf("deployments box lacks %q:\n%s", w, box)
		}
	}
}

// r138StuckSet: stateful set web (1 copy, pods one at a time) gone back
// from nginx:bad to nginx:1 (u), its pod web-x still on the bad version,
// failing; with stuck false, its pod is of the version it updates to.
func r138StuckSet(stuck bool) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	e, pods, w := r136Revisions("StatefulSet", false, "nginx:1", "nginx:bad")
	update := "web-h1"
	if stuck {
		update = "web-h0"
		for _, cr := range e.List(kube.ControllerRevisions, "mxa") {
			if cr.GetName() == "web-h0" {
				cr.Object["revision"] = int64(3) // brought back: its number moves up
			}
		}
	}
	w.Object["status"] = map[string]any{"replicas": int64(1), "currentRevision": "web-h1", "updateRevision": update}
	return e, pods, w
}

// QA round 137: u on a stateful set whose only pod can't pull its image
// put its spec back, but the pod stayed on the bad image: Kubernetes
// doesn't replace a pod of an older version that isn't ready (pods one
// at a time). Home's Fix said to fix the image, the image already good.
// It says the stateful set waits for that pod and to delete it; its box
// too. A pod of the version it updates to keeps the usual Fix.
func TestStatefulSetWaitsOnStuckPod(t *testing.T) {
	e, pods, w := r138StuckSet(true)
	a := r136App(t, e, pods, w)
	fix := appFix(a, false)
	if !strings.Contains(fix, "ctrl-d on web-x") || strings.Contains(fix, "fix the image") {
		t.Errorf("home: %+v, fix %q", a.Status, fix)
	}
	box := strings.Join(workloadDetail(e, w), "\n")
	if !strings.Contains(box, "ctrl-d on web-x") {
		t.Errorf("box:\n%s", box)
	}
	e, pods, w = r138StuckSet(false)
	if fix := appFix(r136App(t, e, pods, w), false); strings.Contains(fix, "ctrl-d") {
		t.Errorf("its pod is of the version it updates to: fix %q", fix)
	}
}

// withEvents is e's objects (of these kinds, in mxa) with these events too.
func withEvents(e *enrich.Enricher, kinds []kube.Resource, evs ...*unstructured.Unstructured) *enrich.Enricher {
	s := listStore{kube.Events: evs}
	for _, k := range kinds {
		s[k.GVR] = e.List(k.GVR, "mxa")
	}
	return enrich.New(mxStore{s}, kube.NewResources(append(kinds, kube.Resource{GVR: kube.Events, Kind: "Event", Namespaced: true})), nil)
}

// QA round 137: u offered to go back, with no warning, to a version that
// had failed too: a daemon set's earlier image whose pull failed (its pod
// long replaced, the event kept), a deployment's earlier ReplicaSet that
// never made a pod. Kept events say so: u is not offered as the fix, and
// says why, as a bound ("when it last ran"), not "fails now".
func TestUndoToVersionThatFailedBefore(t *testing.T) {
	e, _, ds := r136Revisions("DaemonSet", false, "nginx:bad-one", "nginx:bad-two")
	pull := mxEvent("web-old.1", "web-old", "app", "Failed", `Failed to pull image "nginx:bad-one": rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/library/nginx:bad-one": not found`, 20*time.Minute)
	e = withEvents(e, []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true},
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}, pull)
	if u := undoState(e, ds); u.helps || u.why != "going back won't help: that version failed when it last ran: its image nginx:bad-one couldn't be downloaded (that image or tag doesn't exist)" {
		t.Errorf("daemon set: %+v", u)
	}
	// Another workload's pod pulling that image says nothing of this one.
	other := mxEvent("api-1.1", "api-1", "app", "Failed", `Failed to pull image "nginx:bad-one": not found`, 20*time.Minute)
	e, _, ds = r136Revisions("DaemonSet", false, "nginx:bad-one", "nginx:bad-two")
	e = withEvents(e, []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true},
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}, other)
	if u := undoState(e, ds); !u.helps {
		t.Errorf("another's pod: %+v", u)
	}

	e, _, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(2), "updatedReplicas": int64(2), "readyReplicas": int64(2), "availableReplicas": int64(2)}, 2, 0)
	made := mxEvent("web-1.1", "web-1", "", "FailedCreate", `(combined from similar events): Error creating: pods "web-1-x" is forbidden: exceeded quota: two-pods`, 30*time.Minute)
	made.Object["involvedObject"] = map[string]any{"kind": "ReplicaSet", "name": "web-1"}
	e = withEvents(e, []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}, made)
	if u := undoState(e, dep); !u.helps || u.maybe != `going back may not help: that version failed when it last ran: Kubernetes couldn't make its pods: Error creating: pods "web-1-x" is forbidden: exceeded quota: two-pods` {
		t.Errorf("deployment: %+v", u)
	}
}

// Review of round 138: home stopped offering u from evidence that often
// passes (a FailedCreate during a surge, a pull that timed out). Only a
// pull refused because the image or tag doesn't exist, or access is
// denied, is firm ("won't help", no u); the rest keep u, with "may not
// help" in brackets on the Fix line.
func TestUndoFailedBeforeFirmOnlyForMissingImage(t *testing.T) {
	kinds := []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true},
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}
	for _, c := range []struct {
		name, msg string
		firm      bool
	}{
		{"timed out", `Failed to pull image "nginx:bad-one": rpc error: code = Unknown desc = dial tcp 1.2.3.4:443: i/o timeout`, false},
		{"not found", `Failed to pull image "nginx:bad-one": manifest unknown`, true},
		{"denied", `Failed to pull image "nginx:bad-one": unauthorized: authentication required`, true},
	} {
		e, _, ds := r136Revisions("DaemonSet", false, "nginx:bad-one", "nginx:bad-two")
		e = withEvents(e, kinds, mxEvent("web-old.1", "web-old", "app", "Failed", c.msg, 20*time.Minute))
		u := undoState(e, ds)
		if u.helps == c.firm || c.firm && !strings.HasPrefix(u.why, "going back won't help: that version failed when it last ran: ") ||
			!c.firm && !strings.HasPrefix(u.maybe, "going back may not help: that version failed when it last ran: ") {
			t.Errorf("%s: %+v", c.name, u)
		}
	}
	// Home: its new pods can't pull; going back is offered, with the note.
	e, _, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(1), "updatedReplicas": int64(1), "readyReplicas": int64(0)}, 0, 0)
	p := mxPod("web-2-a", "ReplicaSet", corev1.PodPending, mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "nginx:2"`)})
	p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web-2", "uid-web-2"
	pods := []*unstructured.Unstructured{mxObj(p)}
	made := mxEvent("web-1.1", "web-1", "", "FailedCreate", `Error creating: pods "web-1-x" is forbidden: exceeded quota: two-pods`, 30*time.Minute)
	made.Object["involvedObject"] = map[string]any{"kind": "ReplicaSet", "name": "web-1"}
	s := listStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: e.List(kube.ReplicaSets, "mxa"), kube.Events: {made}}
	e = enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	a := r136App(t, e, pods, dep)
	fix := appFix(a, false)
	if !a.CanUndo || !strings.Contains(fix, "u to go back") || !strings.Contains(fix, "(going back may not help: that version failed when it last ran: Kubernetes couldn't make its pods: ") {
		t.Errorf("home: %+v, can undo %v, fix %q", a.Status, a.CanUndo, fix)
	}
	box := strings.Join(workloadDetail(e, dep), "\n")
	if !strings.Contains(box, "u to go back") || !strings.Contains(box, "(going back may not help: that version failed when it last ran: ") {
		t.Errorf("box:\n%s", box)
	}
}

// Review of round 138: a stateful set or daemon set has no condition that
// says it still can't make its pods, and having fewer than it asks for is
// normal while it scales up (one at a time) or a node joins. A quota
// warning from 40 minutes ago, since put right (a pod made after it, or
// Kubernetes saying it made one), is no claim; one newer than both is.
func TestCantMakeWarningMustBeNewest(t *testing.T) {
	for _, kind := range []string{"StatefulSet", "DaemonSet"} {
		t.Run(kind, func(t *testing.T) {
			setup := func(warned, created, podMade time.Duration) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
				e, pods, w := r138Workload(kind, 3, 2, true)
				ev := e.List(kube.Events, "mxa")[0]
				ev.Object["lastTimestamp"] = time.Now().Add(-warned).UTC().Format(time.RFC3339)
				made := mxEvent("web.2", "web", "", "SuccessfulCreate", "create Pod web-b in "+kind+" web successful", created)
				made.Object["type"] = "Normal"
				made.Object["involvedObject"] = map[string]any{"kind": kind, "name": "web"}
				for _, p := range pods {
					p.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-podMade).Truncate(time.Second)))
				}
				gvr := map[string]schema.GroupVersionResource{"StatefulSet": kube.StatefulSets, "DaemonSet": kube.DaemonSets}[kind]
				s := listStore{kube.Pods: pods, gvr: {w}, kube.Events: {ev, made}}
				return enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
					{GVR: gvr, Kind: kind, Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil), pods, w
			}
			for _, c := range []struct {
				name                    string
				warned, created, podAge time.Duration
				claim                   bool
			}{
				{"made one since", 40 * time.Minute, 5 * time.Minute, time.Hour, false},
				{"a pod since", 40 * time.Minute, time.Hour, 5 * time.Minute, false},
				{"newest", time.Minute, 5 * time.Minute, time.Hour, true},
			} {
				e, pods, w := setup(c.warned, c.created, c.podAge)
				a := r136App(t, e, pods, w)
				if got := a.Status.Short == "Missing pods"; got != c.claim {
					t.Errorf("%s: status %+v", c.name, a.Status)
				}
				if box := strings.Join(workloadDetail(e, w), "\n"); strings.Contains(box, "Problem") != c.claim {
					t.Errorf("%s: box %s", c.name, box)
				}
			}
		})
	}
}
