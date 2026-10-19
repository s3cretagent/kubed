package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 128: a deployment whose image can't be pulled is fixed with i
// (busybox:1.37, no command, so its pod keeps exiting 0). Mid-rollout the
// old version's pod still can't pull and the new one keeps stopping. The
// headline is the new version's problem, it says the old pod still can't
// pull, and no screen offers u: going back means the image that can't be
// pulled. With the new pod still starting, the old problem may lead, but
// still without u.
func TestRolloutFixBlamesTheNewVersion(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "badimg", "namespace": "mxa", "uid": "dep-badimg", "labels": map[string]any{"app": "mx"},
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
		"spec":   map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}}},
		"status": map[string]any{"replicas": int64(2), "updatedReplicas": int64(1), "readyReplicas": int64(0)}}}
	rs := func(name, rev, image string, made time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "mxa", "uid": "uid-" + name, "creationTimestamp": time.Now().Add(-made).UTC().Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "badimg", "uid": "dep-badimg", "controller": true}}},
			"spec":   map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}},
			"status": map[string]any{"replicas": int64(1)}}}
	}
	owned := func(p *corev1.Pod, rsName string) *unstructured.Unstructured {
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID, p.OwnerReferences[0].Kind = rsName, types.UID("uid-"+rsName), "ReplicaSet"
		return mxObj(p)
	}
	oldPod := owned(mxPod("badimg-old-a", "ReplicaSet", corev1.PodPending,
		mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "busybox:no-such-tag"`)}), "badimg-old")
	stopping := owned(mxPod("badimg-new-a", "ReplicaSet", corev1.PodRunning,
		mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 20s restarting failed container"), restarts: 2, last: mxEnd("Completed", 0, 10*time.Second)}), "badimg-new")
	starting := owned(mxPod("badimg-new-a", "ReplicaSet", corev1.PodPending,
		mxC{name: "app", state: mxWait("ContainerCreating", "")}), "badimg-new")
	for _, c := range []struct {
		name     string
		newPod   *unstructured.Unstructured
		headline string
		whyMust  []string
		boxMust  []string
		boxNever []string
	}{
		{"new version keeps stopping", stopping, "Restarting", []string{"keeps stopping"},
			[]string{"pod badimg-new-a: Restarting", "going back won't help: the previous version fails too"}, []string{"pod badimg-old-a: Can't pull"}},
		{"new version still starting", starting, "", nil, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			pods := []*unstructured.Unstructured{oldPod, c.newPod}
			s := listStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs("badimg-new", "2", "busybox:1.37", 40*time.Second), rs("badimg-old", "1", "busybox:no-such-tag", 10*time.Minute)}}
			e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
				{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}), nil)
			var a *App
			for _, x := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
				if x.Name == "mx" {
					a = x
				}
			}
			if a == nil {
				t.Fatal("no app mx")
			}
			if c.headline != "" && a.Status.Short != c.headline {
				t.Errorf("headline %q (%s), want %q", a.Status.Short, a.Status.Why, c.headline)
			}
			mxCheck(t, "why", a.Status.Why, c.whyMust, nil)
			for _, ro := range []bool{false, true} {
				if fix := appFix(a, ro); strings.Contains(fix, "go back") {
					t.Errorf("fix (read-only %v) offers going back to the image that can't be pulled: %q", ro, fix)
				} else if c.headline != "" && !strings.Contains(fix, "going back won't help: the previous version fails too") { // QA round 129: and says why not
					t.Errorf("fix (read-only %v) doesn't say why it doesn't offer going back: %q", ro, fix)
				}
			}
			box := strings.Join(workloadProblem(e, dep), "\n")
			mxCheck(t, "box", box, c.boxMust, append([]string{"go back"}, c.boxNever...))
		})
	}
}

// QA round 128: i works straight from an app (it asks which workload when
// there are several), so its Fix says i, not "enter → Its workloads".
func TestPullFixSaysIOnTheApp(t *testing.T) {
	for _, undo := range []bool{false, true} {
		a := &App{Status: Status{Short: "Can't pull image"}, Pods: 1, CanUndo: undo}
		fix := appFix(a, false)
		if strings.Contains(fix, "Its workloads") || !strings.Contains(fix, "i to fix the image") {
			t.Errorf("undo %v: %q", undo, fix)
		}
	}
}

func rfDep(name string, rev string, paused bool, stuck bool) *unstructured.Unstructured {
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": name, "namespace": "mxa", "uid": "dep-" + name, "labels": map[string]any{"app": "mx"},
			"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}},
		"spec":   map[string]any{"replicas": int64(1), "paused": paused, "selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}}},
		"status": map[string]any{"replicas": int64(2), "updatedReplicas": int64(1), "readyReplicas": int64(1)}}}
	if stuck {
		d.Object["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded", "message": "ReplicaSet has timed out progressing."}}
	}
	return d
}

func rfRS(dep, name, rev string, made time.Duration, ready int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": name, "namespace": "mxa", "uid": "uid-" + name, "creationTimestamp": time.Now().Add(-made).UTC().Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": dep, "uid": "dep-" + dep, "controller": true}}},
		"spec":   map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "img:" + rev}}}}},
		"status": map[string]any{"replicas": int64(1), "readyReplicas": ready}}}
}

func rfOwned(p *corev1.Pod, rsName string) *unstructured.Unstructured {
	p.OwnerReferences[0].Name, p.OwnerReferences[0].UID, p.OwnerReferences[0].Kind = rsName, types.UID("uid-"+rsName), "ReplicaSet"
	return mxObj(p)
}

func rfReady(name string) *corev1.Pod {
	p := mxPod(name, "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}, {Type: corev1.ContainersReady, Status: corev1.ConditionTrue}}
	return p
}

func rfCrash(name string, restarts int32) *corev1.Pod {
	return mxPod(name, "ReplicaSet", corev1.PodRunning,
		mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 5m restarting failed container"), restarts: restarts, last: mxEnd("Error", 1, 30*time.Second)})
}

func rfRun(t *testing.T, pods, deps, rss []*unstructured.Unstructured) (*App, *enrich.Enricher) {
	s := listStore{kube.Pods: pods, kube.Deployments: deps, kube.ReplicaSets: rss}
	e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}), nil)
	for _, x := range Apps(e, pods, nil, nil, nil, deps) {
		if x.Name == "mx" {
			return x, e
		}
	}
	t.Fatal("no app")
	return nil, nil
}

// Review of round 128: the new version's problem leads with no note about
// the old pod (the note hid the headline's pod: p and the Error line went
// to the old one). HeadPod names the headline's pod, p reads it, and the
// old pod's crash isn't quoted as its Error.
func TestOldVersionHeadlineNamesItsPod(t *testing.T) {
	stop := mxPod("web-new-a", "ReplicaSet", corev1.PodRunning,
		mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 20s restarting failed container"), restarts: 2, last: mxEnd("Completed", 0, 10*time.Second)})
	pods := []*unstructured.Unstructured{rfOwned(rfCrash("web-old-a", 5), "web-old"), rfOwned(stop, "web-new")}
	a, _ := rfRun(t, pods, []*unstructured.Unstructured{rfDep("web", "2", false, false)}, []*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Minute, 0), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
	look := Lookups{Crash: func(x *App) (string, bool) { return "LOG OF " + x.CrashPod, true }}
	if a.Status.Short != "Restarting" || a.HeadPod != "web-new-a" || !a.PrevLogsOfHead() || strings.Contains(a.Status.Why, "old version") ||
		strings.Contains(strings.Join(AppLines(a, look), "\n"), "Error    LOG OF web-old-a") { // the old crash only as "Also pod web-old-a crashed:"
		t.Errorf("status %q head %q crash %q why %q", a.Status.Short, a.HeadPod, a.CrashPod, a.Status.Why)
	}
	// Two old pods crashing, the new one fine: the one that crashed most
	// leads, named, with no note.
	pods = []*unstructured.Unstructured{rfOwned(rfReady("web-new-a"), "web-new"), rfOwned(rfCrash("web-old-a", 5), "web-old"), rfOwned(rfCrash("web-old-b", 20), "web-old")}
	a, _ = rfRun(t, pods, []*unstructured.Unstructured{rfDep("web", "2", false, false)}, []*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Minute, 1), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
	if a.HeadPod != "web-old-b" || strings.Contains(a.Status.Why, "old version") {
		t.Errorf("two old: head %q why %q", a.HeadPod, a.Status.Why)
	}
}

// Review of round 128: a pod being deleted keeps its own status and Fix
// (Stuck stopping), and a paused or Recreate deployment's older pods
// aren't "replaced as the new version's pods become ready".
func TestOldVersionRankExceptions(t *testing.T) {
	const rollFix = "that pod is of the old version"
	old := rfReady("web-old-a")
	ts := metav1.NewTime(time.Now().Add(-20 * time.Minute))
	old.DeletionTimestamp = &ts
	dep := rfDep("web", "2", false, false)
	a, e := rfRun(t, []*unstructured.Unstructured{rfOwned(rfReady("web-new-a"), "web-new"), rfOwned(old, "web-old")}, []*unstructured.Unstructured{dep},
		[]*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Hour, 1), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
	if fix := appFix(a, false); strings.Contains(fix, rollFix) {
		t.Errorf("stuck stopping: %q", fix)
	}
	if box := strings.Join(workloadProblem(e, dep), "\n"); strings.Contains(box, rollFix) {
		t.Errorf("stuck stopping box: %q", box)
	}
	recreate := rfDep("web", "2", false, false)
	recreate.Object["spec"].(map[string]any)["strategy"] = map[string]any{"type": "Recreate"}
	for name, d := range map[string]*unstructured.Unstructured{"paused": rfDep("web", "2", true, false), "recreate": recreate} {
		pods := []*unstructured.Unstructured{rfOwned(rfReady("web-new-a"), "web-new"), rfOwned(rfCrash("web-old-a", 5), "web-old")}
		a, e := rfRun(t, pods, []*unstructured.Unstructured{d}, []*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Hour, 1), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
		if fix := appFix(a, false); strings.Contains(fix, rollFix) {
			t.Errorf("%s: %q", name, fix)
		}
		if box := strings.Join(workloadProblem(e, d), "\n"); strings.Contains(box, rollFix) {
			t.Errorf("%s box: %q", name, box)
		}
	}
}

// Review of round 128: going back skips versions that are only restarts
// of now (as u does), so a restart of the broken version doesn't hide the
// good one before it.
func TestUndoSkipsRestartsOfNow(t *testing.T) {
	dep := rfDep("web", "4", false, false)
	mk := func(name, rev, image string, made time.Duration, restarted string) *unstructured.Unstructured {
		r := rfRS("web", name, rev, made, 0)
		tmpl := r.Object["spec"].(map[string]any)["template"].(map[string]any)
		tmpl["spec"] = map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}
		if restarted != "" {
			tmpl["metadata"] = map[string]any{"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": restarted}}
		}
		return r
	}
	rss := []*unstructured.Unstructured{mk("web-r4", "4", "img:bad", time.Minute, "2026-10-16T00:00:00Z"), mk("web-r3", "3", "img:bad", 10*time.Minute, ""), mk("web-r2", "2", "img:good", 3*time.Hour, "")}
	pull := func(n string) *corev1.Pod {
		return mxPod(n, "ReplicaSet", corev1.PodPending, mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "img:bad"`)})
	}
	a, e := rfRun(t, []*unstructured.Unstructured{rfOwned(pull("web-r4-a"), "web-r4"), rfOwned(pull("web-r3-a"), "web-r3")}, []*unstructured.Unstructured{dep}, rss)
	if fix := appFix(a, false); !a.CanUndo || !strings.Contains(fix, "u to go back") {
		t.Errorf("canUndo %v fix %q", a.CanUndo, fix)
	}
	if box := strings.Join(workloadProblem(e, dep), "\n"); !strings.Contains(box, "u to go back") {
		t.Errorf("box %q", box)
	}
}

// Review of round 128: why going back isn't offered is the headline's own
// deployment's reason, not another workload's of the app.
func TestNoUndoWhyIsTheHeadlinesDeployment(t *testing.T) {
	pull := mxPod("a-new-a", "ReplicaSet", corev1.PodPending, mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "img:nope"`)})
	pods := []*unstructured.Unstructured{rfOwned(pull, "a-new"), rfOwned(rfCrash("b-new-a", 3), "b-new"), rfOwned(rfCrash("b-old-a", 3), "b-old")}
	depA, depB := rfDep("a", "1", false, false), rfDep("b", "2", false, false)
	a, e := rfRun(t, pods, []*unstructured.Unstructured{depA, depB},
		[]*unstructured.Unstructured{rfRS("a", "a-new", "1", time.Hour, 0), rfRS("b", "b-new", "2", time.Hour, 0), rfRS("b", "b-old", "1", 3*time.Hour, 0)})
	if fix := appFix(a, false); a.HeadPod != "a-new-a" || !strings.Contains(fix, "no earlier version") || strings.Contains(fix, "fails too") {
		t.Errorf("head %q fix %q", a.HeadPod, fix)
	}
	// QA round 129: each deployment's box says why it doesn't offer u, as home does.
	if box := strings.Join(workloadProblem(e, depA), "\n"); !strings.Contains(box, "no earlier version") || strings.Contains(box, "fails too") {
		t.Errorf("box a %q", box)
	}
	if box := strings.Join(workloadProblem(e, depB), "\n"); !strings.Contains(box, "fails too") || strings.Contains(box, "no earlier version") {
		t.Errorf("box b %q", box)
	}
}

// Review of round 128: a stuck rollout whose previous version fails too
// doesn't say there is no earlier version.
func TestStuckFixSaysPreviousFails(t *testing.T) {
	pods := []*unstructured.Unstructured{rfOwned(rfReady("web-new-a"), "web-new"), rfOwned(rfCrash("web-old-a", 5), "web-old")}
	a, _ := rfRun(t, pods, []*unstructured.Unstructured{rfDep("web", "2", false, true)}, []*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Hour, 1), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
	for _, ro := range []bool{false, true} {
		if fix := appFix(a, ro); a.Status.Short != "Rollout stuck" || strings.Contains(fix, "no earlier version") || !strings.Contains(fix, "fails too") {
			t.Errorf("read-only %v: status %q fix %q", ro, a.Status.Short, fix)
		}
	}
}
