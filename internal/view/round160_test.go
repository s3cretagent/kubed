package view

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// r160BothFailing: stateful set web (2 copies, pods one at a time) gone
// back (u) from a crashing version (web-h1) to the one before it, brought
// back as web-h2; both its pods are still on web-h1 and crashing, web-1
// more than web-0.
func r160BothFailing() (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	e, _, w := r136Revisions("StatefulSet", false, "nginx:1", "nginx:crash", "nginx:1")
	w.Object["spec"].(map[string]any)["replicas"] = int64(2)
	w.Object["status"] = map[string]any{"replicas": int64(2), "currentRevision": "web-h1", "updateRevision": "web-h2"}
	var pods []*unstructured.Unstructured
	for i, restarts := range []int32{3, 8} {
		p := mxPod("web-"+string(rune('0'+i)), "StatefulSet", corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off"), restarts: restarts, last: mxEnd("Error", 1, time.Minute)})
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web", "w-web"
		p.Labels["controller-revision-hash"] = "web-h1"
		pods = append(pods, mxObj(p))
	}
	s := listStore{kube.Pods: pods, kube.StatefulSets: {w}, kube.ControllerRevisions: e.List(kube.ControllerRevisions, "mxa")}
	e = enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true},
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}), nil)
	return e, pods, w
}

// QA round 160: after u on a stateful set whose two pods both reached the
// crashing version, u's confirm said to delete web-0, but home and its
// box gave the crash Fix: their headline named web-1, which crashed more.
// Kubernetes waits for web-0 (the lowest one not ready) first: all three
// say to delete it.
func TestStatefulSetWaitsOnLowestFailingPod(t *testing.T) {
	e, pods, w := r160BothFailing()
	if held := HeldAfterGoingBack(e, w); held != "web-0" {
		t.Fatalf("held %q", held)
	}
	a := r136App(t, e, pods, w)
	fix := appFix(a, false)
	if !strings.Contains(fix, "waits for its pod web-0") || !strings.Contains(fix, "ctrl-d on web-0") || strings.Contains(fix, "logs") {
		t.Errorf("home: head %s %+v, fix %q", a.HeadPod, a.Status, fix)
	}
	box := strings.Join(workloadDetail(e, w), "\n")
	if !strings.Contains(box, "waits for its pod web-0") || !strings.Contains(box, "ctrl-d on web-0") || strings.Contains(box, "Fix      p shows") {
		t.Errorf("box:\n%s", box)
	}
}

// QA round 160: a postStart hook crash loop read "keeps crashing; 8
// restarts; last exit code 137" after about 10 minutes: the kubelet stops
// repeating a frequent event, so none was kept for the last run. A run
// ended alike (killed, exit 137) of a container that has that hook, with a
// hook kill earlier in the hour and nothing else killing it since, keeps
// the hook's words; any other case doesn't claim one.
func TestHookKillKeptWhenEventsStopRepeating(t *testing.T) {
	hook := `Exec lifecycle hook ([sh -c exit 3]) for Container "app" in Pod "h_mxa(uid-h)" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`
	pod := func(hasHook bool, last *corev1.ContainerStateTerminated) *corev1.Pod {
		p := mxPod("h", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 5m0s restarting failed container=app"), restarts: 8, last: last})
		if hasHook {
			p.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "exit 3"}}}}
		}
		return p
	}
	killed := mxEnd("Error", 137, time.Minute) // ran from 2m ago
	earlier := mxEvent("h.1", "h", "app", "FailedPostStartHook", hook, 8*time.Minute)
	for _, c := range []struct {
		name string
		p    *corev1.Pod
		evs  []*unstructured.Unstructured
		hook bool
	}{
		{"earlier run's event", pod(true, killed), []*unstructured.Unstructured{earlier}, true},
		{"no hook in its spec", pod(false, killed), []*unstructured.Unstructured{earlier}, false},
		{"another exit", pod(true, mxEnd("Error", 1, time.Minute)), []*unstructured.Unstructured{earlier}, false},
		{"out of memory", pod(true, mxEnd("OOMKilled", 137, time.Minute)), []*unstructured.Unstructured{earlier}, false},
		{"event over an hour before", pod(true, killed), []*unstructured.Unstructured{mxEvent("h.1", "h", "app", "FailedPostStartHook", hook, 2*time.Hour)}, false},
		{"killed by its liveness check since", pod(true, killed), []*unstructured.Unstructured{earlier,
			mxEvent("h.2", "h", "app", "Killing", "Container app failed liveness probe, will be restarted", 4*time.Minute)}, false},
		{"no event at all", pod(true, killed), nil, false},
	} {
		e, pods := r140Env(c.p, c.evs...)
		st := Explain(e, c.p)
		if got := strings.Contains(st.Why, `container "app" is killed because its postStart hook failed (command 'sh -c exit 3' exited with 3); 8 restarts`); got != c.hook {
			t.Errorf("%s: %+v", c.name, st)
		}
		if a := r136App(t, e, pods); c.hook && !strings.HasPrefix(appFix(a, false), "fix its postStart hook") {
			t.Errorf("%s: home Fix %q", c.name, appFix(a, false))
		}
	}
}

// r160Ordinals: stateful set web gone back as r160BothFailing, with pods
// web-0 to web-10; web-0 is as given (nil: running and ready), web-1
// running and ready, web-2 to web-10 still on web-h1 and crashing.
func r160Ordinals(first *corev1.Pod) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	e, _, w := r160BothFailing()
	w.Object["spec"].(map[string]any)["replicas"] = int64(11)
	w.Object["status"] = map[string]any{"replicas": int64(11), "currentRevision": "web-h1", "updateRevision": "web-h2"}
	var pods []*unstructured.Unstructured
	for i := range 11 {
		name := fmt.Sprintf("web-%d", i)
		p := mxPod(name, "StatefulSet", corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off"), restarts: int32(3 + i), last: mxEnd("Error", 1, time.Minute)})
		hash := "web-h1"
		switch {
		case i == 0 && first != nil:
			p = first
			hash = "web-h2"
		case i < 2:
			p = mxPod(name, "StatefulSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
			hash = "web-h2"
		}
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web", "w-web"
		p.Labels["controller-revision-hash"] = hash
		pods = append(pods, mxObj(p))
	}
	slices.SortFunc(pods, func(a, b *unstructured.Unstructured) int { return strings.Compare(a.GetName(), b.GetName()) }) // by name, as the cache lists them
	s := listStore{kube.Pods: pods, kube.StatefulSets: {w}, kube.ControllerRevisions: e.List(kube.ControllerRevisions, "mxa")}
	e = enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true},
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}), nil)
	return e, pods, w
}

// Review of round 160: the pod waited for was the first by name, web-10
// before web-2; Kubernetes waits for the lowest number not ready. And when
// that one isn't a failing pod of another version (one still starting), it
// waits for it: no pod to delete is named.
func TestStatefulSetWaitsByOrdinal(t *testing.T) {
	e, pods, w := r160Ordinals(nil)
	if held := HeldAfterGoingBack(e, w); held != "web-2" {
		t.Errorf("held %q", held)
	}
	if fix := appFix(r136App(t, e, pods, w), false); !strings.Contains(fix, "waits for its pod web-2") || !strings.Contains(fix, "ctrl-d on web-2") {
		t.Errorf("home fix %q", fix)
	}
	if box := strings.Join(workloadDetail(e, w), "\n"); !strings.Contains(box, "waits for its pod web-2") || !strings.Contains(box, "ctrl-d on web-2)") {
		t.Errorf("box:\n%s", box)
	}

	starting := mxPod("web-0", "StatefulSet", corev1.PodPending, mxC{name: "app", state: mxWait("ContainerCreating", "")})
	starting.CreationTimestamp = mxAgo(10 * time.Second)
	e, pods, w = r160Ordinals(starting)
	if held := HeldAfterGoingBack(e, w); held != "" {
		t.Errorf("starting first: held %q", held)
	}
	if fix := appFix(r136App(t, e, pods, w), false); strings.Contains(fix, "ctrl-d") {
		t.Errorf("starting first: home fix %q", fix)
	}
	if box := strings.Join(workloadDetail(e, w), "\n"); strings.Contains(box, "ctrl-d") {
		t.Errorf("starting first: box:\n%s", box)
	}
}
