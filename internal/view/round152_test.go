package view

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// r152Mixed is app mx: deployment web's healthy pod and bare pod badimg
// (nothing makes it) with c, in the state given.
func r152Mixed(t *testing.T, c mxC) *App {
	t.Helper()
	e, pods, w := r138Workload("Deployment", 1, 1, false)
	bare := mxPod("badimg", "", corev1.PodPending, c)
	if c.state.Running != nil || c.state.Terminated != nil {
		bare.Status.Phase = corev1.PodRunning
	}
	return r136App(t, e, append(pods, mxObj(bare)), w)
}

// QA round 152: a namespace's app ran deployments and a bare pod badimg
// whose image can't be pulled. Home's Fix said "i to fix the image name
// or tag (there is no earlier version to go back to)", but i offers only
// the deployments, none of which runs that image. When the pod the
// headline is about has nothing that makes it, the Fix says it is fixed
// where that pod is defined, as its pod box does.
func TestBareHeadPodFixSaysWhereItIsDefined(t *testing.T) {
	const where = "where pod badimg is defined (nothing makes it again if it goes)"
	pull := r152Mixed(t, mxC{name: "c", state: mxWait("ImagePullBackOff", `Back-off pulling image "busybox:no-such-tag"`)})
	if !strings.HasPrefix(pull.Status.Short, "Can't pull image") || pull.HeadPod != "badimg" {
		t.Fatalf("status %+v, head %q", pull.Status, pull.HeadPod)
	}
	for _, ro := range []bool{false, true} {
		fix := appFix(pull, ro)
		if !strings.Contains(fix, "fix the image name or tag "+where) || strings.Contains(fix, "i to") || strings.Contains(fix, noEarlierVersion) {
			t.Errorf("pull, readonly %v: %q", ro, fix)
		}
	}
	// A command it can't start, and its postStart hook: fixed where it is defined too.
	start := r152Mixed(t, mxC{name: "c", state: mxWait("RunContainerError", `exec: "nope": executable file not found in $PATH`)})
	if fix := appFix(start, false); start.Status.Short != "Can't start" || !strings.Contains(fix, "Fix the command "+where) {
		t.Errorf("can't start %+v: %q", start.Status, fix)
	}
	hook := &App{Name: "mx", Status: Status{"Crashing", "container c " + hookFailed, ToneBad}, Pods: 2, barePods: 1,
		kinds: map[string]bool{"Deployment": true}, HeadPod: "badimg", headPod: mxPod("badimg", "", corev1.PodRunning)}
	if fix := appFix(hook, false); fix != "fix its postStart hook "+where {
		t.Errorf("hook: %q", fix)
	}
	// The deployment's own pod failing: its workload, as before.
	dep := &App{Name: "mx", Status: Status{"Can't pull image", "img/app:2 not found", ToneBad}, Pods: 2, barePods: 1,
		kinds: map[string]bool{"Deployment": true}, HeadPod: "web-a", headPod: mxPod("web-a", "ReplicaSet", corev1.PodPending)}
	if fix := appFix(dep, false); !strings.HasPrefix(fix, "i to fix the image name or tag") {
		t.Errorf("deployment's pod: %q", fix)
	}
	if fix := appFix(&App{Name: "mx", Status: hook.Status, Pods: 2, barePods: 1, kinds: hook.kinds, HeadPod: "web-a", headPod: dep.headPod}, false); fix != "fix its postStart hook (enter → Its workloads, then e)" {
		t.Errorf("deployment's hook: %q", fix)
	}
}

// QA round 152: a paused deployment that went back at once (nothing
// rolling out) showed only its images: nothing said it was still
// paused, so a later i would not roll out. Its box and its app's box say
// so; paused partway, the app's headline already does.
func TestPausedDeploymentSaysSoWhenIdle(t *testing.T) {
	const line = "deployment pz: a change to it rolls out only once it is resumed (kubectl rollout resume deployment/pz -n qa)"
	box := func(updated int64) (string, string) {
		e, dep, pods := r132Paused(updated)
		for _, a := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
			if a.Name == "pz" {
				return strings.Join(AppLines(a, Lookups{}), "\n"), strings.Join(workloadDetail(e, dep), "\n")
			}
		}
		t.Fatal("no app pz")
		return "", ""
	}
	app, dep := box(2)
	if !strings.Contains(app, "Paused   "+line) {
		t.Errorf("app box:\n%s", app)
	}
	if !strings.Contains(dep, "Paused   a change to it rolls out only once it is resumed (kubectl rollout resume deployment/pz -n qa)") {
		t.Errorf("deployment box:\n%s", dep)
	}
	app, _ = box(1)
	if strings.Count(app, "Paused") != 1 || !strings.Contains(app, "Problem  Paused: rolling out") {
		t.Errorf("paused partway, app box:\n%s", app)
	}
	// Not paused: nothing.
	e, dep2, pods := r132Paused(2)
	unstructured.RemoveNestedField(dep2.Object, "spec", "paused")
	for _, a := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep2}) {
		if a.Name == "pz" && strings.Contains(strings.Join(AppLines(a, Lookups{}), "\n")+strings.Join(workloadDetail(e, dep2), "\n"), "Paused") {
			t.Errorf("not paused: %v", AppLines(a, Lookups{}))
		}
	}
}
