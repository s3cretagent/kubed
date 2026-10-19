package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// QA round 158: the pods screen's box for bare pod nocmd (nothing makes
// it, no ArgoCD), whose command isn't in its image, said "Fix the command
// in Git". It says where the pod is defined, as the rest of that screen
// and home do for a bare pod; a deployment's pod says its deployment; a
// pod ArgoCD manages still says Git. A cron job whose last run never
// started says the cron job (e), not Git.
func TestNeverStartedFixSaysWhere(t *testing.T) {
	e, pods, w := r138Workload("Deployment", 1, 1, false)
	cantStart := mxC{name: "c", state: mxWait("RunContainerError", `exec: "/no/such/cmd": stat /no/such/cmd: no such file or directory`)}
	fixOf := func(p *corev1.Pod) string {
		t.Helper()
		for _, l := range podDetail(e, mxObj(p)) {
			if strings.HasPrefix(l, "Fix      ") {
				return l
			}
		}
		t.Fatalf("no Fix line for %s", p.Name)
		return ""
	}
	bare := mxPod("nocmd", "", corev1.PodPending, cantStart)
	if want := "Fix      it never started, so it has no logs: the command or a file it runs is missing from the image (or can't be run). Fix the command where this pod is defined (nothing makes it again if it goes), or use an image that has it"; fixOf(bare) != want {
		t.Errorf("bare pod:\n%s\nwant\n%s", fixOf(bare), want)
	}
	dep := mxPod("web-z", "ReplicaSet", corev1.PodPending, cantStart)
	dep.OwnerReferences[0].Name, dep.OwnerReferences[0].UID = "web-2", types.UID("uid-web-2")
	if fix := fixOf(dep); !strings.Contains(fix, "Fix the command (on its deployment), or use an image") {
		t.Errorf("deployment's pod: %s", fix)
	}
	// ArgoCD manages them: Git, as before.
	bare.Annotations = map[string]string{"argocd.argoproj.io/tracking-id": "shop:/Pod:mxa/nocmd"}
	w.SetAnnotations(map[string]string{"argocd.argoproj.io/tracking-id": "shop:apps/Deployment:mxa/web"})
	for _, p := range []*corev1.Pod{bare, dep} {
		if fix := fixOf(p); !strings.Contains(fix, "Fix the command in Git, or use an image") {
			t.Errorf("%s in ArgoCD: %s", p.Name, fix)
		}
	}
	// Its deployment's box, its pod that one: Git too.
	dep.Name = "web-a"
	*pods[0] = *mxObj(dep)
	if box := strings.Join(workloadDetail(e, w), "\n"); !strings.Contains(box, "Fix the command in Git, or use an image") {
		t.Errorf("deployment box in ArgoCD:\n%s", box)
	}

	// A cron job's last run never started: fixed on the cron job itself.
	cron := mxCron(4*time.Minute, 9*time.Minute, false)
	x := mxBuild([]mxJob{{name: "cj-2", ago: 4 * time.Minute, cron: "cj", conds: []mxCond{{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 3 * time.Minute}}}},
		[]mxJobPod{{name: "cj-2-a", job: "cj-2", ago: 4 * time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "start", ago: 3 * time.Minute}}}}, cron)
	box := strings.Join(cronDetail(x.e, cron), "\n")
	if !strings.Contains(box, "Fix the command (e), or use an image") || strings.Contains(box, "in Git") {
		t.Errorf("cron box:\n%s", box)
	}
}

// QA round 158: app api is deployment api (2 copies, paused partway with
// maxSurge 1: 3 pods, all ready) and bare pod apibare, which can't start.
// READY read 3/3: the rollout's extra ready copy stood in for apibare. A
// pod no workload asks for counts as it is: 3/4. A plain rollout still
// counts as kubectl does (3/2).
func TestReadyNotAllWhileABarePodIsnt(t *testing.T) {
	e, pods, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(3), "updatedReplicas": int64(2),
		"readyReplicas": int64(3), "availableReplicas": int64(2), "unavailableReplicas": int64(1)}, 2, 1)
	if got := readyCell(r136App(t, e, pods, dep)); got != "3/2" {
		t.Fatalf("plain rollout READY %s, want 3/2", got)
	}
	bare := mxPod("apibare", "", corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 5m0s restarting failed container"),
		last: mxEnd("Error", 1, time.Minute), restarts: 5})
	a := r136App(t, e, append(pods, mxObj(bare)), dep)
	if got := readyCell(a); got != "3/4" {
		t.Errorf("READY %s (status %+v), want 3/4", got, a.Status)
	}
	// Ready, it counts as ready, next to the rollout's 3/2 as before: 4/3.
	ok := mxPod("apibare", "", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
	ok.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if got := readyCell(r136App(t, e, append(pods, mxObj(ok)), dep)); got != "4/3" {
		t.Errorf("ready bare pod: READY %s", got)
	}
}

// QA round 158: adding bare pod apibare, which never started, to app api
// moved its Deployed to apibare's creation (24s ago), though the
// deployment's version went out minutes before. A pod that never ran
// doesn't date the app; one that runs does, and a bare pod alone still
// dates its app.
func TestDeployedSkipsBarePodThatNeverRan(t *testing.T) {
	e, pods, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(2), "updatedReplicas": int64(2),
		"readyReplicas": int64(2), "availableReplicas": int64(2)}, 2, 0)
	base := r136App(t, e, pods, dep).LastDeploy
	if base.IsZero() || time.Since(base) < 50*time.Second {
		t.Fatalf("deployment's Deployed %v", base)
	}
	never := mxPod("apibare", "", corev1.PodPending, mxC{name: "app", state: mxWait("RunContainerError", `exec: "/no/such/cmd": not found`)})
	never.CreationTimestamp = mxAgo(24 * time.Second)
	if got := r136App(t, e, append(pods, mxObj(never)), dep).LastDeploy; !got.Equal(base) {
		t.Errorf("with a bare pod that never ran: Deployed %v ago, want %v ago", time.Since(got).Round(time.Second), time.Since(base).Round(time.Second))
	}
	ran := mxPod("apibare", "", corev1.PodRunning, mxC{name: "app", state: mxRun(20 * time.Second), ready: true})
	ran.CreationTimestamp = mxAgo(24 * time.Second)
	if got := r136App(t, e, append(pods, mxObj(ran)), dep).LastDeploy; time.Since(got) > 30*time.Second {
		t.Errorf("with a bare pod that runs: Deployed %v ago, want 24s", time.Since(got).Round(time.Second))
	}
	if got := r136App(t, e, []*unstructured.Unstructured{mxObj(never)}).LastDeploy; time.Since(got) > 30*time.Second {
		t.Errorf("bare pod alone: Deployed %v", got)
	}
}
