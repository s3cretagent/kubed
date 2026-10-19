package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Round 120: when Kubernetes' events can't date a go-back (they expired,
// or were merged with others), Deployed said "Kubernetes no longer keeps
// exactly when", a claim about what it keeps that may be untrue. It says
// what it keeps can't tell, as a bound, and the list marks it too.
func TestUndatedGoBackDeployedSaysABound(t *testing.T) {
	a := &App{Name: "web", Namespace: "qa", Pods: 1, Ready: 1, LastDeploy: time.Now().Add(-2 * time.Hour), DeployAfter: true}
	var deployed string
	for _, l := range AppLines(a, Lookups{}) {
		if strings.HasPrefix(l, "Deployed") {
			deployed = l
		}
	}
	if !strings.Contains(deployed, "after ") || !strings.Contains(deployed, "can't say exactly when") || strings.Contains(deployed, "no longer keeps") {
		t.Errorf("%q", deployed)
	}
}

// Round 124: one finished pod of a job with a TTL read "1 finished pod,
// kept for their logs until Kubernetes deletes them"; one pod is "it".
func TestOneLeftoverRunIsIt(t *testing.T) {
	for _, c := range []struct {
		done int
		want string
	}{
		{1, "1 finished pod, kept for its logs until Kubernetes deletes it"},
		{2, "2 finished pods, kept for their logs until Kubernetes deletes them"},
		{-1, "1 finished pod, kept by Kubernetes (it never started, so it has no logs)"}, // round 102
	} {
		a := &App{Name: "nightly", Namespace: "qa", Leftovers: c.done, RunTTL: 3600}
		if c.done < 0 {
			a = &App{Name: "nightly", Namespace: "qa", Leftovers: 1, doneNeverRan: 1, Status: Status{"Can't start", "x", ToneBad}}
		}
		found := false
		for _, l := range AppLines(a, Lookups{}) {
			if strings.HasPrefix(l, "Leftover") {
				found = strings.Contains(l, c.want)
				if !found {
					t.Errorf("%d: %q", c.done, l)
				}
			}
		}
		if !found && !t.Failed() {
			t.Errorf("%d: no Leftover line", c.done)
		}
	}
}

// Round 107: an app made only of an overprovisioner's pause pods (negative
// priority, waiting for a node by design) read "healthy" though nothing of
// it runs; it reads Placeholder, not a problem either.
func TestPlaceholderApp(t *testing.T) {
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "overprovisioner-x", "namespace": "karpenter", "uid": "op1", "labels": map[string]any{"app": "overprovisioner"}},
		"spec":     map[string]any{"priority": int64(-10), "containers": []any{map[string]any{"name": "reserve", "image": "registry.k8s.io/pause:3.9"}}},
		"status": map[string]any{"phase": "Pending", "conditions": []any{map[string]any{"type": "PodScheduled", "status": "False",
			"reason": "Unschedulable", "message": "0/16 nodes are available: 1 Too many pods."}}}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	e := enrich.New(nsStore{kube.Pods: {pod}}, res, nil)
	for _, a := range Apps(e, []*unstructured.Unstructured{pod}, nil, nil, nil, nil) {
		if a.Name == "overprovisioner" {
			if a.Status.Short != "Placeholder" || a.Status.Tone != ToneNormal || strings.Contains(a.Status.Why, "healthy") {
				t.Errorf("%+v", a.Status)
			}
			return
		}
	}
	t.Error("no app overprovisioner")
}

// Round 104: a try that failed while another container keeps its pod up
// reads "Try failed". Its Fix still says where a job is fixed (not "Its
// workloads", which has no job), and sends you to the logs from before.
func TestTryFailedFixIsAJobs(t *testing.T) {
	a := &App{Status: Status{"Try failed", "x", ToneBad}, jobKinds: map[string]bool{"Job": true}}
	b := &App{Status: Status{"Job failed", "x", ToneBad}, jobKinds: map[string]bool{"Job": true}}
	if got, want := fixWhere(a, false), fixWhere(b, false); got != want {
		t.Errorf("Try failed: %q, Job failed: %q", got, want)
	}
	if !crashyShort("Try failed") {
		t.Error("Try failed doesn't send you to the earlier logs")
	}
}

// Round 103: a try's containers all stop seconds before its pod's phase
// turns Failed; the job's cause is quoted from them straight away.
func TestJobCauseBeforeThePhaseSaysFailed(t *testing.T) {
	st := r102failed(2*time.Second, 1, "Error")
	st["phase"] = "Running" // not yet
	e, j, _ := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)}, "status": map[string]any{"active": int64(1)}}, st)
	if cause, ok := JobCause(e, j); !ok || cause == "" {
		t.Errorf("no cause: %q %v", cause, ok)
	}
}

// Round 103: a job's pod whose only container failed, its phase not yet
// Failed, read "Running: a run in progress"; it failed.
func TestJobPodFailedBeforeItsPhase(t *testing.T) {
	ctrl := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "migrate-x", Namespace: "qa",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: "migrate", Controller: &ctrl}}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main", Image: "busybox"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "main",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", FinishedAt: metav1.Now()}}}}}}
	if st := Explain(enrich.New(benchStore{}, nil, nil), p); st.Short != "Failed" || st.Tone != ToneBad {
		t.Errorf("%+v", st)
	}
}

// Round 102: a failed Job can't be run again as it is; its Fix said "run
// the job again". It is created again.
func TestFailedJobFixSaysCreateAgain(t *testing.T) {
	a := &App{Status: Status{"Job failed", "x", ToneBad}, jobKinds: map[string]bool{"Job": true}}
	if got := appFix(a, false); !strings.Contains(got, "create the job again") || strings.Contains(got, "run the job again") {
		t.Errorf("%q", got)
	}
}

// Round 101: a job's own failed count can lag its pods; the job box counted
// tries failed from the job ("0 tries failed so far") while one had.
func TestRetryingJobBoxCountsFromItsPods(t *testing.T) {
	e, j, _ := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3)}, "status": map[string]any{"active": int64(1)}}, r102failed(2*time.Second, 1, "Error"))
	lines := workloadProblem(e, j)
	if len(lines) == 0 || !strings.Contains(lines[0], "1 try failed so far") {
		t.Errorf("%q", lines)
	}
}

// Round 99: with --readonly no Fix line offers a change key. A cron job's
// failed run's box offered t (run it now), which read-only refuses.
func TestReadOnlyCronRunFixOffersNoKey(t *testing.T) {
	ReadOnly = true
	defer func() { ReadOnly = false }()
	cj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "nightly", "namespace": "qa", "uid": "cj99"}}}
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "nightly-1", "namespace": "qa", "uid": "j99",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": "nightly", "uid": "cj99", "controller": true}}},
		"spec": map[string]any{"backoffLimit": int64(0)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{
			map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "message": "Job has reached the specified backoff limit"}}}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true},
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	e := enrich.New(nsStore{kube.CronJobs: {cj}, kube.Jobs: {job}}, res, nil)
	lines := workloadProblem(e, job)
	fix := ""
	for _, l := range lines {
		if strings.HasPrefix(l, "Fix") {
			fix = l
		}
	}
	if fix == "" || strings.Contains(fix, "t on its cron job") {
		t.Errorf("%q", lines)
	}
}
