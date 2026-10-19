package view

// Case map for jobs and cron jobs: what home (the app's status and Fix),
// the jobs list (gj STATUS), a job's box (Problem/Fix), a cron job's box
// and :changes say about a run, checked against what is true.
//
// Dimensions that change what must be said:
//   - stage: no pod yet; try running; a try failed and none runs yet;
//     the next try runs; the last try failed but the job isn't marked yet;
//     FailureTarget only (its last pods stopping); Failed; Complete;
//     SuccessCriteriaMet only (its last pods stopping after success).
//   - backoffLimit: 0, 1, several (6 by default): whether the try that just
//     failed was the last.
//   - podFailurePolicy: none, or set (tries counted by the policy, so no
//     "it fails now" guess).
//   - parallelism/completions: 1 (tries can be numbered), several (no try
//     number).
//   - restartPolicy: Never (a try is a pod), OnFailure (a try is a restart
//     in the same pod, nothing for the job loop to say).
//   - what Kubernetes still keeps: the failed pods, or none (ttl, by hand).
//   - why it failed: an exit code, a program that never started, the run's
//     deadline (activeDeadlineSeconds: pods stopped by Kubernetes).
//   - paused (suspend), queued by Kueue.
//   - cron jobs: newest run running/failed/succeeded/failing (FailureTarget),
//     overlapping runs (concurrency Allow: an older run succeeds after a
//     newer one failed), paused.
//
// 37 job cases, 13 cron job cases and 2 timing cases below; each says
// what must be said and what must never be said.

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

const mxNS = "mxjobs"

func mxTime(ago time.Duration) string { return time.Now().Add(-ago).UTC().Format(time.RFC3339) }

type mxCond struct {
	typ, reason, msg string
	ago              time.Duration
}

type mxJob struct {
	name            string
	ago             time.Duration // created
	spec, status    map[string]any
	conds           []mxCond
	cron            string // its cron job, if a run
	kueue           bool
	succeeded, fail int64
	active          int64
}

func (j mxJob) obj() *unstructured.Unstructured {
	spec := map[string]any{}
	for k, v := range j.spec {
		spec[k] = v
	}
	status := map[string]any{}
	for k, v := range j.status {
		status[k] = v
	}
	if j.fail > 0 {
		status["failed"] = j.fail
	}
	if j.succeeded > 0 {
		status["succeeded"] = j.succeeded
	}
	if j.active > 0 {
		status["active"] = j.active
	}
	var conds []any
	for _, c := range j.conds {
		if c.typ == "Complete" {
			status["completionTime"] = mxTime(c.ago) // as Kubernetes sets it, and the cron job's lastSuccessfulTime
		}
		conds = append(conds, map[string]any{"type": c.typ, "status": "True", "reason": c.reason, "message": c.msg, "lastTransitionTime": mxTime(c.ago)})
	}
	if conds != nil {
		status["conditions"] = conds
	}
	meta := map[string]any{"name": j.name, "namespace": mxNS, "uid": "uid-" + j.name,
		"labels": map[string]any{"app": "mx"}, "creationTimestamp": mxTime(j.ago)}
	if j.kueue {
		meta["labels"] = map[string]any{"app": "mx", "kueue.x-k8s.io/queue-name": "q"}
	}
	if j.cron != "" {
		meta["ownerReferences"] = []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": j.cron, "uid": "uid-" + j.cron, "controller": true}}
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": meta, "spec": spec, "status": status}}
}

// mx135Done and mx135Failed are jobs as a Kubernetes 1.35 cluster sends
// them once ended (read from kubectl on staging, QA round 128): both
// conditions (SuccessCriteriaMet then Complete; FailureTarget then
// Failed), status.terminating and ready 0, a completionTime only on
// success.
func mx135Done(name string, ago, ended time.Duration) mxJob {
	msg := "Reached expected number of succeeded pods"
	return mxJob{name: name, ago: ago, spec: map[string]any{"backoffLimit": int64(6), "completions": int64(1), "parallelism": int64(1)}, succeeded: 1,
		status: map[string]any{"terminating": int64(0), "ready": int64(0), "startTime": mxTime(ago), "uncountedTerminatedPods": map[string]any{}},
		conds:  []mxCond{{"SuccessCriteriaMet", "CompletionsReached", msg, ended}, {"Complete", "CompletionsReached", msg, ended}}}
}

func mx135Failed(name string, ago, ended time.Duration) mxJob {
	msg := "Job has reached the specified backoff limit"
	return mxJob{name: name, ago: ago, spec: map[string]any{"backoffLimit": int64(0), "completions": int64(1), "parallelism": int64(1)}, fail: 1,
		status: map[string]any{"terminating": int64(0), "ready": int64(0), "startTime": mxTime(ago), "uncountedTerminatedPods": map[string]any{}},
		conds:  []mxCond{{"FailureTarget", "BackoffLimitExceeded", msg, ended}, {"Failed", "BackoffLimitExceeded", msg, ended}}}
}

// mxState is a container's state: "run", "exit:N" (ended N, with ago),
// "start" (never started), "wait:Reason".
type mxCtr struct {
	name, state string
	image, msg  string // a waiting container's image and kubelet's message
	code        int64
	ago         time.Duration
}

func (c mxCtr) status() map[string]any {
	st := map[string]any{}
	switch c.state {
	case "run":
		st["running"] = map[string]any{"startedAt": mxTime(c.ago)}
	case "exit":
		reason := "Error"
		if c.code == 0 {
			reason = "Completed"
		}
		if c.code == 137 {
			reason = "OOMKilled"
		}
		st["terminated"] = map[string]any{"exitCode": c.code, "reason": reason, "finishedAt": mxTime(c.ago)}
	case "start":
		st["terminated"] = map[string]any{"exitCode": int64(128), "reason": "StartError", "finishedAt": mxTime(c.ago),
			"message": `failed to create containerd task: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: exec: "/run.sh": stat /run.sh: no such file or directory: unknown`}
	default:
		st["waiting"] = map[string]any{"reason": strings.TrimPrefix(c.state, "wait:"), "message": c.msg}
	}
	image := "busybox"
	if c.image != "" {
		image = c.image
	}
	return map[string]any{"name": c.name, "image": image, "state": st, "restartCount": int64(0)}
}

type mxJobPod struct {
	name, job string
	ago       time.Duration // created
	phase     string
	ctrs      []mxCtr
	sidecar   *mxCtr
	setup     *mxCtr // a setup step (init container) that runs first
	deleting  bool
	onFailure bool
}

func (p mxJobPod) obj() *unstructured.Unstructured {
	var specCtrs, sts []any
	for _, c := range p.ctrs {
		specCtrs = append(specCtrs, map[string]any{"name": c.name, "image": "busybox"})
		sts = append(sts, c.status())
	}
	policy := "Never"
	if p.onFailure {
		policy = "OnFailure"
	}
	spec := map[string]any{"restartPolicy": policy, "containers": specCtrs}
	status := map[string]any{"phase": p.phase, "containerStatuses": sts}
	if p.setup != nil {
		spec["initContainers"] = []any{map[string]any{"name": p.setup.name, "image": "busybox"}}
		status["initContainerStatuses"] = []any{p.setup.status()}
	}
	if p.sidecar != nil {
		spec["initContainers"] = []any{map[string]any{"name": p.sidecar.name, "image": "busybox", "restartPolicy": "Always"}}
		status["initContainerStatuses"] = []any{p.sidecar.status()}
	}
	meta := map[string]any{"name": p.name, "namespace": mxNS, "uid": "uid-" + p.name, "creationTimestamp": mxTime(p.ago),
		"labels":          map[string]any{"app": "mx", "batch.kubernetes.io/job-name": p.job},
		"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": p.job, "uid": "uid-" + p.job, "controller": true}}}
	if p.deleting {
		meta["deletionTimestamp"] = mxTime(5 * time.Second)
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": meta, "spec": spec, "status": status}}
}

func mxFailed(name, job string, ago time.Duration, code int64) mxJobPod {
	return mxJobPod{name: name, job: job, ago: ago + time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "exit", code: code, ago: ago}}}
}

func mxRunning(name, job string, ago time.Duration) mxJobPod {
	return mxJobPod{name: name, job: job, ago: ago, phase: "Running", ctrs: []mxCtr{{name: "main", state: "run", ago: ago}}}
}

type mxWorld struct {
	e     *enrich.Enricher
	jobs  []*unstructured.Unstructured
	pods  []*unstructured.Unstructured
	crons []*unstructured.Unstructured
}

func mxBuild(jobs []mxJob, pods []mxJobPod, crons ...*unstructured.Unstructured) mxWorld {
	return mxBuildWith(jobs, pods, nil, crons...)
}

// mxBuildWith is mxBuild with events kept in the namespace.
func mxBuildWith(jobs []mxJob, pods []mxJobPod, events []*unstructured.Unstructured, crons ...*unstructured.Unstructured) mxWorld {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}})
	var w mxWorld
	for _, j := range jobs {
		w.jobs = append(w.jobs, j.obj())
	}
	for _, p := range pods {
		w.pods = append(w.pods, p.obj())
	}
	w.crons = crons
	w.e = enrich.New(jobStore{kube.Jobs: w.jobs, kube.Pods: w.pods, kube.CronJobs: crons, kube.Events: events}, res, nil)
	return w
}

func (w mxWorld) app() *App {
	for _, a := range Apps(w.e, w.pods, nil, nil, w.jobs, w.crons) {
		if a.Name == "mx" {
			return a
		}
	}
	return &App{}
}

func (w mxWorld) jobRow(name string) string {
	for _, j := range w.jobs {
		if j.GetName() == name {
			cells, _ := registry[kube.Jobs.GroupResource()].Row(w.e, j)
			return cells[1]
		}
	}
	return ""
}

func (w mxWorld) jobBox(name string) string {
	for _, j := range w.jobs {
		if j.GetName() == name {
			return strings.Join(workloadProblem(w.e, j), "\n")
		}
	}
	return ""
}

func mxCron(lastSchedule, lastSuccess time.Duration, suspend bool) *unstructured.Unstructured {
	const name = "cj"
	status := map[string]any{}
	if lastSchedule > 0 {
		status["lastScheduleTime"] = mxTime(lastSchedule)
	}
	if lastSuccess > 0 {
		status["lastSuccessfulTime"] = mxTime(lastSuccess)
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": name, "namespace": mxNS, "uid": "uid-" + name, "labels": map[string]any{"app": "mx"},
			"creationTimestamp": mxTime(48 * time.Hour)},
		"spec": map[string]any{"schedule": "*/5 * * * *", "suspend": suspend,
			"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}},
				"spec": map[string]any{"containers": []any{map[string]any{"name": "main", "image": "busybox"}}}}}}},
		"status": status}}
}

// mxPodEvent is an event about pod name: reason and message, ago.
func mxPodEvent(pod, typ, reason, msg string, ago time.Duration) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"name": pod + "." + reason, "namespace": mxNS},
		"involvedObject": map[string]any{"kind": "Pod", "name": pod, "namespace": mxNS},
		"type":           typ, "reason": reason, "message": msg, "lastTimestamp": mxTime(ago)}}
}

// mxCronKeeping sets how many failed runs a cron job keeps.
func mxCronKeeping(cj *unstructured.Unstructured, failed int64) *unstructured.Unstructured {
	_ = unstructured.SetNestedField(cj.Object, failed, "spec", "failedJobsHistoryLimit")
	return cj
}

func mxCheck(t *testing.T, what, got string, must, never []string) {
	t.Helper()
	for _, m := range must {
		if !strings.Contains(got, m) {
			t.Errorf("%s: want %q in %q", what, m, got)
		}
	}
	for _, n := range never {
		if n != "" && strings.Contains(got, n) {
			t.Errorf("%s: must not say %q: %q", what, n, got)
		}
	}
}

func TestMatrixJobs(t *testing.T) {
	limit := func(n int64) map[string]any { return map[string]any{"backoffLimit": n} }
	failedCond := func(ago time.Duration) mxCond {
		return mxCond{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", ago}
	}
	cases := []struct {
		name      string
		jobs      []mxJob
		pods      []mxJobPod
		home      string   // the app's status word
		homeMust  []string // in its Why
		homeNever []string
		fixMust   []string
		fixNever  []string
		row       string // gj STATUS (prefix)
		rowNever  []string
		boxMust   []string
		boxNever  []string
		events    []*unstructured.Unstructured
	}{
		{name: "first try running",
			jobs: []mxJob{{name: "j", ago: time.Minute, spec: limit(6), active: 1}},
			pods: []mxJobPod{mxRunning("j-a", "j", 30*time.Second)},
			home: "Running", row: "Running", boxNever: []string{"Problem"}},
		{name: "try failed, none running, tries left",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 20*time.Second, 2)},
			home: "Retrying", homeMust: []string{"job j", "1 try failed", "exit"}, homeNever: []string{"try 2 running"},
			row: "Retrying (1 try failed)", boxMust: []string{"1 try failed so far"}, boxNever: []string{"this run failed"}},
		{name: "try failed, next running",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1, active: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 40*time.Second, 2), mxRunning("j-b", "j", 10*time.Second)},
			home: "Retrying", homeMust: []string{"try 2 running", "1 try failed before"},
			row: "Retrying (1 try failed, another running)", boxMust: []string{"1 try failed so far"}},
		{name: "parallel job: no try number",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: map[string]any{"backoffLimit": int64(6), "parallelism": int64(2), "completions": int64(4)}, fail: 1, active: 2}},
			pods: []mxJobPod{mxFailed("j-a", "j", 40*time.Second, 2), mxRunning("j-b", "j", 10*time.Second), mxRunning("j-c", "j", 10*time.Second)},
			home: "Retrying", homeMust: []string{"running again"}, homeNever: []string{"try 2"}},
		{name: "backoffLimit 0: the only try failed, not marked yet",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(0), fail: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 3*time.Second, 2)},
			home: "Job failed", homeMust: []string{"job j failed"}, homeNever: []string{"tries again"},
			row: "Failed", boxMust: []string{"this run failed"}, boxNever: []string{"so far"}},
		{name: "backoffLimit 1: second try failed, not marked yet",
			jobs: []mxJob{{name: "j", ago: 3 * time.Minute, spec: limit(1), fail: 2}},
			pods: []mxJobPod{mxFailed("j-a", "j", 90*time.Second, 2), mxFailed("j-b", "j", 3*time.Second, 2)},
			home: "Job failed", homeNever: []string{"tries again"}, row: "Failed", boxMust: []string{"this run failed"}},
		{name: "backoffLimit 1: first try failed, one left",
			jobs: []mxJob{{name: "j", ago: 3 * time.Minute, spec: limit(1), fail: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 3*time.Second, 2)},
			home: "Retrying", row: "Retrying"},
		{name: "marked Failed, pods kept",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(1), fail: 2, conds: []mxCond{failedCond(5 * time.Minute)}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 7*time.Minute, 3), mxFailed("j-b", "j", 5*time.Minute, 3)},
			home: "Job failed", homeMust: []string{"job j failed 5m", "exit"}, fixMust: []string{"l on the failed one"},
			row: "Failed", boxMust: []string{"this run failed", "exit"}},
		{name: "marked Failed, pods gone",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(1), fail: 2, conds: []mxCond{failedCond(5 * time.Minute)}}},
			home: "Job failed", homeMust: []string{"job j failed 5m"}, homeNever: []string{"Job has reached"},
			fixMust: []string{"its failed pods are gone"}, fixNever: []string{"l on the failed one", "fix it"},
			row: "Failed", boxMust: []string{"its failed pods are gone"}, boxNever: []string{"l on the failed one", "fix it"}},
		{name: "image never pulled, then its deadline passed (pod still stopping)",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			pods: []mxJobPod{{name: "j-bcdfg", job: "j", ago: 2 * time.Minute, phase: "Pending", deleting: true,
				ctrs: []mxCtr{{name: "main", state: "wait:ErrImagePull", image: "nope:1", msg: "failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found"}}}},
			home: "Job failed", homeMust: []string{"never ran", "nope:1", "doesn't exist", "time limit"}, homeNever: []string{"ran longer than it is allowed"},
			fixMust: []string{"image name or tag"}, fixNever: []string{"logs with them", "l on the failed one"},
			row: "Failed", boxMust: []string{"never ran", "nope:1", "time limit", "image name or tag"}, boxNever: []string{"l on the failed one", "logs with them"}},
		{name: "image never pulled, then its deadline passed (pod gone, its events kept)",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-bcdfg", "Normal", "Pulling", `Pulling image "nope:1"`, 2*time.Minute),
				mxPodEvent("j-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found`, 2*time.Minute),
				mxPodEvent("j-bcdfg", "Normal", "BackOff", `Back-off pulling image "nope:1"`, 90*time.Second),
				mxPodEvent("jx-bcdfg", "Normal", "Started", "Started container main", time.Minute)},
			home: "Job failed", homeMust: []string{"never ran", "nope:1", "doesn't exist", "time limit"},
			fixMust: []string{"image name or tag"}, fixNever: []string{"logs with them", "l on the failed one"},
			row: "Failed", boxMust: []string{"never ran", "nope:1", "time limit"}, boxNever: []string{"l on the failed one", "logs with them"}},
		{name: "a pull failed once, then it ran; its deadline passed (pod gone)",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": failed to pull and unpack image \"docker.io/library/nope:1\": failed to resolve reference \"docker.io/library/nope:1\": docker.io/library/nope:1: not found`, 3*time.Minute),
				mxPodEvent("j-bcdfg", "Normal", "Started", "Started container main", 2*time.Minute)},
			home: "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran"}},
		{name: "setup step failed, then its deadline passed",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(600)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			pods: []mxJobPod{{name: "j-bcdfg", job: "j", ago: 9 * time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "wait:PodInitializing"}},
				setup: &mxCtr{name: "setup", state: "exit", code: 1, ago: 8 * time.Minute}}},
			home: "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran", "then the run passed"},
			fixNever: []string{"it never ran"}, boxNever: []string{"it never ran"}},
		{name: "made again with the same name: the old run's pull failure isn't this one's",
			jobs:   []mxJob{{name: "j", ago: 3 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1, conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 20*time.Minute)},
			home:   "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran"}},
		{name: "a pod replaced while pulling, and the new one ran",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(300)}, fail: 1, conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 6*time.Minute),
				mxPodEvent("j-hjklm", "Normal", "Started", "Started container main", 4*time.Minute)},
			home: "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran"}},
		{name: "a cron run's pod of a longer name isn't this job's",
			jobs:   []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1, conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-29345678-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 2*time.Minute)},
			home:   "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran"}},
		{name: "an indexed job's pod name that is another job's (j-2)",
			jobs: []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60), "completionMode": "Indexed", "completions": int64(3)}, fail: 1, conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}},
				{name: "j-2", ago: 10 * time.Minute}},
			events: []*unstructured.Unstructured{mxPodEvent("j-2-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 2*time.Minute)},
			home:   "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"never ran"}},
		{name: "an indexed job's own pod never pulled (gone)",
			jobs:   []mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60), "completionMode": "Indexed", "completions": int64(3)}, fail: 1, conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			events: []*unstructured.Unstructured{mxPodEvent("j-2-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 2*time.Minute)},
			home:   "Job failed", homeMust: []string{"never ran", "nope:1"}},
		{name: "deadline exceeded, pods stopped and gone",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", 2 * time.Minute}}}},
			home: "Job failed", homeMust: []string{"ran longer than it is allowed"}, homeNever: []string{"Job was active"},
			row: "Failed", boxMust: []string{"ran longer than it is allowed"}},
		{name: "deadline exceeded after an earlier try failed",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"activeDeadlineSeconds": int64(600)}, fail: 2,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", 2 * time.Minute}}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 20*time.Minute, 2)},
			home: "Job failed", homeMust: []string{"ran longer than it is allowed"},
			row: "Failed", boxMust: []string{"ran longer than it is allowed"}},
		{name: "FailureTarget only, its pods already ended (not marked Failed yet)",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(0), fail: 1, conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 10 * time.Second}}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 12*time.Second, 2)},
			home: "Job failed", homeNever: []string{"tries again"}, row: "Failed", rowNever: []string{"stopping"}, boxMust: []string{"this run failed"}},
		{name: "FailureTarget only: last pods stopping",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"backoffLimit": int64(0), "parallelism": int64(2), "completions": int64(2)}, fail: 1,
				conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 10 * time.Second}}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 12*time.Second, 2), {name: "j-b", job: "j", ago: time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: time.Minute}}}},
			home: "Job failed", homeNever: []string{"tries again"}, row: "Failed (its last pods stopping)", boxMust: []string{"this run failed"}},
		{name: "FailureTarget only, status.terminating counts a pod not seen",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(0), fail: 1, status: map[string]any{"terminating": int64(1)},
				conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 10 * time.Second}}}},
			home: "Job failed", row: "Failed (its last pods stopping)"},
		// Kubernetes 1.31+ sends both: FailureTarget, then Failed once its
		// pods stopped; SuccessCriteriaMet, then Complete.
		{name: "failed, as 1.35 sends it (FailureTarget and Failed)",
			jobs: []mxJob{mx135Failed("j", time.Hour, 5*time.Minute)},
			pods: []mxJobPod{mxFailed("j-a", "j", 5*time.Minute, 2)},
			home: "Job failed", row: "Failed", rowNever: []string{"stopping"}, boxMust: []string{"this run failed"}},
		{name: "done, as 1.35 sends it (SuccessCriteriaMet and Complete)",
			jobs:      []mxJob{mx135Done("j", 5*time.Minute, 30*time.Second)},
			pods:      []mxJobPod{{name: "j-a", job: "j", ago: 2 * time.Minute, phase: "Succeeded", ctrs: []mxCtr{{name: "main", state: "exit", ago: 30 * time.Second}}}},
			homeNever: []string{"failed", "Retrying"}, row: "Done", rowNever: []string{"stopping"}, boxNever: []string{"Problem"}},
		{name: "done, pods gone, as 1.35 sends it",
			jobs: []mxJob{mx135Done("j", 5*time.Hour, 4*time.Hour)},
			row:  "Done", rowNever: []string{"stopping"}, boxNever: []string{"Problem"}},
		{name: "failed, pods gone, as 1.35 sends it",
			jobs: []mxJob{mx135Failed("j", 5*time.Hour, 4*time.Hour)},
			row:  "Failed", rowNever: []string{"stopping"}},
		{name: "SuccessCriteriaMet only, its pods already ended (not marked Complete yet)",
			jobs: []mxJob{{name: "j", ago: 5 * time.Minute, spec: limit(6), succeeded: 1,
				conds: []mxCond{{"SuccessCriteriaMet", "CompletionsReached", "Reached expected number of succeeded pods", time.Second}}}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: 2 * time.Minute, phase: "Succeeded", ctrs: []mxCtr{{name: "main", state: "exit", ago: time.Second}}}},
			row:  "Done", rowNever: []string{"stopping"}, boxNever: []string{"Problem"}},
		{name: "succeeded after a failed try",
			jobs:      []mxJob{{name: "j", ago: 5 * time.Minute, spec: limit(6), fail: 1, succeeded: 1, conds: []mxCond{{"Complete", "", "", 30 * time.Second}}}},
			pods:      []mxJobPod{mxFailed("j-a", "j", 3*time.Minute, 2), {name: "j-b", job: "j", ago: 2 * time.Minute, phase: "Succeeded", ctrs: []mxCtr{{name: "main", state: "exit", ago: 30 * time.Second}}}},
			homeNever: []string{"tries again", "failed ("}, row: "Done", boxNever: []string{"Problem"}},
		{name: "success criteria met, last pods stopping",
			jobs: []mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"completionMode": "Indexed", "completions": int64(3), "parallelism": int64(3),
				"successPolicy": map[string]any{"rules": []any{map[string]any{"succeededCount": int64(1)}}}}, fail: 1, succeeded: 1, active: 1,
				conds: []mxCond{{"SuccessCriteriaMet", "SuccessPolicy", "", 5 * time.Second}}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 60*time.Second, 2), {name: "j-b", job: "j", ago: 2 * time.Minute, phase: "Succeeded", ctrs: []mxCtr{{name: "main", state: "exit", ago: 5 * time.Second}}},
				{name: "j-c", job: "j", ago: 2 * time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: time.Minute}}}},
			homeNever: []string{"tries again", "Retrying", "failed before"}, row: "Done", boxNever: []string{"Problem"}},
		{name: "never started, tries left",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "start", ago: 20 * time.Second}}}},
			home: "Retrying", homeMust: []string{"never started"}, row: "Retrying", boxMust: []string{"never started"}},
		{name: "never started, then its deadline passed",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
				conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: 2 * time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "start", ago: time.Minute}}}},
			home: "Job failed", homeMust: []string{"never started", "time limit"}, fixMust: []string{"never started"}, fixNever: []string{"l on the failed one"},
			row: "Failed", boxMust: []string{"never started", "time limit", "Fix      it never started"}},
		{name: "never started, marked Failed",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(0), fail: 1, conds: []mxCond{failedCond(time.Minute)}}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: 2 * time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "start", ago: time.Minute}}}},
			home: "Job failed", homeMust: []string{"never started"}, fixMust: []string{"never started"}, fixNever: []string{"l on the failed one"},
			row: "Failed", boxMust: []string{"never started", "Fix"}},
		{name: "newest try's cause is named, not an older one's",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: limit(1), fail: 2, conds: []mxCond{failedCond(time.Minute)}}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: 10 * time.Minute, phase: "Failed", ctrs: []mxCtr{{name: "main", state: "start", ago: 9 * time.Minute}}},
				mxFailed("j-b", "j", time.Minute, 3)},
			home: "Job failed", homeMust: []string{"exit"}, homeNever: []string{"never started"},
			boxMust: []string{"exit"}, boxNever: []string{"never started"}},
		{name: "out of memory, tries left",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 20*time.Second, 137)},
			home: "Retrying", homeMust: []string{"out of memory"}, fixMust: []string{"memory limit"}},
		{name: "paused before it ran",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"suspend": true}}},
			home: "Paused", row: "Paused", boxNever: []string{"Problem"}},
		{name: "queued by Kueue",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"suspend": true}, kueue: true}},
			home: "Queued", row: "Paused"},
		{name: "paused after a failed try",
			jobs: []mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"suspend": true}, fail: 1, conds: []mxCond{{"Suspended", "JobSuspended", "", time.Minute}}}},
			pods: []mxJobPod{mxFailed("j-a", "j", 2*time.Minute, 2)},
			home: "Paused", homeNever: []string{"tries again"}, row: "Paused", boxNever: []string{"so far"}},
		{name: "failed a long time ago",
			jobs:      []mxJob{{name: "j", ago: 72 * time.Hour, spec: limit(0), fail: 1, conds: []mxCond{failedCond(48 * time.Hour)}}},
			pods:      []mxJobPod{mxFailed("j-a", "j", 48*time.Hour, 2)},
			homeNever: []string{"failed 2d", "Retrying"}, row: "Failed"},
		{name: "running try deleted by hand, still stopping",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: time.Minute}}}},
			home: "Retrying", homeMust: []string{"still stopping"}, fixNever: []string{"l on the failed one"},
			boxMust: []string{"a deleted pod is still stopping"}, boxNever: []string{"l on a failed one"}},
		// QA round 129: its only try deleted by hand while it ran (backoffLimit 0,
		// a 45s grace): the job failed at once, its pod still stopping. Not
		// "the failed one shows why", nor tries used up as if each one failed.
		{name: "running try deleted by hand, its job failed, pod still stopping",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(0), fail: 1, status: map[string]any{"terminating": int64(1)},
				conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 5 * time.Second}}}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: time.Minute}}}},
			home: "Job failed", homeMust: []string{"a deleted pod is still stopping"}, homeNever: []string{"so did any retries"},
			fixNever: []string{"l on the failed one", "fix it"}, row: "Failed (its last pods stopping)",
			boxMust: []string{"a deleted pod is still stopping"}, boxNever: []string{"so did any retries", "l on the failed one", "fix it"}},
		// QA round 130: the same once its pod is gone. Kubernetes counts the
		// deleted try as failed (so "failed" is its word), but nothing says
		// anything is broken: no "fix it", only how to run it again.
		{name: "running try deleted by hand, its job failed, pod gone",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(0), fail: 1,
				conds: []mxCond{{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", time.Minute}}}},
			home: "Job failed", homeMust: []string{"job j failed 1m"},
			fixMust: []string{"its failed pods are gone", "E shows its events", "create the job again if it should run"}, fixNever: []string{"fix it", "l on the failed one"},
			row: "Failed", boxMust: []string{"its failed pods are gone", "create the job again if it should run"}, boxNever: []string{"fix it", "l on the failed one"}},
		{name: "a failed try gone, none running",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), fail: 1}},
			home: "Retrying", homeMust: []string{"gone"}, fixMust: []string{"gone"}, fixNever: []string{"l on the failed one"}},
		{name: "OnFailure: container crashing in place",
			jobs:      []mxJob{{name: "j", ago: 10 * time.Minute, spec: limit(6), active: 1}},
			pods:      []mxJobPod{{name: "j-a", job: "j", ago: 10 * time.Minute, phase: "Running", onFailure: true, ctrs: []mxCtr{{name: "main", state: "wait:CrashLoopBackOff"}}}},
			homeNever: []string{"try 2", "tries again after a pause"}, row: "Running"},
		{name: "pod failure policy: try failed, unknown whether it is the last",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: map[string]any{"backoffLimit": int64(0), "podFailurePolicy": map[string]any{"rules": []any{}}}, fail: 1}},
			pods: []mxJobPod{mxFailed("j-a", "j", 3*time.Second, 2)},
			home: "Retrying", row: "Retrying"},
		{name: "native sidecar keeps a failed try open, tries left",
			jobs: []mxJob{{name: "j", ago: 2 * time.Minute, spec: limit(6), active: 1}},
			pods: []mxJobPod{{name: "j-a", job: "j", ago: time.Minute, phase: "Running", ctrs: []mxCtr{{name: "main", state: "exit", code: 1, ago: 3 * time.Second}},
				sidecar: &mxCtr{name: "side", state: "run", ago: time.Minute}}},
			home: "Retrying", homeMust: []string{"then Kubernetes tries again"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := mxBuildWith(c.jobs, c.pods, c.events)
			a := w.app()
			if c.home != "" && a.Status.Short != c.home {
				t.Errorf("home: %q (%s), want %q", a.Status.Short, a.Status.Why, c.home)
			}
			mxCheck(t, "home why", a.Status.Why, c.homeMust, c.homeNever)
			mxCheck(t, "home fix", appFix(a, false), c.fixMust, c.fixNever)
			if ro := appFix(a, true); strings.Contains(ro, "ctrl-d") || strings.Contains(ro, "t on its") && !strings.Contains(ro, "without --readonly") {
				t.Errorf("read-only fix offers a change: %q", ro)
			}
			if c.row != "" {
				if got := w.jobRow(c.jobs[0].name); !strings.HasPrefix(got, c.row) {
					t.Errorf("gj: %q, want %q", got, c.row)
				}
			}
			mxCheck(t, "gj", w.jobRow(c.jobs[0].name), nil, c.rowNever)
			mxCheck(t, "box", w.jobBox(c.jobs[0].name), c.boxMust, c.boxNever)
			// home and the gj row agree on whether it failed or retries
			row := w.jobRow(c.jobs[0].name)
			if a.Status.Short == "Retrying" && strings.HasPrefix(row, "Failed") || a.Status.Short == "Job failed" && strings.HasPrefix(row, "Retrying") {
				t.Errorf("home %q, gj %q", a.Status.Short, row)
			}
		})
	}
}

// :changes and home's recent problems date a failed job as home does.
func TestMatrixJobTimesAgree(t *testing.T) {
	for _, conds := range [][]mxCond{
		{{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 7 * time.Minute}},
		{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 9 * time.Minute},
			{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 7 * time.Minute}},
	} {
		w := mxBuild([]mxJob{{name: "j", ago: time.Hour, spec: map[string]any{"backoffLimit": int64(0)}, fail: 1, conds: conds}}, nil)
		a := w.app()
		want := conds[0].ago
		mxCheck(t, "home", a.Status.Why, []string{"failed " + Age(time.Now().Add(-want)) + " ago"}, nil)
		found := false
		for _, ch := range w.e.Changes(mxNS, "", time.Now(), nil) {
			if ch.Object == "job/j" && ch.What == "failed" {
				found = true
				if d := time.Since(ch.When) - want; d > 2*time.Second || d < -2*time.Second {
					t.Errorf(":changes at %v ago, home %v ago", time.Since(ch.When), want)
				}
			}
		}
		if !found {
			t.Errorf(":changes has no failed row")
		}
		for _, p := range w.e.Home(time.Now()).Problems {
			if p.Name == "j" {
				if d := time.Since(p.Since) - want; d > 2*time.Second || d < -2*time.Second {
					t.Errorf("recent problems at %v ago, home %v ago", time.Since(p.Since), want)
				}
			}
		}
	}
}

func TestMatrixCronBox(t *testing.T) {
	run := func(name string, ago time.Duration, conds ...mxCond) mxJob {
		return mxJob{name: name, ago: ago, cron: "cj", conds: conds}
	}
	failed := func(ago time.Duration) mxCond {
		return mxCond{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", ago}
	}
	done := func(ago time.Duration) mxCond { return mxCond{"Complete", "", "", ago} }
	cronRun := func(j mxJob) mxJob { j.cron = "cj"; return j }
	cases := []struct {
		name       string
		cron       *unstructured.Unstructured
		jobs       []mxJob
		pods       []mxJobPod
		must       []string
		never      []string
		homeFailed bool
		running    string // the list's RUNNING
		bad        bool   // the list row flagged failing
		warn       bool   // the list row flagged to check
		homeShort  string
		homeMust   []string
	}{
		{name: "last run worked", cron: mxCron(4*time.Minute, 3*time.Minute, false),
			jobs: []mxJob{run("cj-2", 4*time.Minute, done(3*time.Minute))},
			must: []string{"and it worked"}, never: []string{"Problem"}, running: "0"},
		{name: "last run failed", cron: mxCron(4*time.Minute, 9*time.Minute, false),
			jobs: []mxJob{run("cj-1", 9*time.Minute, done(9*time.Minute)), run("cj-2", 4*time.Minute, failed(3*time.Minute))},
			pods: []mxJobPod{mxFailed("cj-2-a", "cj-2", 3*time.Minute, 2)},
			must: []string{"its last run failed", "last one that worked was"}, never: []string{"and it worked"}, homeFailed: true, running: "0", bad: true},
		{name: "newest run running, the one before failed", cron: mxCron(time.Minute, 20*time.Minute, false),
			jobs: []mxJob{run("cj-1", 6*time.Minute, failed(5*time.Minute)), run("cj-2", time.Minute)},
			pods: []mxJobPod{mxFailed("cj-1-a", "cj-1", 5*time.Minute, 2), mxRunning("cj-2-a", "cj-2", time.Minute)},
			must: []string{"still running", "the last run that finished failed"}, never: []string{"its last run failed"}, running: "1", bad: true},
		{name: "newest run failing (FailureTarget)", cron: mxCron(2*time.Minute, 20*time.Minute, false),
			jobs: []mxJob{run("cj-2", 2*time.Minute, mxCond{"FailureTarget", "BackoffLimitExceeded", "", 10 * time.Second})},
			pods: []mxJobPod{mxFailed("cj-2-a", "cj-2", 12*time.Second, 2)},
			must: []string{"failed"}, never: []string{"still running"}, homeFailed: true, running: "0", bad: true},
		{name: "overlapping runs: the newer failed, an older one worked after", cron: mxCron(5*time.Minute, time.Minute, false),
			jobs: []mxJob{run("cj-1", 10*time.Minute, done(time.Minute)), run("cj-2", 5*time.Minute, failed(3*time.Minute))},
			pods: []mxJobPod{mxFailed("cj-2-a", "cj-2", 3*time.Minute, 2)},
			must: []string{"Last run 5m ago failed; an earlier run finished fine 1m ago"}, never: []string{"and it worked", "but the last one that worked"},
			homeFailed: true, running: "0", bad: true},
		{name: "paused after a failed run", cron: mxCron(4*time.Minute, 9*time.Minute, true),
			jobs: []mxJob{run("cj-2", 4*time.Minute, failed(3*time.Minute))},
			pods: []mxJobPod{mxFailed("cj-2-a", "cj-2", 3*time.Minute, 2)},
			must: []string{"Paused"}},
		{name: "older runs failed, the newest worked", cron: mxCron(4*time.Minute, 3*time.Minute, false),
			jobs: []mxJob{run("cj-1", 14*time.Minute, failed(13*time.Minute)), run("cj-2", 9*time.Minute, failed(8*time.Minute)), run("cj-3", 4*time.Minute, done(3*time.Minute))},
			pods: []mxJobPod{mxFailed("cj-1-a", "cj-1", 13*time.Minute, 2), mxFailed("cj-2-a", "cj-2", 8*time.Minute, 2)},
			must: []string{"and it worked"}, never: []string{"Problem"}, running: "0"},
		{name: "newest run met its success criteria, an older one failed", cron: mxCron(4*time.Minute, 20*time.Minute, false),
			jobs: []mxJob{run("cj-1", 9*time.Minute, failed(8*time.Minute)), run("cj-2", 4*time.Minute, mxCond{"SuccessCriteriaMet", "SuccessPolicy", "", 10 * time.Second})},
			pods: []mxJobPod{mxFailed("cj-1-a", "cj-1", 8*time.Minute, 2)},
			must: []string{"and it worked"}, never: []string{"Problem", "run failed"}, running: "0"},
		{name: "last run worked, as 1.35 sends it (SuccessCriteriaMet and Complete)", cron: mxCron(4*time.Minute, 3*time.Minute, false),
			jobs: []mxJob{cronRun(mx135Done("cj-2", 4*time.Minute, 3*time.Minute))},
			must: []string{"and it worked"}, never: []string{"Problem", "stopping"}, running: "0"},
		{name: "last run failed, as 1.35 sends it (FailureTarget and Failed)", cron: mxCron(4*time.Minute, 9*time.Minute, false),
			jobs: []mxJob{cronRun(mx135Done("cj-1", 9*time.Minute, 9*time.Minute)), cronRun(mx135Failed("cj-2", 4*time.Minute, 3*time.Minute))},
			pods: []mxJobPod{mxFailed("cj-2-a", "cj-2", 3*time.Minute, 2)},
			must: []string{"its last run failed", "last one that worked was"}, never: []string{"and it worked"}, homeFailed: true, running: "0", bad: true},
		{name: "newest run past its deadline (FailureTarget), its pod that can't pull stopping", cron: mxCron(3*time.Minute, 20*time.Minute, false),
			jobs: []mxJob{run("cj-2", 3*time.Minute, mxCond{"FailureTarget", "DeadlineExceeded", "Job was active longer than specified deadline", 5 * time.Second})},
			pods: []mxJobPod{{name: "cj-2-a", job: "cj-2", ago: 3 * time.Minute, phase: "Pending", deleting: true,
				ctrs: []mxCtr{{name: "main", state: "wait:ImagePullBackOff", image: "nope:1", msg: `Back-off pulling image "nope:1"`}}}},
			never: []string{"never finish"}, homeFailed: true, running: "0", bad: true},
		{name: "always fails, failed runs not kept", cron: mxCronKeeping(mxCron(time.Minute, 0, false), 0),
			must: []string{"it has never succeeded"}, running: "0", warn: true,
			homeShort: "Not succeeding", homeMust: []string{"cron job cj", "1m ago didn't succeed", "never succeeded", "failedJobsHistoryLimit 0"}},
		{name: "last run didn't succeed and was removed, it worked before", cron: mxCron(5*time.Minute, 30*time.Minute, false),
			running: "0", warn: true,
			homeShort: "Not succeeding", homeMust: []string{"5m ago didn't succeed", "last worked 30m ago", "isn't kept"}},
		{name: "runs cleaned up after working", cron: mxCronKeeping(mxCron(5*time.Minute, 4*time.Minute, false), 0),
			must: []string{"and it worked"}, running: "0", homeShort: "Scheduled"},
		{name: "a run started by hand is going on (not in status.active)", cron: mxCron(10*time.Minute, 9*time.Minute, false),
			jobs: []mxJob{run("cj-manual-bcdfg", time.Minute)}, pods: []mxJobPod{mxRunning("cj-manual-bcdfg-a", "cj-manual-bcdfg", time.Minute)},
			running: "1", homeShort: "Running"},
		{name: "never ran", cron: mxCron(0, 0, false), homeShort: "Scheduled", must: []string{"Last run never"}, never: []string{"Problem"}},
		// Review: the newest run failed and its job was removed (a failed
		// history limit of 0, or ctrl-d); an older success is still kept.
		// The kept run isn't the last one, so not "and it worked".
		{name: "newest run gone, an older success kept", cron: mxCron(2*time.Minute, 29*time.Minute, false),
			jobs: []mxJob{run("cj-1", 30*time.Minute, done(29*time.Minute))},
			must: []string{"the last one that worked was 29m ago"}, never: []string{"and it worked"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := mxBuild(c.jobs, c.pods, c.cron)
			mxCheck(t, "cron box", strings.Join(cronDetail(w.e, c.cron), "\n"), c.must, c.never)
			a := w.app()
			cells, tone := registry[kube.CronJobs.GroupResource()].Row(w.e, c.cron)
			if c.running != "" && cells[4] != c.running {
				t.Errorf("RUNNING %q, want %q", cells[4], c.running)
			}
			if c.running != "" && (tone == ToneBad) != c.bad {
				t.Errorf("list tone %v, want failing %v", tone, c.bad)
			}
			if c.running != "" && (tone == ToneWarn) != c.warn {
				t.Errorf("list tone %v, want to check %v", tone, c.warn)
			}
			if c.homeShort != "" && a.Status.Short != c.homeShort {
				t.Errorf("home %q (%s), want %q", a.Status.Short, a.Status.Why, c.homeShort)
			}
			mxCheck(t, "home why", a.Status.Why, c.homeMust, nil)
			if a.Status.Short == "Not succeeding" { // nothing kept to read: no "l" advice
				mxCheck(t, "home fix", appFix(a, false), []string{"E on the cron job"}, []string{"l on the failed one"})
			}
			if got := a.Status.Short == "Job failed"; got != c.homeFailed {
				t.Errorf("home %q (%s), want failed %v", a.Status.Short, a.Status.Why, c.homeFailed)
			}
		})
	}
}

// unsyncedJobs is a store whose Jobs haven't loaded yet (home doesn't
// wait for them).
type unsyncedJobs struct{ jobStore }

func (s unsyncedJobs) Synced(g schema.GroupVersionResource) bool { return g != kube.Jobs }

// While Jobs load, a cron job's kept failed run isn't known yet: its
// last run must not read as removed ("isn't kept").
func TestMatrixCronGoneWaitsForJobs(t *testing.T) {
	cj := mxCron(4*time.Minute, 30*time.Minute, false)
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true}})
	e := enrich.New(unsyncedJobs{jobStore{kube.CronJobs: {cj}}}, res, nil)
	if e.CronLastRunGone(cj) {
		t.Error("removed, before Jobs are listed")
	}
	for _, a := range Apps(e, nil, nil, nil, nil, []*unstructured.Unstructured{cj}) {
		if a.Status.Short == "Not succeeding" {
			t.Errorf("home %q: %s", a.Status.Short, a.Status.Why)
		}
	}
}

// A job retrying now leads over a cron job whose last run is gone: the
// problem happening now first.
func TestMatrixRetryingLeadsCronGone(t *testing.T) {
	w := mxBuild([]mxJob{{name: "j", ago: 2 * time.Minute, spec: map[string]any{"backoffLimit": int64(6)}, fail: 1}},
		[]mxJobPod{mxFailed("j-a", "j", 20*time.Second, 2)}, mxCronKeeping(mxCron(time.Minute, 0, false), 0))
	if a := w.app(); a.Status.Short != "Retrying" {
		t.Errorf("home %q: %s", a.Status.Short, a.Status.Why)
	}
}

// :changes (and w) give a failed job the cause home gives: its image
// never pulled, not only that its deadline passed.
func TestMatrixChangesJobCause(t *testing.T) {
	w := mxBuildWith([]mxJob{{name: "j", ago: 10 * time.Minute, spec: map[string]any{"activeDeadlineSeconds": int64(60)}, fail: 1,
		conds: []mxCond{{"Failed", "DeadlineExceeded", "Job was active longer than specified deadline", time.Minute}}}}, nil,
		[]*unstructured.Unstructured{mxPodEvent("j-bcdfg", "Warning", "Failed", `Failed to pull image "nope:1": docker.io/library/nope:1: not found`, 2*time.Minute)})
	home := w.app().Status.Why
	found := false
	for _, ch := range w.e.Changes(mxNS, "", time.Now(), nil) {
		if ch.Object == "job/j" && ch.What == "failed" {
			found = true
			if !strings.Contains(ch.Detail, "never ran") || !strings.Contains(ch.Detail, "nope:1") || !strings.Contains(home, ch.Detail) {
				t.Errorf(":changes %q, home %q", ch.Detail, home)
			}
		}
	}
	if !found {
		t.Error(":changes has no failed row")
	}
}

// An app with a one-off job that failed and an unrelated cron job: the
// Fix follows the job that failed (made again by hand), not the cron
// job's schedule. And the other way round.
func TestMatrixJobFixFollowsItsOwner(t *testing.T) {
	failed := mxCond{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 5 * time.Minute}
	for _, c := range []struct {
		name        string
		job         mxJob
		code        int64
		readOnly    bool
		must, never []string
	}{
		{"one-off job failed", mxJob{name: "failjob", ago: time.Hour, fail: 1, conds: []mxCond{failed}}, 2, false,
			[]string{"fix it and create the job again"}, []string{"next run", "cron job"}},
		{"one-off job failed, read-only", mxJob{name: "failjob", ago: time.Hour, fail: 1, conds: []mxCond{failed}}, 2, true,
			[]string{"fix it and create the job again"}, []string{"next run", "cron job"}},
		{"one-off job out of memory", mxJob{name: "failjob", ago: time.Hour, fail: 1, conds: []mxCond{failed}}, 137, false,
			[]string{"where the job is defined"}, []string{"next run", "cron job"}},
		{"one-off job out of memory, read-only", mxJob{name: "failjob", ago: time.Hour, fail: 1, conds: []mxCond{failed}}, 137, true,
			[]string{"where the job is defined"}, []string{"next run", "cron job"}},
		{"the cron job's run failed", mxJob{name: "cj-1", ago: time.Hour, fail: 1, cron: "cj", conds: []mxCond{failed}}, 2, false,
			[]string{"next run is on schedule"}, []string{"create the job again"}},
		{"the cron job's run out of memory", mxJob{name: "cj-1", ago: time.Hour, fail: 1, cron: "cj", conds: []mxCond{failed}}, 137, false,
			[]string{"in its cron job"}, []string{"create it again"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := mxBuild([]mxJob{c.job}, []mxJobPod{mxFailed(c.job.name+"-a", c.job.name, 6*time.Minute, c.code)}, mxCron(30*time.Minute, 30*time.Minute, false))
			a := w.app()
			if a.Status.Short != "Job failed" {
				t.Fatalf("home %q: %s", a.Status.Short, a.Status.Why)
			}
			mxCheck(t, "fix", appFix(a, c.readOnly), c.must, c.never)
		})
	}
}

// QA round 129: a job's pod deleted by hand still runs while it stops:
// the Pods line doesn't say none running without the one stopping.
func TestPodsLineCountsStopping(t *testing.T) {
	w := mxBuild([]mxJob{{name: "j", ago: 2 * time.Minute, spec: map[string]any{"backoffLimit": int64(0)}, fail: 1, status: map[string]any{"terminating": int64(1)},
		conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 5 * time.Second}}}},
		[]mxJobPod{{name: "j-a", job: "j", ago: time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: time.Minute}}}})
	for _, l := range AppLines(w.app(), Lookups{}) {
		if strings.HasPrefix(l, "Pods") && !strings.Contains(l, "none running; 1 pod shutting down") {
			t.Errorf("%q", l)
		}
	}
}
