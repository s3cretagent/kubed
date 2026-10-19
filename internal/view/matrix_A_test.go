package view

// Case map: pod and container status (area A).
//
// What changes what kubed must say about a pod, and so what these tests
// cover:
//
//  1. A container's state now: waiting (CrashLoopBackOff, RunContainerError,
//     ImagePullBackOff / ErrImagePull, CreateContainerConfigError,
//     ContainerCreating), running (ready, not ready, just started), or
//     terminated (exit 0 Completed, exit 1 Error, 137 OOMKilled, 137/143 by
//     a signal, StartError).
//  2. Its last state and how long ago it ended: none, within 5m, 10m, 2h,
//     24h, older.
//  3. Its restart count: 0, 1, 2, 3 and more (3 is where "keeps" starts).
//  4. Probe outcomes: a readiness failure event for this run, for another
//     container, or from before this run (stale); an alive check kill.
//  5. Kinds of container: regular, setup (init) steps (running, failed,
//     restarting, with their own memory limit), native sidecars (init
//     containers with restartPolicy Always: they run, crash and restart as
//     the app's containers do), several containers in one pod.
//  6. Pod phase: Pending, Running, Succeeded, Failed, Unknown; the Ready
//     condition (false while the containers last said ready: a readiness
//     gate, or a node that stopped reporting).
//  7. Deletion: within its grace period, past it (Kubernetes stores the
//     deadline, request time plus grace, as the deletion time), with
//     finalizers; eviction; node shutdown.
//  8. Owner: ReplicaSet (Deployment), StatefulSet, DaemonSet, Job (with
//     restartPolicy OnFailure or Never), bare pod.
//  9. In an app: several copies, or several containers, at the same and at
//     different states: the headline, its pod, the Error line's pod and
//     container and the pod p opens must agree, whatever order the pods
//     come in.
//
// Each case below states the truth, then checks kubed's words for what
// they must say and what they must never say. 73 pod cases, 9 app cases (each in both pod orders), a
// pods-list row check and 4 workload-box checks.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// mxC is one container of a case's pod.
type mxC struct {
	name     string
	kind     string // "" a regular container, "init" a setup step, "side" a native sidecar
	limit    string // memory limit
	state    corev1.ContainerState
	last     *corev1.ContainerStateTerminated
	restarts int32
	ready    bool
}

func mxAgo(d time.Duration) metav1.Time { return metav1.NewTime(time.Now().Add(-d)) }

func mxRun(ago time.Duration) corev1.ContainerState {
	return corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: mxAgo(ago)}}
}

func mxWait(reason, msg string) corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: msg}}
}

func mxEnd(reason string, code int32, ago time.Duration) *corev1.ContainerStateTerminated {
	return &corev1.ContainerStateTerminated{Reason: reason, ExitCode: code, FinishedAt: mxAgo(ago), StartedAt: mxAgo(ago + time.Minute)}
}

func mxStopped(reason string, code int32, ago time.Duration) corev1.ContainerState {
	return corev1.ContainerState{Terminated: mxEnd(reason, code, ago)}
}

// mxPod builds a pod of the given phase and owner kind ("" a bare pod).
func mxPod(name, owner string, phase corev1.PodPhase, cs ...mxC) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "mxa", UID: types.UID("uid-" + name),
		Labels: map[string]string{"app": "mx"}, CreationTimestamp: mxAgo(3 * time.Hour)},
		Status: corev1.PodStatus{Phase: phase}}
	if owner != "" {
		yes := true
		p.OwnerReferences = []metav1.OwnerReference{{Kind: owner, Name: "mx-owner", Controller: &yes, APIVersion: "apps/v1"}}
		if owner == "Job" {
			p.OwnerReferences[0].APIVersion = "batch/v1"
			p.Spec.RestartPolicy = corev1.RestartPolicyNever
		}
	}
	always := corev1.ContainerRestartPolicyAlways
	for _, c := range cs {
		spec := corev1.Container{Name: c.name, Image: "img/" + c.name + ":1"}
		if c.limit != "" {
			spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(c.limit)}
		}
		st := corev1.ContainerStatus{Name: c.name, Image: spec.Image, State: c.state, RestartCount: c.restarts, Ready: c.ready}
		if c.last != nil {
			st.LastTerminationState.Terminated = c.last
		}
		switch c.kind {
		case "init", "side":
			if c.kind == "side" {
				spec.RestartPolicy = &always
			}
			p.Spec.InitContainers = append(p.Spec.InitContainers, spec)
			p.Status.InitContainerStatuses = append(p.Status.InitContainerStatuses, st)
		default:
			p.Spec.Containers = append(p.Spec.Containers, spec)
			p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, st)
		}
	}
	return p
}

func mxObj(p *corev1.Pod) *unstructured.Unstructured {
	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	u := &unstructured.Unstructured{Object: obj}
	u.SetKind("Pod")
	u.SetAPIVersion("v1")
	return u
}

// mxEvent is an event about pod p (container "" for the pod itself).
func mxEvent(name, pod, container, reason, msg string, ago time.Duration) *unstructured.Unstructured {
	inv := map[string]any{"kind": "Pod", "name": pod}
	if container != "" {
		inv["fieldPath"] = "spec.containers{" + container + "}"
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": name, "namespace": "mxa"}, "reason": reason, "type": "Warning", "message": msg,
		"involvedObject": inv, "lastTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339)}}
}

// mxStore is a listStore that finds objects by name too.
type mxStore struct{ listStore }

func (s mxStore) Get(g schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s.listStore[g] {
		if o.GetName() == name && o.GetNamespace() == ns {
			return o, true
		}
	}
	return nil, false
}

func mxEnricher(objs map[string][]*unstructured.Unstructured) *enrich.Enricher {
	s := listStore{}
	for kind, list := range objs {
		switch kind {
		case "events":
			s[kube.Events] = list
		case "nodes":
			s[kube.Nodes] = list
		}
	}
	return enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}, {GVR: kube.Nodes, Kind: "Node"}}), nil)
}

type mxCase struct {
	name   string
	pod    *corev1.Pod
	events []*unstructured.Unstructured
	nodes  []*unstructured.Unstructured
	short  string   // the status word it must show
	has    []string // words its Why must have
	never  []string // words its Why must never have
}

func TestMatrixPodStatus(t *testing.T) {
	const lim = "256Mi"
	var (
		ok      = func(name string) mxC { return mxC{name: name, limit: lim, state: mxRun(time.Hour), ready: true} }
		grace30 = int64(30)
	)
	deleting := func(p *corev1.Pod, deadlineAgo time.Duration, grace int64, finalizers ...string) *corev1.Pod {
		at := mxAgo(deadlineAgo) // Kubernetes stores request time plus grace: the deadline
		p.DeletionTimestamp, p.DeletionGracePeriodSeconds, p.Finalizers = &at, &grace, finalizers
		return p
	}
	withCond := func(p *corev1.Pod, conds ...corev1.PodCondition) *corev1.Pod {
		p.Status.Conditions = conds
		return p
	}
	onFailure := func(p *corev1.Pod) *corev1.Pod {
		p.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		return p
	}
	notReadyNode := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{"name": "n1"},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "Unknown"}}}}}
	onNode := func(p *corev1.Pod) *corev1.Pod { p.Spec.NodeName = "n1"; return p }
	gate := func(p *corev1.Pod, cond corev1.PodConditionType) *corev1.Pod {
		p.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: cond}}
		return p
	}

	cases := []mxCase{
		// Running, ready: restarts and how long ago.
		{name: "ready, never restarted", pod: mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")),
			short: "Running", has: []string{"healthy"}, never: []string{"restart"}},
		{name: "ready, 1 restart 5m ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(5 * time.Minute), ready: true, restarts: 1, last: mxEnd("Error", 1, 5*time.Minute)}),
			short: "Running", has: []string{"1 restart in total", "5m ago"}},
		{name: "ready, 2 restarts 3m ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(3 * time.Minute), ready: true, restarts: 2, last: mxEnd("Error", 1, 3*time.Minute)}),
			short: "Running", has: []string{"2 restarts in total"}},
		{name: "ready, 3 restarts 1m ago: between crashes", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(time.Minute), ready: true, restarts: 3, last: mxEnd("Error", 1, time.Minute)}),
			short: "Crashing", has: []string{`"app"`, "3 restarts", "1m ago"}},
		{name: "ready, 3 restarts 10m ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute), ready: true, restarts: 3, last: mxEnd("Error", 1, 10*time.Minute)}),
			short: "Restarting", has: []string{`"app"`, "3 times", "10m ago"}},
		{name: "ready, 4 restarts 3h ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(3 * time.Hour), ready: true, restarts: 4, last: mxEnd("Error", 1, 3*time.Hour)}),
			short: "Running", has: []string{"4 restarts in total", "3h"}},
		{name: "ready, 5 restarts 25h ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(25 * time.Hour), ready: true, restarts: 5, last: mxEnd("Error", 1, 25*time.Hour)}),
			short: "Running", has: []string{"5 restarts in total"}},
		{name: "ready, finished cleanly 4 times (Completed)", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(5 * time.Minute), ready: true, restarts: 4, last: mxEnd("Completed", 0, 5*time.Minute)}),
			short: "Restarting", has: []string{"4 times", "keeps stopping", "exit code 0"}, never: []string{"crash", "memory"}},

		// Out of memory.
		{name: "OOM once, 1h ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, time.Hour)}),
			short: "Hit memory limit", has: []string{"1h ago", `"app"`, "256Mi"}},
		{name: "OOM 4 times, last 30m ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(30 * time.Minute), ready: true, restarts: 4, last: mxEnd("OOMKilled", 137, 30*time.Minute)}),
			short: "Out of memory", has: []string{"keeps running out of memory", "256Mi", "4 restarts", "30m ago"}},
		{name: "OOM 4 times, last 3h ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(3 * time.Hour), ready: true, restarts: 4, last: mxEnd("OOMKilled", 137, 3*time.Hour)}),
			short: "Hit memory limit", has: []string{"3h ago"}},
		{name: "OOM 25h ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(25 * time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, 25*time.Hour)}),
			short: "Running", never: []string{"memory"}},
		{name: "OOM with no limit", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, time.Hour)}),
			short: "Hit memory limit", has: []string{"limit none set"}},

		// Crash loops.
		{name: "CrashLoopBackOff, exit 1, 5 restarts", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 5, last: mxEnd("Error", 1, 2*time.Minute)}),
			short: "Crashing", has: []string{`"app"`, "5 restarts", "exit code 1", "2m ago"}},
		{name: "CrashLoopBackOff, exit by signal 143", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 5, last: mxEnd("Error", 143, 2*time.Minute)}),
			short: "Crashing", has: []string{"exit code 143"}},
		{name: "CrashLoopBackOff, OOM", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 9, last: mxEnd("OOMKilled", 137, 2*time.Minute)}),
			short: "Out of memory", has: []string{"256Mi", "9 restarts"}},
		{name: "CrashLoopBackOff, exits 0 each time", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 6, last: mxEnd("Completed", 0, time.Minute)}),
			short: "Restarting", has: []string{`"app"`, "exit code 0", "keeps stopping"}, never: []string{"keeps crashing", "crashed run"}},
		{name: "exits 0, stopped now for its back-off (newer Kubernetes): the stop now is the latest", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxStopped("Completed", 0, time.Minute), restarts: 7, last: mxEnd("Completed", 0, 6*time.Minute)}),
			short: "Restarting", has: []string{`"app"`, "keeps stopping", "exit code 0", "1m ago"}, never: []string{"crash", "6m"}},
		{name: "a job's (OnFailure) container that exited 0 is done, not restarting", pod: onFailure(mxPod("p", "Job", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxStopped("Completed", 0, time.Minute), restarts: 2, last: mxEnd("Error", 1, 6*time.Minute)},
			mxC{name: "proxy", state: mxRun(time.Hour), ready: true})),
			short: "Running", never: []string{"keeps"}},
		{name: "CrashLoopBackOff, alive check kill", pod: mxPod("pk", "ReplicaSet", corev1.PodRunning, // its own name: kubed remembers a pod's kills
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 5, last: mxEnd("Error", 137, time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("k", "pk", "app", "Killing", "Container app failed liveness probe, will be restarted", time.Minute)},
			short:  "Crashing", has: []string{"alive check"}},
		{name: "exits 0, killed by its startup check (it handles the stop signal)", pod: mxPod("pks", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 5, last: mxEnd("Completed", 0, time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("k", "pks", "app", "Killing", "Container app failed startup probe, will be restarted", time.Minute),
				mxEvent("u", "pks", "app", "Unhealthy", "Startup probe failed: HTTP probe failed with statuscode: 503", time.Minute)},
			short: "Crashing", has: []string{"startup check", "answered 503"}, never: []string{"alive check", "keeps stopping"}},
		{name: "exits 0 by itself while its startup check still fails (normal as it starts)", pod: mxPod("pus", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 5, last: mxEnd("Completed", 0, time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("u", "pus", "app", "Unhealthy", "Startup probe failed: HTTP probe failed with statuscode: 503", time.Minute)},
			short:  "Restarting", has: []string{"keeps stopping"}, never: []string{"check"}},
		{name: "between restarts: terminated now", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxStopped("Error", 1, 40*time.Second), restarts: 7, last: mxEnd("Error", 1, 4*time.Minute)}),
			short: "Crashing", has: []string{"7 restarts", "40s ago"}},
		{name: "first stop, no restart yet", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxStopped("Error", 1, 5*time.Second)}),
			short: "Not ready", has: []string{`"app"`}, never: []string{"healthy", "keeps"}},
		{name: "crashed 30m ago, not ready, running for 30m", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(30 * time.Minute), restarts: 2, last: mxEnd("Error", 1, 30*time.Minute)}),
			short: "Not ready", never: []string{"crash"}},

		// Can't start.
		{name: "start error", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 3,
				last: &corev1.ContainerStateTerminated{Reason: "StartError", ExitCode: 128, FinishedAt: mxAgo(time.Minute),
					Message: `failed to create containerd task: exec: "/app/run.sh": stat /app/run.sh: no such file or directory: unknown`}}),
			short: "Can't start", has: []string{"/app/run.sh isn't in the image"}},
		{name: "run error on the first try, nothing before", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "app", limit: lim, state: mxWait("RunContainerError",
				`failed to create containerd task: exec: "/app/run.sh": stat /app/run.sh: no such file or directory: unknown`)}),
			short: "Can't start", has: []string{"/app/run.sh isn't in the image"}, never: []string{"keeps crashing", "0 restarts", "0 tries", "logs"}},
		{name: "image pull", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "app", limit: lim, state: mxWait("ErrImagePull", "rpc error: manifest unknown")}),
			short: "Can't pull image", has: []string{"app", "img/app:1", "doesn't exist"}},
		{name: "image pull back-off", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "app", limit: lim, state: mxWait("ImagePullBackOff", `Back-off pulling image "img/app:1"`)}),
			short: "Can't pull image", has: []string{"retrying"}},
		{name: "bad config", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "app", limit: lim, state: mxWait("CreateContainerConfigError", `secret "db" not found`)}),
			short: "Bad config", has: []string{`secret "db" not found`}},

		// Not ready and starting.
		{name: "running 30s, not ready", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(30 * time.Second)}),
			short: "Starting", has: []string{"app"}},
		{name: "running 10m, not ready, no events", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute)}),
			short: "Not ready", has: []string{"not ready for traffic"}},
		{name: "not ready, ready check fails now", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("u", "p", "app", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503", 20*time.Second)},
			short:  "Not ready", has: []string{"ready check failed", "503"}},
		{name: "not ready, the failing check is another container's", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute)}, ok("proxy")),
			events: []*unstructured.Unstructured{mxEvent("u", "p", "proxy", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 500", 20*time.Second)},
			short:  "Not ready", has: []string{`app`}, never: []string{"500"}},
		{name: "not ready, the failure is from before this run", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute), restarts: 1, last: mxEnd("Error", 1, 11*time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("u", "p", "app", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503", 40*time.Minute)},
			short:  "Not ready", never: []string{"503"}},
		{name: "not ready, only an alive check failed", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(10 * time.Minute)}),
			events: []*unstructured.Unstructured{mxEvent("u", "p", "app", "Unhealthy", "Liveness probe failed: HTTP probe failed with statuscode: 500", 20*time.Second)},
			short:  "Not ready", never: []string{"ready check failed"}},
		{name: "containers ready, a readiness gate isn't", pod: withCond(gate(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), "target-health.elbv2.k8s.aws/tg"),
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			corev1.PodCondition{Type: "target-health.elbv2.k8s.aws/tg", Status: corev1.ConditionFalse}),
			short: "Not ready", has: []string{"target-health.elbv2.k8s.aws/tg"}, never: []string{"healthy"}},
		{name: "a load balancer's gate pending 30s after the containers became ready: starting", pod: withCond(gate(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), "target-health.elbv2.k8s.aws/tg"),
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: mxAgo(30 * time.Second)}),
			short: "Starting", has: []string{"target-health.elbv2.k8s.aws/tg", "load balancer"}, never: []string{"healthy", "hasn't passed"}},
		{name: "a load balancer's gate pending 10m: not ready", pod: withCond(gate(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), "target-health.elbv2.k8s.aws/tg"),
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: mxAgo(10 * time.Minute)}),
			short: "Not ready", has: []string{"hasn't passed after 10m"}},
		{name: "containers last said ready, node stopped reporting", pod: onNode(withCond(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")),
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})),
			nodes: []*unstructured.Unstructured{notReadyNode},
			short: "Not ready", has: []string{"node"}, never: []string{"healthy"}},
		{name: "phase Unknown", pod: mxPod("p", "ReplicaSet", corev1.PodUnknown, ok("app")),
			short: "Unknown", never: []string{"healthy"}},

		// Pending, setup steps.
		{name: "waiting for a node", pod: withCond(mxPod("p", "ReplicaSet", corev1.PodPending, mxC{name: "app", limit: lim}),
			corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Message: "0/3 nodes are available: 3 Insufficient cpu."}),
			short: "Waiting for a node", has: []string{"CPU"}},
		{name: "creating containers", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "app", limit: lim, state: mxWait("ContainerCreating", "")}),
			short: "Starting"},
		{name: "setup step running for 20m", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "wait-db", kind: "init", state: mxRun(20 * time.Minute)},
			mxC{name: "app", limit: lim, state: mxWait("PodInitializing", "")}),
			short: "Starting", has: []string{`"wait-db"`, "20m"}, never: []string{"pulling images"}},
		{name: "setup step crash looping", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "migrate", kind: "init", state: mxWait("CrashLoopBackOff", ""), restarts: 4, last: mxEnd("Error", 2, time.Minute)},
			mxC{name: "app", limit: lim, state: mxWait("PodInitializing", "")}),
			short: "Crashing", has: []string{`"migrate"`, "4 restarts", "exit code 2"}},
		{name: "setup step failed just now", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "migrate", kind: "init", state: mxStopped("Error", 2, 10*time.Second), restarts: 1, last: mxEnd("Error", 2, 2*time.Minute)},
			mxC{name: "app", limit: lim, state: mxWait("PodInitializing", "")}),
			short: "Crashing", has: []string{`"migrate"`, "10s ago"}},
		{name: "setup step out of memory, its own limit", pod: mxPod("p", "ReplicaSet", corev1.PodPending,
			mxC{name: "migrate", kind: "init", limit: "64Mi", state: mxWait("CrashLoopBackOff", ""), restarts: 4, last: mxEnd("OOMKilled", 137, time.Minute)},
			mxC{name: "app", limit: lim, state: mxWait("PodInitializing", "")}),
			short: "Out of memory", has: []string{"migrate", "64Mi"}, never: []string{"none set"}},

		// Native sidecars.
		{name: "sidecar crash looping", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", limit: "32Mi", state: mxWait("CrashLoopBackOff", ""), restarts: 6, last: mxEnd("Error", 1, time.Minute)},
			ok("app")),
			short: "Crashing", has: []string{`"proxy"`, "6 restarts"}},
		{name: "sidecar out of memory, its own limit", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", limit: "32Mi", state: mxWait("CrashLoopBackOff", ""), restarts: 6, last: mxEnd("OOMKilled", 137, time.Minute)},
			ok("app")),
			short: "Out of memory", has: []string{"proxy", "32Mi"}, never: []string{"none set"}},
		{name: "sidecar between restarts", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", state: mxStopped("Error", 1, 20*time.Second), restarts: 6, last: mxEnd("Error", 1, 3*time.Minute)},
			ok("app")),
			short: "Crashing", has: []string{`"proxy"`}, never: []string{"healthy"}},
		{name: "sidecar restarted 4 times, last 20m ago", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", state: mxRun(20 * time.Minute), ready: true, restarts: 4, last: mxEnd("Error", 1, 20*time.Minute)},
			ok("app")),
			short: "Restarting", has: []string{`"proxy"`, "4 times"}},
		{name: "sidecar restarted once, settled", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", state: mxRun(5 * time.Hour), ready: true, restarts: 1, last: mxEnd("Error", 1, 5*time.Hour)},
			ok("app")),
			short: "Running", has: []string{"1 restart in total", "5h"}},
		{name: "sidecar not ready", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", state: mxRun(10 * time.Minute)}, ok("app")),
			short: "Not ready", has: []string{"proxy"}, never: []string{"healthy"}},

		// Several containers.
		{name: "one crashing, one fine", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			ok("web"), mxC{name: "worker", limit: lim, state: mxWait("CrashLoopBackOff", ""), restarts: 8, last: mxEnd("Error", 1, time.Minute)}),
			short: "Crashing", has: []string{`"worker"`, "8 restarts"}, never: []string{`"web"`}},
		{name: "an old OOM in the first, crash looping in the second", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "a", limit: lim, state: mxRun(3 * time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, 3*time.Hour)},
			mxC{name: "b", limit: lim, state: mxRun(time.Minute), ready: true, restarts: 5, last: mxEnd("Error", 1, time.Minute)}),
			short: "Crashing", has: []string{`"b"`}},
		{name: "restarting first, out of memory second", pod: mxPod("p", "ReplicaSet", corev1.PodRunning,
			mxC{name: "a", limit: lim, state: mxRun(time.Hour), ready: true, restarts: 3, last: mxEnd("Error", 1, time.Hour)},
			mxC{name: "b", limit: "128Mi", state: mxRun(10 * time.Minute), ready: true, restarts: 4, last: mxEnd("OOMKilled", 137, 10*time.Minute)}),
			short: "Out of memory", has: []string{"b", "128Mi"}},

		// Finished pods.
		{name: "succeeded", pod: mxPod("p", "Job", corev1.PodSucceeded, mxC{name: "app", state: mxStopped("Completed", 0, time.Minute)}),
			short: "Completed"},
		{name: "failed, exit 2", pod: mxPod("p", "Job", corev1.PodFailed, mxC{name: "app", state: mxStopped("Error", 2, time.Minute)}),
			short: "Failed", has: []string{`"app"`, "exit 2"}},
		{name: "failed, OOM", pod: mxPod("p", "Job", corev1.PodFailed, mxC{name: "app", state: mxStopped("OOMKilled", 137, time.Minute)}),
			short: "Failed", has: []string{"ran out of memory"}},
		{name: "failed, never started", pod: mxPod("p", "Job", corev1.PodFailed, mxC{name: "app",
			state: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "StartError", ExitCode: 128, Message: `exec: "x": executable file not found in $PATH`}}}),
			short: "Can't start"},
		{name: "failed setup step", pod: mxPod("p", "Job", corev1.PodFailed,
			mxC{name: "migrate", kind: "init", state: mxStopped("Error", 3, time.Minute)}, mxC{name: "app", state: mxWait("PodInitializing", "")}),
			short: "Failed", has: []string{`setup step "migrate"`, "never ran"}},
		{name: "evicted", pod: func() *corev1.Pod {
			p := mxPod("p", "ReplicaSet", corev1.PodFailed, mxC{name: "app"})
			p.Status.Reason, p.Status.Message = "Evicted", "The node was low on resource: memory."
			return p
		}(), short: "Evicted", has: []string{"low on resource: memory"}},
		{name: "node shut down", pod: func() *corev1.Pod {
			p := mxPod("p", "ReplicaSet", corev1.PodFailed, mxC{name: "app", state: mxStopped("Error", 143, time.Minute)})
			p.Status.Reason, p.Status.Message = "Terminated", "Pod was terminated in response to imminent node shutdown."
			return p
		}(), short: "Failed", has: []string{"143"}},
		// Review of round 126: the app exited 0 on the shutdown, and it had
		// restarted before: an ended pod is never started again.
		{name: "node shut down, clean exit after restarts", pod: func() *corev1.Pod {
			p := mxPod("p", "ReplicaSet", corev1.PodFailed, mxC{name: "app", state: mxStopped("Completed", 0, time.Minute), restarts: 2})
			p.Status.Reason, p.Status.Message = "Terminated", "Pod was terminated in response to imminent node shutdown."
			return p
		}(), short: "Failed", never: []string{"keeps stopping", "Crashing"}},

		// Jobs.
		{name: "job run in progress", pod: mxPod("p", "Job", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Minute), ready: true}),
			short: "Running", has: []string{"run in progress"}},
		{name: "job run not ready is not flagged", pod: mxPod("p", "Job", corev1.PodRunning, mxC{name: "app", state: mxRun(10 * time.Minute)}),
			short: "Running", never: []string{"not ready"}},
		{name: "job (OnFailure) running out of memory again and again", pod: onFailure(mxPod("p", "Job", corev1.PodRunning,
			mxC{name: "app", limit: lim, state: mxRun(time.Minute), ready: true, restarts: 4, last: mxEnd("OOMKilled", 137, time.Minute)})),
			short: "Out of memory", has: []string{"256Mi"}},
		{name: "job's work done, a native sidecar that restarted is being stopped", pod: mxPod("p", "Job", corev1.PodRunning,
			mxC{name: "proxy", kind: "side", state: mxRun(time.Minute), ready: true, restarts: 4, last: mxEnd("Error", 1, 2*time.Minute)},
			mxC{name: "main", state: mxStopped("Completed", 0, 5*time.Second)}),
			short: "Running", never: []string{"crash"}},
		{name: "job main failed, regular sidecar still running", pod: mxPod("p", "Job", corev1.PodRunning,
			mxC{name: "main", state: mxStopped("Error", 1, time.Minute)}, mxC{name: "proxy", state: mxRun(5 * time.Minute), ready: true}),
			short: "Failed", has: []string{`"main"`, "proxy", "keeps the pod running"}},

		// Deletion.
		{name: "stopping within its grace period (deployment): why isn't known", pod: deleting(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), -20*time.Second, grace30),
			short: "Stopping", never: []string{"replaced", "scaled down"}},
		{name: "stopping, a bare pod: nothing replaces it", pod: deleting(mxPod("p", "", corev1.PodRunning, ok("app")), -20*time.Second, grace30),
			short: "Stopping", never: []string{"replaced", "scaled down"}},
		{name: "stopping, a daemon set's", pod: deleting(mxPod("p", "DaemonSet", corev1.PodRunning, ok("app")), -20*time.Second, grace30),
			short: "Stopping", never: []string{"scaled down"}},
		{name: "stopping, a stateful set's", pod: deleting(mxPod("p", "StatefulSet", corev1.PodRunning, ok("app")), -20*time.Second, grace30),
			short: "Stopping"},
		{name: "stopping, a job's run", pod: deleting(mxPod("p", "Job", corev1.PodRunning, ok("app")), -20*time.Second, grace30),
			short: "Stopping", never: []string{"scaled down"}},
		{name: "past its deadline by 1m: not stuck yet", pod: deleting(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), time.Minute, grace30),
			short: "Stopping"},
		{name: "past a 10m grace by 6m: stuck, asked 16m ago", pod: deleting(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), 6*time.Minute, 600),
			short: "Stuck stopping", has: []string{"16m ago"}},
		{name: "stuck with a finalizer", pod: deleting(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), 10*time.Minute, grace30, "example.com/hold"),
			short: "Stuck stopping", has: []string{"example.com/hold"}},
		{name: "stuck, no finalizer", pod: deleting(mxPod("p", "ReplicaSet", corev1.PodRunning, ok("app")), 10*time.Minute, grace30),
			short: "Stuck stopping", never: []string{"finalizer"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := mxEnricher(map[string][]*unstructured.Unstructured{"events": c.events, "nodes": c.nodes})
			st := Explain(e, c.pod)
			if st.Short != c.short {
				t.Errorf("short %q (%q), want %q", st.Short, st.Why, c.short)
			}
			for _, w := range c.has {
				if !strings.Contains(st.Why, w) {
					t.Errorf("why %q lacks %q", st.Why, w)
				}
			}
			for _, w := range c.never {
				if strings.Contains(st.Why, w) {
					t.Errorf("why %q must not say %q", st.Why, w)
				}
			}
		})
	}
}

// The pods list's RESTARTS cell and the status words count the same
// restarts, native sidecars' included.
func TestMatrixPodRowAgreesWithStatus(t *testing.T) {
	e := mxEnricher(nil)
	p := mxPod("p", "ReplicaSet", corev1.PodRunning,
		mxC{name: "proxy", kind: "side", state: mxRun(5 * time.Hour), ready: true, restarts: 2, last: mxEnd("Error", 1, 5*time.Hour)},
		mxC{name: "app", state: mxRun(5 * time.Hour), ready: true, restarts: 1, last: mxEnd("Error", 1, 6*time.Hour)})
	row, _ := podRow(e, mxObj(p))
	if !strings.HasPrefix(row[3], "3 ") {
		t.Errorf("restarts cell %q", row[3])
	}
	if st := Explain(e, p); !strings.Contains(st.Why, "3 restarts in total") {
		t.Errorf("status %q / %q disagrees with the row's %q", st.Short, st.Why, row[3])
	}
	// The about box lists the sidecar's restart too.
	if lines := strings.Join(podDetail(e, mxObj(p)), "\n"); !strings.Contains(lines, `container "proxy"`) {
		t.Errorf("detail lacks the sidecar's restart:\n%s", lines)
	}
}

// In an app, the headline, its pod, the Error line's pod and container
// agree, whatever order the pods come in.
func TestMatrixAppHeadline(t *testing.T) {
	crash := func(name, container string, restarts int32, ago time.Duration) *corev1.Pod {
		return mxPod(name, "ReplicaSet", corev1.PodRunning, mxC{name: container, limit: "256Mi", state: mxWait("CrashLoopBackOff", ""), restarts: restarts, last: mxEnd("Error", 1, ago)})
	}
	healthy := mxPod("ok-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
	cases := []struct {
		name      string
		pods      []*corev1.Pod
		short     string
		head      string // the pod the headline is about
		crashPod  string // the Error line's pod ("" any)
		container string // and container
		ready     int    // READY's count, when it matters
	}{
		{name: "two copies crash alike: the one crashed most", pods: []*corev1.Pod{crash("c-1", "app", 5, time.Minute), crash("c-2", "app", 9, time.Minute)},
			short: "Crashing", head: "c-2", crashPod: "c-2", container: "app"},
		{name: "a crash loop beats a healthy copy", pods: []*corev1.Pod{healthy, crash("c-1", "app", 5, time.Minute)},
			short: "Crashing", head: "c-1", crashPod: "c-1", container: "app"},
		{name: "a crash loop now beats an OOM an hour ago", pods: []*corev1.Pod{
			mxPod("m-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", limit: "256Mi", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, time.Hour)}),
			crash("c-1", "app", 5, time.Minute)},
			short: "Crashing", head: "c-1", crashPod: "c-1", container: "app"},
		{name: "not ready now beats an OOM an hour ago", pods: []*corev1.Pod{
			mxPod("m-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", limit: "256Mi", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, time.Hour)}),
			mxPod("n-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour)})},
			short: "Not ready", head: "n-1"},
		{name: "two restarting: the one that restarted last", pods: []*corev1.Pod{
			mxPod("r-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true, restarts: 3, last: mxEnd("Error", 1, time.Hour)}),
			mxPod("r-2", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(20 * time.Minute), ready: true, restarts: 3, last: mxEnd("Error", 1, 20*time.Minute)})},
			short: "Restarting", head: "r-2"},
		{name: "the crashing container in a two-container pod", pods: []*corev1.Pod{
			mxPod("c-1", "ReplicaSet", corev1.PodRunning,
				mxC{name: "web", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("Error", 1, time.Hour)},
				mxC{name: "worker", state: mxWait("CrashLoopBackOff", ""), restarts: 4, last: mxEnd("Error", 1, time.Minute)})},
			short: "Crashing", head: "c-1", crashPod: "c-1", container: "worker"},
		{name: "an OOM headline, another pod's crash quoted by its crashing container", pods: []*corev1.Pod{
			mxPod("m-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", limit: "256Mi", state: mxWait("CrashLoopBackOff", ""), restarts: 9, last: mxEnd("OOMKilled", 137, time.Minute)}),
			mxPod("c-1", "ReplicaSet", corev1.PodRunning,
				mxC{name: "web", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("Error", 1, time.Hour)},
				mxC{name: "worker", state: mxWait("CrashLoopBackOff", ""), restarts: 4, last: mxEnd("Error", 1, time.Minute)})},
			crashPod: "", container: "worker"},
		{name: "a copy its node stopped reporting for", pods: []*corev1.Pod{healthy, func() *corev1.Pod {
			p := mxPod("lost-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			return p
		}()}, short: "Not ready", head: "lost-1", ready: 1},
		{name: "two sidecars restarting: the one that restarted last", pods: []*corev1.Pod{
			mxPod("s-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true}, mxC{name: "proxy", kind: "side", state: mxRun(time.Hour), ready: true, restarts: 3, last: mxEnd("Error", 1, time.Hour)}),
			mxPod("s-2", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true}, mxC{name: "proxy", kind: "side", state: mxRun(20 * time.Minute), ready: true, restarts: 3, last: mxEnd("Error", 1, 20*time.Minute)})},
			short: "Restarting", head: "s-2"},
	}
	for _, c := range cases {
		for _, reverse := range []bool{false, true} {
			t.Run(c.name+map[bool]string{false: "", true: " (reversed)"}[reverse], func(t *testing.T) {
				var objs []*unstructured.Unstructured
				for i := range c.pods {
					p := c.pods[i]
					if reverse {
						p = c.pods[len(c.pods)-1-i]
					}
					objs = append(objs, mxObj(p))
				}
				var a *App
				for _, x := range Apps(mxEnricher(nil), objs, nil, nil, nil, nil) {
					if x.Name == "mx" {
						a = x
					}
				}
				if a == nil {
					t.Fatal("no app mx")
				}
				if c.short != "" && (a.Status.Short != c.short || a.HeadPod != c.head) {
					t.Errorf("headline %q (%q) about %q, want %q about %q", a.Status.Short, a.Status.Why, a.HeadPod, c.short, c.head)
				}
				if c.ready > 0 && a.Ready != c.ready {
					t.Errorf("READY counts %d, want %d", a.Ready, c.ready)
				}
				if c.crashPod != "" && (a.CrashPod != c.crashPod || a.CrashContainer != c.container) || a.CrashPod == "c-1" && a.CrashContainer != c.container {
					t.Errorf("Error line from %s/%s, want %s/%s", a.CrashPod, a.CrashContainer, c.crashPod, c.container)
				}
				if m := quotedContainer.FindStringSubmatch(a.Status.Why); m != nil && strings.HasPrefix(a.Status.Short, "Crashing") && (a.CrashPod != a.HeadPod || a.CrashContainer != m[1]) {
					t.Errorf("headline names %q in %s, Error line %s/%s", m[1], a.HeadPod, a.CrashPod, a.CrashContainer)
				}
			})
		}
	}
}

// A stateful set's box names the pod the app's headline is about, with
// the same words.
func TestMatrixWorkloadBoxAgreesWithHeadline(t *testing.T) {
	sts := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"name": "mx-owner", "namespace": "mxa", "uid": "sts-uid"}}}
	oomOld := func(name string) *corev1.Pod {
		return mxPod(name, "StatefulSet", corev1.PodRunning, mxC{name: "app", limit: "256Mi", state: mxRun(time.Hour), ready: true, restarts: 1, last: mxEnd("OOMKilled", 137, time.Hour)})
	}
	notReady := func(name string) *corev1.Pod {
		return mxPod(name, "StatefulSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour)})
	}
	crash := func(name string, restarts int32) *corev1.Pod {
		return mxPod(name, "StatefulSet", corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", ""), restarts: restarts, last: mxEnd("Error", 1, time.Minute)})
	}
	for _, pods := range [][]*corev1.Pod{
		{oomOld("mx-0"), notReady("mx-1")},
		{notReady("mx-0"), oomOld("mx-1")},
		{crash("mx-0", 3), crash("mx-1", 9)},
		{crash("mx-0", 9), crash("mx-1", 3)},
	} {
		var objs []*unstructured.Unstructured
		for _, p := range pods {
			p.OwnerReferences[0].UID = "sts-uid"
			objs = append(objs, mxObj(p))
		}
		s := listStore{kube.Pods: objs, kube.StatefulSets: {sts}}
		e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{
			{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true}}), nil)
		var a *App
		for _, x := range Apps(e, objs, nil, nil, nil, nil) {
			if x.Name == "mx" {
				a = x
			}
		}
		if a == nil {
			t.Fatal("no app mx")
		}
		box := strings.Join(workloadProblem(e, sts), "\n")
		if !strings.Contains(box, "pod "+a.HeadPod+": "+a.Status.Short) {
			t.Errorf("headline %q about %s, box:\n%s", a.Status.Short, a.HeadPod, box)
		}
	}
}

// A pod behind a load balancer's readiness gate starts as a copy that
// is starting, not a problem; the Fix lines for a pod kept out of traffic
// though no probe fails don't send you to its probes; a container exiting
// 0 again and again gets the same advice on home and on the pod.
func TestMatrixHeldBackAndStoppingAdvice(t *testing.T) {
	e := mxEnricher(nil)
	gated := mxPod("g-1", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Minute), ready: true})
	gated.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "target-health.elbv2.k8s.aws/tg"}}
	gated.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: mxAgo(20 * time.Second)}}
	other := mxPod("g-0", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true}) // with none ready, any start is worth a look
	for _, a := range Apps(e, []*unstructured.Unstructured{mxObj(other), mxObj(gated)}, nil, nil, nil, nil) {
		if a.Name == "mx" && (a.Status.Tone != ToneNormal || a.starting != 1) {
			t.Errorf("a gate pending on a rollout: %+v, starting %d", a.Status, a.starting)
		}
	}
	for _, why := range []string{
		"its containers are ready, but its readiness gate target-health.elbv2.k8s.aws/tg (its load balancer's health check) hasn't passed after 10m",
		"its containers are ready, but its readiness gate example.com/x hasn't passed after 10m",
		"its node ip-1 isn't reporting, so Kubernetes took it out of traffic",
	} {
		st := Status{"Not ready", why, ToneWarn}
		for where, fix := range map[string]string{"pod": podFixFor(st, "ReplicaSet"), "app": appFix(&App{Status: st, Pods: 1}, false)} {
			if strings.Contains(fix, "probe") || fix == "" {
				t.Errorf("%s fix for %q: %q", where, why, fix)
			}
		}
	}
	if f := podFixFor(Status{"Not ready", "its node ip-1 isn't reporting, so Kubernetes took it out of traffic", ToneWarn}, "ReplicaSet"); !strings.Contains(f, "gn") {
		t.Errorf("lost node fix %q", f)
	}
	st := Explain(e, mxPod("z-1", "ReplicaSet", corev1.PodRunning,
		mxC{name: "app", state: mxWait("CrashLoopBackOff", ""), restarts: 6, last: mxEnd("Completed", 0, time.Minute)}))
	pod, app := podFixFor(st, "ReplicaSet"), appFix(&App{Status: st, Pods: 1}, false)
	if pod != app || strings.Contains(app, "crash") {
		t.Errorf("exit 0 advice: pod %q, app %q", pod, app)
	}
}

// Review of area A: kept out of traffic with no known cause (nodes still
// loading, a gate just passed) isn't a lost node: the advice doesn't say so.
func TestHeldBackUnknownCauseAdvice(t *testing.T) {
	fix := heldBackFix("Kubernetes keeps it out of traffic, though its containers say they are ready")
	if strings.Contains(fix, "unreachable") || fix == "" {
		t.Errorf("%q", fix)
	}
	if fix := heldBackFix("its node ip-1 isn't reporting, so Kubernetes took it out of traffic"); !strings.Contains(fix, "gn") || !strings.Contains(fix, "unreachable") {
		t.Errorf("lost node: %q", fix)
	}
}

// QA round 126: a container that exits 0 stays stopped (not waiting) for
// its whole back-off on newer Kubernetes. Home, the pod and its box say
// it keeps stopping, with the time of the stop now, and the same advice;
// the box's Restart line doesn't say it was started again while it waits.
func TestMatrixExitZeroStoppedNow(t *testing.T) {
	e := mxEnricher(nil)
	for _, waiting := range []bool{false, true} {
		c := mxC{name: "app", state: mxStopped("Completed", 0, time.Minute), restarts: 7, last: mxEnd("Completed", 0, 6*time.Minute)}
		if waiting {
			c.state, c.last = mxWait("CrashLoopBackOff", ""), mxEnd("Completed", 0, time.Minute)
		}
		p := mxPod("x-1", "ReplicaSet", corev1.PodRunning, c)
		var a *App
		for _, x := range Apps(e, []*unstructured.Unstructured{mxObj(p)}, nil, nil, nil, nil) {
			if x.Name == "mx" {
				a = x
			}
		}
		if a == nil || !strings.Contains(a.Status.Why, "keeps stopping") || !strings.Contains(a.Status.Why, "1m ago") || appFix(a, false) != stoppingFix {
			t.Errorf("waiting %v: home %+v, fix %q", waiting, a.Status, appFix(a, false))
		}
		box := strings.Join(podDetail(e, mxObj(p)), "\n")
		if !strings.Contains(box, "Fix      "+stoppingFix) || !strings.Contains(box, `container "app" finished, 1m ago; waits to be started again`) || strings.Contains(box, "was started again") {
			t.Errorf("waiting %v: box\n%s", waiting, box)
		}
	}
}

// QA round 127: a container that exits 0 every few seconds has one status
// word, Restarting (worth a look, not a crash), whether it is stopped now,
// waiting or running again: home's STATUS, the pods list and the pod box
// agree and don't flip with each refresh.
func TestMatrixExitZeroOneWordEveryPhase(t *testing.T) {
	e := mxEnricher(nil)
	phases := map[string]mxC{
		"stopped": {name: "main", state: mxStopped("Completed", 0, 10*time.Second), restarts: 7, last: mxEnd("Completed", 0, 40*time.Second)},
		"waiting": {name: "main", state: mxWait("CrashLoopBackOff", ""), restarts: 7, last: mxEnd("Completed", 0, 10*time.Second)},
		"running": {name: "main", state: mxRun(5 * time.Second), ready: true, restarts: 7, last: mxEnd("Completed", 0, 10*time.Second)},
	}
	for phase, c := range phases {
		p := mxPod("exiter", "ReplicaSet", corev1.PodRunning, c)
		st := Explain(e, p)
		if st.Short != "Restarting" || st.Tone != ToneWarn {
			t.Errorf("%s: %q (tone %v): %q", phase, st.Short, st.Tone, st.Why)
		}
		if row, _ := podRow(e, mxObj(p)); row[2] != "Restarting" {
			t.Errorf("%s: pods list says %q", phase, row[2])
		}
		if box := strings.Join(podDetail(e, mxObj(p)), "\n"); !strings.Contains(box, "Problem  Restarting:") || strings.Contains(box, "Crashing") {
			t.Errorf("%s: box\n%s", phase, box)
		}
		for _, a := range Apps(e, []*unstructured.Unstructured{mxObj(p)}, nil, nil, nil, nil) {
			if a.Name == "mx" && (a.Status.Short != "Restarting" || a.HeadPod != "exiter" || appFix(a, false) != stoppingFix) {
				t.Errorf("%s: home %+v about %q, fix %q", phase, a.Status, a.HeadPod, appFix(a, false))
			}
		}
	}
}

// QA round 127: a stateful set scaled to 0 stops its pods one at a time;
// while one still runs (ready) and another shuts down, the app is
// stopping, not "Running: healthy". A deployment scaled to 0 the same. A
// rolling update that keeps copies is untouched.
func TestMatrixScaledToZeroIsStopping(t *testing.T) {
	workload := func(kind string, replicas int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": kind,
			"metadata": map[string]any{"name": "mx-owner", "namespace": "mxa", "uid": "w-uid"},
			"spec": map[string]any{"replicas": replicas,
				"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}}}}
	}
	running := func(name, kind string) *corev1.Pod {
		p := mxPod(name, kind, corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
		p.OwnerReferences[0].UID = "w-uid"
		return p
	}
	stopping := func(name, kind string) *corev1.Pod {
		p := running(name, kind)
		at, grace := mxAgo(-20*time.Second), int64(30)
		p.DeletionTimestamp, p.DeletionGracePeriodSeconds = &at, &grace
		return p
	}
	cases := []struct {
		name     string
		w        *unstructured.Unstructured
		pods     []*corev1.Pod
		short    string
		has      string
		notShort string
	}{
		{"stateful set at 0: one running, one stopping", workload("StatefulSet", 0),
			[]*corev1.Pod{running("store-0", "StatefulSet"), stopping("store-1", "StatefulSet")}, "Stopping", "1 pod still running, 1 pod shutting down", ""},
		{"stateful set at 0: the last one stopping", workload("StatefulSet", 0),
			[]*corev1.Pod{stopping("store-0", "StatefulSet")}, "Stopping", "asks for 0 copies", ""},
		{"deployment at 0, its pods not yet marked", workload("Deployment", 0),
			[]*corev1.Pod{running("web-1", "Deployment")}, "Stopping", "1 pod still running", ""},
		{"stateful set rolling update keeps copies", workload("StatefulSet", 2),
			[]*corev1.Pod{running("store-0", "StatefulSet"), stopping("store-1", "StatefulSet")}, "", "", "Stopping"},
	}
	for _, c := range cases {
		var objs []*unstructured.Unstructured
		for _, p := range c.pods {
			objs = append(objs, mxObj(p))
		}
		s := listStore{kube.Pods: objs, kube.StatefulSets: {c.w}, kube.Deployments: {c.w}}
		if c.w.GetKind() == "StatefulSet" {
			s[kube.Deployments] = nil
		} else {
			s[kube.StatefulSets] = nil
		}
		e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
			{GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true}, {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}}), nil)
		var a *App
		for _, x := range Apps(e, objs, nil, nil, nil, []*unstructured.Unstructured{c.w}) {
			if x.Name == "mx" {
				a = x
			}
		}
		if a == nil {
			t.Fatalf("%s: no app", c.name)
		}
		if c.short != "" && (a.Status.Short != c.short || !strings.Contains(a.Status.Why, c.has)) {
			t.Errorf("%s: %q %q", c.name, a.Status.Short, a.Status.Why)
		}
		if c.notShort != "" && a.Status.Short == c.notShort {
			t.Errorf("%s: %q %q", c.name, a.Status.Short, a.Status.Why)
		}
	}
}

// Review of round 127: an app with one deployment scaled to 0 (its pod
// shutting down) and another that asks for 2 copies (none made yet)
// still asks for copies: its pods are being replaced, not going away. A
// deployment deleted with its pods orphaned leaves them running: it is
// not stopping either.
func TestMatrixGoingAwayOnlyWhenNothingAsked(t *testing.T) {
	workload := func(name, uid string, replicas int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": name, "namespace": "mxa", "uid": uid},
			"spec": map[string]any{"replicas": replicas,
				"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}}}}
	}
	pod := func(name, owner, uid string, stopping bool) *corev1.Pod {
		p := mxPod(name, "Deployment", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: true})
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = owner, types.UID(uid)
		if stopping {
			at, grace := mxAgo(-20*time.Second), int64(30)
			p.DeletionTimestamp, p.DeletionGracePeriodSeconds = &at, &grace
		}
		return p
	}
	orphaned := workload("web", "a-uid", 1)
	orphaned.SetDeletionTimestamp(&metav1.Time{Time: time.Now().Add(-time.Minute)})
	orphaned.SetFinalizers([]string{"orphan"})
	cases := []struct {
		name     string
		ws       []*unstructured.Unstructured
		pods     []*corev1.Pod
		short    string
		has      string
		notShort string
	}{
		{"one at 0 stopping, another asks for 2", []*unstructured.Unstructured{workload("web-old", "a-uid", 0), workload("web", "b-uid", 2)},
			[]*corev1.Pod{pod("web-old-1", "web-old", "a-uid", true)}, "Replacing a pod", "it asks for 2 copies", ""},
		{"deleted with its pods orphaned", []*unstructured.Unstructured{orphaned},
			[]*corev1.Pod{pod("web-1", "web", "a-uid", false)}, "", "", "Stopping"},
	}
	for _, c := range cases {
		var objs []*unstructured.Unstructured
		for _, p := range c.pods {
			objs = append(objs, mxObj(p))
		}
		s := listStore{kube.Pods: objs, kube.Deployments: c.ws}
		e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
			{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}}), nil)
		var a *App
		for _, x := range Apps(e, objs, nil, nil, nil, c.ws) {
			if x.Name == "mx" {
				a = x
			}
		}
		if a == nil {
			t.Fatalf("%s: no app", c.name)
		}
		if c.short != "" && (a.Status.Short != c.short || !strings.Contains(a.Status.Why, c.has)) {
			t.Errorf("%s: %q %q", c.name, a.Status.Short, a.Status.Why)
		}
		if c.notShort != "" && a.Status.Short == c.notShort {
			t.Errorf("%s: %q %q", c.name, a.Status.Short, a.Status.Why)
		}
	}
}

// Review of round 127: a container killed by its startup check gets that
// check's fix, on its pod and on its app, not the alive check's or a
// crash's.
func TestStartupCheckKillFix(t *testing.T) {
	st := Status{"Crashing", `container "app" is killed by its startup check (answered 503); 5 restarts, the last 1m ago`, ToneBad}
	if fix := podFixWords(st, "ReplicaSet"); !strings.Contains(fix, "startup check") || strings.Contains(fix, "alive check") {
		t.Errorf("pod: %q", fix)
	}
	if fix := appFixFor(&App{Status: st}, false); !strings.Contains(fix, "startup check") || strings.Contains(fix, "alive check") {
		t.Errorf("app: %q", fix)
	}
}

// Second review of round 127: an app whose containers were killed by both
// checks gets advice for both, not only the alive check's (or a crash's).
func TestBothChecksKillFix(t *testing.T) {
	for _, st := range []Status{
		{"Restarting", "3 of its containers were killed by their alive check or startup check in the last day, the last 5m ago; :changes shows when", ToneWarn},
		{"Crashing", `container "app" is killed by its alive check or startup check; 5 restarts`, ToneBad},
	} {
		fix := appFixFor(&App{Status: st}, false)
		if !strings.Contains(fix, "alive check") || !strings.Contains(fix, "startup check") {
			t.Errorf("%s: %q", st.Short, fix)
		}
	}
}
