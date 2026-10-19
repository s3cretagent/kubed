package view

import (
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 146: an ingress whose TLS secret doesn't exist showed no TLS
// problem when :ing was opened directly: the check waits for the
// namespace's Secrets, which the ingress list didn't load.
// Review: loading every Secret for that (values and all) is heavy; it
// asks for the names it needs only, without values (the ui's TLSSecret).
func TestIngressListChecksTLSSecretsByName(t *testing.T) {
	if needs := For(kube.Resource{GVR: kube.Ingresses}).Needs; slices.Contains(needs, kube.Secrets) {
		t.Errorf("ingress list loads every Secret: %v", needs)
	}
	ing := r132Obj("networking.k8s.io/v1", "Ingress", "web", map[string]any{"spec": map[string]any{"ingressClassName": "alb",
		"tls": []any{map[string]any{"secretName": "gone-tls"}, map[string]any{"secretName": "kept-tls"}, map[string]any{"secretName": "hidden-tls"}}}})
	e := enrich.New(notSynced{mxStore{listStore{}}, []schema.GroupVersionResource{kube.Secrets}}, nil, nil)
	defer func(f func(ns, name string) (bool, bool)) { TLSSecret = f }(TLSSecret)
	var asked []string
	TLSSecret = func(ns, name string) (exists, known bool) {
		asked = append(asked, ns+"/"+name)
		switch name {
		case "gone-tls":
			return false, true
		case "kept-tls":
			return true, true
		}
		return false, false // not read yet, or not allowed to
	}
	got := ingressProblems(e, ing)
	if len(got) != 1 || got[0] != "TLS secret gone-tls isn't there" || len(asked) != 3 || asked[0] != "qa/gone-tls" {
		t.Errorf("problems %q, asked %q", got, asked)
	}
}

// r146OnDelete is stateful set web (OnDelete) whose pod runs img/app:1
// while its template is set to image.
func r146OnDelete(t *testing.T, strategy, image string) (string, string) {
	t.Helper()
	e, pods, w := r138Workload("StatefulSet", 1, 1, false)
	spec := w.Object["spec"].(map[string]any)
	spec["updateStrategy"] = map[string]any{"type": strategy}
	spec["template"].(map[string]any)["spec"] = map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}
	w.Object["status"].(map[string]any)["currentRevision"] = "web-1"
	w.Object["status"].(map[string]any)["updateRevision"] = "web-2"
	box := strings.Join(workloadDetail(e, w), "\n")
	app := strings.Join(AppLines(r136App(t, e, pods, w), Lookups{}), "\n")
	return box, app
}

// QA round 146: after i on an OnDelete stateful set, its box said only
// "Images busybox:1.37" while its pod still ran 1.36. It says what its
// pods run, and what it is set to that they take only once deleted.
func TestOnDeleteImagesSayWhatPodsRun(t *testing.T) {
	const note = "img/app:1; set to img/app:2; pods take it only once deleted (OnDelete)"
	box, app := r146OnDelete(t, "OnDelete", "img/app:2")
	if !strings.Contains(box, "Images   "+note) {
		t.Errorf("box:\n%s", box)
	}
	if !strings.Contains(app, note) {
		t.Errorf("app box:\n%s", app)
	}
	// Its pods run what it is set to: as before.
	box, app = r146OnDelete(t, "OnDelete", "img/app:1")
	if strings.Contains(box+app, "set to") || !strings.Contains(box, "Images   img/app:1") {
		t.Errorf("same image: box:\n%s\napp:\n%s", box, app)
	}
	// Rolling updates replace pods by themselves: no note.
	box, app = r146OnDelete(t, "RollingUpdate", "img/app:2")
	if strings.Contains(box+app, "set to") {
		t.Errorf("rolling update: box:\n%s\napp:\n%s", box, app)
	}
}

// QA round 146: bare pods (nothing makes them) got Fix lines pointing at
// a workload that doesn't exist. Each Fix family names where it is fixed.
func TestBarePodFixesSayWhereItIsDefined(t *testing.T) {
	const bareWhere = "where its pod is defined (nothing makes it again if it goes)"
	families := []Status{
		{"Out of memory", "container app keeps running out of memory", ToneBad},
		{"Hit memory limit", "container app ran out of memory", ToneWarn},
		{"Crashing", "container app " + hookFailed, ToneBad},
		{"Too big for any node", "it asks for more than any node here has", ToneBad},
		{"Crashing", "container app is killed by its alive check", ToneBad},
		{"Can't pull image", "img/app:1 not found", ToneBad},
	}
	for _, st := range families {
		bare := &App{Name: "mx", Status: st, Pods: 1, barePods: 1}
		fix := appFix(bare, false)
		if strings.Contains(fix, "Its workloads") || strings.Contains(fix, "deployment") {
			t.Errorf("home, bare pod, %s: %q", st.Short, fix)
		}
		if (st.Short == "Crashing" && strings.Contains(st.Why, hookFailed) || st.Short == "Too big for any node") && !strings.Contains(fix, bareWhere) {
			t.Errorf("home, bare pod, %s: %q, want %q", st.Short, fix, bareWhere)
		}
		dep := &App{Name: "mx", Status: st, Pods: 1, kinds: map[string]bool{"Deployment": true}}
		if fix := appFix(dep, false); strings.Contains(fix, "nothing makes it again") {
			t.Errorf("home, deployment, %s: %q", st.Short, fix)
		}

		pod := podFixFor(st, "Pod")
		if strings.Contains(pod, "deployment") {
			t.Errorf("pod box, bare pod, %s: %q", st.Short, pod)
		}
		if strings.Contains(st.Short, "memory") || strings.Contains(st.Why, hookFailed) {
			if !strings.Contains(pod, "where this pod is defined (nothing makes it again if it goes)") {
				t.Errorf("pod box, bare pod, %s: %q", st.Short, pod)
			}
			if fix := podFixFor(st, "Deployment"); !strings.Contains(fix, "(on its deployment)") {
				t.Errorf("pod box, deployment, %s: %q", st.Short, fix)
			}
			if fix := podFixFor(st, "StatefulSet"); !strings.Contains(fix, "(on its stateful set)") {
				t.Errorf("pod box, stateful set, %s: %q", st.Short, fix)
			}
		}
	}
	hook := &App{Name: "mx", Status: families[2], Pods: 1, kinds: map[string]bool{"Deployment": true}}
	if fix := appFix(hook, false); fix != "fix its postStart hook (enter → Its workloads, then e)" {
		t.Errorf("home, deployment hook: %q", fix)
	}
}

// QA round 146: a job (backoffLimit 0) whose pod was deleted: once the
// pod is gone, home, its box and :changes said "it failed, and so did
// any retries it was allowed". A kept Killing event for its pod, and
// nothing that says a try failed, says it was stopped.
func TestJobOfDeletedPodSaysStopped(t *testing.T) {
	const said = "its pod was stopped (deleted), not failed by itself"
	job := []mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"backoffLimit": int64(0)}, fail: 1,
		conds: []mxCond{{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", time.Minute}}}}
	killing := mxPodEvent("j-x7k2m", "Normal", "Killing", "Stopping container main", 2*time.Minute)
	check := func(name string, events []*unstructured.Unstructured, want bool) {
		t.Helper()
		sawDeletingMu.Lock()
		clear(sawDeleting) // a world of its own: kubed saw none of its pods deleting
		sawDeletingMu.Unlock()
		w := mxBuildWith(job, nil, events)
		a := w.app()
		changes := ""
		for _, ch := range w.e.Changes(mxNS, "", time.Now(), nil) {
			if ch.Object == "job/j" {
				changes += ch.Detail
			}
		}
		for what, got := range map[string]string{"home": a.Status.Why, "box": w.jobBox("j"), ":changes": changes} {
			if strings.Contains(got, said) != want || want && strings.Contains(got, "so did any retries") {
				t.Errorf("%s: %s: %q (want said %v)", name, what, got, want)
			}
		}
		for what, fix := range map[string]string{"home": appFix(a, false), "box": w.jobBox("j")} {
			if !strings.Contains(fix, "create the job again if it should run") || strings.Contains(fix, "fix it") {
				t.Errorf("%s: %s fix %q", name, what, fix)
			}
		}
	}
	check("killed", []*unstructured.Unstructured{killing}, true)
	check("no event", nil, false)
	check("killed after crashing", []*unstructured.Unstructured{killing, mxPodEvent("j-x7k2m", "Warning", "BackOff", "Back-off restarting failed container main", 3*time.Minute)}, false)
	// Kubed saw its pod deleting, and then it went with its events.
	clear(sawDeleting)
	stopping := mxBuild([]mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"backoffLimit": int64(0)}, fail: 1, status: map[string]any{"terminating": int64(1)},
		conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", time.Minute}}}},
		[]mxJobPod{{name: "j-x7k2m", job: "j", ago: 4 * time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: 4 * time.Minute}}}})
	stopping.pods[0].Object["metadata"].(map[string]any)["deletionGracePeriodSeconds"] = int64(120) // asked 2m5s ago, before it failed
	if a := stopping.app(); !strings.Contains(a.Status.Why, deletedStopping) {
		t.Errorf("stopping: %q", a.Status.Why)
	}
	if a := mxBuild(job, nil).app(); !strings.Contains(a.Status.Why, said) {
		t.Errorf("seen deleting, now gone: %q", a.Status.Why)
	}
	check("killed by its alive check", []*unstructured.Unstructured{mxPodEvent("j-x7k2m", "Normal", "Killing", "Container main failed liveness probe, will be restarted", 2*time.Minute)}, false)
}

// QA round 146: a deployment paused partway said why on home but had no
// Fix line, and kubed has no key to resume it. The Fix says how.
func TestPausedPartwayFixSaysResume(t *testing.T) {
	e, dep, pods := r132Paused(1)
	var a *App
	for _, x := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if x.Name == "pz" {
			a = x
		}
	}
	if a == nil {
		t.Fatal("no app pz")
	}
	if got, want := appFix(a, false), "resume it: kubectl rollout resume deployment/pz -n qa (or e and set spec.paused to false)"; got != want {
		t.Errorf("fix %q, want %q", got, want)
	}
	if got := appFix(a, true); got != "resume it: kubectl rollout resume deployment/pz -n qa (or set spec.paused to false), without --readonly" {
		t.Errorf("read-only fix %q", got)
	}
	if got := strings.Join(workloadDetail(e, dep), "\n"); !strings.Contains(got, "Fix      resume it: kubectl rollout resume deployment/pz -n qa") {
		t.Errorf("box: %s", got)
	}
}

// Review of round 146: when a parallel job's own try fails (backoffLimit
// 0), Kubernetes stops its other tries itself. Those stops come after it
// failed: they don't say it was stopped by hand.
func TestJobStoppedByItselfAfterFailingFailedByItself(t *testing.T) {
	sawDeletingMu.Lock()
	clear(sawDeleting)
	sawDeletingMu.Unlock()
	job := []mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"backoffLimit": int64(0), "parallelism": int64(3), "completions": int64(3)}, fail: 3,
		conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 2 * time.Minute},
			{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 2 * time.Minute}}}}
	cleanup := []*unstructured.Unstructured{ // after it failed: its own cleanup
		mxPodEvent("j-b4n7q", "Normal", "Killing", "Stopping container main", 90*time.Second),
		mxPodEvent("j-c5p8r", "Normal", "Killing", "Stopping container main", 90*time.Second),
		mxPodEvent("j-d6q9s", "Normal", "Killing", "Stopping container main", 90*time.Second),
	}
	if a := mxBuildWith(job, nil, cleanup).app(); strings.Contains(a.Status.Why, "not failed by itself") {
		t.Errorf("cleanup after it failed: %q", a.Status.Why)
	}
	// Kubed saw one deleting (asked 35s ago, a 30s grace), after it failed:
	// the same.
	stopping := []mxJobPod{{name: "j-b4n7q", job: "j", ago: 4 * time.Minute, phase: "Running", deleting: true, ctrs: []mxCtr{{name: "main", state: "run", ago: 4 * time.Minute}}}}
	w := mxBuild(job, stopping)
	w.pods[0].Object["metadata"].(map[string]any)["deletionGracePeriodSeconds"] = int64(30)
	_ = w.app()
	if a := mxBuildWith(job, nil, cleanup[1:]).app(); strings.Contains(a.Status.Why, "not failed by itself") {
		t.Errorf("seen deleting after it failed: %q", a.Status.Why)
	}
}

// Review of round 146: a static pod (the node runs it) and a pod an
// operator makes read "(on its deployment)". They name where the fix goes.
func TestPodFixNamesNodeAndOperator(t *testing.T) {
	hook := `Exec lifecycle hook ([sh -c exit 3]) for Container "app" in Pod "h_mxa(uid-h)" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`
	pod := func(kind, apiVersion, name string) *unstructured.Unstructured {
		p := mxPod("h", kind, corev1.PodRunning, mxC{name: "app", state: mxWait("PostStartHookError", hook), restarts: 3, last: mxEnd("Error", 137, time.Minute)})
		p.OwnerReferences[0].APIVersion, p.OwnerReferences[0].Name = apiVersion, name
		return mxObj(p)
	}
	node := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "ip-10-0-1-5"}}}
	for _, c := range []struct {
		name string
		e    *enrich.Enricher
		pod  *unstructured.Unstructured
		want string
	}{
		{"static, nodes not loaded", enrich.New(mxStore{listStore{}}, kube.NewResources(nil), nil), pod("Node", "v1", "ip-10-0-1-5"),
			"fix its postStart hook in its static pod manifest on node ip-10-0-1-5"},
		{"static, nodes loaded", enrich.New(mxStore{listStore{kube.Nodes: {node}}}, kube.NewResources([]kube.Resource{{GVR: kube.Nodes, Kind: "Node"}}), nil), pod("Node", "v1", "ip-10-0-1-5"),
			"fix its postStart hook in its static pod manifest on node ip-10-0-1-5"},
		{"operator", enrich.New(mxStore{listStore{}}, kube.NewResources(nil), nil), pod("CloneSet", "apps.kruise.io/v1alpha1", "web"),
			"fix its postStart hook (on its cloneset)"},
	} {
		if box := strings.Join(podDetail(c.e, c.pod), "\n"); !strings.Contains(box, "Fix      "+c.want+"\n") {
			t.Errorf("%s: box %s", c.name, box)
		}
	}
	if fix := podFixFor(Status{"Out of memory", "container app keeps running out of memory", ToneBad}, "CloneSet"); !strings.Contains(fix, "(on its cloneset)") {
		t.Errorf("operator memory: %q", fix)
	}
}

// Second review of round 146: whole-second times put the job's own cleanup
// up to a second before its give-up time on older Kubernetes; and a static
// pod that can't pull its image read "(i on its deployment)".
func TestStopsJustBeforeGivingUpAndStaticPull(t *testing.T) {
	sawDeletingMu.Lock()
	clear(sawDeleting)
	sawDeletingMu.Unlock()
	job := []mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"backoffLimit": int64(0), "parallelism": int64(3), "completions": int64(3)}, fail: 3,
		conds: []mxCond{{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 2 * time.Minute}}}}
	cleanup := []*unstructured.Unstructured{ // the same sync as Failed, a second earlier by the clock
		mxPodEvent("j-b4n7q", "Normal", "Killing", "Stopping container main", 121*time.Second),
		mxPodEvent("j-c5p8r", "Normal", "Killing", "Stopping container main", 121*time.Second),
	}
	// Round 150: the job's own cleanup always keeps the try that crashed
	// (failed pods are kept); the stops then can't be what made it give up.
	crashed := []mxJobPod{mxFailed("j-a3m6p", "j", 2*time.Minute, 1)}
	if a := mxBuildWith(job, crashed, cleanup).app(); strings.Contains(a.Status.Why, "not failed by itself") {
		t.Errorf("cleanup a second before it failed: %q", a.Status.Why)
	}
	p := mxPod("s", "Node", corev1.PodPending, mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "nope:1"`)})
	p.OwnerReferences[0].APIVersion, p.OwnerReferences[0].Name = "v1", "ip-10-0-1-5"
	e := enrich.New(mxStore{listStore{}}, kube.NewResources(nil), nil)
	if box := strings.Join(podDetail(e, mxObj(p)), "\n"); strings.Contains(box, "deployment") || !strings.Contains(box, "static") {
		t.Errorf("static pod pull: %s", box)
	}
}
