package view

// Real-shaped objects: what kubed says about objects as a real cluster
// (EKS 1.33, staging) sends them, not as a test writes them by hand. Each
// file under internal/testdata/real was captured read-only with
// kubectl get -o json, then trimmed and renamed (see its "note").
//
// Every timestamp is moved forward by the time since capture, so each
// object is as old now as it was then.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

type realStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s realStore) List(g schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, o := range s[g] {
		if ns == "" || o.GetNamespace() == ns {
			out = append(out, o)
		}
	}
	return out
}

func (s realStore) Get(g schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s[g] {
		if o.GetName() == name && (ns == "" || o.GetNamespace() == ns) {
			return o, true
		}
	}
	return nil, false
}

func (realStore) Synced(schema.GroupVersionResource) bool { return true }

var realKinds = map[string]kube.Resource{
	"Pod":                {GVR: kube.Pods, Kind: "Pod", Namespaced: true},
	"Job":                {GVR: kube.Jobs, Kind: "Job", Namespaced: true},
	"CronJob":            {GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true},
	"Event":              {GVR: kube.Events, Kind: "Event", Namespaced: true},
	"Deployment":         {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
	"ReplicaSet":         {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
	"StatefulSet":        {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true},
	"DaemonSet":          {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true},
	"ControllerRevision": {GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true},
}

var realStamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)$`)

// realShift moves every timestamp in v forward by d.
func realShift(v any, d time.Duration) any {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			x[k] = realShift(val, d)
		}
	case []any:
		for i, val := range x {
			x[i] = realShift(val, d)
		}
	case string:
		if realStamp.MatchString(x) {
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return t.Add(d).UTC().Format(time.RFC3339)
			}
		}
	}
	return v
}

type realWorld struct {
	e    *enrich.Enricher
	all  []*unstructured.Unstructured
	kind map[string][]*unstructured.Unstructured
}

// realLoad reads fixture files (internal/testdata/real) into one cluster.
func realLoad(t *testing.T, files ...string) realWorld {
	t.Helper()
	store := realStore{}
	w := realWorld{kind: map[string][]*unstructured.Unstructured{}}
	for _, f := range files {
		raw, err := os.ReadFile(filepath.Join("..", "testdata", "real", f))
		if err != nil {
			t.Fatal(err)
		}
		var file struct {
			Captured time.Time         `json:"captured"`
			Items    []json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(raw, &file); err != nil {
			t.Fatal(err)
		}
		shift := time.Since(file.Captured)
		for _, item := range file.Items {
			o := &unstructured.Unstructured{}
			if err := o.UnmarshalJSON(item); err != nil { // whole numbers as int64, as from the cluster
				t.Fatal(err)
			}
			o.Object = realShift(o.Object, shift).(map[string]any)
			res, ok := realKinds[o.GetKind()]
			if !ok {
				t.Fatalf("%s: kind %s", f, o.GetKind())
			}
			store[res.GVR] = append(store[res.GVR], o)
			w.all = append(w.all, o)
			w.kind[o.GetKind()] = append(w.kind[o.GetKind()], o)
		}
	}
	var res []kube.Resource
	for _, r := range realKinds {
		res = append(res, r)
	}
	w.e = enrich.New(store, kube.NewResources(res), nil)
	return w
}

func (w realWorld) get(kind, name string) *unstructured.Unstructured {
	for _, o := range w.kind[kind] {
		if o.GetName() == name {
			return o
		}
	}
	return nil
}

func (w realWorld) row(o *unstructured.Unstructured) ([]string, Tone) {
	return registry[realKinds[o.GetKind()].GVR.GroupResource()].Row(w.e, o)
}

var realAll = []string{"cron_runs.json", "cron_gone.json", "cron_manual.json", "deploy_restarted.json", "deploy_sidecar.json",
	"deploy_setup.json", "deploy_zero.json", "deploy_longrun.json", "deploy_events.json", "sts.json", "ds.json"}

func realTrue(o *unstructured.Unstructured, typ string) bool {
	c, ok := enrich.Condition(o, typ)
	return ok && c.Status == "True"
}

func realTime(o *unstructured.Unstructured, fields ...string) time.Time {
	s, _, _ := unstructured.NestedString(o.Object, fields...)
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func realCondTime(o *unstructured.Unstructured, typ string) time.Time {
	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	for _, c := range conds {
		if m, ok := c.(map[string]any); ok && m["type"] == typ {
			t, _ := time.Parse(time.RFC3339, fmt.Sprint(m["lastTransitionTime"]))
			return t
		}
	}
	return time.Time{}
}

func realOwnedBy(o *unstructured.Unstructured, kind, name string) bool {
	for _, r := range o.GetOwnerReferences() {
		if r.Kind == kind && r.Name == name {
			return true
		}
	}
	return false
}

func (w realWorld) apps() []*App {
	var workloads []*unstructured.Unstructured
	for _, k := range []string{"CronJob", "Deployment", "StatefulSet", "DaemonSet"} {
		workloads = append(workloads, w.kind[k]...)
	}
	return Apps(w.e, w.kind["Pod"], nil, nil, w.kind["Job"], workloads)
}

func (w realWorld) app(t *testing.T, ns string) *App {
	t.Helper()
	for _, a := range w.apps() {
		if a.Namespace == ns {
			return a
		}
	}
	t.Fatalf("no app in %s", ns)
	return nil
}

// Every real Job (EKS 1.33 sets two conditions when a run ends: Complete
// with SuccessCriteriaMet, Failed with FailureTarget): gj says what the
// conditions say, the counts and how long it took as the object has them.
// Complete + SuccessCriteriaMet reading "Done (its last pods stopping)" is
// QA round 128's bug, fixed separately: this test shows it until then.
func TestRealJobStatus(t *testing.T) {
	w := realLoad(t, "cron_runs.json", "cron_gone.json", "cron_manual.json")
	if len(w.kind["Job"]) < 15 {
		t.Fatalf("only %d jobs loaded", len(w.kind["Job"]))
	}
	for _, j := range w.kind["Job"] {
		cells, tone := w.row(j)
		status := cells[1]
		start := realTime(j, "status", "startTime")
		succeeded, _, _ := unstructured.NestedInt64(j.Object, "status", "succeeded")
		switch {
		case realTrue(j, "Complete"):
			// Complete is set once every pod has stopped: nothing is stopping.
			if status != "Done" || tone == ToneBad {
				t.Errorf("job %s is Complete (and SuccessCriteriaMet): STATUS %q, want \"Done\"", j.GetName(), status)
			}
			if want := span(realTime(j, "status", "completionTime").Sub(start)); cells[3] != want {
				t.Errorf("job %s took %s (startTime to completionTime), TOOK says %q", j.GetName(), want, cells[3])
			}
		case realTrue(j, "Failed"):
			// Failed is set once its pods have stopped, so FailureTarget
			// beside it doesn't mean anything is still stopping.
			if status != "Failed" || tone != ToneBad {
				t.Errorf("job %s is Failed (and FailureTarget): STATUS %q, want \"Failed\"", j.GetName(), status)
			}
			if want := span(realCondTime(j, "Failed").Sub(start)); cells[3] != want {
				t.Errorf("job %s ran %s before it was marked Failed, TOOK says %q", j.GetName(), want, cells[3])
			}
		default:
			t.Fatalf("job %s: fixture has an unended job", j.GetName())
		}
		if want := fmt.Sprintf("%d/1", succeeded); cells[2] != want {
			t.Errorf("job %s: DONE %q, want %q", j.GetName(), cells[2], want)
		}
	}
}

// A run's pods as kept after it ends: one that exited 1 under
// restartPolicy Never failed once (no crash loop, no restarts), one that
// exited 0 completed.
func TestRealRunPods(t *testing.T) {
	w := realLoad(t, "cron_runs.json", "cron_manual.json")
	for _, o := range w.kind["Pod"] {
		p, err := ToPod(o)
		if err != nil {
			t.Fatal(err)
		}
		st := Explain(w.e, p)
		cells, _ := w.row(o)
		term := p.Status.ContainerStatuses[0].State.Terminated
		if term == nil {
			t.Fatalf("pod %s: not ended in the fixture", o.GetName())
		}
		what := "pod " + o.GetName()
		switch p.Status.Phase {
		case "Succeeded":
			if st.Short != "Completed" {
				t.Errorf("%s exited 0 and Succeeded: says %q %q", what, st.Short, st.Why)
			}
		case "Failed":
			if st.Short != "Failed" {
				t.Errorf("%s failed (exit %d, never restarted): says %q", what, term.ExitCode, st.Short)
			}
			mxCheck(t, what, st.Why, []string{fmt.Sprintf("exit %d", term.ExitCode)}, []string{"crash", "restart", "memory", "Restarting"})
		}
		if cells[3] != "0" {
			t.Errorf("%s: restartCount 0, RESTARTS %q", what, cells[3])
		}
	}
}

// Each cron job's row and box against its own status and the runs that
// are there: LAST RUN is the newest run started, by schedule or by hand
// (a run made from it with kubectl create job --from carries the cron job
// as its owner); LAST SUCCESS is lastSuccessfulTime; the row is red only
// when the newest finished run failed.
func TestRealCronJobs(t *testing.T) {
	w := realLoad(t, "cron_runs.json", "cron_gone.json", "cron_manual.json")
	for _, cj := range w.kind["CronJob"] {
		last := realTime(cj, "status", "lastScheduleTime")
		var newest *unstructured.Unstructured
		for _, j := range w.kind["Job"] {
			if realOwnedBy(j, "CronJob", cj.GetName()) && (newest == nil || j.GetCreationTimestamp().After(newest.GetCreationTimestamp().Time)) {
				newest = j
			}
		}
		if newest != nil && newest.GetCreationTimestamp().After(last) {
			last = newest.GetCreationTimestamp().Time
		}
		cells, tone := w.row(cj)
		what := "cron job " + cj.GetName()
		if want := agoOrNever(last); cells[2] != want {
			t.Errorf("%s: newest run started %s, LAST RUN %q", what, want, cells[2])
		}
		if want := agoOrNever(realTime(cj, "status", "lastSuccessfulTime")); cells[3] != want {
			t.Errorf("%s: lastSuccessfulTime %s, LAST SUCCESS %q", what, want, cells[3])
		}
		if cells[4] != "0" {
			t.Errorf("%s: no run is going, RUNNING %q", what, cells[4])
		}
		box := strings.Join(cronDetail(w.e, cj), "\n")
		newestFailed := newest != nil && realTrue(newest, "Failed")
		switch {
		case newest == nil:
			mxCheck(t, what, box, []string{"Last run never"}, []string{"failed", "worked"})
		case newestFailed:
			if tone != ToneBad {
				t.Errorf("%s: its newest run failed, row tone %d", what, tone)
			}
			// (its pods are gone, so no Problem line can say why)
			mxCheck(t, what, box, []string{"Last run " + Age(last) + " ago, but the last one that worked was"}, []string{"and it worked"})
		default:
			if tone == ToneBad || tone == ToneWarn {
				t.Errorf("%s: its newest run worked, row tone %d", what, tone)
			}
			mxCheck(t, what, box, []string{"and it worked"}, []string{"Problem", "failed"})
		}
	}
}

// Home for cron jobs: msgs has a failed run on each of its cron jobs, but
// a run after each worked; feeds' newest export-b run failed (its pods
// gone); pauser's newest run was made by hand and worked.
func TestRealCronApps(t *testing.T) {
	w := realLoad(t, "cron_runs.json", "cron_gone.json", "cron_manual.json")
	for _, c := range []struct {
		ns          string
		short       string
		must, never []string
	}{
		{"msgs", "Scheduled", nil, []string{"Problem", "failed ("}},
		{"pauser", "Scheduled", nil, []string{"Problem", "failed ("}},
		{"feeds", "Job failed", []string{"job feeds-export-b-29867320 failed", "its failed pods are gone"}, []string{"l on the failed one", "is running", "running again"}},
	} {
		a := w.app(t, c.ns)
		if a.Status.Short != c.short {
			t.Errorf("app %s: %q (%s), want %q", c.ns, a.Status.Short, a.Status.Why, c.short)
		}
		mxCheck(t, "app "+c.ns, strings.Join(AppLines(a, Lookups{}), "\n"), c.must, c.never)
	}
}

// A failed run that a later run of the same cron job has made good: its
// box must not send you to wait for "the next run", which already ran
// and worked.
func TestRealFailedRunAlreadyFollowedBySuccess(t *testing.T) {
	w := realLoad(t, "cron_runs.json")
	for _, name := range []string{"msgs-sweep-a-29868195", "msgs-sweep-b-29868165"} {
		j := w.get("Job", name)
		later := false
		for _, o := range w.kind["Job"] {
			if o != j && realOwnedBy(o, "CronJob", j.GetOwnerReferences()[0].Name) && o.GetCreationTimestamp().After(j.GetCreationTimestamp().Time) && realTrue(o, "Complete") {
				later = true
			}
		}
		if !later {
			t.Fatalf("fixture: %s has no later run that worked", name)
		}
		box := strings.Join(workloadProblem(w.e, j), "\n")
		mxCheck(t, "job "+name, box, []string{"this run failed", "exit 1"}, []string{"the next run is on schedule"})
	}
}

// Pods that restarted at startup (the database came up after them), then
// ran without a stop for close to an hour: ready, running, and kubed's
// words say how many restarts and when, never that they crash now.
func TestRealRestartedPodsSteadyNow(t *testing.T) {
	w := realLoad(t, "deploy_restarted.json", "deploy_sidecar.json", "deploy_setup.json", "deploy_longrun.json")
	for _, o := range w.kind["Pod"] {
		p, err := ToPod(o)
		if err != nil {
			t.Fatal(err)
		}
		what := "pod " + o.GetName()
		var restarts int32
		ready, total := 0, 0
		var lastExit time.Time
		for _, cs := range RunningStatuses(p) {
			restarts += cs.RestartCount
			total++
			if cs.Ready {
				ready++
			}
			if cs.State.Running == nil {
				t.Fatalf("%s: %s not running in the fixture", what, cs.Name)
			}
			if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(lastExit) {
				lastExit = t.FinishedAt.Time
			}
		}
		if ready != total || time.Since(lastExit) < 20*time.Minute {
			t.Fatalf("%s: fixture not steady (%d/%d ready, last exit %s ago)", what, ready, total, Age(lastExit))
		}
		st := Explain(w.e, p)
		if st.Tone == ToneBad || strings.HasPrefix(st.Short, "Crashing") {
			t.Errorf("%s: steady for %s, says %q %q", what, Age(lastExit), st.Short, st.Why)
		}
		mxCheck(t, what, st.Why, []string{Age(lastExit) + " ago"}, []string{"keeps crashing", "running again now", "keeps stopping"})
		cells, _ := w.row(o)
		if want := fmt.Sprintf("%d/%d", ready, total); cells[1] != want {
			t.Errorf("%s: READY %q, want %s (its containers and helpers)", what, cells[1], want)
		}
		if want := fmt.Sprint(restarts); !strings.HasPrefix(cells[3], want) {
			t.Errorf("%s: RESTARTS %q, want %s", what, cells[3], want)
		}
	}
}

// realSettings is a pod template with what a restart changes left out.
func realSettings(t *testing.T, tmpl map[string]any) string {
	t.Helper()
	c := runtime.DeepCopyJSON(tmpl)
	unstructured.RemoveNestedField(c, "metadata", "labels", "pod-template-hash")
	unstructured.RemoveNestedField(c, "metadata", "labels", "controller-revision-hash")
	unstructured.RemoveNestedField(c, "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// realDeployedAt is when a workload's current settings went out, from
// its versions alone: the oldest of the newest versions that differ only
// by a restart.
func realDeployedAt(t *testing.T, w realWorld, o *unstructured.Unstructured) time.Time {
	t.Helper()
	type ver struct {
		n        int64
		settings string
		made     time.Time
	}
	var vs []ver
	for _, k := range []string{"ReplicaSet", "ControllerRevision"} {
		for _, v := range w.kind[k] {
			if !realOwnedBy(v, o.GetKind(), o.GetName()) || v.GetNamespace() != o.GetNamespace() {
				continue
			}
			var n int64
			var tmpl map[string]any
			if k == "ReplicaSet" {
				if _, err := fmt.Sscan(v.GetAnnotations()["deployment.kubernetes.io/revision"], &n); err != nil {
					t.Fatalf("fixture: replica set %s revision: %v", v.GetName(), err)
				}
				tmpl, _, _ = unstructured.NestedMap(v.Object, "spec", "template")
			} else {
				n, _, _ = unstructured.NestedInt64(v.Object, "revision")
				tmpl, _, _ = unstructured.NestedMap(v.Object, "data", "spec", "template")
			}
			vs = append(vs, ver{n, realSettings(t, tmpl), v.GetCreationTimestamp().Time})
		}
	}
	if len(vs) == 0 {
		t.Fatalf("%s: no versions", o.GetName())
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i].n > vs[j].n })
	i := 0
	for i+1 < len(vs) && vs[i].settings == vs[i+1].settings {
		i++
	}
	return vs[i].made
}

// Deployed, for apps whose pods were all made again when the cluster came
// back from its nightly pause (scaled 0 to N, nodes new): the version
// went out when its ReplicaSet (or revision) was made, days before, not
// when the pods came back.
func TestRealDeployedNotTheResume(t *testing.T) {
	w := realLoad(t, realAll...)
	checked := 0
	for _, a := range w.apps() {
		var owner *unstructured.Unstructured
		for _, k := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
			for _, o := range w.kind[k] {
				if o.GetNamespace() == a.Namespace && (k != "DaemonSet" || o.GetName() == "kube-proxy") {
					owner = o
				}
			}
		}
		if owner == nil || a.Pods == 0 {
			continue
		}
		checked++
		want := realDeployedAt(t, w, owner)
		what := fmt.Sprintf("app %s (%s %s)", a.Name, owner.GetKind(), owner.GetName())
		if d := a.LastDeploy.Sub(want); d < -time.Second || d > time.Second || a.DeployBefore || a.DeployAfter || a.DeployUnknown {
			t.Errorf("%s: its version went out %s (%s), Deployed says %s (before=%v after=%v unknown=%v)", what,
				want.UTC().Format(time.RFC3339), Age(want), a.LastDeploy.UTC().Format(time.RFC3339), a.DeployBefore, a.DeployAfter, a.DeployUnknown)
		}
		lines := strings.Join(AppLines(a, Lookups{}), "\n")
		mxCheck(t, what, lines, []string{"Deployed " + Age(want) + " ago", "all its pods were replaced since"}, nil)
	}
	if checked < 11 {
		t.Errorf("checked %d apps, want the 11 that run pods", checked)
	}
}

// READY and UPDATED as each workload's status has them. A Deployment
// scaled to 0 is sent with no replica counts at all, and a DaemonSet no
// node is for with no numberAvailable: 0/0, quiet, not down.
func TestRealReadyCounts(t *testing.T) {
	w := realLoad(t, realAll...)
	n := func(o *unstructured.Unstructured, f ...string) int64 {
		v, _, _ := unstructured.NestedInt64(o.Object, f...)
		return v
	}
	for _, o := range w.kind["Deployment"] {
		cells, tone := w.row(o)
		want := n(o, "spec", "replicas")
		if r := fmt.Sprintf("%d/%d", n(o, "status", "readyReplicas"), want); cells[1] != r {
			t.Errorf("deployment %s: READY %q, want %s", o.GetName(), cells[1], r)
		}
		if r := fmt.Sprintf("%d/%d", n(o, "status", "updatedReplicas"), want); cells[2] != r {
			t.Errorf("deployment %s: UPDATED %q, want %s", o.GetName(), cells[2], r)
		}
		if want == 0 && (tone == ToneBad || tone == ToneWarn) {
			t.Errorf("deployment %s: scaled to 0, tone %d", o.GetName(), tone)
		}
	}
	for _, o := range w.kind["StatefulSet"] {
		cells, _ := w.row(o)
		if r := fmt.Sprintf("%d/%d", n(o, "status", "readyReplicas"), n(o, "spec", "replicas")); cells[1] != r {
			t.Errorf("statefulset %s: READY %q, want %s", o.GetName(), cells[1], r)
		}
	}
	for _, o := range w.kind["DaemonSet"] {
		cells, tone := w.row(o)
		want := n(o, "status", "desiredNumberScheduled")
		if r := fmt.Sprintf("%d/%d", n(o, "status", "numberReady"), want); cells[1] != r {
			t.Errorf("daemonset %s: READY %q, want %s", o.GetName(), cells[1], r)
		}
		if r := fmt.Sprintf("%d/%d", n(o, "status", "updatedNumberScheduled"), want); cells[2] != r {
			t.Errorf("daemonset %s: UPDATED %q, want %s", o.GetName(), cells[2], r)
		}
		if want == 0 && (tone == ToneBad || tone == ToneWarn) {
			t.Errorf("daemonset %s: wants no pods, tone %d", o.GetName(), tone)
		}
	}
	a := w.app(t, "audits")
	if a.Status.Short != "Stopped" || a.Status.Tone == ToneBad {
		t.Errorf("app audits is scaled to 0: %q %q", a.Status.Short, a.Status.Why)
	}
}

// Healthy apps with warnings from the last hour, as a cluster coming
// back from a pause has them: pods that waited for a node, a volume that
// attached on the second go, readiness that failed while starting, pods
// preempted or moved by Karpenter, a webhook that refused pods for a
// moment, Kyverno audit failures. Every pod there now is running and
// ready: no app or pod is in trouble.
func TestRealOldWarningsOnHealthyApps(t *testing.T) {
	w := realLoad(t, "deploy_events.json", "sts.json")
	for _, o := range w.kind["Pod"] {
		p, _ := ToPod(o)
		for _, cs := range p.Status.ContainerStatuses {
			if !cs.Ready {
				t.Fatalf("fixture: pod %s not ready", o.GetName())
			}
		}
		if st := Explain(w.e, p); st.Short != "Running" || st.Tone == ToneBad || st.Tone == ToneWarn {
			t.Errorf("pod %s: running and ready, says %q %q", o.GetName(), st.Short, st.Why)
		}
	}
	for _, a := range w.apps() {
		if a.Status.Short != "Running" || a.Status.Tone == ToneBad || a.Status.Tone == ToneWarn {
			t.Errorf("app %s: all its pods ready, says %q %q", a.Name, a.Status.Short, a.Status.Why)
		}
	}
}

// The taint manager's "Cancelling deletion of Pod x": the pod was going
// to be deleted for a node taint, then the taint went and it stayed. The
// pod is still there, on its node. Every such event on staging (72 at
// capture) has this message.
func TestRealTaintEvictionCancelled(t *testing.T) {
	w := realLoad(t, "sts.json", "deploy_events.json")
	seen := 0
	for _, ev := range w.kind["Event"] {
		msg, _, _ := unstructured.NestedString(ev.Object, "message")
		if !strings.HasPrefix(msg, "Cancelling deletion of Pod") {
			continue
		}
		seen++
		cells, _ := w.row(ev)
		pod, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
		mxCheck(t, "event about "+pod, cells[1], nil, []string{"moved off", "evicted", "pushed off"})
	}
	if seen == 0 {
		t.Fatal("fixture: no cancelled taint eviction")
	}
	if w.get("Pod", "queue-cluster-server-0") == nil {
		t.Fatal("fixture: the pod whose deletion was cancelled should be there")
	}
}

// ArgoCD tracks these objects (argocd.argoproj.io/tracking-id), but its
// Application isn't in this cluster (ArgoCD in another cluster, or not
// readable): nothing says how it syncs.
func TestRealArgoRecordNotHere(t *testing.T) {
	w := realLoad(t, "deploy_restarted.json")
	a := w.app(t, "login")
	if a.Argo == nil || a.Argo.Found {
		t.Fatalf("fixture: want an ArgoCD app that isn't in the cluster, got %+v", a.Argo)
	}
	lines := strings.Join(AppLines(a, Lookups{}), "\n")
	mxCheck(t, "app login", lines, nil, []string{"you sync it", "auto-sync"})
}
