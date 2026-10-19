package enrich

// Real-shaped objects (internal/testdata/real, captured read-only from
// staging and renamed): what the enricher works out from objects as a
// cluster sends them. Timestamps move forward by the time since capture.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

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
	e    *Enricher
	kind map[string][]*unstructured.Unstructured
}

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
			if err := o.UnmarshalJSON(item); err != nil {
				t.Fatal(err)
			}
			o.Object = realShift(o.Object, shift).(map[string]any)
			res, ok := realKinds[o.GetKind()]
			if !ok {
				t.Fatalf("%s: kind %s", f, o.GetKind())
			}
			store[res.GVR] = append(store[res.GVR], o)
			w.kind[o.GetKind()] = append(w.kind[o.GetKind()], o)
		}
	}
	var res []kube.Resource
	for _, r := range realKinds {
		res = append(res, r)
	}
	w.e = New(store, kube.NewResources(res), nil)
	return w
}

func realTime(o *unstructured.Unstructured, fields ...string) time.Time {
	s, _, _ := unstructured.NestedString(o.Object, fields...)
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// A cron job's last run is the newest run it started, on its schedule or
// made from it by hand (kubectl create job --from=cronjob/..., which owns
// the job too and sets cronjob.kubernetes.io/instantiate: manual). Its
// last finished run failed only when the newest ended run failed.
func TestRealCronLastRun(t *testing.T) {
	w := realLoad(t, "cron_runs.json", "cron_gone.json", "cron_manual.json")
	for _, cj := range w.kind["CronJob"] {
		last := realTime(cj, "status", "lastScheduleTime")
		var newest *unstructured.Unstructured
		for _, j := range w.kind["Job"] {
			if ownedBy(j, cj) && (newest == nil || j.GetCreationTimestamp().After(newest.GetCreationTimestamp().Time)) {
				newest = j
			}
		}
		if newest != nil && newest.GetCreationTimestamp().After(last) {
			last = newest.GetCreationTimestamp().Time
		}
		if got := w.e.CronLastRun(cj); !got.Equal(last) {
			t.Errorf("cron job %s: newest run started %s, CronLastRun %s", cj.GetName(), last.UTC().Format(time.RFC3339), got.UTC().Format(time.RFC3339))
		}
		failed, kept := w.e.CronLastFinished(cj)
		if newest == nil {
			if kept {
				t.Errorf("cron job %s never ran: CronLastFinished says a run is kept", cj.GetName())
			}
			continue
		}
		c, _ := Condition(newest, "Failed")
		if want := c.Status == "True"; !kept || failed != want {
			t.Errorf("cron job %s: newest run %s failed=%v, CronLastFinished failed=%v kept=%v", cj.GetName(), newest.GetName(), want, failed, kept)
		}
		if n, why := w.e.CronRunsStuck(cj); n != 0 {
			t.Errorf("cron job %s: every run ended, %d stuck (%s)", cj.GetName(), n, why)
		}
		if w.e.CronRunning(cj) != 0 {
			t.Errorf("cron job %s: no run is going, CronRunning %d", cj.GetName(), w.e.CronRunning(cj))
		}
	}
	cj := w.kind["CronJob"][len(w.kind["CronJob"])-1]
	if cj.GetName() != "pauser-resume" || !w.e.CronLastRun(cj).After(realTime(cj, "status", "lastScheduleTime")) {
		t.Fatalf("fixture: pauser-resume's newest run should be one made by hand, after its last scheduled run")
	}
}

// A StatefulSet's and a DaemonSet's version went out when the revision
// they run now was made, unless it only restarted the one before: here
// queue-cluster-server's revision 2 changed its scheduling (anti-affinity
// and spread) four days after revision 1; kube-proxy's revision 2 went
// out in June. Their pods are an hour old (new nodes after the pause).
func TestRealRevisionDeployTime(t *testing.T) {
	w := realLoad(t, "sts.json", "ds.json")
	for _, c := range []struct{ kind, name, rev string }{
		{"StatefulSet", "queue-cluster-server", "queue-cluster-server-8587875566"},
		{"DaemonSet", "kube-proxy", "kube-proxy-67f678976f"},
		{"DaemonSet", "headroom-xlarge", "headroom-xlarge-659865ff56"},
	} {
		var owner, rev *unstructured.Unstructured
		for _, o := range w.kind[c.kind] {
			if o.GetName() == c.name {
				owner = o
			}
		}
		for _, o := range w.kind["ControllerRevision"] {
			if o.GetName() == c.rev {
				rev = o
			}
		}
		dt, ok := w.e.RevisionDeployTime(owner)
		if !ok || !dt.At.Equal(rev.GetCreationTimestamp().Time) || dt.Before {
			t.Errorf("%s %s: went out with revision %s at %s, RevisionDeployTime %s ok=%v before=%v", c.kind, c.name, c.rev,
				rev.GetCreationTimestamp().UTC().Format(time.RFC3339), dt.At.UTC().Format(time.RFC3339), ok, dt.Before)
		}
	}
}

// :changes after the cluster came back from its pause: the pods were made
// again (scaled 0 to N), which is no new version and no restart of one.
// The only version in the window is login's revision 33, made when its
// ReplicaSet was.
func TestRealChangesAfterResume(t *testing.T) {
	w := realLoad(t, "deploy_restarted.json", "deploy_sidecar.json", "deploy_setup.json", "deploy_longrun.json", "deploy_events.json")
	now := time.Now()
	var rs *unstructured.Unstructured
	for _, o := range w.kind["ReplicaSet"] {
		if o.GetName() == "login-app-server-7946f98bff" {
			rs = o
		}
	}
	seen := false
	for _, ns := range []string{"login", "perms", "helpdesk", "points", "pool", "tenants", "acme-tools", "places", "gate-proxy"} {
		for _, c := range w.e.Changes(ns, "", now, nil) {
			if c.What != "rollout" && c.What != "restart" && c.What != "undo" {
				continue
			}
			if c.What == "rollout" && c.Object == "deployment/login-app-server" && c.When.Equal(rs.GetCreationTimestamp().Time) {
				seen = true
				continue
			}
			if c.What == "restart" && strings.HasPrefix(c.Object, "pod/") {
				continue // a container's restart: TestRealSetupStepThatFailed
			}
			t.Errorf("%s: %s %s at %s (%s): no version went out then", ns, c.What, c.Object, c.When.UTC().Format(time.RFC3339), c.Detail)
		}
	}
	if !seen {
		t.Errorf("login's revision 33 (made %s) is missing from :changes", rs.GetCreationTimestamp().UTC().Format(time.RFC3339))
	}
}

// helpdesk's setup step (an init container, not a helper) failed and was
// started again 4 times while the cluster came up ("Back-off restarting
// failed container fetch-code"), then finished with exit 0. Kubernetes
// starts a setup step again only after it fails, so the back-off followed
// failures: nothing about it "keeps stopping" cleanly. Its status keeps
// only the last run (exit 0), lastState is empty.
func TestRealSetupStepThatFailed(t *testing.T) {
	w := realLoad(t, "deploy_setup.json")
	seen := false
	for _, c := range w.e.Changes("helpdesk", "", time.Now(), nil) {
		if !strings.Contains(c.Detail, `"fetch-code"`) {
			continue
		}
		seen = true
		if strings.Contains(c.Detail, "keeps stopping") || c.What == "restart" {
			t.Errorf("setup step fetch-code failed before each restart: %s %q", c.What, c.Detail)
		}
	}
	if !seen {
		t.Fatal("fixture: no change about fetch-code's back-off")
	}
}
