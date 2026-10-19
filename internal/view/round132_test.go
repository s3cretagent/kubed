package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 132: a pod waits in CreateContainerConfigError because a key is
// missing from a Secret that exists. Its Fix said to create the missing
// secret, the app's box (Bad config) had no Fix, and a stuck first rollout
// said "fix the image or settings". The Fix says what the kubelet says: add
// the key, or fix the key name; a missing Secret keeps the old words.
func TestBadConfigFixSaysWhatIsMissing(t *testing.T) {
	cases := []struct{ why, want, never string }{
		{`app can't start: couldn't find key apitoken in Secret kubed-qa132/dbcreds`,
			"add key apitoken to secret dbcreds, or fix the key name it refers to", "create the missing"},
		{`app can't start: couldn't find key LEVEL in ConfigMap kubed-qa132/settings`,
			"add key LEVEL to config map settings, or fix the key name it refers to", "create the missing"},
		{`app can't start: secret "dbcreds" not found`,
			"create the missing secret or config map, or fix the name it refers to", "add key"},
		{`app can't start: configmap "settings" not found`,
			"create the missing secret or config map, or fix the name it refers to", "add key"},
	}
	for _, c := range cases {
		st := Status{Short: "Bad config", Why: c.why, Tone: ToneBad}
		pod := podFixFor(st, "ReplicaSet")
		app := appFix(&App{Status: st}, false)
		stuck := appFix(&App{Status: Status{Short: "Rollout stuck", Why: "first version of deployment/api " + cantStartWhy(st)}, stuckBy: st}, false)
		for name, fix := range map[string]string{"pod": pod, "app": app, "stuck rollout": stuck} {
			if !strings.Contains(fix, c.want) || strings.Contains(fix, c.never) {
				t.Errorf("%s Fix for %q = %q, want %q", name, c.why, fix, c.want)
			}
		}
	}
}

// r132Obj is a test object from a map.
func r132Obj(apiVersion, kind, name string, fields map[string]any) *unstructured.Unstructured {
	o := map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": map[string]any{"name": name, "namespace": "qa"}}
	for k, v := range fields {
		if k == "labels" {
			o["metadata"].(map[string]any)["labels"] = v
			continue
		}
		o[k] = v
	}
	return &unstructured.Unstructured{Object: o}
}

func r132Pod(name string, ready string) *unstructured.Unstructured {
	return r132Obj("v1", "Pod", name, map[string]any{"labels": map[string]any{"app": "web"},
		"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": ready}},
			"containerStatuses": []any{map[string]any{"name": "app", "ready": ready == "True", "state": map[string]any{"running": map[string]any{}}}}}})
}

// loadingStore: nothing has loaded yet.
type loadingStore struct{ mxStore }

func (loadingStore) Synced(schema.GroupVersionResource) bool { return false }

func detailHas(lines []string, words string) bool {
	return strings.Contains(strings.Join(lines, "\n"), words)
}

// QA round 132: a service whose only pod runs but isn't ready (its
// EndpointSlice says ready: false) showed no problem and wasn't counted to
// check: nothing answers on it.
func TestServiceWithNoReadyPodIsAProblem(t *testing.T) {
	v := For(kube.Resource{GVR: kube.Services, Kind: "Service", Namespaced: true})
	svc := func(fields map[string]any) *unstructured.Unstructured {
		return r132Obj("v1", "Service", "web", fields)
	}
	sel := map[string]any{"selector": map[string]any{"app": "web"}, "type": "ClusterIP"}
	cases := []struct {
		name  string
		svc   *unstructured.Unstructured
		pods  []*unstructured.Unstructured
		words string // "" no problem
	}{
		{"1 pod not ready", svc(map[string]any{"spec": sel}), []*unstructured.Unstructured{r132Pod("a", "False")}, "its 1 pod isn't ready, so nothing answers on it"},
		{"2 pods not ready", svc(map[string]any{"spec": sel}), []*unstructured.Unstructured{r132Pod("a", "False"), r132Pod("b", "False")}, "its 2 pods aren't ready, so nothing answers on it"},
		{"one ready", svc(map[string]any{"spec": sel}), []*unstructured.Unstructured{r132Pod("a", "False"), r132Pod("b", "True")}, ""},
		{"no pods", svc(map[string]any{"spec": sel}), nil, "no running pod matches it"},
		{"publishes not-ready pods", svc(map[string]any{"spec": map[string]any{"selector": map[string]any{"app": "web"}, "publishNotReadyAddresses": true}}),
			[]*unstructured.Unstructured{r132Pod("a", "False")}, ""},
		{"no selector", svc(map[string]any{"spec": map[string]any{"type": "ClusterIP"}}), []*unstructured.Unstructured{r132Pod("a", "False")}, ""},
		{"ExternalName", svc(map[string]any{"spec": map[string]any{"type": "ExternalName", "externalName": "x.example.com"}}), nil, ""},
	}
	for _, c := range cases {
		e := enrich.New(mxStore{listStore{kube.Pods: c.pods}}, nil, nil)
		_, tone := v.Row(e, c.svc)
		lines := v.Detail(e, c.svc)
		if c.words == "" {
			if tone == ToneWarn || detailHas(lines, "Problem") {
				t.Errorf("%s: flagged (%v): %q", c.name, tone, lines)
			}
			continue
		}
		if tone != ToneWarn || !detailHas(lines, "Problem  "+c.words) {
			t.Errorf("%s: tone %v, lines %q, want %q", c.name, tone, lines, c.words)
		}
	}
}

// QA round 132: an ingress whose service doesn't exist showed no problem
// and wasn't counted to check. HTTPRoutes too. Not while services load.
func TestRouteToAMissingServiceIsAProblem(t *testing.T) {
	ing := r132Obj("networking.k8s.io/v1", "Ingress", "web", map[string]any{"spec": map[string]any{"rules": []any{map[string]any{"host": "host",
		"http": map[string]any{"paths": []any{
			map[string]any{"path": "/", "backend": map[string]any{"service": map[string]any{"name": "missing-svc", "port": map[string]any{"number": int64(80)}}}},
			map[string]any{"path": "/ok", "backend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"number": int64(80)}}}},
		}}}}}})
	route := r132Obj("gateway.networking.k8s.io/v1", "HTTPRoute", "web", map[string]any{"spec": map[string]any{"hostnames": []any{"r.example.com"},
		"parentRefs": []any{map[string]any{"name": "gw"}},
		"rules":      []any{map[string]any{"backendRefs": []any{map[string]any{"name": "missing-svc", "port": int64(80)}, map[string]any{"name": "web", "port": int64(80)}}}}},
		"status": map[string]any{"parents": []any{map[string]any{"conditions": []any{map[string]any{"type": "Accepted", "status": "True"}}}}}})
	web := r132Obj("v1", "Service", "web", map[string]any{"spec": map[string]any{"selector": map[string]any{"app": "web"}}})
	for _, c := range []struct {
		gvr schema.GroupVersionResource
		o   *unstructured.Unstructured
	}{{kube.Ingresses, ing}, {kube.HTTPRoutes, route}} {
		v := For(kube.Resource{GVR: c.gvr, Namespaced: true})
		e := enrich.New(mxStore{listStore{kube.Services: {web}}}, nil, nil)
		_, tone := v.Row(e, c.o)
		lines := v.Detail(e, c.o)
		if tone != ToneWarn || !detailHas(lines, "Problem  service missing-svc isn't there") || detailHas(lines, "service web isn't") {
			t.Errorf("%s: tone %v, lines %q", c.o.GetKind(), tone, lines)
		}
		e = enrich.New(loadingStore{mxStore{listStore{}}}, nil, nil)
		if _, tone := v.Row(e, c.o); tone == ToneWarn || detailHas(v.Detail(e, c.o), "isn't there") {
			t.Errorf("%s: called missing while services load: %q", c.o.GetKind(), v.Detail(e, c.o))
		}
	}
}

// r132Paused is deployment pz paused partway through a rollout: it asks for
// 2, runs 3 (2 old, 1 new), all ready; kubectl shows READY 3/2, UP-TO-DATE
// 1. updated sets status.updatedReplicas.
func r132Paused(updated int64) (*enrich.Enricher, *unstructured.Unstructured, []*unstructured.Unstructured) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	status := map[string]any{"replicas": int64(3), "updatedReplicas": updated, "readyReplicas": int64(3), "availableReplicas": int64(3), "observedGeneration": int64(2),
		"conditions": []any{map[string]any{"type": "Progressing", "status": "Unknown", "reason": "DeploymentPaused"}}}
	if updated == 2 {
		status["replicas"] = int64(2)
		status["readyReplicas"] = int64(2)
	}
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "pz", "namespace": "qa", "uid": "pzd", "generation": int64(2), "labels": map[string]any{"app": "pz"}},
		"spec":     map[string]any{"replicas": int64(2), "paused": true, "selector": map[string]any{"matchLabels": map[string]any{"app": "pz"}}},
		"status":   status}}
	rs := func(name string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name,
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "pz", "uid": "pzd", "controller": true}}}}}
	}
	pod := func(name, owner string) *unstructured.Unstructured {
		at := time.Now().Add(-time.Hour).Format(time.RFC3339)
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "labels": map[string]any{"app": "pz"}, "creationTimestamp": at,
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": owner, "uid": owner, "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
				"containerStatuses": []any{map[string]any{"name": "c", "ready": true, "state": map[string]any{"running": map[string]any{"startedAt": at}}}}}}}
	}
	pods := []*unstructured.Unstructured{pod("pz-old-1", "pz-old"), pod("pz-old-2", "pz-old"), pod("pz-new-1", "pz-new")}
	if updated == 2 {
		pods = []*unstructured.Unstructured{pod("pz-new-1", "pz-new"), pod("pz-new-2", "pz-new")}
	}
	e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs("pz-old"), rs("pz-new")}}, res, nil)
	return e, dep, pods
}

// QA round 132: a deployment paused partway through a rollout read
// "Running 3/3, healthy" on home and nothing in its box. It says it is
// paused partway; READY counts as kubectl does (3/2).
func TestPausedPartwaySaysSo(t *testing.T) {
	const want = "rolling out, 1 of 2 updated; nothing more changes until it is resumed"
	e, dep, pods := r132Paused(1)
	found := false
	for _, a := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if a.Name != "pz" {
			continue
		}
		found = true
		if a.Status.Short != "Paused" || a.Status.Why != want {
			t.Errorf("home: %+v, want Paused: %s", a.Status, want)
		}
		if got := readyCell(a); got != "3/2" {
			t.Errorf("home READY %s, want 3/2 as kubectl", got)
		}
	}
	if !found {
		t.Fatal("no app pz")
	}
	if got := strings.Join(workloadDetail(e, dep), "\n"); !strings.Contains(got, "Paused   "+want) {
		t.Errorf("box: %s", got)
	}

	// Paused with all copies updated: home as before; its box says it is
	// paused, as nothing else does (QA round 152).
	e, dep, pods = r132Paused(2)
	for _, a := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if a.Name == "pz" && (a.Status.Short != "Running" || readyCell(a) != "2/2") {
			t.Errorf("all updated: %+v %s", a.Status, readyCell(a))
		}
	}
	if got := strings.Join(workloadDetail(e, dep), "\n"); strings.Contains(got, "rolling out") || !strings.Contains(got, "Paused   a change to it rolls out only once it is resumed") {
		t.Errorf("all updated box: %s", got)
	}
}

// QA round 132: a cron job that forbids two runs at once, its run still
// going, said "Next run in 17s", and none started then. While a run is
// going, the Next line says what its concurrencyPolicy does.
func TestNextRunFollowsConcurrencyPolicy(t *testing.T) {
	e := enrich.New(mxStore{listStore{}}, nil, nil)
	cj := func(policy string, active bool) *unstructured.Unstructured {
		spec := map[string]any{"schedule": "* * * * *"}
		if policy != "" {
			spec["concurrencyPolicy"] = policy
		}
		st := map[string]any{}
		if active {
			st["active"] = []any{map[string]any{"kind": "Job", "name": "tick-1", "namespace": "qa"}}
		}
		return r132Obj("batch/v1", "CronJob", "tick", map[string]any{"spec": spec, "status": st})
	}
	next := func(o *unstructured.Unstructured) string {
		for _, l := range cronDetail(e, o) {
			if strings.HasPrefix(l, "Next run") {
				return l
			}
		}
		return ""
	}
	if got := next(cj("Forbid", true)); !strings.HasPrefix(got, "Next run in ") || !strings.HasSuffix(got, ": skipped if this run is still going then (it allows one at a time)") {
		t.Errorf("Forbid, running: %q", got)
	}
	if got := next(cj("Replace", true)); !strings.HasPrefix(got, "Next run in ") || !strings.HasSuffix(got, ": if this run is still going then, it is stopped and replaced") {
		t.Errorf("Replace, running: %q", got)
	}
	for _, o := range []*unstructured.Unstructured{cj("", true), cj("Allow", true), cj("Forbid", false), cj("Replace", false)} {
		if got := next(o); !strings.HasPrefix(got, "Next run in ") || strings.Contains(got, ":") && strings.Contains(got, "replaces") {
			t.Errorf("%v: %q", o.Object["spec"], got)
		}
	}
}

// QA round 132: an app whose only pod, a bare one, was evicted left home:
// nothing makes it again, so it stays, with that problem. A controller's
// evicted pod is replaced: as before.
func TestEvictedBarePodKeepsItsApp(t *testing.T) {
	pod := func(owner string) *unstructured.Unstructured {
		md := map[string]any{"name": "evictme", "namespace": "qa", "uid": "ev1", "labels": map[string]any{"app": "evictme"},
			"creationTimestamp": time.Now().Add(-time.Hour).Format(time.RFC3339)}
		if owner != "" {
			md["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": owner, "uid": owner, "controller": true}}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": md,
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Failed", "reason": "Evicted", "message": "The node was low on resource: memory."}}}
	}
	p := pod("")
	e := enrich.New(mxStore{listStore{kube.Pods: {p}}}, nil, nil)
	var app *App
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil) {
		if a.Name == "evictme" {
			app = a
		}
	}
	if app == nil {
		t.Fatal("app evictme left home")
	}
	if app.Status.Tone != ToneBad || !strings.Contains(app.Status.Why, "its pod was evicted") || !strings.Contains(app.Status.Why, "nothing makes it again") {
		t.Errorf("status %+v", app.Status)
	}
	// QA round 133: its box must not say the pod was replaced.
	if box := strings.Join(AppLines(app, Lookups{}), "\n"); strings.Contains(box, "already replaced") || !strings.Contains(box, "nothing replaces it") {
		t.Errorf("box says %q", box)
	}
	// Owned by a ReplicaSet that is gone: as before (not an app).
	p = pod("rs-gone")
	e = enrich.New(mxStore{listStore{kube.Pods: {p}}}, nil, nil)
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil) {
		if a.Name == "evictme" {
			t.Errorf("a controller's evicted pod: %+v", a.Status)
		}
	}
}

// QA round 132: a pod whose ready check failed from 2s after it started
// read "main is starting up" for 80s. Past the check's initial delay, with
// a failure of this run's ready check, it says it isn't ready yet, and why.
func TestStartingSaysWhyItIsNotReadyYet(t *testing.T) {
	mk := func(delay int32) *corev1.Pod {
		p := mxPod("r132", "ReplicaSet", corev1.PodRunning, mxC{name: "main", state: mxRun(30 * time.Second)})
		p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{InitialDelaySeconds: delay}
		return p
	}
	ev := mxEvent("r132.1", "r132", "main", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503", 28*time.Second)
	e := mxEnricher(map[string][]*unstructured.Unstructured{"events": {ev}})
	st := Explain(e, mk(0))
	if st.Short != "Starting" || st.Why != "main isn't ready yet: ready check failed: answered 503 (28s ago)" {
		t.Errorf("failing from the start: %+v", st)
	}
	if st := Explain(e, mk(60)); st.Why != "main is starting up" {
		t.Errorf("within its initial delay: %+v", st)
	}
	if st := Explain(mxEnricher(nil), mk(0)); st.Why != "main is starting up" {
		t.Errorf("no failure: %+v", st)
	}
	// A startup check failing: still starting up.
	ev = mxEvent("r132.2", "r132", "main", "Unhealthy", "Startup probe failed: HTTP probe failed with statuscode: 503", 28*time.Second)
	if st := Explain(mxEnricher(map[string][]*unstructured.Unstructured{"events": {ev}}), mk(0)); st.Why != "main is starting up" {
		t.Errorf("startup check: %+v", st)
	}
}
