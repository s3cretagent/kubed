package view

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// nsStore lists and gets by namespace, as the real cache does.
type nsStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s nsStore) List(g schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, o := range s[g] {
		if ns == "" || o.GetNamespace() == ns {
			out = append(out, o)
		}
	}
	return out
}
func (s nsStore) Get(g schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s[g] {
		if o.GetName() == name && (ns == "" || o.GetNamespace() == ns) {
			return o, true
		}
	}
	return nil, false
}
func (nsStore) Synced(schema.GroupVersionResource) bool { return true }

// Round 113: web went 1.25 (A), i 1.26 (B), i 1.27 (C), i 1.25 (A
// again, as revision 4), u back to C (revision 5), r (R, revision 6).
// The namespace has other workloads made earlier. watched: kubed's watch
// saw each write as it happened. It returns web's app grouped by label
// (the namespace alone, no ArgoCD) and the namespace's app (all
// namespaces, ArgoCD on the cluster), and when the go-back was.
func r113Apps(t *testing.T, ns string, watched bool) (label, all *App, back time.Time) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	madeA := now.Add(-15 * time.Minute)
	madeB, madeC, backA, backC, madeR := now.Add(-55*time.Second), now.Add(-45*time.Second), now.Add(-35*time.Second), now.Add(-25*time.Second), now.Add(-15*time.Second)
	stamp := madeR.Format(time.RFC3339)
	template := func(image, stamp string) map[string]any {
		tmeta := map[string]any{"labels": map[string]any{"app": "web"}}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		return map[string]any{"metadata": tmeta, "spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": image}}}}
	}
	dep := func(rev, image, stamp string, at time.Time) *unstructured.Unstructured {
		d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "web", "namespace": ns, "uid": ns + "-web",
				"creationTimestamp": madeA.Format(time.RFC3339),
				"annotations":       map[string]any{"deployment.kubernetes.io/revision": rev}},
			"spec": map[string]any{"replicas": int64(1), "template": template(image, stamp)}}}
		d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
		return d
	}
	rs := func(name, rev, history string, made time.Time, image, stamp string, ready int64) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": ns, "uid": "r113-" + name, "annotations": ann,
				"creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": ns + "-web", "controller": true}}},
			"spec":   map[string]any{"replicas": ready, "template": template(image, stamp)},
			"status": map[string]any{"replicas": ready, "readyReplicas": ready}}}
	}
	podOf := func(name, owner, ownerKind, ownerUID string, made time.Time, labels map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": ns, "uid": "r113-" + name, "labels": labels,
				"creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": ownerKind, "name": owner, "uid": ownerUID, "controller": true}}},
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running"}}}
	}
	ev := func(name, msg string, count int64, first, last time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": ns}, "reason": "ScalingReplicaSet", "message": msg, "count": count,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
			"firstTimestamp": first.Format(time.RFC3339), "lastTimestamp": last.Format(time.RFC3339)}}
	}

	// kubed's watch saw each write as it happened.
	web := dep("6", "nginx:1.27", stamp, madeR)
	if watched {
		for _, d := range []*unstructured.Unstructured{dep("1", "nginx:1.25", "", madeA), dep("2", "nginx:1.26", "", madeB),
			dep("3", "nginx:1.27", "", madeC), dep("4", "nginx:1.25", "", backA), dep("5", "nginx:1.27", "", backC), web} {
			kube.NoteTemplateChange(d)
		}
	}

	// Another deployment, stuck on its first version, made earlier.
	crash := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "crash", "namespace": ns, "uid": "r113-crash", "creationTimestamp": madeA.Format(time.RFC3339)},
		"spec":     map[string]any{"replicas": int64(1), "template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "crash"}}}}}}
	crashRS := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "crash-1", "namespace": ns, "uid": "r113-crash-1", "creationTimestamp": madeA.Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "crash", "uid": "r113-crash", "controller": true}}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "busybox"}}}}}}}

	store := nsStore{
		kube.Deployments: {web, crash},
		kube.ReplicaSets: {
			rs("web-a", "4", "1", madeA, "nginx:1.25", "", 0),
			rs("web-b", "2", "", madeB, "nginx:1.26", "", 0),
			rs("web-c", "5", "3", madeC, "nginx:1.27", "", 0),
			rs("web-r", "6", "", madeR, "nginx:1.27", stamp, 1),
			crashRS,
		},
		kube.Events: {
			ev("up-a", "Scaled up replica set web-a from 0 to 1", 2, madeA, backA),     // merged
			ev("down-a", "Scaled down replica set web-a from 1 to 0", 2, madeB, backC), // merged
			ev("up-b", "Scaled up replica set web-b from 0 to 1", 1, madeB, madeB),
			ev("down-b", "Scaled down replica set web-b from 1 to 0", 1, madeC, madeC),
			ev("up-c", "Scaled up replica set web-c from 0 to 1", 2, madeC, backC), // merged
			ev("down-c", "Scaled down replica set web-c from 1 to 0", 2, backA, madeR),
			ev("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR),
		},
	}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
		{GVR: kube.Applications, Kind: "Application", Namespaced: true},
	})
	pods := []*unstructured.Unstructured{
		podOf("crash-1-x", "crash-1", "ReplicaSet", "r113-crash-1", madeA, map[string]any{"app": "crash"}),
		podOf("web-r-x", "web-r", "ReplicaSet", "r113-web-r", madeR, map[string]any{"app": "web", "pod-template-hash": "r"}),
	}
	workloads := []*unstructured.Unstructured{web, crash}
	argo := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "other", "namespace": "argocd"},
		"spec":     map[string]any{"destination": map[string]any{"namespace": "other"}}}}

	find := func(apps []*App, name string) *App {
		for _, a := range apps {
			if a.Name == name && a.Namespace == ns {
				return a
			}
		}
		t.Fatalf("no app %s in %s", name, ns)
		return nil
	}
	label = find(Apps(enrich.New(store, res, nil), pods, nil, nil, nil, workloads), "web")
	all = find(Apps(enrich.New(store, res, nil), pods, []*unstructured.Unstructured{argo}, nil, nil, workloads), ns)
	return label, all, backC
}

func deployedWords(a *App) string {
	return fmt.Sprintf("%v (after %v, before %v)", a.LastDeploy.UTC().Format(time.TimeOnly), a.DeployAfter, a.DeployBefore)
}

// Grouped by namespace or by label, web is dated the same: the go-back.
func TestDeployedSameGroupedByNamespace(t *testing.T) {
	label, all, back := r113Apps(t, "r113b", true)
	if !label.LastDeploy.Equal(back) || label.DeployAfter || label.DeployBefore {
		t.Errorf("grouped by label: deployed %s, want the go-back %v", deployedWords(label), back.Format(time.TimeOnly))
	}
	if deployedWords(all) != deployedWords(label) {
		t.Errorf("grouped by namespace: deployed %s, grouped by label %s", deployedWords(all), deployedWords(label))
	}
}

// A kubed started after the go-back and the restart (QA's kubed -n all):
// the scale events are merged, so nothing left dates the go-back. Not
// when the version brought back was first made, as if exact: after it.
func TestDeployedGoBackUnseenThenRestartIsABound(t *testing.T) {
	label, all, back := r113Apps(t, "r113c", false)
	for _, a := range []*App{label, all} {
		if !a.LastDeploy.Equal(back) && (!a.DeployAfter || !a.LastDeploy.Before(back)) {
			t.Errorf("%s: deployed %s, want the go-back %v or a time before it with after", a.Name, deployedWords(a), back.Format(time.TimeOnly))
		}
	}
	if deployedWords(all) != deployedWords(label) {
		t.Errorf("grouped by namespace: deployed %s, grouped by label %s", deployedWords(all), deployedWords(label))
	}
}

// Review of area B: an app's Deployed is the latest of its workloads'. One
// dated exactly, another only as "then or later" from earlier: the second
// may be the later, so the app's is a bound, never the first's exact time.
// One whose settings' first time out isn't known says so, in words.
func TestDeployedSettlesAcrossWorkloads(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ten, nine := now.Add(-10*time.Minute), now.Add(-20*time.Minute)
	for _, c := range []struct {
		name          string
		deploys       []enrich.DeployTime
		want          time.Time
		after, before bool
	}{
		{"all exact", []enrich.DeployTime{{At: ten}, {At: nine}}, ten, false, false},
		{"a bound may be later", []enrich.DeployTime{{At: ten}, {At: nine, After: true}}, ten, true, false},
		{"an unknown may be later", []enrich.DeployTime{{At: ten}, {At: nine, After: true, Unknown: true}}, ten, true, false},
		{"an earlier then-or-earlier can't be later", []enrich.DeployTime{{At: ten}, {At: nine, Before: true}}, ten, false, false},
		{"a later then-or-earlier may be", []enrich.DeployTime{{At: nine}, {At: ten, Before: true}}, nine, true, false},
		// Review: a then-or-earlier next to a brought-back-after: it said
		// nothing; the latest is that bound or later.
		{"unknown after next to then-or-earlier", []enrich.DeployTime{{At: nine, After: true, Unknown: true}, {At: ten, Before: true}}, nine, true, false},
	} {
		a := &App{deploys: c.deploys}
		settleDeployed(a)
		if !a.LastDeploy.Equal(c.want) || a.DeployAfter != c.after || a.DeployBefore != c.before {
			t.Errorf("%s: %s, want %s (after %v, before %v)", c.name, deployedWords(a), c.want.Format(time.TimeOnly), c.after, c.before)
		}
	}

	a := &App{Name: "web", deploys: []enrich.DeployTime{{At: nine, After: true, Unknown: true}}}
	settleDeployed(a)
	if cell := AppRows([]*App{a})[0].Cells[5]; cell == "-" || cell == "≤"+Age(nine)+" ago" {
		t.Errorf("home's Deployed cell reads %q for a time not known", cell)
	}
	box := strings.Join(AppLines(a, Lookups{}), "\n")
	if !strings.Contains(box, "brought back after") || !strings.Contains(box, "unknown") {
		t.Errorf("the box doesn't say what is known:\n%s", box)
	}
}
