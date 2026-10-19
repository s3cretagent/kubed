package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// r136Deploy: deployment web asking for 2 copies after an image change
// (minReadySeconds 30), with the given status, its new version's pods
// and the old version's.
func r136Deploy(status map[string]any, newPods, oldPods int) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "mxa", "uid": "dep-web", "generation": int64(2), "labels": map[string]any{"app": "mx"},
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
		"spec": map[string]any{"replicas": int64(2), "minReadySeconds": int64(30), "selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}},
		"status": status}}
	rs := func(name, rev, image string, n int, made time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "mxa", "uid": "uid-" + name, "creationTimestamp": time.Now().Add(-made).UTC().Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}},
			"spec":   map[string]any{"replicas": int64(n), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}},
			"status": map[string]any{"replicas": int64(n), "readyReplicas": int64(n)}}}
	}
	var pods []*unstructured.Unstructured
	add := func(name, rsName string, ago time.Duration) {
		p := mxPod(name, "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(ago), ready: true})
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = rsName, types.UID("uid-"+rsName)
		p.CreationTimestamp = mxAgo(ago)
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		pods = append(pods, mxObj(p))
	}
	for i := range newPods {
		add("web-2-"+string(rune('a'+i)), "web-2", 20*time.Second)
	}
	for i := range oldPods {
		add("web-1-"+string(rune('a'+i)), "web-1", time.Hour)
	}
	s := listStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs("web-2", "2", "nginx:2", newPods, time.Minute), rs("web-1", "1", "nginx:1", oldPods, 2*time.Hour)}}
	e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}), nil)
	return e, pods, dep
}

func r136App(t *testing.T, e *enrich.Enricher, pods []*unstructured.Unstructured, workloads ...*unstructured.Unstructured) *App {
	t.Helper()
	for _, x := range Apps(e, pods, nil, nil, nil, workloads) {
		if x.Name == "mx" {
			return x
		}
	}
	t.Fatal("no app mx")
	return nil
}

// QA round 136: with its old pod still there (pending termination, as
// kubectl rollout status says) home said "Running: healthy", READY 3/3.
// It is rolling out, and READY counts as kubectl does (3/2).
func TestHomeSaysRollingOutWhileOldPodsLeft(t *testing.T) {
	e, pods, dep := r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(3), "updatedReplicas": int64(2),
		"readyReplicas": int64(3), "availableReplicas": int64(2), "unavailableReplicas": int64(1)}, 2, 1)
	a := r136App(t, e, pods, dep)
	if a.Status.Short != "Rolling out" || !strings.Contains(a.Status.Why, "2 of 2 updated") || !strings.Contains(a.Status.Why, "1 pod of the old version") {
		t.Errorf("status %+v", a.Status)
	}
	if got := readyCell(a); got != "3/2" {
		t.Errorf("READY %s, want 3/2 as kubectl counts", got)
	}
	// Done: healthy again.
	e, pods, dep = r136Deploy(map[string]any{"observedGeneration": int64(2), "replicas": int64(2), "updatedReplicas": int64(2),
		"readyReplicas": int64(2), "availableReplicas": int64(2)}, 2, 0)
	if a := r136App(t, e, pods, dep); a.Status != (Status{"Running", "healthy", ToneNormal}) || readyCell(a) != "2/2" {
		t.Errorf("done: status %+v, READY %s", a.Status, readyCell(a))
	}
}

// r136Revisions: a daemon set or stateful set "web" whose pod (of its
// newest version) can't pull its image, with ControllerRevisions of the
// given images, oldest first.
func r136Revisions(kind string, oldFails bool, images ...string) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	w := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": kind,
		"metadata": map[string]any{"name": "web", "namespace": "mxa", "uid": "w-web"},
		"spec": map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}}}}
	if kind == "StatefulSet" {
		w.Object["spec"].(map[string]any)["replicas"] = int64(1)
	}
	var crs []*unstructured.Unstructured
	for i, img := range images {
		crs = append(crs, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ControllerRevision",
			"metadata": map[string]any{"name": "web-h" + string(rune('0'+i)), "namespace": "mxa", "uid": "cr-" + string(rune('0'+i)),
				"creationTimestamp": time.Now().Add(-time.Duration(len(images)-i) * time.Hour).UTC().Format(time.RFC3339),
				"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": kind, "name": "web", "uid": "w-web", "controller": true}}},
			"revision": int64(i + 1),
			"data":     map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": img}}}}}}}})
	}
	p := mxPod("web-x", kind, corev1.PodPending, mxC{name: "app", state: mxWait("ImagePullBackOff", `Back-off pulling image "`+images[len(images)-1]+`"`)})
	p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web", "w-web"
	p.Labels["controller-revision-hash"] = "h" + string(rune('0'+len(images)-1))
	if kind == "StatefulSet" {
		p.Labels["controller-revision-hash"] = "web-h" + string(rune('0'+len(images)-1))
	}
	pods := []*unstructured.Unstructured{mxObj(p)}
	if oldFails { // a pod of the version before, crashing
		o := mxPod("web-y", kind, corev1.PodRunning, mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off"), restarts: 5, last: mxEnd("Error", 1, time.Minute)})
		o.OwnerReferences[0].Name, o.OwnerReferences[0].UID = "web", "w-web"
		o.Labels["controller-revision-hash"] = "h0"
		if kind == "StatefulSet" {
			o.Labels["controller-revision-hash"] = "web-h0"
		}
		pods = append(pods, mxObj(o))
	}
	gvr := map[string]kube.Resource{"DaemonSet": {GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true}, "StatefulSet": {GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true}}[kind]
	s := listStore{kube.Pods: pods, gvr.GVR: {w}, kube.ControllerRevisions: crs}
	e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, gvr,
		{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true}}), nil)
	return e, pods, w
}

// QA round 136: the Fix of a daemon set (2 ControllerRevisions) and a
// stateful set (3) said "there is no earlier version to go back to",
// while u on them goes back. It answers as u does; with one version kept
// it still says there is none.
func TestFixGoesBackForDaemonAndStatefulSets(t *testing.T) {
	for _, kind := range []string{"DaemonSet", "StatefulSet"} {
		t.Run(kind, func(t *testing.T) {
			e, pods, w := r136Revisions(kind, false, "nginx:1", "nginx:no-such-tag")
			a := r136App(t, e, pods, w)
			if !strings.HasPrefix(a.Status.Short, "Can't pull image") {
				t.Fatalf("status %+v", a.Status)
			}
			if fix := appFix(a, false); strings.Contains(fix, noEarlierVersion) || !strings.Contains(fix, "u to go back") {
				t.Errorf("fix %q", fix)
			}
			e, pods, w = r136Revisions(kind, false, "nginx:no-such-tag")
			if fix := appFix(r136App(t, e, pods, w), false); !strings.Contains(fix, noEarlierVersion) {
				t.Errorf("one version: fix %q", fix)
			}
			e, pods, w = r136Revisions(kind, true, "nginx:1", "nginx:no-such-tag")
			a = r136App(t, e, pods, w)
			if fix := appFix(a, false); strings.Contains(fix, "u to go back") || strings.Contains(fix, noEarlierVersion) {
				t.Errorf("version before fails too: %+v, fix %q", a.Status, fix)
			}
		})
	}
}

// QA round 136: a deployment made today at 14:35 that keeps no history
// (revisionHistoryLimit 0), restarted at 14:46, said Deployed "before 16
// Oct": it went out no earlier than it was made. Between the two.
func TestDeployedBeforeIsBoundedByCreation(t *testing.T) {
	made, restarted := time.Now().Add(-11*time.Minute).UTC().Truncate(time.Second), time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "mxa", "uid": "dep-web", "creationTimestamp": made.Format(time.RFC3339),
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
		"spec":   map[string]any{"replicas": int64(1), "revisionHistoryLimit": int64(0), "template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}},
		"status": map[string]any{"replicas": int64(1), "updatedReplicas": int64(1), "readyReplicas": int64(1), "availableReplicas": int64(1)}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-2", "namespace": "mxa", "uid": "uid-web-2", "creationTimestamp": restarted.Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}},
		"spec": map[string]any{"replicas": int64(1), "template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": restarted.Format(time.RFC3339)}},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nginx:1"}}}}},
		"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}}}
	p := mxPod("web-2-a", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Minute), ready: true})
	p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web-2", "uid-web-2"
	p.CreationTimestamp = mxAgo(time.Minute)
	pods := []*unstructured.Unstructured{mxObj(p)}
	s := nsStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs}}
	e := enrich.New(s, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}), nil)
	a := r136App(t, e, pods, dep)
	var line string
	for _, l := range AppLines(a, Lookups{}) {
		if strings.HasPrefix(l, "Deployed") {
			line = l
		}
	}
	if !strings.Contains(line, "between "+DateWords(made, true)) || strings.Contains(line, "before "+DateWords(made, false)+" ") {
		t.Errorf("%q", line)
	}
}

// QA round 136: a daemon set whose node selector matches no node
// (desiredNumberScheduled 0) left home, and no screen said why. Home keeps
// it, as a stateful set at 0 copies, and its Why and its box say it runs
// on no node; "matches none" only when its selector is set and no node
// has those labels (taints kubed can't weigh: only that it runs on none).
func TestDaemonSetOnNoNode(t *testing.T) {
	node := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{"name": "n1", "labels": map[string]any{"kubernetes.io/os": "linux"}}}}
	for _, c := range []struct {
		name     string
		selector map[string]any
		want     string
	}{
		{"selector matches no node", map[string]any{"disk": "nvme"}, "it runs on no node: its node selector matches none"},
		{"selector matches a node (a taint keeps it off)", map[string]any{"kubernetes.io/os": "linux"}, "it runs on no node now"},
		{"no selector", nil, "it runs on no node now"},
	} {
		t.Run(c.name, func(t *testing.T) {
			spec := map[string]any{"containers": []any{map[string]any{"name": "agent", "image": "agent:1"}}}
			if c.selector != nil {
				spec["nodeSelector"] = c.selector
			}
			ds := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet",
				"metadata": map[string]any{"name": "agent", "namespace": "mxa", "uid": "ds-agent", "generation": int64(1)},
				"spec": map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}},
					"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}, "spec": spec}},
				"status": map[string]any{"observedGeneration": int64(1), "desiredNumberScheduled": int64(0), "currentNumberScheduled": int64(0), "numberReady": int64(0)}}}
			s := listStore{kube.DaemonSets: {ds}, kube.Nodes: {node}}
			e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
				{GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true}, {GVR: kube.Nodes, Kind: "Node"}}), nil)
			a := r136App(t, e, nil, ds)
			if a.Status.Why != c.want || a.Status.Tone == ToneBad {
				t.Errorf("status %+v", a.Status)
			}
			if box := strings.Join(workloadDetail(e, ds), "\n"); !strings.Contains(box, c.want) {
				t.Errorf("box %q", box)
			}
		})
	}
}
