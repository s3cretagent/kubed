package view

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// argoApp is an Application with the given status.
func argoApp(status map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "shop", "namespace": "argocd"},
		"spec":     map[string]any{"destination": map[string]any{"namespace": "shop"}},
		"status":   status,
	}}
}

func appDetail(o *unstructured.Unstructured) string {
	return strings.Join(For(kube.Resource{GVR: kube.Applications}).Detail(nil, o), "\n")
}

// QA round 144: the ga box didn't say why an app fails or what is out of
// sync, and "Synced last time: succeeded" read as "in sync now".
func TestArgoAppBoxSaysWhy(t *testing.T) {
	// Failing, with no resource marked failing: ArgoCD's own message.
	got := appDetail(argoApp(map[string]any{
		"sync":   map[string]any{"status": "Synced"},
		"health": map[string]any{"status": "Degraded", "message": "Deployment web has timed out progressing"},
	}))
	if !strings.Contains(got, "timed out progressing") {
		t.Errorf("failing app says no why:\n%s", got)
	}
	// Health unknown: the first part that isn't healthy, with its message.
	got = appDetail(argoApp(map[string]any{
		"sync":   map[string]any{"status": "Synced"},
		"health": map[string]any{"status": "Unknown"},
		"resources": []any{
			map[string]any{"kind": "Service", "name": "web", "health": map[string]any{"status": "Healthy"}},
			map[string]any{"kind": "Deployment", "name": "worker", "health": map[string]any{"status": "Unknown", "message": "no status yet"}},
		},
	}))
	if !strings.Contains(got, "deployment worker") || !strings.Contains(got, "no status yet") {
		t.Errorf("unknown health names no part:\n%s", got)
	}
	// Out of sync: the parts that differ, the first few.
	var res []any
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		res = append(res, map[string]any{"kind": "ConfigMap", "name": n, "status": "OutOfSync"})
	}
	finished := time.Now().Add(-2 * time.Minute).UTC()
	got = appDetail(argoApp(map[string]any{
		"sync":      map[string]any{"status": "OutOfSync"},
		"health":    map[string]any{"status": "Healthy"},
		"resources": res,
		"operationState": map[string]any{"phase": "Succeeded", "message": "successfully synced (all tasks run)",
			"finishedAt": finished.Format(time.RFC3339)},
	}))
	if !strings.Contains(got, "configmap/a, configmap/b, configmap/c and 2 more differ from Git") {
		t.Errorf("out of sync names no parts:\n%s", got)
	}
	want := "Last sync 2m ago (" + DateWords(finished, true) + "): succeeded"
	if !strings.Contains(got, want) || strings.Contains(got, "Synced   last time") {
		t.Errorf("want %q:\n%s", want, got)
	}
}

// r144Deploy is deployment web asking for want copies, its ReplicaSet,
// and pods with containers ready as given (running an hour, failing their
// readiness probe when not ready).
func r144Deploy(want int64, tweak func(*corev1.Pod), ready ...bool) (*enrich.Enricher, []*unstructured.Unstructured, *unstructured.Unstructured) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "mxa", "uid": "r144-web", "creationTimestamp": mxTime(3 * time.Hour)},
		"spec": map[string]any{"replicas": want, "selector": map[string]any{"matchLabels": map[string]any{"app": "mx"}},
			"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "mx"}}}},
		"status": map[string]any{"replicas": int64(len(ready)), "updatedReplicas": int64(len(ready))}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-1", "namespace": "mxa", "uid": "r144-web-1", "creationTimestamp": mxTime(3 * time.Hour),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "r144-web", "controller": true}}},
		"spec": map[string]any{"replicas": want}}}
	var pods []*unstructured.Unstructured
	var evs []*unstructured.Unstructured
	n := int64(0)
	for i, r := range ready {
		name := fmt.Sprintf("web-1-p%d", i)
		p := mxPod(name, "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(time.Hour), ready: r})
		p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "web-1", "r144-web-1"
		start := mxAgo(time.Hour)
		p.Status.StartTime = &start
		cond := corev1.ConditionFalse
		if r {
			cond = corev1.ConditionTrue
			n++
		} else {
			evs = append(evs, mxEvent(name+".1", name, "app", "Unhealthy", "Readiness probe failed: dial tcp 10.0.0.1:9999: connect: connection refused", time.Minute))
		}
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}, {Type: corev1.ContainersReady, Status: cond}}
		if tweak != nil && !r {
			tweak(p)
		}
		pods = append(pods, mxObj(p))
	}
	dep.Object["status"].(map[string]any)["readyReplicas"] = n
	s := listStore{kube.Pods: pods, kube.Events: evs, kube.Deployments: {dep}, kube.ReplicaSets: {rs}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	return enrich.New(mxStore{s}, res, nil), pods, dep
}

// QA round 144: a deployment asking for a copy with none ready (its
// readiness probe never passes) was "Not ready" (to check) on home but
// failing in the deployments list. None serving is failing on both; some
// ready is still one to check.
func TestNoneReadyIsFailingOnHome(t *testing.T) {
	for _, c := range []struct {
		want  int64
		ready []bool
		tone  Tone
	}{
		{1, []bool{false}, ToneBad},
		{2, []bool{false, false}, ToneBad},
		{2, []bool{true, false}, ToneWarn},
	} {
		e, pods, dep := r144Deploy(c.want, nil, c.ready...)
		apps := Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep})
		_, listTone := For(kube.Resource{GVR: kube.Deployments}).Row(e, dep)
		if len(apps) != 1 || apps[0].Status.Tone != c.tone || listTone != c.tone {
			for _, a := range apps {
				t.Logf("app %s: %+v", a.Name, a.Status)
			}
			t.Errorf("%d of %d ready: home and list should both be %v, list %v", len(c.ready), c.want, c.tone, listTone)
		}
	}
}

// Review of round 144: a slow first start (its ready check waits minutes
// before it first runs, or its startup check hasn't passed) is not
// failing yet, though none is ready.
func TestSlowFirstStartNotFailing(t *testing.T) {
	for name, tweak := range map[string]func(*corev1.Pod){
		"ready check not run yet": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = mxRun(3 * time.Minute)
			p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{InitialDelaySeconds: 300, PeriodSeconds: 10}
		},
		"startup check not passed": func(p *corev1.Pod) {
			no := false
			p.Status.ContainerStatuses[0].Started = &no
			p.Spec.Containers[0].StartupProbe = &corev1.Probe{PeriodSeconds: 10, FailureThreshold: 60}
		},
	} {
		e, pods, dep := r144Deploy(1, tweak, false)
		apps := Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep})
		if len(apps) != 1 || apps[0].Status.Tone == ToneBad {
			for _, a := range apps {
				t.Errorf("%s: %+v", name, a.Status)
			}
		}
	}
	// Past its delay and one check, still none ready: failing.
	e, pods, dep := r144Deploy(1, func(p *corev1.Pod) {
		p.Status.ContainerStatuses[0].State = mxRun(6 * time.Minute)
		p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{InitialDelaySeconds: 300, PeriodSeconds: 10}
	}, false)
	if apps := Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}); len(apps) != 1 || apps[0].Status.Tone != ToneBad {
		t.Errorf("past its first check: %+v", apps[0].Status)
	}
}
