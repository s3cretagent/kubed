package view

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 154: shop-tools crashed 361 times and ArgoCD can't build it.
// After so many crashes Kubernetes waits 5 minutes before each restart, so
// when it ran again its last crash was over 5 minutes old: home dropped the
// crash loop for "Can't build", and the row flipped on every crash. Up
// again just after that wait, it is still crashing.
func TestCrashingStaysAfterBackOff(t *testing.T) {
	ago := func(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339) }
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "tools-1", "namespace": "tools", "uid": "tools-1", "creationTimestamp": ago(3 * time.Hour),
			"labels": map[string]any{"app.kubernetes.io/instance": "tools"}, "annotations": map[string]any{"argocd.argoproj.io/tracking-id": "tools:/Pod:tools/tools-1"}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "x"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "app", "restartCount": int64(361), "ready": true,
			"state":     map[string]any{"running": map[string]any{"startedAt": ago(20 * time.Second)}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(3), "reason": "Error", "finishedAt": ago(5*time.Minute + 25*time.Second)}}}}}}}
	app := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "tools", "namespace": "argocd"},
		"spec":     map[string]any{"destination": map[string]any{"namespace": "tools"}},
		"status": map[string]any{"sync": map[string]any{"status": "Unknown"}, "health": map[string]any{"status": "Healthy"},
			"conditions": []any{map[string]any{"type": "ComparisonError", "message": "app path does not exist"}}},
	}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	pods := []*unstructured.Unstructured{pod}
	e := enrich.New(jobStore{kube.Pods: pods, kube.Applications: {app}}, res, nil)
	apps := Apps(e, pods, []*unstructured.Unstructured{app}, nil, nil, nil)
	if len(apps) != 1 || apps[0].Status.Short != "Crashing" {
		for _, a := range apps {
			t.Errorf("between crashes: %s %+v", a.Name, a.Status)
		}
	}
}
