package enrich

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 155: a node's pods reserved 1815m of CPU by kubectl describe
// (98%), but kubed said 1360m: it left out setup steps (init containers)
// that ask for more than the pod's containers. The scheduler counts the
// larger of the two, helpers that run alongside, and the pod's overhead.
func TestNodeReservedCountsInitContainers(t *testing.T) {
	res := func(cpu, mem string) map[string]any {
		return map[string]any{"requests": map[string]any{"cpu": cpu, "memory": mem}}
	}
	pod := func(name string, spec map[string]any) *unstructured.Unstructured {
		spec["nodeName"] = "n1"
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "ns"}, "spec": spec, "status": map[string]any{"phase": "Running"}}}
	}
	pods := []*unstructured.Unstructured{
		// a setup step bigger than its app: 100m and 64Mi count
		pod("init", map[string]any{"initContainers": []any{map[string]any{"name": "migrate", "resources": res("100m", "64Mi")}},
			"containers": []any{map[string]any{"name": "app", "resources": res("10m", "16Mi")}}}),
		// a helper alongside (sidecar) and a later setup step: 50m+100m while it runs; after, 50m+20m
		pod("side", map[string]any{"initContainers": []any{
			map[string]any{"name": "proxy", "restartPolicy": "Always", "resources": res("50m", "32Mi")},
			map[string]any{"name": "setup", "resources": res("100m", "8Mi")}},
			"containers": []any{map[string]any{"name": "app", "resources": res("20m", "32Mi")}}}),
		// the pod's own overhead (a runtime class) on top
		pod("overhead", map[string]any{"overhead": map[string]any{"cpu": "250m", "memory": "120Mi"},
			"containers": []any{map[string]any{"name": "app", "resources": res("100m", "1Mi")}}}),
	}
	e := New(fakeStore{kube.Pods: pods}, nil, nil)
	cpu, mem := e.NodeReserved("n1")
	const mi = 1 << 20
	if wantCPU, wantMem := int64(100+150+350), int64((64+64+121)*mi); cpu != wantCPU || mem != wantMem {
		t.Errorf("node n1 reserves %dm and %dMi, want %dm and %dMi", cpu, mem/mi, wantCPU, wantMem/mi)
	}
}
