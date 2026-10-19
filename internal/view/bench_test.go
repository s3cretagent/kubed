package view

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

type benchStore struct{}

func (benchStore) List(schema.GroupVersionResource, string) []*unstructured.Unstructured { return nil }
func (benchStore) Get(schema.GroupVersionResource, string, string) (*unstructured.Unstructured, bool) {
	return nil, false
}
func (benchStore) Synced(schema.GroupVersionResource) bool { return true }

func benchPods(n int) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, n)
	for i := range out {
		out[i] = &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": fmt.Sprintf("app-%d", i), "namespace": "ns", "resourceVersion": fmt.Sprint(i),
				"uid": fmt.Sprintf("uid-%d", i), "creationTimestamp": "2026-09-30T00:00:00Z"},
			"spec": map[string]any{"nodeName": "node-1", "containers": []any{
				map[string]any{"name": "app", "image": "img", "resources": map[string]any{
					"requests": map[string]any{"cpu": "100m", "memory": "128Mi"}, "limits": map[string]any{"cpu": "1", "memory": "256Mi"}}},
				map[string]any{"name": "sidecar", "image": "img2"},
			}},
			"status": map[string]any{"phase": "Running", "podIP": "10.0.0.1", "containerStatuses": []any{
				map[string]any{"name": "app", "ready": true, "restartCount": int64(0), "state": map[string]any{"running": map[string]any{}}},
				map[string]any{"name": "sidecar", "ready": true, "restartCount": int64(2), "state": map[string]any{"running": map[string]any{}}},
			}},
		}}
	}
	return out
}

func BenchmarkPodRows5000(b *testing.B) {
	pods := benchPods(5000)
	e := enrich.New(benchStore{}, nil, nil)
	v := For(kube.Resource{GVR: kube.Pods})
	b.ResetTimer()
	for range b.N {
		Rows(v, e, pods, true)
	}
}
