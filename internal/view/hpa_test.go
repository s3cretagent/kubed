package view

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TARGETS reads like kubectl's: what it is now over what it aims for.
func TestHPATargets(t *testing.T) {
	o := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"metrics": []any{
			map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "target": map[string]any{"type": "Utilization", "averageUtilization": int64(300)}}},
			map[string]any{"type": "External", "external": map[string]any{"metric": map[string]any{"name": "s0-rabbitmq-q"}, "target": map[string]any{"averageValue": "5"}}},
		}},
		"status": map[string]any{"currentMetrics": []any{
			map[string]any{"type": "Resource", "resource": map[string]any{"name": "cpu", "current": map[string]any{"averageUtilization": int64(78)}}},
		}},
	}}
	if got := hpaTargets(o); got != "cpu: 78%/300%, s0-rabbitmq-q: ?/5" {
		t.Errorf("got %q", got)
	}
}
