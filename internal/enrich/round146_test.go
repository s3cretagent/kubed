package enrich

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 146: + on a deployment whose autoscaler isn't scaling (it
// can't read its metrics: "missing request for cpu") warned that it
// "will change this back"; it didn't. It says the autoscaler can't act
// now, and won't change this until it can.
func TestScaleWarningWhenAutoscalerCantAct(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "shop"}}}
	hpa := func(typ, reason string) *unstructured.Unstructured {
		h := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
			"metadata": map[string]any{"name": "web-hpa", "namespace": "shop"},
			"spec": map[string]any{"minReplicas": int64(1), "maxReplicas": int64(3),
				"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}}}}
		if typ != "" {
			h.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": typ, "status": "False", "reason": reason,
				"message": `the HPA was unable to compute the replica count: failed to get cpu utilization: missing request for cpu in container app of Pod web-1`}}}
		}
		return h
	}
	for _, c := range []struct {
		name, typ, reason string
		want              string
	}{
		{"metrics", "ScalingActive", "FailedGetResourceMetric", "Autoscaler web-hpa can't act now (it can't read its metrics), so it won't change this until it can."},
		{"can't scale", "AbleToScale", "FailedGetScale", "Autoscaler web-hpa can't act now, so it won't change this until it can."},
		{"scaling", "", "", "Autoscaler web-hpa sets the count between 1 and 3 by itself: it will change this back"},
		{"scaled to 0", "ScalingActive", "ScalingDisabled", "Autoscaler web-hpa sets the count between 1 and 3 by itself: it will change this back"},
	} {
		e := New(fakeStore{kube.Deployments: {dep}, kube.HPAs: {hpa(c.typ, c.reason)}}, nil, nil)
		for _, action := range []Action{ActionScale, ActionEdit} {
			got := strings.Join(e.Warnings(dep, action), " | ")
			want := c.want
			if action == ActionEdit && c.typ == "" || action == ActionEdit && c.reason == "ScalingDisabled" {
				want = "Autoscaler web-hpa sets its count (replicas) between 1 and 3 by itself"
			}
			if !strings.Contains(got, want) {
				t.Errorf("%s, %s: %q, want %q", c.name, action, got, want)
			}
		}
	}
}
