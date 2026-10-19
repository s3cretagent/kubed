package enrich

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// slowRolloutFixture: deployment web partway through a slow rollout, its
// new version web-new (made 20 minutes ago) beside the old web-575db5c7b5,
// an autoscaler on it, and the given events.
func slowRolloutFixture(events ...*unstructured.Unstructured) *Enricher {
	e := hpaFixture("web")
	dep, _ := e.get(kube.Deployments, "qa", "web")
	dep.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "2"})
	old, _ := e.get(kube.ReplicaSets, "qa", "web-575db5c7b5")
	hpa := e.list(kube.HPAs, "qa")
	newRS := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-new", "namespace": "qa", "uid": "web-new", "creationTimestamp": time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}},
		"spec": map[string]any{"replicas": int64(5)}, "status": map[string]any{"replicas": int64(5)}}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
		{GVR: kube.HPAs, Kind: "HorizontalPodAutoscaler", Namespaced: true},
	})
	return New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {old, newRS}, kube.HPAs: hpa, kube.Events: events}, res, nil)
}

// Review of round 138: mid-rollout (older than 2 minutes), the autoscaler
// took web to 8; the deployment scaled its new and old versions up
// together, each event with that version's own size (5 and 3), neither
// the autoscaler's 8. Both were listed as scaled by hand. Their sizes add
// up to its new size: the autoscaler's. Sizes that don't add up stay.
func TestAutoscalerScalesBothVersionsMidRollout(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := slowRolloutFixture(
		rescaleEvent("web", "8", at),
		scaleEvent("new", "Scaled up replica set web-new from 4 to 5", 1, at, at),
		scaleEvent("old", "Scaled up replica set web-575db5c7b5 from 2 to 3", 1, at.Add(time.Second), at.Add(time.Second)),
	)
	if got := scaleDetails(e); len(got) != 0 {
		t.Errorf("the autoscaler's rescale of both versions shown as hand scales: %q", got)
	}
	// Proportional scaling aims at its new size plus the surge (8 with
	// 25% surge: 10 in all), not the new size: two versions scaled the same
	// way within seconds of its rescale are its own, whatever they add up to.
	e = slowRolloutFixture(
		rescaleEvent("web", "8", at),
		scaleEvent("new", "Scaled up replica set web-new from 4 to 6", 1, at, at),
		scaleEvent("old", "Scaled up replica set web-575db5c7b5 from 3 to 4", 1, at.Add(time.Second), at.Add(time.Second)),
	)
	if got := scaleDetails(e); len(got) != 0 {
		t.Errorf("the rescale with a surge shown as hand scales: %q", got)
	}
	// One version alone, its size not the new size: not proof, still listed.
	e = slowRolloutFixture(
		rescaleEvent("web", "9", at),
		scaleEvent("new", "Scaled up replica set web-new from 4 to 5", 1, at, at),
	)
	if got := scaleDetails(e); len(got) != 1 {
		t.Errorf("one version scaled near a rescale of another size: want its row, got %q", got)
	}
}
