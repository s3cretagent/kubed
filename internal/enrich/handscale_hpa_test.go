package enrich

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// hpaFixture: deployment web running web-575db5c7b5 for a day, an
// autoscaler (named hpaName) targeting it, and the given events.
func hpaFixture(hpaName string, events ...*unstructured.Unstructured) *Enricher {
	day := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-web", "creationTimestamp": day,
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-575db5c7b5", "namespace": "qa", "uid": "web-575db5c7b5", "creationTimestamp": day,
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}},
		"spec": map[string]any{"replicas": int64(2)}, "status": map[string]any{"replicas": int64(2)}}}
	hpa := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]any{"name": hpaName, "namespace": "qa"},
		"spec": map[string]any{"minReplicas": int64(1), "maxReplicas": int64(3),
			"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}}}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
		{GVR: kube.HPAs, Kind: "HorizontalPodAutoscaler", Namespaced: true},
	})
	return New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.HPAs: {hpa}, kube.Events: events}, res, nil)
}

func rescaleEvent(hpaName string, size string, at time.Time) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": hpaName + "-rescale-" + size, "namespace": "qa"}, "reason": "SuccessfulRescale",
		"message":        "New size: " + size + "; reason: All metrics below target",
		"involvedObject": map[string]any{"kind": "HorizontalPodAutoscaler", "name": hpaName},
		"lastTimestamp":  at.Format(time.RFC3339)}}
}

func scaleDetails(e *Enricher) []string {
	var out []string
	for _, c := range e.handScales("qa") {
		out = append(out, c.Object+" "+c.Detail)
	}
	return out
}

// QA round 136: web scaled by hand 2 -> 3, then the autoscaler took it
// 3 -> 2 twelve seconds later. The hand scale was hidden as the
// autoscaler's own because it fell within a minute of its rescale.
func TestHandScaleNearAutoscalerRescaleKept(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := hpaFixture("web",
		scaleEvent("hand", "Scaled up replica set web-575db5c7b5 from 2 to 3", 1, at, at),
		scaleEvent("hpa", "Scaled down replica set web-575db5c7b5 from 3 to 2", 1, at.Add(12*time.Second), at.Add(12*time.Second)),
		rescaleEvent("web", "2", at.Add(12*time.Second)),
	)
	got := scaleDetails(e)
	if len(got) != 1 || got[0] != "deployment/web scaled from 2 to 3" {
		t.Errorf("want only the hand scale 2 to 3, got %q", got)
	}
}

// An autoscaler named other than its deployment (as KEDA's keda-hpa-web):
// its own rescale is the autoscaler's, not a hand scale.
func TestAutoscalerNamedOtherThanTarget(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := hpaFixture("keda-hpa-web",
		scaleEvent("hpa", "Scaled down replica set web-575db5c7b5 from 3 to 2", 1, at, at),
		rescaleEvent("keda-hpa-web", "2", at),
	)
	if got := scaleDetails(e); len(got) != 0 {
		t.Errorf("the autoscaler's own rescale shown as a hand scale: %q", got)
	}
}

// The autoscaler's rescale and the deployment's scale a second apart, the
// same size: the autoscaler's, no hand row.
func TestAutoscalerRescaleOneSecondApart(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := hpaFixture("web",
		rescaleEvent("web", "3", at),
		scaleEvent("hpa", "Scaled up replica set web-575db5c7b5 from 2 to 3", 1, at.Add(time.Second), at.Add(time.Second)),
	)
	if got := scaleDetails(e); len(got) != 0 {
		t.Errorf("the autoscaler's own rescale shown as a hand scale: %q", got)
	}
}

// A hand scale whose size equals a later autoscaler size, but further
// apart than the autoscaler's two lines ever are: still a hand scale.
func TestHandScaleSameSizeOutsideWindow(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := hpaFixture("web",
		scaleEvent("hand", "Scaled up replica set web-575db5c7b5 from 1 to 2", 1, at, at),
		rescaleEvent("web", "2", at.Add(40*time.Second)),
	)
	got := scaleDetails(e)
	if len(got) != 1 || got[0] != "deployment/web scaled from 1 to 2" {
		t.Errorf("want the hand scale 1 to 2, got %q", got)
	}
}

// KEDA scales to 0 itself (no autoscaler line), here 10s after its
// autoscaler took web to 1: still listed, as before, not the autoscaler's.
// (Its version runs again in the fixture, so the line says it came back.)
func TestKedaScaleToZeroStillListed(t *testing.T) {
	at := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	e := hpaFixture("keda-hpa-web",
		rescaleEvent("keda-hpa-web", "1", at),
		scaleEvent("hpa", "Scaled down replica set web-575db5c7b5 from 2 to 1", 1, at, at),
		scaleEvent("keda", "Scaled down replica set web-575db5c7b5 from 1 to 0", 1, at.Add(10*time.Second), at.Add(10*time.Second)),
	)
	got := scaleDetails(e)
	if len(got) != 1 || !strings.HasPrefix(got[0], "deployment/web scaled from 1 to 0") {
		t.Errorf("want KEDA's scale to 0 listed, got %q", got)
	}
}
