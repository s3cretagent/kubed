package ui

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// QA round 157: in v, enter on the version running now did nothing (b
// said why). Enter says it too, and both say "running now" only when it is.
func TestEnterOnRunningVersionSaysWhy(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d-157"}, "spec": map[string]any{"replicas": int64(1)}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-1", "namespace": "ns", "uid": "rs-157",
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "d-157", "controller": true}}},
		"spec":   map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nginx:1.27"}}}}},
		"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}}}
	rs.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Hour)))
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, res, nil)
	items, _ := m.versionItems(view.Row{Obj: dep})
	if len(items) != 1 || items[0].run == nil {
		t.Fatalf("enter on the running version does nothing: %+v", items)
	}
	items[0].run()
	if m.flash != "this is the version running now: pick an older one to go back to" {
		t.Errorf("flash %q", m.flash)
	}
	// Scaled to 0, it runs nothing: enter says only that it is the newest.
	dep.Object["spec"] = map[string]any{"replicas": int64(0)}
	items, _ = m.versionItems(view.Row{Obj: dep})
	items[0].run()
	if m.flash != "this is the newest version (what it is set to run): pick an older one to go back to" {
		t.Errorf("scaled to 0: flash %q", m.flash)
	}
}
