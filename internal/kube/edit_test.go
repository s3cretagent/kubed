package kube

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// Status and the rollout's revision number move on their own; a changed
// spec or label is someone's edit.
func TestSameIntent(t *testing.T) {
	mk := func(rv, rev string, replicas int64, ready int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "web", "namespace": "shop", "resourceVersion": rv, "generation": int64(3),
				"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}},
			"spec":   map[string]any{"replicas": replicas},
			"status": map[string]any{"readyReplicas": ready},
		}}
	}
	if !sameIntent(mk("1", "4", 2, 1), mk("9", "5", 2, 2)) {
		t.Error("only status and revision moved: same intent")
	}
	if sameIntent(mk("1", "4", 2, 1), mk("9", "4", 3, 1)) {
		t.Error("replicas changed: someone else's edit")
	}
}

// The editor's copy comes from YAML (numbers as float64), the live one
// from the API (int64): the same value must still compare equal.
func TestSameIntentAcrossNumberTypes(t *testing.T) {
	var was unstructured.Unstructured
	if err := yaml.Unmarshal([]byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  resourceVersion: \"1\"\nspec:\n  replicas: 2\n"), &was.Object); err != nil {
		t.Fatal(err)
	}
	cur := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "resourceVersion": "7"}, "spec": map[string]any{"replicas": int64(2)}}}
	if !sameIntent(&was, cur) {
		t.Error("2 from YAML and 2 from the API are the same")
	}
}
