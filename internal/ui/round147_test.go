package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// QA round 147: right after i on a slow Deployment (2 copies), the new
// version's ReplicaSet wants 1 and has 1 ready while the old one still
// serves 2. v called the new one "(running now)": it compared the
// ReplicaSet's own count, not the Deployment's 2. It says "(newest, 1 of
// 2 ready)" until all 2 are ready and no older version serves.
func TestVersionsHalfRolledOutIsNotRunningNow(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "ns", "uid": "d-half"}, "spec": map[string]any{"replicas": int64(2)}}}
	rs := func(name string, rev int, made time.Time, image string, want, ready int64) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "ns", "uid": "half-" + name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": fmt.Sprint(rev)},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "d-half", "controller": true}}},
			"spec":   map[string]any{"replicas": want, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}},
			"status": map[string]any{"replicas": want, "readyReplicas": ready}}}
		o.SetCreationTimestamp(metav1.NewTime(made))
		return o
	}
	now := time.Now()
	newest, older := rs("web-4", 4, now.Add(-15*time.Second), "nginx:1.35", 1, 1), rs("web-3", 3, now.Add(-time.Hour), "nginx:1.36", 2, 2)
	store := versionStore{kube.Deployments: {dep}, kube.ReplicaSets: {newest, older}}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, Singular: "deployment"},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, Singular: "replicaset"},
	})
	m := &Model{client: &kube.Client{Resources: res}}
	m.enr = enrich.New(store, res, nil)
	label := func() string {
		items, _ := m.versionItems(view.Row{Obj: dep})
		if len(items) != 2 {
			t.Fatalf("items %+v", items)
		}
		return items[0].label
	}
	if l := label(); strings.Contains(l, "running now") || !strings.Contains(l, "(newest, 1 of 2 ready)") {
		t.Errorf("half rolled out: %q", l)
	}
	// All its copies ready, but the old one's pods still serve: not yet.
	newest.Object["status"] = map[string]any{"replicas": int64(2), "readyReplicas": int64(2)}
	older.Object["status"] = map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}
	if l := label(); strings.Contains(l, "running now") || !strings.Contains(l, "(newest, 2 of 2 ready)") {
		t.Errorf("old one still serves: %q", l)
	}
	// Done: all of the Deployment's copies ready, nothing older serving.
	older.Object["status"] = map[string]any{"replicas": int64(0), "readyReplicas": int64(0)}
	if l := label(); !strings.Contains(l, "(running now)") {
		t.Errorf("done: %q", l)
	}
}

// QA round 147: a go-back dated only by a bound rounds up, as Deployed
// does: 10m53s is "≤11m ago", not "≤10m ago".
func TestVersionWhenBoundRoundsUp(t *testing.T) {
	now := time.Now()
	v3 := &enrich.Version{Revision: 3, Reused: true, Created: now.Add(-time.Hour), BackAfter: now.Add(-(10*time.Minute + 53*time.Second))}
	v2 := &enrich.Version{Revision: 2, Created: now.Add(-30 * time.Minute)}
	if _, ago, _ := versionWhen(v3, []*enrich.Version{v3, v2}); ago != "≤11m ago" {
		t.Errorf("%q", ago)
	}
}
