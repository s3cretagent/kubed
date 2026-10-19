package ui

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// QA round 160: b from the versions list on a version whose pods are
// crashing now said nothing of it, while u's confirm does. It says so too.
func TestGoBackFromVersionsSaysItFailsNow(t *testing.T) {
	lbl := map[string]any{"app": "web"}
	tmpl := func(image string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": lbl}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}
	}
	rs := func(name, rev, image string) *unstructured.Unstructured {
		return mxObj("apps/v1", "ReplicaSet", "shop", name, map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": rev},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl(image)}, "status": map[string]any{"replicas": int64(1)}})
	}
	ended := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339)
	crashing := mxObj("v1", "Pod", "shop", "web-1-a", map[string]any{"metadata": map[string]any{"labels": lbl, "ownerReferences": mxOwner("apps/v1", "ReplicaSet", "web-1")["ownerReferences"]},
		"spec": map[string]any{"nodeName": "node-1", "containers": []any{map[string]any{"name": "app", "image": "nginx:1"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "app", "image": "nginx:1", "restartCount": int64(6),
			"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off 5m0s restarting failed container"}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ended}}}}}})
	for _, failing := range []bool{true, false} {
		objs := []runtime.Object{mxObj("v1", "Namespace", "", "shop", nil),
			mxObj("apps/v1", "Deployment", "shop", "web", map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
				"spec":   map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl("nginx:2")},
				"status": map[string]any{"replicas": int64(2), "updatedReplicas": int64(1)}}),
			rs("web-2", "2", "nginx:2"), rs("web-1", "1", "nginx:1")}
		if failing {
			objs = append(objs, crashing.DeepCopy())
		}
		cl := mxNewCluster(objs, false)
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		dep, ok := m.enr.Get(kube.Deployments, "shop", "web")
		if !ok {
			t.Fatal("no deployment web")
		}
		m.versionsNow(view.Row{Obj: dep})
		if m.prompt == nil || len(m.prompt.items) != 2 {
			t.Fatalf("prompt %+v, flash %q", m.prompt, m.flash)
		}
		m.prompt.items[1].alsoRun()
		if m.prompt == nil {
			t.Fatalf("no confirm, flash %q", m.flash)
		}
		if says := strings.Contains(m.prompt.title, "That version's pods are failing now: crashing"); says != failing {
			t.Errorf("failing %v: b asks %q", failing, m.prompt.title)
		}
		cl.client.Cache.Stop()
	}
}
