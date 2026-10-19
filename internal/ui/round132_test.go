package ui

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 132: a new pod waits in CreateContainerConfigError because its
// Secret has no key apitoken. The rollout view said "a secret or config map
// it needs is missing": the Secret is there, its key isn't.
func TestRolloutSaysWhichKeyIsMissing(t *testing.T) {
	for _, c := range []struct{ msg, want, never string }{
		{"couldn't find key apitoken in Secret shop/dbcreds", `container "app" can't start: secret dbcreds has no key apitoken`, "is missing"},
		{`secret "dbcreds" not found`, `container "app" can't start: a secret or config map it needs is missing`, "has no key"},
	} {
		var objs []runtime.Object
		for _, o := range mxFixtures() {
			if o.(*unstructured.Unstructured).GetName() != "api-1-x" {
				objs = append(objs, o)
			}
		}
		objs = append(objs, mxObj("v1", "Pod", "shop", "api-1-x", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "api"},
			"ownerReferences": mxOwner("apps/v1", "ReplicaSet", "api-1")["ownerReferences"]},
			"spec": map[string]any{"nodeName": "node-1", "containers": []any{map[string]any{"name": "app", "image": "shop/api:1"}}},
			"status": map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "app", "ready": false, "restartCount": int64(0),
				"image": "shop/api:1", "state": map[string]any{"waiting": map[string]any{"reason": "CreateContainerConfigError", "message": c.msg}}}}}}))
		m := mxModel(mxNewCluster(objs, false), 160, 50)
		dep, ok := m.client.Cache.Get(kube.Deployments, "shop", "api")
		if !ok {
			t.Fatal("no deployment api")
		}
		got := m.newPodsCrashing(dep, m.enr.RolloutOf(dep))
		if got != c.want || strings.Contains(got, c.never) {
			t.Errorf("for %q: %q, want %q", c.msg, got, c.want)
		}
	}
}

// QA round 132: a service forward that dropped while its only pod wasn't
// ready said "Going on through another pod of service X (waiting for one
// to be ready)", then connected again to that same unready pod. It says
// what it does: connects again through a pod of it, a ready one if there
// is one within followWait.
func TestMovingForwardSaysItConnectsAgain(t *testing.T) {
	m := testModel()
	m.forwards = &kube.Forwards{}
	m.moving = map[int]*kube.Forward{18081: {LocalPort: 18081, PodPort: 80, Target: "service web"}}
	rows := m.forwardRows()
	want := "Connecting again through a pod of service web (a ready one if there is one within 30s)"
	if len(rows) != 1 || !strings.Contains(strings.Join(rows[0].Cells, " "), want) {
		t.Errorf("rows %v, want %q", rows, want)
	}
	if err := m.portMoving(18081); err == nil || strings.Contains(err.Error(), "another pod") || !strings.Contains(err.Error(), "connecting again through a pod of service web") {
		t.Errorf("port taken: %v", err)
	}
}

// QA round 132: ":pods -n" and ":pods -l" with no value said kubed doesn't
// know "-n"; they say what is missing. ":ns default other" switched to
// default and dropped the rest, and ":ns all extra" switched to all: extra
// words are refused, and nothing changes. :ctx the same.
func TestCommandArgsSayWhatIsWrong(t *testing.T) {
	c := mxNewCluster(mxFixtures(), false)
	for _, tc := range []struct{ line, want string }{
		{"pods -n", "-n needs a namespace name"},
		{"pods --namespace", "-n needs a namespace name"},
		{"pods -l", "-l needs a label selector"},
		{"pods --selector", "-l needs a label selector"},
		{"apps -n", "-n needs a namespace name"},
		{"helm -n", "-n needs a namespace name"},
		{"ns default shop", `:ns takes one namespace; kubed doesn't know "shop" here`},
		{"ns all extra", `:ns takes one namespace; kubed doesn't know "extra" here`},
		{"ns - extra", `:ns takes one namespace; kubed doesn't know "extra" here`},
		{"ctx other more", `:ctx takes one cluster; kubed doesn't know "more" here`},
	} {
		m := mxModel(c, 160, 50)
		m.setNamespace("shop")
		before := m.namespace
		m.flash, m.flashErr = "", false
		m.runCommand(tc.line)
		if !m.flashErr || m.flash != tc.want {
			t.Errorf(":%s says %q, want %q", tc.line, m.flash, tc.want)
		}
		if m.namespace != before {
			t.Errorf(":%s changed the namespace to %q", tc.line, m.namespace)
		}
	}
}
