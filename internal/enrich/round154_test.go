package enrich

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 154: a deployment paused partway from revision 1 to 2 (one of
// three copies updated) was put back on revision 1, which became 3.
// Kubernetes took it at once: the old version already ran its copies, so
// it scaled nothing up from 0, only the new one down to 0. Only a bound
// was said ("back after revision 2 was made"). That scale-down is when it
// came back, when kubed saw the go-back's write then. With no pod kept
// from before, a merged event, one that happened twice, no write seen, or
// a scale-down long before the write (a scale by hand took revision 2 to
// 0, the go-back came later), it still can't say.
func TestGoBackTakenAtOnceDatedByTheScaleDown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB, back := now.Add(-13*time.Minute), now.Add(-12*time.Minute), now.Add(-50*time.Second)
	early := now.Add(-5 * time.Minute) // a scale by hand, before the go-back
	for i, c := range []struct {
		name    string
		podMade time.Time // v's pod
		count   int64
		first   time.Time // of the scale-down; its last is down
		down    time.Time
		merged  bool
		written bool // kubed saw the go-back's write, at back
		exact   bool
	}{
		{"kept its pods", madeA.Add(time.Second), 1, back, back, false, true, true},
		{"no pod from before", back, 1, back, back, false, true, false},
		{"merged", madeA.Add(time.Second), 1, back, back, true, true, false},
		{"scaled down twice", madeA.Add(time.Second), 2, madeB.Add(time.Minute), back, false, true, false},
		{"no write seen", madeA.Add(time.Second), 1, back, back, false, false, false},
		{"scaled to 0 by hand before the go-back", madeA.Add(time.Second), 1, early, early, false, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := goBackFixture{uid: fmt.Sprintf("dep-qa154-%d", i)}
			dep := f.dep("3", "", "")
			dep.Object["spec"].(map[string]any)["paused"] = true
			if c.written {
				dep.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl-rollout", Operation: metav1.ManagedFieldsOperationUpdate,
					Time: &metav1.Time{Time: back}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{"f:spec":{}}}}`)}})
				kube.NoteTemplateChange(dep)
			}
			pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
				"metadata": map[string]any{"name": "web-a-1", "namespace": "qa", "creationTimestamp": c.podMade.Format(time.RFC3339),
					"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-a", "uid": f.uid + "-web-a", "controller": true}}},
				"status": map[string]any{"phase": "Running"}}}
			msg := "Scaled down replica set web-b from 1 to 0"
			if c.merged {
				msg = "(combined from similar events): " + msg
			}
			e := New(fakeStore{kube.Deployments: {dep}, kube.Pods: {pod}, kube.ReplicaSets: {
				f.rs("web-a", "3", "1", madeA, "", ""),
				f.rs("web-b", "2", "", madeB, "", "1.37"),
			}, kube.Events: {scaleEvent("down-b", msg, c.count, c.first, c.down)}}, testResources(), nil)
			want, after := back, false
			if !c.exact {
				want, after = madeB, true // after revision 2 was made, as before
			}
			for _, v := range e.RolloutOf(dep).All {
				if v.Revision != 3 {
					continue
				}
				if at := v.LiveSince; c.exact != !at.IsZero() || c.exact && !at.Equal(want) || !c.exact && !v.BackAfter.Equal(want) {
					t.Errorf("versions list: back at %v, after %v", v.LiveSince, v.BackAfter)
				}
			}
			if dt := e.DeployTimes("qa")[dep.GetUID()]; !dt.At.Equal(want) || dt.After != after {
				t.Errorf("Deployed %v (after %v), want %v", dt.At, dt.After, want)
			}
			seen := false
			for _, ch := range e.Changes("qa", "", now, nil) {
				if ch.What == "rollout" && len(ch.Detail) > 10 && ch.Detail[:10] == "revision 3" {
					seen = true
					if !ch.When.Equal(want) || ch.After != after {
						t.Errorf("what changed: %v (after %v) %q", ch.When, ch.After, ch.Detail)
					}
				}
			}
			if !seen {
				t.Error("no revision 3 row in what changed")
			}
		})
	}
}
