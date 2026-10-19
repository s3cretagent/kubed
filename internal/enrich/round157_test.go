package enrich

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 157: a version brought back twice (revisions 5 and 7, one
// ReplicaSet) made both its pods at the first go-back, but one read "added
// a copy: its scale events were merged ...": nobody scaled. Only the latest
// go-back has a row, and the earlier one's time wasn't used to tell the
// go-back's own scale-up from a scale.
func TestPodOfAnEarlierGoBackIsNotACopy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	kube.ResetPodWatch(now.Add(-2 * time.Hour)) // kubed watching all along
	fx, _ := fixture(t, false)
	back5, back7 := now.Add(-7*time.Minute), now.Add(-4*time.Minute)
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-r157",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "7"}},
		"spec": map[string]any{"replicas": int64(2)}}}
	rs := func(name, rev, history string, made time.Time) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": "r157-" + name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-r157", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "busybox:1.37"}}}}}}}
	}
	rss := []*unstructured.Unstructured{
		rs("web-77f49f4bf5", "7", "2,5", now.Add(-20*time.Minute)), // revision 2, brought back as 5 and 7
		rs("web-old", "3", "1", now.Add(-30*time.Minute)),
		rs("web-restart", "4", "", now.Add(-10*time.Minute)),
		rs("web-bad", "6", "", now.Add(-6*time.Minute)),
	}
	noteWentBack(rss[0].GetUID(), "5", back5) // both go-backs seen live
	noteWentBack(rss[0].GetUID(), "7", back7)
	pod := func(name string, made time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": "r157-" + name, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-77f49f4bf5", "uid": "r157-web-77f49f4bf5", "controller": true}}}}}
	}
	ev := func(name, msg string, count int64, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "count": count,
			"message": msg, "involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
			"lastTimestamp": at.Format(time.RFC3339)}}
	}
	// The go-back's 1 to 2 said the same as revision 2's (count 2); the other
	// versions' scale-downs were folded into one event that now says the
	// last of them.
	goBackUp := ev("up12", "Scaled up replica set web-77f49f4bf5 from 1 to 2", 2, back5.Add(5*time.Second))
	combined := ev("comb", "(combined from similar events): Scaled down replica set web-bad from 1 to 0", 9, back7)
	newPods := func(events []*unstructured.Unstructured, pods ...*unstructured.Unstructured) map[string]string {
		e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: rss, kube.Pods: pods, kube.Events: events}, nil, nil)
		e.res = fx.res
		got := map[string]string{}
		for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
			if c.What == "new pod" {
				got[c.Object] = c.Detail
			}
		}
		return got
	}
	first, second := pod("web-77f49f4bf5-k2x9q", back5), pod("web-77f49f4bf5-s6fsf", back5.Add(5*time.Second))
	if got := newPods([]*unstructured.Unstructured{goBackUp, combined}, first, second); len(got) != 0 {
		t.Errorf("pods the first go-back made are part of it, got %v", got)
	}

	// A real scale by hand a minute after that go-back still reads as one;
	// the go-back's own pods still aren't listed.
	hand := ev("up23", "Scaled up replica set web-77f49f4bf5 from 2 to 3", 1, back5.Add(time.Minute))
	copied := pod("web-77f49f4bf5-h4nd1", back5.Add(time.Minute+time.Second))
	got := newPods([]*unstructured.Unstructured{goBackUp, combined, hand}, first, second, copied)
	if want := "added a copy: scaled up from 2 to 3"; got["pod/web-77f49f4bf5-h4nd1"] != want || len(got) != 1 {
		t.Errorf("a hand scale near a go-back: %v, want only %q", got, want)
	}
	// A merged scale-up well after the last go-back is still said with care.
	late := ev("up23", "Scaled up replica set web-77f49f4bf5 from 2 to 3", 2, back7.Add(150*time.Second))
	copied = pod("web-77f49f4bf5-l8te1", back7.Add(151*time.Second))
	got = newPods([]*unstructured.Unstructured{goBackUp, combined, late}, first, second, copied)
	if want := "added a copy: " + mergedCopyWords; got["pod/web-77f49f4bf5-l8te1"] != want || len(got) != 1 {
		t.Errorf("a merged scale after a go-back: %v, want only %q", got, want)
	}
}
