package enrich

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Home works out Deployed every second: with 200 Deployments, each brought
// back, and 5000 events, the scaling events are read once a pass, not once
// a Deployment (that took half a second).
func TestDeployTimesStaysQuick(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := fakeStore{}
	for d := range 200 {
		f := goBackFixture{uid: fmt.Sprintf("dep-speed-%d", d)}
		dep := f.dep("3", "", "")
		dep.SetName(fmt.Sprintf("web%d", d))
		store[kube.Deployments] = append(store[kube.Deployments], dep)
		for _, rs := range []*unstructured.Unstructured{
			f.rs(fmt.Sprintf("web%d-a", d), "3", "1", now.Add(-time.Hour), "", ""),
			f.rs(fmt.Sprintf("web%d-e", d), "2", "", now.Add(-30*time.Minute), "", "bar"),
		} {
			refs := rs.GetOwnerReferences()
			refs[0].Name = dep.GetName()
			rs.SetOwnerReferences(refs)
			store[kube.ReplicaSets] = append(store[kube.ReplicaSets], rs)
		}
	}
	for i := range 5000 {
		at := now.Add(-time.Duration(i) * time.Second)
		store[kube.Events] = append(store[kube.Events], scaleEvent(fmt.Sprintf("ev%d", i),
			fmt.Sprintf("Scaled up replica set web%d-a from 0 to 1", i%200), 1, at, at))
	}
	e := New(store, testResources(), nil)
	e.DeployTimes("qa") // warm the caches, as a running kubed has them
	best := time.Hour
	defer func() { t.Logf("best of 3: %v", best) }()
	for range 3 {
		start := time.Now()
		if got := len(e.DeployTimes("qa")); got != 200 {
			t.Fatalf("%d Deployments dated, want 200", got)
		}
		best = min(best, time.Since(start))
	}
	limit := 20 * time.Millisecond
	if raceOn {
		limit *= 10
	}
	if best > limit {
		t.Errorf("DeployTimes took %v for 200 Deployments and 5000 events, want under %v", best, limit)
	}
}
