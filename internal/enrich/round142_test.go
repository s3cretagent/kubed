package enrich

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 142 (10): during a rolling update the box showed the new image
// but Deployed the old time (it waits for the new version's first ready
// pod, by design). Deployed says a newer version is rolling out while one
// with other settings has no ready pod yet: never for a restart, never
// while paused, and no time moves.
func TestDeployTimesSayANewerVersionIsRollingOut(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB := now.Add(-time.Hour), now.Add(-5*time.Second)
	ready := func(o *unstructured.Unstructured, n int64) *unstructured.Unstructured {
		_ = unstructured.SetNestedField(o.Object, n, "status", "readyReplicas")
		return o
	}
	for _, c := range []struct {
		name            string
		newStamp, env   string
		newReady        int64
		paused, pending bool
		at              time.Time
	}{
		{"restart", "2026-10-16T00:00:00Z", "", 0, false, false, madeA},
		{"new settings, none ready", "", "bar", 0, false, true, madeA},
		{"new settings, paused", "", "bar", 0, true, false, madeA},
		{"new settings, ready", "", "bar", 1, false, false, madeB},
	} {
		f := goBackFixture{uid: "dep-qa142-" + c.name} // settings are cached by uid
		dep := f.dep("2", c.newStamp, c.env)
		if c.paused {
			_ = unstructured.SetNestedField(dep.Object, true, "spec", "paused")
		}
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
			ready(f.rs("web-a", "1", "", madeA, "", ""), 2),
			ready(f.rs("web-b", "2", "", madeB, c.newStamp, c.env), c.newReady),
		}}, testResources(), nil)
		dt := e.DeployTimes("qa")[types.UID(f.uid)]
		if dt.Pending != c.pending {
			t.Errorf("%s: pending %v, want %v", c.name, dt.Pending, c.pending)
		}
		if !dt.At.Equal(c.at) {
			t.Errorf("%s: dated %v, want %v (%+v)", c.name, dt.At, c.at, dt)
		}
	}
}

// Review of round 142: going back (u) renumbers an earlier version as the
// newest and scales it up: while it has no ready pod it is an earlier
// version coming back, not a newer one.
func TestDeployTimesSayAnEarlierVersionIsComingBack(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB := now.Add(-2*time.Hour), now.Add(-time.Hour)
	ready := func(o *unstructured.Unstructured, n int64) *unstructured.Unstructured {
		_ = unstructured.SetNestedField(o.Object, n, "status", "readyReplicas")
		return o
	}
	f := goBackFixture{uid: "dep-qa142-back"}
	e := New(fakeStore{kube.Deployments: {f.dep("3", "", "")}, kube.ReplicaSets: {
		ready(f.rs("web-a", "3", "1", madeA, "", ""), 0), // revision 1 brought back as 3
		ready(f.rs("web-b", "2", "", madeB, "", "bar"), 2),
	}}, testResources(), nil)
	dt := e.DeployTimes("qa")[types.UID(f.uid)]
	if !dt.Pending || !dt.Returning {
		t.Errorf("an earlier version coming back: pending %v returning %v, want both", dt.Pending, dt.Returning)
	}
}
