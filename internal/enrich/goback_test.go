package enrich

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// goBackFixture builds a Deployment "web" and its ReplicaSets for the
// go-back tests: kubed's watch saw none of it, so only the ReplicaSets
// and events are left to date the go-backs.
type goBackFixture struct {
	uid string
}

func (f goBackFixture) template(stamp, env string) map[string]any {
	tmeta := map[string]any{}
	if stamp != "" {
		tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
	}
	c := map[string]any{"name": "web", "image": "nginx"}
	if env != "" {
		c["env"] = []any{map[string]any{"name": "FOO", "value": env}}
	}
	return map[string]any{"metadata": tmeta, "spec": map[string]any{"containers": []any{c}}}
}

func (f goBackFixture) dep(rev, stamp, env string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": f.uid,
			"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}},
		"spec": map[string]any{"template": f.template(stamp, env)}}}
}

func (f goBackFixture) rs(name, rev, history string, made time.Time, stamp, env string) *unstructured.Unstructured {
	ann := map[string]any{"deployment.kubernetes.io/revision": rev}
	if history != "" {
		ann["deployment.kubernetes.io/revision-history"] = history
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": name, "namespace": "qa", "uid": f.uid + "-" + name, "annotations": ann,
			"creationTimestamp": made.Format(time.RFC3339),
			"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": f.uid, "controller": true}}},
		"spec": map[string]any{"template": f.template(stamp, env)}}}
}

func scaleEvent(name, msg string, count int64, first, last time.Time) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg, "count": count,
		"involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
		"firstTimestamp": first.Format(time.RFC3339), "lastTimestamp": last.Format(time.RFC3339)}}
}

// QA round 107: set env (revision 3), u back to revision 2 (a restart of
// revision 1) as revision 4, a restart (revision 5), u back to revision 3
// as revision 6. Revision 4's scale-up event is merged with its first
// rollout's. The versions list dated revision 4's go-back by revision 1's
// scale-down (when revision 2 first went out), and the timeline called the
// go-back a restart, so :shipped left it out though it took the env away.
func TestGoBackPastARestartKeepsItsTimeAndChange(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB, madeC := now.Add(-10*time.Minute), now.Add(-191*time.Second), now.Add(-105*time.Second)
	back4, madeD, back6 := now.Add(-84*time.Second), now.Add(-57*time.Second), now.Add(-19*time.Second)
	x, y := madeB.Format(time.RFC3339), madeD.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-qa107"}
	dep := f.dep("6", x, "bar")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "1", "", madeA, "", ""),
		f.rs("web-b", "4", "2", madeB, x, ""),    // revision 2, a restart of 1; back as 4
		f.rs("web-c", "6", "3", madeC, x, "bar"), // revision 3, set env; back as 6
		f.rs("web-d", "5", "", madeD, y, ""),     // a restart of 4
	}, kube.Events: {
		scaleEvent("up-b", "Scaled up replica set web-b from 0 to 1", 2, madeB, back4), // merged
		scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 1, madeB.Add(time.Second), madeB.Add(time.Second)),
		scaleEvent("down-c", "Scaled down replica set web-c from 1 to 0", 1, back4.Add(time.Second), back4.Add(time.Second)),
		scaleEvent("up-c", "Scaled up replica set web-c from 0 to 1", 2, madeC, back6),
		scaleEvent("up-d", "Scaled up replica set web-d from 0 to 1", 1, madeD, madeD),
		scaleEvent("down-d", "Scaled down replica set web-d from 1 to 0", 1, back6.Add(time.Second), back6.Add(time.Second)),
	}}, testResources(), nil)

	var timeline time.Time
	for _, c := range e.Changes("qa", "", now, nil) {
		if strings.HasPrefix(c.Detail, "revision 4: went back") {
			timeline = c.When
			if strings.Contains(c.Detail, "restarted, same images") {
				t.Errorf("timeline calls the go-back a restart: %q", c.Detail)
			}
		}
	}
	if timeline.IsZero() {
		t.Fatal("no timeline row for revision 4's go-back")
	}
	// The version it left (web-c) was brought back later too, so the
	// events can't date it: a bound, the same on both.
	for _, v := range e.RolloutOf(dep).All {
		at := v.LiveSince
		if at.IsZero() {
			at = v.BackAfter
		}
		if v.Revision == 4 && !at.Equal(timeline) {
			t.Errorf("versions list: revision 4 back at %v, timeline says %v", at.Format(time.TimeOnly), timeline.Format(time.TimeOnly))
		}
	}
	shipped := false
	for _, c := range e.Shipped("qa", now, ShipWindow) {
		if strings.HasPrefix(c.Detail, "revision 4: went back to an earlier version: settings changed") {
			shipped = true
		}
	}
	if !shipped {
		t.Error(":shipped leaves out revision 4's go-back, which took the env away")
	}
}

// A go-back that only changes the restart stamp is still a restart: it
// stays off :shipped.
func TestGoBackToARestartOfTheSameStaysOffShipped(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB, madeC := now.Add(-10*time.Minute), now.Add(-5*time.Minute), now.Add(-3*time.Minute)
	x, y := madeB.Format(time.RFC3339), madeC.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-qa107b"}
	dep := f.dep("4", x, "")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "1", "", madeA, "", ""),
		f.rs("web-b", "4", "2", madeB, x, ""), // a restart, brought back
		f.rs("web-c", "3", "", madeC, y, ""),  // another restart
	}}, testResources(), nil)
	for _, c := range e.Shipped("qa", now, ShipWindow) {
		if strings.Contains(c.Detail, "went back") {
			t.Errorf(":shipped lists a go-back that only restarted: %q", c.Detail)
		}
	}
}

// QA round 108: set env (E, revision 2), u back to revision 1 as
// revision 3, a restart (revision 4), u back past the restart to E as
// revision 5, a new image (revision 6), u back to E as revision 7. E was
// scaled to 0 twice, so its scale-down event is merged: the go-back to
// revision 3 was dated when E was made, 23s early, and the versions list
// had no time for it. The merged event's first time is that go-back.
func TestGoBackDatedByTheFirstOfAMergedScaleDown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	base := now.Add(-13 * time.Minute)
	madeE, back3, madeR := base.Add(26*time.Second), base.Add(49*time.Second), base.Add(92*time.Second)
	back5, madeB, back7 := base.Add(137*time.Second), base.Add(204*time.Second), base.Add(239*time.Second)
	x := madeR.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-qa108"}
	dep := f.dep("7", "", "bar")
	busybox := f.rs("web-b", "6", "", madeB, "", "bar")
	c, _, _ := unstructured.NestedSlice(busybox.Object, "spec", "template", "spec", "containers")
	c[0].(map[string]any)["image"] = "busybox:1.37"
	_ = unstructured.SetNestedSlice(busybox.Object, c, "spec", "template", "spec", "containers")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", base, "", ""),       // revision 1; back as 3
		f.rs("web-e", "7", "2,5", madeE, "", "bar"), // set env; back as 5 and 7
		f.rs("web-r", "4", "", madeR, x, ""),        // a restart of 3
		busybox,
	}, kube.Events: {
		scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 2, base, back3), // merged
		scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 3, madeE, back7),
		scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 2, madeE.Add(2*time.Second), madeR.Add(2*time.Second)),
		scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 2, back3, madeB.Add(2*time.Second)), // merged
		scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR),
		scaleEvent("down-r", "Scaled down replica set web-r from 1 to 0", 1, back5.Add(time.Second), back5.Add(time.Second)),
		scaleEvent("up-b", "Scaled up replica set web-b from 0 to 1", 1, madeB, madeB),
		scaleEvent("down-b", "Scaled down replica set web-b from 1 to 0", 1, back7.Add(time.Second), back7.Add(time.Second)),
	}}, testResources(), nil)

	// E, the version it left, went out three times: which of its
	// scale-downs is this go-back's only the missing ones could tell (and
	// Kubernetes drops some). A bound, no later than back3, everywhere.
	var timeline time.Time
	for _, c := range e.Changes("qa", "", now, nil) {
		if strings.HasPrefix(c.Detail, "revision 3: went back") {
			timeline = c.When
			if !c.After || c.When.After(back3) {
				t.Errorf("timeline: revision 3 back at %v (after %v), want a bound no later than %v", c.When.UTC().Format(time.TimeOnly), c.After, back3.Format(time.TimeOnly))
			}
		}
	}
	for _, v := range e.RolloutOf(dep).All {
		if v.Revision == 3 && (!v.LiveSince.IsZero() || !v.BackAfter.Equal(timeline)) {
			t.Errorf("versions list: revision 3 back at %v (or after %v), want after %v", v.LiveSince.UTC().Format(time.TimeOnly), v.BackAfter.UTC().Format(time.TimeOnly), timeline.Format(time.TimeOnly))
		}
	}
	for _, c := range e.Shipped("qa", now, ShipWindow) {
		if strings.HasPrefix(c.Detail, "revision 3: went back") && (!c.After || !c.When.Equal(timeline)) {
			t.Errorf(":shipped: revision 3 back at %v (after %v), want after %v", c.When.UTC().Format(time.TimeOnly), c.After, timeline.Format(time.TimeOnly))
		}
	}
}

// goBack3Times is when revision 3's go-back is dated by the timeline
// (over a week) and by the versions list (zero: not known).
func goBack3Times(t *testing.T, e *Enricher, dep *unstructured.Unstructured, now time.Time) (timeline, versions time.Time) {
	t.Helper()
	const rev = "3"
	for _, c := range e.changesOver("qa", "", now, ShipWindow, nil) {
		if strings.HasPrefix(c.Detail, "revision "+rev+": went back") {
			timeline = c.When
		}
	}
	if timeline.IsZero() {
		t.Fatalf("no timeline row for revision %s's go-back", rev)
	}
	for _, v := range e.RolloutOf(dep).All {
		if strconv.Itoa(v.Revision) == rev {
			versions = v.LiveSince
		}
	}
	return timeline, versions
}

// handScaledFixture: revision 1 (A), set env (E, revision 2), E scaled
// to 0 by hand at T1 and back up, u back to A as revision 3 at T2, a
// restart (revision 4). downs are E's scale-down events.
func handScaledFixture(now time.Time, downs ...*unstructured.Unstructured) (*Enricher, *unstructured.Unstructured) {
	base := now.Add(-20 * time.Minute)
	madeE, madeR := base.Add(time.Minute), base.Add(10*time.Minute)
	f := goBackFixture{uid: "dep-qa108-hand"}
	dep := f.dep("4", madeR.Format(time.RFC3339), "")
	events := []*unstructured.Unstructured{
		scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR),
		scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 1, madeR.Add(time.Second), madeR.Add(time.Second)),
	}
	return New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", base, "", ""),
		f.rs("web-e", "2", "", madeE, "", "bar"),
		f.rs("web-r", "4", "", madeR, madeR.Format(time.RFC3339), ""),
	}, kube.Events: append(events, downs...)}, testResources(), nil), dep
}

// A version scaled to 0 by hand in its first time out: that scale-down
// is not the go-back's. Kept apart, the go-back's own (last) dates it.
func TestGoBackNotDatedByAHandScaleDown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	t1, t2 := now.Add(-16*time.Minute), now.Add(-14*time.Minute)
	e, dep := handScaledFixture(now,
		scaleEvent("down-e-hand", "Scaled down replica set web-e from 2 to 0", 1, t1, t1),
		scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 1, t2, t2))
	// E's scale-down alone can't date it (no scale-up of A is kept: a scale
	// to 0 by hand, with a go-back made while nothing ran, looks the
	// same): a bound, no later than t2.
	timeline, versions := goBack3Times(t, e, dep, now)
	if timeline.After(t2) || timeline.Equal(t1) || !versions.IsZero() {
		t.Errorf("revision 3 back at %v (timeline) and %v (versions), want a bound no later than %v", timeline.UTC().Format(time.TimeOnly), versions.UTC().Format(time.TimeOnly), t2.Format(time.TimeOnly))
	}
	// Merged with the go-back's: neither can be told apart.
	e, dep = handScaledFixture(now, scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 2, t1, t2))
	timeline, versions = goBack3Times(t, e, dep, now)
	if timeline.Equal(t1) || versions.Equal(t1) {
		t.Errorf("revision 3 dated by the hand scale-down at %v", t1.Format(time.TimeOnly))
	}
}

// The go-back's own scale-down expired (events last about an hour); a
// later one, merged, is from leaving the same version later on.
func TestGoBackNotDatedByALaterMergedScaleDown(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	base := now.Add(-3 * time.Hour)
	madeE, madeR := base.Add(time.Minute), base.Add(10*time.Minute)
	back5, madeB, back7, madeC := now.Add(-60*time.Minute), now.Add(-50*time.Minute), now.Add(-30*time.Minute), now.Add(-20*time.Minute)
	f := goBackFixture{uid: "dep-qa108-expired"}
	dep := f.dep("8", "", "baz")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", base, "", ""),
		f.rs("web-e", "7", "2,5", madeE, "", "bar"),
		f.rs("web-r", "4", "", madeR, madeR.Format(time.RFC3339), ""),
		f.rs("web-b", "6", "", madeB, "", "qux"),
		f.rs("web-c", "8", "", madeC, "", "baz"),
	}, kube.Events: {
		scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 2, back5, back7),
		scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 2, madeB.Add(time.Second), madeC.Add(time.Second)),
		scaleEvent("up-c", "Scaled up replica set web-c from 0 to 1", 1, madeC, madeC),
	}}, testResources(), nil)
	timeline, versions := goBack3Times(t, e, dep, now)
	if !timeline.Before(madeR) || versions.After(madeR) {
		t.Errorf("revision 3 back at %v (timeline) and %v (versions), after revision 4 was made at %v", timeline.UTC().Format(time.TimeOnly), versions.UTC().Format(time.TimeOnly), madeR.Format(time.TimeOnly))
	}
}

// A combined event (client-go's aggregator) began with other scalings:
// its first time is not this version's.
func TestGoBackNotDatedByACombinedEvent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	base := now.Add(-13 * time.Minute)
	madeE, back3, madeR := base.Add(26*time.Second), base.Add(49*time.Second), base.Add(92*time.Second)
	begun := madeE.Add(5 * time.Second)
	f := goBackFixture{uid: "dep-qa108-combined"}
	dep := f.dep("4", madeR.Format(time.RFC3339), "")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", base, "", ""),
		f.rs("web-e", "2", "", madeE, "", "bar"),
		f.rs("web-r", "4", "", madeR, madeR.Format(time.RFC3339), ""),
	}, kube.Events: {
		scaleEvent("down-e", "(combined from similar events): Scaled down replica set web-e from 1 to 0", 1, begun, back3),
		scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR),
	}}, testResources(), nil)
	timeline, versions := goBack3Times(t, e, dep, now)
	if timeline.Equal(begun) || versions.Equal(begun) {
		t.Errorf("revision 3 dated by when the combined event began, %v", begun.Format(time.TimeOnly))
	}
}

// QA round 110: revision 1 (A), set env (E, revision 2), u back to A as
// revision 3, a restart (revision 4), then a hand scale to 0 and back.
// A's scale-up is merged with its first rollout's, so only E's
// scale-down dates the go-back: the versions list and the timeline had
// it, home's Deployed fell back to "after E was made".
func TestDeployedAfterARestartKeepsTheGoBackTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA := now.Add(-5 * time.Minute)
	madeE, back3, madeR := madeA.Add(27*time.Second), madeA.Add(82*time.Second), madeA.Add(121*time.Second)
	down, up := madeA.Add(138*time.Second), madeA.Add(153*time.Second)
	x := madeR.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-qa110"}
	dep := f.dep("4", x, "")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", madeA, "", ""), // revision 1; back as 3
		f.rs("web-e", "2", "", madeE, "", "bar"),
		f.rs("web-r", "4", "", madeR, x, ""), // a restart of 3
	}, kube.Events: {
		scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 2, madeA, back3), // merged
		scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 1, madeE, madeE),
		scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 1, madeE.Add(2*time.Second), madeE.Add(2*time.Second)),
		scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 1, back3, back3),
		scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR),
		scaleEvent("down-a2", "Scaled down replica set web-a from 1 to 0", 1, madeR.Add(2*time.Second), madeR.Add(2*time.Second)),
		scaleEvent("down-r", "Scaled down replica set web-r from 1 to 0", 1, down, down), // by hand
		scaleEvent("up-r2", "Scaled up replica set web-r from 0 to 1", 1, up, up),
	}}, testResources(), nil)

	timeline, versions := goBack3Times(t, e, dep, now)
	if !timeline.Equal(back3) || !versions.Equal(back3) {
		t.Fatalf("revision 3 back at %v (timeline) and %v (versions), want %v", timeline.UTC().Format(time.TimeOnly), versions.UTC().Format(time.TimeOnly), back3.Format(time.TimeOnly))
	}
	dt := e.DeployTimes("qa")["dep-qa110"]
	if !dt.At.Equal(back3) || dt.After || dt.Before {
		t.Errorf("Deployed: %v (after %v, before %v), want %v as the go-back", dt.At.UTC().Format(time.TimeOnly), dt.After, dt.Before, back3.Format(time.TimeOnly))
	}
}

// r118Fixture loads QA round 118's staging objects (testdata/goback_r118.json):
// weba and webb, each set to 1.25 then 1.27, undone and restarted, all
// before kubed started. Its watch saw none of it, and Kubernetes merged
// many of the scaling events, some into a "(combined from similar
// events)" one.
func r118Fixture(t *testing.T) (*Enricher, map[string]*unstructured.Unstructured) {
	t.Helper()
	raw, err := os.ReadFile("testdata/goback_r118.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	store := fakeStore{}
	deps := map[string]*unstructured.Unstructured{}
	for _, item := range file.Items {
		o := &unstructured.Unstructured{}
		if err := o.UnmarshalJSON(item); err != nil { // whole numbers as int64, as from the cluster
			t.Fatal(err)
		}
		switch o.GetKind() {
		case "Deployment":
			store[kube.Deployments] = append(store[kube.Deployments], o)
			deps[o.GetName()] = o
		case "ReplicaSet":
			store[kube.ReplicaSets] = append(store[kube.ReplicaSets], o)
		case "Event":
			store[kube.Events] = append(store[kube.Events], o)
		}
	}
	return New(store, testResources(), nil), deps
}

func at118(t *testing.T, clock string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, "2026-10-15T"+clock+"Z")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func within(v, from, to time.Time) bool { return !v.Before(from) && !v.After(to) }

// QA round 118, the app's box: both said "Deployed after 16:53 (brought
// back after that version went out; Kubernetes no longer keeps exactly
// when)". The go-back's own scale-up is still there, merged with the
// version's first rollout (weba), and the scale-down of the version it
// left, merged with that one's earlier time out (webb).
//
// Neither go-back can be proved from what is there: weba's left version
// has no scale-down of its own left (folded into a combined event), and
// webb's were brought back more than once. Dating them would rest on
// events being missing, and Kubernetes drops some. A bound, no later than
// the undo, as every screen says.
func TestDeployedDatesAGoBackFromMergedScaleEvents(t *testing.T) {
	e, deps := r118Fixture(t)
	times := e.DeployTimes("kubed-qa118c")
	for _, c := range []struct {
		app      string
		from, to string
		bound    bool
	}{
		{"weba", "11:23:18", "11:23:41", true}, // undo at 11:23:41, restart at 11:24:05
		{"webb", "11:23:19", "11:24:05", true}, // second undo at 11:24:05, done 11:24:16; restart at 11:24:30
	} {
		dt := times[deps[c.app].GetUID()]
		if dt.After != c.bound || dt.Before || !within(dt.At, at118(t, c.from), at118(t, c.to)) {
			t.Errorf("%s: Deployed %v (after %v, before %v), want the go-back, %s to %s",
				c.app, dt.At.UTC().Format(time.TimeOnly), dt.After, dt.Before, c.from, c.to)
		}
	}
}

// QA round 118, w on webb: the restart (revision 6) was listed below the
// go-backs to revisions 4 and 5. Revision 4 was dated by the scale-down to
// 0 of the version it left, but that one was from the restart, when the
// same version was left again under a later number.
func TestGoBacksDatedBeforeALaterRestart(t *testing.T) {
	e, deps := r118Fixture(t)
	now := at118(t, "11:28:39")
	for _, c := range []struct {
		app   string
		order []string // the revision rows, newest first
		back  map[string][2]string
		bound map[string]bool // only a bound, within the span (see above)
	}{
		{"weba", []string{"revision 5:", "revision 4:", "revision 3:", "revision 2 ", "revision 1:"},
			map[string][2]string{"4": {"11:23:18", "11:23:41"}}, map[string]bool{"4": true}},
		{"webb", []string{"revision 6:", "revision 5:", "revision 4:", "revision 3 ", "revision 2 ", "revision 1:"},
			map[string][2]string{"4": {"11:22:54", "11:23:41"}, "5": {"11:23:19", "11:24:05"}}, map[string]bool{"4": true, "5": true}},
	} {
		object := "deployment/" + c.app
		var got []string
		timeline := map[string]time.Time{}
		for _, ch := range e.Changes("kubed-qa118c", "", now, func(o string) bool { return o == object }) {
			if ch.What != "rollout" || !strings.HasPrefix(ch.Detail, "revision ") {
				continue
			}
			got = append(got, ch.Detail)
			if rev, _, ok := strings.Cut(strings.TrimPrefix(ch.Detail, "revision "), ": went back"); ok {
				timeline[rev] = ch.When
			}
		}
		ok := len(got) == len(c.order)
		for i := 0; ok && i < len(got); i++ {
			ok = strings.HasPrefix(got[i], c.order[i])
		}
		if !ok {
			t.Errorf("%s: what changed lists, newest first:\n  %s", c.app, strings.Join(got, "\n  "))
		}
		versions := map[string]time.Time{}
		for _, v := range e.RolloutOf(deps[c.app]).All {
			versions[strconv.Itoa(v.Revision)] = v.LiveSince
			if c.bound[strconv.Itoa(v.Revision)] {
				versions[strconv.Itoa(v.Revision)] = v.BackAfter
			}
		}
		for rev, span := range c.back {
			if !within(timeline[rev], at118(t, span[0]), at118(t, span[1])) {
				t.Errorf("%s: what changed has revision %s back at %v, want %s to %s", c.app, rev, timeline[rev].UTC().Format(time.TimeOnly), span[0], span[1])
			}
			if !versions[rev].Equal(timeline[rev]) {
				t.Errorf("%s: versions list has revision %s back at %v, what changed %v", c.app, rev, versions[rev].UTC().Format(time.TimeOnly), timeline[rev].UTC().Format(time.TimeOnly))
			}
		}
	}
}

// A merged scale-up from 0 dates a go-back only when it is all the
// version's times going live: a scale by hand to 0 and back, while the
// version ran, adds one. Revision 1 (A), set env (E, revision 2), u back
// to A as revision 3, A scaled to 0 by hand and back, a restart
// (revision 4). Deployed must not be the hand scale.
func TestDeployedNotDatedByAMergedHandScaleUp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA := now.Add(-5 * time.Minute)
	madeE, back3, madeR := madeA.Add(27*time.Second), madeA.Add(82*time.Second), madeA.Add(121*time.Second)
	up := madeA.Add(110 * time.Second) // by hand, after a scale to 0 at 100s
	x := madeR.Format(time.RFC3339)
	for _, c := range []struct {
		name   string
		events []*unstructured.Unstructured
	}{
		{"every scale-up kept", []*unstructured.Unstructured{
			scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 3, madeA, up), // first rollout, go-back, by hand
			scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 3, madeE.Add(2*time.Second), madeR.Add(2*time.Second)),
		}},
		{"the go-back's scale-up combined away", []*unstructured.Unstructured{
			scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 2, madeA, up), // first rollout, by hand
			scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 3, madeE.Add(2*time.Second), madeR.Add(2*time.Second)),
		}},
	} {
		f := goBackFixture{uid: "dep-hand-up-" + strings.ReplaceAll(c.name, " ", "-")}
		dep := f.dep("4", x, "")
		events := append(slices.Clone(c.events),
			scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 1, madeE, madeE),
			scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 1, back3, back3),
			scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR))
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
			f.rs("web-a", "3", "1", madeA, "", ""),
			f.rs("web-e", "2", "", madeE, "", "bar"),
			f.rs("web-r", "4", "", madeR, x, ""),
		}, kube.Events: events}, testResources(), nil)
		// A's scale-ups can't tell the go-back's from the hand one's, and
		// E's scale-down alone could be a scale to 0 by hand with a go-back
		// made while nothing ran: a bound, no later than back3, and never
		// the hand scale-up.
		dt := e.DeployTimes("qa")[types.UID(f.uid)]
		if !dt.After || dt.At.After(back3) {
			t.Errorf("%s: Deployed %v (after %v), want a bound no later than %v (the hand scale-up was at %v)", c.name, dt.At.UTC().Format(time.TimeOnly), dt.After, back3.Format(time.TimeOnly), up.Format(time.TimeOnly))
		}
		timeline, versions := goBack3Times(t, e, dep, now)
		if timeline.After(back3) || !versions.IsZero() {
			t.Errorf("%s: revision 3 back at %v (timeline) and %v (versions), want a bound no later than %v", c.name, timeline.UTC().Format(time.TimeOnly), versions.UTC().Format(time.TimeOnly), back3.Format(time.TimeOnly))
		}
	}
}

// A go-back from a version that never became ready: revision 1 (A), a
// broken image (E, revision 2, A kept running), u back to A as revision
// 3 (E scaled to 0), A scaled to 0 by hand and back, then a restart
// (revision 4). A's merged scale-ups from 0 are its first rollout and the
// hand scale, not the go-back: E's scale-down to 0 dates it.
func TestGoBackFromABrokenVersionNotDatedByAMergedHandScaleUp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA := now.Add(-5 * time.Minute)
	madeE, back3 := madeA.Add(27*time.Second), madeA.Add(82*time.Second)
	down, up, madeR := madeA.Add(100*time.Second), madeA.Add(110*time.Second), madeA.Add(121*time.Second)
	x := madeR.Format(time.RFC3339)
	for _, restarted := range []bool{true, false} {
		f := goBackFixture{uid: "dep-broken-hand-" + strconv.FormatBool(restarted)}
		events := []*unstructured.Unstructured{
			scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 2, madeA, up), // first rollout, by hand
			scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 1, madeE, madeE),
			scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 1, back3, back3),
		}
		rss := []*unstructured.Unstructured{
			f.rs("web-a", "3", "1", madeA, "", ""),
			f.rs("web-e", "2", "", madeE, "", "bar"),
		}
		dep := f.dep("3", "", "")
		if restarted {
			dep = f.dep("4", x, "")
			rss = append(rss, f.rs("web-r", "4", "", madeR, x, ""))
			events = append(events,
				scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 2, down, madeR.Add(2*time.Second)), // by hand, the restart
				scaleEvent("up-r", "Scaled up replica set web-r from 0 to 1", 1, madeR, madeR))
		} else {
			events = append(events, scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 1, down, down)) // by hand
		}
		// E's scale-down dates it only if A ran all along, and that rests
		// on a scale-down of A that isn't there: Kubernetes drops events
		// (its spam filter), so a missing one proves nothing. A bound, no
		// later than back3, and never the hand scale-up.
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: rss, kube.Events: events}, testResources(), nil)
		timeline, versions := goBack3Times(t, e, dep, now)
		if timeline.After(back3) {
			t.Errorf("restarted %v: what changed has revision 3 back at %v, want a bound no later than %v (the hand scale-up was at %v)", restarted, timeline.UTC().Format(time.TimeOnly), back3.Format(time.TimeOnly), up.Format(time.TimeOnly))
		}
		if !restarted {
			continue // running now: the versions list and Deployed read the Deployment
		}
		if !versions.IsZero() {
			t.Errorf("versions list has revision 3 back at %v, want a bound", versions.UTC().Format(time.TimeOnly))
		}
		if dt := e.DeployTimes("qa")[types.UID(f.uid)]; dt.At.After(back3) || !dt.After {
			t.Errorf("Deployed %v (after %v), want a bound no later than %v", dt.At.UTC().Format(time.TimeOnly), dt.After, back3.Format(time.TimeOnly))
		}
	}
}

// The go-back from a broken version over an hour ago: the broken one's
// events are gone, A's merged scale-ups (its first rollout and a later
// hand scale) are still there. They can't date the go-back: what changed,
// the versions list and Deployed keep the bound, never the hand scale.
func TestGoBackNotDatedByAMergedHandScaleUpOnceTheLeftVersionsEventsExpired(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA := now.Add(-90 * time.Minute)
	madeE := madeA.Add(27 * time.Second) // broken; scaled to 0 by the go-back 55s later
	down, up, madeC := now.Add(-40*time.Minute), now.Add(-39*time.Minute), now.Add(-30*time.Minute)
	for _, restart := range []bool{true, false} {
		f := goBackFixture{uid: "dep-expired-hand-" + strconv.FormatBool(restart)}
		stamp, env := "", "baz"
		if restart {
			stamp, env = madeC.Format(time.RFC3339), ""
		}
		dep := f.dep("4", stamp, env)
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
			f.rs("web-a", "3", "1", madeA, "", ""),
			f.rs("web-e", "2", "", madeE, "", "bar"),
			f.rs("web-c", "4", "", madeC, stamp, env),
		}, kube.Events: {
			scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 2, madeA, up), // first rollout, by hand
			scaleEvent("down-a", "Scaled down replica set web-a from 1 to 0", 2, down, madeC.Add(2*time.Second)),
			scaleEvent("up-c", "Scaled up replica set web-c from 0 to 1", 1, madeC, madeC),
		}}, testResources(), nil)
		timeline, versions := goBack3Times(t, e, dep, now)
		if timeline.Equal(up) || versions.Equal(up) {
			t.Errorf("restart %v: revision 3 back at %v (what changed) and %v (versions), the hand scale-up", restart, timeline.UTC().Format(time.TimeOnly), versions.UTC().Format(time.TimeOnly))
		}
		if dt := e.DeployTimes("qa")[types.UID(f.uid)]; restart && (dt.At.Equal(up) || !dt.After) {
			t.Errorf("Deployed %v (after %v), want a bound, not the hand scale-up at %v", dt.At.UTC().Format(time.TimeOnly), dt.After, up.Format(time.TimeOnly))
		}
	}
}

// Round 121: an image changed after a restart carries the restart's stamp
// over; an older version ran that image without it. That is an image
// change, not a restart, in what changed and :shipped (i, u, r, u).
func TestImageChangeAfterARestartIsNoRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	t2, t6, t7, t9 := now.Add(-20*time.Minute), now.Add(-10*time.Minute), now.Add(-5*time.Minute), now.Add(-4*time.Minute)
	stamp6, stamp9 := t6.Format(time.RFC3339), t9.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-image-after-restart"}
	dep := f.dep("10", stamp6, "b")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-2", "2", "", t2, "", "b"),       // image b, never restarted
		f.rs("web-6", "8", "6", t6, stamp6, "a"),  // a restart of image a, brought back
		f.rs("web-7", "10", "7", t7, stamp6, "b"), // image b again: the stamp carried over
		f.rs("web-9", "9", "", t9, stamp9, "a"),   // a restart
	}}, testResources(), nil)
	found := false
	for _, c := range e.changesOver("qa", "", now, ShipWindow, nil) {
		if strings.HasPrefix(c.Detail, "revision 7 ") {
			found = true
			if strings.Contains(c.Detail, "restarted") {
				t.Errorf("an image change read as a restart: %q", c.Detail)
			}
		}
		if strings.HasPrefix(c.Detail, "revision 9:") && !strings.Contains(c.Detail, "restarted") {
			t.Errorf("a real restart: %q", c.Detail)
		}
	}
	if !found {
		t.Fatal("no row for revision 7")
	}
}

// Review of round 121: by the stamp itself, not by when it was written.
// An image changed 30s after a restart is still no restart; a restart of a
// paused deployment, its version made 10 minutes after its stamp (or with
// a clock off), is still one.
func TestRestartToldByItsOwnStamp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	t2, t6 := now.Add(-20*time.Minute), now.Add(-10*time.Minute)
	t7, t9 := t6.Add(30*time.Second), now.Add(-4*time.Minute)
	stamp6, stamp9 := t6.Format(time.RFC3339), t9.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-quick-image"}
	e := New(fakeStore{kube.Deployments: {f.dep("10", stamp6, "b")}, kube.ReplicaSets: {
		f.rs("web-2", "2", "", t2, "", "b"),
		f.rs("web-6", "8", "6", t6, stamp6, "a"),
		f.rs("web-7", "10", "7", t7, stamp6, "b"), // 30s after the restart: the stamp carried over
		f.rs("web-9", "9", "", t9, stamp9, "a"),
	}}, testResources(), nil)
	for _, c := range e.changesOver("qa", "", now, ShipWindow, nil) {
		if strings.HasPrefix(c.Detail, "revision 7 ") && strings.Contains(c.Detail, "restarted") {
			t.Errorf("quick image change read as a restart: %q", c.Detail)
		}
	}
	// rev 1 (a), rev 2 (b), back to rev 1 (now 3), then a paused restart:
	// stamped 10 minutes before its version was made on resume.
	m1, m2, m3 := now.Add(-40*time.Minute), now.Add(-30*time.Minute), now.Add(-12*time.Minute)
	stamp := m3.Format(time.RFC3339)
	g := goBackFixture{uid: "dep-paused-restart"}
	e = New(fakeStore{kube.Deployments: {g.dep("4", stamp, "a")}, kube.ReplicaSets: {
		g.rs("web-1", "3", "1", m1, "", "a"),
		g.rs("web-2", "2", "", m2, "", "b"),
		g.rs("web-4", "4", "", m3.Add(10*time.Minute), stamp, "a"),
	}}, testResources(), nil)
	found := false
	for _, c := range e.changesOver("qa", "", now, ShipWindow, nil) {
		if strings.HasPrefix(c.Detail, "revision 4:") {
			found = true
			if !strings.Contains(c.Detail, "restarted") {
				t.Errorf("paused restart: %q", c.Detail)
			}
		}
	}
	if !found {
		t.Fatal("no row for revision 4")
	}
}

// QA round 124: web (2 copies) set to 1.25 (revision 2) and 1.26 (revision
// 3, X), u back to 1.25 (revision 4), a restart (revision 5, R1), u back to
// X (revision 6), another restart (revision 7), all before kubed started.
// So many scalings in a few minutes: Kubernetes folded every new message
// after the tenth into one "(combined from similar events)" event, which
// now names the last restart's version. None names R1 any more, so
// nothing dates the go-back to X (a hand scale to 0 and back could pass
// for its merged scale-ups). w and :changes showed the bound as an exact
// time, a second after R1 was made; the versions list and Deployed said it
// was a bound. All four must say the same bound, as one.
func TestGoBackBoundSaidTheSameEverywhere(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	at := func(clock string) time.Time { // the round's times, kubed looking at 15:08:25
		c, err := time.Parse(time.TimeOnly, clock)
		if err != nil {
			t.Fatal(err)
		}
		return now.Add(-time.Duration(8*60+25-(c.Minute()*60+c.Second())) * time.Second)
	}
	clock := func(v time.Time) string { // on the round's clock
		if v.IsZero() {
			return "none"
		}
		round, _ := time.Parse(time.TimeOnly, "15:08:25")
		return round.Add(v.Sub(now)).Format(time.TimeOnly)
	}
	madeA, madeB, madeX := at("15:01:40"), at("15:02:14"), at("15:02:44")
	back4, madeR1, back6, madeR2 := at("15:03:04"), at("15:03:20"), at("15:03:50"), at("15:07:04")
	s1, s2 := madeR1.Format(time.RFC3339), madeR2.Format(time.RFC3339)
	f := goBackFixture{uid: "dep-qa124"}
	image := func(o *unstructured.Unstructured, img string) *unstructured.Unstructured {
		c, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "containers")
		c[0].(map[string]any)["image"] = img
		_ = unstructured.SetNestedSlice(o.Object, c, "spec", "template", "spec", "containers")
		return o
	}
	dep := image(f.dep("7", s2, ""), "nginx:1.26")
	rss := []*unstructured.Unstructured{
		image(f.rs("web-a", "1", "", madeA, "", ""), "nginx:1.24"),
		image(f.rs("web-b", "4", "2", madeB, "", ""), "nginx:1.25"),
		image(f.rs("web-x", "6", "3", madeX, "", ""), "nginx:1.26"),
		image(f.rs("web-r1", "5", "", madeR1, s1, ""), "nginx:1.25"), // a restart of 4
		image(f.rs("web-r2", "7", "", madeR2, s2, ""), "nginx:1.26"), // a restart of 6
	}
	events := []*unstructured.Unstructured{
		scaleEvent("up-a", "Scaled up replica set web-a from 0 to 2", 1, madeA, madeA),
		scaleEvent("up-b", "Scaled up replica set web-b from 0 to 1", 2, madeB, back4),
		scaleEvent("down-a", "Scaled down replica set web-a from 2 to 1", 1, madeB.Add(3*time.Second), madeB.Add(3*time.Second)),
		scaleEvent("up-b2", "Scaled up replica set web-b from 1 to 2", 2, madeB.Add(3*time.Second), back4.Add(3*time.Second)),
		scaleEvent("down-a2", "Scaled down replica set web-a from 1 to 0", 1, madeB.Add(6*time.Second), madeB.Add(6*time.Second)),
		scaleEvent("up-x", "Scaled up replica set web-x from 0 to 1", 2, madeX, back6), // as kubectl showed it
		scaleEvent("down-b", "Scaled down replica set web-b from 2 to 1", 2, madeX.Add(3*time.Second), madeR1.Add(3*time.Second)),
		scaleEvent("up-x2", "Scaled up replica set web-x from 1 to 2", 2, madeX.Add(3*time.Second), back6.Add(3*time.Second)),
		scaleEvent("down-b2", "Scaled down replica set web-b from 1 to 0", 2, madeX.Add(6*time.Second), madeR1.Add(6*time.Second)),
		// X scaled down at the first go-back, R1's every scaling, R2's.
		scaleEvent("combined", "(combined from similar events): Scaled up replica set web-r2 from 1 to 2", 8, back4.Add(3*time.Second), madeR2.Add(3*time.Second)),
		scaleEvent("down-x", "Scaled down replica set web-x from 2 to 1", 1, madeR2.Add(3*time.Second), madeR2.Add(3*time.Second)),
		scaleEvent("down-x2", "Scaled down replica set web-x from 1 to 0", 1, madeR2.Add(6*time.Second), madeR2.Add(6*time.Second)),
	}
	// say: rev's go-back in w, :changes, :shipped, the versions list and
	// (when it is the version running now, or restarted since) Deployed,
	// each at want, and said as a bound (after) or not.
	say := func(name string, dep *unstructured.Unstructured, rss, events []*unstructured.Unstructured, rev int, deployed bool, want time.Time, after bool) {
		t.Helper()
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: rss, kube.Events: events}, testResources(), nil)
		var got []string
		saw := func(screen string, when time.Time, bound bool) {
			if !when.Equal(want) || bound != after {
				got = append(got, fmt.Sprintf("%s %s (bound %v)", screen, clock(when), bound))
			}
		}
		row := func(screen string, list []Change) {
			for _, c := range list {
				if strings.HasPrefix(c.Detail, fmt.Sprintf("revision %d: went back", rev)) {
					saw(screen, c.When, c.After)
					return
				}
			}
			got = append(got, screen+" has no row")
		}
		row("w", e.Changes("qa", "", now, func(o string) bool { return o == "deployment/web" }))
		row(":changes", e.Changes("qa", "", now, nil))
		row(":shipped", e.Shipped("qa", now, ShipWindow))
		for _, v := range e.RolloutOf(dep).All {
			if v.Revision == rev {
				when := v.LiveSince
				if when.IsZero() {
					when = v.BackAfter
				}
				saw("versions", when, !v.BackAfter.IsZero())
			}
		}
		if deployed {
			dt := e.DeployTimes("qa")[dep.GetUID()]
			saw("Deployed", dt.At, dt.After)
		}
		if len(got) > 0 {
			t.Errorf("%s: want %s (bound %v) everywhere, got %s", name, clock(want), after, strings.Join(got, ", "))
		}
	}
	check := func(name string, events []*unstructured.Unstructured, want time.Time, after bool) {
		t.Helper()
		say(name, dep, rss, events, 6, true, want, after)
	}
	check("round 124", events, madeR1, true)

	// The events from before R1 was made expired too: still the same bound.
	var later []*unstructured.Unstructured
	for _, ev := range events {
		if EventTime(ev).After(madeR1) {
			later = append(later, ev)
		}
	}
	check("older events expired", later, madeR1, true)

	// R1's scale-down to 0 kept in its own name: the go-back is dated, exact
	// everywhere, from when X was scaled up from 0 (it began then; R1, out
	// only the once, ran until that scale-down).
	left := back6.Add(3 * time.Second)
	check("dated", append(slices.Clone(events), scaleEvent("down-r1", "Scaled down replica set web-r1 from 1 to 0", 1, left, left)), back6, false)

	// Review: a plain double undo, its events long gone. A (revision 1)
	// made 5h ago, B (revision 2) 4h ago, u back to A (3), u back to B (4).
	// B came back no earlier than it was made, nor than A came back (the
	// revision just below): 4h ago or later, never 5h, and never as exact.
	m1, m2, m5 := now.Add(-5*time.Hour), now.Add(-4*time.Hour), now.Add(-time.Hour)
	g := goBackFixture{uid: "dep-double-undo"}
	twice := []*unstructured.Unstructured{
		g.rs("web-a", "3", "1", m1, "", ""),
		g.rs("web-b", "4", "2", m2, "", "b"),
	}
	say("double undo, B running", g.dep("4", "", "b"), twice, nil, 4, true, m2, true)
	say("double undo, then a new version", g.dep("5", "", "c"), append(slices.Clone(twice), g.rs("web-c", "5", "", m5, "", "c")), nil, 4, false, m2, true)
}

// Review of area B: a Deployment once paused and resumed (kubectl-rollout
// owns spec.paused as well as the template), later set to a broken image
// and undone; kubed started 3 minutes after. The undo's template write is
// what the timeline's hand-scale rows read to know the broken version's
// scale-down to 0 was the undo's: no "scaled to 0" row may appear.
func TestUndoAfterAPauseIsNoHandScale(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB, undo := now.Add(-2*time.Hour), now.Add(-15*time.Minute), now.Add(-3*time.Minute)
	f := goBackFixture{uid: "dep-pause-undo"}
	dep := f.dep("3", "", "")
	dep.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl-rollout", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: undo}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:paused":{},"f:template":{"f:spec":{}}}}`)}})
	kube.NoteTemplateChange(dep)
	broken := f.rs("web-b", "2", "", madeB, "", "")
	c, _, _ := unstructured.NestedSlice(broken.Object, "spec", "template", "spec", "containers")
	c[0].(map[string]any)["image"] = "nginx:nope"
	_ = unstructured.SetNestedSlice(broken.Object, c, "spec", "template", "spec", "containers")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", madeA, "", ""), broken,
	}, kube.Events: {
		scaleEvent("up-b", "Scaled up replica set web-b from 0 to 1", 1, madeB, madeB),
		scaleEvent("down-b", "Scaled down replica set web-b from 1 to 0", 1, undo, undo),
	}}, testResources(), nil)
	for _, ch := range e.Changes("qa", "", now, nil) {
		if ch.What == "scale" {
			t.Errorf("the undo's scale-down read as a scale by hand: %s %s", ch.Object, ch.Detail)
		}
	}
}

// Review of area B: a go-back kubed's watch saw (u in kubed), then the
// Deployment paused. Its exact time stays: the write was seen as it
// happened, before the pause. With no progress deadline the Deployment has
// no Progressing condition at all; a write seen live needs none.
func TestGoBackSeenLiveThenPausedKeepsItsTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeB, undo, pause := now.Add(-50*time.Second), now.Add(-40*time.Second), now.Add(-30*time.Second), now.Add(-20*time.Second)
	f := goBackFixture{uid: "dep-live-pause"}
	write := func(rev, env string, at time.Time, paused bool) *unstructured.Unstructured {
		d := f.dep(rev, "", env)
		_ = unstructured.SetNestedField(d.Object, paused, "spec", "paused")
		fields := `{"f:spec":{"f:template":{"f:spec":{}}}}`
		if paused {
			fields = `{"f:spec":{"f:paused":{},"f:template":{"f:spec":{}}}}`
		}
		d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl-rollout", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(fields)}})
		kube.NoteTemplateChange(d)
		return d
	}
	write("1", "", madeA, false)
	write("2", "b", madeB, false)
	write("3", "", undo, false)
	dep := write("3", "", pause, true)
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", madeA, "", ""),
		f.rs("web-b", "2", "", madeB, "", "b"),
	}}, testResources(), nil)
	for _, v := range e.RolloutOf(dep).All {
		if v.Revision == 3 && !v.LiveSince.Equal(undo) {
			t.Errorf("revision 3 back at %v (or after %v), want %v as seen", v.LiveSince.Format(time.TimeOnly), v.BackAfter.Format(time.TimeOnly), undo.Format(time.TimeOnly))
		}
	}
	if dt := e.DeployTimes("qa")[types.UID(f.uid)]; !dt.At.Equal(undo) || dt.After || dt.Before {
		t.Errorf("Deployed %v (after %v, before %v), want %v", dt.At.Format(time.TimeOnly), dt.After, dt.Before, undo.Format(time.TimeOnly))
	}
}

// Review of area B: a Deployment deleted and made again with the same
// name and templates gets ReplicaSets of the same names, and the old one's
// events stay for an hour. Events naming another Deployment (by UID) are
// not this one's and date nothing.
func TestGoBackNotDatedByAnEarlierDeploymentsEvents(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeE, back := now.Add(-10*time.Minute), now.Add(-8*time.Minute), now.Add(-6*time.Minute)
	f := goBackFixture{uid: "dep-again"}
	dep := f.dep("4", "", "")
	old := func(ev *unstructured.Unstructured) *unstructured.Unstructured {
		_ = unstructured.SetNestedField(ev.Object, "dep-before", "involvedObject", "uid")
		return ev
	}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", madeA, "", ""),
		f.rs("web-e", "2", "", madeE, "", "bar"),
	}, kube.Events: {
		old(scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 1, back, back)),
		old(scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 1, back.Add(2*time.Second), back.Add(2*time.Second))),
	}}, testResources(), nil)
	for _, v := range e.RolloutOf(dep).All {
		if v.Revision == 3 && !v.LiveSince.IsZero() {
			t.Errorf("revision 3 dated %v by another Deployment's events", v.LiveSince.Format(time.TimeOnly))
		}
	}
}

// Review of area B: A runs, a broken version E goes out (A keeps running),
// everything is scaled to 0 by hand and back up (only E, the newest, gets
// a pod), then u back to A. Kubernetes' spam filter dropped A's own
// scalings. E's first scale-down to 0 was the hand scale, not the go-back:
// with A's scale-up missing, nothing proves when it came back.
func TestGoBackNotDatedWhenEventsWereDropped(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	madeA, madeE, hand, handUp, undo := now.Add(-30*time.Minute), now.Add(-20*time.Minute), now.Add(-15*time.Minute), now.Add(-12*time.Minute), now.Add(-5*time.Minute)
	f := goBackFixture{uid: "dep-dropped"}
	dep := f.dep("3", "", "")
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {
		f.rs("web-a", "3", "1", madeA, "", ""),
		f.rs("web-e", "2", "", madeE, "", "bar"),
	}, kube.Events: {
		scaleEvent("up-a", "Scaled up replica set web-a from 0 to 1", 1, madeA, madeA),
		scaleEvent("up-e", "Scaled up replica set web-e from 0 to 1", 2, madeE, handUp),
		scaleEvent("down-e", "Scaled down replica set web-e from 1 to 0", 2, hand, undo),
	}}, testResources(), nil)
	for _, v := range e.RolloutOf(dep).All {
		if v.Revision == 3 && !v.LiveSince.IsZero() {
			t.Errorf("revision 3 back at %v, want a bound (the undo was at %v)", v.LiveSince.Format(time.TimeOnly), undo.Format(time.TimeOnly))
		}
	}
}
