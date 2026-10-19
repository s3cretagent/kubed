package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
)

// QA round 142: right after a postStart hook first failed, the count was
// 0 but a last stop was known: "0 restarts, the last 16s ago" contradicts
// itself. With no restart yet it says when it stopped.
func TestNoRestartsYetSaysStopped(t *testing.T) {
	hook := `Exec lifecycle hook ([sh -c exit 3]) for Container "app" in Pod "h_mxa(uid-h)" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`
	for _, c := range []struct {
		state  corev1.ContainerState
		events []*unstructured.Unstructured
	}{
		{mxWait("CrashLoopBackOff", "back-off 10s restarting failed container=app"), []*unstructured.Unstructured{
			mxEvent("h.1", "h", "app", "FailedPostStartHook", hook, 20*time.Second)}},
		{mxWait("PostStartHookError", hook), nil},
	} {
		p := mxPod("h", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: c.state, restarts: 0, last: mxEnd("Error", 137, 16*time.Second)})
		e, _ := r140Env(p, c.events...)
		st := Explain(e, p)
		if strings.Contains(st.Why, "0 restarts") || !strings.Contains(st.Why, "stopped 16s ago") {
			t.Errorf("status %+v", st)
		}
	}
	if got := restartsAgo(3, mxEnd("Error", 1, time.Minute), false); got != "3 restarts, the last 1m ago" {
		t.Errorf("%q", got)
	}
	if got := restartsAgo(0, mxEnd("OOMKilled", 137, time.Minute), true); got != "killed 1m ago" {
		t.Errorf("%q", got)
	}
	if got := restartsAgo(2, mxEnd("OOMKilled", 137, time.Minute), true); got != "2 restarts, last killed 1m ago" {
		t.Errorf("%q", got)
	}
}

// QA round 142 (9): "Rollout stuck: first version of deployment/badkey
// bad config: nginx can't start: ..." had no verb. Every Short glued after
// "first version of deployment/x " reads as a sentence, and says "can't
// start" once.
func TestCantStartWhyHasAVerb(t *testing.T) {
	for short, want := range map[string]string{
		"Bad config":           `has bad config: nginx can't start: secret "x" not found`,
		"Can't start":          "can't start: nginx can't start: ...",
		"Can't pull image":     "can't pull image: nginx can't start: ...",
		"Can't mount":          "can't mount: nginx can't start: ...",
		"Crashing":             "can't start: crashing: nginx can't start: ...",
		"Out of memory":        "can't start: out of memory: nginx can't start: ...",
		"Waiting for a node":   "can't start: waiting for a node: nginx can't start: ...",
		"Too big for any node": "can't start: too big for any node: nginx can't start: ...",
	} {
		why := "nginx can't start: ..."
		if short == "Bad config" {
			why = `nginx can't start: secret "x" not found`
		}
		if got := cantStartWhy(Status{Short: short, Why: why}); got != want {
			t.Errorf("%s: %q, want %q", short, got, want)
		}
	}
}

// QA round 142 (10): Deployed says a newer version is rolling out while
// one with other settings has no ready pod yet, and only then.
func TestDeployedSaysANewerVersionIsRollingOut(t *testing.T) {
	at := time.Now().Add(-time.Hour)
	for _, pending := range []bool{false, true} {
		a := &App{Name: "web", deploys: []enrich.DeployTime{{At: at, Pending: pending}}}
		settleDeployed(a)
		box := strings.Join(AppLines(a, Lookups{}), "\n")
		if got := strings.Contains(box, "; a newer version is rolling out (not ready yet)"); got != pending {
			t.Errorf("pending %v: box:\n%s", pending, box)
		}
		if !a.LastDeploy.Equal(at) {
			t.Errorf("the time moved: %v", a.LastDeploy)
		}
	}
}

// Review of round 142: a go-back coming up is an earlier version, not a
// newer one.
func TestDeployedSaysAnEarlierVersionIsComingBack(t *testing.T) {
	a := &App{Name: "web", deploys: []enrich.DeployTime{{At: time.Now().Add(-time.Hour), Pending: true, Returning: true}}}
	settleDeployed(a)
	box := strings.Join(AppLines(a, Lookups{}), "\n")
	if !strings.Contains(box, "; an earlier version is coming back (not ready yet)") || strings.Contains(box, "newer version") {
		t.Errorf("box:\n%s", box)
	}
}
