package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Round 150: a postStart hook kill read as a plain error in :changes
// ("container main stopped with an error (exit 137)") while home said the
// hook failed. The crash row says what home says.
func TestChangesSaysPostStartHookKill(t *testing.T) {
	hook := `Exec lifecycle hook ([sh -c exit 3]) for Container "main" in Pod "h_mxa(uid-h)" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`
	p := mxPod("h", "ReplicaSet", corev1.PodRunning, mxC{name: "main", state: mxWait("CrashLoopBackOff", "back-off 10s restarting failed container=main"),
		restarts: 1, last: mxEnd("Error", 137, 16*time.Second)})
	e, _ := r140Env(p, mxEvent("h.1", "h", "main", "FailedPostStartHook", hook, 20*time.Second))
	got := ""
	for _, ch := range e.Changes("mxa", "", time.Now(), nil) {
		if ch.What == "crash" {
			got += ch.Detail
		}
	}
	if !strings.Contains(got, `container "main" was killed because its postStart hook failed`) || strings.Contains(got, "stopped with an error") {
		t.Errorf(":changes crash row: %q", got)
	}
	// No hook event: the plain crash it was.
	e, _ = r140Env(p)
	got = ""
	for _, ch := range e.Changes("mxa", "", time.Now(), nil) {
		if ch.What == "crash" {
			got += ch.Detail
		}
	}
	if !strings.Contains(got, "stopped with an error (exit 137)") {
		t.Errorf("no hook: %q", got)
	}
}
