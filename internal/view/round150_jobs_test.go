package view

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Round 150: a hand delete of the only pod of a backoffLimit 0 job: the
// Killing event and FailureTarget come in the same second. Once the pod
// is gone, home, the box and :changes say it was stopped (deleted).
func TestJobGaveUpTheSecondItsPodWasDeleted(t *testing.T) {
	sawDeletingMu.Lock()
	clear(sawDeleting)
	sawDeletingMu.Unlock()
	const said = "its pod was stopped (deleted), not failed by itself"
	job := []mxJob{{name: "j", ago: 5 * time.Minute, spec: map[string]any{"backoffLimit": int64(0)}, fail: 1,
		conds: []mxCond{{"FailureTarget", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 2 * time.Minute},
			{"Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit", 2 * time.Minute}}}}
	w := mxBuildWith(job, nil, []*unstructured.Unstructured{mxPodEvent("j-x7k2m", "Normal", "Killing", "Stopping container main", 2*time.Minute)})
	changes := ""
	for _, ch := range w.e.Changes(mxNS, "", time.Now(), nil) {
		if ch.Object == "job/j" {
			changes += ch.Detail
		}
	}
	for what, got := range map[string]string{"home": w.app().Status.Why, "box": w.jobBox("j"), ":changes": changes} {
		if !strings.Contains(got, said) || strings.Contains(got, "so did any retries") {
			t.Errorf("%s: %q", what, got)
		}
	}

	// A kept try that failed by itself: a stop that second is its cleanup.
	crashed := mxBuildWith(job, []mxJobPod{mxFailed("j-a3m6p", "j", 2*time.Minute, 1)},
		[]*unstructured.Unstructured{mxPodEvent("j-b4n7q", "Normal", "Killing", "Stopping container main", 2*time.Minute)})
	if stoppedByDelete(crashed.e, crashed.jobs[0]) {
		t.Error("a kept crashed try, and its stop read as stopped by delete")
	}
}
