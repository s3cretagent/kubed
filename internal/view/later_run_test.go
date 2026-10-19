package view

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A failed cron run: its pod's box and its job's box say a later run
// worked only when a run started after it is Complete. An earlier
// success, or a later run still going, leaves "the next run is on
// schedule".
func TestFailedRunLaterSuccessOnly(t *testing.T) {
	w := realLoad(t, "cron_runs.json")
	pod, job := w.get("Pod", "msgs-sweep-a-29868195-6th6m"), w.get("Job", "msgs-sweep-a-29868195")
	box := func() string { return strings.Join(append(podDetail(w.e, pod), workloadProblem(w.e, job)...), "\n") }
	if b := box(); strings.Contains(b, "the next run is on schedule") || strings.Count(b, "a later run worked, ") != 2 {
		t.Errorf("later run worked:\n%s", b)
	}
	unstructured.RemoveNestedField(w.get("Job", "msgs-sweep-a-29868255").Object, "status", "conditions") // still going
	if b := box(); strings.Contains(b, "a later run worked") || strings.Count(b, "the next run is on schedule") != 2 {
		t.Errorf("only an earlier run worked:\n%s", b)
	}
}
