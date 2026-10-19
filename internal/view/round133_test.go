package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 133: an app made of a pod nothing owns, deleted by hand with a
// long grace, said "new ones follow if it asks for any" on home. Nothing
// follows; the pods list says so ("nothing makes it again"), and so does
// home now.
func TestBarePodStoppingSaysNothingFollows(t *testing.T) {
	now := time.Now()
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "evictme", "namespace": "qa", "uid": "b1", "labels": map[string]any{"app": "evictme"},
			"creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339), "deletionTimestamp": now.Add(590 * time.Second).Format(time.RFC3339),
			"deletionGracePeriodSeconds": int64(600)},
		"spec":   map[string]any{"terminationGracePeriodSeconds": int64(600), "containers": []any{map[string]any{"name": "c", "image": "x"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": true, "state": map[string]any{"running": map[string]any{"startedAt": now.Add(-time.Hour).Format(time.RFC3339)}}}}}}}
	e := enrich.New(mxStore{listStore{kube.Pods: {p}}}, nil, nil)
	var app *App
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil) {
		if a.Name == "evictme" {
			app = a
		}
	}
	if app == nil {
		t.Fatal("app evictme left home")
	}
	if app.Status.Short != "Stopping" || strings.Contains(app.Status.Why, "follow") || !strings.Contains(app.Status.Why, "nothing makes it again") {
		t.Errorf("status %+v", app.Status)
	}
	if box := strings.Join(AppLines(app, Lookups{}), "\n"); strings.Contains(box, "follow") || strings.Contains(box, "replaced") {
		t.Errorf("box says %q", box)
	}
}

// QA round 133: an evicted pod's box said "Restart  container "main"
// stopped (ContainerStatusUnknown, exit 137), - ago": the kubelet gives no
// time, and a pod that ended doesn't restart. No time when none is known,
// and no "Restart" for a pod that ended; the containers list too.
func TestEvictedPodStopHasNoRestartOrUnknownTime(t *testing.T) {
	unknown := &corev1.ContainerStateTerminated{Reason: "ContainerStatusUnknown", ExitCode: 137, Message: "The container could not be located when the pod was terminated"}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "evictme", Namespace: "qa", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "x"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: memory.",
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: unknown}, LastTerminationState: corev1.ContainerState{Terminated: unknown}}}}}
	e := enrich.New(mxStore{listStore{kube.Pods: {mxObj(p)}}}, nil, nil)
	box := strings.Join(podDetail(e, mxObj(p)), "\n")
	if strings.Contains(box, "- ago") || strings.Contains(box, "Restart") {
		t.Errorf("box says %q", box)
	}
	for _, r := range ContainerRows(e, mxObj(p)) {
		if row := strings.Join(r.Cells, " | "); strings.Contains(row, "- ago") {
			t.Errorf("containers list says %q", row)
		}
	}
}

// QA round 135: in the all-namespaces view a whole namespace is one app.
// A failed job there whose own pods are gone must not send you to "the
// failed one" among the namespace's other pods.
func TestFailedJobFixWhenItsPodsAreGoneInABiggerApp(t *testing.T) {
	a := &App{Name: "ns", Status: Status{"Job failed", "job once failed 4m ago: it failed, and so did any retries it was allowed", ToneBad},
		Pods: 2, Leftovers: 1, jobPodsGone: true, failedNoCause: true, kinds: map[string]bool{"Job": true, "Deployment": true}}
	fix := appFix(a, false)
	if strings.Contains(fix, "the failed one") || !strings.Contains(fix, "its failed pods are gone") {
		t.Errorf("fix %q", fix)
	}
	a.jobPodsGone = false // its pod still there: open it
	if fix := appFix(a, false); !strings.Contains(fix, "the failed one") {
		t.Errorf("job pod kept: fix %q", fix)
	}
}
