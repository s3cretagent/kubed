package enrich

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Round 122: a failed job's time is the moment it was marked failing, the
// same in home and :changes as in its box; both said when it was marked
// Failed, once its last pod had stopped.
func TestFailedJobOneTimeInHomeAndChanges(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	target, failed := now.Add(-2*time.Minute), now.Add(-90*time.Second)
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "migrate", "namespace": "qa", "uid": "job-migrate", "creationTimestamp": now.Add(-5 * time.Minute).Format(time.RFC3339)},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "FailureTarget", "status": "True", "reason": "BackoffLimitExceeded", "lastTransitionTime": target.Format(time.RFC3339)},
			map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "lastTransitionTime": failed.Format(time.RFC3339)},
		}}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true}, {GVR: kube.Nodes, Kind: "Node"}})
	e := New(fakeStore{kube.Jobs: {job}}, res, nil)
	seen := false
	for _, c := range e.Changes("qa", "", now, nil) {
		if c.Object == "job/migrate" {
			seen = true
			if !c.When.Equal(target) {
				t.Errorf(":changes says %v, want %v", c.When, target)
			}
		}
	}
	if !seen {
		t.Error("no :changes row for the failed job")
	}
	seen = false
	for _, p := range e.Home(now).Problems {
		if p.Name == "migrate" {
			seen = true
			if !p.Since.Equal(target) {
				t.Errorf("home says %v, want %v", p.Since, target)
			}
		}
	}
	if !seen {
		t.Error("no home problem for the failed job")
	}
}
