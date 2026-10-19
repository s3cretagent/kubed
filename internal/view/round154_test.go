package view

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 154 follow-up: an app whose box is about a pod that never
// started said every finished pod it keeps "never started, so they have
// no logs", though finished pods that ran have theirs. Only those that
// never started a container are said to have none.
func TestLeftoverNeverStartedOnlyThoseThatDidnt(t *testing.T) {
	pod := func(name, phase string, st map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "labels": map[string]any{"app": "web"}},
			"spec":     map[string]any{"nodeName": "n1", "containers": []any{map[string]any{"name": "c", "image": "busybox:1.36"}}},
			"status":   map[string]any{"phase": phase, "containerStatuses": []any{map[string]any{"name": "c", "restartCount": int64(0), "image": "busybox:1.36", "state": st}}}}}
	}
	noStart := map[string]any{"terminated": map[string]any{"exitCode": int64(128), "reason": "StartError",
		"message": `exec: "/no/such/binary": stat /no/such/binary: no such file or directory: unknown`}}
	bare := pod("bare-cmd", "Running", map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}})
	bare.Object["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["lastState"] = noStart
	bare.Object["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["restartCount"] = int64(3)
	ran := pod("done-ok", "Succeeded", map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}})
	never := pod("done-never", "Failed", noStart) // a job's run, its Job deleted without it
	never.SetLabels(map[string]string{"app": "web", "job-name": "migrate"})
	for _, c := range []struct {
		name string
		pods []*unstructured.Unstructured
		want string
		not  string
	}{
		{"one finished pod that ran", []*unstructured.Unstructured{bare, ran}, "1 finished pod, kept for its logs", "never started"},
		{"one that ran, one that didn't", []*unstructured.Unstructured{bare, ran, never}, "2 finished pods, kept for their logs; 1 of them never started, so it has no logs", "kept by Kubernetes"},
		{"only one that didn't", []*unstructured.Unstructured{bare, never}, "1 finished pod, kept by Kubernetes (it never started, so it has no logs)", "kept for"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
			e := enrich.New(nsStore{kube.Pods: c.pods}, res, nil)
			apps := Apps(e, c.pods, nil, nil, nil, nil)
			if len(apps) != 1 || apps[0].Status.Short != "Can't start" {
				for _, a := range apps {
					t.Logf("%s: %+v", a.Name, a.Status)
				}
				t.Fatalf("%d apps", len(apps))
			}
			line := ""
			for _, l := range AppLines(apps[0], Lookups{}) {
				if strings.HasPrefix(l, "Leftover") {
					line = l
				}
			}
			if !strings.Contains(line, c.want) || strings.Contains(line, c.not) {
				t.Errorf("%q, want %q", line, c.want)
			}
		})
	}
}
