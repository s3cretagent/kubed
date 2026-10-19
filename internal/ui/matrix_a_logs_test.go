package ui

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 126: p on a crashing native sidecar (an init container that
// keeps running) ends with how its run ended, as a regular container's does.
func TestLogEndNoteForSidecar(t *testing.T) {
	var cluster dynamic.Interface
	m := testModelOn(func(d dynamic.Interface) dynamic.Interface { cluster = d; return d })
	defer m.client.Cache.Stop()
	m.client.Cache.Watch(kube.Pods)
	ended := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	stopped := map[string]any{"terminated": map[string]any{"reason": "Error", "exitCode": int64(3), "finishedAt": ended}}
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "side", "namespace": "kubed-qa126", "uid": "side"},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "app"}},
			"initContainers": []any{map[string]any{"name": "proxy", "restartPolicy": "Always"}}},
		"status": map[string]any{"phase": "Running",
			"containerStatuses":     []any{map[string]any{"name": "app", "ready": true, "state": map[string]any{"running": map[string]any{}}}},
			"initContainerStatuses": []any{map[string]any{"name": "proxy", "restartCount": int64(4), "state": stopped, "lastState": stopped}}}}}
	if _, err := cluster.Resource(kube.Pods).Namespace("kubed-qa126").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, ok := m.client.Cache.Get(kube.Pods, "kubed-qa126", "side"); ok {
			break
		}
	}
	for _, previous := range []bool{false, true} {
		src := logSource{ns: "kubed-qa126", pod: "side", container: "proxy", previous: previous}
		if why := m.howItStopped(src); !strings.Contains(why, "exited with an error (exit 3)") {
			t.Errorf("previous %v: %q", previous, why)
		}
	}
	if !m.waitingToRestart(logSource{ns: "kubed-qa126", pod: "side", container: "proxy"}) {
		t.Error("a sidecar stopped between restarts isn't waiting to restart")
	}
}
