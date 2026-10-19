package ui

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// r154App is namespace qa154 shown as one app: a deployment's pod that
// runs (with logs), and, when bare, a pod of its own whose command isn't
// in the image (it never started), which the app's box is about. The
// home list is open on the app.
func r154App(t *testing.T, running bool) *Model {
	t.Helper()
	bare := mxObj("v1", "Pod", "qa154", "bare-cmd", map[string]any{
		"spec": map[string]any{"nodeName": "node-1", "containers": []any{map[string]any{"name": "c", "image": "busybox:1.36", "command": []any{"/no/such/binary"}}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": false, "restartCount": int64(3), "image": "busybox:1.36",
			"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off 40s restarting failed container=c pod=bare-cmd_qa154"}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(128), "reason": "StartError",
				"message": `failed to create containerd task: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: exec: "/no/such/binary": stat /no/such/binary: no such file or directory: unknown`}}}}}})
	objs := []runtime.Object{mxObj("v1", "Namespace", "", "qa154", nil), bare}
	if running {
		web := mxPod("web-abc-x", "apps/v1", "ReplicaSet", "web-abc", "Running", 0, nil)
		web.SetNamespace("qa154")
		objs = append(objs, web)
	}
	c := mxNewCluster(objs, false)
	t.Cleanup(c.client.Cache.Stop)
	m := mxModel(c, 160, 50)
	m.jumpTo(location{kind: locHome})
	found := false
	for i, r := range m.tbl.rows {
		if a, ok := m.appFor(r); ok && a.Namespace == "qa154" {
			m.tbl.jump(i)
			found = true
			if a.Status.Short != "Can't start" || running && a.Pods != 2 {
				t.Fatalf("app %s: %d pods, %q, %q", a.Name, a.Pods, a.Status.Short, a.Status.Why)
			}
		}
	}
	if !found {
		t.Fatal("no app in qa154")
	}
	return m
}

// QA round 154: l on an app whose box is about a pod that never started
// refused all its logs, though its other pods run and have logs. It
// follows them; with no other pod it still says it never ran.
func TestAppLogsOtherPodsRan(t *testing.T) {
	for _, running := range []bool{true, false} {
		m := r154App(t, running)
		m.menu()
		seen := false
		for _, it := range m.prompt.items {
			seen = seen || it.label == "Logs (all pods)"
			if it.label == "Logs (all pods)" && it.disabled != !running {
				t.Errorf("other pod runs %v: menu line %q · %q", running, it.off, it.about)
			}
		}
		if !seen {
			t.Errorf("other pod runs %v: no Logs (all pods) line", running)
		}
		m.prompt = nil
		row, _ := m.tbl.selected()
		m.flash = ""
		cmd := m.appLogs(row)
		if running && (cmd == nil || strings.Contains(m.flash, "never started")) {
			t.Errorf("other pod runs: l flashed %q", m.flash)
		}
		if !running && (cmd != nil || !strings.Contains(m.flash, "never started its program, so it has no logs")) {
			t.Errorf("only the pod that never started: l flashed %q", m.flash)
		}
	}
}
