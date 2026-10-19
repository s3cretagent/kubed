package ui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 136: going back on a daemon set to a version whose own pods
// fail said nothing (only a Deployment's were checked), while home's Fix
// says going back won't help. u says so for a daemon set too.
func TestUndoWarnsWhenADaemonSetsEarlierVersionFails(t *testing.T) {
	ds := mxObj("apps/v1", "DaemonSet", "shop", "agent", map[string]any{"spec": map[string]any{
		"selector": map[string]any{"matchLabels": map[string]any{"app": "agent"}}}})
	cr := func(name string, rev int64, image string) runtime.Object {
		return mxObj("apps/v1", "ControllerRevision", "shop", name, map[string]any{"metadata": mxOwner("apps/v1", "DaemonSet", "agent"), "revision": rev,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}}}})
	}
	old := mxPod("agent-old", "apps/v1", "DaemonSet", "agent", "Running", 6, map[string]any{"app": "agent", "controller-revision-hash": "h1"})
	cs := old.Object["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)
	cs["ready"] = false
	cs["state"] = map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off"}}
	cs["lastState"] = map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error"}}
	c := mxNewCluster([]runtime.Object{ds, cr("agent-h1", 1, "agent:1"), cr("agent-h2", 2, "agent:bad"), old}, false)
	m := mxModel(c, 120, 40)
	if _, fails := m.undoTargetFails(ds); !fails {
		t.Error("u on the daemon set doesn't say its earlier version fails")
	}
}

// QA round 136: p on a setup step that restarted once (restartCount 1)
// but whose last state is empty said "this container hasn't restarted".
// It did; Kubernetes no longer keeps that run's logs. With no restart
// the words stay.
func TestPreviousLogsGoneAfterARestart(t *testing.T) {
	for _, restarts := range []int64{1, 0} {
		var cluster dynamic.Interface
		m := testModelOn(func(d dynamic.Interface) dynamic.Interface { cluster = d; return d })
		typed := kubefake.NewSimpleClientset()
		typed.PrependReactor("get", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			if a.GetSubresource() != "log" {
				return false, nil, nil
			}
			return true, nil, errors.New(`previous terminated container "setup" in pod "web-x" not found`)
		})
		m.client.Typed = typed
		m.client.Cache.Watch(kube.Pods)
		pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "web-x", "namespace": "qa", "uid": "web-x"},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": "app"}}, "initContainers": []any{map[string]any{"name": "setup"}}},
			"status": map[string]any{"phase": "Running",
				"containerStatuses":     []any{map[string]any{"name": "app", "ready": true, "state": map[string]any{"running": map[string]any{}}}},
				"initContainerStatuses": []any{map[string]any{"name": "setup", "restartCount": restarts, "lastState": map[string]any{}, "state": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}}}}}}}
		if _, err := cluster.Resource(kube.Pods).Namespace("qa").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if _, ok := m.client.Cache.Get(kube.Pods, "qa", "web-x"); ok {
				break
			}
		}
		msg := m.openLogs(logSource{ns: "qa", pod: "web-x", container: "setup", previous: true})()
		got := strings.Join(msg.(logLinesMsg).lines, "\n")
		m.client.Cache.Stop()
		if restarts > 0 && (strings.Contains(got, "hasn't restarted") || !strings.Contains(got, "no longer keeps")) {
			t.Errorf("restarted %d: %q", restarts, got)
		}
		if restarts == 0 && !strings.Contains(got, "hasn't restarted") {
			t.Errorf("not restarted: %q", got)
		}
	}
}

// r136Result runs a command and what it leads to, and returns the first
// resultMsg.
func r136Result(cmd tea.Cmd) (resultMsg, bool) {
	if cmd == nil {
		return resultMsg{}, false
	}
	switch msg := cmd().(type) {
	case resultMsg:
		return msg, true
	case tea.BatchMsg:
		for _, c := range msg {
			if r, ok := r136Result(c); ok {
				return r, true
			}
		}
	}
	return resultMsg{}, false
}

// QA round 136: i on a stateful set that replaces pods only once they
// are deleted (OnDelete) said "now runs <image>", though no pod changed.
// It says the image is set and that pods take it once deleted, as the
// confirm says.
func TestSetImageOnDeleteSaysNotRolledOut(t *testing.T) {
	db := map[string]any{"app": "db"}
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil),
		mxObj("apps/v1", "StatefulSet", "shop", "db", map[string]any{"spec": map[string]any{"replicas": int64(3), "selector": map[string]any{"matchLabels": db},
			"template": mxTemplate(db), "updateStrategy": map[string]any{"type": "OnDelete"}}, "status": map[string]any{"replicas": int64(3), "readyReplicas": int64(3)}}),
	}, false)
	defer cl.client.Cache.Stop()
	cl.client.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("patch", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet"}}, nil
	})
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("statefulsets", true)
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == "db" {
			m.tbl.jump(i)
		}
	}
	if _, err := mxPress(m, "i"); err != nil || m.prompt == nil {
		t.Fatalf("i: %v, flash %q", err, m.flash)
	}
	m.prompt.onSubmit("shop/db:9")
	cmd, err := mxPress(m, "y")
	if err != nil {
		t.Fatal(err)
	}
	res, ok := r136Result(cmd)
	if !ok || res.err != nil || strings.Contains(res.text, "now runs") || !strings.Contains(res.text, "deleted") {
		t.Errorf("result %+v", res)
	}
}

// The same for u: going back on it changes no pod until they are deleted.
func TestGoBackOnDeleteSaysNotRolledOut(t *testing.T) {
	db := map[string]any{"app": "db"}
	cr := func(name string, rev int64, image string) *unstructured.Unstructured {
		return mxObj("apps/v1", "ControllerRevision", "shop", name, map[string]any{"metadata": mxOwner("apps/v1", "StatefulSet", "db"), "revision": rev,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}}}})
	}
	ss := mxObj("apps/v1", "StatefulSet", "shop", "db", map[string]any{"spec": map[string]any{"replicas": int64(3), "selector": map[string]any{"matchLabels": db},
		"template": mxTemplate(db), "updateStrategy": map[string]any{"type": "OnDelete"}}, "status": map[string]any{"replicas": int64(3), "readyReplicas": int64(3)}})
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), ss, cr("db-1", 1, "db:1"), cr("db-2", 2, "db:2")}, false)
	defer cl.client.Cache.Stop()
	yes := true
	typed := kubefake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "shop", UID: types.UID("StatefulSet-db")}},
		&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "db-1", Namespace: "shop", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: "StatefulSet-db", Controller: &yes}}},
			Revision: 1, Data: runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"app","image":"db:1"}]}}}}`)}},
		&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "db-2", Namespace: "shop", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: "StatefulSet-db", Controller: &yes}}},
			Revision: 2, Data: runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"app","image":"db:2"}]}}}}`)}})
	typed.PrependReactor("patch", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &appsv1.StatefulSet{}, nil
	})
	cl.client.Typed = typed // this test's own cluster
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("statefulsets", true)
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == "db" {
			m.tbl.jump(i)
		}
	}
	if _, err := mxPress(m, "u"); err != nil || m.prompt == nil {
		t.Fatalf("u: %v, flash %q", err, m.flash)
	}
	cmd, err := mxPress(m, "y")
	if err != nil {
		t.Fatal(err)
	}
	res, ok := r136Result(cmd)
	if !ok || res.err != nil || strings.Contains(res.text, "is going back") || !strings.Contains(res.text, "deleted") {
		t.Errorf("result %+v", res)
	}
}

// QA round 136: a daemon set that runs on no node left home (its pods
// list it, and it has none). Home keeps it, as a stateful set at 0.
func TestHomeKeepsDaemonSetOnNoNode(t *testing.T) {
	agent := map[string]any{"app": "agent"}
	ds := mxObj("apps/v1", "DaemonSet", "shop", "agent", map[string]any{"metadata": map[string]any{"generation": int64(1)},
		"spec":   map[string]any{"selector": map[string]any{"matchLabels": agent}, "template": mxTemplate(agent)},
		"status": map[string]any{"observedGeneration": int64(1), "desiredNumberScheduled": int64(0)}})
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), ds}, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	m.refreshApps()
	for _, a := range m.apps {
		if a.Name == "agent" {
			if !strings.Contains(a.Status.Why, "runs on no node") {
				t.Errorf("status %+v", a.Status)
			}
			return
		}
	}
	t.Error("the daemon set left home")
}

// QA round 136: k and gg pressed a moment after confirming r with y on
// gd didn't move the cursor. Keys work while the restart runs: k moves
// before its result arrives, and after it, and the restart runs once.
func TestKeysMoveWhileARestartRuns(t *testing.T) {
	var objs []runtime.Object
	objs = append(objs, mxObj("v1", "Namespace", "", "shop", nil))
	for _, name := range []string{"api", "cart", "web"} {
		l := map[string]any{"app": name}
		objs = append(objs, mxObj("apps/v1", "Deployment", "shop", name, map[string]any{"spec": map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": l},
			"template": mxTemplate(l)}, "status": map[string]any{"replicas": int64(2), "readyReplicas": int64(2), "updatedReplicas": int64(2), "availableReplicas": int64(2)}}))
	}
	cl := mxNewCluster(objs, false)
	defer cl.client.Cache.Stop()
	patches := 0
	cl.client.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("patch", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return true, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment"}}, nil
	})
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	if _, err := mxPress(m, "gd"); err != nil {
		t.Fatal(err)
	}
	if len(m.tbl.rows) != 3 {
		t.Fatalf("rows %d", len(m.tbl.rows))
	}
	m.tbl.jump(2)
	if _, err := mxPress(m, "r"); err != nil || m.prompt == nil {
		t.Fatalf("r: %v, flash %q", err, m.flash)
	}
	cmd, err := mxPress(m, "y")
	if err != nil || cmd == nil {
		t.Fatalf("y: %v", err)
	}
	if _, err := mxPress(m, "k"); err != nil || m.tbl.cursor != 1 {
		t.Errorf("k before the result: cursor %d, mode %v, prompt %v", m.tbl.cursor, m.mode, m.prompt != nil)
	}
	res, ok := r136Result(cmd)
	if !ok {
		t.Fatal("no result")
	}
	m.Update(res)
	if _, err := mxPress(m, "gg"); err != nil || m.tbl.cursor != 0 {
		t.Errorf("gg after the result: cursor %d", m.tbl.cursor)
	}
	if patches != 1 {
		t.Errorf("restarted %d times", patches)
	}
}

// QA round 136: s on a setup step that finished (Completed) said "that
// container isn't running right now (it may be crashing). p shows the
// logs from before it stopped", and p then had none; the footer offered
// s on it. It has finished: nothing to open a shell in, no p unless it
// has earlier logs, and no s in the footer for a container not running.
func TestShellOnAFinishedContainer(t *testing.T) {
	pod := mxPodWith("web-abc-1", []mxCS{{name: "setup", state: "done"}, {name: "migrate", restarts: 2, state: "crashed"}},
		[]mxCS{{name: "app", state: "running"}})
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), pod}, false)
	defer cl.client.Cache.Stop()
	for _, c := range []struct {
		container   string
		must, never []string
	}{
		{"setup", []string{"finished"}, []string{"crashing", "p shows"}},
		{"migrate", []string{"p shows"}, []string{"finished"}},
	} {
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		m.openResource("pods", true)
		m.navigate(location{kind: locContainers, podNS: "shop", podName: "web-abc-1"}, true)
		for i, r := range m.tbl.rows {
			if strings.TrimPrefix(r.Obj.GetName(), markPrefix) == c.container {
				m.tbl.jump(i)
				m.tbl.touched = true
			}
		}
		if _, ok := hintKeys(m.hints())["s"]; ok {
			t.Errorf("%s: the footer offers s", c.container)
		}
		if _, err := mxPress(m, "s"); err != nil {
			t.Fatal(err)
		}
		for _, w := range c.must {
			if !strings.Contains(m.flash, w) {
				t.Errorf("%s: s says %q, want %q", c.container, m.flash, w)
			}
		}
		for _, w := range c.never {
			if strings.Contains(m.flash, w) {
				t.Errorf("%s: s says %q", c.container, m.flash)
			}
		}
	}
}
