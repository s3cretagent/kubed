package ui

// Case map for p (logs from before a restart) on a job in the jobs list:
//   - a job whose tries are new pods (restartPolicy Never), a try failed:
//     no pod restarted, and its pods aren't live; l shows their logs.
//   - a job whose only try never started its program: no logs at all.
//   - a job whose pods are gone: nothing to read.
//   - a job whose setup step (init container) failed: it ran and has
//     logs, so p doesn't say it never started.
//   - a job that ended while its try waited for an image it can't pull:
//     never ran, no logs.
//   - a job whose pod restarts in place (OnFailure), running, not
//     restarted yet: its pod is live, nothing came before.

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

func TestMatrixJobPreviousLogs(t *testing.T) {
	jobWith := func(policy string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "mxj", "namespace": "mx", "uid": "mxj"},
			"spec":     map[string]any{"template": map[string]any{"spec": map[string]any{"restartPolicy": policy}}},
			"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "DeadlineExceeded", // ended: a waiting try is why
				"message": "Job was active longer than specified deadline"}}}}}
	}
	podWith := func(policy, phase string, state map[string]any, restarts int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "mxj-a", "namespace": "mx", "uid": "mxj-a",
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "mxj", "uid": "mxj", "controller": true}}},
			"spec":   map[string]any{"restartPolicy": policy, "containers": []any{map[string]any{"name": "main"}}},
			"status": map[string]any{"phase": phase, "containerStatuses": []any{map[string]any{"name": "main", "restartCount": restarts, "state": state}}}}}
	}
	pod := func(state map[string]any, restarts int64) *unstructured.Unstructured {
		return podWith("Never", "Failed", state, restarts)
	}
	running := map[string]any{"running": map[string]any{}}
	setupFailed := pod(map[string]any{"waiting": map[string]any{"reason": "PodInitializing"}}, 0)
	setupFailed.Object["spec"].(map[string]any)["initContainers"] = []any{map[string]any{"name": "setup"}}
	setupFailed.Object["status"].(map[string]any)["initContainerStatuses"] = []any{map[string]any{"name": "setup", "restartCount": int64(0),
		"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error"}}}}
	pullWait := map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff", "message": `Back-off pulling image "nope:1"`}}
	exit2 := map[string]any{"terminated": map[string]any{"exitCode": int64(2), "reason": "Error"}}
	noStart := map[string]any{"terminated": map[string]any{"exitCode": int64(128), "reason": "StartError",
		"message": `exec: "/run.sh": stat /run.sh: no such file or directory: unknown`}}
	for _, c := range []struct {
		name   string
		policy string
		pods   []*unstructured.Unstructured
		flash  string
		never  string
	}{
		{"failed try, a new pod each try", "Never", []*unstructured.Unstructured{pod(exit2, 0)}, "each try in a new pod", "live"},
		{"never started", "Never", []*unstructured.Unstructured{pod(noStart, 0)}, "never started its program", "live"},
		{"pods gone", "Never", nil, "no pod of mxj exists", ""},
		{"setup step failed: it ran, and has logs", "Never", []*unstructured.Unstructured{setupFailed}, "each try in a new pod", "never started"},
		{"image never pulled", "Never", []*unstructured.Unstructured{podWith("Never", "Pending", pullWait, 0)}, "never started its program", "live"},
		// OnFailure restarts in place: the pod is live, and a restart would have earlier logs.
		{"OnFailure, running, no restart yet", "OnFailure", []*unstructured.Unstructured{podWith("OnFailure", "Running", running, 0)},
			"no pod of mxj has restarted, so there are no earlier logs. l shows the live ones", "new pod"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Jobs, Kind: "Job", Namespaced: true}})
			job := jobWith(c.policy)
			objs := []runtime.Object{job}
			for _, p := range c.pods {
				objs = append(objs, p)
			}
			dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
				map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Jobs: "JobList"}, objs...)
			m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
			defer m.client.Cache.Stop()
			m.enr = enrich.New(m.client.Cache, res, nil)
			m.client.Cache.Watch(kube.Pods)
			m.client.Cache.Watch(kube.Jobs)
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && (!m.client.Cache.Synced(kube.Pods) || !m.client.Cache.Synced(kube.Jobs)); time.Sleep(10 * time.Millisecond) {
				m.client.Cache.List(kube.Pods, "")
				m.client.Cache.List(kube.Jobs, "")
			}
			if _, err := dyn.Resource(kube.Jobs).Namespace("mx").Get(context.Background(), "mxj", metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}
			if cmd := m.workloadPreviousLogs(view.Row{Obj: job}); cmd != nil {
				t.Errorf("opened logs")
			}
			if !strings.Contains(m.flash, c.flash) || c.never != "" && strings.Contains(m.flash, c.never) {
				t.Errorf("flash %q, want %q", m.flash, c.flash)
			}
		})
	}
}

// The Watch line agrees with gC and home on a cron job whose runs don't
// succeed and aren't kept (failedJobsHistoryLimit 0): not "look fine".
func TestMatrixWatchCronRunsGone(t *testing.T) {
	cj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "mxcj", "namespace": "mx", "uid": "mxcj"},
		"spec":     map[string]any{"schedule": "* * * * *", "failedJobsHistoryLimit": int64(0)},
		"status":   map[string]any{"lastScheduleTime": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Jobs, Kind: "Job", Namespaced: true},
		{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Jobs: "JobList", kube.CronJobs: "CronJobList"}, cj)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	defer m.client.Cache.Stop()
	m.enr = enrich.New(m.client.Cache, res, nil)
	var items []watchItem
	loading := true
	for deadline := time.Now().Add(5 * time.Second); loading && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		items, loading = m.watchItems()
	}
	if len(items) != 1 || items[0].text != "1 cron job not succeeding" || items[0].key != "gC" {
		t.Errorf("watch items %+v (loading %v)", items, loading)
	}
}

// A job that never ran (its image never pulled, its pods gone, the pull
// failure kept in events): l and p, on home and on gj, all say it never
// started, so it has no logs, and point at the box.
func TestMatrixNeverRanLogsAgree(t *testing.T) {
	made := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "badimg", "namespace": "mx", "uid": "badimg", "labels": map[string]any{"app": "badimg"}, "creationTimestamp": made},
		"spec": map[string]any{"activeDeadlineSeconds": int64(60), "selector": map[string]any{"matchLabels": map[string]any{"batch.kubernetes.io/job-name": "badimg"}},
			"template": map[string]any{"spec": map[string]any{"restartPolicy": "Never"}}},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "DeadlineExceeded",
			"message": "Job was active longer than specified deadline", "lastTransitionTime": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}}}}}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"name": "badimg-bcdfg.1", "namespace": "mx"},
		"involvedObject": map[string]any{"kind": "Pod", "name": "badimg-bcdfg", "namespace": "mx"},
		"type":           "Warning", "reason": "Failed", "message": `Failed to pull image "nope:1": docker.io/library/nope:1: not found`,
		"lastTimestamp": time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Jobs, Kind: "Job", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Jobs: "JobList", kube.Events: "EventList"}, job, ev)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	defer m.client.Cache.Stop()
	m.enr = enrich.New(m.client.Cache, res, nil)
	for _, g := range []schema.GroupVersionResource{kube.Pods, kube.Jobs, kube.Events} {
		m.client.Cache.Watch(g)
	}
	synced := func() bool {
		return m.client.Cache.Synced(kube.Pods) && m.client.Cache.Synced(kube.Jobs) && m.client.Cache.Synced(kube.Events)
	}
	for deadline := time.Now().Add(5 * time.Second); !synced() && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	m.apps = view.Apps(m.enr, nil, nil, nil, m.client.Cache.List(kube.Jobs, ""), nil)
	if len(m.apps) != 1 || !strings.Contains(m.apps[0].Status.Why, "never ran") {
		t.Fatalf("apps %+v", m.apps)
	}
	app := view.Row{Obj: view.Named("mx", m.apps[0].Name)}
	for _, c := range []struct {
		name string
		run  func() tea.Cmd
	}{
		{"home l", func() tea.Cmd { m.loc = location{kind: locHome}; return m.appLogs(app) }},
		{"home p", func() tea.Cmd { m.loc = location{kind: locHome}; return m.appPreviousLogs(app) }},
		{"gj l", func() tea.Cmd {
			m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Jobs, Kind: "Job"}}
			return m.workloadLogs(view.Row{Obj: job})
		}},
		{"gj p", func() tea.Cmd {
			m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Jobs, Kind: "Job"}}
			return m.workloadPreviousLogs(view.Row{Obj: job})
		}},
	} {
		m.flash = ""
		if cmd := c.run(); cmd != nil || !strings.Contains(m.flash, "never started its program, so it has no logs. The box says why") {
			t.Errorf("%s: flash %q", c.name, m.flash)
		}
	}
	// Review of round 127: the menu's Logs (all pods) line says what l
	// does, and asking it flashes nothing.
	m.flash = ""
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Jobs, Kind: "Job"}}
	if err := m.noLogPods(job); err == nil || !strings.Contains(err.Error(), "never started its program, so it has no logs") || m.flash != "" {
		t.Errorf("gj Logs (all pods) line: %v, flash %q", err, m.flash)
	}
}

// QA round 128: the Enter menu of a job that never ran (its image never
// pulled, its deadline passed, its pods gone) says why its two Logs lines
// are off as l and p do, not "no pods": built through the real menu, on
// gj and on home.
func TestNeverRanJobMenuLogsLines(t *testing.T) {
	made := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	labels := map[string]any{"app": "badimg"}
	job := mxObj("batch/v1", "Job", "mx", "badimg", map[string]any{
		"metadata": map[string]any{"labels": labels, "creationTimestamp": made},
		"spec": map[string]any{"activeDeadlineSeconds": int64(60), "selector": map[string]any{"matchLabels": map[string]any{"batch.kubernetes.io/job-name": "badimg"}},
			"template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"restartPolicy": "Never",
				"containers": []any{map[string]any{"name": "main", "image": "registry.invalid/nope/never:1"}}}}},
		"status": map[string]any{"failed": int64(1), "terminating": int64(0), "conditions": []any{
			map[string]any{"type": "FailureTarget", "status": "True", "reason": "DeadlineExceeded", "message": "Job was active longer than specified deadline",
				"lastTransitionTime": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)},
			map[string]any{"type": "Failed", "status": "True", "reason": "DeadlineExceeded", "message": "Job was active longer than specified deadline",
				"lastTransitionTime": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}}}})
	ev := mxObj("v1", "Event", "mx", "badimg-bcdfg.1", map[string]any{
		"involvedObject": map[string]any{"kind": "Pod", "name": "badimg-bcdfg", "namespace": "mx"},
		"type":           "Warning", "reason": "Failed", "message": `Failed to pull image "registry.invalid/nope/never:1": failed to resolve reference "registry.invalid/nope/never:1": dial tcp: lookup registry.invalid: no such host`,
		"lastTimestamp": time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)})
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "mx", nil), job, ev}, false)
	defer cl.client.Cache.Stop()
	check := func(where string, open func(m *Model)) {
		m := mxModel(cl, 160, 50)
		open(m)
		m.menu()
		seen := 0
		for _, it := range m.prompt.items {
			if it.label != "Logs (all pods)" && it.label != "Logs before the last restart" {
				continue
			}
			seen++
			if !it.disabled || it.off == "no pods" || !strings.Contains(it.about, "never started its program, so it has no logs") {
				t.Errorf("%s: %q · %q: %q", where, it.label, it.off, it.about)
			}
		}
		if seen != 2 {
			t.Errorf("%s: %d Logs lines in the menu", where, seen)
		}
	}
	check("gj", func(m *Model) {
		m.jumpTo(location{kind: locHome})
		m.openResource("jobs", true)
		for i, r := range m.tbl.rows {
			if r.Obj.GetName() == "badimg" {
				m.tbl.jump(i)
			}
		}
	})
	check("home", func(m *Model) {
		m.jumpTo(location{kind: locHome})
		for i, r := range m.tbl.rows {
			if a, ok := m.appFor(r); ok && a.Name == "badimg" {
				m.tbl.jump(i)
			}
		}
	})
}
