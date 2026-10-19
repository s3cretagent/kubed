package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// Round 150: at 60 columns find dropped NAMESPACE though two rows read
// "configmap cm1"; the namespace stays when it is what tells rows apart.
func TestFindKeepsNamespaceForSameNames(t *testing.T) {
	row := func(what, ns, name string) view.Row {
		return view.Row{Obj: view.Named(ns, name), Cells: []string{what, name, ns}}
	}
	rows := []view.Row{
		row("configmap", "kubed-qa1", "cm1"),
		row("configmap", "kubed-qa2-payments", "cm1"),
		row("configmap", "kubed-qa1", "cm1-settings-for-the-payments-worker"),
	}
	tb := table{}
	tb.reset()
	tb.optional = []string{"NAMESPACE"}
	tb.setRows(findHeader, rows, nil)
	out := tb.render(60, 20)
	for _, ns := range []string{"kubed-qa1", "kubed-qa2-payments"} {
		if !strings.Contains(out, ns) {
			t.Errorf("at 60 columns %q is not shown, so the two cm1 rows look the same:\n%s", ns, out)
		}
	}

	// Names that differ: NAMESPACE still goes first when room is short.
	rows[1] = row("configmap", "kubed-qa2-payments", "cm2")
	tb.setRows(findHeader, rows, nil)
	if out := tb.render(60, 20); strings.Contains(out, "kubed-qa2-payments") {
		t.Errorf("names differ, yet NAMESPACE kept at 60 columns:\n%s", out)
	}
}

// Round 150: l on a container that never started, on a node (an image it
// can't get, a volume that can't mount) read "· ended" with the API's raw
// text and "r reads it again". It reads as the no-node case does: "not
// started", one plain line with the reason.
func TestLogsOfContainersThatNeverStarted(t *testing.T) {
	ago := func(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339) }
	pod := func(name, image string, waiting map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "kubed-qa150", "uid": name, "creationTimestamp": ago(5 * time.Minute)},
			"spec": map[string]any{"nodeName": "ip-10-0-1-5", "containers": []any{map[string]any{"name": "main", "image": image}},
				"volumes": []any{map[string]any{"name": "cfg", "configMap": map[string]any{"name": "nope"}}}},
			"status": map[string]any{"phase": "Pending", "startTime": ago(5 * time.Minute),
				"containerStatuses": []any{map[string]any{"name": "main", "image": image, "ready": false, "restartCount": int64(0),
					"state": map[string]any{"waiting": waiting}}}}}}
	}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"name": "nomount.1", "namespace": "kubed-qa150"},
		"involvedObject": map[string]any{"kind": "Pod", "name": "nomount", "namespace": "kubed-qa150"},
		"type":           "Warning", "reason": "FailedMount", "message": `MountVolume.SetUp failed for volume "cfg" : configmap "nope" not found`,
		"firstTimestamp": ago(4 * time.Minute), "lastTimestamp": ago(30 * time.Second), "count": int64(5)}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Events: "EventList"},
		pod("neverpull", "app:local", map[string]any{"reason": "ErrImageNeverPull", "message": `Container image "app:local" is not present with pull policy of Never`}),
		pod("badimage", "nope:1", map[string]any{"reason": "ImagePullBackOff", "message": `Back-off pulling image "nope:1"`}),
		pod("nomount", "busybox", map[string]any{"reason": "ContainerCreating"}), ev)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	defer m.client.Cache.Stop()
	m.enr = enrich.New(m.client.Cache, res, nil)
	m.width, m.height = 140, 30
	for _, g := range []schema.GroupVersionResource{kube.Pods, kube.Events} {
		m.client.Cache.Watch(g)
	}
	for deadline := time.Now().Add(5 * time.Second); (!m.client.Cache.Synced(kube.Pods) || !m.client.Cache.Synced(kube.Events)) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	for name, want := range map[string]string{
		"neverpull": "pull policy is Never",
		"badimage":  "can't download nope:1",
		"nomount":   "nope",
	} {
		ch := make(chan string)
		m.pager = newPager("logs", "", 100, 20)
		m.pager.lines, m.pager.isLogs, m.pager.follow, m.pager.source, m.pager.logs = nil, true, true, logSource{ns: "kubed-qa150", pod: name, container: "main"}, ch
		m.mode = modePager
		raw := `container "main" in pod "` + name + `" is waiting to start: ` + map[string]string{"neverpull": "ErrImageNeverPull", "badimage": "image can't be pulled", "nomount": "ContainerCreating"}[name]
		m.Update(logLinesMsg{ch: ch, lines: []string{"kubed: the container hasn't started yet, so there are no logs. Details: " + raw}, done: true})
		got := ansi.Strip(strings.Join(m.pager.lines, "\n") + "\n" + m.renderBody())
		if strings.Contains(got, "Details") || strings.Contains(got, "is waiting to start") || strings.Contains(got, "stream ended") ||
			strings.Contains(got, "r reads") || strings.Contains(got, "· ended") || !strings.Contains(got, "· not started") ||
			!strings.Contains(got, "hasn't started yet") || !strings.Contains(got, want) {
			t.Errorf("%s: %s", name, got)

		}
	}
}

// Round 150: p on a container its postStart hook got killed read "end of
// the run: it exited with an error (exit 137)" while home said the hook
// failed. The end line says what home says.
func TestPreviousLogsSayPostStartHookKill(t *testing.T) {
	ago := func(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339) }
	po := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "hook", "namespace": "kubed-qa150", "uid": "hook", "creationTimestamp": ago(10 * time.Minute)},
		"spec":     map[string]any{"nodeName": "ip-10-0-1-5", "containers": []any{map[string]any{"name": "main", "image": "busybox"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main", "image": "busybox", "restartCount": int64(1),
			"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off 10s restarting failed container=main"}},
			"lastState": map[string]any{"terminated": map[string]any{"reason": "Error", "exitCode": int64(137), "startedAt": ago(80 * time.Second), "finishedAt": ago(16 * time.Second)}}}}}}}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"name": "hook.1", "namespace": "kubed-qa150"},
		"involvedObject": map[string]any{"kind": "Pod", "name": "hook", "namespace": "kubed-qa150", "fieldPath": "spec.containers{main}"},
		"type":           "Warning", "reason": "FailedPostStartHook", "message": `Exec lifecycle hook ([sh -c exit 3]) for Container "main" in Pod "hook" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`,
		"lastTimestamp": ago(20 * time.Second)}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.Pods: "PodList", kube.Events: "EventList"}, po, ev)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	defer m.client.Cache.Stop()
	m.enr = enrich.New(m.client.Cache, res, nil)
	m.width, m.height = 140, 30
	for _, g := range []schema.GroupVersionResource{kube.Pods, kube.Events} {
		m.client.Cache.Watch(g)
	}
	for deadline := time.Now().Add(5 * time.Second); (!m.client.Cache.Synced(kube.Pods) || !m.client.Cache.Synced(kube.Events)) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	ch := make(chan string)
	m.pager = newPager("logs", "", 100, 20)
	m.pager.lines, m.pager.isLogs, m.pager.source, m.pager.logs = nil, true, logSource{ns: "kubed-qa150", pod: "hook", container: "main", previous: true}, ch
	m.mode = modePager
	m.Update(logLinesMsg{ch: ch, lines: []string{"starting", "hook ran"}, done: true})
	got := ansi.Strip(strings.Join(m.pager.lines, "\n"))
	if !strings.Contains(got, "end of the run: it was killed because its postStart hook failed") || strings.Contains(got, "exited with an error") {
		t.Errorf("%s", got)
	}
}

// Round 150: w on a deployment missed its autoscaler's scale rows when
// the autoscaler has another name (hpa-real scales hpadep): the row's
// object is the autoscaler, which the workload's filter dropped.
func TestWorkloadChangesKeepItsAutoscaler(t *testing.T) {
	now := time.Now().UTC()
	ev := func(name, kind, obj, reason, msg string, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": reason, "message": msg, "type": "Normal",
			"involvedObject": map[string]any{"kind": kind, "name": obj, "namespace": "qa"},
			"lastTimestamp":  at.Format(time.RFC3339)}}
	}
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "hpadep", "namespace": "qa", "uid": "dep-hpadep", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339)}}}
	hpa := func(name, target string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
			"metadata": map[string]any{"name": name, "namespace": "qa"},
			"spec":     map[string]any{"scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": target}}}}
	}
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.HPAs, Kind: "HorizontalPodAutoscaler", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
	})
	m := testModel()
	t.Cleanup(m.client.Cache.Stop)
	m.enr = enrich.New(listStore{
		kube.Deployments: {dep},
		kube.HPAs:        {hpa("hpa-real", "hpadep"), hpa("hpa-other", "otherdep")},
		kube.Events: {
			ev("a", "Deployment", "hpadep", "ScalingReplicaSet", "Scaled up replica set hpadep-abc from 1 to 3", now.Add(-5*time.Minute)),
			ev("b", "HorizontalPodAutoscaler", "hpa-real", "SuccessfulRescale", "New size: 1; reason: All metrics below target", now.Add(-time.Minute)),
			ev("c", "HorizontalPodAutoscaler", "hpa-other", "SuccessfulRescale", "New size: 4; reason: cpu resource utilization (percentage of request) above target", now.Add(-2*time.Minute)),
		},
	}, res, nil)
	m.namespace = "qa"
	m.width, m.height = 140, 40
	msg, ok := m.changesIn("qa", "", "deployment hpadep", m.workloadObjects(dep))().(pagerMsg)
	if !ok {
		t.Fatal("no pager")
	}
	if !strings.Contains(msg.content, "hpa-real") || !strings.Contains(msg.content, "1 to 3") || strings.Contains(msg.content, "hpa-other") {
		t.Errorf("deployment w:\n%s", msg.content)
	}
}

// Logs of one container that restarted know it ran before, whether
// opened on the pod or on a workload with that one pod.
func TestOneStreamLogsKnowRestarts(t *testing.T) {
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "web-1", "namespace": "kubed-qa150", "uid": "web-1"},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "app", "image": "app:1"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "app", "image": "app:1", "restartCount": int64(3),
			"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}}}}}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{kube.Pods: "PodList"}, pod)
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn)}}
	m.client.Cache.Watch(kube.Pods)
	for deadline := time.Now().Add(5 * time.Second); !m.client.Cache.Synced(kube.Pods) && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	for name, src := range map[string]logSource{
		"pod":      {ns: "kubed-qa150", pod: "web-1", container: "app"},
		"workload": {ns: "kubed-qa150", workload: "deployment/web", streams: [][2]string{{"web-1", "app"}}},
	} {
		opts := kube.LogOptions{Container: src.container}
		got, ok := m.oneStream(src, &opts)
		if !ok || got != "web-1" || opts.Container != "app" || !opts.Restarted {
			t.Errorf("%s: pod %q ok %v opts %+v", name, got, ok, opts)
		}
	}
	if _, ok := m.oneStream(logSource{ns: "kubed-qa150", workload: "deployment/web", streams: [][2]string{{"web-1", "app"}, {"web-2", "app"}}}, &kube.LogOptions{}); ok {
		t.Error("two pods read as one stream")
	}
}
