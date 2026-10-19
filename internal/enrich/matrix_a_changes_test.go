package enrich

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 127: Kubernetes words a back-off "Back-off restarting failed
// container" even for a container that exits 0. Where the pod shows its
// last stop was clean, the timeline says it keeps stopping, not crashing;
// a real crash, or a pod that is gone, keeps the crash words.
func TestBackOffOfACleanExitIsNotACrash(t *testing.T) {
	now := time.Now().UTC()
	stop := func(reason string, code int64, ago time.Duration) map[string]any {
		return map[string]any{"terminated": map[string]any{"reason": reason, "exitCode": code,
			"finishedAt": now.Add(-ago).Format(time.RFC3339)}}
	}
	pod := func(name string, state, last map[string]any) *unstructured.Unstructured {
		cs := map[string]any{"name": "main", "restartCount": int64(7), "lastState": last}
		if state != nil {
			cs["state"] = state
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa127"},
			"status":   map[string]any{"phase": "Running", "containerStatuses": []any{cs}}}}
	}
	waiting := map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}}
	cases := []struct {
		name  string
		pod   *unstructured.Unstructured // nil: gone
		clean bool
	}{
		{"stopped now, exit 0", pod("exiter", stop("Completed", 0, time.Minute), stop("Completed", 0, 6*time.Minute)), true},
		{"waiting, last exit 0", pod("exiter", waiting, stop("Completed", 0, time.Minute)), true},
		{"stopped now with an error after a clean one", pod("exiter", stop("Error", 1, time.Minute), stop("Completed", 0, 6*time.Minute)), false},
		{"waiting, last exit 1", pod("exiter", waiting, stop("Error", 1, time.Minute)), false},
		{"pod gone", nil, false},
	}
	for _, c := range cases {
		ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": "b", "namespace": "qa127"}, "type": "Warning", "reason": "BackOff",
			"message":        "Back-off restarting failed container main in pod exiter_qa127(abc)",
			"involvedObject": map[string]any{"kind": "Pod", "name": "exiter"},
			"lastTimestamp":  now.Add(-30 * time.Second).Format(time.RFC3339)}}
		store := fakeStore{kube.Events: {ev}}
		if c.pod != nil {
			store[kube.Pods] = []*unstructured.Unstructured{c.pod}
		}
		e := New(store, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true},
			{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}), nil)
		var row *Change
		for _, ch := range e.Changes("qa127", "", now, nil) {
			if ch.Object == "pod/exiter" && strings.Contains(ch.Detail, "waiting before") {
				row = &ch
			}
		}
		if row == nil {
			t.Fatalf("%s: no back-off row", c.name)
		}
		clean := row.What == "restart" && strings.Contains(row.Detail, `container "main" keeps stopping; waiting before it is started again`) &&
			!strings.Contains(row.Detail, "crash")
		crash := row.What == "crash" && strings.Contains(row.Detail, "keeps crashing")
		if c.clean && !clean || !c.clean && !crash {
			t.Errorf("%s: %q %q", c.name, row.What, row.Detail)
		}
	}
}

// Review of round 127: a back-off row is reworded only from the stop it
// followed. A back-off 3h ago, while it crashed, stays a crash though its
// newest stop (10 minutes ago) was clean; a clean stop up to 5 minutes
// (the longest back-off) before the back-off is the one it waited after.
// A clean exit forced by the startup check is a kill, not a clean stop.
func TestBackOffRewordedOnlyFromItsOwnStop(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name      string
		stopAgo   time.Duration
		evAgo     time.Duration
		startKill bool
		clean     bool
	}{
		{"back-off 3h ago, clean stop 10m ago", 10 * time.Minute, 3 * time.Hour, false, false},
		{"clean stop 4m before the back-off", 5 * time.Minute, time.Minute, false, true},
		{"clean stop 10m after the back-off", 10 * time.Minute, 20 * time.Minute, false, false},
		{"clean exit forced by the startup check", time.Minute, 30 * time.Second, true, false},
	}
	for _, c := range cases {
		killedByCheck.Delete("qa127/exiter/main") // each case is a new pod
		pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "exiter", "namespace": "qa127"},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main", "restartCount": int64(7),
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"reason": "Completed", "exitCode": int64(0), "finishedAt": now.Add(-c.stopAgo).Format(time.RFC3339)}}}}}}}
		evs := []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": "b", "namespace": "qa127"}, "type": "Warning", "reason": "BackOff",
			"message":        "Back-off restarting failed container main in pod exiter_qa127(abc)",
			"involvedObject": map[string]any{"kind": "Pod", "name": "exiter"},
			"lastTimestamp":  now.Add(-c.evAgo).Format(time.RFC3339)}}}
		if c.startKill {
			evs = append(evs, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
				"metadata": map[string]any{"name": "k", "namespace": "qa127"}, "type": "Normal", "reason": "Killing",
				"message":        "Container main failed startup probe, will be restarted",
				"involvedObject": map[string]any{"kind": "Pod", "name": "exiter", "fieldPath": "spec.containers{main}"},
				"lastTimestamp":  now.Add(-c.stopAgo).Format(time.RFC3339)}})
		}
		e := New(fakeStore{kube.Events: evs, kube.Pods: {pod}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true},
			{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}), nil)
		var row *Change
		for _, ch := range e.Changes("qa127", "", now, nil) {
			if ch.Object == "pod/exiter" && strings.Contains(ch.Detail, "waiting before") {
				row = &ch
			}
		}
		if row == nil {
			t.Fatalf("%s: no back-off row", c.name)
		}
		clean := row.What == "restart" && strings.Contains(row.Detail, "keeps stopping")
		crash := row.What == "crash" && strings.Contains(row.Detail, "keeps crashing")
		if c.clean && !clean || !c.clean && !crash {
			t.Errorf("%s: %q %q", c.name, row.What, row.Detail)
		}
	}
}

// Kubernetes starts a setup step (an init container that is not a native
// sidecar) again only after it failed, so its back-off stays a crash even
// when its newest run, after the back-off, finished clean. A native
// sidecar (restartPolicy Always) is reworded like any container.
func TestBackOffOfASetupStepIsACrash(t *testing.T) {
	now := time.Now().UTC()
	for _, c := range []struct {
		name    string
		sidecar bool
		clean   bool
	}{{"setup step", false, false}, {"native sidecar", true, true}} {
		killedByCheck.Delete("qa128/exiter/fetch")
		spec := map[string]any{"name": "fetch"}
		if c.sidecar {
			spec["restartPolicy"] = "Always"
		}
		pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "exiter", "namespace": "qa128"},
			"spec":     map[string]any{"initContainers": []any{spec}, "containers": []any{map[string]any{"name": "main"}}},
			"status": map[string]any{"phase": "Running", "initContainerStatuses": []any{map[string]any{"name": "fetch", "restartCount": int64(4),
				"lastState": map[string]any{},
				"state":     map[string]any{"terminated": map[string]any{"reason": "Completed", "exitCode": int64(0), "finishedAt": now.Add(-time.Minute).Format(time.RFC3339)}}}}}}}
		ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": "b", "namespace": "qa128"}, "type": "Warning", "reason": "BackOff",
			"message":        "Back-off restarting failed container fetch in pod exiter_qa128(abc)",
			"involvedObject": map[string]any{"kind": "Pod", "name": "exiter"},
			"lastTimestamp":  now.Add(-time.Minute - 13*time.Second).Format(time.RFC3339)}}
		e := New(fakeStore{kube.Events: {ev}, kube.Pods: {pod}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true},
			{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}), nil)
		var row *Change
		for _, ch := range e.Changes("qa128", "", now, nil) {
			if ch.Object == "pod/exiter" && strings.Contains(ch.Detail, "waiting before") {
				row = &ch
			}
		}
		if row == nil {
			t.Fatalf("%s: no back-off row", c.name)
		}
		clean := row.What == "restart" && strings.Contains(row.Detail, "keeps stopping")
		crash := row.What == "crash" && !strings.Contains(row.Detail, "keeps stopping")
		if c.clean && !clean || !c.clean && !crash {
			t.Errorf("%s: %q %q", c.name, row.What, row.Detail)
		}
	}
}
