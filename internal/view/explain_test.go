package view

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

func pod(status corev1.PodStatus) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}}}}},
		Status: status}
}

func TestExplain(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	ago := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	cases := []struct {
		name   string
		status corev1.PodStatus
		short  string
		whyHas string
	}{
		{"oom", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 14,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, FinishedAt: ago}}}}},
			"Out of memory", "limit 256Mi"},
		{"crash", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 3,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: ago}}}}},
			"Crashing", "last exit code 1"},
		{"image", corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Image: "busybox:nope",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: `rpc error: ... busybox:nope: not found`}}}}},
			"Can't pull image", "doesn't exist"},
		{"unschedulable", corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
			Message: "0/38 nodes are available: 3 Insufficient cpu, 35 node(s) had untolerated taint {workload-type: x}. preemption: 0/38 nodes are available"}}},
			"Waiting for a node", "not enough free CPU on 3 nodes; 35 nodes are reserved"},
		{"config", corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "app",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerConfigError", Message: `secret "db" not found`}}}}},
			"Bad config", `secret "db" not found`},
		{"caught between restarts", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 13,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: ago}}}}},
			"Crashing", "keeps crashing"},
		{"stopped again since the stop before", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 7,
			State:                corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: metav1.NewTime(time.Now().Add(-40 * time.Second))}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: ago}}}}},
			"Crashing", "last exit code 1, 40s ago"},
		{"just restarted after an error", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 13,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: ago}}}}},
			"Crashing", "keeps crashing"},
		{"healthy", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago}}}}}, "Running", "healthy"},
	}
	for _, c := range cases {
		st := Explain(e, pod(c.status))
		if st.Short != c.short || !strings.Contains(st.Why, c.whyHas) {
			t.Errorf("%s: got %q / %q, want %q containing %q", c.name, st.Short, st.Why, c.short, c.whyHas)
		}
	}
}

func TestAppsWithoutArgoGroupByLabelAndSkipLeftovers(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	mk := func(name, app, phase, reason string) *unstructured.Unstructured {
		o := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "default", "uid": name, "resourceVersion": "1",
				"labels": map[string]any{"app.kubernetes.io/instance": app}},
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "c"}}},
			"status": map[string]any{"phase": phase, "reason": reason, "containerStatuses": []any{map[string]any{"name": "c", "ready": phase == "Running", "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}},
		}}
		return o
	}
	pods := []*unstructured.Unstructured{
		mk("api-1", "api", "Running", ""),
		mk("api-old", "api", "Failed", "Evicted"),
		mk("worker-1", "worker", "Running", ""),
	}
	apps := Apps(e, pods, nil, nil, nil, nil)
	if len(apps) != 2 {
		t.Fatalf("want 2 apps (api, worker), got %d", len(apps))
	}
	for _, a := range apps {
		if a.Name == "api" && (a.Pods != 1 || a.Leftovers != 1 || a.Status.Short != "Running") {
			t.Errorf("api = %+v; the evicted pod must not count", a)
		}
	}
}

func TestAppsSortBrokenFirstIdleLast(t *testing.T) {
	apps := []*App{{Name: "idle", Status: Status{Tone: ToneDim}}, {Name: "ok", Status: Status{Tone: ToneNormal}},
		{Name: "broken", Status: Status{Tone: ToneBad}}, {Name: "warn", Status: Status{Tone: ToneWarn}}}
	sort.SliceStable(apps, func(i, j int) bool { return severity(apps[i].Status.Tone) > severity(apps[j].Status.Tone) })
	if apps[0].Name != "broken" || apps[1].Name != "warn" || apps[3].Name != "idle" {
		t.Errorf("order = %s %s %s %s", apps[0].Name, apps[1].Name, apps[2].Name, apps[3].Name)
	}
}

// An ArgoCD app must be one row: its Application record (often without a
// destination namespace) and its pods are the same app.
func TestArgoAppIsOneRow(t *testing.T) {
	app := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "auth", "namespace": "argocd"},
		"spec":     map[string]any{"destination": map[string]any{"server": "https://kubernetes.default.svc"}},
	}}
	e := enrich.New(benchStore{}, nil, nil)
	apps := Apps(e, nil, []*unstructured.Unstructured{app}, nil, nil, nil)
	if len(apps) != 1 || apps[0].Name != "auth" {
		t.Fatalf("apps = %+v", apps)
	}
	if lines := AppLines(apps[0], Lookups{}); strings.Contains(strings.Join(lines, "\n"), "namespace default") {
		t.Errorf("an app with no namespace must not claim 'default': %v", lines)
	}
}

func TestNamespaceOwner(t *testing.T) {
	vm, op := &App{Name: "victoria-metrics"}, &App{Name: "victoria-metrics-operator"}
	if namespaceOwner([]*App{op, vm}, "victoria-metrics") != vm {
		t.Error("the app named after the namespace owns its untagged pods")
	}
	if namespaceOwner([]*App{op}, "anything") != op {
		t.Error("a sole app owns its namespace")
	}
	if namespaceOwner([]*App{op, {Name: "x"}}, "other") != nil {
		t.Error("ambiguous namespaces have no owner")
	}
}

// A failing ExternalSecret: a warning with the real error, its impact and
// a fix.
func TestFailingSecretExplained(t *testing.T) {
	since := time.Date(2026, 6, 1, 12, 0, 0, 0, time.Local) // the middle of the day: the same date in any zone
	a := &App{Name: "argocd", Ready: 7, Pods: 7, Secrets: []FailingSecret{{
		Namespace: "argocd", Name: "argocd-notifications-secret", Target: "argocd-notifications-secret", Since: since, UsedBy: 7}}}
	a.Status = secretStatus(a.Secrets)
	if a.Status.Tone != ToneWarn || a.Status.Short != "Secret not syncing" {
		t.Errorf("status: %+v", a.Status)
	}
	check := SecretCheck{Error: "key slackToken does not exist in secret /acme/prod/common-alerting-secrets", TargetExists: true}
	lookup := func(_, _ string) (SecretCheck, bool) { return check, true }
	got := strings.Join(AppLines(a, Lookups{Secret: lookup}), "\n")
	for _, want := range []string{
		"Problem  ExternalSecret argocd-notifications-secret can't update secret argocd-notifications-secret since 1 Jun",
		"Error    key slackToken does not exist",
		"Impact   none yet: 7 pods run with the secret's last synced values",
		"Fix      add slackToken to /acme/prod/common-alerting-secrets in the store, or remove it from ExternalSecret argocd-notifications-secret",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	// The tester's case: never synced, and no pod mounts it.
	a.Secrets[0].UsedBy, a.Secrets[0].NeverSynced = 0, true
	if l := strings.Join(AppLines(a, Lookups{Secret: lookup}), "\n"); !strings.Contains(l, "has never had its values: no pod uses it yet") {
		t.Errorf("never synced:\n%s", l)
	}
	if strings.Contains(got, "Where") {
		t.Error("the namespace is already a column")
	}
	if l := strings.Join(AppLines(a, Lookups{}), "\n"); !strings.Contains(l, "reading the error…") || strings.Contains(l, "Impact") {
		t.Errorf("before the lookup returns, say so and don't guess the impact:\n%s", l)
	}
	check.TargetExists = false
	if l := strings.Join(AppLines(a, Lookups{Secret: lookup}), "\n"); !strings.Contains(l, "doesn't exist: pods that use it can't start") {
		t.Errorf("missing secret:\n%s", l)
	}
}

func TestSecretFix(t *testing.T) {
	for msg, want := range map[string]string{
		"AccessDeniedException: User is not authorized": "IAM role",
		"ParameterNotFound: /a/b":                       "create it in the store",
		"":                                              "E shows the app's events",
	} {
		if got := SecretFix(msg, "es"); !strings.Contains(got, want) {
			t.Errorf("SecretFix(%q) = %q, want %q", msg, got, want)
		}
	}
}

func TestUnhealthyAppSaysWhich(t *testing.T) {
	a := &App{Name: "shop-backend", Ready: 11, Pods: 11, Argo: &enrich.ArgoApp{Health: "Degraded"},
		Status: Status{"Failing", "ArgoCD marks part of it as failing", ToneBad}}
	check := ArgoCheck{Broken: []string{"scaledobject/app-server: At least one trigger is falling back on this scaled object"}}
	got := strings.Join(AppLines(a, Lookups{Argo: func(string) (ArgoCheck, bool) { return check, true }}), "\n")
	for _, want := range []string{"Problem  a KEDA scaler can't read its metric", "Error    scaledobject/app-server: At least one trigger is falling back", "Fix      check the system the scaler's trigger reads from"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if l := strings.Join(AppLines(a, Lookups{}), "\n"); !strings.Contains(l, "Finding out which") {
		t.Errorf("while checking: %s", l)
	}
}

func TestOutOfSyncNamesResources(t *testing.T) {
	a := enrich.ArgoApp{Resources: []enrich.ArgoResource{
		{Kind: "Deployment", Name: "api", OutOfSync: true}, {Kind: "ConfigMap", Name: "cfg"}, {Kind: "Service", Name: "api", OutOfSync: true}}}
	if got := outOfSyncWhy(a); got != "deployment/api, service/api differ from Git" {
		t.Errorf("got %q", got)
	}
}

// Scheduled job runs group under their CronJob, and a failed latest run
// marks the app failing; an older failure followed by a success doesn't.
func TestJobsGroupAndFailures(t *testing.T) {
	e := enrich.New(benchStore{}, kube.NewResources([]kube.Resource{
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true},
	}), nil)
	job := func(name string, created time.Time, failed bool) *unstructured.Unstructured {
		status := map[string]any{"succeeded": int64(1)}
		if failed {
			status = map[string]any{"conditions": []any{map[string]any{
				"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "message": "Job has reached the specified backoff limit",
				"lastTransitionTime": created.Add(time.Minute).Format(time.RFC3339)}}}
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": name, "namespace": "jobs", "creationTimestamp": created.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": "sync", "controller": true, "uid": "c"}}},
			"status": status,
		}}
	}
	now := time.Now()
	apps := Apps(e, nil, nil, nil, []*unstructured.Unstructured{job("sync-29854440", now.Add(-2*time.Hour), true), job("sync-29854445", now.Add(-time.Hour), true)}, nil)
	if len(apps) != 1 || apps[0].Status.Short != "Job failed" || !strings.Contains(apps[0].Status.Why, "sync-29854445") {
		t.Fatalf("latest failed run should mark one app failing: %+v", apps)
	}
	apps = Apps(e, nil, nil, nil, []*unstructured.Unstructured{job("sync-29854440", now.Add(-2*time.Hour), true), job("sync-29854445", now.Add(-time.Hour), false)}, nil)
	for _, a := range apps {
		if a.Status.Short == "Job failed" {
			t.Errorf("a later successful run clears the failure: %+v", a.Status)
		}
	}
}

// A pod shutting down normally is calm; one stuck long past its grace
// period is flagged.
func TestStoppingPods(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	grace := int64(120)
	p := pod(corev1.PodStatus{Phase: corev1.PodRunning})
	now := metav1.NewTime(time.Now().Add(-time.Minute))
	p.DeletionTimestamp, p.DeletionGracePeriodSeconds = &now, &grace
	if st := Explain(e, p); st.Short != "Stopping" || st.Tone != ToneNormal {
		t.Errorf("routine stop: %+v", st)
	}
	old := metav1.NewTime(time.Now().Add(-20 * time.Minute))
	p.DeletionTimestamp = &old
	if st := Explain(e, p); st.Short != "Stuck stopping" || st.Tone != ToneWarn {
		t.Errorf("stuck stop: %+v", st)
	}
}

// A routine scale-down (one pod shutting down, the rest ready) leaves the
// app's headline alone and doesn't count as something to check.
func TestScaleDownDoesNotAlarmApp(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	mk := func(name string, stopping bool) *unstructured.Unstructured {
		meta := map[string]any{"name": name, "namespace": "pay", "uid": name, "resourceVersion": "1",
			"labels": map[string]any{"app.kubernetes.io/instance": "payment"}}
		if stopping {
			meta["deletionTimestamp"] = time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
			meta["deletionGracePeriodSeconds"] = int64(120)
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod", "metadata": meta,
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": true,
				"state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}},
		}}
	}
	apps := Apps(e, []*unstructured.Unstructured{mk("payment-1", false), mk("payment-2", true)}, nil, nil, nil, nil)
	if len(apps) != 1 || apps[0].Status.Short != "Running" || apps[0].Status.Tone != ToneNormal {
		t.Fatalf("a routine scale-down must not change the app's status: %+v", apps[0].Status)
	}
	if apps[0].Pods != 1 || apps[0].Ready != 1 {
		t.Errorf("a pod shutting down isn't one of its copies, as in kubectl's READY: pods %d, ready %d", apps[0].Pods, apps[0].Ready)
	}
}

// An app with no pods still gets a row from its workloads: Stopped when
// scaled to 0, a problem when it asks for copies that don't exist,
// Scheduled for a cron job.
func TestAppsWithNoPodsComeFromWorkloads(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	wl := func(kind, name string, replicas int64) *unstructured.Unstructured {
		labels := map[string]any{"app": name}
		o := map[string]any{"apiVersion": "apps/v1", "kind": kind,
			"metadata": map[string]any{"name": name, "namespace": "shop"},
			"spec":     map[string]any{"replicas": replicas, "template": map[string]any{"metadata": map[string]any{"labels": labels}}}}
		if kind == "CronJob" {
			o["spec"] = map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"labels": labels}}}}}
		}
		return &unstructured.Unstructured{Object: o}
	}
	apps := Apps(e, nil, nil, nil, nil, []*unstructured.Unstructured{
		wl("Deployment", "fe", 0), wl("Deployment", "be", 2), wl("CronJob", "tick", 0)})
	got := map[string]Status{}
	for _, a := range apps {
		got[a.Name] = a.Status
	}
	if got["fe"].Short != "Stopped" || got["be"].Tone != ToneBad || got["tick"].Short != "Scheduled" {
		t.Fatalf("got %+v", got)
	}
	// The stopped app still knows its deployment, so it can be scaled up.
	for _, a := range apps {
		if a.Name == "fe" && (!a.OwnsWorkload(e, wl("Deployment", "fe", 0)) || a.OwnsWorkload(e, wl("Deployment", "be", 2))) {
			t.Error("app fe should own deployment fe and only that")
		}
	}
}

// WHY leaves out the key hints the box gives: a column has no room.
func TestWhyWordsDropsKeyHints(t *testing.T) {
	got := whyWords(Status{"Hit memory limit", `6h ago, container "server" (limit 512Mi); p shows the logs from before`, ToneWarn})
	if got != `6h ago, container "server" (limit 512Mi)` {
		t.Fatalf("whyWords = %q", got)
	}
}

// An app ArgoCD can't build from Git is flagged, even when its pods run.
func TestAppsArgoBuildError(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	app := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "vm", "namespace": "argocd"},
		"status": map[string]any{"sync": map[string]any{"status": "Unknown"}, "health": map[string]any{"status": "Healthy"},
			"conditions": []any{map[string]any{"type": "ComparisonError", "message": "Failed to load target state: failed to generate manifest"}}},
	}}
	apps := Apps(e, nil, []*unstructured.Unstructured{app}, nil, nil, nil)
	if len(apps) != 1 || apps[0].Status.Short != "Can't build" {
		t.Fatalf("status = %+v", apps[0].Status)
	}
}

// A job's pods running now don't make the app look freshly deployed.
func TestJobPodsAreNotDeploys(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "sync-1-abc", "namespace": "dwh", "uid": "p1", "resourceVersion": "1",
			"creationTimestamp": time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
			"labels":            map[string]any{"app.kubernetes.io/instance": "dwh"},
			"ownerReferences":   []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "sync-1", "controller": true, "uid": "j"}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "c"}}},
		"status": map[string]any{"phase": "Running"},
	}}
	apps := Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil)
	if len(apps) != 1 || !apps[0].LastDeploy.IsZero() {
		t.Fatalf("a job's run is not a deploy: %+v", apps)
	}
}

func TestShortInterval(t *testing.T) {
	for in, want := range map[string]string{"1h0m0s": "1h", "30m0s": "30m", "1h": "1h", "1h30m0s": "1h30m", "45s": "45s", "0": "0", "": ""} {
		if got := shortInterval(in); got != want {
			t.Errorf("shortInterval(%q) = %q, want %q", in, got, want)
		}
	}
}

// An app fine now but with crashes today says so in WHY, without alarm.
func TestRecentCrashesOnHealthyApp(t *testing.T) {
	a := &App{Status: Status{"Running", "healthy now; crashed in the last day, the last time 8h ago; p shows why", ToneNormal}}
	if got := AppWhy(a, Lookups{}); got != "crashed in the last day, the last time 8h ago" {
		t.Errorf("got %q", got)
	}
}

// A failed pod left behind by a Job that's already gone (a scan tool's
// run) is history: no made-up failing app named after its namespace.
func TestOrphanFailedJobPodIsNoApp(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "scan-abc", "namespace": "trivy-system", "uid": "orphan-scan", "resourceVersion": "1",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "scan", "controller": true, "uid": "gone"}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "c"}}},
		"status": map[string]any{"phase": "Failed"},
	}}
	if apps := Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil); len(apps) != 0 {
		t.Fatalf("want no app, got %+v pods=%d left=%d kinds=%v group=%s", apps[0].Status, apps[0].Pods, apps[0].Leftovers, apps[0].kinds, apps[0].group)
	}
}

// Scaled to 0 is idle; a secret that differs from Git is drift to check.
func TestOnlyWorkloadsDiffer(t *testing.T) {
	scaled := enrich.ArgoApp{Resources: []enrich.ArgoResource{{Kind: "Deployment", Name: "a", OutOfSync: true}, {Kind: "Secret", Name: "s"}}}
	drift := enrich.ArgoApp{Resources: []enrich.ArgoResource{{Kind: "Deployment", Name: "a", OutOfSync: true}, {Kind: "Secret", Name: "s", OutOfSync: true}}}
	if !onlyWorkloadsDiffer(scaled) || onlyWorkloadsDiffer(drift) {
		t.Error("only a differing workload is a pause; a differing secret is drift")
	}
}

// The same pod orphaned (its Job deleted with --cascade=orphan) keeps only
// the job's label: still a finished run, not an app.
func TestOrphanedJobPodByLabelIsNoApp(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "gone-x1", "namespace": "qa", "uid": "orphan-by-label", "resourceVersion": "1",
			"labels": map[string]any{"job-name": "gone", "batch.kubernetes.io/job-name": "gone"}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "c"}}},
		"status": map[string]any{"phase": "Failed"},
	}}
	if apps := Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, nil); len(apps) != 0 {
		t.Fatalf("want no app, got %+v", apps[0].Status)
	}
}

// A container that never started says why in words, from the runtime's
// long message.
func TestStartWords(t *testing.T) {
	missing := `failed to create containerd task: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: exec: "/usr/src/app/workers/manage.bash": stat /usr/src/app/workers/manage.bash: no such file or directory: unknown`
	denied := `failed to create containerd task: OCI runtime create failed: runc create failed: unable to start container process: exec: "/app/run.sh": permission denied: unknown`
	other := `failed to create containerd task: OCI runtime create failed: runc create failed: unable to start container process: error during container init: cgroup setup failed: unknown`
	for in, want := range map[string]string{
		missing: "it never started: /usr/src/app/workers/manage.bash isn't in the image",
		denied:  "it never started: /app/run.sh can't be run (no permission to run it)",
		other:   "it never started: error during container init: cgroup setup failed",
	} {
		if got := StartWords(in); got != want {
			t.Errorf("got  %q\nwant %q", got, want)
		}
	}
}

// Kyverno files passing checks under PolicyViolation too.
func TestPolicyPassIsNotAFailure(t *testing.T) {
	pass := "policy require-resource-limits/autogen-require-requests pass: validation rule 'autogen-require-requests' passed."
	if got := EventWords("PolicyViolation", "Pod", pass); got != "policy check passed" {
		t.Errorf("pass reads %q", got)
	}
	if got := EventWords("PolicyViolation", "Pod", "policy require-resource-limits/x fail: validation error"); got != "policy check failed" {
		t.Errorf("fail reads %q", got)
	}
}

// The taint manager files both "Marking for deletion" and "Cancelling
// deletion" under TaintManagerEviction; a called-off deletion moved nothing.
func TestTaintEvictionCancelledIsNotAMove(t *testing.T) {
	if got := EventWords("TaintManagerEviction", "Pod", "Cancelling deletion of Pod ns/x"); strings.Contains(got, "moved") {
		t.Errorf("cancelled reads %q", got)
	}
	if got := EventWords("TaintManagerEviction", "Pod", "Marking for deletion Pod ns/x"); got != "moved off a node" {
		t.Errorf("marked reads %q", got)
	}
}

// An app ArgoCD tracks whose Application isn't in this cluster: nothing
// says how it syncs, so the box claims no sync policy.
func TestArgoRecordNotHereClaimsNoPolicy(t *testing.T) {
	got := argoWords(enrich.ArgoApp{Name: "login"})
	if strings.Contains(got, "you sync") || strings.Contains(got, "auto-sync") || strings.Contains(got, "in this cluster") || !strings.Contains(got, "can't be read here") {
		t.Errorf("not found reads %q", got)
	}
	if got := argoWords(enrich.ArgoApp{Name: "login", Found: true, Sync: "Synced"}); !strings.Contains(got, "you sync it") {
		t.Errorf("found, manual reads %q", got)
	}
}

// The waste screen never suggests cutting memory an app ran out of.
func TestWasteKeepsMemoryAfterOutOfMemory(t *testing.T) {
	w := enrich.Waste{ReqCPU: 500, PeakCPU: 50, ReqMem: 512 << 20, PeakMem: 100 << 20, SuggestCPU: 65, SuggestMem: 512 << 20,
		RanOutOfMemory: time.Now().Add(-5 * time.Hour)}
	if got := WasteAdvice(w); !strings.Contains(got, "don't cut memory: it ran out of memory 5h") {
		t.Errorf("advice %q", got)
	}
	if rows := WasteRows([]enrich.Waste{{Workload: Named("ns", "gw"), RanOutOfMemory: time.Now()}}, false); rows[0].Tone != ToneWarn {
		t.Error("an app that ran out of memory isn't flagged")
	}
}

// Round 65: a stuck rollout that asks for more than any node has says to
// ask for less, and a fix goes in Git only when ArgoCD manages the app.
func TestAppFixSaysWhereAndWhat(t *testing.T) {
	huge := &App{Status: Status{"Rollout stuck", "pod huge-1: asks for 200 CPU, more than any node here has (the biggest: 8 CPU); unless a bigger machine joins, it never starts", ToneBad}}
	if got := appFix(huge, false); !strings.HasPrefix(got, "lower what it asks for") || strings.Contains(got, "Git") {
		t.Errorf("huge: %q", got)
	}
	nosa := &App{Status: Status{"No pods", "", ToneBad}}
	if got := appFix(nosa, false); strings.Contains(got, "Git") {
		t.Errorf("not managed by ArgoCD, yet: %q", got)
	}
	nosa.Argo = &enrich.ArgoApp{}
	if got := appFix(nosa, false); !strings.Contains(got, "in Git") {
		t.Errorf("managed by ArgoCD, yet: %q", got)
	}
}

// Round 70: a volume claim stuck Pending is flagged; a bound one isn't.
func TestClaimStatus(t *testing.T) {
	claim := func(phase string, age time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
			"metadata": map[string]any{"name": "stuck", "namespace": "qa", "creationTimestamp": time.Now().Add(-age).Format(time.RFC3339)},
			"status":   map[string]any{"phase": phase}}}
	}
	if s, tone := claimStatus(nil, claim("Pending", 6*time.Minute)); tone != ToneWarn || !strings.HasPrefix(s, "Pending") {
		t.Errorf("pending: %q %v", s, tone)
	}
	if _, tone := claimStatus(nil, claim("Pending", 10*time.Second)); tone != ToneNormal {
		t.Error("a claim made seconds ago is flagged")
	}
	if s, tone := claimStatus(nil, claim("Bound", time.Hour)); tone != ToneNormal || s != "Bound" {
		t.Errorf("bound: %q %v", s, tone)
	}
}

// Round 75: a daemon set's only pod shutting down in a restart keeps its
// app on home, as stopping, not dropped as leftovers.
func TestAppWithOnlyStoppingPodsStays(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	o := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "ds-abc", "namespace": "qa", "uid": "ds-abc", "resourceVersion": "1",
			"labels":            map[string]any{"app.kubernetes.io/instance": "ds"},
			"deletionTimestamp": time.Now().Add(-5 * time.Second).UTC().Format(time.RFC3339),
			"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "ds-1", "controller": true}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "c"}}, "terminationGracePeriodSeconds": int64(30)},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": true, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}},
	}}
	apps := Apps(e, []*unstructured.Unstructured{o}, nil, nil, nil, nil)
	for _, a := range apps {
		if a.Name == "ds" {
			if a.Status.Short != "Stopping" {
				t.Errorf("status %q: %q", a.Status.Short, a.Status.Why)
			}
			return
		}
	}
	t.Errorf("app ds dropped: %+v", apps)
}

type listStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s listStore) List(g schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, o := range s[g] {
		if ns == "" || o.GetNamespace() == ns {
			out = append(out, o)
		}
	}
	return out
}
func (s listStore) Get(schema.GroupVersionResource, string, string) (*unstructured.Unstructured, bool) {
	return nil, false
}
func (listStore) Synced(schema.GroupVersionResource) bool { return true }

// Review of round 80: an hour-old "no storage class is set" event doesn't
// describe a claim that has a class now.
func TestClaimStatusIgnoresStaleReason(t *testing.T) {
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "e", "namespace": "qa"}, "reason": "FailedBinding", "type": "Normal",
		"message":        "no persistent volumes available for this claim and no storage class is set",
		"involvedObject": map[string]any{"kind": "PersistentVolumeClaim", "name": "data"},
		"lastTimestamp":  time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339)}}
	e := enrich.New(listStore{kube.Events: {ev}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	claim := func(class string) *unstructured.Unstructured {
		spec := map[string]any{}
		if class != "" {
			spec["storageClassName"] = class
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
			"metadata": map[string]any{"name": "data", "namespace": "qa", "creationTimestamp": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)},
			"spec":     spec, "status": map[string]any{"phase": "Pending"}}}
	}
	if s, _ := claimStatus(e, claim("")); !strings.Contains(s, "names no storage class") {
		t.Errorf("no class: %q", s)
	}
	if s, _ := claimStatus(e, claim("gp3")); strings.Contains(s, "names no storage class") {
		t.Errorf("has a class now, yet: %q", s)
	}
	// "gp3x" not found (an older claim of that name) says nothing of "gp3";
	// nor does any class named like a word of the message ("storage").
	ev.Object["reason"], ev.Object["type"] = "ProvisioningFailed", "Warning"
	ev.Object["message"] = `storageclass.storage.k8s.io "gp3x" not found`
	for _, class := range []string{"gp3", "storage"} {
		if s, _ := claimStatus(e, claim(class)); strings.Contains(s, "doesn't exist") {
			t.Errorf("class %q: %q", class, s)
		}
	}
	if s, _ := claimStatus(e, claim("gp3x")); !strings.Contains(s, "doesn't exist") {
		t.Errorf("class gp3x: %q", s)
	}
}

// Round 82: a claim whose class waits for a pod says so, not flagged.
func TestClaimWaitingForItsPod(t *testing.T) {
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "w", "namespace": "qa"}, "reason": "WaitForFirstConsumer", "type": "Normal",
		"message":        "waiting for first consumer to be created before binding",
		"involvedObject": map[string]any{"kind": "PersistentVolumeClaim", "name": "data"},
		"lastTimestamp":  time.Now().UTC().Format(time.RFC3339)}}
	e := enrich.New(listStore{kube.Events: {ev}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	c := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "data", "namespace": "qa", "creationTimestamp": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)},
		"spec":     map[string]any{"storageClassName": "ebs-sc"}, "status": map[string]any{"phase": "Pending"}}}
	if s, tone := claimStatus(e, c); !strings.Contains(s, "once a pod uses it") || tone != ToneNormal {
		t.Errorf("%q %v", s, tone)
	}
}

// Round 82: an exec alive check kill, the container running again for a
// moment, reads as the kill, not an ordinary crash; its empty answer reads
// in words.
func TestExecAliveCheckKillBetweenRestarts(t *testing.T) {
	now := time.Now().UTC()
	ev := func(name, reason, msg string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "ns"}, "reason": reason, "message": msg,
			"involvedObject": map[string]any{"kind": "Pod", "name": "p", "fieldPath": "spec.containers{app}"},
			"lastTimestamp":  now.Add(-20 * time.Second).Format(time.RFC3339)}}
	}
	e := enrich.New(listStore{kube.Events: {ev("k", "Killing", "Container app failed liveness probe, will be restarted"), ev("u", "Unhealthy", "Liveness probe failed: ")}},
		kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	st := Explain(e, pod(corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 3, Ready: true,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-5 * time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 137, FinishedAt: metav1.NewTime(now.Add(-20 * time.Second))}}}}}))
	if !strings.Contains(st.Why, "killed by its alive check (its command gave no output)") {
		t.Errorf("%q: %q", st.Short, st.Why)
	}
}

// Review: a claim placed on a node (a pod uses it) whose volume fails is
// flagged, though an hour-old "waiting for a pod" event is still kept.
func TestClaimPlacedButFailingIsFlagged(t *testing.T) {
	now := time.Now().UTC()
	ev := func(name, reason, typ, msg string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": reason, "type": typ, "message": msg,
			"involvedObject": map[string]any{"kind": "PersistentVolumeClaim", "name": "data"},
			"lastTimestamp":  now.Add(-time.Minute).Format(time.RFC3339)}}
	}
	e := enrich.New(listStore{kube.Events: {
		ev("w", "WaitForFirstConsumer", "Normal", "waiting for first consumer to be created before binding"),
		ev("f", "ProvisioningFailed", "Warning", "failed to provision volume with StorageClass \"gp3\": UnauthorizedOperation"),
	}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	c := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "data", "namespace": "qa", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations": map[string]any{"volume.kubernetes.io/selected-node": "ip-1"}},
		"spec": map[string]any{"storageClassName": "gp3"}, "status": map[string]any{"phase": "Pending"}}}
	if s, tone := claimStatus(e, c); tone == ToneNormal {
		t.Errorf("a failing volume read as fine: %q", s)
	}
}

// Round 83: in a pod with two containers, a crashing one counts its own
// restarts, not the pod's.
func TestCrashCountsItsOwnRestarts(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	ago := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	p := pod(corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
		{Name: "crash", RestartCount: 6, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: ago}}},
		{Name: "web", RestartCount: 7, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago}}},
	}})
	if st := Explain(e, p); !strings.Contains(st.Why, "6 restarts") {
		t.Errorf("%q", st.Why)
	}
}

// Round 84: the containers list says a container was killed by its alive
// check, not "finished" (Kubernetes records it as a clean exit).
func TestContainerListSaysAliveCheckKill(t *testing.T) {
	now := time.Now().UTC()
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "k", "namespace": "ns"}, "reason": "Killing", "message": "Container app failed liveness probe, will be restarted",
		"involvedObject": map[string]any{"kind": "Pod", "name": "p"}, "lastTimestamp": now.Add(-10 * time.Second).Format(time.RFC3339)}}
	e := enrich.New(listStore{kube.Events: {ev}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
	p := pod(corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 2,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", ExitCode: 0, FinishedAt: metav1.NewTime(now.Add(-10 * time.Second))}}}}})
	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	u := &unstructured.Unstructured{Object: obj}
	u.SetKind("Pod")
	rows := ContainerRows(e, u)
	if len(rows) == 0 || !strings.Contains(strings.Join(rows[0].Cells, " "), "killed by its alive check") {
		t.Errorf("rows %v", rows)
	}
	if lines := ContainerDetail(e, u, "app"); !strings.Contains(strings.Join(lines, "\n"), "Stopped  killed by its alive check") {
		t.Errorf("box %v", lines)
	}
}

// Round 85: a 10m CPU request read "0.0 cores" in the containers list.
func TestContainerListSmallCPU(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := pod(corev1.PodStatus{Phase: corev1.PodRunning})
	p.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")}
	p.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("1")
	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	u := &unstructured.Unstructured{Object: obj}
	u.SetKind("Pod")
	rows := ContainerRows(e, u)
	if len(rows) == 0 || !strings.Contains(strings.Join(rows[0].Cells, "|"), "|0.01 cores / 1 core|") {
		t.Errorf("rows %v", rows)
	}
}

// Round 87: events in the same second (one with milliseconds, a kubelet
// one without) sort as equal, so they must read the same age.
func TestEventAgesMatchTheirOrder(t *testing.T) {
	v := For(kube.Resource{GVR: kube.Events, Kind: "Event", Namespaced: true})
	sec := time.Now().Add(-22 * time.Second).Truncate(time.Second)
	kubelet := &unstructured.Unstructured{Object: map[string]any{"kind": "Event", "reason": "Started",
		"lastTimestamp": sec.Format(time.RFC3339)}}
	kyverno := &unstructured.Unstructured{Object: map[string]any{"kind": "Event", "reason": "PolicyViolation",
		"eventTime": sec.Add(900 * time.Millisecond).Format(time.RFC3339Nano)}}
	if v.Time(kubelet).Unix() != v.Time(kyverno).Unix() {
		t.Fatal("not the same second")
	}
	a, _ := v.Row(nil, kubelet)
	b, _ := v.Row(nil, kyverno)
	if a[0] != b[0] {
		t.Errorf("same second, ages %q and %q", a[0], b[0])
	}
}

// Round 89: between a failed try and the next, a job's app read "Running"
// though nothing ran. Not when it is paused, ending or long quiet.
type jobStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (s jobStore) List(g schema.GroupVersionResource, _ string) []*unstructured.Unstructured {
	return s[g]
}
func (s jobStore) Get(g schema.GroupVersionResource, _, name string) (*unstructured.Unstructured, bool) {
	for _, o := range s[g] {
		if o.GetName() == name {
			return o, true
		}
	}
	return nil, false
}
func (jobStore) Synced(schema.GroupVersionResource) bool { return true }

func TestJobBetweenTriesIsRetrying(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	job := func(status map[string]any, suspend bool) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "failjob", "namespace": "jobs", "uid": "j1", "creationTimestamp": time.Now().Add(-time.Minute).Format(time.RFC3339)},
			"spec":     map[string]any{"suspend": suspend},
			"status":   status,
		}}
	}
	failedPod := func(ago time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "failjob-x", "namespace": "jobs", "uid": "p1",
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "failjob", "uid": "j1", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}},
			"status": map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "j",
				"state": map[string]any{"terminated": map[string]any{"exitCode": int64(2), "reason": "Error",
					"finishedAt": time.Now().Add(-ago).Format(time.RFC3339)}}}}}}}
	}
	status := func(j *unstructured.Unstructured, p *unstructured.Unstructured, more ...*unstructured.Unstructured) string {
		e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: append([]*unstructured.Unstructured{p}, more...)}, res, nil)
		for _, a := range Apps(e, nil, nil, nil, []*unstructured.Unstructured{j}, nil) {
			if a.Status.Short == "Retrying" {
				return a.Status.Why
			}
		}
		return ""
	}
	if why := status(job(map[string]any{"failed": int64(1)}, false), failedPod(20*time.Second)); !strings.Contains(why, "1 try failed (") || strings.Contains(why, "()") {
		t.Errorf("between tries: %q", why)
	}
	running := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "failjob-y", "namespace": "jobs", "uid": "p2",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "failjob", "uid": "j1", "controller": true}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "j", "state": map[string]any{"running": map[string]any{}}}}}}}
	if why := status(job(map[string]any{"failed": int64(1), "active": int64(1)}, false), failedPod(20*time.Second), running); !strings.Contains(why, "try 2 running, 1 try failed before") {
		t.Errorf("the next try runs: %q", why) // round 99: not "Running: healthy"
	}
	if why := status(job(map[string]any{"failed": int64(1)}, true), failedPod(20*time.Second)); why != "" {
		t.Errorf("paused: %q", why)
	}
	ending := map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "FailureTarget", "status": "True"}}}
	if why := status(job(ending, false), failedPod(20*time.Second)); why != "" {
		t.Errorf("already failing: %q", why)
	}
	if why := status(job(map[string]any{"failed": int64(1)}, false), failedPod(time.Hour)); why != "" {
		t.Errorf("quiet for an hour: %q", why)
	}
}

// Round 90: the Fix line named a menu item ("Its pods") the enter menu
// doesn't have; it is "Open app".
func TestJobFixNamesTheMenuItem(t *testing.T) {
	for _, s := range []string{"Job failed", "Retrying"} {
		if fix := appFix(&App{Status: Status{Short: s, Why: "container failed"}, Leftovers: 1}, false); !strings.Contains(fix, "enter → Open app") {
			t.Errorf("%s: %q", s, fix)
		}
	}
}

// Round 90: once a first rollout was marked stuck, an out-of-memory app's
// Fix said "fix the image or settings"; it keeps the pods' own advice.
func TestStuckFirstRolloutKeepsItsFix(t *testing.T) {
	a := &App{Status: Status{Short: "Rollout stuck", Why: "first version of deployment/oomer can't start: out of memory: limit 40Mi"},
		stuckBy: Status{Short: "Out of memory", Why: "limit 40Mi", Tone: ToneBad}, Restarts: 3}
	if fix := appFix(a, false); !strings.Contains(fix, "raise its memory limit") {
		t.Errorf("fix %q", fix)
	}
	a.stuckBy = Status{}
	if fix := appFix(a, false); !strings.Contains(fix, "fix the image or settings") {
		t.Errorf("no inner problem: %q", fix)
	}
}

// Round 91: a pod no node takes because of its node selector was told
// "often the cluster is adding a node; nodes are full".
func TestWaitingForANodeFixFollowsTheReason(t *testing.T) {
	fix := func(why string) string {
		return appFix(&App{Status: Status{Short: "Waiting for a node", Why: why}}, false)
	}
	if f := fix("no node fits: 12 nodes are reserved for other workloads (taints); 7 nodes don't match its node selector"); !strings.Contains(f, "fix its node selector") {
		t.Errorf("selector: %q", f)
	}
	if f := fix("no node fits: 19 nodes are reserved for other workloads (taints)"); !strings.Contains(f, "toleration") {
		t.Errorf("taints: %q", f)
	}
	if f := fix("no node fits: not enough free CPU on 3 nodes; 16 nodes are reserved for other workloads (taints)"); !strings.Contains(f, "nodes are full") {
		t.Errorf("space: %q", f)
	}
	if f := fix("no node fits: 19 nodes are reserved for other workloads (taints); 3 Insufficient nvidia.com/gpu"); !strings.Contains(f, "nodes are full") {
		t.Errorf("no GPU free: %q", f)
	}
	if f := fix("no node fits: 7 nodes don't match its node selector"); !strings.Contains(f, "being added, wait") {
		t.Errorf("a node may be coming: %q", f)
	}
}

// Round 91: a failed Job without labels made an app named after the
// namespace whose pod list was empty (its pod was named after the Job);
// the Job and its pods now make one app, and l on the pod works.
func TestUnlabeledFailedJobKeepsItsPods(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	now := time.Now()
	j := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "failjob", "namespace": "qa", "uid": "j1", "creationTimestamp": now.Add(-time.Minute).Format(time.RFC3339)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True",
			"reason": "BackoffLimitExceeded", "lastTransitionTime": now.Add(-30 * time.Second).Format(time.RFC3339)}}},
	}}
	jobPod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "failjob-x", "namespace": "qa", "uid": "p1", "labels": map[string]any{"job-name": "failjob"},
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "failjob", "uid": "j1", "controller": true}}},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}},
		"status": map[string]any{"phase": "Failed"}}}
	web := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "web-1", "namespace": "qa", "uid": "w1", "labels": map[string]any{"app": "web"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx"}}},
		"status":   map[string]any{"phase": "Running"}}}
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {jobPod, web}}, res, nil)
	var app *App
	for _, a := range Apps(e, []*unstructured.Unstructured{jobPod, web}, nil, nil, []*unstructured.Unstructured{j}, nil) {
		if a.Status.Short == "Job failed" {
			app = a
		}
	}
	if app == nil || app.Name != "failjob" {
		t.Fatalf("failed job's app: %+v", app)
	}
	if !AppPods(e, app)(jobPod) {
		t.Error("its pod isn't among its pods: Open app would be empty")
	}
}

// Round 92: a stuck rollout's crash-looping new pod was left out of READY
// ("3 of 3 ready" next to its 7 restarts); mid-rollout overlap still isn't.
func TestReadyCountsAStuckRolloutsNewPod(t *testing.T) {
	a := &App{Pods: 4, Ready: 3, wants: 3, Status: Status{Short: "Rollout stuck"}}
	if got := readyCell(a); got != "3/4" {
		t.Errorf("stuck: %s", got)
	}
	a.Status.Short = "Running"
	if got := readyCell(a); got != "3/3" {
		t.Errorf("mid-rollout: %s", got)
	}
}

// Round 94: a failed Job's box had no Error line (a crash loop's quotes
// its log); it now points at the Job's newest failed run.
func TestFailedJobPointsAtItsFailedRun(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	now := time.Now()
	j := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "migrate", "namespace": "qa", "uid": "jm", "creationTimestamp": now.Add(-time.Minute).Format(time.RFC3339)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True",
			"reason": "BackoffLimitExceeded", "lastTransitionTime": now.Add(-10 * time.Second).Format(time.RFC3339)}}},
	}}
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "migrate-x", "namespace": "qa", "uid": "pm",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "migrate", "uid": "jm", "controller": true}}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}},
		"status": map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "j",
			"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error",
				"finishedAt": now.Add(-15 * time.Second).Format(time.RFC3339)}}}}}}}
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil)
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, []*unstructured.Unstructured{j}, nil) {
		if a.Status.Short == "Job failed" && (a.CrashPod != "migrate-x" || a.CrashContainer != "j") {
			t.Errorf("crash pod %q container %q", a.CrashPod, a.CrashContainer)
		}
	}
}

// Review of round 94: the Error line quotes the run of the job the
// Problem line names, from its own container, and nothing when that job's
// newest run never started.
func TestFailedRunPicksTheRightRun(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	now := time.Now()
	j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "mig", "namespace": "qa", "uid": "jr"}}}
	pod := func(name string, made time.Time, phase string, statuses ...any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "mig", "uid": "jr", "controller": true}}},
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "main", "image": "x"}, map[string]any{"name": "proxy", "image": "y"}}},
			"status": map[string]any{"phase": phase, "containerStatuses": statuses}}}
	}
	term := func(name string, code int64, ago time.Duration) any {
		return map[string]any{"name": name, "state": map[string]any{"terminated": map[string]any{"exitCode": code,
			"finishedAt": now.Add(-ago).Format(time.RFC3339)}}}
	}
	failed := pod("mig-1", now.Add(-2*time.Minute), "Failed", term("main", 3, 90*time.Second), term("proxy", 137, 60*time.Second))
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {failed}}, res, nil)
	if _, p, c := failedRun(e, j); p != "mig-1" || c != "main" {
		t.Errorf("run %q container %q, want mig-1 main (not the sidecar killed after it)", p, c)
	}
	pending := pod("mig-2", now.Add(-30*time.Second), "Pending")
	e = enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {failed, pending}}, res, nil)
	if _, p, _ := failedRun(e, j); p != "" {
		t.Errorf("newest run never started, still quoted %q", p)
	}
}

// Round 95: a Job that can't pull its image was told "Its workloads, then
// i", which has nothing to act on (a Job's image can't be changed).
func TestJobImageFixWorks(t *testing.T) {
	a := &App{Status: Status{Short: "Can't pull image"}, kinds: map[string]bool{"Job": true}}
	if f := appFix(a, false); !strings.Contains(f, "create the job again") {
		t.Errorf("job: %q", f)
	}
	a.kinds = map[string]bool{"CronJob": true}
	if f := appFix(a, false); !strings.Contains(f, "cron job") {
		t.Errorf("cron job: %q", f)
	}
	a.kinds = map[string]bool{"Deployment": true}
	if f := appFix(a, false); !strings.HasPrefix(f, "i to fix") { // i works from the app itself (round 128)
		t.Errorf("deployment: %q", f)
	}
}

// Round 95: a job pod whose main container failed while its sidecar runs
// on read "Running: healthy"; it failed, and waits on the sidecar.
func TestFailedMainWithSidecarRunning(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jside-x", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}, {Name: "side"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Reason: "Error"}}},
			{Name: "side", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}},
		}}}
	st := Explain(e, p)
	if st.Short != "Failed" || !strings.Contains(st.Why, `container "main" stopped with an error (exit 2)`) || !strings.Contains(st.Why, "side still running") {
		t.Errorf("%+v", st)
	}
	// A service's pod restarts its containers: not this case.
	p.Spec.RestartPolicy = corev1.RestartPolicyAlways
	if st := Explain(e, p); st.Short == "Failed" {
		t.Errorf("restartPolicy Always: %+v", st)
	}
}

// Review of round 95: a run held open by its sidecar gets a way to end it;
// a cron job's image advice respects --readonly.
func TestHungRunAndReadOnlyCronFix(t *testing.T) {
	a := &App{Status: Status{Short: "Try failed", Why: `container "main" stopped with an error (exit 2); side still running and keeps the pod running`},
		kinds: map[string]bool{"Job": true}}
	if f := appFix(a, false); !strings.Contains(f, "ctrl-d on that pod ends this try") {
		t.Errorf("hung run: %q", f)
	}
	if f := appFix(a, true); !strings.Contains(f, "without --readonly") {
		t.Errorf("hung run, read-only: %q", f)
	}
	c := &App{Status: Status{Short: "Can't pull image"}, kinds: map[string]bool{"CronJob": true}}
	if f := appFix(c, true); strings.Contains(f, " e on it") || !strings.Contains(f, "without --readonly") {
		t.Errorf("cron job, read-only: %q", f)
	}
}

// Round 96: read-only, a first version that can't pull its image was
// offered a go-back that doesn't exist.
func TestReadOnlyImageFixRespectsNoEarlierVersion(t *testing.T) {
	a := &App{Status: Status{Short: "Can't pull image"}, kinds: map[string]bool{"Deployment": true}}
	if f := appFix(a, true); strings.Contains(f, "go back to the previous") || !strings.Contains(f, "no earlier version") {
		t.Errorf("first version: %q", f)
	}
	a.CanUndo = true
	if f := appFix(a, true); !strings.Contains(f, "go back to the previous version") {
		t.Errorf("with an earlier version: %q", f)
	}
}

// Round 96: in the jobs list a job that can't pull its image was told
// "i on its deployment"; pod Fix lines name what really runs the pod.
func TestPodFixNamesItsOwner(t *testing.T) {
	pull := Status{Short: "Can't pull image"}
	if f := podFixFor(pull, "Job"); strings.Contains(f, "deployment") || !strings.Contains(f, "create the job again") {
		t.Errorf("job: %q", f)
	}
	if f := podFixFor(pull, "CronJob"); !strings.Contains(f, "cron job") {
		t.Errorf("cron job: %q", f)
	}
	if f := podFixFor(Status{Short: "Out of memory"}, "StatefulSet"); !strings.Contains(f, "its stateful set") {
		t.Errorf("stateful set: %q", f)
	}
	if f := podFixFor(pull, "Deployment"); !strings.Contains(f, "its deployment") {
		t.Errorf("deployment: %q", f)
	}
}

// Round 96: before its rollout was marked stuck, a deployment whose new
// version can't pull its image was told "there is no earlier version to
// go back to", while u offered one.
func TestCanUndoBeforeStuck(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	now := time.Now()
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "stuckdep", "namespace": "qa", "uid": "d96", "labels": map[string]any{"app": "stuckdep"},
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
		"spec": map[string]any{"replicas": int64(1)}}}
	rs := func(name, rev string, made time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "stuckdep", "uid": "d96", "controller": true}}},
			// each version runs its own image, as real ones do (round 128: going back skips restarts of now)
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:" + rev}}}}}}}
	}
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "stuckdep-new-x", "namespace": "qa", "uid": "p96", "labels": map[string]any{"app": "stuckdep"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "stuckdep-new", "uid": "stuckdep-new", "controller": true}}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:nope"}}},
		"status": map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "c", "image": "nginx:nope",
			"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff", "message": "not found"}}}}}}}
	e := enrich.New(jobStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs("stuckdep-old", "1", now.Add(-time.Hour)), rs("stuckdep-new", "2", now)}, kube.Pods: {pod}}, res, nil)
	for _, a := range Apps(e, []*unstructured.Unstructured{pod}, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if strings.HasPrefix(a.Status.Short, "Can't pull image") && (!a.CanUndo || strings.Contains(appFix(a, false), "no earlier version")) {
			t.Errorf("CanUndo %v, fix %q", a.CanUndo, appFix(a, false))
		}
	}
}

// Round 96: a cron job fixed since kept "Can't pull image" and the same
// advice for its old runs stuck on the old image; a mirror's rewritten
// address is still the same image.
func TestStaleCronRuns(t *testing.T) {
	if imageKey("111122223333.dkr.ecr.ap-south-1.amazonaws.com/docker-hub/library/busybox:1.36") != imageKey("busybox:1.36") {
		t.Error("a mirror's address counted as another image")
	}
	a := &App{Status: Status{Short: "Can't pull image"}, kinds: map[string]bool{"CronJob": true}, staleRuns: true}
	if f := appFix(a, false); !strings.Contains(f, "runs another image now") {
		t.Errorf("fix %q", f)
	}
}

// Review of round 96: static pods, a job's memory, and "no tag is latest".
func TestPodFixMoreOwners(t *testing.T) {
	if f := podFixFor(Status{Short: "Can't pull image"}, "Node"); !strings.Contains(f, "manifest file") {
		t.Errorf("static pod: %q", f)
	}
	if f := podFixFor(Status{Short: "Out of memory"}, "Job"); !strings.Contains(f, "where the job is defined") {
		t.Errorf("job memory: %q", f)
	}
	if imageKey("busybox") != imageKey("busybox:latest") {
		t.Error("no tag should be latest")
	}
}

// Round 97: an app led with a memory kill from 47m ago in a pod running
// again, while another pod crash-looped now; its Error line and p were
// that other pod's. The crash now is the headline, and its pod the
// Error line's.
func TestHeadlinePrefersAProblemNow(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	old := metav1.NewTime(time.Now().Add(-47 * time.Minute))
	recent := metav1.NewTime(time.Now().Add(-30 * time.Second))
	pod := func(name string, cs corev1.ContainerStatus, phase corev1.PodPhase) *unstructured.Unstructured {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{"app": "be"}},
			Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: cs.Name, Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")}}}}},
			Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{cs}}}
		obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
		u := &unstructured.Unstructured{Object: obj}
		u.SetKind("Pod")
		return u
	}
	celery := pod("be-celery", corev1.ContainerStatus{Name: "celery", Ready: true, RestartCount: 4,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: old}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, FinishedAt: old}}}, corev1.PodRunning)
	consumer := pod("be-consumer", corev1.ContainerStatus{Name: "consumer", RestartCount: 171,
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: recent}}}, corev1.PodRunning)
	for _, a := range Apps(e, []*unstructured.Unstructured{celery, consumer}, nil, nil, nil, nil) {
		if a.Name != "be" {
			continue
		}
		if !strings.HasPrefix(a.Status.Short, "Crashing") || a.CrashPod != "be-consumer" {
			t.Errorf("headline %+v, error line from %q", a.Status, a.CrashPod)
		}
	}
}

// Round 97 (tester A): a job pod killed for memory, the jobs list's
// states, a bare pod's app, and a cron job whose runs can't start.
func TestRound97JobsAndBarePods(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "job-oom-x", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "c"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{
			{Name: "c", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}}}
	st := Explain(e, p)
	if !strings.Contains(st.Why, `container "c" ran out of memory and was killed`) {
		t.Errorf("wording %+v", st)
	}
	if f := podFixFor(st, "Job"); !strings.Contains(f, "memory limit where the job is defined") {
		t.Errorf("job memory fix %q", f)
	}
	job := func(status map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"status": status}}
	}
	if s, _ := jobStatus(nil, job(map[string]any{"failed": int64(2), "active": int64(1)}), false); s != "Retrying (2 tries failed, another running)" {
		t.Errorf("a try running: %q", s)
	}
	if s, _ := jobStatus(nil, job(map[string]any{"failed": int64(2)}), false); !strings.HasPrefix(s, "Retrying (2 tries failed)") {
		t.Errorf("between tries: %q", s)
	}
	if s, tone := jobStatus(nil, job(map[string]any{"conditions": []any{map[string]any{"type": "FailureTarget", "status": "True"}}}), false); !strings.HasPrefix(s, "Failed") || tone != ToneBad {
		t.Errorf("failing: %q", s)
	}
	bare := &App{Status: Status{Short: "Can't pull image"}, Pods: 1, barePods: 1}
	if f := appFix(bare, false); !strings.Contains(f, "where its pod is defined") {
		t.Errorf("bare pod app: %q", f)
	}
	operatorMade := &App{Status: Status{Short: "Can't pull image"}, Pods: 1} // owned by something kubed doesn't know: not bare
	if f := appFix(operatorMade, false); strings.Contains(f, "nothing makes it again") {
		t.Errorf("operator-made pod called bare: %q", f)
	}
	if f := podFixFor(Status{Short: "Failed", Why: `container "c" never started (its command or a file it needs is missing or can't run)`}, "Job"); strings.Contains(f, "l shows its logs") {
		t.Errorf("never started: %q", f)
	}
}

// Review of round 97: a crash loop between crashes (ready for a moment)
// still beats a failed job's pod for the headline, and the Error line
// names the pod the headline does.
func TestHeadlineCrashLoopBetweenCrashes(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	recent := metav1.NewTime(time.Now().Add(-40 * time.Second))
	mk := func(p *corev1.Pod) *unstructured.Unstructured {
		obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
		u := &unstructured.Unstructured{Object: obj}
		u.SetKind("Pod")
		return u
	}
	crash := mk(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a-crash", Namespace: "ns", Labels: map[string]string{"app": "mix"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "c", Ready: true, RestartCount: 5,
				State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: recent}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, FinishedAt: recent}}}}}})
	failed := mk(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b-failed", Namespace: "ns", Labels: map[string]string{"app": "mix"}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "j"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "j",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 2, FinishedAt: recent}}}}}})
	for _, a := range Apps(e, []*unstructured.Unstructured{crash, failed}, nil, nil, nil, nil) {
		if a.Name == "mix" && a.CrashPod != "" && !strings.Contains(a.Status.Why, a.CrashPod) && a.Status.Short == "Failed" && a.CrashPod != "b-failed" {
			t.Errorf("headline %+v but Error line from %q", a.Status, a.CrashPod)
		}
	}
}

// Round 98: job memory advice in the app box too; a cron job's failed run
// gets the cron job's advice in its pod box.
func TestJobMemoryAndCronRunAdvice(t *testing.T) {
	a := &App{Status: Status{Short: "Job failed", Why: `job oomjob failed: container "main" ran out of memory and was killed`}, kinds: map[string]bool{"Job": true}}
	if f := appFix(a, false); !strings.Contains(f, "raise its memory limit where the job is defined") {
		t.Errorf("job app memory: %q", f)
	}
	if f := podFixFor(Status{Short: "Failed", Why: `container "main" stopped with an error (exit 2)`}, "CronJob"); !strings.Contains(f, "next run is on schedule") {
		t.Errorf("cron run pod: %q", f)
	}
	if f := podFixFor(Status{Short: "Failed", Why: `container "main" ran out of memory and was killed`}, "Job"); strings.Contains(f, "cron job") {
		t.Errorf("plain job memory mentions cron jobs: %q", f)
	}
}

// Round 99: a native sidecar (an init container that keeps running) holds
// a job's pod open after its main container failed: Failed, not running.
func TestFailedMainWithNativeSidecar(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	always := corev1.ContainerRestartPolicyAlways
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jobside-x", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}},
			InitContainers: []corev1.Container{{Name: "side", RestartPolicy: &always}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning,
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "side", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	if st := Explain(e, p); st.Short != "Failed" || !strings.Contains(st.Why, "side still running") {
		t.Errorf("%+v", st)
	}
}

// Round 99: with --readonly the pod and job boxes offer no change keys.
func TestReadOnlyPodFix(t *testing.T) {
	ReadOnly = true
	defer func() { ReadOnly = false }()
	if f := podFixFor(Status{Short: "Out of memory"}, "CronJob"); strings.Contains(f, "e on it") || !strings.Contains(f, "without --readonly") {
		t.Errorf("cron memory: %q", f)
	}
	if f := podFixFor(Status{Short: "Failed", Why: "x; side still running and keeps the pod running"}, "Job"); strings.Contains(f, "ctrl-d") {
		t.Errorf("sidecar: %q", f)
	}
}

// Review of round 98: memory advice doesn't hide a sidecar holding the run
// open, and respects ArgoCD and read-only.
func TestMemoryAdviceOrderAndPlaces(t *testing.T) {
	why := `container "main" ran out of memory and was killed; side still running and keeps the pod running`
	a := &App{Status: Status{Short: "Try failed", Why: why}, kinds: map[string]bool{"Job": true}}
	if f := appFix(a, false); !strings.Contains(f, "ctrl-d on that pod ends this try") {
		t.Errorf("held open, app: %q", f)
	}
	if f := podFixFor(Status{Short: "Failed", Why: why}, "Job"); !strings.Contains(f, "ctrl-d on it ends this try") {
		t.Errorf("held open, pod: %q", f)
	}
	c := &App{Status: Status{Short: "Job failed", Why: `container "main" ran out of memory and was killed`}, kinds: map[string]bool{"CronJob": true}}
	if f := appFix(c, true); strings.Contains(f, "gC, then e") {
		t.Errorf("read-only cron memory: %q", f)
	}
}

// Review of round 99: a native sidecar stopping after the work is done (or
// between restarts) isn't the run failing.
func TestSidecarStoppingIsNotFailed(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	always := corev1.ContainerRestartPolicyAlways
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jobside-y", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}},
			InitContainers: []corev1.Container{{Name: "side1", RestartPolicy: &always}, {Name: "side2", RestartPolicy: &always}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}}}},
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "side1", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 143, Reason: "Error"}}},
				{Name: "side2", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	if st := Explain(e, p); st.Short == "Failed" {
		t.Errorf("%+v", st)
	}
}

// Review of round 99: a job running pods side by side, or one that has
// had tries succeed, has no single try number.
func TestParallelJobRetryHasNoTryNumber(t *testing.T) {
	j := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"completions": int64(5)}, "status": map[string]any{"succeeded": int64(3)}}}
	if oneAtATime(j) {
		t.Error("completions 5")
	}
	j = &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}, "status": map[string]any{"failed": int64(1)}}}
	if !oneAtATime(j) {
		t.Error("plain job")
	}
}

// Review of round 99: --readonly words every change key the boxes offer.
func TestReadOnlyWordsAllChangeKeys(t *testing.T) {
	ReadOnly = true
	defer func() { ReadOnly = false }()
	for _, c := range []struct {
		st    Status
		owner string
	}{{Status{Short: "Can't pull image"}, "Deployment"}, {Status{Short: "Can't pull image"}, "StatefulSet"}, {Status{Short: "Failed", Why: "exit 1"}, "CronJob"}} {
		if f := podFixFor(c.st, c.owner); strings.Contains(f, "(i on") || strings.Contains(f, "t on its") {
			t.Errorf("%s %s: %q", c.owner, c.st.Short, f)
		}
	}
	for _, f := range []string{"fix it (e), then enter opens its runs: ctrl-d on the stuck ones", "it runs another image now; the stuck runs still wait on the old one: enter opens its runs, ctrl-d on them",
		"u to go back to the previous version, or i to fix the image name or tag"} {
		if g := readOnlyFix(f); strings.Contains(g, "ctrl-d") || strings.Contains(g, "(e)") || strings.Contains(g, "u to") {
			t.Errorf("%q", g)
		}
	}
}

// Review of round 99: a headline about something else (a failed job)
// doesn't keep pointing p at the pod an earlier headline named.
func TestHeadPodClearedByJobHeadline(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "r99job", "namespace": "r99ns", "labels": map[string]any{"app": "r99"}, "uid": "r99j", "creationTimestamp": time.Now().Add(-time.Hour).Format(time.RFC3339)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded",
			"lastTransitionTime": time.Now().Add(-time.Minute).Format(time.RFC3339)}}}}}
	start := time.Now().Add(-time.Hour).Format(time.RFC3339)
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "r99web", "namespace": "r99ns", "labels": map[string]any{"app": "r99"}, "uid": "r99p", "creationTimestamp": start},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "web", "ready": false, "restartCount": int64(1),
			"state":     map[string]any{"running": map[string]any{"startedAt": start}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "finishedAt": start}}}}}}}
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil)
	found := false
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, []*unstructured.Unstructured{j}, nil) {
		if a.Status.Short == "Job failed" {
			found = true
			if a.HeadPod != "" {
				t.Errorf("headline %q, p opens %q", a.Status.Why, a.HeadPod)
			}
		}
	}
	if !found {
		t.Error("no Job failed app")
	}
}

// Review of round 99: when the crash the box quotes is another pod's than
// the headline's, the box says whose.
func TestCrashOfAnotherPodIsNamed(t *testing.T) {
	a := &App{Name: "r99", Status: Status{"Out of memory", "container web ran out of memory", ToneBad},
		HeadNS: "ns", HeadPod: "a", CrashNS: "ns", CrashPod: "b", CrashContainer: "web"}
	look := Lookups{Crash: func(*App) (string, bool) { return "boom", true }}
	if got := strings.Join(AppLines(a, look), "\n"); !strings.Contains(got, "pod b crashed: boom") {
		t.Errorf("%s", got)
	}
	a.HeadPod = "b"
	if got := strings.Join(AppLines(a, look), "\n"); strings.Contains(got, "pod b crashed") || !strings.Contains(got, "boom") {
		t.Errorf("same pod: %s", got)
	}
}

// Review of round 99: a native sidecar still running just after the work
// failed is being stopped by Kubernetes: the run is only ending.
func TestNativeSidecarJustAfterFailure(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	always := corev1.ContainerRestartPolicyAlways
	mk := func(ago time.Duration) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jobside-z", Namespace: "qa"},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}},
				InitContainers: []corev1.Container{{Name: "side", RestartPolicy: &always}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(time.Now().Add(-ago))}}}},
				InitContainerStatuses: []corev1.ContainerStatus{{Name: "side", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	}
	if st := Explain(e, mk(5*time.Second)); strings.Contains(st.Why, "keeps the pod running") {
		t.Errorf("5s after: %+v", st)
	}
	if st := Explain(e, mk(2*time.Minute)); !strings.Contains(st.Why, "side still running") {
		t.Errorf("2m after: %+v", st)
	}
}

// Review of round 99: under a headline that isn't a crash, p opens the
// crash the box quotes, not the headline pod's old restart.
func TestPrevLogsOfHeadOnlyForCrashes(t *testing.T) {
	a := &App{Status: Status{Short: "Bad config"}, HeadPod: "a", CrashPod: "b"}
	if a.PrevLogsOfHead() {
		t.Error("bad config")
	}
	a.Status.Short = "Out of memory"
	if !a.PrevLogsOfHead() {
		t.Error("out of memory")
	}
	a.Status.Short, a.stuckBy = "Rollout stuck", Status{Short: "Not ready"}
	if a.PrevLogsOfHead() {
		t.Error("stuck, not ready")
	}
	a.stuckBy.Short = "Crashing"
	if !a.PrevLogsOfHead() {
		t.Error("stuck, crashing")
	}
}

// Review of round 99: Kubernetes stops native sidecars once all the work
// has ended, a container that succeeded too: the wait counts from then.
func TestNativeSidecarWaitsForAllWork(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	always := corev1.ContainerRestartPolicyAlways
	ended := func(code int32, ago time.Duration) corev1.ContainerState {
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, FinishedAt: metav1.NewTime(time.Now().Add(-ago))}}
	}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jobside-w", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "a"}, {Name: "b"}},
			InitContainers: []corev1.Container{{Name: "side", RestartPolicy: &always}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning,
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "a", State: ended(1, 10*time.Minute)}, {Name: "b", State: ended(0, 2*time.Second)}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "side", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	if st := Explain(e, p); strings.Contains(st.Why, "keeps the pod running") {
		t.Errorf("%+v", st)
	}
}

// Round 100: a native sidecar being stopped after the work failed: the
// run failed (not "healthy"), and nothing to end by hand.
func TestNativeSidecarStoppingSaysFailed(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	always := corev1.ContainerRestartPolicyAlways
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jobside-v", Namespace: "qa"},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}},
			InitContainers: []corev1.Container{{Name: "side", RestartPolicy: &always}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(time.Now().Add(-4 * time.Second))}}}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "side", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	st := Explain(e, p)
	if st.Short != "Failed" || !strings.Contains(st.Why, "Kubernetes is stopping side") {
		t.Errorf("%+v", st)
	}
	if f := podFixFor(st, "Job"); strings.Contains(f, "ctrl-d") {
		t.Errorf("fix: %q", f)
	}
}

// Round 100: read-only app box of a failed cron job offers no t, and the
// stuck reason says "can't start" once.
func TestRound100Wording(t *testing.T) {
	a := &App{Status: Status{Short: "Job failed"}, kinds: map[string]bool{"CronJob": true}}
	if f := appFix(a, true); strings.Contains(f, "t on its") {
		t.Errorf("read-only: %q", f)
	}
	if f := appFix(a, false); !strings.Contains(f, "t on its cron job") {
		t.Errorf("read-write: %q", f)
	}
	ReadOnly = true
	defer func() { ReadOnly = false }()
	if f := podFixFor(Status{Short: "Failed", Why: "x; side still running and keeps the pod running"}, "Job"); strings.Contains(f, ") (") {
		t.Errorf("brackets: %q", f)
	}
}

// Round 100: a stuck rollout whose pods can't start says so once.
func TestCantStartWhySaysItOnce(t *testing.T) {
	if w := cantStartWhy(Status{Short: "Can't start", Why: "it never started"}); w != "can't start: it never started" {
		t.Errorf("%q", w)
	}
	if w := cantStartWhy(Status{Short: "Crashing", Why: "exit 1"}); w != "can't start: crashing: exit 1" {
		t.Errorf("%q", w)
	}
}

// Round 100: a retrying job's box says what failed and what happens next.
func TestRetryingJobBox(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "r100job", "namespace": "r100", "uid": "r100j"},
		"status":   map[string]any{"failed": int64(1), "active": int64(1)}}}
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "r100job-x", "namespace": "r100", "uid": "r100p",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r100job", "uid": "r100j", "controller": true}}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}},
		"status": map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "j",
			"state": map[string]any{"terminated": map[string]any{"exitCode": int64(2), "reason": "Error",
				"finishedAt": time.Now().Add(-20 * time.Second).Format(time.RFC3339)}}}}}}}
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil)
	got := strings.Join(workloadProblem(e, j), "\n")
	if !strings.Contains(got, "1 try failed so far") || !strings.Contains(got, "Fix") {
		t.Errorf("%s", got)
	}
}

// Round 100: the NODEPOOL column shows the pool's real name, which
// kubectl get nodepool takes.
func TestNodePoolColumnRealName(t *testing.T) {
	n := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{"name": "ip-10-0-0-1.ec2.internal", "labels": map[string]any{"karpenter.sh/nodepool": "nodepool-app-arm64"}}}}
	e := enrich.New(benchStore{}, nil, nil)
	cells, _ := registry[kube.Nodes.GroupResource()].Row(e, n)
	if cells[2] != "nodepool-app-arm64" {
		t.Errorf("%q", cells[2])
	}
}

// Review of round 100: a try that failed while its sidecar stops reads
// Retrying when the job has tries left, Job failed on its last.
func TestFailedTryWithTriesLeft(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	status := func(limit int64, more ...*unstructured.Unstructured) Status {
		j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "r100try", "namespace": "r100t", "uid": "r100tj", "labels": map[string]any{"app": "r100try"}},
			"spec":     map[string]any{"backoffLimit": limit, "suspend": limit == 5},
			"status":   map[string]any{"active": int64(1)}}}
		ended := time.Now().Add(-3 * time.Second).Format(time.RFC3339)
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "r100try-x", "namespace": "r100t", "uid": "r100tp", "labels": map[string]any{"app": "r100try"},
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r100try", "uid": "r100tj", "controller": true}}},
			"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "main", "image": "busybox"}},
				"initContainers": []any{map[string]any{"name": "side", "image": "busybox", "restartPolicy": "Always"}}},
			"status": map[string]any{"phase": "Running",
				"containerStatuses":     []any{map[string]any{"name": "main", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ended}}}},
				"initContainerStatuses": []any{map[string]any{"name": "side", "state": map[string]any{"running": map[string]any{"startedAt": ended}}}}}}}
		jobs := append([]*unstructured.Unstructured{j}, more...)
		e := enrich.New(jobStore{kube.Jobs: jobs, kube.Pods: {p}}, res, nil)
		for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, jobs, nil) {
			if a.Name == "r100try" {
				return a.Status
			}
		}
		return Status{}
	}
	if st := status(6); st.Short != "Retrying" || !strings.Contains(st.Why, "then Kubernetes tries again") {
		t.Errorf("tries left: %+v", st)
	}
	if st := status(0); st.Short != "Job failed" {
		t.Errorf("last try: %+v", st)
	}
	if st := status(5); st.Short != "Job failed" {
		t.Errorf("paused, starts no try: %+v", st)
	}
	other := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "r100other", "namespace": "r100t", "uid": "r100to", "labels": map[string]any{"app": "r100try"},
			"creationTimestamp": time.Now().Add(-time.Hour).Format(time.RFC3339)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded",
			"lastTransitionTime": time.Now().Add(-time.Hour).Format(time.RFC3339)}}}}}
	if st := status(6, other); st.Tone != ToneBad {
		t.Errorf("another job failed: %+v", st)
	}
}

// Review of round 100: "Bad config" already says it can't start.
func TestCantStartWhyBadConfig(t *testing.T) {
	if w := cantStartWhy(Status{Short: "Bad config", Why: `c can't start: secret "x" is missing`}); strings.Count(w, "can't start") != 1 {
		t.Errorf("%q", w)
	}
}

// Round 101: in the moment after a try's pod has failed but before the
// job counts it, the app says Retrying (or Job failed on the last try),
// never healthy.
func TestFailedPodNotCountedYet(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	status := func(limit int64) Status {
		j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "r101gap", "namespace": "r101", "uid": "r101j", "labels": map[string]any{"app": "r101gap"}},
			"spec":     map[string]any{"backoffLimit": limit},
			"status":   map[string]any{"active": int64(1)}}} // the pod has ended, the job hasn't counted it
		ended := time.Now().Add(-time.Second).Format(time.RFC3339)
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "r101gap-x", "namespace": "r101", "uid": "r101p", "labels": map[string]any{"app": "r101gap"},
				"finalizers":      []any{"batch.kubernetes.io/job-tracking"}, // not counted yet
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r101gap", "uid": "r101j", "controller": true}}},
			"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "main", "image": "busybox"}}},
			"status": map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "main",
				"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ended}}}}}}}
		e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil)
		for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, []*unstructured.Unstructured{j}, nil) {
			if a.Name == "r101gap" {
				return a.Status
			}
		}
		return Status{}
	}
	if st := status(3); st.Short != "Retrying" || !strings.Contains(st.Why, "1 try failed") {
		t.Errorf("tries left: %+v", st)
	}
	if st := status(0); st.Short != "Job failed" {
		t.Errorf("last try: %+v", st)
	}
}

// Round 101: an app that only runs jobs has no workload to edit: its Fix
// says where the change goes.
func TestJobOnlyFixWhere(t *testing.T) {
	why := `container "main" it never started: /no/such isn't in the image`
	job := &App{Status: Status{Short: "Job failed", Why: why}, kinds: map[string]bool{"Job": true}}
	if f := appFix(job, false); strings.Contains(f, "Its workloads") || !strings.Contains(f, "create the job again") {
		t.Errorf("job: %q", f)
	}
	cron := &App{Status: Status{Short: "Job failed", Why: why}, kinds: map[string]bool{"Job": true, "CronJob": true}}
	if f := appFix(cron, false); !strings.Contains(f, "in its cron job (gC, then e on it)") {
		t.Errorf("cron job: %q", f)
	}
	dep := &App{Status: Status{Short: "Can't start", Why: why}, kinds: map[string]bool{"Deployment": true, "Job": true}}
	if f := appFix(dep, false); !strings.Contains(f, "Its workloads") {
		t.Errorf("deployment: %q", f)
	}
}

// Review of round 101: only failures the job will count make it Retrying
// (a pod failure policy can ignore one: no tracking finalizer left), and
// the jobs list and the job's box agree with the app in the moment before
// the job counts a failed pod.
func TestUncountedFailuresAgree(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	mk := func(limit int64, finalizer bool, code int64) (*enrich.Enricher, *unstructured.Unstructured, *unstructured.Unstructured) {
		j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "r101u", "namespace": "r101u", "uid": "r101uj", "labels": map[string]any{"app": "r101u"}},
			"spec":     map[string]any{"backoffLimit": limit},
			"status":   map[string]any{"active": int64(0)}}}
		if !finalizer { // a policy that ignores some failures (a node taken away)
			j.Object["spec"].(map[string]any)["podFailurePolicy"] = map[string]any{"rules": []any{}}
		}
		meta := map[string]any{"name": "r101u-x", "namespace": "r101u", "uid": "r101up", "labels": map[string]any{"app": "r101u"},
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r101u", "uid": "r101uj", "controller": true}}}
		if finalizer {
			meta["finalizers"] = []any{"batch.kubernetes.io/job-tracking"}
		}
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": meta,
			"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "main", "image": "busybox"}}},
			"status": map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "main",
				"state": map[string]any{"terminated": map[string]any{"exitCode": code, "reason": "Error", "finishedAt": time.Now().Add(-time.Second).Format(time.RFC3339)}}}}}}}
		return enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil), j, p
	}
	app := func(e *enrich.Enricher, j, p *unstructured.Unstructured) Status {
		for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, []*unstructured.Unstructured{j}, nil) {
			if a.Name == "r101u" {
				return a.Status
			}
		}
		return Status{}
	}
	if e, j, p := mk(3, false, 1); app(e, j, p).Short == "Retrying" {
		t.Errorf("ignored by its policy: %+v", app(e, j, p))
	}
	e, j, _ := mk(3, true, 1)
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Retrying (1 try failed") {
		t.Errorf("gj, tries left: %q", cells[1])
	}
	e, j, p := mk(0, true, 1)
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Failed") {
		t.Errorf("gj, last try: %q", cells[1])
	}
	if box := strings.Join(workloadProblem(e, j), "\n"); !strings.Contains(box, "this run failed") {
		t.Errorf("box, last try: %s", box)
	}
	if st := app(e, j, p); st.Short != "Job failed" {
		t.Errorf("app, last try: %+v", st)
	}
	if e, j, p := mk(0, true, 0); strings.Contains(app(e, j, p).Why, "h ago") {
		t.Errorf("no error code: %+v", app(e, j, p))
	}
}

// Review of round 101: a pod whose setup step failed says which, not an
// empty reason (the app never ran, so its containers say nothing).
func TestFailedSetupStepNamed(t *testing.T) {
	p := &corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "migrate"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed,
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "migrate", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}}}}}
	if w := lastExit(p); w != `setup step "migrate" stopped with an error (exit 1), so the app never ran` {
		t.Errorf("%q", w)
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}
	if w := lastExit(p); w != `container "main" ran out of memory and was killed` {
		t.Errorf("main first: %q", w)
	}
}

// r102 builds a job and its pods for the round 102 tests.
func r102(job map[string]any, pods ...map[string]any) (*enrich.Enricher, *unstructured.Unstructured, []*unstructured.Unstructured) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "r102", "namespace": "r102", "uid": "r102j", "labels": map[string]any{"app": "r102"}}}}
	for k, v := range job {
		j.Object[k] = v
	}
	var ps []*unstructured.Unstructured
	for i, st := range pods {
		ps = append(ps, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": fmt.Sprintf("r102-%d", i), "namespace": "r102", "uid": fmt.Sprintf("r102p%d", i), "labels": map[string]any{"app": "r102"},
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r102", "uid": "r102j", "controller": true}}},
			"spec":   map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "main", "image": "busybox"}}},
			"status": st}})
	}
	return enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: ps}, res, nil), j, ps
}

func r102failed(ago time.Duration, code int64, reason string) map[string]any {
	return map[string]any{"phase": "Failed", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"terminated": map[string]any{"exitCode": code, "reason": reason, "finishedAt": time.Now().Add(-ago).Format(time.RFC3339)}}}}}
}

func r102app(e *enrich.Enricher, j *unstructured.Unstructured, ps []*unstructured.Unstructured) *App {
	for _, a := range Apps(e, ps, nil, nil, []*unstructured.Unstructured{j}, nil) {
		if a.Name == "r102" {
			return a
		}
	}
	return &App{}
}

// Round 102: kubed can see a job's own count seconds behind its pods. The
// pods say how many tries failed and whether one runs: never "Running",
// never "try N running" with none running, never Retrying after the last.
func TestJobBehindItsPods(t *testing.T) {
	// The try failed, the job still says active 1, failed 0.
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3)}, "status": map[string]any{"active": int64(1)}}, r102failed(2*time.Second, 1, "Error"))
	if a := r102app(e, j, ps); a.Status.Short != "Retrying" || strings.Contains(a.Status.Why, "running") {
		t.Errorf("app: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); cells[1] != "Retrying (1 try failed)" {
		t.Errorf("gj: %q", cells[1])
	}
	// The last try failed, the job still says failed 1, active 1.
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(1)}, "status": map[string]any{"active": int64(1), "failed": int64(1)}},
		r102failed(time.Minute, 1, "Error"), r102failed(2*time.Second, 1, "Error"))
	if a := r102app(e, j, ps); a.Status.Short != "Job failed" {
		t.Errorf("app, last try: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Failed") {
		t.Errorf("gj, last try: %q", cells[1])
	}
	// A pod whose work ended (its sidecar stopping) isn't a try running.
	e, j, _ = r102(nil, map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1)}}}}})
	if n := liveTries(e, j); n != 0 {
		t.Errorf("live: %d", n)
	}
}

// Round 102: a failed job's time is when its last try ended, so it doesn't
// jump from "4s ago" to "0s ago" when the job marks it.
func TestJobFailedTimeSteady(t *testing.T) {
	now := time.Now().Format(time.RFC3339)
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "lastTransitionTime": now}}}},
		r102failed(10*time.Second, 1, "Error"))
	if a := r102app(e, j, ps); !strings.Contains(a.Status.Why, "failed 10s ago") {
		t.Errorf("%+v", a.Status)
	}
}

// Round 102: a job whose command is missing, its pods all finished: the
// app and the job box say where the fix goes (not Its workloads).
func TestJobNeverStartedFix(t *testing.T) {
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "lastTransitionTime": time.Now().Format(time.RFC3339)}}}},
		r102failed(10*time.Second, 128, "StartError"))
	a := r102app(e, j, ps)
	if f := appFix(a, false); strings.Contains(f, "Its workloads") || !strings.Contains(f, "create the job again") {
		t.Errorf("app %+v: %q", a.Status, f)
	}
	if a.kinds["Job"] { // its pods are leftovers: kinds (which READY reads) stay the live pods'
		t.Errorf("kinds: %v", a.kinds)
	}
	if f := jobNeverStartedFix(e, j); !strings.Contains(f, "create the job again") || strings.Contains(f, "in Git") {
		t.Errorf("box: %q", f)
	}
}

// Round 102: the memory reading is the whole pod's: it is said only when
// that is the container's own (one container, running).
func TestOutOfMemoryReadingIsTheContainers(t *testing.T) {
	e := enrich.New(benchStore{}, nil, nil)
	e.SetUsage(fixedUsage{})
	cs := func(name string, running bool) corev1.ContainerStatus {
		c := corev1.ContainerStatus{Name: name, RestartCount: 3,
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.Now()}}}
		if running {
			c.State.Running = &corev1.ContainerStateRunning{}
		} else {
			c.State.Waiting = &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}
		}
		return c
	}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "oom", Namespace: "qa"},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}, {Name: "tail"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{cs("app", false), {Name: "tail", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	if st := crashing(e, p, p.Status.ContainerStatuses[0]); strings.Contains(st.Why, "using") {
		t.Errorf("two containers: %q", st.Why)
	}
	p.Spec.Containers, p.Status.ContainerStatuses = p.Spec.Containers[:1], []corev1.ContainerStatus{cs("app", true)}
	if st := crashing(e, p, p.Status.ContainerStatuses[0]); !strings.Contains(st.Why, "using") {
		t.Errorf("one container, running: %q", st.Why)
	}
}

type fixedUsage struct{}

func (fixedUsage) Available() bool { return true }
func (fixedUsage) Pod(string, string) (kube.Usage, bool) {
	return kube.Usage{MemBytes: 100 << 20}, true
}
func (fixedUsage) Node(string) (kube.Usage, bool) { return kube.Usage{}, false }
func (fixedUsage) Peak(string, string) (kube.Usage, time.Time, bool) {
	return kube.Usage{}, time.Time{}, false
}

// Round 102: two containers crash looping in one app: the headline names
// the one that crashed more, and says the other crashes too.
func TestTwoCrashersBothNamed(t *testing.T) {
	ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
	pod := func(name, container string, restarts int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r102c", "uid": name, "labels": map[string]any{"app": "r102c"}, "creationTimestamp": ago},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": container, "image": "busybox"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": container, "restartCount": restarts, "ready": false,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	pods := []*unstructured.Unstructured{pod("a-1", "consumer", 6), pod("b-1", "scheduler", 22)}
	e := enrich.New(jobStore{kube.Pods: pods}, res, nil)
	for _, a := range Apps(e, pods, nil, nil, nil, nil) {
		if a.Name != "r102c" {
			continue
		}
		if !strings.Contains(a.Status.Why, `"scheduler"`) || !strings.Contains(a.Status.Why, "consumer crashing too") {
			t.Errorf("%+v", a.Status)
		}
		if a.HeadPod != "b-1" || a.CrashPod != "b-1" {
			t.Errorf("head %q crash %q", a.HeadPod, a.CrashPod)
		}
		if w := AppWhy(a, Lookups{}); !strings.Contains(w, "consumer crashing too") { // what the WHY cell shows
			t.Errorf("WHY cell: %q", w)
		}
		if lines := strings.Join(AppLines(a, Lookups{}), "\n"); !strings.Contains(lines, "consumer crashing too") {
			t.Errorf("box: %s", lines)
		}
		return
	}
	t.Error("no app")
}

// Review of round 102: the headline's crash container is the one its
// status names (not a sidecar that restarted once long ago), it is never
// "crashing too" about itself, and replicas of one container don't swap
// the headline with each crash.
func TestCrashContainerIsTheNamedOne(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour).Format(time.RFC3339)
	ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
	cs := func(name string, restarts int64, crashing bool, at string) any {
		m := map[string]any{"name": name, "restartCount": restarts, "ready": !crashing,
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": at}}}
		if crashing {
			m["state"] = map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}}
		} else {
			m["state"] = map[string]any{"running": map[string]any{"startedAt": old}}
		}
		return m
	}
	pod := func(name string, statuses ...any) *unstructured.Unstructured {
		var cont []any
		for _, s := range statuses {
			cont = append(cont, map[string]any{"name": s.(map[string]any)["name"], "image": "busybox"})
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r102d", "uid": name, "labels": map[string]any{"app": "r102d"}, "creationTimestamp": old},
			"spec":     map[string]any{"containers": cont},
			"status":   map[string]any{"phase": "Running", "containerStatuses": statuses}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	app := func(pods ...*unstructured.Unstructured) *App {
		e := enrich.New(jobStore{kube.Pods: pods}, res, nil)
		for _, a := range Apps(e, pods, nil, nil, nil, nil) {
			if a.Name == "r102d" {
				return a
			}
		}
		return &App{}
	}
	a := app(pod("one", cs("proxy", 1, false, old), cs("api", 9, true, ago)))
	if a.CrashContainer != "api" || strings.Contains(a.Status.Why, "crashing too") {
		t.Errorf("crash %q: %q", a.CrashContainer, a.Status.Why)
	}
	// Round 116: copies of one container: the one that crashed more; ties
	// keep the first (no swapping with each crash).
	a = app(pod("r-1", cs("api", 5, true, ago)), pod("r-2", cs("api", 7, true, ago)))
	if a.HeadPod != "r-2" || !strings.Contains(a.Status.Why, "7 restarts") || strings.Contains(a.Status.Why, "crashing too") {
		t.Errorf("replicas: head %q, %q", a.HeadPod, a.Status.Why)
	}
	// Crashing alike, leapfrogging by one: the headline stays (refreshes
	// rebuild it, so a lead of one would swap it with each crash).
	for _, n := range [][2]int64{{5, 5}, {5, 6}, {6, 6}, {6, 7}} {
		if a = app(pod("r-1", cs("api", n[0], true, ago)), pod("r-2", cs("api", n[1], true, ago))); a.HeadPod != "r-1" {
			t.Errorf("%v: head %q", n, a.HeadPod)
		}
	}
}

// Round 117: two copies crashing alike, one 2 ahead for a moment: it takes
// the headline and keeps it as the other catches up (it went back to the
// first by name, so the two swapped as they crashed in turn). The workload
// box names the same copy.
func TestNamedCopyKeepsItsPlace(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour).Format(time.RFC3339)
	ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
	pod := func(name string, restarts int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r117", "uid": name, "labels": map[string]any{"app": "r117"}, "creationTimestamp": old},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": "busybox"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "api", "restartCount": restarts, "ready": false,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	e := enrich.New(jobStore{}, res, nil) // one for every refresh, as in the app
	head := func(n1, n2 int64) string {
		for _, a := range Apps(e, []*unstructured.Unstructured{pod("r-1", n1), pod("r-2", n2)}, nil, nil, nil, nil) {
			if a.Name == "r117" {
				if a.CrashPod != a.HeadPod {
					t.Errorf("%d/%d: crash %q, head %q", n1, n2, a.CrashPod, a.HeadPod)
				}
				return a.HeadPod
			}
		}
		return ""
	}
	for _, step := range []struct {
		n1, n2 int64
		want   string
	}{
		{6, 7, "r-1"}, // one ahead: the first keeps it
		{6, 8, "r-2"}, // two ahead: takes it
		{7, 8, "r-2"},
		{8, 8, "r-2"}, // caught up: stays (was r-1)
		{9, 8, "r-2"}, // one ahead: stays
		{10, 8, "r-1"},
		{10, 10, "r-1"},
		// The review's sequence: each copy one ahead in turn, no swaps.
		{10, 11, "r-1"}, {11, 11, "r-1"}, {11, 12, "r-1"}, {12, 12, "r-1"}, {12, 13, "r-1"},
	} {
		if got := head(step.n1, step.n2); got != step.want {
			t.Errorf("%d/%d: head %q, want %q", step.n1, step.n2, got, step.want)
		}
	}
	// Three copies: the named one stays only within one of the most
	// crashed, not of whichever the pass held.
	three := func(n1, n2, n3 int64) string {
		for _, a := range Apps(e, []*unstructured.Unstructured{pod("r-1", n1), pod("r-2", n2), pod("r-3", n3)}, nil, nil, nil, nil) {
			if a.Name == "r117" {
				return a.HeadPod
			}
		}
		return ""
	}
	if got := three(10, 10, 12); got != "r-3" {
		t.Errorf("r-3 two ahead: %q", got)
	}
	if got := three(11, 12, 12); got != "r-3" { // named, within one: stays
		t.Errorf("named within one: %q", got)
	}
	if got := three(11, 14, 12); got != "r-2" { // named, two behind the most: goes
		t.Errorf("named two behind: %q", got)
	}
}

// Review of round 102: the container that crashed most is the headline
// whatever order the pods come in; a failed job's wording is used only
// when the headline is about the job.
func TestMostCrashesAnyOrder(t *testing.T) {
	ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
	pod := func(name, container string, restarts int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r102e", "uid": name, "labels": map[string]any{"app": "r102e"}, "creationTimestamp": ago},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": container, "image": "busybox"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": container, "restartCount": restarts,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	pods := []*unstructured.Unstructured{pod("svc-1", "api", 5), pod("svc-2", "api", 9), pod("svc-3", "worker", 7)}
	e := enrich.New(jobStore{kube.Pods: pods}, res, nil)
	for _, a := range Apps(e, pods, nil, nil, nil, nil) {
		if a.Name == "r102e" && a.CrashContainer != "api" {
			t.Errorf("%q: %q", a.CrashContainer, a.Status.Why)
		}
	}
	a := &App{Status: Status{Short: "Can't start", Why: "x"}, jobKinds: map[string]bool{"Job": true}}
	if w := fixWhere(a, false); strings.Contains(w, "job") {
		t.Errorf("not about the job: %q", w)
	}
}

// Round 103: a pod's phase says Failed a moment after its containers
// have all stopped; in that moment its try has failed already, and home,
// gj and the count say so (never "Running", never Retrying after the last).
func TestTryFailedBeforeItsPhase(t *testing.T) {
	ended := func(ago time.Duration) map[string]any { // all stopped, phase not yet Failed
		return map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main",
			"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": time.Now().Add(-ago).Format(time.RFC3339)}}}}}
	}
	behind := map[string]any{"active": int64(1)}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3)}, "status": behind}, ended(time.Second))
	if a := r102app(e, j, ps); a.Status.Short != "Retrying" {
		t.Errorf("first try: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Retrying (1 try failed") {
		t.Errorf("gj, first try: %q", cells[1])
	}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)}, "status": behind}, ended(time.Second))
	if a := r102app(e, j, ps); a.Status.Short != "Job failed" {
		t.Errorf("only try: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Failed") {
		t.Errorf("gj, only try: %q", cells[1])
	}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3)}, "status": map[string]any{"active": int64(1), "failed": int64(3)}},
		r102failed(3*time.Minute, 1, "Error"), r102failed(2*time.Minute, 1, "Error"), r102failed(time.Minute, 1, "Error"), ended(time.Second))
	if a := r102app(e, j, ps); a.Status.Short != "Job failed" || strings.Contains(a.Status.Why, "running") {
		t.Errorf("last try: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Failed") {
		t.Errorf("gj, last try: %q", cells[1])
	}
	if n := failedTries(e, j); n != 4 {
		t.Errorf("count: %d", n)
	}
}

// Round 103: two tries failed (the second pod's phase not yet Failed),
// the job still says active 1, failed 1: the app counts 2, both are
// leftovers, none is running.
func TestSecondTryNotCountedYet(t *testing.T) {
	ended := map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": time.Now().Add(-time.Second).Format(time.RFC3339)}}}}}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(2)}, "status": map[string]any{"active": int64(1), "failed": int64(1)}},
		r102failed(time.Minute, 1, "Error"), ended)
	ps[0].SetCreationTimestamp(metav1.NewTime(time.Now().Add(-2 * time.Minute)))
	ps[1].SetCreationTimestamp(metav1.NewTime(time.Now().Add(-30 * time.Second)))
	a := r102app(e, j, ps)
	if a.Status.Short != "Retrying" || !strings.Contains(a.Status.Why, "2 tries failed") || a.Leftovers != 2 || a.Pods != 0 {
		t.Errorf("%+v leftovers %d pods %d", a.Status, a.Leftovers, a.Pods)
	}
	if a.CrashPod != "r102-1" { // the Error line quotes the try that just failed
		t.Errorf("error line from %q", a.CrashPod)
	}
	// One main container failed, another not started: not ended yet.
	p := &corev1.Pod{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{Name: "a", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}},
			{Name: "b", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}}}
	if st := Explain(enrich.New(benchStore{}, nil, nil), p); strings.Contains(st.Why, "then the pod ends") {
		t.Errorf("one still to start: %+v", st)
	}
}

// Review of round 103: a try that failed while another main container
// hasn't started counts once (backoffLimit 0: Job failed, not Retrying).
func TestStoppingTryCountsOnce(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	j := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "r103s", "namespace": "r103s", "uid": "r103sj", "labels": map[string]any{"app": "r103s"}},
		"spec":     map[string]any{"backoffLimit": int64(0)}, "status": map[string]any{"active": int64(1)}}}
	now := time.Now().Format(time.RFC3339)
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "r103s-x", "namespace": "r103s", "uid": "r103sp", "labels": map[string]any{"app": "r103s"},
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "r103s", "uid": "r103sj", "controller": true}}},
		"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "c1", "image": "x"}, map[string]any{"name": "c2", "image": "x"}},
			"initContainers": []any{map[string]any{"name": "side", "image": "x", "restartPolicy": "Always"}}},
		"status": map[string]any{"phase": "Running",
			"containerStatuses": []any{
				map[string]any{"name": "c1", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": now}}},
				map[string]any{"name": "c2", "state": map[string]any{"waiting": map[string]any{"reason": "ContainerCreating"}}}},
			"initContainerStatuses": []any{map[string]any{"name": "side", "state": map[string]any{"running": map[string]any{"startedAt": now}}}}}}}
	e := enrich.New(jobStore{kube.Jobs: {j}, kube.Pods: {p}}, res, nil)
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, []*unstructured.Unstructured{j}, nil) {
		if a.Name == "r103s" && a.Status.Short == "Retrying" {
			t.Errorf("%+v", a.Status)
		}
	}
}

// Round 104: a paused job with a failed try stays listed (paused), a try
// held open by another container is not "Job failed" before Kubernetes
// says so (and gj agrees), and a failed job's time is when its pod ended.
func TestRound104Jobs(t *testing.T) {
	ago := func(d time.Duration) string { return time.Now().Add(-d).Format(time.RFC3339) }
	// Paused, its failed try left over.
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3), "suspend": true},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Suspended", "status": "True"}}}},
		r102failed(time.Minute, 1, "Error"))
	if a := r102app(e, j, ps); a.Status.Short != "Paused" || a.Leftovers != 1 {
		t.Errorf("paused: %+v (leftovers %d)", a.Status, a.Leftovers)
	}
	// Held open: main failed, a second container still at work.
	held := map[string]any{"phase": "Running", "containerStatuses": []any{
		map[string]any{"name": "main", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago(5 * time.Second)}}},
		map[string]any{"name": "slow", "state": map[string]any{"running": map[string]any{"startedAt": ago(time.Minute)}}}}}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)}, "status": map[string]any{"active": int64(1)}}, held)
	ps[0].Object["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "main", "image": "x"}, map[string]any{"name": "slow", "image": "x"}}
	if a := r102app(e, j, ps); a.Status.Short != "Try failed" {
		t.Errorf("held open, app: %+v", a.Status)
	}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Try failed") {
		t.Errorf("held open, gj: %q", cells[1])
	}
	// Failed once the second container stopped, a minute after main.
	ended := map[string]any{"phase": "Failed", "containerStatuses": []any{
		map[string]any{"name": "main", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago(55 * time.Second)}}},
		map[string]any{"name": "slow", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed", "finishedAt": ago(2 * time.Second)}}}}}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True", "lastTransitionTime": ago(0)}}}}, ended)
	if a := r102app(e, j, ps); !strings.Contains(a.Status.Why, "failed 2s ago") {
		t.Errorf("time: %+v", a.Status)
	}
}

// Review of round 104: one clock for "failed N ago" before and after the
// job marks it; gj says Try failed after a try was counted too; a paused
// job counts its own finished pods.
func TestRound104Followups(t *testing.T) {
	ago := func(d time.Duration) string { return time.Now().Add(-d).Format(time.RFC3339) }
	ended := map[string]any{"phase": "Failed", "containerStatuses": []any{
		map[string]any{"name": "main", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago(55 * time.Second)}}},
		map[string]any{"name": "slow", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed", "finishedAt": ago(2 * time.Second)}}}}}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(0)}, "status": map[string]any{"active": int64(1)}}, ended)
	if a := r102app(e, j, ps); !strings.Contains(a.Status.Why, "failed 2s ago") {
		t.Errorf("before the job marks it: %+v", a.Status)
	}
	held := map[string]any{"phase": "Running", "containerStatuses": []any{
		map[string]any{"name": "main", "state": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago(5 * time.Second)}}},
		map[string]any{"name": "slow", "state": map[string]any{"running": map[string]any{"startedAt": ago(time.Minute)}}}}}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3)}, "status": map[string]any{"active": int64(1), "failed": int64(1)}},
		r102failed(2*time.Minute, 1, "Error"), held)
	ps[1].Object["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "main", "image": "x"}, map[string]any{"name": "slow", "image": "x"}}
	if cells, _ := registry[kube.Jobs.GroupResource()].Row(e, j); !strings.HasPrefix(cells[1], "Try failed") {
		t.Errorf("gj, second try held open: %q", cells[1])
	}
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(3), "suspend": true}, "status": map[string]any{"failed": int64(1)}},
		r102failed(time.Minute, 1, "Error"))
	evicted := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "r102-ev", "namespace": "r102", "uid": "r102ev", "labels": map[string]any{"app": "r102"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "main", "image": "x"}}},
		"status":   map[string]any{"phase": "Failed", "reason": "Evicted"}}}
	if a := r102app(e, j, append(ps, evicted)); !strings.Contains(a.Status.Why, "1 finished pod kept") {
		t.Errorf("paused: %+v", a.Status)
	}
}

// Round 106: a try whose pod was deleted (or pushed off its node) still
// counts: the job's own count keeps it. Home says Retrying while the next
// try runs or before it exists, and the last try reads failed.
func TestTryWhosePodIsGone(t *testing.T) {
	running := map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"running": map[string]any{"startedAt": time.Now().Format(time.RFC3339)}}}}}
	// Try 1's pod deleted, try 2 running.
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(1)}, "status": map[string]any{"failed": int64(1), "active": int64(1)}}, running)
	if a := r102app(e, j, ps); a.Status.Short != "Retrying" || !strings.Contains(a.Status.Why, "try 2 running, 1 try failed before") {
		t.Errorf("next try runs: %+v", a.Status)
	}
	// No pod at all between tries: still listed, Retrying.
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(1)}, "status": map[string]any{"failed": int64(1)}})
	if a := r102app(e, j, ps); a.Status.Short != "Retrying" {
		t.Errorf("between tries, no pod: %+v", a.Status)
	}
	// Try 2 failed (not yet counted), try 1's pod gone: 2 tries, the last.
	pending := r102failed(time.Second, 1, "Error")
	e, j, ps = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(1)}, "status": map[string]any{"failed": int64(1), "active": int64(1)}}, pending)
	ps[0].SetFinalizers([]string{"batch.kubernetes.io/job-tracking"})
	if n := failedTries(e, j); n != 2 {
		t.Errorf("count: %d", n)
	}
	if a := r102app(e, j, ps); a.Status.Short != "Job failed" {
		t.Errorf("last try: %+v", a.Status)
	}
}

// Round 106: a pod shutting down while its replacement isn't ready: the
// app isn't "healthy" (none ready is worth a look).
func TestReplacingAPod(t *testing.T) {
	now := metav1.Now()
	mk := func(name string, del bool, ready bool) *unstructured.Unstructured {
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r106", "uid": name, "labels": map[string]any{"app": "r106"},
				"creationTimestamp": time.Now().Add(-time.Hour).Format(time.RFC3339)},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[ready]}},
				"containerStatuses": []any{map[string]any{"name": "c", "ready": ready, "state": map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-time.Second).Format(time.RFC3339)}}}}}}}
		if del {
			p.SetDeletionTimestamp(&now)
		}
		return p
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	pods := []*unstructured.Unstructured{mk("old", true, true), mk("new", false, false)}
	e := enrich.New(jobStore{kube.Pods: pods}, res, nil)
	for _, a := range Apps(e, pods, nil, nil, nil, nil) {
		if a.Name == "r106" && (a.Status.Short != "Replacing a pod" || a.Status.Tone != ToneWarn) {
			t.Errorf("%+v", a.Status)
		}
	}
}

// Review of round 106: "Replacing" counts against what READY counts: a
// surge rollout at full strength stays healthy; a StatefulSet restart
// one short says so.
func TestReplacingAgainstReady(t *testing.T) {
	healthy := Status{"Running", "healthy", ToneNormal}
	surge := &App{Status: healthy, Pods: 4, Ready: 3, Stopping: 1, wants: 3}
	if st, ok := replacing(surge); ok {
		t.Errorf("surge at full strength: %+v (READY %s)", st, readyCell(surge))
	}
	sts := &App{Status: healthy, Pods: 1, Ready: 1, Stopping: 1, wants: 2}
	if st, ok := replacing(sts); !ok || !strings.HasPrefix(st.Why, "1 of 2 ready") || readyCell(sts) != "1/2" {
		t.Errorf("one short: %+v %v (READY %s)", st, ok, readyCell(sts))
	}
}

// Round 107: a pod not ready yet with nothing on its way out reads
// Starting (none ready: worth a look), not healthy; a try whose pod is
// being deleted reads Retrying, not Stopping.
func TestStartingAndDeletedTry(t *testing.T) {
	healthy := Status{"Running", "healthy", ToneNormal}
	one := &App{Status: healthy, Pods: 1, Ready: 0, wants: 1, starting: 1, notReady: Status{"Starting", `c is starting up`, ToneNormal}}
	if st, ok := replacing(one); !ok || st.Short != "Starting" || st.Tone != ToneWarn || !strings.Contains(st.Why, "0 of 1 ready; c is starting up") {
		t.Errorf("one copy: %+v %v", st, ok)
	}
	two := &App{Status: healthy, Pods: 2, Ready: 1, wants: 2, starting: 1}
	if st, ok := replacing(two); !ok || st.Tone != ToneNormal || st.Why != "1 of 2 ready" {
		t.Errorf("one of two: %+v %v", st, ok)
	}
	now := metav1.Now()
	running := map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"running": map[string]any{"startedAt": time.Now().Format(time.RFC3339)}}}}}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(2)}, "status": map[string]any{"failed": int64(1)}}, running)
	ps[0].SetDeletionTimestamp(&now)
	if a := r102app(e, j, ps); a.Status.Short != "Retrying" {
		t.Errorf("deleted try: %+v", a.Status)
	}
}

// Round 107: an overprovisioner's low-priority pod waits for a node by
// design: not a problem, and not one the app is short of.
func TestPlaceholderPod(t *testing.T) {
	prio := int32(-10)
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "overprovisioner-x", Namespace: "karpenter"},
		Spec: corev1.PodSpec{Priority: &prio, Containers: []corev1.Container{{Name: "reserve", Image: "registry.k8s.io/pause:3.9"}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "0/16 nodes are available: 1 Too many pods."}}}}
	e := enrich.New(benchStore{}, nil, nil)
	if st := Explain(e, p); st.Short != "Placeholder" || st.Tone != ToneNormal {
		t.Errorf("pause pod: %+v", st)
	}
	p.Spec.Containers[0].Image = "myorg/batch:1" // a real low-priority workload that can't be placed: a problem
	if st := Explain(e, p); st.Short == "Placeholder" {
		t.Errorf("real workload: %+v", st)
	}
	a := &App{Status: Status{"Running", "healthy", ToneNormal}, Pods: 4, Ready: 3, placeholders: 1}
	if st, ok := replacing(a); ok {
		t.Errorf("short of a placeholder: %+v", st)
	}
}

// Review of round 107: a cron run starting up isn't the app "Starting"
// (every run would flag it), and a job being removed isn't retrying.
func TestStartingSkipsRunsAndRemovedJobs(t *testing.T) {
	pending := map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"waiting": map[string]any{"reason": "ContainerCreating"}}}}}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(2)}, "status": map[string]any{"active": int64(1)}}, pending)
	if a := r102app(e, j, ps); a.Status.Short == "Starting" || a.Status.Tone != ToneNormal {
		t.Errorf("a run starting: %+v", a.Status)
	}
	now := metav1.Now()
	e, j, _ = r102(map[string]any{"spec": map[string]any{"backoffLimit": int64(2)}, "status": map[string]any{"failed": int64(1)}})
	j.SetDeletionTimestamp(&now)
	if jobRetrying(e, j) {
		t.Error("a job being removed reads Retrying")
	}
}

// Round 108: a starting or replacing app's WHY says why it isn't all ready,
// though it isn't a problem.
func TestWhyForStarting(t *testing.T) {
	a := &App{Status: Status{"Starting", "1 of 2 ready; db is starting up", ToneNormal}}
	if w := AppWhy(a, Lookups{}); w != "1 of 2 ready; db is starting up" {
		t.Errorf("%q", w)
	}
	if w := AppWhy(&App{Status: Status{"Running", "healthy", ToneNormal}}, Lookups{}); w != "" {
		t.Errorf("healthy: %q", w)
	}
}

// Round 110: Kubernetes stops repeating a ready check's failure event
// while the check goes on failing: the age says it was the last report,
// not when it failed.
func TestNotReadyReasonWhenEventsStop(t *testing.T) {
	ev := func(ago time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": "u", "namespace": "ns"}, "reason": "Unhealthy", "message": "Readiness probe failed: ",
			"involvedObject": map[string]any{"kind": "Pod", "name": "p"}, "lastTimestamp": time.Now().Add(-ago).UTC().Format(time.RFC3339)}}
	}
	status := func(ago time.Duration) Status {
		e := enrich.New(listStore{kube.Events: {ev(ago)}}, kube.NewResources([]kube.Resource{{GVR: kube.Events, Kind: "Event", Namespaced: true}}), nil)
		return Explain(e, pod(corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: false,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now().Add(-10 * time.Minute))}}}}}))
	}
	if st := status(10 * time.Second); st.Why != "ready check failed (its command gave no output) (10s ago)" {
		t.Errorf("fresh: %q", st.Why)
	}
	if st := status(20 * time.Minute); strings.Contains(st.Why, "still") { // from a run before this one: says nothing about it
		t.Errorf("earlier run: %q", st.Why)
	}
	if st := status(4 * time.Minute); !strings.Contains(st.Why, "still failing; last reported 4m ago") {
		t.Errorf("stale: %q", st.Why)
	}
}

// Round 112: a rollout stuck because its pod crashes quotes that crash as
// its Error, not as "Also" (a second problem).
func TestStuckCrashIsTheError(t *testing.T) {
	a := &App{Status: Status{"Rollout stuck", "first version of deployment/crash can't start: crashing: ...", ToneBad},
		stuckBy: Status{Short: "Crashing"}, HeadNS: "ns", HeadPod: "crash-1", CrashNS: "ns", CrashPod: "crash-1", CrashContainer: "crash"}
	look := Lookups{Crash: func(*App) (string, bool) { return "FATAL: cannot connect to db", true }}
	got := strings.Join(AppLines(a, look), "\n")
	if !strings.Contains(got, "Error    FATAL") || strings.Contains(got, "Also") {
		t.Errorf("%s", got)
	}
}

// Round 113: a Deployment's own box says its rollout is stuck, as home
// does, ahead of the pod's problem.
func TestWorkloadBoxSaysStuck(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}})
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "crashy", "namespace": "r113", "uid": "r113d"},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}}}}
	ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "crashy-1", "namespace": "r113", "uid": "r113p",
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "crashy", "uid": "r113d", "controller": true}}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "main", "image": "x"}}},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main", "restartCount": int64(7),
			"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": ago}}}}}}}
	e := enrich.New(jobStore{kube.Pods: {p}, kube.Deployments: {dep}}, res, nil)
	if got := strings.Join(workloadProblem(e, dep), "\n"); !strings.Contains(got, "rollout stuck") || !strings.Contains(got, "Crashing") {
		t.Errorf("%s", got)
	}
}

// Round 113: a paused job that never ran is listed on home as Paused.
func TestPausedJobNeverRan(t *testing.T) {
	e, j, ps := r102(map[string]any{"spec": map[string]any{"suspend": true}, "status": map[string]any{
		"conditions": []any{map[string]any{"type": "Suspended", "status": "True"}}}})
	a := r102app(e, j, ps)
	if a.Status.Short != "Paused" || strings.Contains(a.Status.Why, "kept") {
		t.Errorf("%+v", a.Status)
	}
}

// Review of round 113: a paused job that never ran stays listed though
// its app also keeps another job's finished pod; one held by Kueue reads
// Queued.
func TestPausedJobBesideAFinishedOne(t *testing.T) {
	done := map[string]any{"phase": "Succeeded", "containerStatuses": []any{map[string]any{"name": "main",
		"state": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}}}}}
	e, j, ps := r102(map[string]any{"spec": map[string]any{"suspend": true}}, done)
	ps[0].Object["metadata"].(map[string]any)["ownerReferences"] = []any{} // another job's pod, its job gone
	a := r102app(e, j, ps)
	if a.Status.Short != "Paused" {
		t.Errorf("beside a finished pod: %+v", a.Status)
	}
	j.SetLabels(map[string]string{"app": "r102", "kueue.x-k8s.io/queue-name": "batch"})
	if a := r102app(e, j, ps); a.Status.Short != "Queued" {
		t.Errorf("Kueue: %+v", a.Status)
	}
}

// Round 115: a stuck rollout with two pods crashing quotes the one that
// crashed most, whatever order they come in.
func TestStuckQuotesMostCrashedPod(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "crashdep", "namespace": "r115", "uid": "r115d", "labels": map[string]any{"app": "crashdep"}},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "crashdep-a", "namespace": "r115", "uid": "r115rs",
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "crashdep", "uid": "r115d", "controller": true}}}}}
	pod := func(name string, restarts int64) *unstructured.Unstructured {
		ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r115", "uid": name, "labels": map[string]any{"app": "crashdep"},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "crashdep-a", "uid": "r115rs", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "restartCount": restarts,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(3), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	for _, pods := range [][]*unstructured.Unstructured{{pod("crashdep-6", 6), pod("crashdep-1", 1)}, {pod("crashdep-1", 1), pod("crashdep-6", 6)}} {
		e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, res, nil)
		for _, a := range Apps(e, pods, nil, nil, nil, nil) {
			if a.Name == "crashdep" && (a.Status.Short != "Rollout stuck" || !strings.Contains(a.Status.Why, "6 restarts") || a.CrashPod != a.HeadPod || a.CrashPod != "crashdep-6") {
				t.Errorf("order %s,%s: %+v head %q crash %q", pods[0].GetName(), pods[1].GetName(), a.Status, a.HeadPod, a.CrashPod)
			}
		}
	}
	// Round 117: the copy quoted (two ahead a moment ago) stays quoted as
	// the other catches up, the Error line and p with it.
	e := enrich.New(jobStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, res, nil)
	for _, step := range []struct {
		n1, n6 int64
		want   string
	}{{4, 6, "crashdep-6"}, {6, 6, "crashdep-6"}, {7, 6, "crashdep-6"}, {8, 6, "crashdep-1"}} {
		pods := []*unstructured.Unstructured{pod("crashdep-1", step.n1), pod("crashdep-6", step.n6)}
		for _, a := range Apps(e, pods, nil, nil, nil, nil) {
			if a.Name == "crashdep" && (a.Status.Short != "Rollout stuck" || a.CrashPod != step.want || a.HeadPod != step.want || !strings.Contains(a.Status.Why, fmt.Sprintf("; %d restarts", map[string]int64{"crashdep-1": step.n1, "crashdep-6": step.n6}[step.want]))) {
				t.Errorf("%d/%d: %+v head %q crash %q", step.n1, step.n6, a.Status, a.HeadPod, a.CrashPod)
			}
		}
	}
}

// Review of round 115: one app, two stuck deployments: the Fix's go back
// (CanUndo) follows the deployment its stuck reason names.
func TestStuckUndoFollowsTheQuotedDeployment(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	stuck := map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}}
	dep := func(name string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": name, "namespace": "r115u", "uid": name, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"status":   stuck}}
	}
	rs := func(name, dep, rev string, ago time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "r115u", "uid": name, "creationTimestamp": time.Now().Add(-ago).Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": dep, "uid": dep, "controller": true}}}}}
	}
	pod := func(name, owner string, restarts int64) *unstructured.Unstructured {
		ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r115u", "uid": name, "labels": map[string]any{"app": "r115u"},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": owner, "uid": owner, "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "restartCount": restarts,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(3), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	// x: only a first version (no going back), crashing more; y: has an earlier version, crashing less.
	pods := []*unstructured.Unstructured{pod("x-1", "x-a", 6), pod("y-1", "y-b", 1)}
	e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep("x"), dep("y")},
		kube.ReplicaSets: {rs("x-a", "x", "2", time.Hour), rs("y-a", "y", "1", 2*time.Hour), rs("y-b", "y", "2", time.Hour)}}, res, nil)
	for _, a := range Apps(e, pods, nil, nil, nil, nil) {
		if a.Name == "r115u" && (!strings.Contains(a.Status.Why, "deployment/x") || a.CanUndo) {
			t.Errorf("%+v undo %v", a.Status, a.CanUndo)
		}
	}
}

// Round 116: a stuck deployment's own box quotes the pod that crashed
// most, as home and p do.
func TestWorkloadBoxQuotesMostCrashed(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}})
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "crashy", "namespace": "r116", "uid": "r116d"},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}}}}
	pod := func(name string, restarts int64) *unstructured.Unstructured {
		ago := time.Now().Add(-time.Minute).Format(time.RFC3339)
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r116", "uid": name,
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "crashy", "uid": "r116d", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "main", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "main", "restartCount": restarts,
				"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(3), "reason": "Error", "finishedAt": ago}}}}}}}
	}
	for _, pods := range [][]*unstructured.Unstructured{{pod("c-3", 3), pod("c-7", 7)}, {pod("c-7", 7), pod("c-3", 3)}} {
		e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep}}, res, nil)
		if got := strings.Join(workloadProblem(e, dep), "\n"); !strings.Contains(got, "pod c-7") {
			t.Errorf("%s", got)
		}
	}
	// A lead of one: the first stays (no swapping as they crash alike).
	pods := []*unstructured.Unstructured{pod("c-6", 6), pod("c-7", 7)}
	e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep}}, res, nil)
	if got := strings.Join(workloadProblem(e, dep), "\n"); !strings.Contains(got, "pod c-6") {
		t.Errorf("lead of one: %s", got)
	}
	// Round 117: the copy home names (c-7, two ahead a moment ago) stays
	// the box's too once the other catches up.
	two := []*unstructured.Unstructured{pod("c-6", 6), pod("c-7", 8)}
	e = enrich.New(jobStore{kube.Pods: two, kube.Deployments: {dep}}, res, nil)
	Apps(e, two, nil, nil, nil, []*unstructured.Unstructured{dep})
	e2 := enrich.New(jobStore{kube.Pods: []*unstructured.Unstructured{pod("c-6", 8), pod("c-7", 8)}, kube.Deployments: {dep}}, res, nil)
	e2.SetNamed(map[string]bool{"r116/c-7/main": true}) // as home left it
	if !e.Named("r116", "c-7", "main") {
		t.Errorf("home didn't name c-7")
	}
	if got := strings.Join(workloadProblem(e2, dep), "\n"); !strings.Contains(got, "pod c-7") {
		t.Errorf("box after home: %s", got)
	}
}

// Round 116: pods pushed off a node: while they stop and before their
// replacements show, the app says it is replacing them and keeps its
// Deployed time.
func TestEvictedPodsBeingReplaced(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	made := time.Now().Add(-10 * time.Minute)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "r116e", "uid": "r116d", "labels": map[string]any{"app": "web"},
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}},
		"spec": map[string]any{"replicas": int64(1)}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-a", "namespace": "r116e", "uid": "r116rs", "creationTimestamp": made.Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "r116d", "controller": true}}}}}
	now := metav1.Now()
	p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "web-a-1", "namespace": "r116e", "uid": "r116p", "labels": map[string]any{"app": "web"}, "creationTimestamp": made.Format(time.RFC3339),
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-a", "uid": "r116rs", "controller": true}}},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}, "terminationGracePeriodSeconds": int64(30)},
		"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": true,
			"state": map[string]any{"running": map[string]any{"startedAt": made.Format(time.RFC3339)}}}}}}}
	p.SetDeletionTimestamp(&now)
	e := enrich.New(jobStore{kube.Pods: {p}, kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, res, nil)
	found := false
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if a.Name == "web" {
			found = true
			if a.Status.Short != "Replacing a pod" || a.LastDeploy.IsZero() {
				t.Errorf("%+v deployed %v", a.Status, a.LastDeploy)
			}
		}
	}
	if !found {
		t.Fatal("no app web")
	}
	// The deployment itself being deleted (foreground): nothing comes back.
	dep.SetDeletionTimestamp(&now)
	for _, a := range Apps(e, []*unstructured.Unstructured{p}, nil, nil, nil, []*unstructured.Unstructured{dep}) {
		if a.Name == "web" && strings.HasPrefix(a.Status.Short, "Replacing") {
			t.Errorf("deployment being deleted: %+v", a.Status)
		}
	}
}

// Round 118: a copy that exits cleanly as it is asked to stop reads
// Succeeded for a moment: it is stopping, not a finished pod kept for its
// logs.
func TestCleanlyStoppedCopyIsStopping(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-a", "namespace": "r118", "uid": "r118rs"}}}
	pod := func(name, phase string, deleting time.Time) *unstructured.Unstructured {
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r118", "uid": name, "labels": map[string]any{"app": "web"},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-a", "uid": "r118rs", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}, "terminationGracePeriodSeconds": int64(30)},
			"status": map[string]any{"phase": phase, "containerStatuses": []any{map[string]any{"name": "c",
				"state": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}}}}}}}
		if !deleting.IsZero() {
			at := metav1.NewTime(deleting)
			p.SetDeletionTimestamp(&at)
		}
		return p
	}
	run := pod("web-a-4", "Succeeded", time.Now())
	run.Object["spec"].(map[string]any)["restartPolicy"] = "Never"
	for _, c := range []struct {
		p                  *unstructured.Unstructured
		stopping, leftover int
	}{
		{pod("web-a-1", "Succeeded", time.Now()), 1, 1},                      // stopping (counted in both, as any stopping pod)
		{pod("web-a-2", "Succeeded", time.Time{}), 0, 1},                     // finished, not deleted: a leftover
		{pod("web-a-3", "Succeeded", time.Now().Add(-10*time.Minute)), 0, 1}, // stuck past its grace period: as before
		{run, 0, 1}, // a run (restart Never) that finished: a leftover, deleted or not
	} {
		live := pod("web-b-0", "Running", time.Time{}) // the copy that stays: the app is listed
		live.Object["status"] = map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "ready": true,
			"state": map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-time.Hour).Format(time.RFC3339)}}}}}
		pods := []*unstructured.Unstructured{c.p, live}
		e := enrich.New(jobStore{kube.Pods: pods, kube.ReplicaSets: {rs}}, res, nil)
		found := false
		for _, a := range Apps(e, pods, nil, nil, nil, nil) {
			if a.Name == "web" {
				found = true
				if a.Stopping != c.stopping || a.Leftovers != c.leftover {
					t.Errorf("%s: stopping %d leftovers %d", c.p.GetName(), a.Stopping, a.Leftovers)
				}
			}
		}
		if !found {
			t.Errorf("%s: no app web", c.p.GetName())
		}
	}
}

// Round 119: two containers restarting now and then (neither crashing
// now): home names the one that restarted last, whatever their names'
// order, and keeps it.
func TestRestartingNamesTheLatest(t *testing.T) {
	old := time.Now().Add(-3 * time.Hour).Format(time.RFC3339)
	pod := func(name, container string, restarts int64, ago time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r119", "uid": name, "labels": map[string]any{"app": "r119"}, "creationTimestamp": old},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": container, "image": "x"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": container, "restartCount": restarts, "ready": true,
				"state":     map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-ago).Format(time.RFC3339)}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": time.Now().Add(-ago).Format(time.RFC3339)}}}}}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	for _, pods := range [][]*unstructured.Unstructured{
		{pod("job-consumer-1", "job-consumer", 19, 38*time.Minute), pod("job-scheduler-1", "job-scheduler", 20, 33*time.Minute)},
		{pod("job-scheduler-1", "job-scheduler", 20, 33*time.Minute), pod("job-consumer-1", "job-consumer", 19, 38*time.Minute)},
	} {
		e := enrich.New(jobStore{kube.Pods: pods}, res, nil)
		found := false
		for _, a := range Apps(e, pods, nil, nil, nil, nil) {
			if a.Name == "r119" {
				found = true
				if a.Status.Short != "Restarting" || !strings.Contains(a.Status.Why, `"job-scheduler"`) || a.HeadPod != "job-scheduler-1" {
					t.Errorf("%+v head %q", a.Status, a.HeadPod)
				}
			}
		}
		if !found {
			t.Fatal("no app r119")
		}
	}
}

// Review of round 119: a stuck deployment whose pods both restart now and
// then: home's stuck reason and the deployment's box name the same pod, the
// one that restarted last, whatever the order.
func TestStuckRestartingNamesTheLatest(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}})
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "back", "namespace": "r119s", "uid": "r119d", "labels": map[string]any{"app": "back"}},
		"status":   map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "back-a", "namespace": "r119s", "uid": "r119rs",
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "back", "uid": "r119d", "controller": true}}}}}
	pod := func(name string, ago time.Duration) *unstructured.Unstructured {
		at := time.Now().Add(-ago).Format(time.RFC3339)
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r119s", "uid": name, "labels": map[string]any{"app": "back"},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "back-a", "uid": "r119rs", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "c", "restartCount": int64(20), "ready": true,
				"state":     map[string]any{"running": map[string]any{"startedAt": at}},
				"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error", "finishedAt": at}}}}}}}
	}
	for _, pods := range [][]*unstructured.Unstructured{{pod("back-1", 38*time.Minute), pod("back-2", 33*time.Minute)}, {pod("back-2", 33*time.Minute), pod("back-1", 38*time.Minute)}} {
		e := enrich.New(jobStore{kube.Pods: pods, kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, res, nil)
		found := false
		for _, a := range Apps(e, pods, nil, nil, nil, []*unstructured.Unstructured{dep}) {
			if a.Name == "back" {
				found = true
				if a.Status.Short != "Rollout stuck" || a.HeadPod != "back-2" {
					t.Errorf("home: %+v head %q", a.Status, a.HeadPod)
				}
			}
		}
		if !found {
			t.Fatal("no app back")
		}
		if got := strings.Join(workloadProblem(e, dep), "\n"); !strings.Contains(got, "pod back-2") {
			t.Errorf("box: %s", got)
		}
	}
}

// Round 120: a job's running pod deleted counts as a failed try at once,
// while the pod still stops: not "gone".
func TestRetryingJobWhosePodStillStops(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "longjob", "namespace": "r120", "uid": "j120", "creationTimestamp": time.Now().Add(-14 * time.Minute).Format(time.RFC3339)},
		"status":   map[string]any{"failed": int64(1), "active": int64(1)}}}
	pod := func(name string, deleting bool) *unstructured.Unstructured {
		p := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "r120", "uid": name,
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "longjob", "uid": "j120", "controller": true}}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "j", "image": "busybox"}}, "restartPolicy": "Never", "terminationGracePeriodSeconds": int64(30)},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "j", "ready": true,
				"state": map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-14 * time.Minute).Format(time.RFC3339)}}}}}}}
		if deleting {
			now := metav1.Now()
			p.SetDeletionTimestamp(&now)
		}
		return p
	}
	for _, pods := range [][]*unstructured.Unstructured{{pod("longjob-old", true)}, {pod("longjob-old", true), pod("longjob-new", false)}} {
		e := enrich.New(jobStore{kube.Jobs: {job}, kube.Pods: pods}, res, nil)
		why := ""
		for _, a := range Apps(e, pods, nil, nil, []*unstructured.Unstructured{job}, nil) {
			if a.Status.Short == "Retrying" {
				why = a.Status.Why
			}
		}
		if !strings.Contains(why, "still stopping") || strings.Contains(why, "gone") {
			t.Errorf("%d pods: %q", len(pods), why)
		}
	} // A pod that finished fine being cleaned up isn't the failed try.
	done := pod("longjob-done", true)
	done.Object["status"] = map[string]any{"phase": "Succeeded"}
	pods := []*unstructured.Unstructured{done, pod("longjob-new", false)}
	e := enrich.New(jobStore{kube.Jobs: {job}, kube.Pods: pods}, res, nil)
	for _, a := range Apps(e, pods, nil, nil, []*unstructured.Unstructured{job}, nil) {
		if strings.Contains(a.Status.Why, "still stopping") {
			t.Errorf("finished pod: %q", a.Status.Why)
		}
	}
}

// Round 122: a failed job's pods removed (its cleanup): the Fix line
// doesn't send you to open them.
func TestJobFixWhenItsPodsAreGone(t *testing.T) {
	for _, s := range []string{"Job failed", "Retrying"} {
		gone := appFix(&App{Status: Status{Short: s, Why: "Job has reached the specified backoff limit"}}, false)
		if strings.Contains(gone, "l on the failed one") || !strings.Contains(gone, "pods are gone") {
			t.Errorf("%s, pods gone: %q", s, gone)
		}
		kept := appFix(&App{Status: Status{Short: s}, Leftovers: 2, CrashPod: "j-1"}, false)
		if !strings.Contains(kept, "l on the failed one") {
			t.Errorf("%s, pods kept: %q", s, kept)
		}
	}
}

// Round 122: a failed job's time stayed on the moment it failed
// (FailureTarget) while its last pod stopped, then jumped to when it was
// marked Failed; it keeps the first.
func TestFailedJobTimeIsSteady(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	target, failed := time.Now().Add(-time.Minute), time.Now().Add(-15*time.Second)
	cond := func(typ string, at time.Time) any {
		return map[string]any{"type": typ, "status": "True", "reason": "BackoffLimitExceeded", "message": "Job has reached the specified backoff limit", "lastTransitionTime": at.Format(time.RFC3339)}
	}
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "slowjob", "namespace": "r122", "uid": "j122", "creationTimestamp": time.Now().Add(-5 * time.Minute).Format(time.RFC3339)},
		"status":   map[string]any{"failed": int64(1), "conditions": []any{cond("FailureTarget", target), cond("Failed", failed)}}}}
	e := enrich.New(jobStore{kube.Jobs: {job}}, res, nil)
	why := ""
	for _, a := range Apps(e, nil, nil, nil, []*unstructured.Unstructured{job}, nil) {
		if a.Status.Short == "Job failed" {
			why = a.Status.Why
		}
	}
	if !strings.Contains(why, "failed 1m ago") {
		t.Errorf("%q", why)
	}
}

// Round 123: one kill for memory 44m ago, in an app whose other containers
// restarted 39 times: the Fix says "if this repeats", not "keeps being
// killed"; kills that repeat say so.
func TestMemoryFixFollowsTheKills(t *testing.T) {
	once := appFix(&App{Status: Status{Short: "Hit memory limit", Why: "ran out of memory 44m ago and restarted"}, Restarts: 41}, false)
	if strings.Contains(once, "keeps being killed") || !strings.Contains(once, "if this repeats") {
		t.Errorf("one kill: %q", once)
	}
	again := appFix(&App{Status: Status{Short: "Out of memory", Why: "main keeps running out of memory"}, Restarts: 3}, false)
	if !strings.Contains(again, "keeps being killed") {
		t.Errorf("repeating: %q", again)
	}
}

// Round 123: a failed job's own box (gj) once its pods are gone says so,
// as home does, instead of sending you to open them.
func TestFailedJobBoxWhenItsPodsAreGone(t *testing.T) {
	res := kube.NewResources([]kube.Resource{{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "jdel", "namespace": "r123", "uid": "j123"},
		"status": map[string]any{"failed": int64(1), "conditions": []any{map[string]any{"type": "Failed", "status": "True",
			"reason": "BackoffLimitExceeded", "message": "Job has reached the specified backoff limit", "lastTransitionTime": time.Now().Add(-time.Minute).Format(time.RFC3339)}}}}}
	e := enrich.New(jobStore{kube.Jobs: {job}}, res, nil)
	got := strings.Join(workloadProblem(e, job), "\n")
	if strings.Contains(got, "l on the failed one") || !strings.Contains(got, "pods are gone") {
		t.Errorf("%s", got)
	}
}
