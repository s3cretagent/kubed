package enrich

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/kube"
)

// HomeNeeds are the resources the home dashboard reads.
var HomeNeeds = []schema.GroupVersionResource{
	kube.Pods, kube.Deployments, kube.StatefulSets, kube.DaemonSets, kube.Nodes, kube.Jobs,
	kube.ExternalSecrets, kube.Applications, kube.Events,
}

// Pending pods younger than this are still starting, not stuck.
const stuckPending = 5 * time.Minute

// recentWindow is how far back failed jobs, OOM kills and warning events count.
const recentWindow = time.Hour

// Problem is one thing on the home dashboard that needs a look.
type Problem struct {
	GVR       schema.GroupVersionResource
	Kind      string // "Pod", "Node"…
	Namespace string
	Name      string
	What      string // "CrashLoopBackOff", "0/2 ready"…
	Since     time.Time
	Health    Health // HealthWarn or HealthBad
}

// Summary is the dashboard's headline numbers.
type Summary struct {
	Pods, PodsRunning int
	Nodes, NodesReady int
	WarningEvents     int // last hour
	Problems          []Problem
}

// Home finds what is wrong in the cluster right now, worst first.
func (e *Enricher) Home(now time.Time) Summary {
	var s Summary
	add := func(gvr schema.GroupVersionResource, o *unstructured.Unstructured, what string, since time.Time, h Health) {
		s.Problems = append(s.Problems, Problem{GVR: gvr, Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName(),
			What: what, Since: since, Health: h})
	}

	for _, p := range e.list(kube.Pods, "") {
		s.Pods++
		phase := str(p, "status", "phase")
		if phase == "Running" {
			s.PodsRunning++
		}
		created := p.GetCreationTimestamp().Time
		switch reason := podProblem(p); {
		case reason != "" && !isJobPod(p):
			add(kube.Pods, p, reason, created, HealthBad)
		case phase == "Pending" && now.Sub(created) > stuckPending:
			add(kube.Pods, p, "Pending for "+now.Sub(created).Round(time.Minute).String(), created, HealthWarn)
		default:
			if when, ok := recentOOM(p, now); ok {
				add(kube.Pods, p, "OOMKilled", when, HealthBad)
			}
		}
	}

	for _, gvr := range []schema.GroupVersionResource{kube.Deployments, kube.StatefulSets, kube.DaemonSets} {
		for _, o := range e.list(gvr, "") {
			if note, h := workloadHealth(o); h == HealthBad || h == HealthWarn {
				add(gvr, o, note, o.GetCreationTimestamp().Time, h)
			}
		}
	}

	for _, n := range e.list(kube.Nodes, "") {
		s.Nodes++
		c, ok := Condition(n, "Ready")
		if ok && c.Status == "True" {
			s.NodesReady++
		} else {
			add(kube.Nodes, n, "NotReady", n.GetCreationTimestamp().Time, HealthBad)
		}
		for _, p := range []string{"MemoryPressure", "DiskPressure", "PIDPressure"} {
			if c, ok := Condition(n, p); ok && c.Status == "True" {
				add(kube.Nodes, n, p, n.GetCreationTimestamp().Time, HealthBad)
			}
		}
		if u, _, _ := unstructured.NestedBool(n.Object, "spec", "unschedulable"); u {
			add(kube.Nodes, n, "cordoned", n.GetCreationTimestamp().Time, HealthWarn)
		}
	}

	for _, j := range e.list(kube.Jobs, "") {
		if c, ok := Condition(j, "Failed"); ok && c.Status == "True" {
			if when := JobFailedAt(j); now.Sub(when) < recentWindow {
				add(kube.Jobs, j, "failed: "+c.Reason, when, HealthBad)
			}
		}
	}

	for _, es := range e.list(kube.ExternalSecrets, "") {
		if x := ExternalSecret(es); x.Ready == "False" {
			add(kube.ExternalSecrets, es, "not syncing: "+x.Reason, x.RefreshedAt, HealthBad)
		}
	}

	for _, app := range e.list(kube.Applications, "") {
		a := Application(app)
		switch {
		case a.Health == "Degraded" || a.Health == "Missing":
			add(kube.Applications, app, "ArgoCD "+a.Health, app.GetCreationTimestamp().Time, HealthBad)
		case a.Sync == "OutOfSync":
			add(kube.Applications, app, "ArgoCD OutOfSync", app.GetCreationTimestamp().Time, HealthWarn)
		}
	}

	for _, ev := range e.list(kube.Events, "") {
		if IsWarning(ev) && now.Sub(EventTime(ev)) < recentWindow {
			s.WarningEvents++
		}
	}

	sort.SliceStable(s.Problems, func(i, j int) bool {
		if s.Problems[i].Health != s.Problems[j].Health {
			return s.Problems[i].Health > s.Problems[j].Health
		}
		return s.Problems[i].Since.After(s.Problems[j].Since)
	})
	return s
}

// isJobPod: a failed pod of a Job is reported through its Job instead.
func isJobPod(p *unstructured.Unstructured) bool {
	for _, o := range p.GetOwnerReferences() {
		if o.Kind == "Job" {
			return true
		}
	}
	return false
}

func recentOOM(p *unstructured.Unstructured, now time.Time) (time.Time, bool) {
	statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
	for _, st := range statuses {
		m, _ := st.(map[string]any)
		reason, _, _ := unstructured.NestedString(m, "lastState", "terminated", "reason")
		finished, _, _ := unstructured.NestedString(m, "lastState", "terminated", "finishedAt")
		if reason != "OOMKilled" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, finished); err == nil && now.Sub(t) < recentWindow {
			return t, true
		}
	}
	return time.Time{}, false
}

func conditionTime(o *unstructured.Unstructured, condType string) time.Time {
	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		if m["type"] == condType {
			t, _ := time.Parse(time.RFC3339, fmt.Sprint(m["lastTransitionTime"]))
			return t
		}
	}
	return time.Time{}
}

// EventTime is when an event last happened.
func EventTime(o *unstructured.Unstructured) time.Time {
	// The newer events API keeps a repeating event's first time in
	// eventTime and its latest in series.lastObservedTime: the latest wins.
	var latest time.Time
	for _, f := range [][]string{{"lastTimestamp"}, {"series", "lastObservedTime"}, {"eventTime"}} {
		v, _, _ := unstructured.NestedString(o.Object, f...)
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil && t.After(latest) {
			latest = t
		}
	}
	if latest.IsZero() {
		return o.GetCreationTimestamp().Time
	}
	return latest
}

// EventCount is how many times an event happened: its count, or its
// series' count in the newer events API.
func EventCount(o *unstructured.Unstructured) int64 {
	n, _, _ := unstructured.NestedInt64(o.Object, "count")
	if s, _, _ := unstructured.NestedInt64(o.Object, "series", "count"); s > n {
		n = s
	}
	return max(n, 1)
}

// LatestEvent returns the newest event with reason about an object, e.g.
// the last "Unhealthy" (probe failure) of a pod.
func (e *Enricher) LatestEvent(ns, kind, name, reason string) (string, time.Time, bool) {
	var msg string
	var when time.Time
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") != reason || str(ev, "involvedObject", "name") != name || str(ev, "involvedObject", "kind") != kind {
			continue
		}
		if t := EventTime(ev); t.After(when) {
			msg, when = str(ev, "message"), t
		}
	}
	return msg, when, msg != ""
}

// killedByCheck remembers, per container ("ns/pod/container"), the check
// that killed it and its last failure in words once a kill by it was
// seen: Kubernetes rate-limits a busy pod's events, so later kills leave
// none.
var killedByCheck sync.Map

type checkKill struct{ check, words string }

// KilledByCheck reports that Kubernetes killed a pod's container because
// its alive (liveness) or startup check failed, for the stop that ended
// at since with exitCode. check names it ("alive check" or "startup
// check"); words is its last failure in words ("alive check failed:
// answered 404").
//
// A kill by the check ends the container as a stop does (exit 0, or 137
// or 143 from the signal); any other exit is the app's own crash. A
// startup check failing is normal while the app starts, so only its kill
// event says it killed the container; an alive check failing as it
// stopped does too.
func (e *Enricher) KilledByCheck(ns, pod, container string, since time.Time, exitCode int32) (check, words string, ok bool) {
	if exitCode != 0 && exitCode != 137 && exitCode != 143 {
		return "", "", false
	}
	key := ns + "/" + pod + "/" + container
	var killedAt time.Time
	failed := map[string]string{} // per check, its newest failure in words
	failedAt := map[string]time.Time{}
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "involvedObject", "kind") != "Pod" || str(ev, "involvedObject", "name") != pod {
			continue
		}
		msg := str(ev, "message")
		switch str(ev, "reason") {
		case "Killing":
			// Any kill by the check still kept: the kill event stops coming
			// after a few, the kills don't. The newest kill names the check.
			for probe, name := range map[string]string{"liveness": "alive check", "startup": "startup check"} {
				if strings.Contains(msg, "failed "+probe+" probe") && strings.Contains(msg, "Container "+container+" ") {
					if t := EventTime(ev); !ok || t.After(killedAt) {
						check, killedAt = name, t
					}
					ok = true
				}
			}
		case "Unhealthy":
			name := ""
			switch {
			case strings.HasPrefix(msg, "Liveness probe"):
				name = "alive check"
			case strings.HasPrefix(msg, "Startup probe"):
				name = "startup check"
			}
			if name == "" || !strings.Contains(str(ev, "involvedObject", "fieldPath"), "{"+container+"}") {
				continue // another check, or another container's (a probe's event names its container)
			}
			t := EventTime(ev)
			if t.After(failedAt[name]) {
				failed[name], failedAt[name] = MessageWords(strings.Join(strings.Fields(msg), " ")), t
			}
		}
	}
	if !ok && !failedAt["alive check"].IsZero() && !failedAt["alive check"].Before(since.Add(-90*time.Second)) {
		check, ok = "alive check", true // failing as it stopped (no check runs while it's down), or failing again since
	}
	if ok {
		words = failed[check]
		killedByCheck.Store(key, checkKill{check, words})
		return check, words, true
	}
	if k, seen := killedByCheck.Load(key); seen { // its events have stopped coming, its kills haven't
		return k.(checkKill).check, k.(checkKill).words, true
	}
	return "", "", false
}

// JobFailedAt is when a failed Job failed: the moment it was marked failing
// (FailureTarget) when Kubernetes set that first, else when it was marked
// Failed (after its last pods stopped). Every screen says the one time.
func JobFailedAt(j *unstructured.Unstructured) time.Time {
	failed := conditionTime(j, "Failed")
	if c, ok := Condition(j, "FailureTarget"); ok && c.Status == "True" {
		if at := conditionTime(j, "FailureTarget"); !at.IsZero() && (failed.IsZero() || at.Before(failed)) {
			return at
		}
	}
	return failed
}
