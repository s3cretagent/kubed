package enrich

import (
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// CronLastRun is when a CronJob last started a run: its schedule's last
// run, or a run started by hand (which Kubernetes doesn't record on the
// CronJob), whichever is later.
func (e *Enricher) CronLastRun(cj *unstructured.Unstructured) time.Time {
	last, _ := time.Parse(time.RFC3339, str(cj, "status", "lastScheduleTime"))
	for _, j := range e.list(kube.Jobs, cj.GetNamespace()) {
		if ownedBy(j, cj) && j.GetCreationTimestamp().After(last) {
			last = j.GetCreationTimestamp().Time
		}
	}
	return last
}

// LaterRunWorked reports that a kept run of run's CronJob, started after run, is
// Complete. finished is when the newest such run ended. Only runs still
// kept count: a later success that was removed says nothing here.
func (e *Enricher) LaterRunWorked(run *unstructured.Unstructured) (finished time.Time, ok bool) {
	cj := e.TopOwner(run)
	if run.GetKind() != "Job" || cj.GetKind() != "CronJob" {
		return time.Time{}, false
	}
	for _, j := range e.list(kube.Jobs, run.GetNamespace()) {
		if j.GetUID() == run.GetUID() || !ownedBy(j, cj) || !j.GetCreationTimestamp().After(run.GetCreationTimestamp().Time) {
			continue
		}
		c, found := Condition(j, "Complete")
		done, err := time.Parse(time.RFC3339, str(j, "status", "completionTime"))
		if found && c.Status == "True" && err == nil && done.After(finished) {
			finished, ok = done, true
		}
	}
	return finished, ok
}

// CronLastFinished is the newest finished run of a CronJob still kept:
// whether it failed. ok is false when none is kept.
func (e *Enricher) CronLastFinished(cj *unstructured.Unstructured) (failed, ok bool) {
	var newest *unstructured.Unstructured
	isTrue := func(j *unstructured.Unstructured, typ string) bool {
		c, found := Condition(j, typ)
		return found && c.Status == "True"
	}
	// One failing or done with its last pods stopping (FailureTarget,
	// SuccessCriteriaMet) has finished, as home and its box say.
	failedRun := func(j *unstructured.Unstructured) bool { return isTrue(j, "Failed") || isTrue(j, "FailureTarget") }
	for _, j := range e.list(kube.Jobs, cj.GetNamespace()) {
		if !ownedBy(j, cj) || (!isTrue(j, "Complete") && !isTrue(j, "SuccessCriteriaMet") && !failedRun(j)) {
			continue // not its run, or still running
		}
		if newest == nil || j.GetCreationTimestamp().After(newest.GetCreationTimestamp().Time) {
			newest = j
		}
	}
	if newest == nil {
		return false, false
	}
	// Kubernetes keeps failed runs longer than good ones: a failure older
	// than the last success is history, not the state now. Unless that
	// success is a kept run that started before it (runs side by side):
	// then the run started last still failed, as home and the box say.
	if last, err := time.Parse(time.RFC3339, str(cj, "status", "lastSuccessfulTime")); err == nil &&
		last.After(newest.GetCreationTimestamp().Time) && !e.olderRunEndedAt(cj, newest, last) {
		return false, true
	}
	return failedRun(newest), true
}

// olderRunEndedAt: a kept run of cj, started before run, succeeded at at
// (or later): the success the CronJob records is that run's.
func (e *Enricher) olderRunEndedAt(cj, run *unstructured.Unstructured, at time.Time) bool {
	for _, j := range e.list(kube.Jobs, cj.GetNamespace()) {
		done, err := time.Parse(time.RFC3339, str(j, "status", "completionTime"))
		if err == nil && ownedBy(j, cj) && j.GetCreationTimestamp().Time.Before(run.GetCreationTimestamp().Time) && !done.Before(at) {
			return true
		}
	}
	return false
}

// CronLastRunGone reports that a CronJob's last run started after its last success
// (or it has never succeeded), none of its runs is going on, and no run
// that finished is kept. So that run didn't succeed (a success would be
// recorded) and was removed: by its history limit, or by hand.
func (e *Enricher) CronLastRunGone(cj *unstructured.Unstructured) bool {
	if !e.Synced(kube.Jobs) { // its runs not listed yet: a kept one would look removed
		return false
	}
	last := e.CronLastRun(cj)
	if last.IsZero() || e.CronRunning(cj) > 0 { // one going on, started by hand too (not in status.active)
		return false
	}
	if active, _, _ := unstructured.NestedSlice(cj.Object, "status", "active"); len(active) > 0 {
		return false
	}
	if _, kept := e.CronLastFinished(cj); kept {
		return false
	}
	ok, _ := time.Parse(time.RFC3339, str(cj, "status", "lastSuccessfulTime"))
	return last.Sub(ok) > 2*time.Minute // a run started a moment before its success was recorded is not one
}

// PodsOf are the pods a workload runs, through any chain of owners
// (a Deployment's ReplicaSets, a Job's pods).
func (e *Enricher) PodsOf(o *unstructured.Unstructured) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, p := range e.list(kube.Pods, o.GetNamespace()) {
		for c := e.controller(p); c != nil; c = e.controller(c) {
			if c.GetUID() == o.GetUID() && c.GetKind() == o.GetKind() {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

// CronRunning counts a CronJob's runs going on now, including ones
// started by hand (status.active lists only scheduled ones).
func (e *Enricher) CronRunning(cj *unstructured.Unstructured) int {
	n := 0
	for _, j := range e.list(kube.Jobs, cj.GetNamespace()) {
		if !ownedBy(j, cj) {
			continue
		}
		done := false
		for _, typ := range []string{"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet"} { // the last two: ended, its last pods stopping
			if c, ok := Condition(j, typ); ok && c.Status == "True" {
				done = true
			}
		}
		if !done {
			n++
		}
	}
	return n
}

// LatestWarning is the newest warning event about an object, in words,
// with when it was; ok is false when events have none (they last about
// an hour).
func (e *Enricher) LatestWarning(o *unstructured.Unstructured) (msg string, at time.Time, ok bool) {
	for _, ev := range e.list(kube.Events, o.GetNamespace()) {
		if !IsWarning(ev) || str(ev, "involvedObject", "name") != o.GetName() ||
			str(ev, "involvedObject", "kind") != o.GetKind() {
			continue
		}
		if t := EventTime(ev); !ok || t.After(at) {
			msg, at, ok = MessageWords(strings.Join(strings.Fields(str(ev, "message")), " ")), t, true
		}
	}
	return msg, at, ok
}

// CronRuns are a CronJob's runs still kept, newest first.
func (e *Enricher) CronRuns(cj *unstructured.Unstructured) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, j := range e.list(kube.Jobs, cj.GetNamespace()) {
		if ownedBy(j, cj) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].GetCreationTimestamp().After(out[b].GetCreationTimestamp().Time) })
	return out
}

// List is what the cache holds of a kind in a namespace ("" = all).
func (e *Enricher) List(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	return e.list(gvr, ns)
}

// LatestWarningFor is the newest warning event of one reason about an
// object ("FailedCreate"), in words; ok is false when there is none.
func (e *Enricher) LatestWarningFor(o *unstructured.Unstructured, reason string) (msg string, at time.Time, ok bool) {
	for _, ev := range e.list(kube.Events, o.GetNamespace()) {
		if str(ev, "reason") != reason || str(ev, "involvedObject", "name") != o.GetName() ||
			str(ev, "involvedObject", "kind") != o.GetKind() {
			continue
		}
		if t := EventTime(ev); !ok || t.After(at) {
			msg, at, ok = MessageWords(strings.Join(strings.Fields(str(ev, "message")), " ")), t, true
		}
	}
	return msg, at, ok
}

// Synced reports that the cache has finished loading a kind, so an empty list means
// none (not "not loaded yet").
func (e *Enricher) Synced(gvr schema.GroupVersionResource) bool {
	if g, ok := e.served(gvr); ok {
		return e.store.Synced(g)
	}
	return false
}

// SyncedIn reports whether the first list for gvr in namespace ns has
// completed (as Synced, for a store that doesn't keep namespaces apart).
func (e *Enricher) SyncedIn(gvr schema.GroupVersionResource, ns string) bool {
	g, ok := e.served(gvr)
	if !ok {
		return false
	}
	if s, ok := e.store.(interface {
		SyncedIn(schema.GroupVersionResource, string) bool
	}); ok {
		return s.SyncedIn(g, ns)
	}
	return e.store.Synced(g)
}

// Get is one object from the cache, by namespace and name.
func (e *Enricher) Get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	return e.get(gvr, ns, name)
}

// OwnedPods are a workload's pods, through any chain of owners, from an
// index rebuilt at most once a second (as PodsOf, but cheap for lists).
func (e *Enricher) OwnedPods(o *unstructured.Unstructured) []*unstructured.Unstructured {
	e.podIndexMu.Lock()
	defer e.podIndexMu.Unlock()
	if e.ownedPods == nil || time.Since(e.podIndexAt) > time.Second {
		idx := map[types.UID][]*unstructured.Unstructured{}
		for _, p := range e.list(kube.Pods, "") {
			for c := e.controller(p); c != nil; c = e.controller(c) {
				idx[c.GetUID()] = append(idx[c.GetUID()], p)
			}
		}
		e.ownedPods, e.podIndexAt = idx, time.Now()
	}
	return e.ownedPods[o.GetUID()]
}

// CronRunsStuck counts a CronJob's runs going on now whose pods can't
// start (an image that can't be pulled, a config that is missing): they
// never finish, so the last finished run says nothing about them. why is
// the first one's reason, in words.
func (e *Enricher) CronRunsStuck(cj *unstructured.Unstructured) (n int, why string) {
	for _, j := range e.CronRuns(cj) {
		if done := func() bool {
			for _, typ := range []string{"Complete", "Failed"} {
				if c, ok := Condition(j, typ); ok && c.Status == "True" {
					return true
				}
			}
			return false
		}(); done {
			continue
		}
		for _, p := range e.OwnedPods(j) {
			if p.GetDeletionTimestamp() != nil {
				continue // being deleted (ctrl-d on it): gone in a moment
			}
			statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
			inits, _, _ := unstructured.NestedSlice(p.Object, "status", "initContainerStatuses")
			stuck := ""
			for _, s := range append(statuses, inits...) {
				m, _ := s.(map[string]any)
				switch reason, _, _ := unstructured.NestedString(m, "state", "waiting", "reason"); reason {
				case "ImagePullBackOff", "InvalidImageName": // backing off: failed again and again, not one blip
					stuck = "can't pull its image"
				case "ErrImagePull": // between back-offs: stuck too once it has tried a while
					if time.Since(p.GetCreationTimestamp().Time) > 30*time.Second {
						stuck = "can't pull its image"
					}
				case "CreateContainerConfigError", "CreateContainerError":
					stuck = "can't start (a missing secret or config)"
				}
			}
			if stuck != "" {
				n++
				if why == "" {
					why = stuck
				}
				break
			}
		}
	}
	return n, why
}
