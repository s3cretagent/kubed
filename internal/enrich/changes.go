package enrich

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// ChangeWindow is how far back "what changed" looks.
const ChangeWindow = 24 * time.Hour

// ChangeNeeds are the resources the timeline reads.
var ChangeNeeds = []schema.GroupVersionResource{
	kube.ReplicaSets, kube.Applications, kube.ExternalSecrets, kube.Nodes, kube.Events, kube.Pods, kube.Jobs,
	kube.CronJobs, kube.StatefulSets, kube.DaemonSets, kube.ControllerRevisions,
}

// Change is one entry of the "what changed" timeline.
type Change struct {
	When      time.Time
	What      string // "rollout", "sync", "scale", "node", "crash", "secret", "new pod"
	Namespace string
	Object    string // "deployment/auth-app-server"
	Detail    string
	Health    Health
	owner     string // new pods: what runs them, so pods of one workload group together
	times     int64  // an event's own count: how often it happened
	// After: it happened at When or later. Nothing dates a go-back any
	// more, only that it came after the versions it came back over were
	// made (goBackBound), as the versions list and Deployed say too.
	After bool
	rev   int // a Deployment's revision: of rows at the same time, the higher went out later
	// Objects: every workload of a grouped rollout ("deployment/a",
	// "deployment/b"), when Object only names the first and a count.
	Objects []string
}

// scaledUpRS reads the ReplicaSet out of a Deployment's scaling event.
// Many in a short time are merged into one, prefixed "(combined from
// similar events): ".
var scaledUpRS = regexp.MustCompile(`^(?:\(combined from similar events\): )?Scaled up replica set (\S+)`)

// scaledFromZero: a scale-up that brought a ReplicaSet back from no pods
// ("from 0 to 2", or "to 2 from 0" in older clusters): a version going
// live (again), not copies added to one running.
var scaledFromZero = regexp.MustCompile(`(?:from 0 to \d+|to \d+ from 0)\s*$`)

// firstTimeOut is the row for a version's first time out, when it has
// since been brought back: the number it had then, and what it changed
// against before, the version it replaced then (ranBefore; nil when no
// longer kept: then only its images, as a restart stamp can't tell a
// restart from a change made seconds after one).
func firstTimeOut(rs, before *unstructured.Unstructured, held []int) string {
	label := "first went out"
	if len(held) > 1 {
		label = fmt.Sprintf("revision %d (later brought back)", slices.Min(held))
	}
	images := strings.Join(templateImages(rs), ", ")
	switch {
	case before != nil && restartOnly(before, rs):
		return label + ": restarted, same images (" + images + ")"
	case before != nil && strings.Join(templateImages(before), ", ") == images:
		return label + ": settings changed, same images (" + images + ")"
	}
	return label + ": " + images
}

// ranBefore is the version that went out under number n-1, the one a
// version given n replaced, while it is still kept: a go-back renumbers a
// version and keeps its old numbers in its history (held), so the
// numbers say which. nil when it is gone: no other version can stand in
// for it (the one made just before may have been replaced by a go-back in
// between, and comparing with it calls a change a restart or the reverse).
func ranBefore(all []*unstructured.Unstructured, held map[*unstructured.Unstructured][]int, n int) *unstructured.Unstructured {
	for _, o := range all {
		if slices.Contains(held[o], n-1) {
			return o
		}
	}
	return nil
}

// restartOnly: version b differs from a only by a restart (the same
// images, a new restartedAt stamp).
func restartOnly(a, b *unstructured.Unstructured) bool {
	stamp := func(o *unstructured.Unstructured) string {
		return str(o, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	}
	return stamp(a) != stamp(b) && templateSettings(a) == templateSettings(b)
}

// interestingEvents are the event reasons worth a line on the timeline.
var interestingEvents = map[string]string{
	"SuccessfulRescale": "scale", // HPA changed replicas
	// RegisteredNode is left out: the node's own "joined" line says more.
	"RemovingNode": "node",    // node left
	"NodeNotReady": "node",    // node stopped answering
	"Evicted":      "evicted", // pod pushed off its node
	"OOMKilling":   "memory",  // kernel OOM on the node
	"BackOff":      "crash",   // container restarting in a loop
	"FailedMount":  "volume",  // volume / secret / configmap missing
	"Unhealthy":    "health",  // probe failing
	"Descheduled":  "evicted", // moved by the descheduler
	"Preempted":    "evicted", // pushed out for a more important pod
}

// Changes builds the timeline for a namespace ("" = whole cluster), newest
// first, over the last ChangeWindow. app, when set, also pulls in that
// ArgoCD app's syncs (its Application often names no namespace).
//
// keep, when set, keeps only changes to those objects ("kind/name"); it is
// applied before rollouts are grouped, so an app's own rollout survives
// even when it went out with 40 others.
func (e *Enricher) Changes(ns, appName string, now time.Time, keep func(object string) bool) []Change {
	return e.changesOver(ns, appName, now, ChangeWindow, keep)
}

// ShipWindow is how far back "what shipped" looks: a week.
const ShipWindow = 7 * 24 * time.Hour

// Shipped are the new versions that went out over the last week (or
// window), newest first: rollouts that changed something, not restarts.
func (e *Enricher) Shipped(ns string, now time.Time, window time.Duration) []Change {
	var out []Change
	for _, c := range e.changesOver(ns, "", now, window, nil) {
		// A stateful set's or daemon set's first version is its "created"
		// row ("new stateful set running db:1"): it shipped then.
		if _, images, ok := strings.Cut(c.Detail, " running "); c.What == "created" && ok {
			c.Detail = "first version: " + images
			out = append(out, c)
			continue
		}
		if c.What != "rollout" || strings.Contains(c.Detail, "restarted, same images") {
			continue
		}
		if len(c.Objects) == 0 || len(c.Objects) > 3 {
			// One release to many workloads is one row ("(13 more) web…");
			// w on the namespace lists them all.
			out = append(out, c)
			continue
		}
		// A few: one row each, so every one is named.
		images := groupImages(c.Detail)
		for _, o := range c.Objects {
			one := c
			one.Object, one.Objects, one.Detail = o, nil, "new version: "+images
			out = append(out, one)
		}
	}
	return out
}

// kindWords names a kind in words: "cron job", "stateful set".
func kindWords(kind string) string {
	switch kind {
	case "CronJob":
		return "cron job"
	case "StatefulSet":
		return "stateful set"
	case "DaemonSet":
		return "daemon set (one pod on each node)"
	}
	return kind
}

// scaleUpWords reads a scaling event's counts: "from 2 to 3" (or "to 3
// from 2"), up or down.
var scaleUpWords = regexp.MustCompile(`(?:from (\d+) to (\d+)|to (\d+) from (\d+))\s*$`)

// firstImages is what a StatefulSet or DaemonSet ran when it was made: the
// version made with it, not what it runs today (it may have changed since).
// "" when that version is no longer kept.
func (e *Enricher) firstImages(o *unstructured.Unstructured) string {
	if o.GetKind() == "CronJob" {
		return ""
	}
	var first *Version
	for _, v := range e.revisionRollout(o).All {
		if first == nil || v.Created.Before(first.Created) {
			first = v
		}
	}
	if first == nil || first.Created.Sub(o.GetCreationTimestamp().Time).Abs() > time.Minute {
		return ""
	}
	return strings.Join(first.Images, ", ")
}

// rescaleSize reads an autoscaler's SuccessfulRescale: "New size: 3; reason: ...".
var rescaleSize = regexp.MustCompile(`^New size: (\d+);`)

// handScales are a namespace's Deployments scaled other than by a rollout
// or an autoscaler, from ScalingReplicaSet events (kept about an hour).
func (e *Enricher) handScales(ns string) []Change {
	type scale struct {
		at       time.Time
		dep, rs  string
		ns       string
		from, to int
		up       bool
		combined bool // a merged event: only its last scale's words are left
	}
	var all []scale
	type rescale struct {
		at   time.Time
		size int
	}
	// An autoscaler scales what its scaleTargetRef names (KEDA's keda-hpa-x
	// too); one not seen is taken to share its target's name.
	targets := map[string]string{} // ns/autoscaler → ns/deployment it scales
	for _, h := range e.list(kube.HPAs, ns) {
		target := ""
		if str(h, "spec", "scaleTargetRef", "kind") == "Deployment" {
			target = h.GetNamespace() + "/" + str(h, "spec", "scaleTargetRef", "name")
		}
		targets[h.GetNamespace()+"/"+h.GetName()] = target
	}
	autoscaled := map[string][]rescale{} // ns/deployment → its autoscaler's rescales ("New size: N")
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") == "SuccessfulRescale" {
			key := ev.GetNamespace() + "/" + str(ev, "involvedObject", "name")
			if target, ok := targets[key]; ok {
				key = target
			}
			msg := strings.TrimPrefix(str(ev, "message"), "(combined from similar events): ")
			if n := rescaleSize.FindStringSubmatch(msg); n != nil && key != "" {
				size, _ := strconv.Atoi(n[1])
				autoscaled[key] = append(autoscaled[key], rescale{EventTime(ev), size})
			}
			continue
		}
		if str(ev, "reason") != "ScalingReplicaSet" || str(ev, "involvedObject", "kind") != "Deployment" {
			continue
		}
		raw := str(ev, "message")
		msg := strings.TrimPrefix(raw, "(combined from similar events): ")
		up := strings.HasPrefix(msg, "Scaled up replica set ")
		if !up && !strings.HasPrefix(msg, "Scaled down replica set ") {
			continue
		}
		n := scaleUpWords.FindStringSubmatch(msg) // the counts read the same either way
		if n == nil {
			continue
		}
		from, to := n[1], n[2]
		if from == "" {
			from, to = n[4], n[3]
		}
		f, _ := strconv.Atoi(from)
		t, _ := strconv.Atoi(to)
		rest := strings.TrimPrefix(strings.TrimPrefix(msg, "Scaled up replica set "), "Scaled down replica set ")
		all = append(all, scale{EventTime(ev), str(ev, "involvedObject", "name"), firstWord(rest), ev.GetNamespace(), f, t, up, raw != msg || EventCount(ev) > 1})
	}
	// The version each Deployment runs now: scaling by hand moves it; a
	// rollout's steps move the others.
	current := map[string]string{} // ns/deployment → its current ReplicaSet
	for _, rs := range e.list(kube.ReplicaSets, ns) {
		if owner := e.controller(rs); owner != nil && owner.GetKind() == "Deployment" &&
			rs.GetAnnotations()["deployment.kubernetes.io/revision"] == owner.GetAnnotations()["deployment.kubernetes.io/revision"] {
			current[rs.GetNamespace()+"/"+owner.GetName()] = rs.GetName()
		}
	}
	var out []Change
	for _, sc := range all {
		key := sc.ns + "/" + sc.dep
		isCurrent := current[key] == sc.rs
		if isCurrent && !sc.up && e.backSince(sc.ns, sc.dep, sc.rs, sc.at) {
			isCurrent = false // running now only since a go-back brought it back after this scale down
		}
		if (!isCurrent || sc.up) && e.nearRollout(sc.ns, sc.dep, sc.at) {
			// Within about 2 minutes of a version going out: its own steps
			// (a go-back scales the version it brings back up one by one;
			// an older one is scaled down), part of it, as the guide says.
			// The version running now scaled down is never a rollout's step.
			continue
		}
		if current[key] != sc.rs && sc.combined {
			// Not the version running now, and a merged event: it can't be
			// told apart from a rollout's steps. A single event can; a
			// rollout's own steps are caught below (another version scaled
			// the other way near it).
			continue
		}
		if sc.up && sc.from == 0 {
			// From nothing: a scale back up only after a scale down to 0 of
			// the same version (not a new deployment, nor a version coming
			// back with a go-back).
			downed := false
			for _, o := range all {
				downed = downed || (o.ns == sc.ns && o.rs == sc.rs && !o.up && o.to == 0 && o.at.Before(sc.at))
			}
			// A merged event can swallow the scale down: the version running
			// now, long after it went out and with no rollout near, was at 0.
			if !downed && sc.combined && current[key] == sc.rs && !e.nearRollout(sc.ns, sc.dep, sc.at) {
				if rs, ok := e.get(kube.ReplicaSets, sc.ns, sc.rs); ok && sc.at.Sub(rs.GetCreationTimestamp().Time) > 2*time.Minute {
					downed = true
				}
			}
			if !downed {
				continue
			}
		}
		rolling := false
		for _, o := range all { // another version scaled the other way near it: a rollout
			rolling = rolling || (o.ns == sc.ns && o.dep == sc.dep && o.rs != sc.rs && o.up != sc.up && o.at.Sub(sc.at).Abs() < 2*time.Minute)
		}
		for _, r := range autoscaled[key] {
			// The autoscaler's own line says it: its new size, within
			// seconds (the two lines come out together).
			rolling = rolling || (r.size == sc.to && r.at.Sub(sc.at).Abs() <= 15*time.Second)
			// Mid-rollout it scales every version the same way at once, each
			// event with that version's own size, together making its new
			// size plus the surge (not the new size): two or more versions
			// scaled the same way within seconds of its rescale are its own.
			if rolling || r.at.Sub(sc.at).Abs() > 15*time.Second {
				continue
			}
			versions := map[string]bool{}
			for _, o := range all {
				if o.ns == sc.ns && o.dep == sc.dep && o.up == sc.up && r.at.Sub(o.at).Abs() <= 15*time.Second {
					versions[o.rs] = true
				}
			}
			rolling = len(versions) > 1
		}
		if rolling {
			continue
		}
		detail, health := fmt.Sprintf("scaled from %d to %d", sc.from, sc.to), HealthNone
		if sc.to == 0 {
			detail, health = detail+": nothing runs", HealthWarn
			upAgain := false // its scale back up seen
			for _, o := range all {
				// One listed below: not near a rollout (whose own line says it).
				upAgain = upAgain || (o.ns == sc.ns && o.rs == sc.rs && o.up && o.at.After(sc.at) && !e.nearRollout(o.ns, o.dep, o.at))
			}
			// Running again now, its scale back up merged into an older event
			// (so not when): said, rather than "nothing runs" as the last word.
			if rs, ok := e.get(kube.ReplicaSets, sc.ns, sc.rs); ok && !upAgain && isCurrent && num(rs, "spec", "replicas") > 0 && num(rs, "status", "replicas") > 0 { // asked for again, not only not yet emptied
				detail, health = fmt.Sprintf("scaled from %d to 0, then back up (Kubernetes merged that event, so not when)", sc.from), HealthNone
			}
		}
		out = append(out, Change{When: sc.at, What: "scale", Namespace: sc.ns, Object: "deployment/" + sc.dep, Detail: detail, Health: health})
	}
	return out
}

// nearRollout: a Deployment's version went out within 2 minutes of at
// (a ReplicaSet made, its template written, or a version seen coming back).
func (e *Enricher) nearRollout(ns, dep string, at time.Time) bool {
	near := func(t time.Time) bool { return !t.IsZero() && t.Sub(at).Abs() < 2*time.Minute }
	d, ok := e.get(kube.Deployments, ns, dep)
	if !ok {
		return false
	}
	if t, ok := kube.TemplateChangedAt(d.GetUID()); ok && near(t) {
		return true
	}
	for _, rs := range e.list(kube.ReplicaSets, ns) {
		if ref := metav1.GetControllerOf(rs); ref == nil || ref.UID != d.GetUID() {
			continue
		}
		if near(rs.GetCreationTimestamp().Time) {
			return true
		}
		if t, ok := wentBack(rs.GetUID(), rs.GetAnnotations()["deployment.kubernetes.io/revision"]); ok && near(t) {
			return true
		}
	}
	return false
}

// firstWord is the text up to its first space.
func firstWord(s string) string {
	if i := strings.IndexByte(s, ' '); i >= 0 {
		return s[:i]
	}
	return s
}

// copiesAdded is, for each ReplicaSet ("ns/name"), the last time copies
// were added to it while it ran (a scale by hand or by an autoscaler),
// with the counts in words. A rollout scales one version up while it
// scales another down: those scale-ups aren't counted.
func (e *Enricher) copiesAdded(ns string) (map[string]time.Time, map[string]string) {
	type scale struct {
		at      time.Time
		dep, rs string
	}
	var downs []scale
	type up struct {
		scale
		rs, words string
		merged    bool
	}
	var ups []up
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") != "ScalingReplicaSet" {
			continue
		}
		raw := str(ev, "message")
		msg := strings.TrimPrefix(raw, "(combined from similar events): ")
		at, dep := EventTime(ev), ev.GetNamespace()+"/"+str(ev, "involvedObject", "name")
		if strings.HasPrefix(msg, "Scaled down") {
			downs = append(downs, scale{at, dep, firstWord(strings.TrimPrefix(msg, "Scaled down replica set "))})
			continue
		}
		m, n := scaledUpRS.FindStringSubmatch(msg), scaleUpWords.FindStringSubmatch(msg)
		// Merged events (many scalings in a short time) keep the last
		// one's words and time: that last scale is the one read.
		if m == nil || n == nil || scaledFromZero.MatchString(msg) {
			continue
		}
		from, to := n[1], n[2]
		if from == "" {
			from, to = n[4], n[3]
		}
		ups = append(ups, up{scale{at, dep, m[1]}, ev.GetNamespace() + "/" + m[1], "scaled up from " + from + " to " + to, raw != msg || EventCount(ev) > 1})
	}
	// When kubed saw each version brought back, under every number it has
	// had: a version brought back twice lists only its latest go-back, but
	// the earlier one scaled it up too.
	backAt := map[string][]time.Time{}
	for _, rs := range e.list(kube.ReplicaSets, ns) {
		ann := rs.GetAnnotations()
		for _, n := range strings.Split(ann["deployment.kubernetes.io/revision"]+","+ann["deployment.kubernetes.io/revision-history"], ",") {
			if n = strings.TrimSpace(n); n == "" {
				continue
			}
			if t, ok := wentBack(rs.GetUID(), n); ok {
				key := rs.GetNamespace() + "/" + rs.GetName()
				backAt[key] = append(backAt[key], t)
			}
		}
	}
	when, words := map[string]time.Time{}, map[string]string{}
	for _, u := range ups {
		rolling := false
		// A rollout scales the new version up, then the old one down once
		// the new pods are ready: a scale-down of another version soon
		// after. One before it is a rollout that had already ended.
		for _, d := range downs {
			gap := d.at.Sub(u.at)
			rolling = rolling || (d.dep == u.dep && d.rs != u.scale.rs && gap >= 0 && gap < 2*time.Minute)
		}
		// A merged event (its scale-downs may be merged away too) at a
		// go-back of that version: the go-back's own scale-up. A few
		// seconds early is the same moment (events keep whole seconds).
		for _, t := range backAt[u.rs] {
			gap := u.at.Sub(t)
			rolling = rolling || (u.merged && gap >= -newPodsAtOnce && gap < 2*time.Minute)
		}
		if !rolling && u.at.After(when[u.rs]) {
			when[u.rs], words[u.rs] = u.at, u.words
		}
	}
	return when, words
}

// mergedCopyWords: a copy whose scale event Kubernetes merged with others
// (it keeps only the last one's words): what made it isn't known.
const mergedCopyWords = "its scale events were merged, so whether a scale or a rollout made it isn't known"

// mergedScaleUps: which ReplicaSets' scale-ups copiesAdded read ("ns/rs"
// at that time) came from a merged event ("combined from similar events",
// or the same words seen more than once).
func (e *Enricher) mergedScaleUps(ns string, when map[string]time.Time) map[string]bool {
	out := map[string]bool{}
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") != "ScalingReplicaSet" {
			continue
		}
		raw := str(ev, "message")
		msg := strings.TrimPrefix(raw, "(combined from similar events): ")
		m := scaledUpRS.FindStringSubmatch(msg)
		if m == nil {
			continue
		}
		key := ev.GetNamespace() + "/" + m[1]
		if at, ok := when[key]; ok && EventTime(ev).Equal(at) && (raw != msg || EventCount(ev) > 1) {
			out[key] = true
		}
	}
	return out
}

// eventFirstTime is when an event first happened: its first time, or in
// the newer events API its eventTime.
func eventFirstTime(o *unstructured.Unstructured) time.Time {
	for _, f := range [][]string{{"firstTimestamp"}, {"eventTime"}} {
		v, _, _ := unstructured.NestedString(o.Object, f...)
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t
		}
	}
	return EventTime(o)
}

// scaledUpTimes is when each ReplicaSet was last scaled up from no pods
// ("ns/name"), from events (kept about an hour).
func (e *Enricher) scaledUpTimes(ns string) map[string]time.Time {
	scaledUp := map[string]time.Time{}
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") != "ScalingReplicaSet" {
			continue
		}
		// A merged event (the same words seen again, count > 1) holds several
		// scalings under its last time: a go-back's and a hand scale's alike.
		// It can't date any one of them.
		if EventCount(ev) > 1 {
			continue
		}
		if m := scaledUpRS.FindStringSubmatch(str(ev, "message")); m != nil && scaledFromZero.MatchString(str(ev, "message")) {
			key := ev.GetNamespace() + "/" + m[1]
			if t := EventTime(ev); t.After(scaledUp[key]) {
				scaledUp[key] = t
			}
		}
	}
	return scaledUp
}

// releaseRow: a new version going out ("revision 7: api:v42"), which a
// release to several workloads groups; not a first version, a go-back or
// a version brought back later, each its own story.
func releaseRow(detail string) bool {
	return strings.HasPrefix(detail, "revision ") && !strings.Contains(detail, "went back") &&
		!strings.Contains(detail, "(later brought back)")
}

// groupImages is what a grouped rollout runs: "3 workloads got a new
// version: api:v2" or "3 workloads: settings changed, same images
// (api:v2)" both name it after the first colon.
func groupImages(detail string) string {
	if _, images, ok := strings.Cut(detail, "new version: "); ok {
		return images
	}
	if _, images, ok := strings.Cut(detail, ": "); ok {
		return images
	}
	return detail
}

// JobCauseOf is, when set (the view package sets it), why a failed Job
// failed as its screens say it (its pods' exit, an image never pulled):
// the :changes row says the same. It lives in view, which needs enrich.
var JobCauseOf func(e *Enricher, j *unstructured.Unstructured) (string, bool)

// HookKilledOf is, when set (by view, as JobCauseOf), whether a pod's
// container's stop that finished at finished was Kubernetes killing it
// because its postStart hook failed, as home says.
var HookKilledOf func(e *Enricher, pod *unstructured.Unstructured, container string, finished time.Time) bool

func (e *Enricher) changesOver(ns, appName string, now time.Time, window time.Duration, keep func(object string) bool) []Change {
	since := now.Add(-window)
	var out []Change
	add := func(c Change) {
		if c.When.After(since) {
			out = append(out, c)
		}
	}

	// Rollouts: each ReplicaSet of a Deployment is a version. A version
	// brought back (undo, or the same template again) reuses its old
	// ReplicaSet, which keeps its old creation time: when it went live
	// again comes from the "Scaled up replica set" event (kept about an
	// hour) or, for the one running now, from the Deployment itself.
	scaledUp := e.scaledUpTimes(ns)
	type version struct {
		rs  *unstructured.Unstructured
		rev int
	}
	byDeployment := map[*unstructured.Unstructured][]version{}
	for _, rs := range e.list(kube.ReplicaSets, ns) {
		owner := e.controller(rs)
		if owner == nil || owner.GetKind() != "Deployment" {
			continue
		}
		// rev orders the versions; a missing number sorts first
		rev, _ := strconv.Atoi(rs.GetAnnotations()["deployment.kubernetes.io/revision"])
		byDeployment[owner] = append(byDeployment[owner], version{rs, rev})
	}
	var log *scaleLog
	for owner, versions := range byDeployment {
		sort.Slice(versions, func(i, j int) bool { return versions[i].rev < versions[j].rev })
		// held: every number a version has had. Going back renumbers it and
		// keeps the old numbers in its revision history.
		held := map[*unstructured.Unstructured][]int{}
		for _, v := range versions {
			held[v.rs] = []int{v.rev}
			for _, h := range strings.Split(v.rs.GetAnnotations()["deployment.kubernetes.io/revision-history"], ",") {
				if n, err := strconv.Atoi(strings.TrimSpace(h)); err == nil {
					held[v.rs] = append(held[v.rs], n)
				}
			}
		}
		all := make([]*unstructured.Unstructured, 0, len(versions))
		for _, v := range versions {
			all = append(all, v.rs)
		}
		// forBounds: the versions as leftAt and goBackBound read them, newest first.
		forBounds := make([]*Version, len(versions))
		mine := map[*unstructured.Unstructured]*Version{}
		for i, v := range versions {
			j := len(versions) - 1 - i
			forBounds[j] = &Version{Name: v.rs.GetName(), Revision: v.rev, Created: v.rs.GetCreationTimestamp().Time, held: held[v.rs]}
			mine[v.rs] = forBounds[j]
		}
		if log == nil {
			l := e.scaleLog(ns) // read once for every Deployment here
			log = &l
		}
		ro := e.rolloutOf(owner, *log, all).All
		var prevWhen time.Time // the revision before: a later revision went out after it
		for _, v := range versions {
			rs := v.rs
			rev := rs.GetAnnotations()["deployment.kubernetes.io/revision"]
			when, after := rs.GetCreationTimestamp().Time, false
			_, reused := rs.GetAnnotations()["deployment.kubernetes.io/revision-history"]
			label := "revision " + orQ(rev)
			if reused {
				// Its first time out keeps a row of its own, under the number
				// it had then, said against the version made just before it.
				add(Change{When: when, What: "rollout", Namespace: rs.GetNamespace(), Object: "deployment/" + owner.GetName(),
					Detail: firstTimeOut(rs, ranBefore(all, held, slices.Min(held[rs])), held[rs]), Health: HealthNone})
				label += ": went back to an earlier version"
				// When it came back, as the versions list says it: the same
				// sources and the same bounds, so every screen says one time.
				for _, r := range ro {
					if r.Name == rs.GetName() {
						when, after = r.LiveSince, false
						if when.IsZero() {
							when, after = r.BackAfter, true
						}
					}
				}
			}
			if reused && !when.After(prevWhen) {
				// It came back after the revision before it went out, whenever
				// exactly: no earlier than that, never shown as exact. The
				// versions list and Deployed keep the same rule (wentOut), so
				// the rows stay in order and all say one time.
				when, after = prevWhen, true
			}
			prevWhen = when
			images := strings.Join(templateImages(rs), ", ")
			what := images
			// Against the version it replaced: the one that went out under
			// the number just below (ranBefore), which a later go-back may
			// have renumbered. Not the one made just before it: a go-back may
			// have run an older one in between.
			prev := ranBefore(all, held, v.rev)
			switch {
			case prev != nil && restartOnly(prev, rs):
				what = "restarted, same images (" + images + ")"
			case prev != nil && strings.Join(templateImages(prev), ", ") == images && OtherBuild(templateRefs(prev), templateRefs(rs)):
				what = "same tag, new build (" + images + ")"
			case prev != nil && strings.Join(templateImages(prev), ", ") == images:
				what = "settings changed, same images (" + images + ")"
			}
			add(Change{When: when, What: "rollout", Namespace: rs.GetNamespace(),
				Object: "deployment/" + owner.GetName(),
				Detail: label + ": " + what, Health: HealthNone, After: after, rev: v.rev})
		}
	}

	// StatefulSets and DaemonSets: each ControllerRevision is a version.
	// The first is when it was created, which "created" rows already say.
	for _, kind := range []schema.GroupVersionResource{kube.StatefulSets, kube.DaemonSets} {
		for _, o := range e.list(kind, ns) {
			versions := e.revisionRollout(o).All
			for i := len(versions) - 1; i >= 0; i-- { // oldest first, each against the one before
				v := versions[i]
				images := strings.Join(v.Images, ", ")
				object := strings.ToLower(o.GetKind()) + "/" + o.GetName()
				if v.Reused || i == len(versions)-1 {
					// Its first time out: a version brought back later, or the
					// oldest kept, which has none before it to compare with.
					// Made with the workload itself, the "created" row says it.
					label := fmt.Sprintf("revision %d", v.Revision)
					what := images
					if v.Reused {
						label = "an earlier revision (later brought back)" // its number then is gone
						for _, o := range versions {                       // made as a restart of an earlier one
							if o != v && o.Created.Before(v.Created) && o.Settings == v.Settings {
								what = "restarted, same images (" + images + ")"
							}
						}
					}
					// Only the very first revision is said by the "created" row:
					// the earliest made (a version brought back gets a new,
					// higher number, so not the lowest number).
					first := true
					for _, w := range versions {
						first = first && !w.Created.Before(v.Created)
					}
					if !first || v.Created.Sub(o.GetCreationTimestamp().Time).Abs() > time.Minute {
						add(Change{When: v.Created, What: "rollout", Namespace: o.GetNamespace(), Object: object,
							Detail: label + ": " + what, Health: HealthNone})
					}
					if v.Reused && !v.LiveSince.IsZero() {
						add(Change{When: v.LiveSince, What: "rollout", Namespace: o.GetNamespace(), Object: object,
							Detail: fmt.Sprintf("revision %d: went back to an earlier version: %s", v.Revision, images), Health: HealthNone})
					}
					continue
				}
				before := versions[i+1]
				what := images
				switch {
				case v.Settings == before.Settings:
					what = "restarted, same images (" + images + ")"
				case images == strings.Join(before.Images, ", ") && OtherBuild(before.Refs, v.Refs):
					what = "same tag, new build (" + images + ")"
				case images == strings.Join(before.Images, ", "):
					what = "settings changed, same images (" + images + ")"
				}
				add(Change{When: v.Created, What: "rollout", Namespace: o.GetNamespace(), Object: object,
					Detail: fmt.Sprintf("revision %d: %s", v.Revision, what), Health: HealthNone})
			}
		}
	}

	// Scaling by hand (or anything but an autoscaler, which says so on its
	// own): a version scaled with no other version scaled the other way
	// near it. Scaled to 0 is said plainly: its pods stop. Only about that
	// deployment (others of its app may run on), and true while they are
	// still shutting down too. Reworded here: handScales is left as it is.
	for _, c := range e.handScales(ns) {
		c.Detail = strings.Replace(c.Detail, ": nothing runs", ": its pods stop", 1)
		add(c)
	}

	// ArgoCD syncs: every entry of the app's history.
	for _, app := range e.list(kube.Applications, "") {
		// Filed under where the app's things are (its destination is often
		// just "default"): the namespace most of its resources use.
		dest, _, _ := unstructured.NestedString(app.Object, "spec", "destination", "namespace")
		count, inNS := map[string]int{}, false
		for _, r := range Application(app).Resources {
			if r.Namespace != "" {
				count[r.Namespace]++
				inNS = inNS || r.Namespace == ns
				if count[r.Namespace] > count[dest] {
					dest = r.Namespace
				}
			}
		}
		if ns != "" && dest != ns && !inNS && app.GetName() != appName {
			continue
		}
		if inNS {
			dest = ns
		}
		if dest == "" {
			dest = "-"
		}
		hist, _, _ := unstructured.NestedSlice(app.Object, "status", "history")
		for _, h := range hist {
			m, _ := h.(map[string]any)
			when, _ := time.Parse(time.RFC3339, fmt.Sprint(m["deployedAt"]))
			rev := fmt.Sprint(m["revision"])
			if revs, ok := m["revisions"].([]any); ok && len(revs) > 0 {
				rev = fmt.Sprint(revs[0])
			}
			add(Change{When: when, What: "sync", Namespace: dest, Object: "argocd/" + app.GetName(),
				Detail: "applied Git revision " + shortRev(rev)})
		}
		if phase := str(app, "status", "operationState", "phase"); phase == "Failed" || phase == "Error" {
			when, _ := time.Parse(time.RFC3339, str(app, "status", "operationState", "finishedAt"))
			add(Change{When: when, What: "sync", Namespace: dest, Object: "argocd/" + app.GetName(),
				Detail: "sync failed: " + str(app, "status", "operationState", "message"), Health: HealthBad})
		}
	}

	// ExternalSecrets that started or stopped failing.
	for _, es := range e.list(kube.ExternalSecrets, ns) {
		c, ok := Condition(es, "Ready")
		if !ok {
			continue
		}
		when := conditionTime(es, "Ready")
		if c.Status == "True" {
			add(Change{When: when, What: "secret", Namespace: es.GetNamespace(), Object: "externalsecret/" + es.GetName(),
				Detail: "synced again", Health: HealthOK})
		} else {
			add(Change{When: when, What: "secret", Namespace: es.GetNamespace(), Object: "externalsecret/" + es.GetName(),
				Detail: "stopped syncing: " + firstNonEmpty(c.Message, c.Reason), Health: HealthBad})
		}
	}

	// Nodes that joined (leaving shows up as an event).
	if ns == "" {
		for _, n := range e.list(kube.Nodes, "") {
			info := e.Node(n)
			add(Change{When: n.GetCreationTimestamp().Time, What: "node", Namespace: "-", Object: "node/" + n.GetName(),
				Detail: fmt.Sprintf("joined: %s %s %s", Or(info.InstanceType), Or(info.Capacity), Or(info.Pool))})
		}
	}

	// Pods started well after their version went out replaced one that
	// went away (evicted, its node removed, deleted by hand or by the
	// descheduler). Kubernetes keeps no record of why for long, but the
	// replacement itself is worth a line: it explains a young pod.
	copiesAdded, copiesWords := e.copiesAdded(ns)
	mergedUps := e.mergedScaleUps(ns, copiesAdded)
	type podOf struct {
		p     *unstructured.Unstructured
		owner *unstructured.Unstructured
	}
	var pods []podOf
	siblings := map[types.UID][]time.Time{} // when each running pod of an owner was made
	for _, p := range e.list(kube.Pods, ns) {
		owner := e.controller(p)
		if owner == nil || p.GetDeletionTimestamp() != nil {
			continue
		}
		pods = append(pods, podOf{p, owner})
		siblings[owner.GetUID()] = append(siblings[owner.GetUID()], p.GetCreationTimestamp().Time)
	}
	// A scale adds as many pods as it raised the count by: the first ones
	// made after it. A pod made soon after those replaced one that went.
	addedPods := map[string]bool{}
	byOwner := map[string][]*unstructured.Unstructured{}
	ownerUID := map[string]types.UID{}
	for _, po := range pods {
		key := po.owner.GetNamespace() + "/" + po.owner.GetName()
		if at, ok := copiesAdded[key]; ok {
			if started := po.p.GetCreationTimestamp().Time; !started.Before(at) && started.Sub(at) < time.Minute {
				byOwner[key] = append(byOwner[key], po.p)
				ownerUID[key] = po.owner.GetUID()
			}
		}
	}
	for key, made := range byOwner {
		sort.Slice(made, func(i, j int) bool {
			return made[i].GetCreationTimestamp().Time.Before(made[j].GetCreationTimestamp().Time)
		})
		n := len(made) // how many the scale added
		if m := scaleUpWords.FindStringSubmatch(copiesWords[key]); m != nil {
			from, to := m[1], m[2]
			if from == "" {
				from, to = m[4], m[3]
			}
			f, _ := strconv.Atoi(from)
			t, _ := strconv.Atoi(to)
			if t > f {
				n = t - f
			}
		}
		// Every pod kubed's watch saw made in that minute, gone ones too:
		// more than the scale added means a pod went away and was replaced
		// meanwhile, and which is which can't be told (the added one may
		// be gone). Nor can it when kubed wasn't watching yet. None is
		// said to be added then.
		at := copiesAdded[key]
		watched, seen := kube.PodsMadeBetween(ownerUID[key], strings.SplitN(key, "/", 2)[0], at, at.Add(time.Minute))
		if !seen || len(made) > n || watched > n {
			continue
		}
		for _, p := range made {
			addedPods[p.GetNamespace()+"/"+p.GetName()] = true
		}
	}
	for _, po := range pods {
		p, owner := po.p, po.owner
		started := p.GetCreationTimestamp().Time
		top := owner.GetName()
		if t := e.TopOwner(p); t != nil && t != p {
			top = t.GetName()
		}
		row := func(detail string) Change {
			return Change{When: started, What: "new pod", Namespace: p.GetNamespace(), Object: "pod/" + p.GetName(), owner: top, Detail: detail}
		}
		if addedPods[p.GetNamespace()+"/"+p.GetName()] { // a scale made it, whatever is beside it
			words := copiesWords[owner.GetNamespace()+"/"+owner.GetName()]
			if mergedUps[owner.GetNamespace()+"/"+owner.GetName()] {
				words = mergedCopyWords // its counts may be another scale's, or a rollout's
			}
			add(row("added a copy: " + words))
			continue
		}
		// Pods made in a batch, with no older one beside them, came with a
		// rollout, a go-back or a new run: they are a line of their own.
		// A young pod beside older ones replaced one, or was added.
		// Made with a version going out (a rollout, restart or go-back of
		// its workload) within about 2 minutes: part of that, as the guide
		// says, whatever pods were beside it.
		var live time.Time
		if t, ok := scaledUp[owner.GetNamespace()+"/"+owner.GetName()]; ok {
			live = t
		}
		if t, ok := kube.TemplateChangedAt(owner.GetUID()); ok && t.After(live) {
			live = t
		}
		if owner.GetKind() == "ReplicaSet" {
			if owner.GetCreationTimestamp().After(live) {
				live = owner.GetCreationTimestamp().Time
			}
			// The version running now went live when its Deployment's
			// template was last written (a go-back reuses an old one).
			if dep := e.controller(owner); dep != nil && dep.GetAnnotations()["deployment.kubernetes.io/revision"] == owner.GetAnnotations()["deployment.kubernetes.io/revision"] {
				if t, ok := kube.TemplateChangedAt(dep.GetUID()); ok && t.After(live) {
					live = t
				}
			}
		}
		if d := started.Sub(live); !live.IsZero() && d >= -time.Minute && d < 2*time.Minute {
			continue
		}
		others := len(siblings[owner.GetUID()]) - 1 // not itself
		// An older pod beside it when kubed saw it appear (since gone), or now.
		older := kube.PodHadOlder(p.GetUID())
		for _, t := range siblings[owner.GetUID()] {
			older = older || t.Before(started.Add(-2*time.Minute))
		}
		switch {
		case others > 0 && !older:
			continue // a batch: a rollout's own pods
		case others <= 0:
			// Alone: only the workload's own history can tell.
			if started.Sub(owner.GetCreationTimestamp().Time) < 5*time.Minute {
				continue // part of a rollout (or a new job run)
			}
			// A version brought back (undo) keeps its old creation time: it
			// went live again when it was scaled up, or its template was
			// written.
			if d := started.Sub(live); !live.IsZero() && d >= -time.Minute && d < 5*time.Minute {
				continue // came with that version
			}
		}
		switch owner.GetKind() {
		case "ReplicaSet", "StatefulSet", "DaemonSet":
			add(row("replaced a pod that went away, or added a copy"))
		}
	}

	// Job runs (cron jobs' too) that failed.
	for _, j := range e.list(kube.Jobs, ns) {
		c, ok := Condition(j, "Failed")
		if !ok || c.Status != "True" {
			continue
		}
		when := j.GetCreationTimestamp().Time
		if at := JobFailedAt(j); !at.IsZero() {
			when = at // as home and the app's box say
		}
		why := c.Message
		if why == "" {
			why = c.Reason
		}
		why = MessageWords(why)
		if JobCauseOf != nil { // what its tries ran into, as home and its box say
			if cause, ok := JobCauseOf(e, j); ok && cause != "" {
				why = cause
			}
		}
		add(Change{When: when, What: "failed", Namespace: j.GetNamespace(), Object: "job/" + j.GetName(),
			Detail: why, Health: HealthBad})
	}

	// Containers that died: the last termination of each, with its reason.
	for _, p := range e.list(kube.Pods, ns) {
		statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
		for _, s := range statuses {
			m, _ := s.(map[string]any)
			// The stop before, and the one now when it is stopped waiting to
			// restart (the newest, which lastState holds only once it runs
			// again). Not a finished run's first stop: nothing restarts it.
			_, restarted, _ := unstructured.NestedMap(m, "lastState", "terminated")
			var newest time.Time
			detail := ""
			for _, which := range []string{"lastState", "state"} {
				if which == "state" && !restarted {
					continue
				}
				reason, _, _ := unstructured.NestedString(m, which, "terminated", "reason")
				finished, _, _ := unstructured.NestedString(m, which, "terminated", "finishedAt")
				when, err := time.Parse(time.RFC3339, finished)
				if err != nil || reason == "" || !when.After(newest) {
					continue
				}
				code, _, _ := unstructured.NestedInt64(m, which, "terminated", "exitCode")
				name, _ := m["name"].(string)
				if check, _, killed := e.KilledByCheck(p.GetNamespace(), p.GetName(), name, when, int32(code)); killed {
					newest, detail = when, fmt.Sprintf("container %q was killed by its %s", name, check)
					continue
				}
				if reason == "Completed" {
					continue // it ended by itself
				}
				if HookKilledOf != nil && HookKilledOf(e, p, name, when) {
					newest, detail = when, fmt.Sprintf("container %q was killed because its postStart hook failed", name)
					continue
				}
				newest, detail = when, fmt.Sprintf("container %q %s", m["name"], stopWords(reason, code))
			}
			if detail == "" {
				continue
			}
			// One row, the newest stop, with how often it restarted (Kubernetes
			// keeps only the last two stops, so they can't be counted).
			if n, _, _ := unstructured.NestedInt64(m, "restartCount"); n > 1 {
				detail += fmt.Sprintf("; %d restarts so far", n)
			}
			add(Change{When: newest, What: "crash", Namespace: p.GetNamespace(), Object: "pod/" + p.GetName(),
				Detail: detail, Health: HealthBad})
		}
	}

	// Selected events: scaling, nodes coming and going, evictions, probes.
	for _, ev := range e.list(kube.Events, ns) {
		what, ok := interestingEvents[str(ev, "reason")]
		if ok && str(ev, "reason") == "BackOff" && strings.Contains(str(ev, "message"), "pulling image") {
			what = "image" // waiting to try the download again, not a crash
		}
		if !ok {
			continue
		}
		h := HealthNone
		if IsWarning(ev) {
			h = HealthWarn
		}
		obj := strings.ToLower(str(ev, "involvedObject", "kind")) + "/" + str(ev, "involvedObject", "name")
		evNS := ev.GetNamespace()
		if str(ev, "involvedObject", "kind") == "Node" {
			evNS = "-" // node events live in "default"; nodes belong to no namespace
		}
		times := EventCount(ev)
		if str(ev, "reason") == "BackOff" {
			times = 1 // counts the kubelet's checks while it waits, not crashes or pulls: "15 times" for 7 crashes
		}
		detail := eventDetail(str(ev, "reason"), str(ev, "message"))
		if what == "crash" && str(ev, "involvedObject", "kind") == "Pod" {
			// Kubernetes says "failed container" even for one that exits 0:
			// where the pod shows the stop this back-off followed was clean,
			// say so.
			if m := failedContainer.FindStringSubmatch(str(ev, "message")); m != nil && e.stoppedClean(ev.GetNamespace(), str(ev, "involvedObject", "name"), m[1], EventTime(ev)) {
				what, detail = "restart", fmt.Sprintf("container %q keeps stopping; waiting before it is started again", m[1])
			}
		}
		add(Change{When: EventTime(ev), What: what, Namespace: evNS, Object: obj,
			Detail: detail, Health: h, times: times})
	}

	// New workloads: a cron job added yesterday that has never worked is
	// the change behind its failure. (A deployment's first version already
	// has its own rollout row.)
	for _, g := range []schema.GroupVersionResource{kube.CronJobs, kube.StatefulSets, kube.DaemonSets} {
		for _, o := range e.list(g, ns) {
			detail := "new " + strings.ToLower(kindWords(o.GetKind()))
			if images := e.firstImages(o); images != "" {
				detail += " running " + images
			}
			add(Change{When: o.GetCreationTimestamp().Time, What: "created", Namespace: o.GetNamespace(),
				Object: strings.ToLower(o.GetKind()) + "/" + o.GetName(), Detail: detail, Health: HealthNone})
		}
	}

	if keep != nil {
		kept := out[:0:0]
		for _, c := range out {
			if keep(c.Object) {
				kept = append(kept, c)
			}
		}
		out = kept
	}
	sort.SliceStable(out, func(i, j int) bool {
		// At the same time (a go-back known only to be no earlier than the
		// version before it): the higher revision went out later.
		return out[i].When.After(out[j].When) || out[i].When.Equal(out[j].When) && out[i].rev > out[j].rev
	})
	return groupNewPods(dropRepeats(groupRollouts(out)))
}

// groupNewPods folds new pods that started in the same minute in the same
// namespace into one row ("14 new pods: web-1, api-2 and 12 more"): a node
// going away replaces many at once, and a row each buries everything else.
// newPodsAtOnce: pods made this close together were made at once (a
// node going away, a scale), one row for them all.
const newPodsAtOnce = 10 * time.Second

func groupNewPods(in []Change) []Change {
	out := make([]Change, 0, len(in))
	at := map[string]int{}      // namespace, owner and words → the group being filled, in out
	last := map[int]time.Time{} // the newest pod's time in each group
	names := map[int][]string{}
	for _, c := range in {
		if c.What != "new pod" {
			out = append(out, c)
			continue
		}
		// Grouped only with pods said the same way (copies a scale added
		// are never folded into "replaced or added"), and made at once:
		// within seconds of each other, not two deletes a minute apart.
		key := c.Namespace + "\x00" + c.owner + "\x00" + c.Detail
		if i, ok := at[key]; ok && c.When.Sub(last[i]).Abs() <= newPodsAtOnce {
			names[i] = append(names[i], strings.TrimPrefix(c.Object, "pod/"))
			last[i] = c.When
			continue
		}
		at[key] = len(out)
		last[len(out)] = c.When
		names[len(out)] = []string{strings.TrimPrefix(c.Object, "pod/")}
		out = append(out, c)
	}
	for i, n := range names {
		if len(n) < 2 {
			continue
		}
		obj := fmt.Sprintf("pods of %s (%d)", out[i].owner, len(n))
		if out[i].owner == "" {
			obj = fmt.Sprintf("%d pods", len(n))
		}
		out[i].Object = obj
		if words, ok := strings.CutPrefix(out[i].Detail, "added a copy: "); ok {
			out[i].Detail = fmt.Sprintf("added %d copies: %s", len(n), words)
			continue
		}
		out[i].Detail = fmt.Sprintf("%d pods replaced or added at once (a node going away often does this)", len(n))
	}
	return out
}

// dropRepeats keeps the newest of changes that say the same thing about
// the same object, and counts the others: "(3 times)".
func dropRepeats(in []Change) []Change {
	type key struct{ what, object, detail string }
	index := map[key]int{}
	count := map[key]int64{}
	var out []Change
	for _, c := range in {
		k := key{c.What, c.Object, c.Detail}
		count[k] += max(c.times, 1) // an event that happened 240 times counts 240
		if _, seen := index[k]; seen {
			continue
		}
		index[k] = len(out)
		out = append(out, c)
	}
	for k, n := range count {
		if n > 1 {
			out[index[k]].Detail += fmt.Sprintf(" (%d times)", n)
		}
	}
	return out
}

// longestBackOff is the most Kubernetes waits before starting a stopped
// container again.
const longestBackOff = 5 * time.Minute

// stoppedClean: a pod's container (or native sidecar), as the pod shows it now, last stopped
// by itself without an error (exit 0, not killed by a check), and that
// stop is the one a back-off last seen at backOff waited after: it ended
// at most the longest back-off before it. A stop that ended long before
// or after says nothing about that back-off (it may have crashed then).
// False when the pod or the container is gone: then nothing says so.
func (e *Enricher) stoppedClean(ns, pod, container string, backOff time.Time) bool {
	p, ok := e.get(kube.Pods, ns, pod)
	if !ok {
		return false
	}
	statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
	// Of the init containers, only native sidecars (restartPolicy Always):
	// Kubernetes starts a setup step again only after it failed, so a
	// back-off on one never followed a clean stop.
	sidecar := map[any]bool{}
	specInits, _, _ := unstructured.NestedSlice(p.Object, "spec", "initContainers")
	for _, c := range specInits {
		if m, _ := c.(map[string]any); m != nil && m["restartPolicy"] == "Always" {
			sidecar[m["name"]] = true
		}
	}
	inits, _, _ := unstructured.NestedSlice(p.Object, "status", "initContainerStatuses")
	for _, s := range inits {
		if m, _ := s.(map[string]any); m != nil && sidecar[m["name"]] {
			statuses = append(statuses, s)
		}
	}
	for _, s := range statuses {
		m, _ := s.(map[string]any)
		if m["name"] != container {
			continue
		}
		// The newest stop: the one now when it is stopped, else the one before.
		which := "lastState"
		if _, now, _ := unstructured.NestedMap(m, "state", "terminated"); now {
			which = "state"
		}
		reason, _, _ := unstructured.NestedString(m, which, "terminated", "reason")
		code, found, _ := unstructured.NestedInt64(m, which, "terminated", "exitCode")
		finished, _, _ := unstructured.NestedString(m, which, "terminated", "finishedAt")
		when, err := time.Parse(time.RFC3339, finished)
		if err != nil || !found || code != 0 || reason != "Completed" {
			return false
		}
		// 30s: both times are kept to the second, and the back-off is
		// recorded just after the stop.
		if when.Before(backOff.Add(-longestBackOff-30*time.Second)) || when.After(backOff.Add(30*time.Second)) {
			return false
		}
		_, _, killed := e.KilledByCheck(ns, pod, container, when, 0)
		return !killed
	}
	return false
}

// eventDetail is an event's line: a pull back-off read from its own
// message (rewording first would lose "pulling image"), the rest reworded.
func eventDetail(reason, msg string) string {
	msg = strings.Join(strings.Fields(msg), " ")
	if reason == "BackOff" { // its own words name the image or the container: rewording loses them
		return eventWords(reason, msg)
	}
	return eventWords(reason, MessageWords(msg))
}

// eventWords says what an event means instead of its code.
func eventWords(reason, msg string) string {
	switch reason {
	case "BackOff":
		if strings.Contains(msg, "pulling image") { // "Back-off pulling image \"x\"": never started
			if m := quotedImage.FindStringSubmatch(msg); m != nil {
				return "can't download image " + PullImage(m[1]) + "; trying again"
			}
			return "can't download its image; trying again"
		}
		if m := failedContainer.FindStringSubmatch(msg); m != nil {
			return "container \"" + m[1] + "\" keeps crashing; waiting before the next restart"
		}
		return "keeps crashing; waiting before the next restart"
	case "Unhealthy":
		return msg // already in words: "ready check failed: …"
	case "FailedMount":
		return "can't attach a volume or secret: " + msg
	case "Evicted":
		return MessageWords(msg) // "pushed off its node: it ran low on memory"
	case "OOMKilling":
		return "ran out of memory: " + msg
	case "SuccessfulRescale":
		return msg // "scaled to 2 (load is low)"
	case "RegisteredNode":
		return "joined the cluster"
	case "RemovingNode":
		return "leaving the cluster"
	case "NodeNotReady":
		return "stopped answering"
	}
	return reason + ": " + msg
}

var failedContainer = regexp.MustCompile(`failed container (\S+)`)

// quotedImage is the image in "Back-off pulling image \"nginx:nope\"".
var quotedImage = regexp.MustCompile(`image \\?"([^"\\]+)`)

// rolloutGroupWindow: rollouts of the same image this close together are
// one change (a release going out to several workloads, which a sync
// makes within seconds). The row takes the newest one's time, so wider
// would move older rollouts to a time they didn't happen at.
const rolloutGroupWindow = 30 * time.Second

// groupRollouts merges rollouts of the same images close together into one
// row: "deployment/a, deployment/b and 7 more · new version: app:v2".
func groupRollouts(in []Change) []Change {
	type group struct {
		Change
		images  string
		objects []string
		oldest  time.Time // of the rollouts in it: the next one must be this close
	}
	imagesOf := func(c Change) string {
		if _, after, ok := strings.Cut(c.Detail, ": "); ok {
			return after
		}
		return c.Detail
	}
	var groups []*group
	for _, c := range in {
		var into *group
		if c.What == "rollout" {
			for i := len(groups) - 1; i >= 0 && groups[i].oldest.Sub(c.When) <= rolloutGroupWindow; i-- {
				g := groups[i]
				if slices.Contains(g.objects, c.Object) {
					break // a later version of it: a group above would list this one before that
				}
				if g.What == "rollout" && g.Namespace == c.Namespace && g.images == imagesOf(c) &&
					releaseRow(g.Detail) && releaseRow(c.Detail) {
					into = g
					break
				}
			}
		}
		if into != nil {
			seen := false
			for _, o := range into.objects {
				seen = seen || o == c.Object // the same workload twice: an earlier revision
			}
			if !seen {
				into.objects = append(into.objects, c.Object)
				into.oldest = c.When
			}
			continue
		}
		groups = append(groups, &group{Change: c, images: imagesOf(c), objects: []string{c.Object}, oldest: c.When})
	}
	out := make([]Change, 0, len(groups))
	for _, g := range groups {
		if n := len(g.objects); n > 1 {
			g.Objects = g.objects
			g.Object = g.objects[0]
			if n == 2 {
				g.Object += ", " + g.objects[1]
			} else {
				g.Object += fmt.Sprintf(" and %d more", n-1)
			}
			g.Detail = fmt.Sprintf("%d workloads got a new version: %s", n, g.images)
			if strings.HasPrefix(g.images, "restarted") {
				g.Detail = fmt.Sprintf("%d workloads %s", n, g.images)
			}
			if strings.HasPrefix(g.images, "settings changed") {
				g.Detail = fmt.Sprintf("%d workloads: %s", n, g.images)
			}
		}
		out = append(out, g.Change)
	}
	return out
}

func templateImages(o *unstructured.Unstructured) []string {
	short := func(m map[string]any) string { return ShortImage(fmt.Sprint(m["image"])) }
	// The app's own containers first, then helpers that run alongside
	// (init containers that keep running), then setup that runs first.
	var main, helpers, setup []string
	containers, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "containers")
	for _, c := range containers {
		if m, ok := c.(map[string]any); ok {
			main = append(main, short(m))
		}
	}
	inits, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "initContainers")
	for _, c := range inits {
		if m, ok := c.(map[string]any); ok {
			if m["restartPolicy"] == "Always" {
				helpers = append(helpers, short(m)+" (helper)")
			} else {
				setup = append(setup, short(m)+" (runs first)")
			}
		}
	}
	return append(append(main, helpers...), setup...)
}

// ShortImage is an image without its registry and digest, keeping its
// own name: "123.dkr.ecr…/payment:v2@sha256:…" is "payment:v2", and
// "alpine/git:latest" stays itself (the "alpine/" says whose it is).
func ShortImage(image string) string {
	image, _, _ = strings.Cut(image, "@") // the digest is noise next to the tag
	parts := strings.Split(image, "/")
	if len(parts) > 1 && (strings.ContainsAny(parts[0], ".:") || parts[0] == "localhost") {
		parts = parts[1:] // a registry host
	}
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "library" { // Docker Hub's official images, also under a mirror's path
			parts = parts[i+1:]
			break
		}
	}
	if len(parts) > 2 { // a long registry path: keep the last two parts
		parts = parts[len(parts)-2:]
	}
	return strings.Join(parts, "/")
}

// PullImage is an image as a pull failure names it: without its digest,
// but with its registry host unless that is Docker Hub's, since the host
// is often why the pull fails (a typo, a registry it can't reach).
func PullImage(image string) string {
	image, _, _ = strings.Cut(image, "@")
	for _, hub := range []string{"docker.io/library/", "index.docker.io/library/", "docker.io/", "index.docker.io/"} {
		if strings.HasPrefix(image, hub) {
			return strings.TrimPrefix(image, hub)
		}
	}
	return image
}

// OtherBuild says two versions with the same tags run different builds:
// only when both name their digests and those differ. Without digests a
// tag alone can't tell.
func OtherBuild(a, b string) bool {
	return a != b && strings.Contains(a, "@") && strings.Contains(b, "@")
}

// templateRefs are a template's images exactly as written, digests and
// all: two versions with the same tags can still run different builds.
func templateRefs(o *unstructured.Unstructured) string {
	var out []string
	for _, field := range []string{"containers", "initContainers"} {
		list, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", field)
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				out = append(out, fmt.Sprint(m["image"]))
			}
		}
	}
	return strings.Join(out, ",")
}

func shortRev(r string) string {
	if len(r) > 8 {
		return r[:8]
	}
	return r
}

func orQ(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// stopWords says why a container stopped.
func stopWords(reason string, code int64) string {
	switch reason {
	case "OOMKilled":
		return "ran out of memory and was killed"
	case "Error":
		return fmt.Sprintf("stopped with an error (exit %d)", code)
	}
	return fmt.Sprintf("stopped: %s (exit %d)", reason, code)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// backSince: the version a Deployment runs now was brought back (a
// go-back) after at, or when is unknown: a scale down of it before then
// was a rollout replacing it, not a scale of the running version.
func (e *Enricher) backSince(ns, dep, rsName string, at time.Time) bool {
	rs, ok := e.get(kube.ReplicaSets, ns, rsName)
	if !ok {
		return false
	}
	if _, reused := rs.GetAnnotations()["deployment.kubernetes.io/revision-history"]; !reused {
		return false
	}
	d, ok := e.get(kube.Deployments, ns, dep)
	if !ok {
		return false
	}
	if t, ok := cameBack(d.GetUID(), rs); ok {
		return t.After(at)
	}
	return true // brought back, when unknown: can't call it the running version then
}
