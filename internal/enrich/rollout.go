package enrich

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Saturated is Kubernetes' IsSaturated: the version runs all want copies.
func (v *Version) Saturated(want int64) bool {
	return want > 0 && v.specReplicas == want && v.desiredNote == want && v.Available == want
}

// desiredNoteOf is a ReplicaSet's deployment.kubernetes.io/desired-replicas
// note (-1 when it has none or it isn't a number).
func desiredNoteOf(rs *unstructured.Unstructured) int64 {
	n, err := strconv.ParseInt(rs.GetAnnotations()["deployment.kubernetes.io/desired-replicas"], 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// Version is one ReplicaSet of a Deployment.
type Version struct {
	Name     string
	Revision int
	Images   []string
	Ready    int64
	Replicas int64
	// Available: its pods available (ready for minReadySeconds), as
	// Kubernetes counts them to call a version saturated.
	Available int64
	// specReplicas and desiredNote: its spec.replicas and its
	// desired-replicas note, for Saturated.
	specReplicas, desiredNote int64
	Created                   time.Time
	// Restarted is the rollout-restart stamp in its pod template, so two
	// versions with the same images can be told apart.
	Restarted string
	// Reused: an earlier version brought back (undo). Created is when it was
	// first made; LiveSince, when known, is when it came back.
	Reused    bool
	LiveSince time.Time
	// BackAfter, for a version brought back that nothing dates (LiveSince
	// unknown): it came back at this time or later (goBackBound), as what
	// changed and Deployed say too.
	BackAfter time.Time
	// Settings is its pod template without the restart stamp: two versions
	// with equal Settings differ only by a restart.
	Settings string
	// Refs are its images as written, digests included.
	Refs string
	uid  string    // its ReplicaSet's, to remember when it came back
	back time.Time // when it came back, as kubed saw (cameBack)
	held []int     // every number it has had: going back renumbers it
}

// wroteIt: the Deployment's last template write, at, is what brought v
// back. Its template is v's (no write since went elsewhere). Seen live, the
// write is the go-back (writes while paused aren't taken: TemplateChangedAtSure).
// Read later, the Deployment must have moved then or later: written away
// and back while paused, then resumed, the last write rolled nothing out.
func wroteIt(dep *unstructured.Unstructured, v *Version, at time.Time, live bool) bool {
	t, ok, _ := unstructured.NestedMap(dep.Object, "spec", "template")
	if !ok || kube.TemplateSettings(t) != v.Settings || v.Restarted != str(dep, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt") {
		return false
	}
	if live {
		return true
	}
	c, ok := Condition(dep, "Progressing")
	return ok && !c.Updated.Before(at)
}

// heldNumbers reads a ReplicaSet's revision and every number it has held
// (going back renumbers it); reused: it was brought back.
func heldNumbers(rs *unstructured.Unstructured) (rev int, held []int, reused bool) {
	rev, _ = strconv.Atoi(rs.GetAnnotations()["deployment.kubernetes.io/revision"])
	history, reused := rs.GetAnnotations()["deployment.kubernetes.io/revision-history"]
	held = []int{rev}
	for _, h := range strings.Split(history, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(h)); err == nil {
			held = append(held, n)
		}
	}
	return rev, held, reused
}

// scaleOccur is one ScalingReplicaSet event of a ReplicaSet: it happened
// at least at its first and its last time. A combined one ("combined from
// similar events") gathered other scalings of its Deployment before it:
// only its last is this one's.
type scaleOccur struct {
	first, last time.Time
	combined    bool
	dep         string // its Deployment's UID, when the event names it
}

// times are the moments it surely happened.
func (o scaleOccur) times() []time.Time {
	if o.combined || o.first.IsZero() {
		return []time.Time{o.last}
	}
	return []time.Time{o.first, o.last}
}

// scaleLog is what a namespace's scaling events (or every namespace's)
// still say, by ReplicaSet ("ns/name"): scaled up from 0 (up), down to 0
// (down). Only what is there counts: events expire, and Kubernetes drops
// some when a Deployment scales often (its spam filter), so a missing one
// proves nothing.
type scaleLog struct {
	up, down map[string][]scaleOccur
}

func (e *Enricher) scaleLog(ns string) scaleLog {
	l := scaleLog{up: map[string][]scaleOccur{}, down: map[string][]scaleOccur{}}
	for _, ev := range e.list(kube.Events, ns) {
		if str(ev, "reason") != "ScalingReplicaSet" {
			continue
		}
		raw := str(ev, "message")
		msg := strings.TrimPrefix(raw, "(combined from similar events): ")
		// "Scaled up replica set web-1 from 0 to 2", or "... to 2 from 0"
		// (read by hand: home reads thousands every second).
		w := strings.Fields(msg)
		if len(w) != 9 || w[0] != "Scaled" || w[2] != "replica" || w[3] != "set" {
			continue
		}
		from, to := w[6], w[8]
		switch {
		case w[5] == "to" && w[7] == "from":
			from, to = w[8], w[6]
		case w[5] != "from" || w[7] != "to":
			continue
		}
		o := scaleOccur{first: eventFirstTime(ev), last: EventTime(ev), combined: msg != raw, dep: str(ev, "involvedObject", "uid")}
		key := ev.GetNamespace() + "/" + w[4]
		switch up := w[1] == "up"; {
		case up && from == "0":
			l.up[key] = append(l.up[key], o)
		case !up && to == "0":
			l.down[key] = append(l.down[key], o)
		}
	}
	return l
}

// goBackProof is when v, a version brought back, came back under its
// number, when the scaling events there prove it; ok is false otherwise.
// left is the version it replaced: the one that went out under the number
// just below, and only then (made then, never brought back). The proof: v
// scaled up from 0 after left was made, and left scaled down to 0 at that
// time or later. Left ran until then, and nothing scales a version up from
// 0 while another runs but a rollout to it: this go-back, which began
// then. (After it, left is never the running version again, so nothing
// scales it up.) Only events that are there prove anything.
//
// A version that kept a pod running from before left was made (left's
// rollout stopped partway, paused or stuck) never went to 0, so coming back
// scaled nothing up from 0: left's one scale-down to 0, not merged with
// others, is when it took over, when kubed saw v's template written then
// (tookOver). A scale by hand or an autoscaler can take left to 0 earlier,
// v keeping its pods; with no such write seen, only the bound is known.
func (l scaleLog) goBackProof(dep *unstructured.Unstructured, versions []*Version, v *Version, tookOver func(since, at time.Time) bool) (time.Time, bool) {
	var left *Version
	for _, o := range versions {
		if o != v && slices.Contains(o.held, v.Revision-1) {
			left = o
		}
	}
	if left == nil || len(left.held) != 1 {
		return time.Time{}, false
	}
	lower := goBackBound(versions, v)
	fits := func(t time.Time) bool {
		if t.Before(lower) || !t.After(v.Created) || t.Before(left.Created) {
			return false
		}
		for _, o := range versions { // before every version first made after it
			if slices.Min(o.held) > v.Revision && !t.Before(o.Created) {
				return false
			}
		}
		return true
	}
	ours := func(o scaleOccur) bool { return o.dep == "" || o.dep == string(dep.GetUID()) }
	ns := dep.GetNamespace()
	var leftDown time.Time // left's latest scale-down to 0
	for _, o := range l.down[ns+"/"+left.Name] {
		if ours(o) && o.last.After(leftDown) {
			leftDown = o.last
		}
	}
	var start time.Time
	for _, o := range l.up[ns+"/"+v.Name] {
		for _, s := range o.times() {
			if ours(o) && fits(s) && !s.After(leftDown) && (start.IsZero() || s.Before(start)) {
				start = s
			}
		}
	}
	if downs := l.down[ns+"/"+left.Name]; start.IsZero() && len(downs) == 1 && tookOver != nil {
		if o := downs[0]; ours(o) && !o.combined && o.first.Equal(o.last) && fits(o.last) && tookOver(left.Created, o.last) {
			start = o.last
		}
	}
	return start, !start.IsZero()
}

// goBackBound is the earliest v, a version brought back, can have come
// back: when it was made, or when the newest of the versions first made
// under a lower number than its own was made (it came back over each of
// them), whichever is later. What changed, the versions list and Deployed
// all show it, as "at this time or later", when nothing dates the go-back,
// or what does says it came earlier. Each also keeps a go-back no earlier
// than the revision just below it went out (wentOut).
func goBackBound(versions []*Version, v *Version) time.Time {
	t := v.Created
	for _, o := range versions {
		if o != v && len(o.held) > 0 && slices.Min(o.held) < v.Revision && o.Created.After(t) {
			t = o.Created
		}
	}
	return t
}

// wentOut is when a version went out as the versions list says: when it
// came back, the bound for that, or when it was made.
func wentOut(v *Version) time.Time {
	switch {
	case !v.LiveSince.IsZero():
		return v.LiveSince
	case !v.BackAfter.IsZero():
		return v.BackAfter
	}
	return v.Created
}

// wentBackAt remembers, for a ReplicaSet and the revision number it was
// given, when it came back as the running version: seen while it ran (its
// template was written then), it stays known after newer rollouts, when
// only Kubernetes' merged events would be left.
var wentBackAt sync.Map

func noteWentBack(uid types.UID, rev string, at time.Time) {
	if uid != "" {
		wentBackAt.LoadOrStore(string(uid)+"/"+rev, at) // it came back once under this number
	}
}

func wentBack(uid types.UID, rev string) (time.Time, bool) {
	v, ok := wentBackAt.Load(string(uid) + "/" + rev)
	if !ok {
		return time.Time{}, false
	}
	return v.(time.Time), true
}

// wentBackSureAt is wentBackAt for the versions list: only times that
// surely are the go-back's (wroteIt), where wentBackAt keeps what the
// timeline's new-pod and hand-scale rows have always read.
var wentBackSureAt sync.Map

// cameBackSure is cameBack for the versions list: from wentBackSureAt, or
// a write of its template the watch saw as it happened, not while paused.
func cameBackSure(dep types.UID, rs *unstructured.Unstructured) (time.Time, bool) {
	if v, ok := wentBackSureAt.Load(string(rs.GetUID()) + "/" + rs.GetAnnotations()["deployment.kubernetes.io/revision"]); ok {
		return v.(time.Time), true
	}
	stamp := str(rs, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	if t, ok := kube.TemplateWrittenAtSure(dep, templateSettings(rs), stamp); ok && t.After(rs.GetCreationTimestamp().Time) {
		return t, true
	}
	return time.Time{}, false
}

// cameBack is when a Deployment's kept version came back under the number
// it has now: remembered from when it ran, else the last time kubed's
// watch saw the Deployment given its template (restart stamp and all),
// after it was made. Merged events can't date it; this can.
func cameBack(dep types.UID, rs *unstructured.Unstructured) (time.Time, bool) {
	if t, ok := wentBack(rs.GetUID(), rs.GetAnnotations()["deployment.kubernetes.io/revision"]); ok {
		return t, true
	}
	stamp := str(rs, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	if t, ok := kube.TemplateWrittenAt(dep, templateSettings(rs), stamp); ok && t.After(rs.GetCreationTimestamp().Time) {
		return t, true
	}
	return time.Time{}, false
}

// Rollout is where a Deployment's update stands.
type Rollout struct {
	Current, Previous *Version
	Older             int // older versions kept for undo
	Desired, Updated  int64
	Ready, Available  int64
	Stuck             bool   // Progressing=False (ProgressDeadlineExceeded)
	Message           string // the Progressing condition's message
	Paused            bool
	All               []*Version // newest first
	// Total is every pod it runs, old and new (a Deployment's
	// status.replicas): more than Updated means old ones are still there.
	Total int64
	// Behind: its status is of an older spec (observedGeneration), so
	// it says nothing yet about the change just made.
	Behind bool
}

// Done reports whether every desired pod runs the current version.
func (r Rollout) Done() bool {
	// The new version's own pods must be ready: an old pod still serving
	// keeps "available" up while the new one waits for a node.
	newReady := r.Current != nil && r.Current.Ready >= r.Desired
	// As kubectl rollout status decides: no old pod still waiting to be
	// stopped, and a status of the current spec.
	return r.Updated == r.Desired && r.Available == r.Desired && r.Desired > 0 && newReady && r.Total <= r.Updated && !r.Behind
}

// RolloutOf reads a Deployment's versions from its ReplicaSets.
func (e *Enricher) RolloutOf(dep *unstructured.Unstructured) Rollout {
	if k := dep.GetKind(); k == "StatefulSet" || k == "DaemonSet" {
		return e.revisionRollout(dep)
	}
	return e.rolloutOf(dep, e.scaleLog(dep.GetNamespace()), nil)
}

// rolloutOf is RolloutOf for a Deployment, with its namespace's scaling
// events already read (log) and, when not nil, its ReplicaSets (owned): a
// pass over many Deployments reads them once.
func (e *Enricher) rolloutOf(dep *unstructured.Unstructured, log scaleLog, owned []*unstructured.Unstructured) Rollout {
	r := Rollout{
		Desired:   num(dep, "spec", "replicas"),
		Updated:   num(dep, "status", "updatedReplicas"),
		Ready:     num(dep, "status", "readyReplicas"),
		Available: num(dep, "status", "availableReplicas"),
		Total:     num(dep, "status", "replicas"),
		Behind:    num(dep, "status", "observedGeneration") < num(dep, "metadata", "generation"),
	}
	r.Paused, _, _ = unstructured.NestedBool(dep.Object, "spec", "paused")
	if c, ok := Condition(dep, "Progressing"); ok {
		r.Message = c.Message
		r.Stuck = c.Status == "False"
	}
	var versions []*Version
	if owned == nil {
		for _, rs := range e.list(kube.ReplicaSets, dep.GetNamespace()) {
			if ownedBy(rs, dep) {
				owned = append(owned, rs)
			}
		}
	}
	for _, rs := range owned {
		rev, held, reused := heldNumbers(rs)
		back, _ := cameBackSure(dep.GetUID(), rs)
		versions = append(versions, &Version{
			uid: string(rs.GetUID()), back: back, held: held,
			Reused: reused, Settings: templateSettings(rs),
			Name: rs.GetName(), Revision: rev, Images: templateImages(rs), Refs: templateRefs(rs),
			Ready: num(rs, "status", "readyReplicas"), Replicas: num(rs, "status", "replicas"), Available: num(rs, "status", "availableReplicas"),
			specReplicas: num(rs, "spec", "replicas"), desiredNote: desiredNoteOf(rs),
			Created:   rs.GetCreationTimestamp().Time,
			Restarted: str(rs, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt"),
		})
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Revision > versions[j].Revision })
	r.All = versions
	// What the timeline's new-pod and hand-scale rows read, as always.
	if len(versions) > 0 && versions[0].Reused {
		if at, ok := kube.TemplateChangedAt(dep.GetUID()); ok && at.After(versions[0].Created) && versions[0].Restarted == str(dep, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt") {
			noteWentBack(types.UID(versions[0].uid), strconv.Itoa(versions[0].Revision), at)
		}
	}
	// A version brought back: when it came back as kubed saw it, or as
	// the scaling events prove (goBackProof); else only a bound, below.
	for i, v := range versions {
		if !v.Reused {
			continue
		}
		// A write of its template is its go-back only when it came before
		// every later version went out (a write back while paused, or a
		// later go-back to it under another number, comes after).
		beforeLater := func(t time.Time) bool {
			for _, o := range versions[:i] {
				if !o.LiveSince.IsZero() && !t.Before(o.LiveSince) || slices.Min(o.held) > v.Revision && !t.Before(o.Created) {
					return false
				}
			}
			return true
		}
		// Seen coming back (remembered from when it ran, or its template's
		// write as it happened).
		if !v.back.IsZero() && beforeLater(v.back) {
			v.LiveSince = v.back
			continue
		}
		if t, ok := kube.SettingsWrittenAtSure(dep.GetUID(), v.Settings); ok && i > 0 && t.After(v.Created) && t.Before(versions[i-1].Created) && beforeLater(t) {
			// Its settings were last written then, before the version after
			// it was made: when it came back (not a later version's write of
			// the same settings).
			v.LiveSince = t
			continue
		}
		// The one running now: when its pod template was last written (the
		// go-back itself).
		if at, live, ok := kube.TemplateChangedAtSure(dep.GetUID()); ok && i == 0 && at.After(v.Created) && wroteIt(dep, v, at, live) {
			v.LiveSince = at
			if v.uid != "" {
				wentBackSureAt.LoadOrStore(v.uid+"/"+strconv.Itoa(v.Revision), at)
			}
			continue
		}
		tookOver := func(since, at time.Time) bool {
			// v's template written then, as kubed's watch saw it: the go-back
			if w, ok := kube.TemplateWrittenAt(dep.GetUID(), v.Settings, v.Restarted); !ok || w.Sub(at).Abs() > 5*time.Second {
				return false
			}
			for _, p := range e.list(kube.Pods, dep.GetNamespace()) { // a pod of v, not stopping, made by since
				if ref := metav1.GetControllerOfNoCopy(p); ref != nil && string(ref.UID) == v.uid && v.uid != "" && p.GetDeletionTimestamp() == nil && !p.GetCreationTimestamp().After(since) {
					return true
				}
			}
			return false
		}
		if t, ok := log.goBackProof(dep, versions, v, tookOver); ok {
			v.LiveSince = t
		}
	}
	if len(versions) > 0 {
		r.Current = versions[0]
	}
	for i := len(versions) - 1; i >= 0; i-- { // oldest first: each against the one below, already settled
		v := versions[i]
		if !v.Reused {
			continue
		}
		// Nothing dates it, or what does says it came back before a version
		// it came back over was made: only the bound is known.
		if b := goBackBound(versions, v); !b.IsZero() && !v.LiveSince.After(b) {
			v.LiveSince, v.BackAfter = time.Time{}, b
		}
		// Given its number after the revision just below went out: no
		// earlier than that, as what changed keeps its rows in order.
		if i+1 < len(versions) {
			below := wentOut(versions[i+1])
			if v.LiveSince.IsZero() && below.After(v.BackAfter) || !v.LiveSince.IsZero() && !v.LiveSince.After(below) {
				v.LiveSince, v.BackAfter = time.Time{}, below
			}
		}
	}
	if len(versions) > 1 {
		r.Previous = versions[1]
		r.Older = len(versions) - 2
	}
	return r
}

// DeployTime is when a workload's current version went out. Before means
// Kubernetes no longer keeps the version that did it (only restarts of it
// are left), so it went out at At or earlier.
type DeployTime struct {
	At     time.Time
	Before bool
	// After means it went out at At or later: what Kubernetes keeps can't
	// say when a version brought back came back (its events expired, or
	// were merged with others), only that it was after the version made
	// before it.
	After bool
	// ImageSince is when the images it runs first went out, when earlier
	// than At (the last change only touched settings: env, resources…).
	ImageSince time.Time
	// Since, with Before: the workload was made then, so it went out no
	// earlier (between Since and At).
	Since time.Time
	// Unknown: it was brought back at At (or, with After, at At or later),
	// and the version it replaced is gone: that one may have run the same
	// settings, so when they first went out is unknown (at At or earlier).
	Unknown bool
	// Pending: a newer version with other settings (not a restart) has no
	// ready pod yet, so At is still the version serving (not while paused).
	Pending bool
	// Returning: that version is an earlier one brought back (a go-back),
	// not a newer one.
	Returning bool
}

// restartStamped: a version made by a restart (kubectl rollout restart).
func restartStamped(o *unstructured.Unstructured, template ...string) bool {
	path := append(slices.Clone(template), "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	v, found, _ := unstructured.NestedString(o.Object, path...)
	return found && v != ""
}

// DeployTimes is, for each Deployment in a namespace ("" = all), when its
// running version really went out: a restart (the same settings, a new
// stamp) is not a deploy; a version brought back counts from when it came
// back. It reads the ReplicaSets once, for home's every refresh.
func (e *Enricher) DeployTimes(ns string) map[types.UID]DeployTime {
	type version struct {
		rev      int
		settings string
		created  time.Time
		reused   bool
		rs       *unstructured.Unstructured
	}
	byDep := map[types.UID][]version{}
	depName := map[types.UID]string{}
	for _, rs := range e.list(kube.ReplicaSets, ns) {
		ref := metav1.GetControllerOf(rs)
		if ref == nil || ref.Kind != "Deployment" {
			continue
		}
		rev, _ := strconv.Atoi(rs.GetAnnotations()["deployment.kubernetes.io/revision"])
		_, reused := rs.GetAnnotations()["deployment.kubernetes.io/revision-history"]
		byDep[ref.UID] = append(byDep[ref.UID], version{rev, templateSettings(rs), rs.GetCreationTimestamp().Time, reused, rs})
		depName[ref.UID] = ref.Name
	}
	out := make(map[types.UID]DeployTime, len(byDep))
	pending, returning := map[types.UID]bool{}, map[types.UID]bool{}
	var log *scaleLog
	for uid, all := range byDep {
		sort.Slice(all, func(i, j int) bool { return all[i].rev > all[j].rev }) // newest first
		// kept: every version, a stuck new one too (a go-back is dated
		// against all of them).
		kept := all
		// New versions with nothing ready yet (a rollout under way, or one
		// stuck): Deployed is when the version still serving went out, as
		// the guide promises, until the new one serves.
		k := 0
		for k+1 < len(all) && num(all[k].rs, "status", "readyReplicas") == 0 {
			k++
		}
		if k > 0 && num(all[k].rs, "status", "readyReplicas") > 0 {
			// A newer one that runs other settings (not a restart) is rolling
			// out: Deployed says so beside the time it keeps. Not while
			// paused: that has its own words.
			if slices.ContainsFunc(all[:k], func(o version) bool { return o.settings != all[k].settings }) {
				if dep, ok := e.get(kube.Deployments, all[k].rs.GetNamespace(), depName[uid]); ok {
					if paused, _, _ := unstructured.NestedBool(dep.Object, "spec", "paused"); !paused {
						pending[uid] = true
						returning[uid] = slices.ContainsFunc(all[:k], func(o version) bool { return o.reused && o.settings != all[k].settings })
					}
				}
			}
			all = all[k:]
		}
		// Back past restarts by the numbers each went out under: the
		// version that went out under the number just below, not the one
		// numbered just below now (going back renumbers a version).
		rss := make([]*unstructured.Unstructured, 0, len(kept))
		for _, o := range kept {
			rss = append(rss, o.rs)
		}
		first, firstNum := restartedFrom(rss, all[0].rs, all[0].rev)
		i := slices.IndexFunc(all, func(o version) bool { return o.rs == first })
		if rev, held, _ := heldNumbers(first); i < 0 || firstNum != rev {
			// It went out under a number it has had since: when it was
			// made, its first; a later one, nothing dates any more.
			dt := DeployTime{At: first.GetCreationTimestamp().Time, After: true}
			if firstNum == slices.Min(held) {
				dt.After = false
				dt.Before = heldBy(rss, firstNum-1) == nil && restartStamped(first, "spec", "template")
			} else if p := heldBy(rss, firstNum-1); p == nil {
				// The one it replaced is gone: it may have been a restart of
				// an older one. Only that it was brought back after it was
				// made is known.
				dt.Unknown = true
			} else if p.GetCreationTimestamp().After(dt.At) {
				dt.At = p.GetCreationTimestamp().Time // after the one it replaced was made
			}
			out[uid] = e.madeSince(dt, all[0].rs.GetNamespace(), depName[uid])
			continue
		}
		v := all[i]
		when, after := v.created, false
		if v.reused {
			// Brought back: when it came back as the versions list says it,
			// exact or a bound, so both say one time.
			after = true
			if dep, ok := e.get(kube.Deployments, v.rs.GetNamespace(), depName[uid]); ok {
				if log == nil {
					l := e.scaleLog(ns) // read once for every Deployment here
					log = &l
				}
				for _, r := range e.rolloutOf(dep, *log, rss).All {
					if r.Name == v.rs.GetName() {
						when, after = r.LiveSince, false
						if when.IsZero() {
							when, after = r.BackAfter, true
						}
					}
				}
			}
		}
		// The oldest version kept is itself only a restart: the real
		// change is older than anything Kubernetes still has.
		before := heldBy(rss, v.rev-1) == nil && !v.reused && restartStamped(v.rs, "spec", "template")
		// Brought back, and the version it replaced is gone: that one may
		// have been a restart of this, which then went out earlier. When it
		// came back is still said, as such.
		unknown := v.reused && v.rev > 1 && heldBy(rss, v.rev-1) == nil
		dt := DeployTime{At: when, Before: before, After: after, Unknown: unknown}
		images := strings.Join(templateImages(v.rs), ",")
		j := i
		for j+1 < len(all) && strings.Join(templateImages(all[j+1].rs), ",") == images && !OtherBuild(templateRefs(all[j+1].rs), templateRefs(v.rs)) {
			j++ // an older version running the same images (settings changed since)
		}
		if j > i && all[j].created.Before(when) {
			dt.ImageSince = all[j].created
		}
		out[uid] = e.madeSince(dt, all[0].rs.GetNamespace(), depName[uid])
	}
	for uid := range pending { // no time moves: only said beside it
		dt := out[uid]
		dt.Pending, dt.Returning = true, returning[uid]
		out[uid] = dt
	}
	return out
}

// madeSince bounds a "then or earlier" deploy time below by when its
// Deployment was made: it can't have gone out before that.
func (e *Enricher) madeSince(dt DeployTime, ns, name string) DeployTime {
	if !dt.Before {
		return dt
	}
	if dep, ok := e.get(kube.Deployments, ns, name); ok {
		if made := dep.GetCreationTimestamp().Time; !made.IsZero() && !made.After(dt.At) {
			dt.Since = made
		}
	}
	return dt
}

// heldBy is the version that went out under number n, while still kept.
func heldBy(kept []*unstructured.Unstructured, n int) *unstructured.Unstructured {
	for _, o := range kept {
		if _, held, _ := heldNumbers(o); slices.Contains(held, n) {
			return o
		}
	}
	return nil
}

// restartedFrom follows a version (rs, under number n) back past the
// restarts before it by the numbers they went out under: the version that
// went out under n-1 with the same settings, and so on. It returns the
// first of them and the number it went out under.
func restartedFrom(kept []*unstructured.Unstructured, rs *unstructured.Unstructured, n int) (*unstructured.Unstructured, int) {
	for {
		p := heldBy(kept, n-1)
		if p == nil || templateSettings(p) != templateSettings(rs) {
			return rs, n
		}
		rs, n = p, n-1
	}
}

// RevisionDeployTime is when a StatefulSet's or DaemonSet's current
// version went out: its oldest revision that differs from it only by
// restarts. ok is false when the revisions aren't known.
func (e *Enricher) RevisionDeployTime(owner *unstructured.Unstructured) (DeployTime, bool) {
	back := e.broughtBack(owner)
	type rev struct {
		n         int64
		settings  string
		created   time.Time
		restarted bool
		name      string
	}
	var revs []rev
	for _, cr := range e.list(kube.ControllerRevisions, owner.GetNamespace()) {
		ref := metav1.GetControllerOf(cr)
		if ref == nil || ref.UID != owner.GetUID() {
			continue
		}
		data, _, _ := unstructured.NestedMap(cr.Object, "data")
		n, _ := number(cr, "revision")
		key := fmt.Sprintf("%s/%d", cr.GetUID(), cr.GetGeneration())
		settings, cached := settingsCache.Load(key)
		if !cached {
			settings = templateSettingsOf(&unstructured.Unstructured{Object: data})
			settingsCache.Store(key, settings)
		}
		created := cr.GetCreationTimestamp().Time
		if at, ok := back[cr.GetName()]; ok {
			created = at // it went out (again) then
		}
		revs = append(revs, rev{n, settings.(string), created, restartStamped(cr, "data", "spec", "template"), cr.GetName()})
	}
	if len(revs) == 0 {
		return DeployTime{}, false
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i].n > revs[j].n }) // newest first
	// A newer revision no pod runs yet (OnDelete, a partition holding all
	// back, or set a moment ago) hasn't gone out: the one its pods run did.
	if num(owner, "status", "observedGeneration") >= num(owner, "metadata", "generation") {
		switch owner.GetKind() {
		case "StatefulSet":
			cur := str(owner, "status", "currentRevision")
			if num(owner, "status", "updatedReplicas") == 0 && num(owner, "status", "replicas") > 0 && cur != "" && cur != str(owner, "status", "updateRevision") {
				if k := slices.IndexFunc(revs, func(r rev) bool { return r.name == cur }); k > 0 {
					revs = revs[k:]
				}
			}
		case "DaemonSet":
			if num(owner, "status", "updatedNumberScheduled") == 0 && num(owner, "status", "currentNumberScheduled") > 0 && len(revs) > 1 {
				// The newest revision one of its pods runs (a pod's
				// controller-revision-hash names "<daemon set>-<hash>"); if
				// none says, at least not the newest.
				runs := map[string]bool{}
				for _, p := range e.list(kube.Pods, owner.GetNamespace()) {
					if ref := metav1.GetControllerOfNoCopy(p); ref == nil || ref.UID != owner.GetUID() {
						continue // a daemon set owns its pods itself
					}
					if h := p.GetLabels()["controller-revision-hash"]; h != "" {
						runs[owner.GetName()+"-"+h] = true
					}
				}
				if k := slices.IndexFunc(revs, func(r rev) bool { return runs[r.name] }); k > 0 {
					revs = revs[k:]
				} else if k < 0 {
					revs = revs[1:]
				}
			}
		}
	}
	i := 0
	for i+1 < len(revs) && revs[i].settings == revs[i+1].settings {
		i++ // revs[i] only restarted revs[i+1]
	}
	dt := DeployTime{At: revs[i].created, Before: i == len(revs)-1 && revs[i].restarted}
	if made := owner.GetCreationTimestamp().Time; dt.Before && !made.IsZero() && !made.After(dt.At) {
		dt.Since = made // made then: it went out no earlier
	}
	return dt, true
}

// templateSettings is a ReplicaSet's pod template as text, leaving out what
// changes on every restart (the restart stamp, the template hash label).
func templateSettings(rs *unstructured.Unstructured) string {
	key := fmt.Sprintf("%s/%d", rs.GetUID(), rs.GetGeneration()) // status updates don't change the template
	if v, ok := settingsCache.Load(key); ok && rs.GetUID() != "" {
		return v.(string)
	}
	out := templateSettingsOf(rs)
	settingsCache.Store(key, out)
	return out
}

// settingsCache keeps templateSettings per ReplicaSet spec: home works
// out deploy times on every refresh, and templates rarely change.
var settingsCache sync.Map

func templateSettingsOf(rs *unstructured.Unstructured) string {
	t, ok, _ := unstructured.NestedMap(rs.Object, "spec", "template")
	if !ok {
		return ""
	}
	return kube.TemplateSettings(t)
}

// Summary is one line for lists: "3/3 on revision 42" or "2 of 3 updated".
func (r Rollout) Summary() string {
	switch {
	case r.Current == nil:
		return "no versions yet"
	case r.Stuck:
		return fmt.Sprintf("stuck: %d of %d new pods ready", r.Current.Ready, r.Desired)
	case r.Done():
		return fmt.Sprintf("%d/%d on revision %d", r.Available, r.Desired, r.Current.Revision)
	}
	return fmt.Sprintf("rolling out: %d of %d updated, %d available", r.Updated, r.Desired, r.Available)
}

// ImageChange describes what the current version changed vs the previous.
func (r Rollout) ImageChange() string {
	if r.Current == nil || r.Previous == nil {
		return ""
	}
	prev := map[string]bool{}
	for _, i := range r.Previous.Images {
		prev[i] = true
	}
	var changed []string
	for i, img := range r.Current.Images {
		if !prev[img] {
			from := "?"
			if i < len(r.Previous.Images) {
				from = r.Previous.Images[i]
			}
			changed = append(changed, from+" → "+img)
		}
	}
	if len(changed) == 0 {
		if r.Current.Settings == r.Previous.Settings {
			return "same images: only restarted"
		}
		return "same images (config or env changed)"
	}
	return strings.Join(changed, ", ")
}

// broughtBack is, for each of a StatefulSet's or DaemonSet's
// ControllerRevisions (by name), when it was brought back: going back
// renumbers that one revision, so its revision is written well after it
// was made. A write shared by several of them at once is something else
// (an upgrade, an adoption), not a go-back.
func (e *Enricher) broughtBack(o *unstructured.Unstructured) map[string]time.Time {
	written := map[string]time.Time{}
	for _, cr := range e.list(kube.ControllerRevisions, o.GetNamespace()) {
		if ref := metav1.GetControllerOf(cr); ref == nil || ref.UID != o.GetUID() {
			continue
		}
		// Written well after it was made (seconds count: a go-back can come
		// right after a change), not at its making.
		if at, ok := kube.TemplateChangedAt(cr.GetUID()); ok && at.After(cr.GetCreationTimestamp().Add(5*time.Second)) {
			written[cr.GetName()] = at
		}
	}
	out := map[string]time.Time{}
	for name, at := range written {
		alone := true
		for other, t := range written {
			if other != name && t.Sub(at).Abs() < 5*time.Second {
				alone = false
			}
		}
		if alone {
			out[name] = at
		}
	}
	return out
}

// revisionRollout is RolloutOf for a StatefulSet or DaemonSet: their
// versions are ControllerRevisions, and their pods carry the revision
// they run in the controller-revision-hash label.
func (e *Enricher) revisionRollout(o *unstructured.Unstructured) Rollout {
	r := Rollout{Behind: num(o, "status", "observedGeneration") < num(o, "metadata", "generation")}
	back := e.broughtBack(o)
	if o.GetKind() == "DaemonSet" {
		r.Desired, r.Updated = num(o, "status", "desiredNumberScheduled"), num(o, "status", "updatedNumberScheduled")
		r.Ready, r.Available = num(o, "status", "numberReady"), num(o, "status", "numberAvailable")
	} else {
		r.Desired, r.Updated = num(o, "spec", "replicas"), num(o, "status", "updatedReplicas")
		r.Ready, r.Available = num(o, "status", "readyReplicas"), num(o, "status", "availableReplicas")
	}
	var pods []*unstructured.Unstructured // their pods are their own, no ReplicaSet between
	for _, p := range e.list(kube.Pods, o.GetNamespace()) {
		if ref := metav1.GetControllerOf(p); ref != nil && ref.UID == o.GetUID() {
			pods = append(pods, p)
		}
	}
	var versions []*Version
	for _, cr := range e.list(kube.ControllerRevisions, o.GetNamespace()) {
		ref := metav1.GetControllerOf(cr)
		if ref == nil || ref.UID != o.GetUID() {
			continue
		}
		data, _, _ := unstructured.NestedMap(cr.Object, "data")
		t := &unstructured.Unstructured{Object: data}
		n, _ := number(cr, "revision")
		v := &Version{
			Name: cr.GetName(), Revision: int(n), Images: templateImages(t), Refs: templateRefs(t),
			Settings: templateSettingsOf(t), Created: cr.GetCreationTimestamp().Time,
			Restarted: str(t, "spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt"),
		}
		if at, ok := back[cr.GetName()]; ok {
			v.Reused, v.LiveSince = true, at
		}
		for _, p := range pods {
			// A StatefulSet's pods carry the revision's name, a DaemonSet's only its hash.
			if h := p.GetLabels()["controller-revision-hash"]; h != "" && (h == cr.GetName() || o.GetName()+"-"+h == cr.GetName()) {
				v.Replicas++
				if c, ok := Condition(p, "Ready"); ok && c.Status == "True" {
					v.Ready++
				}
			}
		}
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Revision > versions[j].Revision })
	r.All = versions
	if len(versions) > 0 {
		r.Current = versions[0]
		// Going back reuses a kept revision, which keeps its first creation
		// time. The template was written well after it, and its pods came
		// then: it was brought back at that time.
		if at, ok := kube.TemplateChangedAt(o.GetUID()); ok && at.After(r.Current.Created.Add(5*time.Second)) {
			first := time.Time{}
			for _, p := range pods {
				if h := p.GetLabels()["controller-revision-hash"]; h == r.Current.Name || o.GetName()+"-"+h == r.Current.Name {
					if t := p.GetCreationTimestamp().Time; first.IsZero() || t.Before(first) {
						first = t
					}
				}
			}
			if !first.Before(at.Add(-time.Minute)) { // its pods are that new too (none: scaled to 0)
				r.Current.Reused, r.Current.LiveSince = true, at
			}
		}
	}
	if len(versions) > 1 {
		r.Previous = versions[1]
		r.Older = len(versions) - 2
	}
	return r
}
