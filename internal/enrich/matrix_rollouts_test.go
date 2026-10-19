package enrich

// Case map: when versions went out, came back, or were only restarted.
//
// A property test. A generator writes random histories of one Deployment;
// a model of the Deployment controller and the event recorder works out
// what Kubernetes keeps; kubed reads only that, and every time and word it
// says is checked against the model's truth.
//
// Dimensions that change what kubed should say:
//   - what each step did: set image, set env, rollout restart, undo to the
//     previous revision, undo to a given revision, set an image or env back
//     to an earlier value (the same template again, a go-back without undo),
//     scale to 0 by hand and back, scale 1 to 2 and back, pause and resume
//     (with template changes while paused);
//   - a version that never gets ready (a broken image): the old one keeps
//     running and is never scaled down, so a go-back from it scales nothing up;
//   - pace: quick steps seconds apart (before a rollout finishes), or slow
//     ones minutes apart (scale events expire after an hour);
//   - replicas 1 or 2 (different scale words), 0 (rollouts make no events);
//   - the event recorder: the same words again within the hour merge into
//     one event (count, first and last time); more than 9 different scaling
//     words with no 10 minute gap become "(combined from similar events)";
//     an event expires an hour after its last time, and if it happens again
//     later it comes back with its old count and first time (the recorder's
//     cache outlives it);
//   - old or new event words ("from 0 to 1", "to 1 from 0");
//   - revision history limit (default 10, or 2 or 3: old versions deleted);
//   - who wrote the template (managedFields): kubectl-set, kubectl-rollout
//     (restart, undo, and also pause and resume, which move its time);
//   - kubed's state: watched every write live (the history fits in the last
//     minute), or started later with only what Kubernetes keeps.
//
// Checked on every history (seeded, so a failure reproduces):
//   - what changed (rollout rows of the Deployment): an exact time is the
//     true time to the second; a bound ("then or later") is no later than
//     the truth and no earlier than the version was made; rows go newest
//     first by revision; a restart is never called an image or settings
//     change, and an image or settings change never a restart, whenever the
//     version it replaced is still kept (otherwise kubed may only list the
//     images);
//   - the versions list (RolloutOf): the same time and the same kind
//     (exact or bound) as what changed for every version brought back;
//   - Deployed (DeployTimes): exact only when true, a bound when it says
//     so, and the same time as the versions list for a go-back;
//   - :shipped: never a restart, the same time as what changed.
//
// About 4000 histories (3000 started later, 1000 watched live), none
// skipped; MX_RUNS=30000 searches longer. Seeds once wrong are named in
// mxRegressions.

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

type mxRS struct {
	name, uid         string
	image, env, stamp string
	rev               int
	history           []int
	made              time.Time
	spec, ready       int
	readyAt           time.Time
	broken            bool
	out               map[int]time.Time // revision -> when it went out under it (the truth)
	done              map[int]time.Time // revision -> when the last other version left was scaled to 0
	deleted           bool
}

func (r *mxRS) template() string { return r.image + "|" + r.env + "|" + r.stamp }

type mxEvent struct {
	msg         string
	count       int64
	first, last time.Time
	ttl         time.Duration // an hour, give or take: expiry isn't in order
}

type mxMgr struct {
	at       time.Time
	template bool
}

type mxModel struct {
	rng                *rand.Rand
	uid                string
	t                  time.Time
	rss                []*mxRS
	image, env, stamp  string
	paused             bool
	replicas           int
	newWords           bool
	limit              int
	events             map[string]*mxEvent
	order              []string // event keys, first seen first
	aggKeys            []string
	aggLast            time.Time
	mgr                map[string]*mxMgr
	reason, status     string
	updated            time.Time
	log                []string
	live               bool
	nextName           int
	lastTemplateWriter string
	jitter             *rand.Rand // its own stream: the histories stay as they were
	tokens             float64    // the event spam filter's, for this Deployment
	tokensAt           time.Time
	handScaled         bool // a scale by hand of a finished rollout: Progressing stays
}

func (m *mxModel) logf(format string, a ...any) {
	m.log = append(m.log, m.t.Format("15:04:05")+" "+fmt.Sprintf(format, a...))
}

func (m *mxModel) kept() []*mxRS {
	var out []*mxRS
	for _, r := range m.rss {
		if !r.deleted {
			out = append(out, r)
		}
	}
	return out
}

func (m *mxModel) cur() *mxRS {
	var c *mxRS
	for _, r := range m.kept() {
		if c == nil || r.rev > c.rev {
			c = r
		}
	}
	return c
}

// emit records a scaling event as client-go's recorder would: the
// aggregator first (10 different words with no 10 minute gap: combined),
// then the logger (the same key again: count and last time).
func (m *mxModel) emit(msg string) {
	if m.t.Sub(m.aggLast) > 10*time.Minute {
		m.aggKeys = nil
	}
	m.aggLast = m.t
	if !slices.Contains(m.aggKeys, msg) {
		m.aggKeys = append(m.aggKeys, msg)
	}
	key, text := msg, msg
	if len(m.aggKeys) >= 10 {
		i := m.rng.Intn(len(m.aggKeys))
		m.aggKeys = append(m.aggKeys[:i], m.aggKeys[i+1:]...)
		key, text = "\x00combined", "(combined from similar events): "+msg
	}
	// kube-controller-manager's spam filter, per Deployment: 25 at once,
	// then one every 5 minutes. A dropped event is never written, and its
	// count isn't kept either.
	if m.tokensAt.IsZero() {
		m.tokens = 25
	} else {
		m.tokens = min(25, m.tokens+m.t.Sub(m.tokensAt).Seconds()/300)
	}
	m.tokensAt = m.t
	if m.tokens < 1 {
		mxDropped++
		return
	}
	m.tokens--
	if ev, ok := m.events[key]; ok {
		ev.count++
		ev.last = m.t
		ev.msg = text // the patch carries the message too
		return
	}
	m.events[key] = &mxEvent{msg: text, count: 1, first: m.t, last: m.t, ttl: time.Hour + time.Duration(m.jitter.Intn(61))*time.Second}
	m.order = append(m.order, key)
}

func (m *mxModel) scale(r *mxRS, to int) {
	if r.spec == to {
		return
	}
	dir := "up"
	if to < r.spec {
		dir = "down"
	}
	words := fmt.Sprintf("Scaled %s replica set %s from %d to %d", dir, r.name, r.spec, to)
	if m.newWords {
		words = fmt.Sprintf("Scaled %s replica set %s to %d from %d", dir, r.name, to, r.spec)
	}
	m.emit(words)
	if to > r.spec {
		r.readyAt = m.t.Add(time.Duration(1+m.rng.Intn(8)) * time.Second)
	}
	r.spec = to
	r.ready = min(r.ready, to)
	if n := m.cur(); to == 0 && r != n && n != nil {
		n.done[n.rev] = m.t
	}
}

// surge is maxSurge 25% of replicas, rounded up.
func surge(replicas int) int { return (replicas + 3) / 4 }

// handScale is a scale by hand to n replicas, as the Deployment
// controller's scale() does it: one version running (or none) gets them
// all; several share them in proportion (getProportion's rounding), the
// leftover to the biggest.
func (m *mxModel) handScale(n int) {
	prev := m.replicas
	m.replicas = n
	complete := m.reason == "NewReplicaSetAvailable"
	var active []*mxRS
	for _, r := range m.kept() {
		if r.spec > 0 {
			active = append(active, r)
		}
	}
	switch {
	case len(active) == 0:
		m.scale(m.cur(), n)
	case len(active) == 1:
		m.scale(active[0], n)
	default:
		total := 0
		for _, r := range active {
			total += r.spec
		}
		toAdd := n + surge(n) - total
		annotated := prev + surge(prev)
		// Bigger first; of the same size, newer first when adding, older
		// first when taking away.
		sort.SliceStable(active, func(i, j int) bool {
			if active[i].spec != active[j].spec {
				return active[i].spec > active[j].spec
			}
			if toAdd > 0 {
				return active[i].made.After(active[j].made)
			}
			return active[i].made.Before(active[j].made)
		})
		size := map[*mxRS]int{}
		added := 0
		for _, r := range active {
			size[r] = r.spec
			if toAdd == 0 || toAdd == added {
				continue
			}
			fraction := -r.spec
			if n > 0 {
				fraction = int(math.Round(float64(r.spec*(n+surge(n)))/float64(annotated))) - r.spec
			}
			allowed := toAdd - added
			p := max(fraction, allowed)
			if toAdd > 0 {
				p = min(fraction, allowed)
			}
			size[r] += p
			added += p
		}
		if added != toAdd {
			size[active[0]] += toAdd - added
		}
		for _, r := range active {
			m.scale(r, max(size[r], 0))
		}
	}
	m.handScaled = complete
	m.sync()
}

// sync is the Deployment controller at this moment (maxSurge 1,
// maxUnavailable 0), run until nothing changes.
func (m *mxModel) sync() {
	for range 10 {
		changed := false
		progress := false
		for _, r := range m.kept() {
			if r.spec > r.ready && !r.broken && !r.readyAt.IsZero() && !m.t.Before(r.readyAt) {
				r.ready = r.spec
				changed, progress = true, true
			}
		}
		if m.paused {
			break
		}
		n := m.cur()
		total := 0
		var olds []*mxRS
		for _, r := range m.kept() {
			total += r.spec
			if r != n && r.spec > 0 {
				olds = append(olds, r)
			}
		}
		sort.Slice(olds, func(i, j int) bool { return olds[i].made.Before(olds[j].made) })
		switch {
		case m.replicas == 0 || n.spec > m.replicas:
			// A scale: proportional, here everything to the new count.
			for _, r := range olds {
				m.scale(r, 0)
				changed = true
			}
			if n.spec > m.replicas {
				m.scale(n, m.replicas)
				changed = true
			}
		case n.spec < m.replicas && total < m.replicas+1:
			if len(olds) == 0 && n.spec == 0 {
				m.scale(n, m.replicas) // from nothing: all at once
			} else {
				m.scale(n, min(m.replicas, n.spec+m.replicas+1-total))
			}
			changed, progress = true, true
		}
		if m.replicas > 0 {
			// reconcileOldReplicaSets: all pods, the new version's too.
			allPods := 0
			for _, r := range m.kept() {
				allPods += r.spec
			}
			newUnavailable := n.spec - n.ready
			cleanup := allPods - m.replicas - newUnavailable
			for _, r := range olds {
				if cleanup <= 0 {
					break
				}
				if bad := r.spec - r.ready; bad > 0 {
					d := min(bad, cleanup)
					m.scale(r, r.spec-d)
					cleanup -= d
					changed, progress = true, true
				}
			}
			avail := 0
			for _, r := range m.kept() {
				avail += r.ready
			}
			down := avail - m.replicas
			for _, r := range olds {
				if down <= 0 {
					break
				}
				if r.spec > 0 {
					d := min(r.spec, down)
					m.scale(r, r.spec-d)
					down -= d
					changed, progress = true, true
				}
			}
		}
		complete := n.spec == m.replicas && n.ready == m.replicas
		for _, r := range m.kept() {
			complete = complete && (r == n || r.spec == 0)
		}
		switch {
		case complete && m.reason != "NewReplicaSetAvailable":
			m.reason, m.status, m.updated = "NewReplicaSetAvailable", "True", m.t
		case !complete && progress && !m.handScaled:
			m.reason, m.status, m.updated = "ReplicaSetUpdated", "True", m.t
		}
		if complete {
			m.handScaled = false
		}
		if complete {
			// Old versions past the history limit go.
			var idle []*mxRS
			for _, r := range m.kept() {
				if r != n && r.spec == 0 {
					idle = append(idle, r)
				}
			}
			sort.Slice(idle, func(i, j int) bool { return idle[i].rev < idle[j].rev })
			for i := 0; i < len(idle)-m.limit; i++ {
				idle[i].deleted = true
			}
		}
		if !changed {
			break
		}
	}
}

// advance runs the controller up to t: pods getting ready, deadlines.
func (m *mxModel) advance(to time.Time) {
	for {
		next := to
		for _, r := range m.kept() {
			if r.spec > r.ready && !r.broken && !r.readyAt.IsZero() && r.readyAt.After(m.t) && r.readyAt.Before(next) {
				next = r.readyAt
			}
		}
		if m.status == "True" && m.reason == "ReplicaSetUpdated" && !m.paused {
			if d := m.updated.Add(10 * time.Minute); d.After(m.t) && d.Before(next) {
				next = d
			}
		}
		m.t = next
		m.sync()
		if m.status == "True" && m.reason == "ReplicaSetUpdated" && !m.paused && !m.t.Before(m.updated.Add(10*time.Minute)) {
			m.reason, m.status, m.updated = "ProgressDeadlineExceeded", "False", m.t
		}
		if !m.t.Before(to) {
			return
		}
	}
}

func (m *mxModel) newName() string {
	m.nextName++
	return fmt.Sprintf("web-%c%c%d", 'a'+rune(m.nextName%26), 'k'+rune(m.nextName%7), m.nextName)
}

// rollout gives the Deployment's template its version: a new one, or a
// kept one with the same template brought back under the next number.
func (m *mxModel) rollout() {
	m.handScaled = false
	want := m.image + "|" + m.env + "|" + m.stamp
	n := m.cur()
	maxRev := 0
	var same *mxRS
	for _, r := range m.kept() {
		maxRev = max(maxRev, r.rev)
		if r.template() == want {
			same = r
		}
	}
	switch {
	case n != nil && same == n:
	case same != nil:
		same.history = append(same.history, same.rev)
		same.rev = maxRev + 1
		same.out[same.rev] = m.t
		m.logf("  %s back as revision %d", same.name, same.rev)
	default:
		name := m.newName()
		r := &mxRS{name: name, uid: m.uid + "-" + name, image: m.image, env: m.env, stamp: m.stamp,
			rev: maxRev + 1, made: m.t, broken: strings.HasPrefix(m.image, "bad"), out: map[int]time.Time{}, done: map[int]time.Time{}}
		r.out[r.rev] = m.t
		m.rss = append(m.rss, r)
		m.logf("  %s made as revision %d", name, r.rev)
	}
	m.sync()
}

func (m *mxModel) write(manager string) {
	m.mgr[manager] = &mxMgr{at: m.t, template: true}
	m.lastTemplateWriter = manager
}

// step does one random thing a user does; false when it did nothing.
func (m *mxModel) step() bool {
	images := []string{"nginx:1", "nginx:2", "nginx:3", "bad:1"}
	envs := []string{"", "a", "b"}
	switch k := m.rng.Intn(100); {
	case k < 22: // set image
		img := images[m.rng.Intn(len(images))]
		if img == m.image {
			return false
		}
		m.logf("set image %s", img)
		m.image = img
	case k < 34: // set env
		env := envs[m.rng.Intn(len(envs))]
		if env == m.env {
			return false
		}
		m.logf("set env %q", env)
		m.env = env
	case k < 50: // restart
		stamp := m.t.UTC().Format(time.RFC3339)
		if stamp == m.stamp {
			return false
		}
		m.logf("restart")
		m.stamp = stamp
		m.write("kubectl-rollout")
		if !m.paused {
			m.rollout()
		}
		return true
	case k < 72: // undo, to the previous revision or a given one
		if m.paused {
			return false
		}
		kept := m.kept()
		sort.Slice(kept, func(i, j int) bool { return kept[i].rev > kept[j].rev })
		if len(kept) < 2 {
			return false
		}
		to := kept[1]
		if m.rng.Intn(3) == 0 {
			to = kept[1+m.rng.Intn(len(kept)-1)]
		}
		if to.template() == m.image+"|"+m.env+"|"+m.stamp {
			return false
		}
		m.logf("undo to revision %d", to.rev)
		m.image, m.env, m.stamp = to.image, to.env, to.stamp
		m.write("kubectl-rollout")
		m.rollout()
		return true
	case k < 80: // scale to 0 and back, by hand
		if m.paused {
			return false
		}
		if m.replicas == 0 {
			to := 1 + m.rng.Intn(2)
			m.logf("scale to %d", to)
			m.handScale(to)
		} else {
			m.logf("scale to 0")
			m.handScale(0)
		}
		return true
	case k < 86: // scale 1 to 2 and back
		if m.paused || m.replicas == 0 {
			return false
		}
		m.logf("scale to %d", 3-m.replicas)
		m.handScale(3 - m.replicas)
		return true
	case k < 93: // pause or resume
		if mg := m.mgr["kubectl-rollout"]; mg != nil {
			mg.at = m.t // its entry owns spec.paused too now
		} else {
			m.mgr["kubectl-rollout"] = &mxMgr{at: m.t}
		}
		m.paused = !m.paused
		if m.paused {
			m.logf("pause")
		} else {
			m.logf("resume")
			m.rollout()
		}
		return true
	default: // set an image back to one an earlier version runs
		kept := m.kept()
		r := kept[m.rng.Intn(len(kept))]
		if r.image == m.image || r.stamp != m.stamp {
			return false
		}
		m.logf("set image %s (as %s)", r.image, r.name)
		m.image = r.image
	}
	m.write("kubectl-set")
	if !m.paused {
		m.rollout()
	}
	return true
}

func mxTemplate(image, env, stamp, hash string) map[string]any {
	meta := map[string]any{"labels": map[string]any{"app": "web"}}
	if hash != "" {
		meta["labels"].(map[string]any)["pod-template-hash"] = hash
	}
	if stamp != "" {
		meta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
	}
	c := map[string]any{"name": "web", "image": image}
	if env != "" {
		c["env"] = []any{map[string]any{"name": "MODE", "value": env}}
	}
	return map[string]any{"metadata": meta, "spec": map[string]any{"containers": []any{c}}}
}

const mxNS = "mx"

func (m *mxModel) depObj() *unstructured.Unstructured {
	n := m.cur()
	ready := int64(0)
	for _, r := range m.kept() {
		ready += int64(r.ready)
	}
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": mxNS, "uid": m.uid,
			"annotations": map[string]any{"deployment.kubernetes.io/revision": strconv.Itoa(n.rev)}},
		"spec": map[string]any{"replicas": int64(m.replicas), "paused": m.paused, "template": mxTemplate(m.image, m.env, m.stamp, "")},
		"status": map[string]any{"replicas": int64(n.spec), "updatedReplicas": int64(n.spec), "readyReplicas": ready, "availableReplicas": ready,
			"conditions": []any{map[string]any{"type": "Progressing", "status": m.status, "reason": m.reason,
				"lastUpdateTime": m.updated.UTC().Format(time.RFC3339)}}}}}
	var fields []metav1.ManagedFieldsEntry
	names := make([]string, 0, len(m.mgr))
	for name := range m.mgr {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		mg := m.mgr[name]
		raw := `{"f:spec":{"f:paused":{}}}`
		if mg.template {
			raw = `{"f:spec":{"f:paused":{},"f:template":{"f:spec":{}}}}`
		}
		at := metav1.NewTime(mg.at)
		fields = append(fields, metav1.ManagedFieldsEntry{Manager: name, Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &at, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(raw)})
	}
	d.SetManagedFields(fields)
	return d
}

func (m *mxModel) rsObj(r *mxRS) *unstructured.Unstructured {
	ann := map[string]any{"deployment.kubernetes.io/revision": strconv.Itoa(r.rev)}
	if len(r.history) > 0 {
		var h []string
		for _, n := range r.history {
			h = append(h, strconv.Itoa(n))
		}
		ann["deployment.kubernetes.io/revision-history"] = strings.Join(h, ",")
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": r.name, "namespace": mxNS, "uid": r.uid, "annotations": ann,
			"creationTimestamp": r.made.UTC().Format(time.RFC3339),
			"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": m.uid, "controller": true}}},
		"spec":   map[string]any{"replicas": int64(r.spec), "template": mxTemplate(r.image, r.env, r.stamp, r.name)},
		"status": map[string]any{"replicas": int64(r.spec), "readyReplicas": int64(r.ready)}}}
}

// mxHistory makes one seeded history and the store kubed reads at now.
func mxHistory(seed int64, live bool, now time.Time) (*mxModel, fakeStore) {
	rng := rand.New(rand.NewSource(seed))
	m := &mxModel{rng: rng, jitter: rand.New(rand.NewSource(-seed)), uid: fmt.Sprintf("mx-%v-%d", live, seed), events: map[string]*mxEvent{},
		mgr: map[string]*mxMgr{}, newWords: rng.Intn(2) == 0, limit: 10, live: live, replicas: 1}
	if rng.Intn(4) == 0 {
		m.limit = 2 + rng.Intn(2)
	}
	if rng.Intn(4) == 0 {
		m.replicas = 2
	}
	steps := 2 + rng.Intn(10)
	if m.jitter.Intn(5) == 0 {
		steps += 20 // a busy one: the spam filter drops events
	}
	var start time.Time
	quick := live || rng.Intn(3) == 0
	if live {
		start = now.Add(-55 * time.Second)
	} else {
		start = now.Add(-time.Duration(10+rng.Intn(180)) * time.Minute)
	}
	m.t = start
	m.image = "nginx:1"
	m.logf("create (replicas %d, limit %d, new words %v)", m.replicas, m.limit, m.newWords)
	m.mgr["kubectl-create"] = &mxMgr{at: m.t, template: true}
	m.rollout()
	if live {
		kube.NoteTemplateChange(m.depObj())
	}
	for i := 0; i < steps; i++ {
		var gap time.Duration
		switch {
		case live:
			gap = time.Duration(1+rng.Intn(5)) * time.Second
		case quick || rng.Intn(2) == 0:
			gap = time.Duration(1+rng.Intn(20)) * time.Second
		default:
			gap = time.Duration(1+rng.Intn(25)) * time.Minute
		}
		next := m.t.Add(gap)
		if !next.Before(now.Add(-time.Second)) {
			break
		}
		m.advance(next)
		if m.step() && live {
			kube.NoteTemplateChange(m.depObj())
		}
	}
	m.advance(now)
	if !live {
		kube.NoteTemplateChange(m.depObj()) // what kubed sees when it starts
	}
	store := fakeStore{kube.Deployments: {m.depObj()}}
	for _, r := range m.kept() {
		store[kube.ReplicaSets] = append(store[kube.ReplicaSets], m.rsObj(r))
	}
	for i, key := range m.order {
		ev := m.events[key]
		if now.Sub(ev.last) > ev.ttl {
			continue // expired
		}
		store[kube.Events] = append(store[kube.Events], &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": fmt.Sprintf("web.%d", i), "namespace": mxNS}, "reason": "ScalingReplicaSet", "message": ev.msg, "count": ev.count,
			"type":           "Normal",
			"involvedObject": map[string]any{"kind": "Deployment", "name": "web", "namespace": mxNS, "uid": m.uid},
			"firstTimestamp": ev.first.UTC().Format(time.RFC3339), "lastTimestamp": ev.last.UTC().Format(time.RFC3339)}})
	}
	return m, store
}

// holder is the version that went out under revision n, kept or not.
func (m *mxModel) holder(n int) *mxRS {
	for _, r := range m.rss {
		if _, ok := r.out[n]; ok {
			return r
		}
	}
	return nil
}

// kindOf is what revision n changed against revision n-1: "restart",
// "settings", "image", or "" for the first; known: n-1 is still kept, so
// kubed can tell.
func (m *mxModel) kindOf(n int) (kind string, known bool) {
	r, p := m.holder(n), m.holder(n-1)
	if r == nil || p == nil {
		return "", false
	}
	switch {
	case r.image != p.image:
		kind = "image"
	case r.env != p.env:
		kind = "settings"
	default:
		kind = "restart"
	}
	return kind, !p.deleted
}

func (m *mxModel) dump(store fakeStore) string {
	var b strings.Builder
	b.WriteString(strings.Join(m.log, "\n"))
	b.WriteString("\nversions:\n")
	for _, r := range m.rss {
		var outs []string
		for n, t := range r.out {
			outs = append(outs, fmt.Sprintf("%d@%s", n, t.Format("15:04:05")))
		}
		sort.Strings(outs)
		fmt.Fprintf(&b, "  %s rev %d made %s %s spec %d ready %d deleted %v out %v\n", r.name, r.rev, r.made.Format("15:04:05"), r.template(), r.spec, r.ready, r.deleted, outs)
	}
	b.WriteString("events:\n")
	for _, ev := range store[kube.Events] {
		fmt.Fprintf(&b, "  %s x%d %s..%s\n", str(ev, "message"), EventCount(ev), str(ev, "firstTimestamp")[11:19], str(ev, "lastTimestamp")[11:19])
	}
	return b.String()
}

var mxRevRow = regexp.MustCompile(`^revision (\d+)(?: \(later brought back\))?: (went back to an earlier version: )?(.*)$`)

// mxCheck reads one history the ways kubed shows it and returns what is
// untrue, or "".
func mxCheck(m *mxModel, store fakeStore, now time.Time) []string {
	var bad []string
	e := New(store, testResources(), nil)
	dep := store[kube.Deployments][0]
	tf := func(t time.Time) string { return t.UTC().Format("15:04:05") }
	var checkTime func(where string, got time.Time, bound bool, truth, made time.Time)
	// checkTime: exact must be the truth; a bound no later than it and no
	// earlier than the version was made.
	// A go-back's exact time is when it began (its template written, the
	// version scaled up), never when its rollout ended.
	checkBack := func(where string, got time.Time, bound bool, r *mxRS, n int) {
		checkTime(where, got, bound, r.out[n], r.made)
	}
	checkTime = func(where string, got time.Time, bound bool, truth, made time.Time) {
		switch {
		case !bound && !got.Equal(truth):
			bad = append(bad, fmt.Sprintf("%s: says %s exactly, truth %s", where, tf(got), tf(truth)))
		case bound && (got.After(truth) || got.Before(made)):
			bad = append(bad, fmt.Sprintf("%s: says %s or later, truth %s, made %s", where, tf(got), tf(truth), tf(made)))
		}
	}
	byName := map[string]*mxRS{}
	for _, r := range m.rss {
		byName[r.name] = r
	}

	// What changed.
	type row struct {
		when  time.Time
		after bool
	}
	goBack := map[int]row{}
	last := 1 << 30
	var changes []Change
	for _, c := range e.Changes(mxNS, "", now, nil) {
		if c.What != "rollout" || c.Object != "deployment/web" {
			continue
		}
		changes = append(changes, c)
		mt := mxRevRow.FindStringSubmatch(c.Detail)
		if mt == nil {
			bad = append(bad, "what changed: a row kubed can't have meant: "+c.Detail)
			continue
		}
		n, _ := strconv.Atoi(mt[1])
		first := strings.Contains(c.Detail, "(later brought back)")
		r := m.holder(n)
		if r == nil {
			bad = append(bad, "what changed: no version went out as revision "+mt[1]+": "+c.Detail)
			continue
		}
		where := "what changed, " + c.Detail
		if n >= last {
			bad = append(bad, fmt.Sprintf("%s: listed below revision %d", where, last))
		}
		last = n
		switch {
		case first:
			if n != slices.Min(append([]int{r.rev}, r.history...)) {
				bad = append(bad, where+": not its first number")
			}
			checkTime(where, c.When, c.After, r.made, r.made)
		case mt[2] != "":
			if r.rev != n || len(r.history) == 0 {
				bad = append(bad, where+": not a version brought back under that number")
			}
			checkBack(where, c.When, c.After, r, n)
			goBack[n] = row{c.When, c.After}
		default:
			if len(r.history) > 0 && r.rev == n {
				bad = append(bad, where+": a version brought back, not said so")
			}
			checkTime(where, c.When, c.After, r.out[n], r.made)
		}
		say := mt[3]
		switch kind, known := m.kindOf(n); {
		case strings.HasPrefix(say, "restarted, same images") && kind != "restart":
			bad = append(bad, fmt.Sprintf("%s: called a restart, truth %q", where, kind))
		case strings.HasPrefix(say, "settings changed") && kind != "settings":
			bad = append(bad, fmt.Sprintf("%s: called a settings change, truth %q", where, kind))
		case !strings.Contains(say, "same images") && kind == "restart" && known:
			bad = append(bad, where+": a restart not called one")
		case !strings.Contains(say, "same images") && kind == "settings" && known:
			bad = append(bad, where+": a settings change not called one")
		case strings.Contains(say, "same images") && kind == "image":
			bad = append(bad, where+": an image change said to keep the images")
		}
	}

	// The versions list.
	ro := e.RolloutOf(dep)
	for _, v := range ro.All {
		r := byName[v.Name]
		if !v.Reused {
			continue
		}
		where := fmt.Sprintf("versions list, revision %d", v.Revision)
		switch {
		case !v.LiveSince.IsZero():
			checkBack(where, v.LiveSince, false, r, v.Revision)
		case !v.BackAfter.IsZero():
			checkBack(where, v.BackAfter, true, r, v.Revision)
		}
		g, ok := goBack[v.Revision]
		if !ok {
			continue // its row is older than the window: not here
		}
		at, after := v.LiveSince, false
		if at.IsZero() {
			at, after = v.BackAfter, true
		}
		if !at.Equal(g.when) || after != g.after {
			bad = append(bad, fmt.Sprintf("%s: says %s (after %v), what changed %s (after %v)", where, tf(at), after, tf(g.when), g.after))
		}
	}

	// Deployed: the version serving, back past its restarts.
	dt, ok := e.DeployTimes(mxNS)[types.UID(m.uid)]
	if ok {
		kept := m.kept()
		sort.Slice(kept, func(i, j int) bool { return kept[i].rev > kept[j].rev })
		k := 0
		for k+1 < len(kept) && kept[k].ready == 0 {
			k++
		}
		if kept[k].ready == 0 {
			k = 0
		}
		// The truth walks back past restarts through versions no longer
		// kept too: what kubed can't see, it must not claim.
		s, n := kept[k], kept[k].rev
		unknown := false    // the walk passed a version kubed no longer has
		seen, seenN := s, n // the last of the walk kubed still has
		for {
			p := m.holder(n - 1)
			if p == nil || p.image != s.image || p.env != s.env {
				unknown = unknown || p != nil && p.deleted
				break
			}
			unknown = unknown || p.deleted
			s, n = p, n-1
			if !unknown {
				seen, seenN = s, n
			}
		}
		truth := s.out[n]
		where := fmt.Sprintf("Deployed (%s, revision %d)", s.name, n)
		switch {
		case dt.At.IsZero(): // not known: says nothing
		case dt.Unknown && dt.After: // "brought back after At": the go-back of the last it has
			if back := seen.out[seenN]; dt.At.After(back) {
				bad = append(bad, fmt.Sprintf("%s: says brought back after %s, it was at %s", where, tf(dt.At), tf(back)))
			}
		case dt.Unknown: // brought back at At, its settings out then or earlier
			if back := seen.out[seenN]; !dt.At.Equal(back) || dt.At.Before(truth) {
				bad = append(bad, fmt.Sprintf("%s: says brought back at %s, settings out then or earlier; truth back %s, out %s", where, tf(dt.At), tf(back), tf(truth)))
			}
		case dt.Before:
			if dt.At.Before(truth) {
				bad = append(bad, fmt.Sprintf("%s: says %s or earlier, truth %s", where, tf(dt.At), tf(truth)))
			}
		case dt.After:
			if dt.At.After(truth) {
				bad = append(bad, fmt.Sprintf("%s: says %s or later, truth %s", where, tf(dt.At), tf(truth)))
			}
		case !dt.At.Equal(truth):
			bad = append(bad, fmt.Sprintf("%s: says %s exactly, truth %s", where, tf(dt.At), tf(truth)))
		}
		if len(s.history) > 0 && s.rev == n && !unknown {
			for _, v := range ro.All {
				if v.Name == s.name {
					at, after := v.LiveSince, false
					if at.IsZero() {
						at, after = v.BackAfter, true
					}
					if !at.IsZero() && (!at.Equal(dt.At) || after != dt.After) {
						bad = append(bad, fmt.Sprintf("%s: says %s (after %v), the versions list %s (after %v)", where, tf(dt.At), dt.After, tf(at), after))
					}
				}
			}
		}
	}

	// :shipped: never a restart, the same time as what changed.
	for _, c := range e.Shipped(mxNS, now, ShipWindow) {
		if c.Object != "deployment/web" || c.What != "rollout" {
			continue
		}
		mt := mxRevRow.FindStringSubmatch(c.Detail)
		if mt == nil {
			continue
		}
		n, _ := strconv.Atoi(mt[1])
		if kind, known := m.kindOf(n); !strings.Contains(c.Detail, "(later brought back)") && kind == "restart" && known {
			bad = append(bad, ":shipped lists a restart: "+c.Detail)
		}
		found := false
		for _, o := range changes {
			found = found || o.Detail == c.Detail && o.When.Equal(c.When) && o.After == c.After
		}
		if !found {
			bad = append(bad, ":shipped row not as what changed says: "+c.Detail)
		}
	}
	return bad
}

// mxRegressions are histories kubed once got wrong, by seed (started
// later unless live), each run on its own so a failure names it. The model
// has grown since (the spam filter, proportional scaling, busy histories),
// so a seed's history may differ from the one its name tells; each still
// runs as a fixed case.
var mxRegressions = []struct {
	name string
	seed int64
	live bool
}{
	{"set env after a go-back compared with the version made before", 11, false},
	{"a restart walked to by kept order, not by number (Deployed)", 5, false},
	{"Deployed skipped the version in between", 13, false},
	{"a serving go-back: Deployed and the versions list disagreed", 37, false},
	{"a go-back's stamp read as a restart", 583, false},
	{"an image set seconds after a restart whose version is gone", 75, false},
	{"an image set seconds after a restart whose version is gone (live)", 75, true},
	{"a go-back to a renumbered restart", 98, false},
	{"a broken go-back, then another seconds later", 677, false},
	{"a broken go-back, then another seconds later", 1907, false},
	{"a broken go-back, then another seconds later", 2546, false},
	{"pause moved the template write time", 36, false},
	{"a go-back made while scaled to 0 by hand", 68, false},
	{"a hand scale-down after the version left came back", 28, false},
	{"v replaced the same version twice", 871, false},
}

func TestMatrixRegressions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, c := range mxRegressions {
		m, store := mxHistory(c.seed, c.live, now)
		if bad := mxCheck(m, store, now); len(bad) > 0 {
			t.Errorf("%s (seed %d, live %v):\n  %s\n%s", c.name, c.seed, c.live, strings.Join(bad, "\n  "), m.dump(store))
		}
	}
}

func TestMatrixRolloutHistories(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	runs := []struct {
		live bool
		n    int
	}{{false, 3000}, {true, 1000}}
	if n, err := strconv.Atoi(os.Getenv("MX_RUNS")); err == nil { // a longer search: MX_RUNS=30000
		runs[0].n, runs[1].n = n, n/3
	}
	failures := 0
	kinds := map[string]int{}
	for _, run := range runs {
		for seed := int64(1); seed <= int64(run.n); seed++ {
			m, store := mxHistory(seed, run.live, now)
			bad := mxCheck(m, store, now)
			if len(bad) == 0 {
				continue
			}
			failures++
			for _, b := range bad {
				kinds[mxKind(b)]++
			}
			if f := os.Getenv("MX_FOCUS"); failures <= 3 && (f == "" || strings.Contains(strings.Join(bad, "\n"), f)) {
				t.Errorf("seed %d (live %v):\n  %s\n%s", seed, run.live, strings.Join(bad, "\n  "), m.dump(store))
			}
		}
	}
	t.Logf("%d events dropped by the spam filter", mxDropped)
	if failures > 0 {
		t.Errorf("%d histories with untrue words: %v", failures, kinds)
	}
}

// mxDropped counts the events the spam filter dropped, so the test shows
// it was exercised.
var mxDropped int

// mxKind is a failure's kind, for the count by kind.
func mxKind(b string) string {
	b = regexp.MustCompile(`\d\d:\d\d:\d\d`).ReplaceAllString(b, "T")
	b = regexp.MustCompile(`revision \d+`).ReplaceAllString(b, "revision N")
	b = regexp.MustCompile(`\(web-\w+`).ReplaceAllString(b, "(web")
	if i := strings.Index(b, ": "); i > 0 && strings.HasPrefix(b, "what changed") {
		rest := b[i+2:]
		if j := strings.LastIndex(rest, ": "); j >= 0 {
			rest = rest[j+2:]
		}
		return "what changed: " + rest
	}
	return b
}
