package kube

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// ErrPickNamespace means the user may not list a resource across all
// namespaces, only inside one.
var ErrPickNamespace = errors.New("you can only list this in a namespace you have access to. Pick one with :ns <name>")

const resync = 10 * time.Minute

// watchScope is where an informer watches: one namespace, or all ("").
type watchScope struct {
	gvr schema.GroupVersionResource
	ns  string
}

type watch struct {
	inf  cache.SharedIndexInformer
	stop chan struct{}
}

// Cache holds informers, started the first time a resource is asked for.
// Every read after that is served from memory.
//
// Resources are watched cluster-wide. If the API answers "forbidden" (a
// user whose role covers only some namespaces), that resource switches to
// watching the current namespace instead.
type Cache struct {
	dyn dynamic.Interface

	mu        sync.Mutex
	watches   map[watchScope]*watch
	errs      map[watchScope]error
	forbidden map[schema.GroupVersionResource]bool
	wanted    map[schema.GroupVersionResource]bool // asked for with Watch
	ns        string                               // current namespace, for forbidden resources
	stopped   bool

	// Namespaced reports whether a resource lives in namespaces; only those
	// can fall back to namespaced watching. Nil means "assume namespaced".
	Namespaced func(schema.GroupVersionResource) bool
}

// NewCache creates an empty cache. Nothing is watched until asked for.
func NewCache(dyn dynamic.Interface) *Cache {
	ResetPodWatch(time.Now()) // a new cluster: what its pods did before now, kubed didn't see
	return &Cache{
		dyn:       dyn,
		watches:   map[watchScope]*watch{},
		errs:      map[watchScope]error{},
		forbidden: map[schema.GroupVersionResource]bool{},
		wanted:    map[schema.GroupVersionResource]bool{},
	}
}

// SetNamespace tells the cache which namespace to watch for resources the
// user cannot list cluster-wide.
func (c *Cache) SetNamespace(ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ns = ns
	if ns == "" {
		return
	}
	for gvr := range c.wanted { // forbidden ones now watch this namespace
		if c.forbidden[gvr] {
			c.informerLocked(watchScope{gvr: gvr, ns: ns})
		}
	}
}

// scopeFor picks cluster-wide, or the namespace for forbidden resources.
func (c *Cache) scopeFor(gvr schema.GroupVersionResource, ns string) watchScope {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.forbidden[gvr] {
		return watchScope{gvr: gvr}
	}
	if ns == "" {
		ns = c.ns
	}
	return watchScope{gvr: gvr, ns: ns}
}

// Watch starts watching gvr if needed. It does not wait for the first list.
// Reading (List, Get, Synced) never starts a watch: screens say up front
// what they need, so lists nobody looks at don't compete for bandwidth.
func (c *Cache) Watch(gvr schema.GroupVersionResource) {
	s := c.scopeFor(gvr, "")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wanted[gvr] = true
	if c.forbidden[gvr] && s.ns == "" {
		return // can't list cluster-wide; SetNamespace starts the namespaced one
	}
	c.informerLocked(s)
}

// lookup returns the informer for a scope if it was started.
func (c *Cache) lookup(s watchScope) cache.SharedIndexInformer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.watches[s]; ok {
		return w.inf
	}
	return nil
}

func (c *Cache) informerLocked(s watchScope) {
	if _, ok := c.watches[s]; ok {
		return
	}
	inf := dynamicinformer.NewFilteredDynamicInformer(c.dyn, s.gvr, s.ns, resync, cache.Indexers{}, tweakFor(s.gvr)).Informer()
	_ = inf.SetTransform(transformFor(s.gvr))
	_ = inf.SetWatchErrorHandler(func(_ *cache.Reflector, err error) { c.watchFailed(s, err) })
	if s.gvr.Group == "" && s.gvr.Resource == "pods" {
		if s.ns != "" { // pods can't be read cluster-wide: this namespace's are seen from now
			podsMu.Lock()
			watchedFrom[s.ns] = time.Now()
			podsMu.Unlock()
		}
		// A pod deleted at once (pending, finished, evicted) has no
		// deletion time to note on its way out: forget it as it goes.
		_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{DeleteFunc: func(obj any) {
			if gone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = gone.Obj
			}
			if u, ok := obj.(*unstructured.Unstructured); ok {
				forgetPod(u)
			}
		}})
	}
	w := &watch{inf: inf, stop: make(chan struct{})}
	c.watches[s] = w
	if !c.stopped {
		go inf.Run(w.stop)
	}
}

// watchFailed records list/watch errors so the UI can show them. A
// cluster-wide "forbidden" switches the resource to namespaced watching.
func (c *Cache) watchFailed(s watchScope, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	namespaced := c.Namespaced == nil || c.Namespaced(s.gvr)
	if s.ns == "" && namespaced && apierrors.IsForbidden(err) && !c.forbidden[s.gvr] {
		c.forbidden[s.gvr] = true
		if w, ok := c.watches[s]; ok {
			close(w.stop)
			delete(c.watches, s)
		}
		if c.ns != "" {
			c.informerLocked(watchScope{gvr: s.gvr, ns: c.ns})
		}
		return
	}
	c.errs[s] = err
}

// HelmSecretType is the type of the Secrets Helm keeps each release in.
const HelmSecretType = "helm.sh/release.v1"

// tweakFor narrows what the server sends. Secrets leave out Helm's release
// records: they are most of the Secrets on many clusters, each holds a
// whole compressed chart, and the Helm screen reads them on its own.
func tweakFor(gvr schema.GroupVersionResource) dynamicinformer.TweakListOptionsFunc {
	if gvr.Group == "" && gvr.Resource == "secrets" {
		return func(o *metav1.ListOptions) { o.FieldSelector = "type!=" + HelmSecretType }
	}
	return nil
}

// transformFor trims objects before they are stored.
func transformFor(gvr schema.GroupVersionResource) cache.TransformFunc {
	isSecret := gvr.Group == "" && gvr.Resource == "secrets"
	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		// managedFields are large and never shown in tables; the one
		// thing kubed reads from them is noted first.
		NoteTemplateChange(u)
		NotePod(u)
		u.SetManagedFields(nil)
		if isSecret {
			StripSecret(u)
		}
		return u, nil
	}
}

// templateChanged holds, by UID, when a StatefulSet's or DaemonSet's pod
// template was last written: going back to a kept version reuses its
// ControllerRevision, which keeps its old creation time.
var templateChanged sync.Map

// NoteTemplateChange records from an object's managedFields when its pod
// template was last written (by whoever owns it), for TemplateChangedAt.
//
// For a ControllerRevision it is when its revision number was last written:
// going back to it renumbers it, so a time after its creation is when it
// was brought back.
func NoteTemplateChange(u *unstructured.Unstructured) {
	field := `"f:template"`
	switch u.GetKind() {
	case "StatefulSet", "DaemonSet", "Deployment":
	case "ControllerRevision":
		field = `"f:revision"`
	default:
		return
	}
	var last time.Time
	pauser := false // the newest such write also owns spec.paused
	for _, f := range u.GetManagedFields() {
		if f.Subresource != "" || f.Time == nil || f.FieldsV1 == nil || !strings.Contains(f.FieldsV1.GetRawString(), field) {
			continue
		}
		if f.Time.After(last) {
			last = f.Time.Time
			pauser = strings.Contains(f.FieldsV1.GetRawString(), `"f:paused"`)
		}
	}
	if last.IsZero() {
		return
	}
	// sure is last only when it surely is the template's write, for the
	// versions list (TemplateChangedAtSure); the readers of last keep it as
	// it was. kubectl rollout pause and resume write as the same manager as
	// its restart and undo, so unless the watch saw the template change just
	// then, that entry's time may be a pause's. A write while paused rolls
	// nothing out until it is resumed, at a time no write says.
	sure := last
	_, seen := templateChanged.Load(u.GetUID())
	live := seen && time.Since(last) <= time.Minute // seen happen by the watch, not an old write read at start
	if u.GetKind() == "Deployment" {
		paused, _, _ := unstructured.NestedBool(u.Object, "spec", "paused")
		if paused || pauser && (!seen || !live) {
			sure = time.Time{}
		}
	}
	// Whoever wrote the object last owns its whole spec in managedFields,
	// so an edit of replicas alone moves that time too: keep the time the
	// template was first seen as it is now.
	var what any
	if u.GetKind() == "ControllerRevision" {
		what = u.Object["revision"]
	} else {
		what, _, _ = unstructured.NestedFieldNoCopy(u.Object, "spec", "template")
	}
	if u.GetKind() == "Deployment" {
		if t, ok := what.(map[string]any); ok {
			noteSettings(u.GetUID(), TemplateSettings(t), last, sure)
		}
	}
	b, _ := json.Marshal(what) // map keys come out sorted
	now := templateSeen{template: string(b), at: last, sure: sure, live: live && !sure.IsZero()}
	if v, ok := templateChanged.Load(u.GetUID()); ok && v.(templateSeen).template == now.template {
		if u.GetKind() == "Deployment" && last.After(v.(templateSeen).at) && time.Since(last) > time.Minute {
			// Written again while kubed wasn't watching (asleep, a broken
			// watch): it may have gone elsewhere and back meanwhile, so the
			// go-back times seen before can't be trusted.
			forgetTemplateWrites(u.GetUID())
		}
		return // the same template: not changed since
	}
	if t, ok := what.(map[string]any); ok && u.GetKind() == "Deployment" {
		noteTemplateWrite(u.GetUID(), TemplateSettings(t), restartStamp(t), last, sure)
	}
	templateChanged.Store(u.GetUID(), now)
}

// templateSeen is a pod template (as text) and when it was written. sure
// is that time only when it surely is (zero otherwise), and live says the
// watch saw that write as it happened.
type templateSeen struct {
	template string
	at       time.Time
	sure     time.Time
	live     bool
}

// TemplateChangedAt is when an object's pod template was last written, as
// noted by NoteTemplateChange; ok is false when unknown.
func TemplateChangedAt(uid types.UID) (time.Time, bool) {
	v, ok := templateChanged.Load(uid)
	if !ok {
		return time.Time{}, false
	}
	return v.(templateSeen).at, true
}

// TemplateChangedAtSure is TemplateChangedAt only when the time surely is
// the template's write (not a pause's, nor a write while paused); live:
// the watch saw it as it happened.
func TemplateChangedAtSure(uid types.UID) (at time.Time, live, ok bool) {
	v, found := templateChanged.Load(uid)
	if !found || v.(templateSeen).sure.IsZero() {
		return time.Time{}, false, false
	}
	return v.(templateSeen).sure, v.(templateSeen).live, true
}

// StripSecret removes a Secret's values but keeps its keys, so tables can
// count and list them without kubed holding secret data in memory. The
// last-applied annotation is dropped too: `kubectl apply` writes the whole
// Secret, values included, into it.
func StripSecret(u *unstructured.Unstructured) {
	for _, field := range []string{"data", "stringData"} {
		if m, ok := u.Object[field].(map[string]any); ok {
			for k := range m {
				m[k] = ""
			}
		}
	}
	if a := u.GetAnnotations(); a != nil {
		delete(a, "kubectl.kubernetes.io/last-applied-configuration")
		u.SetAnnotations(a)
	}
}

// Synced reports whether the first list for gvr has completed.
func (c *Cache) Synced(gvr schema.GroupVersionResource) bool {
	s := c.scopeFor(gvr, "")
	if c.isForbidden(gvr) && s.ns == "" {
		return false
	}
	inf := c.lookup(s)
	return inf != nil && inf.HasSynced()
}

// SyncedIn reports whether the cache holds a complete list of gvr for
// namespace ns (cluster-wide, or that namespace's own watch), so a missing
// object really is gone.
func (c *Cache) SyncedIn(gvr schema.GroupVersionResource, ns string) bool {
	inf := c.lookup(c.scopeFor(gvr, ns))
	return inf != nil && inf.HasSynced()
}

func (c *Cache) isForbidden(gvr schema.GroupVersionResource) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forbidden[gvr]
}

// Watching reports whether some screen asked for gvr.
func (c *Cache) Watching(gvr schema.GroupVersionResource) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wanted[gvr]
}

// Err returns the last list/watch error for gvr, or ErrPickNamespace when
// it can only be listed per namespace and "all namespaces" is selected.
func (c *Cache) Err(gvr schema.GroupVersionResource) error {
	s := c.scopeFor(gvr, "")
	if c.isForbidden(gvr) && s.ns == "" {
		return ErrPickNamespace
	}
	inf := c.lookup(s)
	if inf == nil {
		return nil // not watched (yet)
	}
	synced := inf.HasSynced()
	c.mu.Lock()
	defer c.mu.Unlock()
	if synced {
		delete(c.errs, s)
		return nil
	}
	return c.errs[s]
}

// List returns objects of gvr in namespace ns ("" = all), sorted by
// namespace/name; nothing if gvr isn't watched.
func (c *Cache) List(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	s := c.scopeFor(gvr, ns)
	if c.isForbidden(gvr) && s.ns == "" {
		return nil
	}
	inf := c.lookup(s)
	if inf == nil {
		return nil
	}
	var out []*unstructured.Unstructured
	for _, item := range inf.GetStore().List() {
		u, ok := item.(*unstructured.Unstructured)
		if !ok || (ns != "" && u.GetNamespace() != ns) {
			continue
		}
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetNamespace() != out[j].GetNamespace() {
			return out[i].GetNamespace() < out[j].GetNamespace()
		}
		return out[i].GetName() < out[j].GetName()
	})
	return out
}

// Get returns one object from the cache.
func (c *Cache) Get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	s := c.scopeFor(gvr, ns)
	if c.isForbidden(gvr) && s.ns == "" {
		return nil, false
	}
	key := name
	if ns != "" {
		key = ns + "/" + name
	}
	inf := c.lookup(s)
	if inf == nil {
		return nil, false
	}
	item, ok, err := inf.GetStore().GetByKey(key)
	if err != nil || !ok {
		return nil, false
	}
	u, ok := item.(*unstructured.Unstructured)
	return u, ok
}

// Stop ends every informer.
func (c *Cache) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	for _, w := range c.watches {
		close(w.stop)
	}
	c.watches = map[watchScope]*watch{}
}

// podsLive is, per pod owner (a ReplicaSet, StatefulSet…), when each of its
// pods kubed has seen running was made; podHadOlder is, per pod, whether
// it joined a running set: a pod older by 2 minutes, or one that had
// itself joined older ones, was running when it was made. podsMade keeps
// every pod seen (gone ones too) for an hour; watchedFrom is, for a
// namespace whose pods are watched on their own (no cluster-wide right),
// when that watch started (else it is when kubed started).
var (
	podsMu      sync.Mutex
	podsLive    = map[types.UID]map[types.UID]time.Time{}
	podHadOlder = map[types.UID]bool{}
	podsMade    = map[types.UID]map[types.UID]time.Time{}
	watchedFrom = map[string]time.Time{}
	watchSince  = time.Now()
)

// NotePod records a pod as kubed's watch sees it arrive or leave, so what
// changed can tell a pod that replaced one (an older pod was beside it)
// from one made with its version, after those older pods are gone.
func NotePod(u *unstructured.Unstructured) {
	if u.GetKind() != "Pod" {
		return
	}
	ref := metav1.GetControllerOf(u)
	if ref == nil {
		return
	}
	podsMu.Lock()
	defer podsMu.Unlock()
	made := u.GetCreationTimestamp().Time
	if all := podsMade[ref.UID]; all == nil {
		podsMade[ref.UID] = map[types.UID]time.Time{u.GetUID(): made}
	} else {
		all[u.GetUID()] = made
		for uid, t := range all { // an hour is what what-changed can say about them
			if time.Since(t) > time.Hour {
				delete(all, uid)
			}
		}
	}
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	live := podsLive[ref.UID]
	if u.GetDeletionTimestamp() != nil || phase == "Succeeded" || phase == "Failed" { // on its way out, or done
		delete(live, u.GetUID())
		return
	}
	if live == nil {
		live = map[types.UID]time.Time{}
		podsLive[ref.UID] = live
	}
	if _, seen := live[u.GetUID()]; !seen {
		// Pods older than it were running when it was made; those it is
		// older than were made beside it. Either can arrive first (a list
		// comes in name order), so both ways are marked.
		older := podHadOlder[u.GetUID()]
		for uid, t := range live {
			older = older || t.Before(made.Add(-2*time.Minute)) || (podHadOlder[uid] && t.Before(made))
		}
		podHadOlder[u.GetUID()] = older
		for uid, t := range live {
			if t.After(made.Add(2*time.Minute)) || (older && t.After(made)) {
				podHadOlder[uid] = true
			}
		}
	}
	live[u.GetUID()] = made
}

// forgetPod drops a deleted pod from the pods kubed sees running.
func forgetPod(u *unstructured.Unstructured) {
	ref := metav1.GetControllerOf(u)
	if ref == nil {
		return
	}
	podsMu.Lock()
	defer podsMu.Unlock()
	delete(podsLive[ref.UID], u.GetUID())
	delete(podHadOlder, u.GetUID())
}

// PodHadOlder reports whether, when kubed saw a pod appear, a pod of the
// same owner was running that was older by 2 minutes or had itself
// joined older ones: it joined a running set rather than came with a
// version.
func PodHadOlder(uid types.UID) bool {
	podsMu.Lock()
	defer podsMu.Unlock()
	return podHadOlder[uid]
}

// PodsMadeBetween counts the pods of an owner in namespace ns kubed's
// watch saw made in [from, to), gone ones too. seen is false when kubed
// wasn't watching that namespace's pods yet at from: it can't know about
// pods that came and went before.
func PodsMadeBetween(owner types.UID, ns string, from, to time.Time) (n int, seen bool) {
	podsMu.Lock()
	defer podsMu.Unlock()
	for _, t := range podsMade[owner] {
		if !t.Before(from) && t.Before(to) {
			n++
		}
	}
	start, ok := watchedFrom[ns] // a namespace watched on its own: from when it started
	if !ok {
		start = watchSince
	}
	return n, start.Before(from)
}

// ResetPodWatch forgets the pods kubed has seen and starts watching again
// from since (a new cache, a new cluster).
func ResetPodWatch(since time.Time) {
	podsMu.Lock()
	defer podsMu.Unlock()
	podsLive = map[types.UID]map[types.UID]time.Time{}
	podHadOlder = map[types.UID]bool{}
	podsMade = map[types.UID]map[types.UID]time.Time{}
	watchedFrom = map[string]time.Time{}
	watchSince = since
}

// settingsHistory keeps, per Deployment, the pod template settings it was
// seen with (a restart's stamp left out) and when each was written: a
// version brought back and then restarted came back when its settings
// were last written, whoever looked meanwhile.
var (
	settingsMu      sync.Mutex
	settingsHistory = map[types.UID][]templateSeen{}
)

func noteSettings(uid types.UID, settings string, at, sure time.Time) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := settingsHistory[uid]
	if n := len(h); n > 0 && h[n-1].template == settings {
		return // the same settings (a restart, a replicas edit): not changed
	}
	if time.Since(at) > time.Minute {
		// Not seen as it happened (kubed starting, waking, reconnecting):
		// the time may be a later restart's or edit's, not when these
		// settings were written. Unknown, so not said.
		at, sure = time.Time{}, time.Time{}
	}
	h = append(h, templateSeen{template: settings, at: at, sure: sure})
	if len(h) > 20 {
		h = h[len(h)-20:]
	}
	settingsHistory[uid] = h
}

// SettingsWrittenAt is when a Deployment's pod template was last given
// these settings (TemplateSettings), as kubed saw; ok is false when not seen.
func SettingsWrittenAt(uid types.UID, settings string) (time.Time, bool) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := settingsHistory[uid]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].template == settings {
			return h[i].at, !h[i].at.IsZero()
		}
	}
	return time.Time{}, false
}

// SettingsWrittenAtSure is SettingsWrittenAt only when that write's time
// surely is when they were written (not while paused).
func SettingsWrittenAtSure(uid types.UID, settings string) (time.Time, bool) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := settingsHistory[uid]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].template == settings {
			return h[i].sure, !h[i].sure.IsZero()
		}
	}
	return time.Time{}, false
}

// templateWrites keeps, per Deployment, each pod template it was given
// (its restart stamp too) and when, as kubed's watch saw it: going back to
// a kept version writes that version's template again, and the time stays
// known after newer rollouts, whether or not anyone looked meanwhile.
var templateWrites = map[types.UID][]templateSeen{}

func noteTemplateWrite(uid types.UID, settings, stamp string, at, sure time.Time) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := templateWrites[uid]
	if time.Since(at) > time.Minute {
		// Not seen as it happened: unknown, as in noteSettings. Writes
		// between may have been missed too (a version gone back to again),
		// so the times seen before can't be trusted either.
		at, sure = time.Time{}, time.Time{}
		for i := range h {
			h[i].at, h[i].sure = time.Time{}, time.Time{}
		}
	}
	h = append(h, templateSeen{template: settings + "\x00" + stamp, at: at, sure: sure})
	if len(h) > 20 {
		h = h[len(h)-20:]
	}
	templateWrites[uid] = h
}

// forgetTemplateWrites makes the write times seen for a Deployment
// unknown: writes may have been missed since.
func forgetTemplateWrites(uid types.UID) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	for i := range templateWrites[uid] {
		templateWrites[uid][i].at, templateWrites[uid][i].sure = time.Time{}, time.Time{}
	}
}

// TemplateWrittenAt is when a Deployment's pod template was last set to
// these settings with this restart stamp (one ReplicaSet's template), as
// kubed saw; ok is false when not seen.
func TemplateWrittenAt(uid types.UID, settings, stamp string) (time.Time, bool) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := templateWrites[uid]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].template == settings+"\x00"+stamp {
			return h[i].at, !h[i].at.IsZero()
		}
	}
	return time.Time{}, false
}

// TemplateWrittenAtSure is TemplateWrittenAt only when that write's time
// surely is when the template was written (not while paused).
func TemplateWrittenAtSure(uid types.UID, settings, stamp string) (time.Time, bool) {
	settingsMu.Lock()
	defer settingsMu.Unlock()
	h := templateWrites[uid]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].template == settings+"\x00"+stamp {
			return h[i].sure, !h[i].sure.IsZero()
		}
	}
	return time.Time{}, false
}

func restartStamp(t map[string]any) string {
	s, _, _ := unstructured.NestedString(t, "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	return s
}

// TemplateSettings is a pod template as text, leaving out what changes on
// every restart (the restart stamp, the template hash label).
func TemplateSettings(t map[string]any) string {
	t = runtime.DeepCopyJSON(t)
	unstructured.RemoveNestedField(t, "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	unstructured.RemoveNestedField(t, "metadata", "labels", "pod-template-hash")
	for _, f := range []string{"annotations", "labels"} { // left empty: the same as none
		if m, ok, _ := unstructured.NestedMap(t, "metadata", f); ok && len(m) == 0 {
			unstructured.RemoveNestedField(t, "metadata", f)
		}
	}
	if m, ok, _ := unstructured.NestedMap(t, "metadata"); ok && len(m) == 0 {
		unstructured.RemoveNestedField(t, "metadata")
	}
	b, _ := json.Marshal(t) // map keys come out sorted
	return string(b)
}
