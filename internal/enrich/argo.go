package enrich

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/kube"
)

// ArgoCD marks the resources it manages with this annotation
// (value "<app>:<group>/<kind>:<namespace>/<name>"), or with the instance
// label when tracking by label.
const (
	annotationTrackingID = "argocd.argoproj.io/tracking-id"
	labelInstance        = "app.kubernetes.io/instance"
)

// ArgoApp is the state of an ArgoCD Application.
type ArgoApp struct {
	Name      string
	Namespace string
	Project   string
	Sync      string
	Health    string
	Revision  string
	AutoSync  bool
	SelfHeal  bool
	Prune     bool
	Found     bool // false when the Application can't be read here (another cluster, no access, or not loaded yet)
	Sources   []AppSource
	Resources []ArgoResource // what it manages (status.resources)
	// BuildError: ArgoCD can't turn Git into resources (a ComparisonError),
	// so changes in Git won't reach the cluster.
	BuildError string
}

// ArgoResource is one resource an Application manages.
type ArgoResource struct {
	Group, Kind, Namespace, Name string
	OutOfSync                    bool
}

// String is "kind/name", the way people say it.
func (r ArgoResource) String() string { return strings.ToLower(r.Kind) + "/" + r.Name }

func argoResources(app *unstructured.Unstructured) []ArgoResource {
	raw, _, _ := unstructured.NestedSlice(app.Object, "status", "resources")
	out := make([]ArgoResource, 0, len(raw))
	for _, r := range raw {
		m, _ := r.(map[string]any)
		s := func(k string) string { v, _ := m[k].(string); return v }
		out = append(out, ArgoResource{Group: s("group"), Kind: s("kind"), Namespace: s("namespace"), Name: s("name"),
			OutOfSync: s("status") == "OutOfSync"})
	}
	return out
}

// AppSource is one entry of spec.source / spec.sources.
type AppSource struct {
	RepoURL, Path, Chart, TargetRevision, Ref string
}

func appSources(app *unstructured.Unstructured) []AppSource {
	var raw []any
	if src, ok, _ := unstructured.NestedMap(app.Object, "spec", "source"); ok {
		raw = []any{src}
	} else {
		raw, _, _ = unstructured.NestedSlice(app.Object, "spec", "sources")
	}
	out := make([]AppSource, 0, len(raw))
	for _, r := range raw {
		m, _ := r.(map[string]any)
		s := func(k string) string { v, _ := m[k].(string); return v }
		out = append(out, AppSource{RepoURL: s("repoURL"), Path: s("path"), Chart: s("chart"), TargetRevision: s("targetRevision"), Ref: s("ref")})
	}
	return out
}

// Application reads an Application object.
func Application(app *unstructured.Unstructured) ArgoApp {
	a := ArgoApp{Name: app.GetName(), Namespace: app.GetNamespace(), Found: true}
	a.Project, _, _ = unstructured.NestedString(app.Object, "spec", "project")
	a.Sync, _, _ = unstructured.NestedString(app.Object, "status", "sync", "status")
	a.Health, _, _ = unstructured.NestedString(app.Object, "status", "health", "status")
	a.Revision, _, _ = unstructured.NestedString(app.Object, "status", "sync", "revision")
	if revs, _, _ := unstructured.NestedStringSlice(app.Object, "status", "sync", "revisions"); a.Revision == "" && len(revs) > 0 {
		a.Revision = revs[0] // multi-source apps
	}
	a.Sources = appSources(app)
	conds, _, _ := unstructured.NestedSlice(app.Object, "status", "conditions")
	for _, c := range conds {
		if m, ok := c.(map[string]any); ok && (m["type"] == "ComparisonError" || m["type"] == "InvalidSpecError") {
			a.BuildError = fmt.Sprint(m["message"])
		}
	}
	a.Resources = argoResources(app)
	if auto, ok, _ := unstructured.NestedMap(app.Object, "spec", "syncPolicy", "automated"); ok {
		// An automated block without enabled:false means auto-sync is on.
		enabled, found, _ := unstructured.NestedBool(auto, "enabled")
		a.AutoSync = !found || enabled
		a.SelfHeal, _, _ = unstructured.NestedBool(auto, "selfHeal")
		a.Prune, _, _ = unstructured.NestedBool(auto, "prune")
	}
	return a
}

// ManagedBy returns the ArgoCD Application that manages obj, looking through
// its owners (a Pod is managed through its Deployment).
func (e *Enricher) ManagedBy(obj *unstructured.Unstructured) (ArgoApp, bool) {
	top := e.TopOwner(obj)
	name := appNameFromTracking(top.GetAnnotations()[annotationTrackingID])
	if name == "" {
		// Label tracking: only trust it when such an Application exists,
		// since app.kubernetes.io/instance is also set by plain Helm.
		name = top.GetLabels()[labelInstance]
		if name == "" {
			return ArgoApp{}, false
		}
		app, ok := e.findApp(name)
		if ok && !app.coversNamespace(obj.GetNamespace()) {
			// The same label in another namespace (a job some tool started)
			// isn't this app's: label tracking is only a hint.
			return ArgoApp{}, false
		}
		return app, ok
	}
	if app, ok := e.findApp(name); ok {
		return app, true
	}
	return ArgoApp{Name: name}, true
}

// coversNamespace: the app manages something in namespace ns (or says
// nothing about namespaces yet, so it isn't ruled out).
func (a ArgoApp) coversNamespace(ns string) bool {
	if ns == "" || len(a.Resources) == 0 {
		return true
	}
	for _, r := range a.Resources {
		if r.Namespace == ns {
			return true
		}
	}
	return false
}

// OwnersLoading reports whether ArgoCD ownership of obj cannot be decided
// yet: an owner's list (e.g. ReplicaSets) or the Applications list has not
// finished loading, so "not managed" would be a guess.
func (e *Enricher) OwnersLoading(obj *unstructured.Unstructured) bool {
	if g, ok := e.served(kube.Applications); ok && !e.store.Synced(g) {
		return true
	}
	if e.res == nil {
		return false
	}
	cur := obj
	for range 4 {
		ref, ok := controllerRef(cur)
		if !ok {
			return false
		}
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return false
		}
		res, ok := e.res.ByKind(gv.Group, ref.Kind)
		if !ok {
			return false
		}
		if w, ok := e.store.(interface {
			Watching(schema.GroupVersionResource) bool
		}); ok && !w.Watching(res.GVR) {
			return false // nobody loads that kind: it will never "finish loading"
		}
		if !e.store.Synced(res.GVR) {
			return true
		}
		next := e.controller(cur)
		if next == nil {
			return false
		}
		cur = next
	}
	return false
}

// appNameFromTracking extracts the app from a tracking id. Apps outside the
// argocd namespace are written "<namespace>_<app>".
func appNameFromTracking(id string) string {
	app, _, _ := strings.Cut(id, ":")
	return app
}

func (e *Enricher) findApp(tracked string) (ArgoApp, bool) {
	ns, name, qualified := strings.Cut(tracked, "_")
	if !qualified {
		ns, name = "", tracked
	}
	for _, app := range e.list(kube.Applications, ns) {
		if app.GetName() == name {
			return Application(app), true
		}
	}
	return ArgoApp{}, false
}
