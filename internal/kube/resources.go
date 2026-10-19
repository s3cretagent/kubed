package kube

import (
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// Resource is one listable API resource.
type Resource struct {
	GVR        schema.GroupVersionResource
	Kind       string
	Namespaced bool
	Verbs      []string
	ShortNames []string
	Singular   string
}

// Can reports whether the resource supports a verb (list, watch, delete…).
func (r Resource) Can(verb string) bool {
	for _, v := range r.Verbs {
		if v == verb {
			return true
		}
	}
	return false
}

// Resources indexes discovered resources by every name kubectl accepts:
// plural, singular, short names, kind, and "plural.group".
type Resources struct {
	byAlias map[string]Resource
	all     []Resource
}

// Discover asks the API server for its preferred resource versions. Partial
// discovery failures (a broken aggregated API) are tolerated.
func Discover(d discovery.DiscoveryInterface) (*Resources, error) {
	lists, err := d.ServerPreferredResources()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		return nil, err
	}
	var all []Resource
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		for _, api := range list.APIResources {
			if strings.Contains(api.Name, "/") { // subresource
				continue
			}
			res := Resource{
				GVR:        gv.WithResource(api.Name),
				Kind:       api.Kind,
				Namespaced: api.Namespaced,
				Verbs:      api.Verbs,
				ShortNames: api.ShortNames,
				Singular:   api.SingularName,
			}
			if res.Can("list") && res.Can("watch") {
				all = append(all, res)
			}
		}
	}
	return NewResources(all), nil
}

// NewResources indexes a set of resources by every name kubectl accepts.
func NewResources(all []Resource) *Resources {
	r := &Resources{byAlias: map[string]Resource{}, all: all}
	for _, res := range all {
		r.add(res.GVR.Resource+"."+res.GVR.Group, res)
		names := append([]string{res.GVR.Resource, res.Singular, strings.ToLower(res.Kind)}, res.ShortNames...)
		for _, n := range names {
			r.addIfFree(n, res)
		}
	}
	sort.Slice(r.all, func(i, j int) bool { return r.all[i].GVR.Resource < r.all[j].GVR.Resource })
	return r
}

// add always wins; addIfFree keeps the first owner so core resources beat
// same-named CRDs (e.g. "events" from core vs events.k8s.io).
func (r *Resources) add(alias string, res Resource) { r.byAlias[strings.ToLower(alias)] = res }

func (r *Resources) addIfFree(alias string, res Resource) {
	alias = strings.ToLower(alias)
	if alias == "" {
		return
	}
	if existing, ok := r.byAlias[alias]; ok && (existing.GVR.Group == "" || res.GVR.Group != "") {
		return
	}
	r.byAlias[alias] = res
}

// Lookup resolves a name typed by the user.
func (r *Resources) Lookup(name string) (Resource, bool) {
	res, ok := r.byAlias[strings.ToLower(name)]
	return res, ok
}

// Has reports whether a GVR is served by this cluster.
func (r *Resources) Has(gvr schema.GroupVersionResource) bool {
	for _, res := range r.all {
		if res.GVR == gvr {
			return true
		}
	}
	return false
}

// ByGVR finds a resource by group+resource, ignoring version.
func (r *Resources) ByGVR(gvr schema.GroupVersionResource) (Resource, bool) {
	for _, res := range r.all {
		if res.GVR.Group == gvr.Group && res.GVR.Resource == gvr.Resource {
			return res, true
		}
	}
	return Resource{}, false
}

// ByKind finds a resource by API group and kind (used for owner references).
func (r *Resources) ByKind(group, kind string) (Resource, bool) {
	for _, res := range r.all {
		if res.GVR.Group == group && res.Kind == kind {
			return res, true
		}
	}
	return Resource{}, false
}

// All returns every listable resource, sorted by name.
func (r *Resources) All() []Resource { return r.all }
