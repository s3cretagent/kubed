package kube

import "testing"

func TestLookupAliases(t *testing.T) {
	coreEvents := Resource{GVR: Events, Kind: "Event", Namespaced: true, ShortNames: []string{"ev"}, Singular: "event"}
	newEvents := Resource{GVR: Events, Kind: "Event", Namespaced: true, Singular: "event"}
	newEvents.GVR.Group = "events.k8s.io"
	deploy := Resource{GVR: Deployments, Kind: "Deployment", Namespaced: true, ShortNames: []string{"deploy"}, Singular: "deployment"}
	r := NewResources([]Resource{newEvents, coreEvents, deploy})

	for alias, want := range map[string]string{
		"events": "", "ev": "", "event": "", "events.events.k8s.io": "events.k8s.io",
		"deploy": "apps", "Deployment": "apps", "deployments.apps": "apps",
	} {
		res, ok := r.Lookup(alias)
		if !ok || res.GVR.Group != want {
			t.Errorf("Lookup(%q) = %v (group %q), want group %q", alias, ok, res.GVR.Group, want)
		}
	}
	if _, ok := r.Lookup("nope"); ok {
		t.Error("unknown alias resolved")
	}
}
