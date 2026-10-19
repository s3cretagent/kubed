package kube

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/kubectl/pkg/describe"
)

// Describe renders `kubectl describe` output for an object. Built-in kinds
// get kubectl's dedicated describers; CRDs get the generic one.
func (c *Client) Describe(r Ref) (string, error) {
	mapping := &meta.RESTMapping{
		Resource:         r.Resource.GVR,
		GroupVersionKind: r.Resource.GVR.GroupVersion().WithKind(r.Resource.Kind),
		Scope:            scope(r.Resource.Namespaced),
	}
	d, ok := describe.DescriberFor(mapping.GroupVersionKind.GroupKind(), c.Config)
	if !ok {
		if d, ok = describe.GenericDescriberFor(mapping, c.Config); !ok {
			return "", fmt.Errorf("no describer for %s", r.Resource.Kind)
		}
	}
	return d.Describe(r.Namespace, r.Name, describe.DescriberSettings{ShowEvents: true, ChunkSize: 500})
}

func scope(namespaced bool) meta.RESTScope {
	if namespaced {
		return meta.RESTScopeNamespace
	}
	return meta.RESTScopeRoot
}
