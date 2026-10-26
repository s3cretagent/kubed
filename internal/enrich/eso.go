package enrich

import (
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// SecretSync is the state of one ExternalSecret.
type SecretSync struct {
	Name        string // ExternalSecret name
	Target      string // Secret it writes
	Store       string
	Ready       string // "True", "False" or "" when unknown
	Reason      string
	Message     string
	RefreshedAt time.Time
	Interval    string
}

// ExternalSecret reads sync state from an ExternalSecret object.
func ExternalSecret(es *unstructured.Unstructured) SecretSync {
	s := SecretSync{Name: es.GetName()}
	s.Target, _, _ = unstructured.NestedString(es.Object, "spec", "target", "name")
	if s.Target == "" {
		s.Target = es.GetName()
	}
	s.Store, _, _ = unstructured.NestedString(es.Object, "spec", "secretStoreRef", "name")
	s.Interval, _, _ = unstructured.NestedString(es.Object, "spec", "refreshInterval")
	if ts, _, _ := unstructured.NestedString(es.Object, "status", "refreshTime"); ts != "" {
		s.RefreshedAt, _ = time.Parse(time.RFC3339, ts)
	}
	if c, ok := Condition(es, "Ready"); ok {
		s.Ready, s.Reason, s.Message = c.Status, c.Reason, c.Message
	}
	return s
}

// ExternalSecretsForPod returns the ExternalSecrets that produce the Secrets
// a pod reads (env, envFrom, volumes).
func (e *Enricher) ExternalSecretsForPod(pod *unstructured.Unstructured) []SecretSync {
	used := PodSecretNames(pod)
	if len(used) == 0 {
		return nil
	}
	var out []SecretSync
	for _, es := range e.list(kube.ExternalSecrets, pod.GetNamespace()) {
		s := ExternalSecret(es)
		if used[s.Target] {
			out = append(out, s)
		}
	}
	return out
}

// ExternalSecretOwner returns the ExternalSecret that owns a Secret, if any.
func (e *Enricher) ExternalSecretOwner(secret *unstructured.Unstructured) (SecretSync, bool) {
	for _, ref := range secret.GetOwnerReferences() {
		if ref.Kind != "ExternalSecret" {
			continue
		}
		if es, ok := e.get(kube.ExternalSecrets, secret.GetNamespace(), ref.Name); ok {
			return ExternalSecret(es), true
		}
		return SecretSync{Name: ref.Name, Target: secret.GetName()}, true
	}
	// No owner link (the Secret was there first, or the ExternalSecret
	// merges into it): an ExternalSecret that names it as its target.
	for _, es := range e.list(kube.ExternalSecrets, secret.GetNamespace()) {
		if s := ExternalSecret(es); s.Target == secret.GetName() {
			return s, true
		}
	}
	return SecretSync{}, false
}

// PodSecretNames collects every Secret name referenced by a pod spec.
func PodSecretNames(pod *unstructured.Unstructured) map[string]bool {
	names := map[string]bool{}
	spec, _, _ := unstructured.NestedMap(pod.Object, "spec")
	for _, key := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(spec, key)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			envFrom, _, _ := unstructured.NestedSlice(cm, "envFrom")
			for _, ef := range envFrom {
				if n, _, _ := unstructured.NestedString(ef.(map[string]any), "secretRef", "name"); n != "" {
					names[n] = true
				}
			}
			env, _, _ := unstructured.NestedSlice(cm, "env")
			for _, ev := range env {
				if n, _, _ := unstructured.NestedString(ev.(map[string]any), "valueFrom", "secretKeyRef", "name"); n != "" {
					names[n] = true
				}
			}
		}
	}
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, v := range volumes {
		vm, _ := v.(map[string]any)
		if n, _, _ := unstructured.NestedString(vm, "secret", "secretName"); n != "" {
			names[n] = true
		}
		sources, _, _ := unstructured.NestedSlice(vm, "projected", "sources")
		for _, s := range sources {
			if n, _, _ := unstructured.NestedString(s.(map[string]any), "secret", "name"); n != "" {
				names[n] = true
			}
		}
	}
	return names
}

// SortedKeys returns map keys in order.
func SortedKeys(m map[string]bool) []string {
	return slices.Sorted(maps.Keys(m))
}
