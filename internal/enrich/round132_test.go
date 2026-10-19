package enrich

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// round132Store: deployment api asks for key apitoken of Secret dbcreds
// and key LEVEL of ConfigMap settings (and optional key extra), and its
// pod waits because apitoken isn't there.
func round132Store(t *testing.T, secretKeys, cmKeys string) fakeStore {
	t.Helper()
	tmpl := `
    metadata: {labels: {app: api}}
    spec:
      containers:
        - name: app
          env:
            - {name: TOKEN, valueFrom: {secretKeyRef: {name: dbcreds, key: apitoken}}}
            - {name: EXTRA, valueFrom: {secretKeyRef: {name: dbcreds, key: extra, optional: true}}}
            - {name: LEVEL, valueFrom: {configMapKeyRef: {name: settings, key: LEVEL}}}
`
	return fakeStore{
		kube.Deployments: {obj(t, `
apiVersion: apps/v1
kind: Deployment
metadata: {name: api, namespace: qa, uid: d1}
spec:
  replicas: 1
  template:`+tmpl)},
		kube.ReplicaSets: {obj(t, `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: api-1
  namespace: qa
  uid: r1
  ownerReferences: [{apiVersion: apps/v1, kind: Deployment, name: api, uid: d1, controller: true}]
spec:
  replicas: 1
  template:`+tmpl)},
		kube.Pods: {obj(t, `
apiVersion: v1
kind: Pod
metadata:
  name: api-1-x
  namespace: qa
  uid: p1
  ownerReferences: [{apiVersion: apps/v1, kind: ReplicaSet, name: api-1, uid: r1, controller: true}]
spec:
  containers:
    - name: app
      env:
        - {name: TOKEN, valueFrom: {secretKeyRef: {name: dbcreds, key: apitoken}}}
status:
  phase: Pending
  containerStatuses:
    - name: app
      ready: false
      state: {waiting: {reason: CreateContainerConfigError, message: "couldn't find key apitoken in Secret qa/dbcreds"}}
`)},
		kube.Secrets: {obj(t, `
apiVersion: v1
kind: Secret
metadata: {name: dbcreds, namespace: qa}
data: {`+secretKeys+`}
`)},
		kube.ConfigMaps: {obj(t, `
apiVersion: v1
kind: ConfigMap
metadata: {name: settings, namespace: qa}
data: {`+cmKeys+`}
`)},
	}
}

func xrayNode(e *Enricher, o *unstructured.Unstructured, kind, name string) *XNode {
	var find func(n *XNode) *XNode
	find = func(n *XNode) *XNode {
		if n.Kind == kind && n.Name == name {
			return n
		}
		for _, c := range n.Children {
			if f := find(c); f != nil {
				return f
			}
		}
		return nil
	}
	return find(e.Xray(o))
}

// QA round 132: X on an app listed "Secret dbcreds" as fine while its pod
// can't start for the key apitoken that isn't in it.
func TestXrayMarksAMissingKey(t *testing.T) {
	store := round132Store(t, "user: ''", "LEVEL: ''")
	e := New(store, nil, nil)
	dep, _ := store.Get(kube.Deployments, "qa", "api")
	pod, _ := store.Get(kube.Pods, "qa", "api-1-x")
	for _, o := range []*unstructured.Unstructured{dep, pod} {
		if n := xrayNode(e, o, "Secret", "dbcreds"); n == nil || n.Health != HealthBad || n.Note != "has no key apitoken" {
			t.Errorf("X on %s: Secret dbcreds %+v, want bad, has no key apitoken", o.GetName(), n)
		}
		if n := xrayNode(e, o, "ConfigMap", "settings"); n != nil && n.Health == HealthBad {
			t.Errorf("X on %s: ConfigMap settings has LEVEL: %+v", o.GetName(), n)
		}
	}

	// A config map that lacks a key the deployment asks for (not optional),
	// with no pod's message to say so: read from its own keys.
	store = round132Store(t, "apitoken: ''", "OTHER: ''")
	store[kube.Pods] = nil
	e = New(store, nil, nil)
	dep, _ = store.Get(kube.Deployments, "qa", "api")
	if n := xrayNode(e, dep, "ConfigMap", "settings"); n == nil || n.Health != HealthBad || n.Note != "has no key LEVEL" {
		t.Errorf("ConfigMap settings %+v, want bad, has no key LEVEL", n)
	}
	// Optional keys (extra) aren't asked for: the Secret has apitoken.
	if n := xrayNode(e, dep, "Secret", "dbcreds"); n == nil || n.Health == HealthBad {
		t.Errorf("Secret dbcreds %+v: has apitoken, extra is optional", n)
	}

	// Secrets kubed can't see: only the pod's own message says the key is
	// missing.
	store = round132Store(t, "user: ''", "LEVEL: ''")
	delete(store, kube.Secrets)
	e = New(store, kube.NewResources([]kube.Resource{
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ConfigMaps, Kind: "ConfigMap", Namespaced: true},
		{GVR: kube.ServiceAccounts, Kind: "ServiceAccount", Namespaced: true},
	}), nil)
	dep, _ = store.Get(kube.Deployments, "qa", "api")
	if n := xrayNode(e, dep, "Secret", "dbcreds"); n == nil || n.Health != HealthBad || n.Note != "has no key apitoken" {
		t.Errorf("unseen Secret dbcreds %+v, want bad from the pod's message", n)
	}
}

// QA round 132: after two go-backs, a pod the go-back made read "added a
// copy: scaled up from 2 to 3": the only scale event left was a merged one
// ("combined from similar events", count 16), which hid the other
// version's scale-downs. From merged evidence the scale isn't said as fact.
func TestAddedCopyFromAMergedEventSaysLess(t *testing.T) {
	now := time.Now().UTC()
	kube.ResetPodWatch(now.Add(-2 * time.Hour))
	fx, _ := fixture(t, false)
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "roll-6f", "namespace": "qa", "uid": "rs-1", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339)}}}
	pod := func(name string, made time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "roll-6f", "uid": "rs-1", "controller": true}}}}}
	}
	for _, c := range []struct {
		msg   string
		count int64
		want  string
	}{
		{"(combined from similar events): Scaled up replica set roll-6f from 2 to 3", 16, "added a copy: its scale events were merged, so whether a scale or a rollout made it isn't known"},
		{"Scaled up replica set roll-6f from 2 to 3", 3, "added a copy: its scale events were merged, so whether a scale or a rollout made it isn't known"},
		{"Scaled up replica set roll-6f from 2 to 3", 1, "added a copy: scaled up from 2 to 3"},
	} {
		ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": "e", "namespace": "qa"}, "reason": "ScalingReplicaSet", "count": c.count,
			"message": c.msg, "involvedObject": map[string]any{"kind": "Deployment", "name": "roll"},
			"lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339)}}
		e := New(fakeStore{kube.ReplicaSets: {rs}, kube.Pods: {pod("old1", now.Add(-time.Hour)), pod("old2", now.Add(-time.Hour)), pod("copy", now.Add(-time.Minute+time.Second))},
			kube.Events: {ev}}, nil, nil)
		e.res = fx.res
		got := ""
		for _, ch := range e.changesOver("qa", "", now, ChangeWindow, nil) {
			if ch.Object == "pod/copy" {
				got = ch.Detail
			}
		}
		if got != c.want {
			t.Errorf("%q (count %d): %q, want %q", c.msg, c.count, got, c.want)
		}
	}
}
