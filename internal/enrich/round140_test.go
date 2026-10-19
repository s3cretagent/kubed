package enrich

import (
	"testing"
	"time"

	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 140: X on a pod whose secret volume lists a key the secret
// lacks showed "Secret has-a" unmarked, though the pod can't mount it.
// Volume items (secret, config map, projected) are checked as env keys
// are: from the cached keys, or the pod's FailedMount message.
func TestXrayMarksAMissingVolumeKey(t *testing.T) {
	pod := obj(t, `
apiVersion: v1
kind: Pod
metadata: {name: vol, namespace: qa, uid: p1}
spec:
  containers: [{name: app, image: x}]
  volumes:
    - {name: s, secret: {secretName: has-a, items: [{key: nope, path: n}]}}
    - {name: o, secret: {secretName: opt, optional: true, items: [{key: gone, path: g}]}}
    - name: p
      projected:
        sources:
          - configMap: {name: settings, items: [{key: LEVEL, path: l}]}
status:
  phase: Pending
  containerStatuses: [{name: app, ready: false, state: {waiting: {reason: ContainerCreating}}}]
`)
	secret := obj(t, `
apiVersion: v1
kind: Secret
metadata: {name: has-a, namespace: qa}
data: {a: ''}
`)
	opt := obj(t, `
apiVersion: v1
kind: Secret
metadata: {name: opt, namespace: qa}
data: {a: ''}
`)
	cm := obj(t, `
apiVersion: v1
kind: ConfigMap
metadata: {name: settings, namespace: qa}
data: {OTHER: ''}
`)
	e := New(fakeStore{kube.Pods: {pod}, kube.Secrets: {secret, opt}, kube.ConfigMaps: {cm}}, nil, nil)
	if n := xrayNode(e, pod, "Secret", "has-a"); n == nil || n.Health != HealthBad || n.Note != "has no key nope" {
		t.Errorf("Secret has-a %+v, want bad, has no key nope", n)
	}
	if n := xrayNode(e, pod, "ConfigMap", "settings"); n == nil || n.Health != HealthBad || n.Note != "has no key LEVEL" {
		t.Errorf("ConfigMap settings %+v, want bad, has no key LEVEL", n)
	}
	if n := xrayNode(e, pod, "Secret", "opt"); n == nil || n.Health == HealthBad {
		t.Errorf("optional Secret opt %+v", n)
	}

	// Secrets kubed can't see: the pod's FailedMount message says so.
	ev := obj(t, `
apiVersion: v1
kind: Event
metadata: {name: vol.1, namespace: qa}
involvedObject: {kind: Pod, name: vol, uid: p1}
reason: FailedMount
type: Warning
message: 'MountVolume.SetUp failed for volume "s" : references non-existent secret key: nope'
lastTimestamp: '`+time.Now().UTC().Format(time.RFC3339)+`'
`)
	e = New(fakeStore{kube.Pods: {pod}, kube.ConfigMaps: {cm}, kube.Events: {ev}}, kube.NewResources([]kube.Resource{
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.ConfigMaps, Kind: "ConfigMap", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true}, {GVR: kube.ServiceAccounts, Kind: "ServiceAccount", Namespaced: true},
	}), nil)
	if n := xrayNode(e, pod, "Secret", "has-a"); n == nil || n.Health != HealthBad || n.Note != "has no key nope" {
		t.Errorf("unseen Secret has-a %+v, want bad from the pod's FailedMount", n)
	}
}
