package enrich

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/s3cretagent/kubed/internal/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// QA round 136: a deployment with minReadySeconds 30 after an image
// change, its old pod still there (1 old replicas are pending
// termination, as kubectl rollout status says). o said "✓ 2/2 on
// revision 2". It is still rolling out until no old pod is left, every
// new one is available, and the status is of the current spec.
func TestRolloutNotDoneWhileOldPodsLeft(t *testing.T) {
	rs := func(name, uid, rev string, replicas, ready int) string {
		return `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: ` + name + `
  namespace: qa
  uid: ` + uid + `
  annotations: {deployment.kubernetes.io/revision: "` + rev + `"}
  ownerReferences: [{apiVersion: apps/v1, kind: Deployment, name: web, uid: d1, controller: true}]
spec:
  replicas: ` + strconv.Itoa(replicas) + `
  template: {spec: {containers: [{name: app, image: "nginx:` + rev + `"}]}}
status: {replicas: ` + strconv.Itoa(replicas) + `, readyReplicas: ` + strconv.Itoa(ready) + `}
`
	}
	for _, c := range []struct {
		name, status string
		generation   int
		done         bool
	}{
		{"old pod pending termination", `{observedGeneration: 2, replicas: 3, updatedReplicas: 2, readyReplicas: 3, availableReplicas: 2, unavailableReplicas: 1}`, 2, false},
		{"new pod not available yet", `{observedGeneration: 2, replicas: 2, updatedReplicas: 2, readyReplicas: 2, availableReplicas: 1, unavailableReplicas: 1}`, 2, false},
		{"status of an older spec", `{observedGeneration: 1, replicas: 2, updatedReplicas: 2, readyReplicas: 2, availableReplicas: 2}`, 2, false},
		{"done", `{observedGeneration: 2, replicas: 2, updatedReplicas: 2, readyReplicas: 2, availableReplicas: 2}`, 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dep := obj(t, `
apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: qa, uid: d1, generation: `+strconv.Itoa(c.generation)+`}
spec: {replicas: 2, minReadySeconds: 30}
status: `+c.status)
			old := 0
			if strings.Contains(c.name, "old pod") {
				old = 1
			}
			e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {obj(t, rs("web-2", "r2", "2", 2, 2)), obj(t, rs("web-1", "r1", "1", old, old))}}, nil, nil)
			r := e.RolloutOf(dep)
			if r.Done() != c.done {
				t.Errorf("done %v, want %v (%s)", r.Done(), c.done, r.Summary())
			}
			if !c.done && strings.Contains(r.Summary(), "on revision") {
				t.Errorf("summary %q", r.Summary())
			}
		})
	}
}

// QA round 136: i on a stateful set that replaces pods only once they are
// deleted (OnDelete) reset its Deployed, though no pod changed. Its
// version went out when a pod runs it: until then, the one its pods run.
func TestOnDeleteRevisionNotDeployedUntilAPodRunsIt(t *testing.T) {
	made := func(ago time.Duration) string { return time.Now().Add(-ago).UTC().Format(time.RFC3339) }
	cr := func(name, uid, image string, rev int, ago time.Duration) string {
		return `
apiVersion: apps/v1
kind: ControllerRevision
metadata:
  name: ` + name + `
  namespace: qa
  uid: ` + uid + `
  creationTimestamp: "` + made(ago) + `"
  ownerReferences: [{apiVersion: apps/v1, kind: StatefulSet, name: web, uid: s1, controller: true}]
revision: ` + strconv.Itoa(rev) + `
data: {spec: {template: {spec: {containers: [{name: web, image: "` + image + `"}]}}}}
`
	}
	for _, c := range []struct {
		name    string
		updated int
		want    time.Duration
	}{
		{"no pod runs the new one", 0, 2 * time.Hour},
		{"a pod runs it", 1, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			ss := obj(t, `
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: web, namespace: qa, uid: s1, generation: 2}
spec: {replicas: 2, updateStrategy: {type: OnDelete}}
status: {observedGeneration: 2, replicas: 2, updatedReplicas: `+strconv.Itoa(c.updated)+`, currentRevision: web-1, updateRevision: web-2}
`)
			e := New(fakeStore{kube.StatefulSets: {ss}, kube.ControllerRevisions: {obj(t, cr("web-1", "c1", "nginx:1", 1, 2*time.Hour)), obj(t, cr("web-2", "c2", "nginx:2", 2, time.Minute))}}, nil, nil)
			got, ok := e.RevisionDeployTime(ss)
			if !ok || got.At.Sub(time.Now().Add(-c.want)).Abs() > 5*time.Second {
				t.Errorf("deployed %v ago, want %v", time.Since(got.At).Round(time.Second), c.want)
			}
		})
	}
}

// Review of round 136: an OnDelete daemon set changed twice, no pod on
// either new version: Deployed is when the version its pods run went out,
// found by the pods' controller-revision-hash, not the one before newest.
func TestOnDeleteDaemonSetChangedTwice(t *testing.T) {
	made := func(ago time.Duration) string { return time.Now().Add(-ago).UTC().Format(time.RFC3339) }
	cr := func(hash, image string, rev int, ago time.Duration) *unstructured.Unstructured {
		return obj(t, `
apiVersion: apps/v1
kind: ControllerRevision
metadata:
  name: agent-`+hash+`
  namespace: qa
  uid: c`+hash+`
  creationTimestamp: "`+made(ago)+`"
  ownerReferences: [{apiVersion: apps/v1, kind: DaemonSet, name: agent, uid: d1, controller: true}]
revision: `+strconv.Itoa(rev)+`
data: {spec: {template: {spec: {containers: [{name: a, image: "`+image+`"}]}}}}
`)
	}
	ds := obj(t, `
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: agent, namespace: qa, uid: d1, generation: 3}
spec: {updateStrategy: {type: OnDelete}}
status: {observedGeneration: 3, desiredNumberScheduled: 2, currentNumberScheduled: 2, updatedNumberScheduled: 0}
`)
	pod := func(name string) *unstructured.Unstructured {
		return obj(t, `
apiVersion: v1
kind: Pod
metadata:
  name: `+name+`
  namespace: qa
  uid: p-`+name+`
  labels: {controller-revision-hash: aaa}
  ownerReferences: [{apiVersion: apps/v1, kind: DaemonSet, name: agent, uid: d1, controller: true}]
status: {phase: Running}
`)
	}
	e := New(fakeStore{kube.DaemonSets: {ds}, kube.Pods: {pod("agent-x"), pod("agent-y")},
		kube.ControllerRevisions: {cr("aaa", "a:1", 1, 3*time.Hour), cr("bbb", "a:2", 2, 2*time.Hour), cr("ccc", "a:3", 3, time.Minute)}}, nil, nil)
	got, ok := e.RevisionDeployTime(ds)
	if !ok || got.At.Sub(time.Now().Add(-3*time.Hour)).Abs() > 5*time.Second {
		t.Errorf("deployed %v ago, want 3h0m0s (the version its pods run)", time.Since(got.At).Round(time.Second))
	}
}
