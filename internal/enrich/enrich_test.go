package enrich

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/pricing"
)

// fakeStore is an in-memory Store keyed by GVR.
type fakeStore map[schema.GroupVersionResource][]*unstructured.Unstructured

func (f fakeStore) List(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, o := range f[gvr] {
		if ns == "" || o.GetNamespace() == ns {
			out = append(out, o)
		}
	}
	return out
}

func (f fakeStore) Synced(schema.GroupVersionResource) bool { return true }

func (f fakeStore) Get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	for _, o := range f[gvr] {
		if o.GetNamespace() == ns && o.GetName() == name {
			return o, true
		}
	}
	return nil, false
}

func obj(t *testing.T, y string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(y), &u.Object); err != nil {
		t.Fatal(err)
	}
	return u
}

// A Pod owned by a ReplicaSet owned by a Deployment that ArgoCD tracks.
func fixture(t *testing.T, selfHeal bool) (*Enricher, *unstructured.Unstructured) {
	heal := "false"
	if selfHeal {
		heal = "true"
	}
	pod := obj(t, `
apiVersion: v1
kind: Pod
metadata:
  name: api-abc-1
  namespace: payment
  ownerReferences: [{apiVersion: apps/v1, kind: ReplicaSet, name: api-abc, controller: true}]
spec:
  serviceAccountName: api-sa
  nodeName: node-1
  containers:
    - name: app
      envFrom: [{secretRef: {name: api-secrets}}]
`)
	store := fakeStore{
		kube.Pods: {pod},
		kube.ReplicaSets: {obj(t, `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: api-abc
  namespace: payment
  ownerReferences: [{apiVersion: apps/v1, kind: Deployment, name: api, controller: true}]
`)},
		kube.Deployments: {obj(t, `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: payment
  annotations: {argocd.argoproj.io/tracking-id: "payment:apps/Deployment:payment/api"}
`)},
		kube.Applications: {obj(t, `
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: payment, namespace: argocd}
spec: {syncPolicy: {automated: {selfHeal: `+heal+`, prune: true}}}
status: {sync: {status: Synced}, health: {status: Healthy}}
`)},
		kube.ServiceAccounts: {obj(t, `
apiVersion: v1
kind: ServiceAccount
metadata:
  name: api-sa
  namespace: payment
  annotations: {eks.amazonaws.com/role-arn: "arn:aws:iam::1:role/api"}
`)},
		kube.ExternalSecrets: {obj(t, `
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: api-es, namespace: payment}
spec: {target: {name: api-secrets}, secretStoreRef: {name: ssm}}
status: {conditions: [{type: Ready, status: "False", reason: SecretSyncedError, message: "key not found"}]}
`)},
		kube.Nodes: {obj(t, `
apiVersion: v1
kind: Node
metadata:
  name: node-1
  labels:
    karpenter.sh/nodepool: general
    karpenter.sh/capacity-type: spot
    node.kubernetes.io/instance-type: m7i.large
    topology.kubernetes.io/region: ap-south-1
    topology.kubernetes.io/zone: ap-south-1a
`)},
	}
	prices := &pricing.Table{Regions: map[string]map[string][2]float64{"ap-south-1": {"m7i.large": {0.1, 0.05}}}}
	e := New(store, testResources(), prices)
	e.SetCosts(true) // the cluster's AWS account is known
	return e, pod
}

// testResources stands in for discovery.
func testResources() *kube.Resources {
	return kube.NewResources([]kube.Resource{
		{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true},
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Nodes, Kind: "Node"},
		{GVR: kube.ServiceAccounts, Kind: "ServiceAccount", Namespaced: true},
		{GVR: kube.ExternalSecrets, Kind: "ExternalSecret", Namespaced: true},
		{GVR: kube.Applications, Kind: "Application", Namespaced: true},
		{GVR: kube.Services, Kind: "Service", Namespaced: true},
		{GVR: kube.Events, Kind: "Event", Namespaced: true},
	})
}

func TestManagedByFollowsOwners(t *testing.T) {
	e, pod := fixture(t, true)
	app, ok := e.ManagedBy(pod)
	if !ok || app.Name != "payment" || !app.SelfHeal || !app.AutoSync || app.Health != "Healthy" {
		t.Fatalf("ManagedBy = %+v, %v", app, ok)
	}
}

func TestIRSA(t *testing.T) {
	e, pod := fixture(t, true)
	sa, role := e.IRSA(pod)
	if sa != "api-sa" || role != "arn:aws:iam::1:role/api" {
		t.Fatalf("IRSA = %s, %s", sa, role)
	}
}

func TestPodNodePrice(t *testing.T) {
	e, pod := fixture(t, true)
	_, n, ok := e.PodNode(pod)
	if !ok || n.Pool != "general" || n.Capacity != "spot" || n.Hourly != 0.05 || !n.PriceKnown {
		t.Fatalf("PodNode = %+v, %v", n, ok)
	}
}

func TestExternalSecretsForPod(t *testing.T) {
	e, pod := fixture(t, true)
	got := e.ExternalSecretsForPod(pod)
	if len(got) != 1 || got[0].Name != "api-es" || got[0].Ready != "False" || got[0].Message != "key not found" {
		t.Fatalf("ExternalSecretsForPod = %+v", got)
	}
}

func TestWarnings(t *testing.T) {
	cases := []struct {
		name     string
		selfHeal bool
		target   string // "pod" or "deploy"
		action   Action
		want     string // substring; "" = no warning
	}{
		{"scale with selfHeal", true, "deploy", ActionScale, "if Git sets the replica count"},
		{"edit with selfHeal", true, "deploy", ActionEdit, "anything Git sets"},
		{"delete with selfHeal", true, "deploy", ActionDelete, "create this again"},
		{"delete without selfHeal", false, "deploy", ActionDelete, "next change in Git"},
		{"restart is not drift", true, "deploy", ActionRestart, ""},
		{"deleting a pod is fine", true, "pod", ActionDelete, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, pod := fixture(t, c.selfHeal)
			target := pod
			if c.target == "deploy" {
				target, _ = e.store.Get(kube.Deployments, "payment", "api")
			}
			w := strings.Join(e.Warnings(target, c.action), " | ")
			if c.want == "" && w != "" || !strings.Contains(w, c.want) {
				t.Fatalf("Warnings = %q, want %q", w, c.want)
			}
		})
	}
}

func TestSecretOwnedByExternalSecretWarns(t *testing.T) {
	e, _ := fixture(t, true)
	secret := obj(t, `
apiVersion: v1
kind: Secret
metadata:
  name: api-secrets
  namespace: payment
  ownerReferences: [{apiVersion: external-secrets.io/v1, kind: ExternalSecret, name: api-es, controller: true}]
`)
	w := strings.Join(e.Warnings(secret, ActionEdit), " | ")
	if !strings.Contains(w, "ExternalSecret api-es owns this Secret") {
		t.Fatalf("Warnings = %q", w)
	}
}

func TestNodeGroupNodes(t *testing.T) {
	e := New(fakeStore{}, nil, nil)
	n := e.Node(obj(t, `
apiVersion: v1
kind: Node
metadata:
  name: n
  labels: {eks.amazonaws.com/nodegroup: ng-a, eks.amazonaws.com/capacityType: ON_DEMAND, node.kubernetes.io/instance-type: t3.large}
`))
	if n.Pool != "ng:ng-a" || n.Capacity != "on-demand" || n.PriceKnown {
		t.Fatalf("Node = %+v", n)
	}
}

type fakeUsage map[string]kube.Usage

func (f fakeUsage) Available() bool                        { return len(f) > 0 }
func (f fakeUsage) Pod(ns, name string) (kube.Usage, bool) { u, ok := f[ns+"/"+name]; return u, ok }
func (f fakeUsage) Node(name string) (kube.Usage, bool)    { u, ok := f[name]; return u, ok }
func (f fakeUsage) Peak(ns, name string) (kube.Usage, time.Time, bool) {
	u, ok := f[ns+"/"+name]
	return u, time.Now().Add(-time.Hour), ok
}

func TestPodUsageAgainstRequests(t *testing.T) {
	e := New(fakeStore{}, nil, nil)
	e.SetUsage(fakeUsage{"ns/p": {CPUMilli: 50, MemBytes: 64 << 20}})
	pod := obj(t, `
apiVersion: v1
kind: Pod
metadata: {name: p, namespace: ns}
spec:
  containers:
    - {name: a, resources: {requests: {cpu: 100m, memory: 128Mi}, limits: {cpu: "1", memory: 256Mi}}}
    - {name: b, resources: {requests: {cpu: 100m}}}
`)
	u := e.PodUsage(pod)
	if !u.Known || u.ReqCPU != 200 || u.LimCPU != 1000 || u.ReqMem != 128<<20 || u.LimMem != 256<<20 {
		t.Fatalf("PodUsage = %+v", u)
	}
	if got := Pct(u.Use.CPUMilli, u.ReqCPU); got != 25 {
		t.Errorf("cpu%%/req = %d, want 25", got)
	}
	if e2 := New(fakeStore{}, nil, nil); e2.PodUsage(pod).Known {
		t.Error("usage must be unknown without a metrics source")
	}
}

func TestXrayDeployment(t *testing.T) {
	e, _ := fixture(t, true)
	store := e.store.(fakeStore)
	dep, _ := store.Get(kube.Deployments, "payment", "api")
	dep.Object["spec"] = map[string]any{"replicas": int64(1), "template": map[string]any{
		"metadata": map[string]any{"labels": map[string]any{"app": "api"}},
		"spec": map[string]any{"serviceAccountName": "api-sa", "containers": []any{
			map[string]any{"name": "app", "envFrom": []any{map[string]any{"secretRef": map[string]any{"name": "api-secrets"}}}},
		}},
	}}
	store[kube.Services] = []*unstructured.Unstructured{obj(t, `
apiVersion: v1
kind: Service
metadata: {name: api, namespace: payment}
spec: {type: ClusterIP, selector: {app: api}}
`)}
	rs, _ := store.Get(kube.ReplicaSets, "payment", "api-abc")
	rs.Object["spec"] = map[string]any{"replicas": int64(1)}
	e.store = store
	tree := e.Xray(dep)
	text := dumpTree(tree, "")
	for _, want := range []string{"Deployment api", "ReplicaSet api-abc", "Pod api-abc-1", "ServiceAccount api-sa AWS role arn:aws:iam::1:role/api",
		"Secret api-secrets ← ExternalSecret api-es: SecretSyncedError", "Service api"} {
		if !strings.Contains(text, want) {
			t.Errorf("xray missing %q:\n%s", want, text)
		}
	}
}

func TestXrayPodShowsItsOwnNodeAndOwner(t *testing.T) {
	e, _ := fixture(t, true)
	store := e.store.(fakeStore)
	pod, _ := store.Get(kube.Pods, "payment", "api-abc-1")
	pod.Object["spec"] = map[string]any{"nodeName": "ip-10-0-0-1", "serviceAccountName": "api-sa",
		"containers": []any{map[string]any{"name": "app"}}}
	tree := e.Xray(pod)
	text := dumpTree(tree, "")
	for _, want := range []string{"Pod api-abc-1", "runs on", "Node ip-10-0-0-1", "made by", "Deployment api", "ServiceAccount api-sa"} {
		if !strings.Contains(text, want) {
			t.Errorf("pod xray missing %q:\n%s", want, text)
		}
	}
}

func dumpTree(n *XNode, indent string) string {
	s := indent + strings.TrimSpace(n.Kind+" "+n.Name+" "+n.Note) + "\n"
	for _, c := range n.Children {
		s += dumpTree(c, indent+"  ")
	}
	return s
}

func TestHomeFindsProblems(t *testing.T) {
	e, pod := fixture(t, true)
	store := e.store.(fakeStore)
	pod.Object["status"] = map[string]any{"phase": "Running", "containerStatuses": []any{
		map[string]any{"name": "app", "ready": false, "state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}}},
	}}
	store[kube.Nodes][0].Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}
	e.store = store
	s := e.Home(time.Now())
	var whats []string
	for _, p := range s.Problems {
		whats = append(whats, p.Kind+":"+p.What)
	}
	joined := strings.Join(whats, " | ")
	for _, want := range []string{"Pod:CrashLoopBackOff", "ExternalSecret:not syncing: SecretSyncedError"} {
		if !strings.Contains(joined, want) {
			t.Errorf("home missing %q: %s", want, joined)
		}
	}
	if s.Nodes != 1 || s.NodesReady != 1 || s.Pods != 1 {
		t.Errorf("summary %+v", s)
	}
}

func TestChangesTimeline(t *testing.T) {
	e, pod := fixture(t, true)
	store := e.store.(fakeStore)
	now := time.Now().UTC()
	recent := now.Add(-30 * time.Minute).Format(time.RFC3339)
	rs, _ := store.Get(kube.ReplicaSets, "payment", "api-abc")
	rs.SetCreationTimestamp(metav1.NewTime(now.Add(-time.Hour)))
	rs.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "7"})
	rs.Object["spec"] = map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{
		map[string]any{"name": "app", "image": "1234.dkr.ecr/payment-api:v42"}}}}}
	app := store[kube.Applications][0]
	app.Object["spec"].(map[string]any)["destination"] = map[string]any{"namespace": "payment"}
	app.Object["status"].(map[string]any)["history"] = []any{map[string]any{"deployedAt": recent, "revision": "abcdef1234567"}}
	pod.Object["status"] = map[string]any{"containerStatuses": []any{map[string]any{"name": "app",
		"lastState": map[string]any{"terminated": map[string]any{"reason": "OOMKilled", "exitCode": int64(137), "finishedAt": recent}}}}}
	es := store[kube.ExternalSecrets][0]
	es.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False",
		"reason": "SecretSyncedError", "lastTransitionTime": recent}}}
	e.store = store

	text := ""
	for _, c := range e.Changes("payment", "", now, nil) {
		text += c.What + " " + c.Object + " " + c.Detail + "\n"
	}
	for _, want := range []string{
		"rollout deployment/api revision 7: payment-api:v42",
		"sync argocd/payment applied Git revision abcdef12",
		`crash pod/api-abc-1 container "app" ran out of memory and was killed`,
		"secret externalsecret/api-es stopped syncing: SecretSyncedError",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("timeline missing %q:\n%s", want, text)
		}
	}
}

// A version brought back is dated by when it came back (its scale-up
// event), not when it was first made; a restart says so.
func TestChangesReusedVersionAndRestart(t *testing.T) {
	e, _ := fixture(t, true)
	store := e.store.(fakeStore)
	now := time.Now().UTC()
	rs, _ := store.Get(kube.ReplicaSets, "payment", "api-abc")
	rs.SetCreationTimestamp(metav1.NewTime(now.Add(-20 * time.Hour)))
	rs.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "9", "deployment.kubernetes.io/revision-history": "7"})
	rs.Object["spec"] = map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{
		map[string]any{"name": "app", "image": "api:v42"}}}}}
	restarted := rs.DeepCopy()
	restarted.SetName("api-def")
	restarted.SetUID("def")
	restarted.SetCreationTimestamp(metav1.NewTime(now.Add(-5 * time.Minute)))
	restarted.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "10"})
	restarted.Object["spec"].(map[string]any)["template"].(map[string]any)["metadata"] = map[string]any{
		"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": now.Format(time.RFC3339)}}
	store[kube.ReplicaSets] = append(store[kube.ReplicaSets], restarted)
	store[kube.Events] = append(store[kube.Events], &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"name": "ev", "namespace": "payment"},
		"reason": "ScalingReplicaSet", "message": "Scaled up replica set api-abc from 0 to 1",
		"lastTimestamp": now.Add(-10 * time.Minute).Format(time.RFC3339)}})
	e.store = store

	var text string
	for _, c := range e.Changes("payment", "", now, nil) {
		text += c.Object + " " + c.Detail + "\n"
	}
	for _, want := range []string{
		"revision 9: went back to an earlier version: api:v42",
		"revision 10: restarted, same images (api:v42)",
		"revision 7 (later brought back): api:v42", // its first time out keeps its row, by the number it had
	} {
		if !strings.Contains(text, want) {
			t.Errorf("timeline missing %q:\n%s", want, text)
		}
	}
	// In the versions list, the version brought back (no longer running)
	// has only a bound: its one scale-up could be a scale by hand after a
	// go-back made while nothing ran, and the version it replaced
	// (revision 8) is gone.
	dep := e.controller(rs)
	e.res = nil // the fixture's cluster serves no events; this one does
	for _, v := range e.RolloutOf(dep).All {
		if v.Name == "api-abc" && (!v.LiveSince.IsZero() || v.BackAfter.IsZero() || v.BackAfter.After(now.Add(-10*time.Minute))) {
			t.Errorf("api-abc came back at %v (or after %v), want a bound no later than 10 minutes ago", v.LiveSince, v.BackAfter)
		}
	}
}

func TestWastesSuggestsPeakPlusHeadroom(t *testing.T) {
	e, pod := fixture(t, true)
	pod.Object["status"] = map[string]any{"phase": "Running"}
	pod.Object["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "app", "resources": map[string]any{
		"requests": map[string]any{"cpu": "1", "memory": "1Gi"}}}}
	node := e.store.(fakeStore)[kube.Nodes][0]
	node.Object["status"] = map[string]any{"allocatable": map[string]any{"cpu": "2", "memory": "8Gi"}}
	e.SetUsage(fakeUsage{"payment/api-abc-1": {CPUMilli: 100, MemBytes: 200 << 20}})
	ws := e.Wastes([]*unstructured.Unstructured{pod})
	if len(ws) != 1 {
		t.Fatalf("wastes = %+v", ws)
	}
	w := ws[0]
	if w.Workload.GetName() != "api" || w.SuggestCPU != 130 || w.SuggestMem != 260<<20 {
		t.Errorf("suggest = %dm / %dMi for %s", w.SuggestCPU, w.SuggestMem>>20, w.Workload.GetName())
	}
	// Gives back 870m of a 2-CPU node at $0.05/hr spot ≈ $15.9/month.
	if w.Monthly < 15 || w.Monthly > 17 {
		t.Errorf("monthly saving = %.2f", w.Monthly)
	}
}

// A workload short of memory, with nothing to give back, is still listed:
// it is a risk the waste list warns about.
func TestWastesListsMemoryShort(t *testing.T) {
	e, pod := fixture(t, true)
	pod.Object["status"] = map[string]any{"phase": "Running"}
	pod.Object["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "app", "resources": map[string]any{
		"requests": map[string]any{"cpu": "10m", "memory": "16Mi"}}}}
	e.SetUsage(fakeUsage{"payment/api-abc-1": {CPUMilli: 10, MemBytes: 60 << 20}})
	ws := e.Wastes([]*unstructured.Unstructured{pod})
	if len(ws) != 1 || ws[0].PeakMem <= ws[0].ReqMem {
		t.Fatalf("a workload using 60MB of 16MB reserved should be listed: %+v", ws)
	}
}

// Node prices: live spot by zone ID, then the account's bill, then list
// less the discount the bill implies, then list.
func TestNodePriceSources(t *testing.T) {
	node := func(typ, capacity string) *unstructured.Unstructured {
		return obj(t, `
apiVersion: v1
kind: Node
metadata:
  name: n
  labels:
    karpenter.sh/capacity-type: `+capacity+`
    node.kubernetes.io/instance-type: `+typ+`
    topology.kubernetes.io/region: ap-south-1
    topology.kubernetes.io/zone: ap-south-1a
    topology.k8s.aws/zone-id: aps1-az1
`)
	}
	prices := &pricing.Table{Regions: map[string]map[string][2]float64{"ap-south-1": {
		"t4g.small": {0.0112, 0.004}, "m6g.large": {0.0506, 0.02}, "t3a.medium": {0.0246, 0.0104},
	}}}
	e := New(fakeStore{}, testResources(), prices)
	e.SetCosts(true)
	near := func(got, want float64) bool { return got > want-1e-9 && got < want+1e-9 }

	if h := e.Node(node("m6g.large", "on-demand")).Hourly; !near(h, 0.0506) {
		t.Errorf("no bill: list price, got %v", h)
	}
	e.SetDiscount(10)
	if h := e.Node(node("m6g.large", "on-demand")).Hourly; !near(h, 0.04554) {
		t.Errorf("config discount: got %v", h)
	}
	e.SetBilled(&pricing.Billed{Region: "ap-south-1", Rates: map[string]float64{"t4g.small": 0.0081}})
	if h := e.Node(node("t4g.small", "on-demand")).Hourly; !near(h, 0.0081) {
		t.Errorf("on the bill: billed rate, got %v", h)
	}
	if h, want := e.Node(node("m6g.large", "on-demand")).Hourly, 0.0506*0.0081/0.0112; !near(h, want) {
		t.Errorf("not on the bill: list less the bill's discount (%v), got %v", want, h)
	}
	if h := e.Node(node("t3a.medium", "spot")).Hourly; !near(h, 0.0104) {
		t.Errorf("spot without live prices: average, got %v", h)
	}
	e.SetSpot(map[string]float64{"t3a.medium/aps1-az1": 0.0123})
	if h := e.Node(node("t3a.medium", "spot")).Hourly; !near(h, 0.0123) {
		t.Errorf("spot: live price by zone ID, got %v", h)
	}
}

func TestNothingPricedUntilCostsOn(t *testing.T) {
	e, pod := fixture(t, true)
	e.SetCosts(false)
	if _, n, _ := e.PodNode(pod); n.PriceKnown || n.Hourly != 0 {
		t.Errorf("costs off: %+v", n)
	}
	if c := e.ClusterSummary(); c.Hourly != 0 {
		t.Errorf("costs off: cluster $%v/hr", c.Hourly)
	}
}

func TestObjectProblem(t *testing.T) {
	cases := []struct {
		yaml, want string
	}{
		{`
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: {name: so}
status: {conditions: [{type: Ready, status: "True"}, {type: Fallback, status: "True", message: "At least one trigger is falling back on this scaled object"}]}
`, "At least one trigger is falling back on this scaled object"},
		{`
apiVersion: apps/v1
kind: Deployment
metadata: {name: d}
spec: {replicas: 3}
status: {readyReplicas: 1, conditions: [{type: Progressing, status: "False", reason: ProgressDeadlineExceeded, message: "ReplicaSet d-1 has timed out progressing."}]}
`, "1 of 3 pods ready; the rollout gave up: ReplicaSet d-1 has timed out progressing."},
		{`
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: c}
status: {conditions: [{type: Ready, status: "False", reason: Failed, message: ""}]}
`, "Failed"},
		{`
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata: {name: apps}
status: {conditions: [{type: ParametersGenerated, status: "True"}, {type: ErrorOccurred, status: "True", message: "Object argocd/api is already owned by another ApplicationSet controller other"}]}
`, "Object argocd/api is already owned by another ApplicationSet controller other"},
		{`
apiVersion: apps/v1
kind: Deployment
metadata: {name: ok}
spec: {replicas: 2}
status: {readyReplicas: 2}
`, ""},
		{`
apiVersion: batch/v1
kind: Job
metadata: {name: running}
status: {active: 1}
`, ""},
	}
	for _, c := range cases {
		got, bad := ObjectProblem(obj(t, c.yaml))
		if got != c.want || bad != (c.want != "") {
			t.Errorf("ObjectProblem = %q, %v; want %q", got, bad, c.want)
		}
	}
}

// A pod reaches its Deployment before ReplicaSets have loaded.
func TestTopOwnerWithoutReplicaSets(t *testing.T) {
	pod := obj(t, `
apiVersion: v1
kind: Pod
metadata:
  name: api-7f6bcc6f86-22v96
  namespace: auth
  labels: {pod-template-hash: 7f6bcc6f86}
  ownerReferences: [{apiVersion: apps/v1, kind: ReplicaSet, name: api-7f6bcc6f86, controller: true, uid: x}]
`)
	deploy := obj(t, `
apiVersion: apps/v1
kind: Deployment
metadata: {name: api, namespace: auth}
`)
	e := New(fakeStore{kube.Deployments: {deploy}}, testResources(), nil)
	if top := e.TopOwner(pod); top.GetKind() != "Deployment" || top.GetName() != "api" {
		t.Errorf("TopOwner = %s/%s, want Deployment/api", top.GetKind(), top.GetName())
	}
}

// Savings on one node can't add up to more than the node costs.
func TestWastesNeverExceedTheNode(t *testing.T) {
	e, pod := fixture(t, true)
	pod.Object["status"] = map[string]any{"phase": "Running"}
	pod.Object["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "app", "resources": map[string]any{
		"requests": map[string]any{"cpu": "1", "memory": "4Gi"}}}}
	node := e.store.(fakeStore)[kube.Nodes][0]
	node.Object["status"] = map[string]any{"allocatable": map[string]any{"cpu": "2", "memory": "8Gi"}}
	twin := pod.DeepCopy()
	twin.SetName("api-abc-2")
	e.SetUsage(fakeUsage{"payment/api-abc-1": {CPUMilli: 1, MemBytes: 1 << 20}, "payment/api-abc-2": {CPUMilli: 1, MemBytes: 1 << 20}})
	total := 0.0
	for _, w := range e.Wastes([]*unstructured.Unstructured{pod, twin}) {
		total += w.Monthly
	}
	nodeMonthly := 0.05 * 730 // spot price in the fixture
	if total > nodeMonthly+0.01 {
		t.Errorf("savings $%.2f/month exceed the node's $%.2f", total, nodeMonthly)
	}
}

func TestRolloutsOfOneReleaseAreOneRow(t *testing.T) {
	now := time.Now()
	in := []Change{
		{When: now, What: "rollout", Namespace: "be", Object: "deployment/api", Detail: "revision 5: app:v2"},
		{When: now.Add(-10 * time.Second), What: "rollout", Namespace: "be", Object: "deployment/celery", Detail: "revision 9: app:v2"},
		{When: now.Add(-20 * time.Second), What: "rollout", Namespace: "be", Object: "deployment/worker", Detail: "revision 3: app:v2"},
		{When: now.Add(-time.Hour), What: "rollout", Namespace: "be", Object: "deployment/api", Detail: "revision 4: app:v1"},
	}
	out := groupRollouts(in)
	if len(out) != 2 || out[0].Object != "deployment/api and 2 more" || out[0].Detail != "3 workloads got a new version: app:v2" {
		t.Errorf("got %+v", out)
	}
	// Round 66: two made 1m51s before a third are not one release; they
	// keep their own time.
	in = []Change{
		{When: now, What: "rollout", Namespace: "qa", Object: "deployment/auto", Detail: "revision 1: nginx:alpine"},
		{When: now.Add(-111 * time.Second), What: "rollout", Namespace: "qa", Object: "deployment/huge", Detail: "revision 1: nginx:alpine"},
		{When: now.Add(-111 * time.Second), What: "rollout", Namespace: "qa", Object: "deployment/nosa", Detail: "revision 1: nginx:alpine"},
	}
	if out := groupRollouts(in); len(out) != 2 || !out[1].When.Equal(now.Add(-111*time.Second)) {
		t.Errorf("got %+v", out)
	}
	if got := eventWords("BackOff", "Back-off restarting failed container celery in pod x"); got != "container \"celery\" keeps crashing; waiting before the next restart" {
		t.Errorf("eventWords = %q", got)
	}
}

func TestOptionalRefs(t *testing.T) {
	spec := map[string]any{
		"containers": []any{map[string]any{"name": "app", "envFrom": []any{
			map[string]any{"secretRef": map[string]any{"name": "both", "optional": true}},
			map[string]any{"configMapRef": map[string]any{"name": "cm", "optional": true}},
		}}},
		"volumes": []any{
			map[string]any{"name": "tls", "secret": map[string]any{"secretName": "tls", "optional": true}},
			map[string]any{"name": "b", "secret": map[string]any{"secretName": "both"}},
		},
	}
	got := optionalRefs(spec)
	if !got["secret/tls"] || !got["configmap/cm"] || got["secret/both"] {
		t.Fatalf("optionalRefs = %v: want tls and cm only (both is also read as required)", got)
	}
}

func TestMessageWordsTCPProbeAndRuns(t *testing.T) {
	for in, want := range map[string]string{
		"Startup probe failed: dial tcp 10.180.128.178:8002: connect: connection refused": "startup check failed: nothing listening on port 8002",
		"Saw completed job: tock-29855456, condition: Failed":                             "run tock-29855456 failed",
		"Job has reached the specified backoff limit":                                     "it failed, and so did any retries it was allowed",
	} {
		if got := MessageWords(in); got != want {
			t.Errorf("MessageWords(%q) = %q, want %q", in, got, want)
		}
	}
}

// Every grouped rollout row in :shipped names what it runs.
func TestGroupImages(t *testing.T) {
	for in, want := range map[string]string{
		"3 workloads got a new version: api:v2":                       "api:v2",
		"3 workloads: settings changed, same images (api:v2)":         "settings changed, same images (api:v2)",
		"2 workloads: same tag, new build (corp:production-9005bc71)": "same tag, new build (corp:production-9005bc71)",
	} {
		if got := groupImages(in); got != want {
			t.Errorf("groupImages(%q) = %q, want %q", in, got, want)
		}
	}
}

// A native sidecar's requests count toward the pod's, as kubectl counts
// them; a setup init container's don't.
func TestPodUsageCountsSidecars(t *testing.T) {
	e := New(nil, nil, nil)
	res := func(cpu string) map[string]any {
		return map[string]any{"requests": map[string]any{"cpu": cpu, "memory": "16Mi"}}
	}
	pod := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "app", "resources": res("100m")}},
		"initContainers": []any{
			map[string]any{"name": "log", "restartPolicy": "Always", "resources": res("5m")},
			map[string]any{"name": "setup", "resources": res("500m")},
		},
	}}}
	if u := e.PodUsage(pod); u.ReqCPU != 105 || u.ReqMem != 32<<20 {
		t.Errorf("reserves %dm CPU and %d bytes, want 105m and 32Mi", u.ReqCPU, u.ReqMem)
	}
}

// Scale-up events are read whether alone or merged by Kubernetes.
func TestScaledUpRS(t *testing.T) {
	for _, msg := range []string{
		"Scaled up replica set web-576fcd6f8b from 0 to 1",
		"(combined from similar events): Scaled up replica set web-576fcd6f8b from 0 to 2",
	} {
		if m := scaledUpRS.FindStringSubmatch(msg); m == nil || m[1] != "web-576fcd6f8b" {
			t.Errorf("%q: got %v", msg, m)
		}
	}
}

// A StatefulSet restarted after its last change keeps that change's date:
// a restart is not a deploy.
func TestRevisionDeployTimeSkipsRestarts(t *testing.T) {
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "db", "namespace": "qa", "uid": "sts-1"}}}
	rev := func(name string, n int64, created time.Time, image, restarted string) *unstructured.Unstructured {
		meta := map[string]any{}
		if restarted != "" {
			meta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": restarted}
		}
		return &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": created.UTC().Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-1", "controller": true}}},
			"revision": n,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{"$patch": "replace", "metadata": meta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "db", "image": image}}}}}},
		}}
	}
	now := time.Now()
	store := fakeStore{kube.ControllerRevisions: {
		rev("db-1", 1, now.Add(-48*time.Hour), "pg:15", ""),
		rev("db-2", 2, now.Add(-24*time.Hour), "pg:16", ""),
		rev("db-3", 3, now.Add(-time.Minute), "pg:16", now.Format(time.RFC3339)),
	}}
	e := New(store, nil, nil)
	got, ok := e.RevisionDeployTime(sts)
	if !ok || got.Before || got.At.Sub(now.Add(-24*time.Hour)).Abs() > time.Second {
		t.Fatalf("deployed %+v (ok %v), want the pg:16 change a day ago", got, ok)
	}
	// Only restarts kept: the change itself is older than the history.
	e = New(fakeStore{kube.ControllerRevisions: {
		rev("db-8", 8, now.Add(-48*time.Hour), "pg:16", now.Add(-48*time.Hour).Format(time.RFC3339)),
		rev("db-9", 9, now.Add(-24*time.Hour), "pg:16", now.Add(-24*time.Hour).Format(time.RFC3339)),
	}}, nil, nil)
	if got, ok := e.RevisionDeployTime(sts); !ok || !got.Before || got.At.Sub(now.Add(-48*time.Hour)).Abs() > time.Second {
		t.Fatalf("with only restarts kept: %+v, want before two days ago", got)
	}
}

func TestShortImage(t *testing.T) {
	for in, want := range map[string]string{
		"123456789012.dkr.ecr.ap-south-1.amazonaws.com/shop-backend:production-8777ac01": "shop-backend:production-8777ac01",
		"alpine/git:latest":                  "alpine/git:latest",
		"docker.io/library/nginx:alpine":     "nginx:alpine",
		"nginx:1.27@sha256:0123456789abcdef": "nginx:1.27",
		"ghcr.io/org/team/tool:v1":           "team/tool:v1",
	} {
		if got := ShortImage(in); got != want {
			t.Errorf("ShortImage(%q) = %q, want %q", in, got, want)
		}
	}
}

// A StatefulSet's versions are its ControllerRevisions: newest first, with
// the pods each one runs, and each change shows on the timeline.
func TestStatefulSetVersions(t *testing.T) {
	now := time.Now()
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "db", "namespace": "qa", "uid": "sts-1", "creationTimestamp": now.Add(-time.Hour).UTC().Format(time.RFC3339)},
		"spec":     map[string]any{"replicas": int64(1)}}}
	rev := func(name string, n int64, created time.Time, image string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": created.UTC().Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-1", "controller": true}}},
			"revision": n,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{"$patch": "replace",
				"spec": map[string]any{"containers": []any{map[string]any{"name": "db", "image": image}}}}}},
		}}
	}
	pod := &unstructured.Unstructured{Object: map[string]any{"kind": "Pod",
		"metadata": map[string]any{"name": "db-0", "namespace": "qa", "uid": "p1", "labels": map[string]any{"controller-revision-hash": "db-b"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-1", "controller": true}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}}
	e := New(fakeStore{
		kube.StatefulSets:        {sts},
		kube.Pods:                {pod},
		kube.ControllerRevisions: {rev("db-a", 1, now.Add(-time.Hour), "nginx:1.26"), rev("db-b", 2, now.Add(-time.Minute), "nginx:1.27")},
	}, nil, nil)
	r := e.RolloutOf(sts)
	if len(r.All) != 2 || r.Current.Revision != 2 || r.Previous.Revision != 1 || r.Current.Ready != 1 || r.Previous.Replicas != 0 {
		t.Fatalf("versions: %+v current %+v", r.All, r.Current)
	}
	found := false
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		if c.Object == "statefulset/db" && strings.Contains(c.Detail, "revision 2: nginx:1.27") {
			found = true
		}
	}
	if !found {
		t.Error("the image change isn't on the timeline")
	}

	// Gone back to revision 2's template (kept, renumbered) 5 minutes ago,
	// long after it was first made: it says when it came back.
	back := now.Add(-5 * time.Minute)
	sts.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: back}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(sts)
	pod.SetCreationTimestamp(metav1.Time{Time: back.Add(2 * time.Second)})
	e.store.(fakeStore)[kube.ControllerRevisions][1].Object["metadata"].(map[string]any)["creationTimestamp"] = now.Add(-30 * time.Minute).UTC().Format(time.RFC3339)
	r = e.RolloutOf(sts)
	if !r.Current.Reused || r.Current.LiveSince.Sub(back).Abs() > time.Second {
		t.Fatalf("came back: %+v", r.Current)
	}
	found = false
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		if strings.Contains(c.Detail, "went back to an earlier version") && c.When.Sub(back).Abs() <= time.Second {
			found = true
		}
	}
	if !found {
		t.Error("going back isn't on the timeline at the time it happened")
	}
}

// A kept revision renumbered by going back says when that happened, even
// once a newer one runs: each go-back keeps its row and its time.
func TestStatefulSetGoBacksKeepTheirTimes(t *testing.T) {
	now := time.Now()
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "db", "namespace": "qa", "uid": "sts-9", "creationTimestamp": now.Add(-time.Hour).UTC().Format(time.RFC3339)}}}
	rev := func(name string, n int64, created, written time.Time, image string) *unstructured.Unstructured {
		cr := &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": created.UTC().Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-9", "controller": true}}},
			"revision": n,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{
				"spec": map[string]any{"containers": []any{map[string]any{"name": "db", "image": image}}}}}},
		}}
		cr.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: written}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:data":{},"f:revision":{}}`)}})
		kube.NoteTemplateChange(cr)
		return cr
	}
	first, second, backTo26, backTo27, restart := now.Add(-50*time.Minute), now.Add(-40*time.Minute), now.Add(-30*time.Minute), now.Add(-20*time.Minute), now.Add(-10*time.Minute)
	e := New(fakeStore{
		kube.StatefulSets: {sts},
		kube.ControllerRevisions: {
			rev("gb-a", 3, first, backTo26, "nginx:1.26"),  // made first, brought back
			rev("gb-b", 4, second, backTo27, "nginx:1.27"), // made second, brought back
			rev("gb-c", 5, restart, restart, "nginx:1.27"), // a restart after
		},
	}, nil, nil)
	got := map[string]time.Time{}
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		got[c.Detail] = c.When
	}
	for detail, when := range map[string]time.Time{
		"revision 3: went back to an earlier version: nginx:1.26": backTo26,
		"revision 4: went back to an earlier version: nginx:1.27": backTo27,
	} {
		if at, ok := got[detail]; !ok || at.Sub(when).Abs() > time.Second {
			t.Errorf("missing %q at %v; got %v", detail, when, got)
		}
	}
	if d, _ := e.RevisionDeployTime(sts); d.At.Sub(backTo27).Abs() > time.Second {
		t.Errorf("deployed %v, want when 1.27 came back (%v)", d.At, backTo27)
	}
}

// Revisions all written at once (an upgrade, an adoption) weren't gone
// back to: only a lone renumbering is a go-back.
func TestRevisionsWrittenTogetherAreNotGoBacks(t *testing.T) {
	now := time.Now()
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "redis", "namespace": "qa", "uid": "sts-r"}}}
	var crs []*unstructured.Unstructured
	together := now.Add(-time.Hour)
	for i, name := range []string{"r-1", "r-2", "r-3"} {
		cr := &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": now.Add(-time.Duration(30-i) * 24 * time.Hour).UTC().Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "redis", "uid": "sts-r", "controller": true}}},
			"revision": int64(i + 1),
			"data":     map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "r", "image": fmt.Sprintf("redis:%d", i)}}}}}},
		}}
		cr.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: together}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:revision":{}}`)}})
		kube.NoteTemplateChange(cr)
		crs = append(crs, cr)
	}
	e := New(fakeStore{kube.StatefulSets: {sts}, kube.ControllerRevisions: crs}, nil, nil)
	for _, v := range e.RolloutOf(sts).All {
		if v.Reused {
			t.Errorf("revision %d marked as gone back to", v.Revision)
		}
	}
}

// Only a scale-up from no pods is a version going live; adding copies to a
// running one isn't.
func TestScaledFromZero(t *testing.T) {
	for msg, want := range map[string]bool{
		"Scaled up replica set web-abc from 0 to 2":   true,
		"Scaled up replica set web-abc to 2 from 0":   true,
		"Scaled up replica set web-abc from 2 to 3":   false,
		"Scaled up replica set web-abc to 3 from 2":   false,
		"Scaled up replica set web-abc from 0 to 10":  true,
		"Scaled up replica set web-abc from 10 to 12": false,
	} {
		if got := scaledFromZero.MatchString(msg); got != want {
			t.Errorf("%q: %v, want %v", msg, got, want)
		}
	}
}

// QA's sequence: new image (2), restart (3), back to the first (renumbered
// 4), back to the restart (renumbered 5). The restart's first time out says
// so, and going back to 4 comes after the restart was made.
func TestGoBackAfterRestartReadsInOrder(t *testing.T) {
	now := time.Now().UTC()
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-w",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "5"}}}}
	rs := func(name string, rev, history string, made time.Time, image, restarted string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		tmeta := map[string]any{}
		if restarted != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": restarted}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann,
				"creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-w", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": image}}}}}}}
	}
	first, image, restart := now.Add(-15*time.Minute), now.Add(-8*time.Minute), now.Add(-7*time.Minute)
	stamp := restart.Format(time.RFC3339)
	e := New(fakeStore{
		kube.Deployments: {dep},
		kube.ReplicaSets: {
			rs("web-a", "4", "1", first, "nginx:alpine", ""),
			rs("web-b", "2", "", image, "nginx:1.27", ""),
			rs("web-c", "5", "3", restart, "nginx:1.27", stamp),
		},
	}, nil, nil)
	fx, _ := fixture(t, false)
	e.res = fx.res // owners are found by kind
	rows := e.changesOver("qa", "", now, ChangeWindow, nil)
	at, index := map[string]Change{}, map[string]int{}
	for i, c := range rows {
		at[c.Detail], index[c.Detail] = c, i
	}
	restartRow := "revision 3 (later brought back): restarted, same images (nginx:1.27)"
	backRow := "revision 4: went back to an earlier version: nginx:alpine"
	if _, ok := at[restartRow]; !ok {
		t.Fatalf("missing %q in %v", restartRow, at)
	}
	// Nothing dates the go-back: no earlier than the restart, said as a
	// bound, and listed above it (newest first).
	if at[backRow].When.Before(at[restartRow].When) || !at[backRow].After || index[backRow] > index[restartRow] {
		t.Errorf("going back to 4 (%v, after %v) must come after the restart (%v)", at[backRow].When, at[backRow].After, at[restartRow].When)
	}
}

// Copies added by a scale show as new pods, even right after a rollout;
// a rollout's own scale-ups (paired with a scale-down) don't count.
func TestCopiesAddedByScaling(t *testing.T) {
	now := time.Now().UTC()
	ev := func(name, msg string, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
			"lastTimestamp":  at.Format(time.RFC3339)}}
	}
	e := New(fakeStore{kube.Events: {
		ev("a", "Scaled up replica set web-new from 1 to 2", now.Add(-4*time.Minute)), // the rollout
		ev("b", "Scaled down replica set web-old from 2 to 1", now.Add(-4*time.Minute+10*time.Second)),
		ev("c", "Scaled up replica set web-new from 2 to 3", now.Add(-time.Minute)), // by hand
	}}, nil, nil)
	when, words := e.copiesAdded("qa")
	if at := when["qa/web-new"]; at.Sub(now.Add(-time.Minute)).Abs() > time.Second || words["qa/web-new"] != "scaled up from 2 to 3" {
		t.Errorf("added at %v (%q), want a minute ago, from 2 to 3", at, words["qa/web-new"])
	}
	e = New(fakeStore{kube.Events: {
		ev("a", "Scaled up replica set web-new from 1 to 2", now.Add(-4*time.Minute)),
		ev("b", "Scaled down replica set web-old from 2 to 1", now.Add(-4*time.Minute+10*time.Second)),
	}}, nil, nil)
	if when, _ := e.copiesAdded("qa"); len(when) != 0 {
		t.Errorf("a rollout's scale-up counted as copies added: %v", when)
	}
	// QA's case: the rollout's last scale-down, then a scale by hand 39s
	// later. The scale-down came before: the rollout had ended.
	e = New(fakeStore{kube.Events: {
		ev("a", "Scaled down replica set web-old from 1 to 0", now.Add(-2*time.Minute)),
		ev("b", "Scaled up replica set web-new from 2 to 3", now.Add(-2*time.Minute+39*time.Second)),
	}}, nil, nil)
	when, _ = e.copiesAdded("qa")
	if when["qa/web-new"].IsZero() {
		t.Error("a scale just after a rollout was missed")
	}
	// Round 46: a fast rollout's scale-down 5s before the scale is over;
	// a merged event's words and time are its last scale's.
	e = New(fakeStore{kube.Events: {
		ev("a", "Scaled down replica set web-old from 1 to 0", now.Add(-time.Minute-5*time.Second)),
		ev("b", "Scaled up replica set web-new from 2 to 3", now.Add(-time.Minute)),
		ev("c", "(combined from similar events): Scaled up replica set web-other from 1 to 2", now),
	}}, nil, nil)
	when, _ = e.copiesAdded("qa")
	if when["qa/web-new"].IsZero() {
		t.Error("a scale 5s after a rollout ended was missed")
	}
	if when["qa/web-other"].IsZero() {
		t.Error("a merged event's last scale was missed")
	}
}

// A container killed for running out of memory is found from its last
// termination.
func TestLastOutOfMemory(t *testing.T) {
	at := time.Now().Add(-5 * time.Hour).UTC().Truncate(time.Second)
	p := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"containerStatuses": []any{
		map[string]any{"name": "server", "lastState": map[string]any{"terminated": map[string]any{"reason": "OOMKilled", "finishedAt": at.Format(time.RFC3339)}}},
	}}}}
	if got := lastOutOfMemory(p); !got.Equal(at) {
		t.Errorf("ran out of memory at %v, want %v", got, at)
	}
}

// Which young pods are worth a line: a batch with no older pod beside it
// came with a rollout or go-back; one young pod beside older ones was
// added (by a scale made just before it) or replaced one.
func TestNewPodsBesideOlderOnes(t *testing.T) {
	now := time.Now().UTC()
	kube.ResetPodWatch(now.Add(-2 * time.Hour)) // kubed watching all along
	fx, _ := fixture(t, false)
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-1", "namespace": "qa", "uid": "rs-1", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339)}}}
	pod := func(name string, made time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-1", "uid": "rs-1", "controller": true}}}}}
	}
	scaled := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "e", "namespace": "qa"}, "reason": "ScalingReplicaSet",
		"message": "Scaled up replica set web-1 from 2 to 3", "involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
		"lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339)}}
	rows := func(pods ...*unstructured.Unstructured) map[string]string {
		e := New(fakeStore{kube.ReplicaSets: {rs}, kube.Pods: pods, kube.Events: {scaled}}, nil, nil)
		e.res = fx.res
		out := map[string]string{}
		for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
			if c.What == "new pod" {
				out[c.Object] = c.Detail
			}
		}
		return out
	}
	// A go-back: three pods made together, none older. No rows.
	if got := rows(pod("a", now.Add(-3*time.Minute)), pod("b", now.Add(-3*time.Minute)), pod("c", now.Add(-3*time.Minute+2*time.Second))); len(got) != 0 {
		t.Errorf("a batch shown as new pods: %v", got)
	}
	// A copy added a minute ago beside pods an hour old: said as added;
	// a rollout pod made 5s before the scale is not.
	got := rows(pod("old1", now.Add(-time.Hour)), pod("old2", now.Add(-time.Hour)), pod("early", now.Add(-time.Minute-5*time.Second)), pod("copy", now.Add(-time.Minute+time.Second)))
	if got["pod/copy"] != "added a copy: scaled up from 2 to 3" {
		t.Errorf("the copy reads %q (all: %v)", got["pod/copy"], got)
	}
	if strings.HasPrefix(got["pod/early"], "added a copy") {
		t.Errorf("a pod made before the scale said added: %q", got["pod/early"])
	}
	// Round 72 to 76: a scale up by one, a pod deleted 30s later, then
	// the old pods replaced one by one, as kubed's watch saw them come
	// and go. Which of the two pods made just after the scale is the
	// added copy can't be told: neither is guessed.
	watch := func(pods ...*unstructured.Unstructured) {
		for _, p := range pods {
			kube.NotePod(p)
		}
	}
	gone := func(p *unstructured.Unstructured) {
		q := p.DeepCopy()
		q.SetDeletionTimestamp(&metav1.Time{Time: now})
		kube.NotePod(q)
	}
	o1, o2, o3 := pod("r76-o1", now.Add(-time.Hour)), pod("r76-o2", now.Add(-time.Hour)), pod("r76-o3", now.Add(-time.Hour))
	a := pod("r76-a", now.Add(-time.Minute))
	watch(o1, o2, o3, a)
	if got := rows(o1, o2, o3, a); !strings.HasPrefix(got["pod/r76-a"], "added a copy") {
		t.Errorf("alone after the scale: %v", got)
	}
	b := pod("r76-b", now.Add(-30*time.Second))
	gone(o1)
	watch(b)
	got = rows(o2, o3, a, b)
	if strings.HasPrefix(got["pod/r76-a"], "added a copy") || strings.HasPrefix(got["pod/r76-b"], "added a copy") ||
		got["pod/r76-a"] == "" || got["pod/r76-b"] == "" {
		t.Errorf("a scale and a replacement in its minute: %v", got)
	}
	c, d := pod("r76-c", now.Add(-15*time.Second)), pod("r76-d", now.Add(-time.Second))
	gone(o2)
	watch(c)
	gone(o3)
	watch(d)
	got = rows(a, b, c, d)
	for _, n := range []string{"r76-a", "r76-b", "r76-c", "r76-d"} {
		if got["pod/"+n] == "" {
			t.Errorf("%s lost its row once the old pods went: %v", n, got)
		}
	}
	// A scale from 0: the pods before it are gone, the new ones start fresh.
	gone(a)
	gone(b)
	gone(c)
	gone(d)
	z1, z2 := pod("r76-z1", now.Add(-40*time.Second)), pod("r76-z2", now.Add(-40*time.Second))
	watch(z1, z2)
	if got := rows(z1, z2); len(got) != 0 {
		t.Errorf("a scale from 0's pods got rows: %v", got)
	}
	// Round 77: the pods made in the scale's minute were more than it
	// added; once the others are gone, the one left is still not called
	// the added copy (it may be a replacement, the copy gone).
	gone(z2)
	if got := rows(z1); strings.HasPrefix(got["pod/r76-z1"], "added a copy") {
		t.Errorf("the one left was called the added copy: %v", got)
	}
	// Round 80: a scale added 2; one of them is deleted later: the other
	// is still the added copy (2 made in the minute, as many as it added).
	kube.ResetPodWatch(now.Add(-2 * time.Hour))
	scaled.Object["message"] = "Scaled up replica set web-1 from 1 to 3"
	k1, k2 := pod("r80-k1", now.Add(-time.Minute+time.Second)), pod("r80-k2", now.Add(-time.Minute+2*time.Second))
	watch(pod("r80-old", now.Add(-time.Hour)), k1, k2)
	gone(k2)
	if got := rows(pod("r80-old", now.Add(-time.Hour)), k1); !strings.HasPrefix(got["pod/r80-k1"], "added a copy") {
		t.Errorf("the scale's other copy lost its words: %v", got)
	}
	scaled.Object["message"] = "Scaled up replica set web-1 from 2 to 3"
	// A kubed that wasn't watching at the scale can't say either.
	kube.ResetPodWatch(now)
	s := pod("r77-s", now.Add(-50*time.Second))
	kube.NotePod(s)
	if got := rows(pod("r77-old", now.Add(-time.Hour)), s); strings.HasPrefix(got["pod/r77-s"], "added a copy") {
		t.Errorf("started after the scale, yet: %v", got)
	}
}

// Scaling by hand shows in what changed; a rollout's own scaling, a new
// deployment's first pods and an old version scaled down after a go-back
// (Kubernetes' merged events) don't.
func TestHandScalesShow(t *testing.T) {
	now := time.Now().UTC()
	ev := func(name, msg string, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
			"lastTimestamp":  at.Format(time.RFC3339)}}
	}
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-web",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}}}}
	rs := func(name, rev string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name,
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web", "controller": true}}}}}
	}
	scales := func(events ...*unstructured.Unstructured) []string {
		e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs("web-new", "2"), rs("web-old", "1")}, kube.Events: events}, testResources(), nil)
		var got []string
		for _, c := range e.handScales("qa") {
			got = append(got, c.Detail)
		}
		return got
	}
	got := scales(
		ev("n0", "Scaled up replica set web-new from 0 to 1", now.Add(-50*time.Minute)), // made: not a scale
		ev("r1", "Scaled up replica set web-new from 1 to 2", now.Add(-30*time.Minute)), // a rollout step
		ev("r2", "Scaled down replica set web-old from 2 to 1", now.Add(-30*time.Minute+20*time.Second)),
		ev("h1", "Scaled down replica set web-new from 2 to 0", now.Add(-10*time.Minute)), // by hand
		ev("h2", "Scaled up replica set web-new from 0 to 2", now.Add(-5*time.Minute)),
	)
	if strings.Join(got, "|") != "scaled from 2 to 0: nothing runs|scaled from 0 to 2" {
		t.Errorf("hand scales: %v", got)
	}
	// Round 66: Kubernetes merged the scale down into the scale up: the
	// version running now, long after it was made, came up from 0.
	if got := scales(ev("m", "(combined from similar events): Scaled up replica set web-new from 0 to 2", now.Add(-5*time.Minute))); strings.Join(got, "|") != "scaled from 0 to 2" {
		t.Errorf("a merged scale up from 0: %v", got)
	}
	// A clean scale while an older version ran stays after newer rollouts.
	if got := scales(ev("o", "Scaled up replica set web-old from 2 to 3", now.Add(-20*time.Minute))); strings.Join(got, "|") != "scaled from 2 to 3" {
		t.Errorf("a scale of the version that ran then: %v", got)
	}
	// After a go-back, only the old version's merged scale-down is left.
	if got := scales(ev("m", "(combined from similar events): Scaled down replica set web-old from 1 to 0", now.Add(-time.Minute))); len(got) != 0 {
		t.Errorf("an old version scaled down read as scaling: %v", got)
	}
}

// A deployment's version brought back says when the go-back wrote its
// template, whatever Kubernetes' merged scaling events say.
func TestGoBackTimeFromTemplate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	back := now.Add(-8 * time.Minute)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-gb",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}},
		// It moved when the go-back rolled out (a write while paused moves nothing).
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx"}}}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "True",
			"lastUpdateTime": back.Add(5 * time.Second).Format(time.RFC3339)}}}}}
	dep.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: back}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(dep)
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-a", "namespace": "qa", "uid": "web-a-gb", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "4", "deployment.kubernetes.io/revision-history": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-gb", "controller": true}}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx"}}}}}}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, testResources(), nil)
	if got := e.RolloutOf(dep).Current.LiveSince; !got.Equal(back) {
		t.Errorf("came back at %v, want %v", got, back)
	}
}

// A stateful set changed 41s after it was made, then brought back: that
// change keeps its row; only its very first revision is the created row.
func TestStatefulSetQuickChangeKept(t *testing.T) {
	now := time.Now().UTC()
	made := now.Add(-20 * time.Minute)
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "db", "namespace": "qa", "uid": "sts-q", "creationTimestamp": made.Format(time.RFC3339)}}}
	rev := func(name string, n int64, created, written time.Time, image string) *unstructured.Unstructured {
		cr := &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": created.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-q", "controller": true}}},
			"revision": n,
			"data":     map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "db", "image": image}}}}}},
		}}
		cr.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: written}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:revision":{}}`)}})
		kube.NoteTemplateChange(cr)
		return cr
	}
	e := New(fakeStore{kube.StatefulSets: {sts}, kube.ControllerRevisions: {
		rev("q-a", 3, made, now.Add(-10*time.Minute), "nginx:1.27"),                    // first, brought back
		rev("q-b", 4, made.Add(41*time.Second), now.Add(-5*time.Minute), "nginx:1.28"), // 41s later, brought back
	}}, nil, nil)
	found := false
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		if strings.Contains(c.Detail, "later brought back") && strings.Contains(c.Detail, "nginx:1.28") {
			found = true
		}
	}
	if !found {
		t.Error("the change made 41s after creation is missing")
	}
}

// A back-off pulling an image is a download that keeps failing, not a crash.
func TestPullBackOffIsNotACrash(t *testing.T) {
	got := eventDetail("BackOff", `Back-off pulling image "docker.io/library/nginx:nope-qa59"`) // as the timeline reads it
	if got != "can't download image nginx:nope-qa59; trying again" {
		t.Errorf("got %q", got)
	}
	if got := eventWords("BackOff", "Back-off restarting failed container web in pod web-1"); !strings.Contains(got, "keeps crashing") {
		t.Errorf("crash reads %q", got)
	}
}

// Gone back from a broken image: the good version was never scaled down,
// so no scale-up dates the go-back. The broken one's scale-down to 0 alone
// doesn't either (see goback_test.go for when it does): only a bound.
func TestGoBackFromBrokenImageKeepsItsTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	back := now.Add(-8 * time.Minute)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-bi",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}}}}
	rs := func(name, rev, history string, made time.Time, image string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339), "annotations": ann,
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-bi", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": image}}}}}}}
	}
	down := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "d", "namespace": "qa"}, "reason": "ScalingReplicaSet",
		"message": "Scaled down replica set web-bad from 1 to 0", "involvedObject": map[string]any{"kind": "Deployment", "name": "web"},
		"lastTimestamp": back.Format(time.RFC3339)}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.Events: {down}, kube.ReplicaSets: {
		rs("web-good", "3", "1", now.Add(-time.Hour), "nginx:alpine"), // brought back from the broken one
		rs("web-bad", "2", "", now.Add(-10*time.Minute), "nginx:nope"),
		rs("web-new", "4", "", now.Add(-2*time.Minute), "nginx:1.27"), // a newer rollout since
	}}, testResources(), nil)
	// The broken version's scale-down alone can't date it: a scale to 0
	// by hand, with a go-back made while nothing ran, leaves the same.
	// A bound, no later than that, everywhere.
	found := false
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		if strings.HasPrefix(c.Detail, "revision 3: went back") {
			found = true
			if !c.After || c.When.After(back) {
				t.Errorf("went back at %v (after %v), want a bound no later than %v", c.When, c.After, back)
			}
		}
	}
	if !found {
		t.Error("no go-back row")
	}
	for _, v := range e.RolloutOf(dep).All {
		if v.Name == "web-good" && (!v.LiveSince.IsZero() || v.BackAfter.IsZero() || v.BackAfter.After(back)) {
			t.Errorf("versions list: back at %v (or after %v), want a bound no later than %v", v.LiveSince, v.BackAfter, back)
		}
	}
}

// A stateful set's restart, later brought back by a go-back, is still a
// restart: not a new version on :shipped.
func TestStatefulSetRestartBroughtBackIsARestart(t *testing.T) {
	now := time.Now().UTC()
	made := now.Add(-30 * time.Minute)
	sts := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet",
		"metadata": map[string]any{"name": "db", "namespace": "qa", "uid": "sts-r2", "creationTimestamp": made.Format(time.RFC3339)}}}
	rev := func(name string, n int64, created, written time.Time, image, restarted string) *unstructured.Unstructured {
		meta := map[string]any{}
		if restarted != "" {
			meta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": restarted}
		}
		cr := &unstructured.Unstructured{Object: map[string]any{"kind": "ControllerRevision",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": created.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db", "uid": "sts-r2", "controller": true}}},
			"revision": n,
			"data": map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": meta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "db", "image": image}}}}}},
		}}
		cr.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: written}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:revision":{}}`)}})
		kube.NoteTemplateChange(cr)
		return cr
	}
	restart := made.Add(10 * time.Minute)
	e := New(fakeStore{kube.StatefulSets: {sts}, kube.ControllerRevisions: {
		rev("r-1", 1, made, made, "nginx:1.27", ""),
		rev("r-2", 4, restart, now.Add(-5*time.Minute), "nginx:1.27", restart.Format(time.RFC3339)), // the restart, brought back
		rev("r-3", 3, restart.Add(time.Minute), restart.Add(time.Minute), "nginx:alpine", ""),
	}}, nil, nil)
	for _, c := range e.Shipped("qa", now, ShipWindow) {
		if c.When.Sub(restart).Abs() < time.Second {
			t.Errorf("the restart is on :shipped: %+v", c)
		}
	}
}

// A crash back-off names its container, read from the event's own words.
func TestCrashBackOffNamesTheContainer(t *testing.T) {
	got := eventDetail("BackOff", "Back-off restarting failed container web in pod web-1_qa(abc)")
	if got != `container "web" keeps crashing; waiting before the next restart` {
		t.Errorf("got %q", got)
	}
}

// An event in a series is dated by its latest time and counts all of it.
func TestEventSeriesLatestTime(t *testing.T) {
	first, last := time.Now().Add(-2*time.Hour).UTC(), time.Now().Add(-time.Minute).UTC()
	ev := &unstructured.Unstructured{Object: map[string]any{"eventTime": first.Format(time.RFC3339Nano),
		"series": map[string]any{"count": int64(40), "lastObservedTime": last.Format(time.RFC3339Nano)}}}
	if got := EventTime(ev); got.Sub(last).Abs() > time.Second {
		t.Errorf("time %v, want %v", got, last)
	}
	if got := EventCount(ev); got != 40 {
		t.Errorf("count %d", got)
	}
}

// Round 65: new image (2), back to the first (renumbered 3), restart (4).
// The restart is said as one, against the version brought back, not the
// one made just before it.
func TestRestartAfterGoBack(t *testing.T) {
	now := time.Now().UTC()
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-rg",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}}}}
	rs := func(name string, rev, history string, made time.Time, image, restarted string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		tmeta := map[string]any{}
		if restarted != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": restarted}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann,
				"creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences":   []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-rg", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "web", "image": image}}}}}}}
	}
	restart := now.Add(-3 * time.Minute)
	e := New(fakeStore{
		kube.Deployments: {dep},
		kube.ReplicaSets: {
			rs("web-rg-a", "3", "1", now.Add(-time.Hour), "nginx:alpine", ""),
			rs("web-rg-b", "2", "", now.Add(-20*time.Minute), "nginx:1.27-alpine", ""),
			rs("web-rg-c", "4", "", restart, "nginx:alpine", restart.Format(time.RFC3339)),
		},
	}, nil, nil)
	fx, _ := fixture(t, false)
	e.res = fx.res
	want := "revision 4: restarted, same images (nginx:alpine)"
	found := false
	for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
		found = found || c.Detail == want
	}
	if !found {
		t.Errorf("missing %q", want)
	}
}

// A restart under way (the Deployment's template has a new stamp, its
// new ReplicaSet not seen yet) doesn't move the time the running version
// came back.
func TestRestartUnderWayKeepsGoBackTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	restart := now.Add(-time.Minute)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-ru",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "3"}},
		"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{
			"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": restart.Format(time.RFC3339)}}}}}}
	dep.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: restart}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(dep)
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "web-ru", "namespace": "qa", "uid": "web-ru", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "3", "deployment.kubernetes.io/revision-history": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-ru", "controller": true}}}}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}}, testResources(), nil)
	if got := e.RolloutOf(dep).Current.LiveSince; got.Equal(restart) {
		t.Errorf("came back at the restart's time %v", got)
	}
	if got := e.DeployTimes("qa")["dep-ru"].At; got.Equal(restart) {
		t.Errorf("deployed at the restart's time %v", got)
	}
	if _, ok := wentBack("web-ru", "3"); ok {
		t.Error("noted the restart as the go-back")
	}
}

// Round 70 to 72: a container killed for failing its liveness check is
// told apart from one that crashes, with the check's last answer in
// words, for as long as it keeps happening: Kubernetes stops recording a
// busy pod's events after a while, its kills go on.
func TestKilledByAliveCheck(t *testing.T) {
	now := time.Now().UTC()
	ev := func(pod, name, reason, msg string, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": reason, "message": msg,
			"involvedObject": map[string]any{"kind": "Pod", "name": pod, "fieldPath": "spec.containers{web}"},
			"lastTimestamp":  at.Format(time.RFC3339)}}
	}
	kill := func(pod string, at time.Time) *unstructured.Unstructured {
		return ev(pod, pod+"-k", "Killing", "Container web failed liveness probe, will be restarted", at)
	}
	failed := func(pod string, at time.Time) *unstructured.Unstructured {
		return ev(pod, pod+"-u", "Unhealthy", "Liveness probe failed: HTTP probe failed with statuscode: 404", at)
	}
	e := New(fakeStore{kube.Events: {kill("p1", now.Add(-time.Minute)), failed("p1", now.Add(-time.Minute))}}, testResources(), nil)
	if _, words, ok := e.KilledByCheck("qa", "p1", "web", now.Add(-time.Minute), 0); !ok || !strings.Contains(words, "404") {
		t.Errorf("got %q, %v", words, ok)
	}
	if _, _, ok := e.KilledByCheck("qa", "p1", "sidecar", now.Add(-time.Minute), 0); ok {
		t.Error("another container's kill counted")
	}
	if _, _, ok := e.KilledByCheck("qa", "p1", "web", now, 1); ok {
		t.Error("a crash with exit 1 counted as a kill by the check")
	}
	// Both events frozen long before this stop: a kill still kept says so.
	e = New(fakeStore{kube.Events: {kill("p2", now.Add(-20*time.Minute)), failed("p2", now.Add(-15*time.Minute))}}, testResources(), nil)
	if _, _, ok := e.KilledByCheck("qa", "p2", "web", now, 0); !ok {
		t.Error("frozen events: the kill was missed")
	}
	// Only the check's event left, at the stop.
	e = New(fakeStore{kube.Events: {failed("p3", now)}}, testResources(), nil)
	if _, _, ok := e.KilledByCheck("qa", "p3", "web", now, 137); !ok {
		t.Error("the check failing as it stopped was missed")
	}
	// Seen once, then every event gone: remembered.
	e = New(fakeStore{}, testResources(), nil)
	if _, _, ok := e.KilledByCheck("qa", "p3", "web", now.Add(time.Hour), 0); !ok {
		t.Error("forgot a container seen killed by its check")
	}
	if _, _, ok := e.KilledByCheck("qa", "p4", "web", now, 0); ok {
		t.Error("a container never seen killed counted")
	}
}

// Round 75: a daemon set's restart replaces its pod; the new pod is part
// of the restart (within about 2 minutes), though kubed saw the old one.
func TestRestartPodIsPartOfRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ds := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "ds", "namespace": "qa", "uid": "ds-r75", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339)},
		"spec":     map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": now.Add(-10 * time.Minute).Format(time.RFC3339)}}}}}}
	ds.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: now.Add(-10 * time.Minute)}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(ds)
	pod := func(name string, made time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "ds-r75", "controller": true}}}}}
	}
	rows := func(pods ...*unstructured.Unstructured) []string {
		e := New(fakeStore{kube.DaemonSets: {ds}, kube.Pods: pods}, kube.NewResources([]kube.Resource{
			{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
			{GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true},
		}), nil)
		var out []string
		for _, c := range e.changesOver("qa", "", now, ChangeWindow, nil) {
			if c.What == "new pod" {
				out = append(out, c.Object+": "+c.Detail)
			}
		}
		return out
	}
	rows(pod("ds-r75-old", now.Add(-time.Hour))) // seen before the restart
	if got := rows(pod("ds-r75-new", now.Add(-9*time.Minute))); len(got) != 0 {
		t.Errorf("the restart's pod got a row: %v", got)
	}
	// A pod replaced well after the restart does get one.
	if got := rows(pod("ds-r75-late", now.Add(-2*time.Minute))); len(got) != 1 {
		t.Errorf("a later replacement: %v", got)
	}
}

// Round 79: a route in another namespace that sends to a service (allowed
// by a ReferenceGrant) is listed, as the app's box lists it.
func TestRoutesFromOtherNamespaces(t *testing.T) {
	route := func(ns, name, toNS string) *unstructured.Unstructured {
		ref := map[string]any{"name": "app-server"}
		if toNS != "" {
			ref["namespace"] = toNS
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
			"metadata": map[string]any{"name": name, "namespace": ns},
			"spec": map[string]any{"hostnames": []any{name + ".example"},
				"rules": []any{map[string]any{"backendRefs": []any{ref}}}}}}
	}
	e := New(fakeStore{kube.HTTPRoutes: {
		route("be", "own", ""),
		route("corp", "corp-route", "be"),
		route("corp", "corp-own", ""), // its own namespace's app-server, not be's
	}}, kube.NewResources([]kube.Resource{{GVR: kube.HTTPRoutes, Kind: "HTTPRoute", Namespaced: true}}), nil)
	var got []string
	for _, n := range e.routesTo("be", "app-server") {
		got = append(got, n.Name)
	}
	if strings.Join(got, ",") != "own,corp/corp-route" {
		t.Errorf("got %v", got)
	}
}

// errStore is a store whose lists fail with err and haven't loaded.
type errStore struct {
	fakeStore
	err error
}

func (errStore) Synced(schema.GroupVersionResource) bool { return false }
func (s errStore) Err(schema.GroupVersionResource) error { return s.err }

// Review of round 80: X never tells you to retry a list kubed can't read.
func TestMissingNoteWhenNotAllowed(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "serviceaccounts"}, "", errors.New("no"))
	e := New(errStore{err: forbidden}, testResources(), nil)
	if note, _ := e.missingNote(kube.ServiceAccounts); !strings.Contains(note, "can't read") {
		t.Errorf("not allowed: %q", note)
	}
	// A timeout passes: the list loads after all.
	e = New(errStore{err: errors.New("context deadline exceeded")}, testResources(), nil)
	if note, _ := e.missingNote(kube.ServiceAccounts); !strings.Contains(note, "not loaded yet") {
		t.Errorf("a timeout: %q", note)
	}
	e = New(fakeStore{}, testResources(), nil)
	if note, h := e.missingNote(kube.ServiceAccounts); note != "missing" || h != HealthBad {
		t.Errorf("loaded and absent: %q", note)
	}
}

// Round 82: a go-back scales the version it brings back up step by step;
// those steps, seconds after its template was written, aren't scales.
func TestGoBackStepsAreNotScales(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	back := now.Add(-3 * time.Minute)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "chg", "namespace": "qa", "uid": "dep-chg",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}}}}
	dep.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: back}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(dep)
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "chg-a", "namespace": "qa", "uid": "chg-a", "creationTimestamp": now.Add(-time.Hour).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "4", "deployment.kubernetes.io/revision-history": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "chg", "uid": "dep-chg", "controller": true}}}}}
	ev := func(name, msg string, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "chg"}, "lastTimestamp": at.Format(time.RFC3339)}}
	}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {
		ev("s1", "Scaled up replica set chg-a from 1 to 2", back.Add(time.Second)),
		ev("s2", "Scaled up replica set chg-a from 2 to 3", back.Add(2*time.Second)),
	}}, testResources(), nil)
	if got := e.handScales("qa"); len(got) != 0 {
		t.Errorf("a go-back's own steps shown as scales: %+v", got)
	}
}

// Review: the version running now scaled to 0 by hand, a minute after a
// rollout, still shows: a rollout never scales the running version down.
func TestScaleToZeroNearRolloutShows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "z", "namespace": "qa", "uid": "dep-z",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "z-b", "namespace": "qa", "uid": "z-b", "creationTimestamp": now.Add(-2 * time.Minute).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "z", "uid": "dep-z", "controller": true}}}}}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "d", "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": "Scaled down replica set z-b from 3 to 0",
		"involvedObject": map[string]any{"kind": "Deployment", "name": "z"}, "lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339)}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {ev}}, testResources(), nil)
	got := e.handScales("qa")
	if len(got) != 1 || !strings.Contains(got[0].Detail, "nothing runs") {
		t.Errorf("a hand scale to 0 near a rollout: %+v", got)
	}
}

// Round 83: image change, go-back, restart. Deployed stays at the go-back
// (seen by kubed while it ran), not at the version made before it.
func TestDeployedAfterGoBackThenRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	img, back, restart := now.Add(-4*time.Minute), now.Add(-3*time.Minute), now.Add(-2*time.Minute)
	dep := func(rev string, at time.Time, stamp string) *unstructured.Unstructured {
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "xapp", "namespace": "qa", "uid": "dep-xapp",
				"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:alpine"}}}}},
			// It moved as that write rolled out.
			"status": map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "True",
				"lastUpdateTime": at.Add(5 * time.Second).Format(time.RFC3339)}}}}}
		d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
		return d
	}
	rs := func(name, rev, history string, made time.Time, image, stamp string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "xapp", "uid": "dep-xapp", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}}}}
	}
	first := rs("xapp-a", "3", "1", now.Add(-time.Hour), "nginx:alpine", "")
	second := rs("xapp-b", "2", "", img, "nginx:1.27-alpine", "")
	d := dep("3", back, "")
	kube.NoteTemplateChange(d)
	e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {first, second}}, testResources(), nil)
	if got := e.DeployTimes("qa")["dep-xapp"].At; !got.Equal(back) {
		t.Fatalf("after the go-back: %v, want %v", got, back)
	}
	stamp := restart.Format(time.RFC3339)
	d = dep("4", restart, stamp)
	kube.NoteTemplateChange(d)
	e = New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {first, second, rs("xapp-c", "4", "", restart, "nginx:alpine", stamp)}}, testResources(), nil)
	if got := e.DeployTimes("qa")["dep-xapp"].At; !got.Equal(back) {
		t.Errorf("after the restart: %v, want the go-back %v", got, back)
	}
}

// Round 83: the same, but nobody looked between the go-back and the
// restart: the watch saw the template's settings written at the go-back.
func TestDeployedAfterGoBackUnseenThenRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	back, restart := now.Add(-30*time.Second), now.Add(-10*time.Second) // seen as they happened
	dep := func(at time.Time, image, stamp string) *unstructured.Unstructured {
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "yapp", "namespace": "qa", "uid": "dep-yapp",
				"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}}}}
		d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
		return d
	}
	rs := func(name, rev, history string, made time.Time, image, stamp string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "yapp", "uid": "dep-yapp", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}}}}
	}
	kube.NoteTemplateChange(dep(now.Add(-50*time.Second), "nginx:1.27-alpine", "")) // the image change
	kube.NoteTemplateChange(dep(back, "nginx:alpine", ""))                          // the go-back
	stamp := restart.Format(time.RFC3339)
	d := dep(restart, "nginx:alpine", stamp) // the restart
	kube.NoteTemplateChange(d)
	e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {
		rs("yapp-a", "3", "1", now.Add(-time.Hour), "nginx:alpine", ""),
		rs("yapp-b", "2", "", now.Add(-50*time.Second), "nginx:1.27-alpine", ""),
		rs("yapp-c", "4", "", restart, "nginx:alpine", stamp),
	}}, testResources(), nil)
	if got := e.DeployTimes("qa")["dep-yapp"].At; !got.Equal(back) {
		t.Errorf("deployed %v, want the go-back %v", got, back)
	}
}

// Review: kubed started after the go-back and the restart: the first
// look's time is the restart's, and a restart is not a deploy.
func TestDeployedStartedAfterRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	restart := now.Add(-2 * time.Minute)
	stamp := restart.Format(time.RFC3339)
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "zapp", "namespace": "qa", "uid": "dep-zapp",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}},
		"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:alpine"}}}}}}}
	d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: restart}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(d)
	rs := func(name, rev, history string, made time.Time, image, stamp string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "zapp", "uid": "dep-zapp", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"metadata": tmeta,
				"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}}}}
	}
	e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {
		rs("zapp-a", "3", "1", now.Add(-time.Hour), "nginx:alpine", ""),
		rs("zapp-b", "2", "", now.Add(-4*time.Minute), "nginx:1.27-alpine", ""),
		rs("zapp-c", "4", "", restart, "nginx:alpine", stamp),
	}}, testResources(), nil)
	dt := e.DeployTimes("qa")["dep-zapp"]
	if !dt.At.Before(restart) {
		t.Errorf("deployed at %v: the restart counted as the deploy", dt.At)
	}
	if !dt.After { // only "after the version made before it" is known: said as a bound
		t.Errorf("an unknown go-back time given as exact: %+v", dt)
	}
}

// Review: after a gap in watching (sleep, reconnect), a template seen late
// carries a later write's time: unknown, not a go-back time.
func TestSettingsSeenLateAreUnknown(t *testing.T) {
	now := time.Now().UTC()
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "late", "namespace": "qa", "uid": "dep-late"},
		"spec":     map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "b"}}}}}}}
	d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate,
		Time: &metav1.Time{Time: now.Add(-5 * time.Minute)}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
	kube.NoteTemplateChange(d)
	settings := kube.TemplateSettings(d.Object["spec"].(map[string]any)["template"].(map[string]any))
	if _, ok := kube.SettingsWrittenAtSure("dep-late", settings); ok {
		t.Error("a template seen five minutes after it was written was dated")
	}
}

// Round 86: a crash loop's BackOff event counts the kubelet's checks, not
// crashes: "(15 times)" next to "7 restarts" misled. Other counts stay.
func TestBackOffCountNotShown(t *testing.T) {
	e, _ := fixture(t, true)
	store := e.store.(fakeStore)
	now := time.Now().UTC()
	ev := func(name, typ, reason, msg string, count int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"name": name, "namespace": "payment"},
			"type": typ, "reason": reason, "message": msg, "count": count,
			"involvedObject": map[string]any{"kind": "Pod", "name": "api-abc-1"},
			"lastTimestamp":  now.Add(-time.Minute).Format(time.RFC3339)}}
	}
	store[kube.Events] = append(store[kube.Events],
		ev("b", "Warning", "BackOff", "Back-off restarting failed container c in pod api-abc-1", 15),
		ev("u", "Warning", "Unhealthy", "Readiness probe failed: connection refused", 4))
	e.store = store
	text := ""
	for _, c := range e.Changes("payment", "", now, nil) {
		text += c.Detail + "\n"
	}
	if strings.Contains(text, "15 times") || !strings.Contains(text, "keeps crashing") {
		t.Errorf("back-off line:\n%s", text)
	}
	if !strings.Contains(text, "(4 times)") {
		t.Errorf("other counts lost:\n%s", text)
	}
}

// Round 88: the events list said "none of the 19 nodes has enough space
// free" for a pod that taints and a node selector kept out.
func TestSchedulingEventWords(t *testing.T) {
	got := MessageWords("0/19 nodes are available: 12 node(s) had untolerated taint(s), 7 node(s) didn't match Pod's node affinity/selector. preemption: 0/19 nodes are available: 19 Preemption is not helpful for scheduling.")
	want := "no node fits: 12 nodes are reserved for other workloads (taints); 7 nodes don't match its node selector"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

// Round 92: restart, image change, go-back, restart, go-back, go-back,
// with nobody looking at the versions in between and Kubernetes merging
// the scaling events. Each go-back keeps the time kubed's watch saw its
// template written, in the timeline and in the versions list.
func TestEachGoBackKeepsItsOwnTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	made1, t2, t3, t4 := now.Add(-58*time.Second), now.Add(-54*time.Second), now.Add(-48*time.Second), now.Add(-42*time.Second)
	t5, t6, t7 := now.Add(-36*time.Second), now.Add(-28*time.Second), now.Add(-10*time.Second)
	x, y := t2.Format(time.RFC3339), t5.Format(time.RFC3339)
	template := func(image, stamp string) map[string]any {
		tmeta := map[string]any{}
		if stamp != "" {
			tmeta["annotations"] = map[string]any{"kubectl.kubernetes.io/restartedAt": stamp}
		}
		return map[string]any{"metadata": tmeta, "spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}
	}
	dep := func(rev string, at time.Time, image, stamp string) *unstructured.Unstructured {
		d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-web92",
				"annotations": map[string]any{"deployment.kubernetes.io/revision": rev}},
			"spec": map[string]any{"template": template(image, stamp)},
			// It moved as each write rolled out.
			"status": map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "True",
				"lastUpdateTime": at.Add(2 * time.Second).Format(time.RFC3339)}}}}}
		d.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
		return d
	}
	rs := func(name, rev, history string, made time.Time, image, stamp string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name + "-92", "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "dep-web92", "controller": true}}},
			"spec": map[string]any{"template": template(image, stamp)}}}
	}
	ev := func(name, msg string, count int64, at time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg, "count": count,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "web"}, "lastTimestamp": at.Format(time.RFC3339)}}
	}
	// What the watch saw, as it happened: each template the Deployment was given.
	kube.NoteTemplateChange(dep("1", made1, "nginx:1.27", ""))
	kube.NoteTemplateChange(dep("2", t2, "nginx:1.27", x))        // a restart
	kube.NoteTemplateChange(dep("3", t3, "nginx:1.27-alpine", x)) // a new image
	kube.NoteTemplateChange(dep("4", t4, "nginx:1.27", x))        // back to revision 2
	mid := dep("5", t5, "nginx:1.27", y)                          // a restart
	kube.NoteTemplateChange(mid)

	check := func(stage string, e *Enricher, d *unstructured.Unstructured, want map[string]time.Time) {
		t.Helper()
		for _, c := range e.Changes("qa", "", now, nil) {
			for label, at := range want {
				if strings.HasPrefix(c.Detail, label+": went back") && !c.When.Equal(at) {
					t.Errorf("%s: timeline %q at %v, want %v", stage, c.Detail, c.When.Format(time.TimeOnly), at.Format(time.TimeOnly))
				}
			}
		}
		for _, v := range e.RolloutOf(d).All {
			if at, ok := want[fmt.Sprintf("revision %d", v.Revision)]; ok && !v.LiveSince.Equal(at) {
				t.Errorf("%s: versions list has revision %d back at %v, want %v", stage, v.Revision, v.LiveSince.Format(time.TimeOnly), at.Format(time.TimeOnly))
			}
		}
	}
	// Right after the restart (revision 5): revision 4 came back at t4.
	e := New(fakeStore{kube.Deployments: {mid}, kube.ReplicaSets: {
		rs("web-a", "1", "", made1, "nginx:1.27", ""),
		rs("web-b", "4", "2", t2, "nginx:1.27", x),
		rs("web-c", "3", "", t3, "nginx:1.27-alpine", x),
		rs("web-d", "5", "", t5, "nginx:1.27", y),
	}, kube.Events: {
		ev("up-b", "Scaled up replica set web-b from 0 to 1", 2, t4),
		ev("up-c", "Scaled up replica set web-c from 0 to 1", 1, t3),
		ev("combined", "(combined from similar events): Scaled up replica set web-d from 0 to 1", 4, t5),
	}}, testResources(), nil)
	check("after revision 5", e, mid, map[string]time.Time{"revision 4": t4})

	kube.NoteTemplateChange(dep("6", t6, "nginx:1.27-alpine", x)) // back to revision 3
	last := dep("7", t7, "nginx:1.27", y)                         // back to revision 5
	kube.NoteTemplateChange(last)
	e = New(fakeStore{kube.Deployments: {last}, kube.ReplicaSets: {
		rs("web-a", "1", "", made1, "nginx:1.27", ""),
		rs("web-b", "4", "2", t2, "nginx:1.27", x),
		rs("web-c", "6", "3", t3, "nginx:1.27-alpine", x),
		rs("web-d", "7", "5", t5, "nginx:1.27", y),
	}, kube.Events: {
		ev("up-b", "Scaled up replica set web-b from 0 to 1", 2, t4),
		ev("up-c", "Scaled up replica set web-c from 0 to 1", 2, t6),
		ev("combined", "(combined from similar events): Scaled up replica set web-d from 0 to 1", 8, t7),
		ev("down-c", "Scaled down replica set web-c from 1 to 0", 1, t7), // the go-back to revision 5's
	}}, testResources(), nil)
	check("after revision 7", e, last, map[string]time.Time{"revision 4": t4, "revision 6": t6, "revision 7": t7})
}

// Round 92: "not enough free memory on 1 nodes".
func TestSchedulingWordsOneNode(t *testing.T) {
	if got := SchedulingWords("0/20 nodes are available: 1 Insufficient memory, 19 node(s) didn't match Pod's node affinity/selector."); !strings.Contains(got, "on 1 node;") {
		t.Errorf("got %q", got)
	}
}

// Round 93: Deployed for a version brought back said 14:43 (the
// Progressing time, moved by a later scale) where what changed said
// 14:42 (its scale-up). Both now read its scale-up.
func TestDeployedGoBackFromItsScaleUp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	back, scaled := now.Add(-8*time.Minute), now.Add(-7*time.Minute)
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "r93app", "namespace": "qa", "uid": "dep-r93app",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "4"}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:1.27"}}}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "True",
			"lastUpdateTime": scaled.Format(time.RFC3339)}}}}}
	rs := func(name, rev, history string, made time.Time, image string) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "r93app", "uid": "dep-r93app", "controller": true}}},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}}}}
	}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "s", "namespace": "qa"}, "reason": "ScalingReplicaSet",
		"message": "Scaled up replica set r93app-a from 0 to 2", "count": int64(1),
		"involvedObject": map[string]any{"kind": "Deployment", "name": "r93app"}, "lastTimestamp": back.Format(time.RFC3339)}}
	e := New(fakeStore{kube.Deployments: {d}, kube.Events: {ev}, kube.ReplicaSets: {
		rs("r93app-a", "4", "1", now.Add(-20*time.Minute), "nginx:1.27"), rs("r93app-b", "3", "", now.Add(-12*time.Minute), "nginx:1.27.1")}}, testResources(), nil)
	// Its scale-up alone can't date the go-back (a scale by hand after a
	// go-back made while nothing ran says the same), nor can the
	// Progressing time (any scale moves it): a bound, no earlier than the
	// version it came back over was made, and no later than the truth.
	made := now.Add(-12 * time.Minute)
	if got := e.DeployTimes("qa")["dep-r93app"]; !got.After || !got.At.Equal(made) {
		t.Errorf("Deployed %v (after %v), want %v or later", got.At, got.After, made)
	}
	// Only its first rollout's scale-up kept (the go-back's merged away):
	// not that, from before the version it came back over was made.
	ev.Object["lastTimestamp"] = now.Add(-19 * time.Minute).Format(time.RFC3339)
	if got := e.DeployTimes("qa")["dep-r93app"]; !got.After || !got.At.Equal(made) {
		t.Errorf("Deployed %v (after %v), want %v or later (not the Progressing time %v)", got.At, got.After, made, scaled)
	}
}

// Round 94: a stuck rollout's Deployed named its new version, which never
// served; it is when the version still running went out.
func TestDeployedStuckRolloutNamesTheServingVersion(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	oldMade, newMade := now.Add(-7*time.Minute), now.Add(-6*time.Minute)
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "r94app", "namespace": "qa", "uid": "dep-r94app",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "nginx:nope"}}}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded",
			"lastUpdateTime": now.Add(-5 * time.Minute).Format(time.RFC3339)}}}}}
	rs := func(name, rev string, made time.Time, image string, ready int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
				"annotations":     map[string]any{"deployment.kubernetes.io/revision": rev},
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "r94app", "uid": "dep-r94app", "controller": true}}},
			"spec":   map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}},
			"status": map[string]any{"readyReplicas": ready}}}
	}
	e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {rs("r94app-old", "1", oldMade, "nginx:1.25", 2), rs("r94app-new", "2", newMade, "nginx:nope", 0)}}, testResources(), nil)
	if got := e.DeployTimes("qa")["dep-r94app"].At; !got.Equal(oldMade) {
		t.Errorf("Deployed %v, want the serving version's %v", got, oldMade)
	}
	// Round 95: not stuck yet (its first minute): still the serving one.
	d.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Progressing", "status": "True"}}}
	if got := e.DeployTimes("qa")["dep-r94app"].At; !got.Equal(oldMade) {
		t.Errorf("rolling out, nothing new ready: %v, want %v", got, oldMade)
	}
	// Once the new version serves, it is what went out.
	e = New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {rs("r94app-old", "1", oldMade, "nginx:1.25", 1), rs("r94app-new", "2", newMade, "nginx:1.26", 1)}}, testResources(), nil)
	if got := e.DeployTimes("qa")["dep-r94app"].At; !got.Equal(newMade) {
		t.Errorf("new version serving: %v, want %v", got, newMade)
	}
}

// Round 97: a go-back drained the bad version (1 to 0); that version was
// brought back later and runs now, so w said "scaled from 1 to 0:
// nothing runs", a scale that never happened.
func TestGoBackDrainOfAVersionBroughtBackLater(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	drain, back1, back2 := now.Add(-3*time.Minute), now.Add(-3*time.Minute-time.Second), now.Add(-time.Minute)
	d := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "r97hist", "namespace": "qa", "uid": "dep-r97hist",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "6"}},
		"spec": map[string]any{"replicas": int64(1)}}}
	rs := func(name, rev, history string, made time.Time) *unstructured.Unstructured {
		ann := map[string]any{"deployment.kubernetes.io/revision": rev}
		if history != "" {
			ann["deployment.kubernetes.io/revision-history"] = history
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{"name": name, "namespace": "qa", "uid": name, "annotations": ann, "creationTimestamp": made.Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "r97hist", "uid": "dep-r97hist", "controller": true}}}}}
	}
	bad := rs("r97hist-bad", "6", "3", now.Add(-10*time.Minute))
	good := rs("r97hist-good", "4", "1,2", now.Add(-20*time.Minute))
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "dn", "namespace": "qa"}, "reason": "ScalingReplicaSet",
		"message": "Scaled down replica set r97hist-bad from 1 to 0", "count": int64(1),
		"involvedObject": map[string]any{"kind": "Deployment", "name": "r97hist"}, "lastTimestamp": drain.Format(time.RFC3339)}}
	noteWentBack(good.GetUID(), "4", back1) // the first go-back, seen live
	noteWentBack(bad.GetUID(), "6", back2)  // the second brought the bad one back
	e := New(fakeStore{kube.Deployments: {d}, kube.ReplicaSets: {bad, good}, kube.Events: {ev}}, testResources(), nil)
	for _, c := range e.handScales("qa") {
		if c.Object == "deployment/r97hist" {
			t.Errorf("a go-back's drain read as a scale: %+v", c)
		}
	}
}

// Round 97: a cron job with runs that can't pull their image said it
// looked fine (none had finished, so none had failed).
func TestCronRunsStuck(t *testing.T) {
	cj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "cj-bad", "namespace": "qa", "uid": "cj97"}}}
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "cj-bad-1", "namespace": "qa", "uid": "j97",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": "cj-bad", "uid": "cj97", "controller": true}}},
		"status": map[string]any{"active": int64(1)}}}
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "cj-bad-1-x", "namespace": "qa", "uid": "p97",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "cj-bad-1", "uid": "j97", "controller": true}}},
		"status": map[string]any{"containerStatuses": []any{map[string]any{"name": "c",
			"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff"}}}}}}}
	res := kube.NewResources([]kube.Resource{{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true},
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	e := New(fakeStore{kube.CronJobs: {cj}, kube.Jobs: {job}, kube.Pods: {pod}}, res, nil)
	if n, why := e.CronRunsStuck(cj); n != 1 || why != "can't pull its image" {
		t.Errorf("stuck %d %q", n, why)
	}
}

// Round 98: a stuck pod flips between ErrImagePull and ImagePullBackOff on
// each try; once it has tried a while, either counts as stuck.
func TestCronRunsStuckCountsErrImagePull(t *testing.T) {
	cj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob",
		"metadata": map[string]any{"name": "cj98", "namespace": "qa", "uid": "cj98"}}}
	job := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": "cj98-1", "namespace": "qa", "uid": "j98",
			"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "CronJob", "name": "cj98", "uid": "cj98", "controller": true}}},
		"status": map[string]any{"active": int64(1)}}}
	pod := func(age time.Duration) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "cj98-1-x", "namespace": "qa", "uid": "p98", "creationTimestamp": time.Now().Add(-age).Format(time.RFC3339),
				"ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "cj98-1", "uid": "j98", "controller": true}}},
			"status": map[string]any{"containerStatuses": []any{map[string]any{"name": "c",
				"state": map[string]any{"waiting": map[string]any{"reason": "ErrImagePull"}}}}}}}
	}
	res := kube.NewResources([]kube.Resource{{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true},
		{GVR: kube.Jobs, Kind: "Job", Namespaced: true}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}})
	if n, _ := New(fakeStore{kube.CronJobs: {cj}, kube.Jobs: {job}, kube.Pods: {pod(2 * time.Minute)}}, res, nil).CronRunsStuck(cj); n != 1 {
		t.Errorf("tried for 2m: %d stuck", n)
	}
	if n, _ := New(fakeStore{kube.CronJobs: {cj}, kube.Jobs: {job}, kube.Pods: {pod(5 * time.Second)}}, res, nil).CronRunsStuck(cj); n != 0 {
		t.Errorf("a first try's blip: %d stuck", n)
	}
}

// Round 111: scaled to 0 and back by hand, the scale back up merged into
// an older event: the version runs again, so not "nothing runs" as the
// last word.
func TestScaleToZeroThenBackMerged(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "z", "namespace": "qa", "uid": "dep-z",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "z-b", "namespace": "qa", "uid": "z-b", "creationTimestamp": now.Add(-10 * time.Minute).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "z", "uid": "dep-z", "controller": true}}},
		"spec": map[string]any{"replicas": int64(1)}, "status": map[string]any{"replicas": int64(1)}}}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "d", "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": "Scaled down replica set z-b from 1 to 0",
		"involvedObject": map[string]any{"kind": "Deployment", "name": "z"}, "lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339)}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {ev}}, testResources(), nil)
	got := e.handScales("qa")
	if len(got) != 1 || strings.Contains(got[0].Detail, "nothing runs") || !strings.Contains(got[0].Detail, "then back up") {
		t.Errorf("%+v", got)
	}
	// Just scaled to 0: asked for 0, its pods not yet gone: nothing runs.
	rs.Object["spec"] = map[string]any{"replicas": int64(0)}
	if got := e.handScales("qa"); len(got) != 1 || !strings.Contains(got[0].Detail, "nothing runs") {
		t.Errorf("going to 0: %+v", got)
	}
}

// Round 112: scaled to 0 and back within 2 minutes of the deployment going
// out: the scale back up isn't listed (merged, and near the rollout), so
// the scale down's line says it ran again.
func TestScaleToZeroThenBackNearRollout(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "sc", "namespace": "qa", "uid": "dep-sc", "creationTimestamp": now.Add(-90 * time.Second).Format(time.RFC3339),
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "sc-a", "namespace": "qa", "uid": "sc-a", "creationTimestamp": now.Add(-90 * time.Second).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "sc", "uid": "dep-sc", "controller": true}}},
		"spec": map[string]any{"replicas": int64(1)}, "status": map[string]any{"replicas": int64(1)}}}
	ev := func(name, msg string, first, last time.Time, count int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg, "count": count,
			"involvedObject": map[string]any{"kind": "Deployment", "name": "sc"},
			"firstTimestamp": first.Format(time.RFC3339), "lastTimestamp": last.Format(time.RFC3339)}}
	}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {
		ev("up", "Scaled up replica set sc-a from 0 to 1", now.Add(-90*time.Second), now.Add(-4*time.Second), 2),
		ev("down", "Scaled down replica set sc-a from 1 to 0", now.Add(-10*time.Second), now.Add(-10*time.Second), 1),
	}}, testResources(), nil)
	for _, c := range e.handScales("qa") {
		if strings.Contains(c.Detail, "nothing runs") {
			t.Errorf("%+v", c)
		}
	}
}

// Review of round 112: the version running now scaled to 0 and back, both
// events merged but its scale up listed: the scale down says nothing runs,
// the scale up line says when it came back.
func TestScaleToZeroThenBackBothListed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "sc", "namespace": "qa", "uid": "dep-sc", "creationTimestamp": now.Add(-3 * time.Hour).Format(time.RFC3339),
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "sc-a", "namespace": "qa", "uid": "sc-a", "creationTimestamp": now.Add(-3 * time.Hour).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "sc", "uid": "dep-sc", "controller": true}}},
		"spec": map[string]any{"replicas": int64(1)}, "status": map[string]any{"replicas": int64(1)}}}
	ev := func(name, msg string, last time.Time) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
			"metadata": map[string]any{"name": name, "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": msg, "count": int64(2),
			"involvedObject": map[string]any{"kind": "Deployment", "name": "sc"}, "lastTimestamp": last.Format(time.RFC3339)}}
	}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {
		ev("down", "Scaled down replica set sc-a from 1 to 0", now.Add(-10*time.Minute)),
		ev("up", "Scaled up replica set sc-a from 0 to 1", now.Add(-5*time.Minute)),
	}}, testResources(), nil)
	for _, c := range e.handScales("qa") {
		if strings.Contains(c.Detail, "then back up") {
			t.Errorf("the scale up is listed: %+v", c)
		}
	}
}

// Round 119: the events list names the image as the cluster pulls it (a
// mirror's path too), the same in every row, as kubectl does.
func TestPullEventsNameTheFullImage(t *testing.T) {
	img := "111122223333.dkr.ecr.ap-south-1.amazonaws.com/docker-hub/library/nginx:no-such-tag"
	for msg, want := range map[string]string{
		`Pulling image "` + img + `"`:                              "downloading image " + img,
		`Back-off pulling image "` + img + `"`:                     "can't download image " + img + "; trying again",
		`Successfully pulled image "` + img + `" in 1.2s`:          "pulled " + img,
		`Container image "` + img + `" already present on machine`: "image " + img + " already on the node",
	} {
		if got := MessageWords(msg); !strings.HasPrefix(got, want) {
			t.Errorf("%q: got %q, want %q", msg, got, want)
		}
	}
}

// Round 122: one failure time for a job everywhere: the moment it was
// marked failing, not when it was marked Failed after its pods stopped.
func TestJobFailedAt(t *testing.T) {
	target, failed := time.Now().Add(-time.Minute).Truncate(time.Second), time.Now().Add(-15*time.Second).Truncate(time.Second)
	cond := func(typ string, at time.Time) any {
		c := map[string]any{"type": typ, "status": "True"}
		if !at.IsZero() {
			c["lastTransitionTime"] = at.Format(time.RFC3339)
		}
		return c
	}
	job := func(conds ...any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"conditions": conds}}}
	}
	if got := JobFailedAt(job(cond("FailureTarget", target), cond("Failed", failed))); !got.Equal(target) {
		t.Errorf("both: %v", got)
	}
	if got := JobFailedAt(job(cond("Failed", failed))); !got.Equal(failed) {
		t.Errorf("older Kubernetes: %v", got)
	}
	if got := JobFailedAt(job(cond("FailureTarget", time.Time{}), cond("Failed", failed))); !got.Equal(failed) {
		t.Errorf("no time on FailureTarget: %v", got)
	}
}

// QA round 128: a pull failure keeps the registry host (often the cause:
// a typo, a registry that isn't reachable), except Docker Hub's.
func TestPullFailureKeepsHost(t *testing.T) {
	for msg, want := range map[string]string{
		`Back-off pulling image "registry.invalid/nope/never:1"`:               "can't download image registry.invalid/nope/never:1; trying again",
		`Back-off pulling image "docker.io/library/nope:1"`:                    "can't download image nope:1; trying again",
		`Back-off pulling image "ghcr.io/org/tool:v1@sha256:0123456789abcdef"`: "can't download image ghcr.io/org/tool:v1; trying again",
	} {
		if got := eventDetail("BackOff", msg); got != want {
			t.Errorf("%q: %q, want %q", msg, got, want)
		}
	}
}

// QA round 128: :changes says what a scale to 0 did to that deployment,
// not "nothing runs": its pods may still be shutting down, and other
// workloads of its app keep running.
func TestChangesScaleToZeroWords(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dep := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "z", "namespace": "qa", "uid": "dep-z",
			"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}}}}
	rs := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{"name": "z-b", "namespace": "qa", "uid": "z-b", "creationTimestamp": now.Add(-10 * time.Minute).Format(time.RFC3339),
			"annotations":     map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "z", "uid": "dep-z", "controller": true}}},
		"spec": map[string]any{"replicas": int64(0)}, "status": map[string]any{"replicas": int64(2)}}}
	ev := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "d", "namespace": "qa"}, "reason": "ScalingReplicaSet", "message": "Scaled down replica set z-b from 2 to 0",
		"involvedObject": map[string]any{"kind": "Deployment", "name": "z"}, "lastTimestamp": now.Add(-10 * time.Second).Format(time.RFC3339)}}
	e := New(fakeStore{kube.Deployments: {dep}, kube.ReplicaSets: {rs}, kube.Events: {ev}}, testResources(), nil)
	found := false
	for _, c := range e.Changes("qa", "", now, nil) {
		if c.What != "scale" {
			continue
		}
		found = true
		if strings.Contains(c.Detail, "nothing runs") || !strings.Contains(c.Detail, "scaled from 2 to 0") || !strings.Contains(c.Detail, "its pods stop") {
			t.Errorf("scale to 0: %q", c.Detail)
		}
	}
	if !found {
		t.Errorf("no scale line")
	}
}
