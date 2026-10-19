package view

import (
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// r140Env is a store with pod p and events evs about it.
func r140Env(p *corev1.Pod, evs ...*unstructured.Unstructured) (*enrich.Enricher, []*unstructured.Unstructured) {
	pods := []*unstructured.Unstructured{mxObj(p)}
	s := listStore{kube.Pods: pods, kube.Events: evs}
	res := []kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true}, {GVR: kube.Events, Kind: "Event", Namespaced: true}}
	return enrich.New(mxStore{s}, kube.NewResources(res), nil), pods
}

// r140Mounting is a pod made ago, its one container waiting to be
// created, with volumes c (config map no-such-cm), s (secret has-a, key
// nope) and p (projected secret proj-missing-secret).
func r140Mounting(ago time.Duration) *corev1.Pod {
	p := mxPod("vol", "", corev1.PodPending, mxC{name: "app", state: mxWait("ContainerCreating", "")})
	p.CreationTimestamp = mxAgo(ago)
	start := mxAgo(ago)
	p.Status.StartTime = &start
	p.Spec.Volumes = []corev1.Volume{
		{Name: "c", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "no-such-cm"}}}},
		{Name: "s", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "has-a", Items: []corev1.KeyToPath{{Key: "nope", Path: "n"}}}}},
		{Name: "p", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
			{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "proj-missing-secret"}}}}}}},
	}
	return p
}

// QA round 140: a pod stuck creating its containers because a volume
// can't be mounted read "Starting: pulling images and starting
// containers" for minutes. It says which volume and why, as a problem,
// with the Fix the bad config case gives.
func TestPodSaysVolumeCantMount(t *testing.T) {
	for _, c := range []struct{ msg, why, fix string }{
		{`MountVolume.SetUp failed for volume "c" : configmap "no-such-cm" not found`, "volume c: config map no-such-cm isn't there",
			"create the missing secret or config map, or fix the name it refers to"},
		{`MountVolume.SetUp failed for volume "s" : references non-existent secret key: nope`, "volume s: secret has-a has no key nope",
			"add key nope to secret has-a, or fix the key name it refers to"},
		{`MountVolume.SetUp failed for volume "p" : secret "proj-missing-secret" not found`, "volume p: secret proj-missing-secret isn't there",
			"create the missing secret or config map, or fix the name it refers to"},
		{`MountVolume.SetUp failed for volume "x" : some driver said no`, `MountVolume.SetUp failed for volume "x" : some driver said no`, ""},
	} {
		p := r140Mounting(5 * time.Minute)
		e, pods := r140Env(p, mxEvent("vol.1", "vol", "", "FailedMount", c.msg, time.Minute))
		st := Explain(e, p)
		if st != (Status{"Can't mount", c.why, ToneBad}) {
			t.Errorf("%s: status %+v", c.msg, st)
		}
		box := strings.Join(podDetail(e, pods[0]), "\n")
		if !strings.Contains(box, "Problem  Can't mount: "+c.why) {
			t.Errorf("box %s", box)
		}
		if c.fix != "" && !strings.Contains(box, "Fix      "+c.fix) || c.fix == "" && strings.Contains(box, "Fix") {
			t.Errorf("want Fix %q: box %s", c.fix, box)
		}
		if row, tone := podRow(e, pods[0]); tone != ToneBad || !strings.Contains(strings.Join(row, " "), "Can't mount") {
			t.Errorf("row %v %v", row, tone)
		}
		if a := r136App(t, e, pods); a.Status.Short != "Can't mount" || a.Status.Tone != ToneBad {
			t.Errorf("home %+v", a.Status)
		} else if c.fix != "" && appFix(a, false) != c.fix {
			t.Errorf("home Fix %q", appFix(a, false))
		}
	}
	// A normal brief delay: a pod 20s old isn't flagged.
	msg := `MountVolume.SetUp failed for volume "c" : configmap "no-such-cm" not found`
	p := r140Mounting(20 * time.Second)
	e, _ := r140Env(p, mxEvent("vol.1", "vol", "", "FailedMount", msg, 10*time.Second))
	if st := Explain(e, p); st.Short != "Starting" {
		t.Errorf("young: %+v", st)
	}
	// Mounted since: an image pull began after the failure.
	p = r140Mounting(5 * time.Minute)
	pull := mxEvent("vol.2", "vol", "app", "Pulling", `Pulling image "img/app:1"`, 30*time.Second)
	pull.Object["type"] = "Normal"
	e, _ = r140Env(p, mxEvent("vol.1", "vol", "", "FailedMount", msg, time.Minute), pull)
	if st := Explain(e, p); st.Short != "Starting" {
		t.Errorf("mounted since: %+v", st)
	}
	// An earlier pod of the same name's warning: not this pod's.
	e, _ = r140Env(p, mxEvent("vol.1", "vol", "", "FailedMount", msg, 10*time.Minute))
	if st := Explain(e, p); st.Short != "Starting" {
		t.Errorf("earlier pod's: %+v", st)
	}
}

// QA round 140: kubelet waiting reasons other than the pull and config
// ones read "Starting". Each says what its message says.
func TestPodWaitingReasonsArentStarting(t *testing.T) {
	for _, c := range []struct{ reason, msg, short, why, fix string }{
		{"ErrImageNeverPull", `Container image "img/app:1" is not present with pull policy of Never`, "Can't start",
			`container "app": image img/app:1 isn't on its node, and its pull policy is Never (it never downloads)`,
			"set imagePullPolicy to IfNotPresent or put the image on the node"},
		{"RegistryUnavailable", "registry is down", "Can't pull image", "app can't download img/app:1: registry is down", ""},
		{"ImageInspectError", "Failed to inspect image: rpc error", "Can't start",
			`container "app": Kubernetes can't read image img/app:1 on its node: Failed to inspect image: rpc error`, "E shows its events"},
		{"PreStartHookError", "cpu manager said no", "Can't start",
			`container "app": Kubernetes couldn't prepare it on its node: cpu manager said no`, "E shows its events"},
		{"PreCreateHookError", "device plugin said no", "Can't start",
			`container "app": Kubernetes couldn't prepare it on its node: device plugin said no`, "E shows its events"},
	} {
		p := mxPod("w", "", corev1.PodPending, mxC{name: "app", state: mxWait(c.reason, c.msg)})
		e, pods := r140Env(p)
		st := Explain(e, p)
		if st.Short != c.short || st.Why != c.why || st.Tone != ToneBad {
			t.Errorf("%s: %+v", c.reason, st)
		}
		if c.fix != "" {
			if box := strings.Join(podDetail(e, pods[0]), "\n"); !strings.Contains(box, "Fix      "+c.fix) {
				t.Errorf("%s: box %s", c.reason, box)
			}
			if a := r136App(t, e, pods); appFix(a, false) != c.fix {
				t.Errorf("%s: home Fix %q", c.reason, appFix(a, false))
			}
		}
	}
}

// QA round 140: a container killed because its postStart hook failed
// read "keeps crashing; last exit code 137" with a Fix for logs it never
// wrote. It says the hook failed, in its event's words, and to fix it.
func TestPodSaysPostStartHookFailed(t *testing.T) {
	hook := `Exec lifecycle hook ([sh -c exit 3]) for Container "app" in Pod "h_mxa(uid-h)" failed - error: command 'sh -c exit 3' exited with 3: , message: ""`
	for _, c := range []struct {
		state  corev1.ContainerState
		events []*unstructured.Unstructured
		why    string
	}{
		{mxWait("CrashLoopBackOff", "back-off 40s restarting failed container=app"), []*unstructured.Unstructured{
			mxEvent("h.1", "h", "app", "FailedPostStartHook", hook, 30*time.Second)},
			`container "app" is killed because its postStart hook failed (command 'sh -c exit 3' exited with 3); 3 restarts, the last 1m ago`},
		{mxWait("PostStartHookError", hook), nil,
			`container "app" is killed because its postStart hook failed (command 'sh -c exit 3' exited with 3); 3 restarts, the last 1m ago`},
		{mxWait("CrashLoopBackOff", "back-off"), []*unstructured.Unstructured{
			mxEvent("h.2", "h", "app", "Killing", "FailedPostStartHook", 30*time.Second)},
			`container "app" is killed because its postStart hook failed; 3 restarts, the last 1m ago`},
	} {
		p := mxPod("h", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: c.state, restarts: 3, last: mxEnd("Error", 137, time.Minute)})
		e, pods := r140Env(p, c.events...)
		if st := Explain(e, p); st != (Status{"Crashing", c.why, ToneBad}) {
			t.Errorf("status %+v", st)
		}
		box := strings.Join(podDetail(e, pods[0]), "\n")
		if !strings.Contains(box, "Fix      fix its postStart hook (on its deployment)") || strings.Contains(box, "p shows") {
			t.Errorf("box %s", box)
		}
	}
	// Running again between kills: the same.
	p := mxPod("h", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(5 * time.Second), restarts: 3, last: mxEnd("Error", 137, 10*time.Second)})
	e, _ := r140Env(p, mxEvent("h.1", "h", "app", "FailedPostStartHook", hook, 10*time.Second))
	if st := Explain(e, p); !strings.Contains(st.Why, "killed because its postStart hook failed") || st.Tone != ToneBad {
		t.Errorf("running again: %+v", st)
	}
}

// QA round 140: a pull that fails because a pull secret is missing said
// only "access denied by the registry", and a 401 came out raw.
func TestPullSaysItsPullSecretIsMissing(t *testing.T) {
	denied := `failed to pull and unpack image "img/app:1": failed to resolve reference "img/app:1": pull access denied, repository does not exist or may require authorization`
	p := mxPod("ps", "", corev1.PodPending, mxC{name: "app", state: mxWait("ErrImagePull", denied)})
	e, pods := r140Env(p, mxEvent("ps.1", "ps", "", "FailedToRetrieveImagePullSecret",
		"Unable to retrieve some image pull secrets (no-such-pull-secret); attempting to pull the image may not succeed.", 30*time.Second))
	st := Explain(e, p)
	if st.Why != "app can't download img/app:1: access denied by the registry; its pull secret no-such-pull-secret isn't there" {
		t.Errorf("status %+v", st)
	}
	if box := strings.Join(podDetail(e, pods[0]), "\n"); !strings.Contains(box, "Fix      create the pull secret, or fix imagePullSecrets") {
		t.Errorf("box %s", box)
	}
	if a := r136App(t, e, pods); appFix(a, false) != "create the pull secret, or fix imagePullSecrets" {
		t.Errorf("home Fix %q", appFix(a, false))
	}
	// 401 from a registry, as containerd writes it over several lines.
	unauth := "failed to pull and unpack image \"img/app@sha256:abc\": failed to resolve reference\n \"img/app@sha256:abc\": unexpected status from HEAD request to\n https://reg.example/v2/app/manifests/sha256:abc:\n 401 Unauthorized\n"
	if got := imagePullCause(unauth); got != "access denied by the registry" {
		t.Errorf("401: %q", got)
	}
}

// fullNode is a usage reading of a node using 95% of its memory.
type fullNode struct{}

func (fullNode) Available() bool                       { return true }
func (fullNode) Pod(string, string) (kube.Usage, bool) { return kube.Usage{}, false }
func (fullNode) Node(string) (kube.Usage, bool) {
	return kube.Usage{CPUMilli: 100, MemBytes: 95 << 20}, true
}
func (fullNode) Peak(string, string) (kube.Usage, time.Time, bool) {
	return kube.Usage{}, time.Time{}, false
}

// QA round 140: a node whose kubelet reports MemoryPressure, DiskPressure
// and PIDPressure all False said "Pressure not reported yet". Reported
// and False is none; only absent conditions are "not reported".
func TestNodePressureReportedFalseIsNone(t *testing.T) {
	node := func(conds ...string) *unstructured.Unstructured {
		var cs []any
		for _, c := range conds {
			cs = append(cs, map[string]any{"type": c, "status": "False", "reason": "KubeletHasSufficient" + strings.TrimSuffix(c, "Pressure"),
				"message": "kubelet has sufficient", "lastHeartbeatTime": "2026-10-16T10:00:00Z", "lastTransitionTime": "2026-10-01T10:00:00Z"})
		}
		cs = append(cs, map[string]any{"type": "Ready", "status": "True"})
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "n1"},
			"status": map[string]any{"allocatable": map[string]any{"cpu": "2", "memory": "100Mi", "pods": "29"}, "conditions": cs}}}
	}
	e := enrich.New(mxStore{listStore{}}, kube.NewResources(nil), nil)
	e.SetUsage(fullNode{})
	box := strings.Join(nodeDetail(e, node("MemoryPressure", "DiskPressure", "PIDPressure")), "\n")
	if !strings.Contains(box, "Pressure none") || strings.Contains(box, "not reported") {
		t.Errorf("reported False: %s", box)
	}
	if box := strings.Join(nodeDetail(e, node()), "\n"); !strings.Contains(box, "Pressure not reported yet") {
		t.Errorf("absent: %s", box)
	}
}

// QA round 140: a bare pod (no workload asks for it) dropped out of READY
// and "Pods N of M ready": 6/6 though kubectl lists 7 pods. It counts.
func TestReadyCountsPodsNoWorkloadAsksFor(t *testing.T) {
	e, pods, w := r138Workload("Deployment", 6, 6, false)
	bare := mxPod("lone", "", corev1.PodPending, mxC{name: "app", state: mxWait("CreateContainerConfigError", `secret "nope" not found`)})
	pods = append(pods, mxObj(bare))
	s := listStore{kube.Pods: pods, kube.Deployments: e.List(kube.Deployments, ""), kube.ReplicaSets: e.List(kube.ReplicaSets, "")}
	e = enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true}, {GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true}}), nil)
	a := r136App(t, e, pods, w)
	if got := readyCell(a); got != "6/7" {
		t.Errorf("READY %s, want 6/7 as kubectl lists 7 pods (status %+v)", got, a.Status)
	}
	if lines := strings.Join(AppLines(a, Lookups{}), "\n"); !strings.Contains(lines, "6 of 7 ready") {
		t.Errorf("box:\n%s", lines)
	}
}

// QA round 140: after u on a stateful set whose pod is stuck on the old
// bad version, the pod's own box said to fix the image, already good. It
// says, as the app box does, to delete the pod so it is made again.
func TestStuckStatefulSetPodBoxSaysDeleteIt(t *testing.T) {
	const want = "Fix      its stateful set now runs another version, but Kubernetes won't replace this pod by itself: delete it (ctrl-d) so it is made again with the current version"
	e, pods, _ := r138StuckSet(true)
	if box := strings.Join(podDetail(e, pods[0]), "\n"); !strings.Contains(box, want) || strings.Contains(box, "fix the image") {
		t.Errorf("box:\n%s", box)
	}
	e, pods, _ = r138StuckSet(false)
	if box := strings.Join(podDetail(e, pods[0]), "\n"); strings.Contains(box, "ctrl-d") {
		t.Errorf("of the version it updates to:\n%s", box)
	}
}

// notSynced is a store where the listed kinds haven't loaded (or can't be read).
type notSynced struct {
	mxStore
	gvrs []schema.GroupVersionResource
}

func (s notSynced) Synced(g schema.GroupVersionResource) bool { return !slices.Contains(s.gvrs, g) }

// QA round 140: an ingress whose TLS secret isn't there, with no class
// and no default class, showed no problem. It says both, as an HTTPRoute
// says its problems, and only what kubed can know.
func TestIngressSaysMissingTLSSecretAndNoClass(t *testing.T) {
	ing := r132Obj("networking.k8s.io/v1", "Ingress", "web", map[string]any{"spec": map[string]any{
		"tls": []any{map[string]any{"hosts": []any{"a.example.com"}, "secretName": "no-such-tls"}},
		"rules": []any{map[string]any{"host": "a.example.com", "http": map[string]any{"paths": []any{
			map[string]any{"path": "/", "backend": map[string]any{"service": map[string]any{"name": "web", "port": map[string]any{"number": int64(80)}}}}}}}}}})
	web := r132Obj("v1", "Service", "web", nil)
	class := func(def string) *unstructured.Unstructured {
		c := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "IngressClass", "metadata": map[string]any{"name": "alb"}}}
		if def != "" {
			c.SetAnnotations(map[string]string{"ingressclass.kubernetes.io/is-default-class": def})
		}
		return c
	}
	v := For(kube.Resource{GVR: kube.Ingresses, Namespaced: true})
	e := enrich.New(mxStore{listStore{kube.Services: {web}, kube.IngressClasses: {class("false")}}}, nil, nil)
	_, tone := v.Row(e, ing)
	lines := v.Detail(e, ing)
	for _, want := range []string{"Problem  TLS secret no-such-tls isn't there", "Problem  it has no class and none is the default, so a controller takes it only if it is set to watch ingresses without one"} {
		if !detailHas(lines, want) {
			t.Errorf("lacks %q: %q", want, lines)
		}
	}
	if tone != ToneWarn {
		t.Errorf("tone %v", tone)
	}
	// A controller took it all the same (it has an address): no claim.
	took := ing.DeepCopy()
	took.Object["status"] = map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"hostname": "lb.example.com"}}}}
	if detailHas(v.Detail(e, took), "has no class") {
		t.Errorf("has an address: %q", v.Detail(e, took))
	}
	// A default class takes it; the secret there: no problem.
	tls := r132Obj("v1", "Secret", "no-such-tls", nil)
	e = enrich.New(mxStore{listStore{kube.Services: {web}, kube.Secrets: {tls}, kube.IngressClasses: {class("true")}}}, nil, nil)
	if _, tone := v.Row(e, ing); tone != ToneNormal || detailHas(v.Detail(e, ing), "Problem") {
		t.Errorf("all fine: %v %q", tone, v.Detail(e, ing))
	}
	// Secrets and classes not loaded (or not readable): it can't know.
	e = enrich.New(notSynced{mxStore{listStore{kube.Services: {web}}}, []schema.GroupVersionResource{kube.Secrets, kube.IngressClasses}}, nil, nil)
	if _, tone := v.Row(e, ing); tone != ToneNormal || detailHas(v.Detail(e, ing), "Problem") {
		t.Errorf("can't know: %v %q", tone, v.Detail(e, ing))
	}
}

// QA round 140: :pdb showed NAME READY AGE, READY "-", and no box. It
// shows what kubectl does, in words, and flags one that blocks every
// drain: none may go down though all its pods are healthy.
func TestDisruptionBudgetList(t *testing.T) {
	pdb := func(minAvail any, allowed, healthy, desired, expected int64) *unstructured.Unstructured {
		spec := map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "db"}}}
		if minAvail != nil {
			spec["minAvailable"] = minAvail
		} else {
			spec["maxUnavailable"] = "25%"
		}
		return r132Obj("policy/v1", "PodDisruptionBudget", "db-pdb", map[string]any{"spec": spec,
			"status": map[string]any{"disruptionsAllowed": allowed, "currentHealthy": healthy, "desiredHealthy": desired, "expectedPods": expected}})
	}
	v := For(kube.Resource{GVR: kube.PDBs, Namespaced: true})
	e := enrich.New(mxStore{listStore{}}, nil, nil)
	if strings.Join(v.Columns, " ") != "NAME MIN AVAILABLE MAX UNAVAILABLE ALLOWED DOWN AGE" {
		t.Errorf("columns %v", v.Columns)
	}
	row, tone := v.Row(e, pdb(int64(2), 0, 2, 2, 2))
	if row[1] != "2" || row[2] != "-" || row[3] != "0" || tone != ToneWarn {
		t.Errorf("blocking: %v %v", row, tone)
	}
	box := strings.Join(v.Detail(e, pdb(int64(2), 0, 2, 2, 2)), "\n")
	for _, want := range []string{"Problem  no pod of it may be taken down now (drains wait on it)", "Covers   2 pods (app=db)", "Healthy  2 of 2 (needs 2)"} {
		if !strings.Contains(box, want) {
			t.Errorf("box lacks %q:\n%s", want, box)
		}
	}
	// None allowed because a pod is down: just the numbers.
	row, tone = v.Row(e, pdb(int64(2), 0, 1, 2, 2))
	if tone != ToneNormal || strings.Contains(strings.Join(v.Detail(e, pdb(int64(2), 0, 1, 2, 2)), "\n"), "Problem") {
		t.Errorf("a pod down: %v %v", row, tone)
	}
	row, tone = v.Row(e, pdb(nil, 1, 4, 3, 4))
	if row[1] != "-" || row[2] != "25%" || row[3] != "1" || tone != ToneNormal {
		t.Errorf("allows one: %v %v", row, tone)
	}
}

// Review of round 140: a disk still attaching takes minutes (Multi-Attach
// waits for Kubernetes to force the old node to let go, about 6
// minutes): before then it is starting, waiting for its disk, not a
// problem. A secret or config map mount keeps its 30s.
func TestPodWaitingForItsDiskIsStarting(t *testing.T) {
	for _, c := range []struct {
		reason, msg string
		young, old  time.Duration
	}{
		{"FailedAttachVolume", `AttachVolume.Attach failed for volume "pvc-1" : rpc error: code = Internal`, 2 * time.Minute, 4 * time.Minute},
		{"FailedMount", `Unable to attach or mount volumes: unmounted volumes=[data], unattached volumes=[data]: timed out waiting for the condition`, 2 * time.Minute, 4 * time.Minute},
		{"FailedAttachVolume", `Multi-Attach error for volume "pvc-1" Volume is already exclusively attached to one node and can't be attached to another`, 5 * time.Minute, 8 * time.Minute},
	} {
		p := r140Mounting(c.young)
		e, _ := r140Env(p, mxEvent("vol.1", "vol", "", c.reason, c.msg, 10*time.Second))
		if st := Explain(e, p); st != (Status{"Starting", "waiting for its disk to attach", ToneNormal}) {
			t.Errorf("%s young: %+v", c.msg, st)
		}
		p = r140Mounting(c.old)
		e, _ = r140Env(p, mxEvent("vol.1", "vol", "", c.reason, c.msg, 10*time.Second))
		if st := Explain(e, p); st.Short != "Can't mount" || st.Tone != ToneBad {
			t.Errorf("%s old: %+v", c.msg, st)
		}
	}
}

// Review of round 140: a sandbox that can't be made after an early
// brief mount failure is past the mount: not "Can't mount".
func TestSandboxEventIsPastTheMount(t *testing.T) {
	msg := `MountVolume.SetUp failed for volume "c" : configmap "no-such-cm" not found`
	for _, reason := range []string{"FailedCreatePodSandBox", "SandboxChanged"} {
		p := r140Mounting(5 * time.Minute)
		e, _ := r140Env(p, mxEvent("vol.1", "vol", "", "FailedMount", msg, 4*time.Minute), mxEvent("vol.2", "vol", "", reason, "sandbox trouble", time.Minute))
		if st := Explain(e, p); st.Short == "Can't mount" {
			t.Errorf("%s: %+v", reason, st)
		}
	}
}

// Review of round 140: a signature the node refuses isn't a download
// failure.
func TestSignatureRefusedIsNotADownload(t *testing.T) {
	p := mxPod("w", "", corev1.PodPending, mxC{name: "app", state: mxWait("SignatureValidationFailed", "bad signature")})
	e, _ := r140Env(p)
	if st := Explain(e, p); st != (Status{"Can't start", `container "app": image img/app:1 was refused: its signature doesn't pass the node's policy (bad signature)`, ToneBad}) {
		t.Errorf("%+v", st)
	}
}

// Review of round 140: a stateful set made by another object (a
// Prometheus, say) has its pods filed under that object at the top: they
// are the stateful set's copies all the same, not extra pods (2/2, not
// 2/4).
func TestReadyCountsAnOperatorsStatefulSetOnce(t *testing.T) {
	_, pods, w := r138Workload("StatefulSet", 2, 2, false)
	w.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "monitoring.coreos.com/v1", Kind: "Prometheus", Name: "prom", UID: "uid-prom", Controller: ptrTo(true)}})
	cr := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "monitoring.coreos.com/v1", "kind": "Prometheus",
		"metadata": map[string]any{"name": "prom", "namespace": "mxa", "uid": "uid-prom"}}}
	// A third copy on its way out (being replaced): READY counts out of
	// what it asks for.
	gone := pods[0].DeepCopy()
	gone.SetName("web-z")
	gone.SetUID("uid-web-z")
	now := metav1.Now()
	gone.SetDeletionTimestamp(&now)
	pods = append(pods, gone)
	prom := schema.GroupVersionResource{Group: "monitoring.coreos.com", Version: "v1", Resource: "prometheuses"}
	s := listStore{kube.Pods: pods, kube.StatefulSets: {w}, prom: {cr}}
	e := enrich.New(mxStore{s}, kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true},
		{GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true}, {GVR: prom, Kind: "Prometheus", Namespaced: true}}), nil)
	if top := e.TopOwner(pods[0]); top.GetKind() != "Prometheus" {
		t.Fatalf("fixture: top owner %s", top.GetKind())
	}
	if a := r136App(t, e, pods, w); readyCell(a) != "2/2" || a.Stopping != 1 {
		t.Errorf("READY %s (stopping %d), want 2/2", readyCell(a), a.Stopping)
	}
}

func ptrTo[T any](v T) *T { return &v }

// Review of round 140: a budget that allows none down by its own setting
// (maxUnavailable 0, minAvailable 100%) isn't helped by more copies: its
// Fix doesn't say so. A minAvailable count equal to its pods is: one
// more copy allows one down.
func TestDisruptionBudgetFixWhenItAllowsNoneBySetting(t *testing.T) {
	v := For(kube.Resource{GVR: kube.PDBs, Namespaced: true})
	e := enrich.New(mxStore{listStore{}}, nil, nil)
	status := map[string]any{"disruptionsAllowed": int64(0), "currentHealthy": int64(2), "desiredHealthy": int64(2), "expectedPods": int64(2)}
	for _, spec := range []map[string]any{{"maxUnavailable": int64(0)}, {"maxUnavailable": "0%"}, {"minAvailable": "100%"}} {
		o := r132Obj("policy/v1", "PodDisruptionBudget", "db-pdb", map[string]any{"spec": spec, "status": status})
		box := strings.Join(v.Detail(e, o), "\n")
		if !strings.Contains(box, "Fix      ") || strings.Contains(box, "more copies") {
			t.Errorf("%v: %s", spec, box)
		}
	}
	o := r132Obj("policy/v1", "PodDisruptionBudget", "db-pdb", map[string]any{"spec": map[string]any{"minAvailable": int64(1)},
		"status": map[string]any{"disruptionsAllowed": int64(0), "currentHealthy": int64(1), "desiredHealthy": int64(1), "expectedPods": int64(1)}})
	if box := strings.Join(v.Detail(e, o), "\n"); !strings.Contains(box, "or run more copies") {
		t.Errorf("minAvailable equal to its pods: %s", box)
	}
}
