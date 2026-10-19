package kube

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func pod(name, owner string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Spec: corev1.PodSpec{NodeName: "n1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if owner != "" {
		p.OwnerReferences = []metav1.OwnerReference{{Kind: owner, Name: "x"}}
	}
	return p
}

func TestEvictable(t *testing.T) {
	mirror := pod("static", "")
	mirror.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "x"}
	done := pod("done", "")
	done.Status.Phase = corev1.PodSucceeded
	got := Evictable([]corev1.Pod{*pod("app", "ReplicaSet"), *pod("ds", "DaemonSet"), *mirror, *done})
	if len(got) != 1 || got[0].Name != "app" {
		t.Fatalf("Evictable = %v", got)
	}
}

func TestDrainCordonsAndEvictsRespectingPDB(t *testing.T) {
	DrainRetry = 10 * time.Millisecond
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	cs := fake.NewSimpleClientset(node, pod("a", "ReplicaSet"), pod("b", "ReplicaSet"), pod("ds", "DaemonSet"))
	refusals := 1
	var evicted []string
	cs.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		name := a.(k8stesting.CreateAction).GetObject().(metav1.Object).GetName()
		if name == "b" && refusals > 0 { // PDB refuses once
			refusals--
			return true, nil, apierrors.NewTooManyRequests("pdb", 1)
		}
		evicted = append(evicted, name)
		return true, nil, nil
	})
	c := &Client{Typed: cs}
	var last DrainProgress
	sawWaiting := false
	err := c.Drain(context.Background(), "n1", func(p DrainProgress) {
		last = p
		if p.Waiting != "" {
			sawWaiting = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if last.Evicted != 2 || last.Total != 2 || len(evicted) != 2 || !sawWaiting {
		t.Fatalf("progress %+v evicted %v waiting %v", last, evicted, sawWaiting)
	}
	n, _ := cs.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
	if !n.Spec.Unschedulable {
		t.Error("node not cordoned")
	}
}

func TestTriggerCronJob(t *testing.T) {
	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: "ns", UID: "u1"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}}}}}}}
	cs := fake.NewSimpleClientset(cj)
	c := &Client{Typed: cs}
	name, err := c.TriggerCronJob(context.Background(), "ns", "report")
	if err != nil {
		t.Fatal(err)
	}
	job, err := cs.BatchV1().Jobs("ns").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.OwnerReferences[0].Name != "report" || job.Annotations["cronjob.kubernetes.io/instantiate"] != "manual" ||
		job.Spec.Template.Spec.Containers[0].Image != "img" {
		t.Fatalf("job = %+v", job)
	}
}

func TestDecodeHelmRelease(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(`{"info":{"status":"deployed"},"chart":{"metadata":{"name":"redis","version":"1.2.3","appVersion":"7"}},"config":{"replicas":2},"manifest":"kind: Service"}`))
	_ = zw.Close()
	d, err := decodeHelmRelease([]byte(base64.StdEncoding.EncodeToString(gz.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if d.Chart != "redis" || d.ChartVersion != "1.2.3" || d.AppVersion != "7" || d.Status != "deployed" ||
		d.Values["replicas"] != float64(2) || d.Manifest != "kind: Service" {
		t.Fatalf("decoded %+v", d)
	}
}

func TestRolloutUndoGoesToPreviousRevision(t *testing.T) {
	ctrl := true
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns", UID: "d1",
		Annotations: map[string]string{"deployment.kubernetes.io/revision": "3"}}}
	rs := func(name, rev, image string) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns",
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": rev},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: "d1", Controller: &ctrl}}},
			Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web", "pod-template-hash": "x"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: image}}}}}}
	}
	cs := fake.NewSimpleClientset(dep, rs("web-1", "1", "app:v1"), rs("web-2", "2", "app:v2"), rs("web-3", "3", "app:v3"))
	c := &Client{Typed: cs}
	res := Resource{GVR: Deployments, Kind: "Deployment", Namespaced: true}
	rev, err := c.RolloutUndo(context.Background(), Ref{Resource: res, Namespace: "ns", Name: "web"})
	if err != nil || rev != 2 {
		t.Fatalf("undo went to %d, %v; want 2", rev, err)
	}
	got, _ := cs.AppsV1().Deployments("ns").Get(context.Background(), "web", metav1.GetOptions{})
	if img := got.Spec.Template.Spec.Containers[0].Image; img != "app:v2" {
		t.Errorf("image after undo = %s", img)
	}
	if _, has := got.Spec.Template.Labels["pod-template-hash"]; has {
		t.Error("pod-template-hash must not be copied onto the deployment")
	}
}

// A StatefulSet goes back by applying a kept ControllerRevision, as
// kubectl rollout undo does.
func TestRolloutUndoStatefulSet(t *testing.T) {
	ctrl := true
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "ns", UID: "s1"},
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "db", Image: "pg:3"}}}}}}
	cr := func(name string, rev int64, image string) *appsv1.ControllerRevision {
		return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db", UID: "s1", Controller: &ctrl}}},
			Revision: rev,
			Data:     runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"db","image":"` + image + `"}]}}}}`)}}
	}
	cs := fake.NewSimpleClientset(sts, cr("db-1", 1, "pg:1"), cr("db-2", 2, "pg:2"), cr("db-3", 3, "pg:3"))
	c := &Client{Typed: cs}
	res := Resource{GVR: StatefulSets, Kind: "StatefulSet", Namespaced: true}
	rev, err := c.RolloutUndo(context.Background(), Ref{Resource: res, Namespace: "ns", Name: "db"})
	if err != nil || rev != 2 {
		t.Fatalf("undo went to %d, %v; want 2", rev, err)
	}
	if rev, err := c.RolloutTo(context.Background(), Ref{Resource: res, Namespace: "ns", Name: "db"}, 9); err == nil {
		t.Errorf("went to missing revision 9: %d", rev)
	}
}

func TestLogErrorWords(t *testing.T) {
	got := logError("unable to retrieve container logs for containerd://ecafc240", true)
	if !strings.Contains(got, "logs from before the last restart are gone") {
		t.Errorf("got %q", got)
	}
}
