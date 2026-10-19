package kube

import (
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func podObj(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind("Pod")
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

// A user allowed to list pods only in "team" must still see them.
func TestCacheFallsBackToNamespaceWhenForbidden(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{Pods: "PodList"}, podObj("team", "a"), podObj("other", "b"))
	dyn.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetNamespace() == "" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", nil)
		}
		return false, nil, nil
	})
	c := NewCache(dyn)
	defer c.Stop()

	c.Watch(Pods)
	wait(t, func() bool { return c.isForbidden(Pods) })
	if err := c.Err(Pods); !errors.Is(err, ErrPickNamespace) {
		t.Fatalf("all namespaces: err = %v, want ErrPickNamespace", err)
	}

	c.SetNamespace("team")
	wait(t, func() bool { return c.Synced(Pods) })
	got := c.List(Pods, "team")
	if len(got) != 1 || got[0].GetName() != "a" {
		t.Fatalf("team pods = %v", got)
	}
	if c.Err(Pods) != nil {
		t.Errorf("unexpected error after fallback: %v", c.Err(Pods))
	}
}

func TestStripSecret(t *testing.T) {
	s := &unstructured.Unstructured{Object: map[string]any{
		"data":       map[string]any{"TOKEN": "c2VjcmV0"},
		"stringData": map[string]any{"PASS": "plain"},
	}}
	s.SetAnnotations(map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"TOKEN":"c2VjcmV0"}}`, "keep": "me"})
	StripSecret(s)
	data := s.Object["data"].(map[string]any)
	if v, ok := data["TOKEN"]; !ok || v != "" {
		t.Errorf("data = %v; want key kept, value blank", data)
	}
	if s.Object["stringData"].(map[string]any)["PASS"] != "" {
		t.Error("stringData value kept")
	}
	if a := s.GetAnnotations(); a["kubectl.kubernetes.io/last-applied-configuration"] != "" || a["keep"] != "me" {
		t.Errorf("annotations = %v", a)
	}
}

func wait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Round 73: an edit of replicas alone rewrites the editor's managedFields
// time, which covers the template too; the template's time stays.
func TestTemplateTimeIgnoresOtherEdits(t *testing.T) {
	back, edit := time.Now().Add(-5*time.Minute).Truncate(time.Second), time.Now().Truncate(time.Second)
	dep := func(at time.Time, replicas int64, image string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": "dep-tt"},
			"spec": map[string]any{"replicas": replicas, "template": map[string]any{"spec": map[string]any{
				"containers": []any{map[string]any{"name": "web", "image": image}}}}}}}
		u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:replicas":{},"f:template":{}}}`)}})
		return u
	}
	NoteTemplateChange(dep(back, 2, "nginx:alpine"))
	NoteTemplateChange(dep(edit, 3, "nginx:alpine")) // replicas only
	if at, _ := TemplateChangedAt("dep-tt"); !at.Equal(back) {
		t.Errorf("template written at %v, want %v", at, back)
	}
	NoteTemplateChange(dep(edit, 3, "nginx:1.27")) // the template itself
	if at, _ := TemplateChangedAt("dep-tt"); !at.Equal(edit) {
		t.Errorf("a new image at %v, want %v", at, edit)
	}
}

func notePodObj(name, owner string, made time.Time, phase string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": name, "namespace": "b", "uid": name, "creationTimestamp": made.Format(time.RFC3339),
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": owner, "uid": owner, "controller": true}}},
		"status": map[string]any{"phase": phase}}}
	return u
}

// Review of round 80: the list at start comes in name order; a young pod
// noted before its older sibling still counts as having joined it.
func TestNotePodEitherOrder(t *testing.T) {
	now := time.Now()
	ResetPodWatch(now.Add(-time.Hour))
	NotePod(notePodObj("rs1-aaa", "rs1", now.Add(-30*time.Minute), "Running")) // a replacement, listed first
	NotePod(notePodObj("rs1-zzz", "rs1", now.Add(-240*time.Hour), "Running"))
	if !PodHadOlder("rs1-aaa") {
		t.Error("the replacement listed first didn't count the older pod")
	}
	if PodHadOlder("rs1-zzz") {
		t.Error("the old pod counted as joining the younger one")
	}
}

// Finished pods and pods deleted at once leave the pods seen running, so
// a later batch of that owner starts fresh.
func TestNotePodForgetsFinishedAndDeleted(t *testing.T) {
	now := time.Now()
	ResetPodWatch(now.Add(-time.Hour))
	NotePod(notePodObj("rs2-done", "rs2", now.Add(-time.Hour), "Succeeded"))
	pending := notePodObj("rs2-pending", "rs2", now.Add(-time.Hour), "Pending")
	NotePod(pending)
	forgetPod(pending) // deleted with no grace: no deletion time was ever seen
	NotePod(notePodObj("rs2-new", "rs2", now, "Running"))
	if PodHadOlder("rs2-new") {
		t.Error("a finished or deleted pod counted as running beside it")
	}
}

// A namespace whose pods are watched on their own (no cluster-wide right)
// is seen from when that watch started, not from when kubed did.
func TestPodsMadeBetweenNamespaceWatch(t *testing.T) {
	now := time.Now()
	ResetPodWatch(now.Add(-time.Hour))
	podsMu.Lock()
	watchedFrom["b"] = now.Add(-time.Minute) // went there a minute ago
	podsMu.Unlock()
	if _, seen := PodsMadeBetween("rs3", "b", now.Add(-10*time.Minute), now.Add(-9*time.Minute)); seen {
		t.Error("a scale before the namespace was watched counted as seen")
	}
	if _, seen := PodsMadeBetween("rs3", "other", now.Add(-10*time.Minute), now.Add(-9*time.Minute)); !seen {
		t.Error("a namespace watched with the cluster didn't count as seen")
	}
}

// Review of round 92: once kubed may have missed template writes (asleep,
// a broken watch), the go-back times it saw before aren't trusted: the
// version may have gone and come back again meanwhile.
func TestMissedWritesForgetSeenTimes(t *testing.T) {
	dep := func(uid string, at time.Time, image string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "web", "namespace": "qa", "uid": uid},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"containers": []any{map[string]any{"name": "web", "image": image}}}}}}}
		u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubed", Operation: metav1.ManagedFieldsOperationUpdate,
			Time: &metav1.Time{Time: at}, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`)}})
		return u
	}
	settings := func(image string) string {
		return TemplateSettings(dep("x", time.Now(), image).Object["spec"].(map[string]any)["template"].(map[string]any))
	}
	now := time.Now().Truncate(time.Second)

	// A seen live, then C seen only after the fact.
	NoteTemplateChange(dep("dep-mw1", now.Add(-20*time.Second), "a"))
	if _, ok := TemplateWrittenAt("dep-mw1", settings("a"), ""); !ok {
		t.Fatal("a live write not kept")
	}
	NoteTemplateChange(dep("dep-mw1", now.Add(-2*time.Minute), "c"))
	if at, ok := TemplateWrittenAt("dep-mw1", settings("a"), ""); ok {
		t.Errorf("after missed writes, a still at %v", at)
	}

	// A seen live, then A again with a later write kubed didn't see.
	NoteTemplateChange(dep("dep-mw2", now.Add(-5*time.Minute+30*time.Second), "a"))
	forgetTemplateWrites("dep-mw2") // start known: make its entry live-like below
	settingsMu.Lock()
	templateWrites["dep-mw2"] = []templateSeen{{template: settings("a") + "\x00", at: now.Add(-4 * time.Minute)}}
	settingsMu.Unlock()
	NoteTemplateChange(dep("dep-mw2", now.Add(-2*time.Minute), "a"))
	if at, ok := TemplateWrittenAt("dep-mw2", settings("a"), ""); ok {
		t.Errorf("a round trip kubed missed: a still at %v", at)
	}
}

// Review of area B: a recent write read on the first sight of a
// Deployment (kubed just started) wasn't seen happen: not "live", so the
// versions list still asks for its progress before dating a go-back by it.
func TestFirstSightIsNotLive(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "fs", "namespace": "qa", "uid": "dep-first-sight",
			"managedFields": []any{map[string]any{"manager": "kubectl-client-side-apply", "operation": "Update",
				"time": time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339), "fieldsType": "FieldsV1",
				"fieldsV1": map[string]any{"f:spec": map[string]any{"f:template": map[string]any{}}}}}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "x"}}}}}}}
	NoteTemplateChange(u)
	if _, live, ok := TemplateChangedAtSure("dep-first-sight"); ok && live {
		t.Error("a write read at first sight counts as seen live")
	}
}
