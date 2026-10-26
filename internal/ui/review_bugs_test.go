package ui

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// reviewGoBack: deployment web at revision 2 (image now), revision 1
// (image before) kept, plus extra objects. It returns u's confirm title
// and b's from the versions list, both going back to revision 1.
func reviewGoBack(t *testing.T, now, before string, extra ...runtime.Object) (u, b string) {
	t.Helper()
	lbl := map[string]any{"app": "web"}
	tmpl := func(image string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": lbl}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}
	}
	rs := func(name, rev, image string) *unstructured.Unstructured {
		return mxObj("apps/v1", "ReplicaSet", "shop", name, map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": rev},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl(image)}, "status": map[string]any{"replicas": int64(1)}})
	}
	objs := append([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil),
		mxObj("apps/v1", "Deployment", "shop", "web", map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"spec":   map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl(now)},
			"status": map[string]any{"replicas": int64(1), "updatedReplicas": int64(1)}}),
		rs("web-2", "2", now), rs("web-1", "1", before)}, extra...)
	cl := mxNewCluster(objs, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	m.jumpTo(location{kind: locHome})
	dep, ok := m.enr.Get(kube.Deployments, "shop", "web")
	if !ok {
		t.Fatal("no deployment web")
	}
	m.undoRolloutNow(view.Row{Obj: dep})
	if m.prompt == nil {
		t.Fatalf("u: no confirm, flash %q", m.flash)
	}
	u = m.prompt.title
	m.prompt, m.mode = nil, modeTable
	m.versionsNow(view.Row{Obj: dep})
	if m.prompt == nil || len(m.prompt.items) != 2 {
		t.Fatalf("v: prompt %+v, flash %q", m.prompt, m.flash)
	}
	m.prompt.items[1].alsoRun()
	if m.prompt == nil {
		t.Fatalf("b: no confirm, flash %q", m.flash)
	}
	return u, m.prompt.title
}

// Review: b from the versions list didn't say the version it goes back
// to failed when it last ran (its image couldn't be downloaded), while u
// to the same version did. Both say it now.
func TestGoBackFromVersionsSaysItFailedBefore(t *testing.T) {
	failed := mxObj("v1", "Event", "shop", "web-old.1", map[string]any{"involvedObject": map[string]any{"kind": "Pod", "name": "web-1-q7x2k", "namespace": "shop"},
		"type": "Warning", "reason": "Failed", "lastTimestamp": time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		"message": `Failed to pull image "nginx:1": not found`})
	u, b := reviewGoBack(t, "nginx:2", "nginx:1", failed)
	const want = "That version failed when it last ran, so going back won't help: its image nginx:1 couldn't be downloaded (that image or tag doesn't exist)"
	if !strings.Contains(u, want) {
		t.Errorf("u asks %q", u)
	}
	if !strings.Contains(b, want) {
		t.Errorf("b asks %q", b)
	}
}

// Review: u to a version with the same tag but another build (the digest
// differs) said "Same image: only settings differ", while b from the
// versions list said it is another build. Both say it is another build.
func TestUndoSaysSameTagOtherBuild(t *testing.T) {
	u, b := reviewGoBack(t, "nginx:1@sha256:bbbb", "nginx:1@sha256:aaaa")
	const want = "Same tag, but a different build of it (the image digest differs)."
	if !strings.Contains(u, want) || strings.Contains(u, "only settings differ") {
		t.Errorf("u asks %q", u)
	}
	if !strings.Contains(b, want) {
		t.Errorf("b asks %q", b)
	}
}

// Review: x on a ConfigMap with only binaryData said it "has no values".
// It lists those keys as binary data with their size, not their bytes.
func TestShowConfigListsBinaryKeys(t *testing.T) {
	cm := mxObj("v1", "ConfigMap", "shop", "assets", map[string]any{"binaryData": map[string]any{"logo.png": "iVBORw0KGgo="}})
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), cm}, false)
	defer cl.client.Cache.Stop()
	m := mxModel(cl, 160, 50)
	cmd := m.showConfig(view.Row{Obj: cm})
	if cmd == nil {
		t.Fatalf("x says %q", m.flash)
	}
	msg, ok := cmd().(pagerMsg)
	if !ok || !strings.Contains(msg.content, "logo.png") || !strings.Contains(msg.content, "binary data, 8 bytes (not shown)") || strings.Contains(msg.content, "iVBOR") {
		t.Errorf("x shows %q", msg.content)
	}
}

// Review: the contexts list coloured a row by its name while PROD, for
// the one you're on, said what kubed decided: an unknown name answered
// production read "PROD yes" in green, a production name kubed was told
// isn't read "PROD no" in red. The colour follows PROD.
func TestContextRowToneFollowsProd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(`apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: "https://127.0.0.1:1"}
users:
- name: u
contexts:
- name: mystery
  context: {cluster: c, user: u}
- name: shop-prod
  context: {cluster: c, user: u}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	m := testModel()
	m.kc = kube.LoadKubeconfig(path)
	m.opts.ProdPattern = regexp.MustCompile("prod")
	row := func(name string) view.Row {
		t.Helper()
		for _, r := range m.contextRows() {
			if r.Obj.GetName() == name {
				return r
			}
		}
		t.Fatalf("no context %s", name)
		return view.Row{}
	}
	m.context, m.env = "mystery", environment{known: true, prod: true, source: "answer"}
	if r := row("mystery"); r.Cells[1] != "yes" || r.Tone != view.ToneBad {
		t.Errorf("answered production: PROD %q, tone %v", r.Cells[1], r.Tone)
	}
	m.context, m.env = "shop-prod", environment{known: true, source: "answer"}
	if r := row("shop-prod"); r.Cells[1] != "no" || r.Tone == view.ToneBad {
		t.Errorf("told not production: PROD %q, tone %v", r.Cells[1], r.Tone)
	}
	if r := row("mystery"); r.Cells[1] != "unknown" || r.Tone != view.ToneNormal { // not the one you're on: its name decides
		t.Errorf("other context: PROD %q, tone %v", r.Cells[1], r.Tone)
	}
}

// Review: a forward to a pod whose connection had just ended named the
// pod again under THROUGH, while a live one to a pod says "-" there.
func TestEndedForwardToPodSaysNoThrough(t *testing.T) {
	m := testModel()
	m.forwards = &kube.Forwards{}
	m.endedHeld = []endedForward{
		{f: &kube.Forward{LocalPort: 18001, Pod: "web-1-a", PodPort: 80}, at: time.Now()},
		{f: &kube.Forward{LocalPort: 18002, Pod: "web-1-b", PodPort: 80, Target: "service web"}, at: time.Now()},
	}
	rows := m.forwardRows()
	if len(rows) != 2 {
		t.Fatalf("rows %+v", rows)
	}
	if c := rows[0].Cells; c[1] != "pod web-1-a" || c[3] != "-" {
		t.Errorf("to a pod: %q", c)
	}
	if c := rows[1].Cells; c[1] != "service web" || c[3] != "web-1-b" {
		t.Errorf("to a service: %q", c)
	}
}
