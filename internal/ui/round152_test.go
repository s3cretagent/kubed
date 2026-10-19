package ui

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// r152Paused is deployment paused (3 copies) in namespace shop, paused
// partway from busybox:1.36 (revision 1) to 1.37 (revision 2): its new
// version made one pod, and its old one still runs old copies, all
// available, the list open on it. note is the versions' desired-replicas
// note, as Kubernetes keeps it.
func r152Paused(t *testing.T, old int64, note string) *Model {
	t.Helper()
	labels := map[string]any{"app": "paused"}
	tmpl := func(image string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": image}}}}
	}
	rs := func(name, rev, image string, n, avail int64) runtime.Object {
		return mxObj("apps/v1", "ReplicaSet", "shop", name, map[string]any{"metadata": map[string]any{"labels": labels, "annotations": map[string]any{"deployment.kubernetes.io/revision": rev, "deployment.kubernetes.io/desired-replicas": note},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "paused")["ownerReferences"]},
			"spec":   map[string]any{"replicas": n, "selector": map[string]any{"matchLabels": labels}, "template": tmpl(image)},
			"status": map[string]any{"replicas": n, "readyReplicas": avail, "availableReplicas": avail}})
	}
	c := mxNewCluster([]runtime.Object{
		mxObj("apps/v1", "Deployment", "shop", "paused", map[string]any{"metadata": map[string]any{"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"spec":   map[string]any{"paused": true, "replicas": int64(3), "selector": map[string]any{"matchLabels": labels}, "template": tmpl("busybox:1.37")},
			"status": map[string]any{"replicas": old + 1, "readyReplicas": old, "availableReplicas": old, "updatedReplicas": int64(1)}}),
		rs("paused-2", "2", "busybox:1.37", 1, 0),
		rs("paused-1", "1", "busybox:1.36", old, old),
	}, false)
	m := mxModel(c, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("deployments", true)
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == "paused" {
			m.tbl.jump(i)
			m.tbl.touched = true
		}
	}
	if r, _ := m.tbl.selected(); r.Obj.GetName() != "paused" {
		t.Fatal("no deployment paused")
	}
	return m
}

// QA round 152: u on a deployment paused partway said nothing rolls out
// until it is resumed, but Kubernetes went back at once: the version
// gone back to already ran all 3 copies, so it scaled the other one to 0.
// The confirm and the result say so, and that it stays paused; going
// back to a version that runs fewer copies waits for resume, as before.
func TestPausedGoBackToFullVersionIsAtOnce(t *testing.T) {
	const atOnce = "already runs all its copies"
	for _, tc := range []struct {
		old  int64
		note string // the versions' desired-replicas note
		want bool
	}{{3, "3", true}, {2, "3", false}, {3, "2", false}} { // a stale note: Kubernetes doesn't count it saturated
		m := r152Paused(t, tc.old, tc.note)
		if _, err := mxPress(m, "u"); err != nil || m.prompt == nil {
			t.Fatalf("u: %v, flash %q", err, m.flash)
		}
		title := m.prompt.title
		if strings.Contains(title, atOnce) != tc.want || strings.Contains(title, pausedWords) == tc.want || tc.want && !strings.Contains(title, stillPaused) {
			t.Errorf("old version runs %d: u asks %q", tc.old, title)
		}
		// The result, as u's says it (the fake cluster can't go back).
		row, _ := m.tbl.selected()
		target, _ := undoTarget(m.enr.RolloutOf(row.Obj))
		ref := m.ref(row)
		for _, text := range []string{goingBackWords(ref, 1, true, takesAtOnce(row.Obj, target), ""),
			imageSetWords(ref, "busybox:1.36", "c", true, takesAtOnce(row.Obj, m.imageVersion(row.Obj, "c", "busybox:1.36", false)), "")} {
			if strings.Contains(text, atOnce) != tc.want || strings.Contains(text, "rolls out once it is resumed") == tc.want || tc.want && !strings.Contains(text, stillPaused) {
				t.Errorf("old version runs %d: result %q", tc.old, text)
			}
		}

		// i to that version's image: the same.
		m = r152Paused(t, tc.old, tc.note)
		if _, err := mxPress(m, "i"); err != nil || m.prompt == nil {
			t.Fatalf("i: %v, flash %q", err, m.flash)
		}
		m.prompt.onSubmit("busybox:1.36")
		said := m.prompt.title + "\n" + strings.Join(m.prompt.warnings, "\n")
		if strings.Contains(said, atOnce) != tc.want || strings.Contains(said, pausedWords) == tc.want {
			t.Errorf("old version runs %d: i asks %q", tc.old, said)
		}
		// Another image: no version has it.
		m = r152Paused(t, tc.old, tc.note)
		if _, err := mxPress(m, "i"); err != nil || m.prompt == nil {
			t.Fatalf("i: %v, flash %q", err, m.flash)
		}
		m.prompt.onSubmit("busybox:1.38")
		if said := m.prompt.title + "\n" + strings.Join(m.prompt.warnings, "\n"); strings.Contains(said, atOnce) || !strings.Contains(said, pausedWords) {
			t.Errorf("old version runs %d: i to a new image asks %q", tc.old, said)
		}
	}
}
