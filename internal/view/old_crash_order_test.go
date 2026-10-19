package view

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Review of round 128: mid-rollout, the new version's pod runs but isn't
// ready and an old version's pod keeps crashing. The box is the same
// whichever pod is read first: the new pod's problem leads, and the old
// crash is quoted as "Also pod web-old-a crashed:", never as the
// headline's Error. With the new pod the headline because it keeps
// stopping (exit 0), the old crash is never its Error, and p opens it.
func TestOldCrashSameInEitherOrder(t *testing.T) {
	look := Lookups{Crash: func(x *App) (string, bool) { return "LOG OF " + x.CrashPod, true }}
	notReady := func() *unstructured.Unstructured {
		p := mxPod("web-new-a", "ReplicaSet", corev1.PodRunning, mxC{name: "app", state: mxRun(5 * time.Minute)})
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Minute))}}
		return rfOwned(p, "web-new")
	}
	stopping := func() *unstructured.Unstructured {
		return rfOwned(mxPod("web-new-a", "ReplicaSet", corev1.PodRunning,
			mxC{name: "app", state: mxWait("CrashLoopBackOff", "back-off 20s restarting failed container"), restarts: 2, last: mxEnd("Completed", 0, 10*time.Second)}), "web-new")
	}
	for _, c := range []struct {
		name   string
		newPod func() *unstructured.Unstructured
		also   bool // the old crash must be quoted as Also; else it may be left out
	}{{"new pod not ready", notReady, true}, {"new pod keeps stopping", stopping, false}} {
		boxes := map[bool]string{}
		for _, oldFirst := range []bool{true, false} {
			newp, oldp := c.newPod(), rfOwned(rfCrash("web-old-a", 5), "web-old")
			pods := []*unstructured.Unstructured{newp, oldp}
			if oldFirst {
				pods = []*unstructured.Unstructured{oldp, newp}
			}
			a, _ := rfRun(t, pods, []*unstructured.Unstructured{rfDep("web", "2", false, false)}, []*unstructured.Unstructured{rfRS("web", "web-new", "2", time.Minute, 0), rfRS("web", "web-old", "1", 3*time.Hour, 0)})
			box := strings.Join(AppLines(a, look), "\n")
			boxes[oldFirst] = box
			if a.HeadPod != "web-new-a" {
				t.Errorf("%s, old first %v: head %q\n%s", c.name, oldFirst, a.HeadPod, box)
			}
			if strings.Contains(box, "Error    LOG OF web-old-a") || c.also && !strings.Contains(box, "pod web-old-a crashed: LOG OF web-old-a") {
				t.Errorf("%s, old first %v: old crash\n%s", c.name, oldFirst, box)
			}
			if a.Status.Short == "Restarting" && !a.PrevLogsOfHead() {
				t.Errorf("%s, old first %v: p doesn't open the headline's pod", c.name, oldFirst)
			}
		}
		if boxes[true] != boxes[false] {
			t.Errorf("%s: depends on order\nold first:\n%s\nnew first:\n%s", c.name, boxes[true], boxes[false])
		}
	}
}
