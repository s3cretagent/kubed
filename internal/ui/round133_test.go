package ui

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// QA round 133: a command for a kind that doesn't exist said so, yet moved
// the screen to the namespace it named (":zzz -n kube-system"), or put a
// "this screen only" namespace back (":zzz"). A failed command changes
// nothing: namespace, the one you chose, where you are, the filter.
func TestFailedCommandChangesNothing(t *testing.T) {
	forms := []string{"", " -A", " --all-namespaces", " all", " -n shop", " --namespace shop", " shop", " -n default", " -l app=web", " -lapp=web", " -n shop -l app=web"}
	starts := []struct{ ns, chosen string }{{"default", "default"}, {"shop", "default"}, {"", "default"}}
	for _, word := range []string{"zzz", "-n", "-A", "-l", "--namespace", "--all-namespaces", "--selector"} {
		for _, f := range forms {
			for _, s := range starts {
				m := kindsModel(t)
				m.namespace, m.chosenNS = s.ns, s.chosen
				m.client.Cache.SetNamespace(s.ns)
				m.openResource("pods", false)
				m.tbl.filter = "web"
				before := m.loc
				typeCommand(m, word+f)
				line := ":" + word + f + " from " + s.ns
				if !m.flashErr {
					t.Errorf("%s: no error, flash %q", line, m.flash)
				}
				if m.namespace != s.ns || m.chosenNS != s.chosen {
					t.Errorf("%s: namespace %q chosen %q, want %q %q (flash %q)", line, m.namespace, m.chosenNS, s.ns, s.chosen, m.flash)
				}
				if m.loc.kind != before.kind || m.loc.res.GVR != before.res.GVR || m.loc.ns != before.ns {
					t.Errorf("%s: moved to %v %v", line, m.loc.kind, m.loc.res.GVR)
				}
				if m.tbl.filter != "web" {
					t.Errorf("%s: filter %q", line, m.tbl.filter)
				}
			}
		}
	}
}

// QA round 133: ":-l app=web" said there is no namespace called "app=web":
// a flag typed first is no kind, and the words say to give one.
func TestLeadingFlagIsNoKind(t *testing.T) {
	for _, line := range []string{"-l app=web", "-A", "-n shop", "--namespace shop", "-l", "-n"} {
		m := kindsModel(t)
		m.namespace, m.chosenNS = "default", "default"
		typeCommand(m, line)
		if !m.flashErr || strings.Contains(m.flash, "no namespace called") || !strings.Contains(m.flash, "kind") {
			t.Errorf(":%s says %q", line, m.flash)
		}
	}
}

// QA round 133: home's footer offered "X What it uses" on an app made of
// a pod nothing owns, and X there said it runs nothing it works on. The
// footer offers what the Enter menu does, and on such an app X explains.
func TestBarePodAppFooterAndX(t *testing.T) {
	for _, phase := range []string{"Running", "Failed", "Stopping"} {
		pod := mxPod("evictme", "", "", "", map[bool]string{true: "Running", false: phase}[phase == "Stopping"], 0, map[string]any{"app": "evictme"})
		switch phase {
		case "Failed":
			pod.Object["status"].(map[string]any)["reason"] = "Evicted"
		case "Stopping":
			md := pod.Object["metadata"].(map[string]any)
			md["deletionTimestamp"] = time.Now().Add(590 * time.Second).UTC().Format(time.RFC3339)
			md["deletionGracePeriodSeconds"] = int64(600)
		}
		c := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), pod}, false)
		m := mxModel(c, 200, 50)
		m.namespace, m.chosenNS = "shop", "shop"
		c.client.Cache.SetNamespace("shop")
		m.jumpTo(location{kind: locHome})
		m.refreshRows()
		for i, r := range m.tbl.rows {
			if r.Obj.GetName() == "evictme" {
				m.tbl.jump(i)
			}
		}
		row, ok := m.tbl.selected()
		if !ok || row.Obj.GetName() != "evictme" {
			var names []string
			for _, r := range m.tbl.rows {
				names = append(names, r.Obj.GetNamespace()+"/"+r.Obj.GetName())
			}
			t.Fatalf("%s: home row %v %v; rows %v", phase, ok, row.Obj, names)
		}
		footer := hintKeys(m.hints())
		m.menu()
		inMenu := false
		if m.prompt != nil {
			for _, it := range m.prompt.items {
				inMenu = inMenu || it.key == "X" && !it.hidden
			}
		}
		if _, offered := footer["X"]; offered != inMenu {
			t.Errorf("%s: footer offers X %v (%q), the menu %v", phase, offered, footer["X"], inMenu)
		}
	}
}

// QA round 134: - on a deployment row did nothing and said nothing. Where +
// scales, - says how to scale down; elsewhere it stays an unknown key.
func TestMinusPointsToPlus(t *testing.T) {
	mxClusters()
	for _, c := range []struct {
		name string
		open func(m *Model)
		tip  bool
	}{
		{"deployments", func(m *Model) { m.jumpTo(location{kind: locHome}); m.openResource("deployments", true) }, true},
		{"home", func(m *Model) { m.jumpTo(location{kind: locHome}) }, true},
		{"pods", func(m *Model) { m.jumpTo(location{kind: locHome}); m.openResource("pods", true) }, false},
		{"configmaps", func(m *Model) { m.jumpTo(location{kind: locHome}); m.openResource("configmaps", true) }, false},
	} {
		for _, ro := range []bool{false, true} {
			m := mxModel(mxFull, 200, 50)
			m.opts.ReadOnly = ro
			c.open(m)
			m.refreshRows()
			m.flash, m.flashErr = "", false
			before := mxSnapOf(m)
			if _, err := mxPress(m, "-"); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			want := ""
			if c.tip && !ro {
				want = "+ scales it: type the new count (lower to scale down)"
			}
			if m.flash != want || m.flashErr || m.prompt != nil {
				t.Errorf("%s readonly=%v: - says %q (error %v, prompt %v), want %q", c.name, ro, m.flash, m.flashErr, m.prompt != nil, want)
			}
			after := mxSnapOf(m)
			after.flash = before.flash // said above
			if after != before {
				t.Errorf("%s readonly=%v: - changed %+v to %+v", c.name, ro, before, after)
			}
		}
	}
}

// QA round 134: ctrl-k on a pod already stopping slowly: the answer to the
// delete was lost on the way back ("lost the connection"), though the pod
// was gone. When the connection drops and a fresh look finds it gone, it
// was stopped (or deleted); when it is still there, the hedge stays.
func TestStopLostAnswerButGone(t *testing.T) {
	for _, c := range []struct {
		name     string
		removes  bool // the delete went through before the answer was lost
		wantGone bool
	}{{"gone", true, true}, {"still there", false, false}} {
		for _, verb := range []string{"stopped", "deleted"} {
			pod := mxPod("slow", "", "", "", "Running", 0, nil)
			dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{kube.Pods: "PodList"}, pod)
			dyn.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				if c.removes {
					_ = dyn.Tracker().Delete(kube.Pods, "shop", "slow")
				}
				return true, nil, errors.New("http2: client connection lost")
			})
			res := kube.NewResources([]kube.Resource{{GVR: kube.Pods, Kind: "Pod", Namespaced: true, Singular: "pod", Verbs: []string{"get", "list", "delete"}}})
			m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(dyn), Dynamic: dyn}}
			m.enr = enrich.New(m.client.Cache, res, nil)
			m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Pods, Kind: "Pod", Namespaced: true}}
			rows := []view.Row{{Obj: pod, Cells: []string{"slow"}}}
			if verb == "stopped" {
				m.killRows(rows)
			} else {
				m.deleteRows(rows)
			}
			if m.prompt == nil || m.prompt.onYes == nil {
				t.Fatalf("%s: no confirm", verb)
			}
			msg, _ := m.prompt.onYes()().(resultMsg)
			switch {
			case c.wantGone && (msg.err != nil || !strings.HasPrefix(msg.text, verb+" pod slow")):
				t.Errorf("%s, %s: says %q, err %v", c.name, verb, msg.text, msg.err)
			case !c.wantGone && (msg.err == nil || !strings.Contains(friendlyError(msg.err.Error()), "lost the connection")):
				t.Errorf("%s, %s: says %q, err %v", c.name, verb, msg.text, msg.err)
			}
		}
	}
}
