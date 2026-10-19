package view

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// QA round 147: the pods box for a pod too big for any node had only its
// Problem line, while home's box said to lower what it asks for or add a
// bigger machine type. The pods box says the same, where it goes by what
// runs the pod; the node selector and taint waits get home's fixes too.
func TestPodBoxFixesTooBigAndNodeWaits(t *testing.T) {
	big := Status{"Too big for any node", "it asks for 64 CPU, more than any node here has (16 CPU at most)", ToneBad}
	for owner, where := range map[string]string{
		"Deployment":  "in its deployment",
		"ReplicaSet":  "in its deployment",
		"StatefulSet": "in its stateful set",
		"Pod":         "where this pod is defined (nothing makes it again if it goes)",
		"CronJob":     "in its cron job (gC, then e on it)",
	} {
		want := "lower what it asks for (its CPU or memory request) " + where + ", or add a bigger machine type to the cluster"
		if fix := podFixFor(big, owner); fix != want {
			t.Errorf("%s: %q, want %q", owner, fix, want)
		}
	}
	sel := Status{"Waiting for a node", "no node fits: 7 nodes don't match its node selector", ToneWarn}
	if fix := podFixFor(sel, "Deployment"); !strings.Contains(fix, "fix its node selector (or node affinity) in its deployment") {
		t.Errorf("node selector: %q", fix)
	}
	taint := Status{"Waiting for a node", "no node fits: 19 nodes are reserved for other workloads (taints)", ToneWarn}
	if fix := podFixFor(taint, "StatefulSet"); !strings.Contains(fix, "give it the toleration for the nodes it should run on in its stateful set") {
		t.Errorf("taints: %q", fix)
	}
	full := Status{"Waiting for a node", "no node fits: not enough free CPU on 3 nodes", ToneWarn}
	if fix := podFixFor(full, "Deployment"); fix != "E shows why no node fits" {
		t.Errorf("nodes full: %q", fix)
	}
}

// QA round 147: a Deployed bound read "≤10m ago" for a deploy 10m53s ago:
// Age rounds down, so "at most" claimed less than the truth. A bound
// rounds up to the unit Age shows; exact ages and "+ ago" as before.
func TestDeployedBoundRoundsUp(t *testing.T) {
	for _, c := range []struct {
		ago  time.Duration
		want string
	}{
		{10*time.Minute + 53*time.Second, "≤11m ago"},
		{59 * time.Second, "≤1m ago"},
		{time.Hour + time.Second, "≤1h1m ago"},
		{4 * time.Minute, "≤4m ago"},
		{30 * time.Hour, "≤30h ago"},
	} {
		at := time.Now().Add(-c.ago)
		a := &App{Name: "web", LastDeploy: at, DeployAfter: true}
		if cell := AppRows([]*App{a})[0].Cells[5]; cell != c.want {
			t.Errorf("%s ago: %q, want %q", c.ago, cell, c.want)
		}
		a.DeployAfter, a.DeployBefore = false, true
		if cell := AppRows([]*App{a})[0].Cells[5]; cell != Age(at)+"+ ago" {
			t.Errorf("%s ago, then or earlier: %q", c.ago, cell)
		}
	}
}

// QA round 149: a node with 54 of 58 pod places taken read "No pod room"
// in the list while its box said room for 4 more. Only a full node has no
// room; one nearly full has few places left.
func TestNodePodRoomWords(t *testing.T) {
	node := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{"name": "n1"}, "status": map[string]any{"allocatable": map[string]any{"pods": "10"},
			"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}}
	for _, c := range []struct {
		pods int
		want string
	}{{9, "Few pod places"}, {10, "No pod room"}} {
		var pods []*unstructured.Unstructured
		for i := range c.pods {
			pods = append(pods, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
				"metadata": map[string]any{"name": fmt.Sprintf("p%d", i), "namespace": "a", "uid": fmt.Sprintf("u%d", i)},
				"spec":     map[string]any{"nodeName": "n1"}, "status": map[string]any{"phase": "Running"}}})
		}
		e := enrich.New(listStore{kube.Nodes: {node}, kube.Pods: pods}, kube.NewResources([]kube.Resource{{GVR: kube.Nodes, Kind: "Node"}, {GVR: kube.Pods, Kind: "Pod", Namespaced: true}}), nil)
		cells, _ := registry[kube.Nodes.GroupResource()].Row(e, node)
		if cells[1] != c.want {
			t.Errorf("%d of 10 pods: status %q, want %q", c.pods, cells[1], c.want)
		}
	}
}
