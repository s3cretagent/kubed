package ui

import (
	"context"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

func TestKeyBytes(t *testing.T) {
	cases := []struct {
		key  tea.KeyMsg
		want string
	}{
		{tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("ls")}, "ls"},
		{tea.KeyMsg{Type: tea.KeyEnter}, "\r"},
		{tea.KeyMsg{Type: tea.KeyTab}, "\t"},
		{tea.KeyMsg{Type: tea.KeyBackspace}, "\x7f"},
		{tea.KeyMsg{Type: tea.KeyEsc}, "\x1b"},
		{tea.KeyMsg{Type: tea.KeyCtrlC}, "\x03"},
		{tea.KeyMsg{Type: tea.KeyCtrlD}, "\x04"},
		{tea.KeyMsg{Type: tea.KeyUp}, "\x1b[A"},
		{tea.KeyMsg{Type: tea.KeyLeft}, "\x1b[D"},
		{tea.KeyMsg{Type: tea.KeySpace}, " "},
		{tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b"), Alt: true}, "\x1bb"},
	}
	for _, c := range cases {
		if got := string(keyBytes(c.key)); got != c.want {
			t.Errorf("keyBytes(%v) = %q, want %q", c.key, got, c.want)
		}
	}
}

func testModel() *Model { return testModelOn(nil) }

// testModelOn is a testModel whose fake cluster is wrapped (nil: as is).
func testModelOn(wrap func(dynamic.Interface) dynamic.Interface) *Model {
	res := kube.NewResources([]kube.Resource{
		{GVR: kube.Nodes, Kind: "Node", ShortNames: []string{"no"}, Singular: "node"},
		{GVR: kube.NodePools, Kind: "NodePool", Singular: "nodepool"},
		{GVR: kube.NodeClaims, Kind: "NodeClaim", Singular: "nodeclaim"},
		{GVR: kube.Pods, Kind: "Pod", Namespaced: true, ShortNames: []string{"po"}, Singular: "pod"},
		{GVR: kube.Namespaces, Kind: "Namespace", ShortNames: []string{"ns"}, Singular: "namespace"},
	})
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		kube.Nodes: "NodeList", kube.NodePools: "NodePoolList", kube.NodeClaims: "NodeClaimList",
		kube.Pods: "PodList", kube.Namespaces: "NamespaceList",
	})
	var cluster dynamic.Interface = dyn
	if wrap != nil {
		cluster = wrap(dyn)
	}
	m := &Model{client: &kube.Client{Resources: res, Cache: kube.NewCache(cluster)}}
	m.enr = enrich.New(m.client.Cache, res, nil)
	return m
}

// waitModel is a testModel whose nodes never load, for tests of waiting
// on a list (the fake cluster otherwise answers at once). Only the node
// list hangs; everything else loads as usual.
func waitModel() *Model {
	return testModelOn(func(d dynamic.Interface) dynamic.Interface { return slowNodes{d} })
}

// slowNodes is a cluster whose node list never answers.
type slowNodes struct{ dynamic.Interface }

func (s slowNodes) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return slowList{s.Interface, kube.Nodes}.Resource(gvr)
}

// slowList is a cluster where one kind's list never answers.
type slowList struct {
	dynamic.Interface
	hangs schema.GroupVersionResource
}

func (s slowList) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	if gvr == s.hangs {
		return hangingList{s.Interface.Resource(gvr)}
	}
	return s.Interface.Resource(gvr)
}

type hangingList struct {
	dynamic.NamespaceableResourceInterface
}

func (hangingList) List(ctx context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSuggest(t *testing.T) {
	m := testModel()
	got := m.suggest("no")
	if len(got) < 3 || got[0].name != "nodes" {
		t.Fatalf("suggest(no) = %+v, want nodes first", got)
	}
	names := map[string]bool{}
	for _, s := range got {
		names[s.name] = true
	}
	for _, want := range []string{"nodepools", "nodeclaims"} {
		if !names[want] {
			t.Errorf("suggest(no) missing %s: %+v", want, got)
		}
	}
	if m.suggest("pods payment") != nil {
		t.Error("no suggestions once an argument is being typed")
	}
	if s := m.suggest("ct"); len(s) == 0 || s[0].name != "ctx" {
		t.Errorf("suggest(ct) = %+v, want ctx", s)
	}
}

func TestJumpTargetsResolve(t *testing.T) {
	// Every jump must name something Lookup understands on a cluster that
	// serves it (full names, so CRDs never collide with core resources).
	for key, target := range jumps {
		if target == "" {
			t.Errorf("g%s has no target", key)
		}
	}
	m := testModel()
	defer m.client.Cache.Stop()
	m.jump("n")
	if m.loc.res.GVR != kube.Nodes {
		t.Errorf("gn opened %v", m.loc.res.GVR)
	}
	m.jump("a") // argo not served here
	if m.flash == "" || !m.flashErr {
		t.Error("g a on a cluster without ArgoCD should explain, not fail silently")
	}
}

func TestBurstKeysSplit(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.onKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("gn")}) // "g" then "n" in one read
	if m.loc.res.GVR != kube.Nodes {
		t.Fatalf("burst gn opened %v, want nodes", m.loc.res.GVR)
	}
}

func TestEmptyPromptListsCommands(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	if got := m.suggest(""); len(got) == 0 || got[0].name != "apps" {
		t.Errorf("empty prompt: %+v", got)
	}
	if got := m.suggest("ns "); len(got) == 0 || got[0].name != "ns all" {
		t.Errorf(":ns should suggest namespaces, all first: %+v", got)
	}
}

// Namespace keeps the list hanging: informers list through it.
func (h hangingList) Namespace(ns string) dynamic.ResourceInterface {
	return hangingNS{h.NamespaceableResourceInterface.Namespace(ns)}
}

type hangingNS struct{ dynamic.ResourceInterface }

func (hangingNS) List(ctx context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
