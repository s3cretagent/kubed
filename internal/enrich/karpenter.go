package enrich

import (
	"cmp"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Node labels set by Karpenter, EKS managed node groups and the kubelet.
const (
	labelNodePool     = "karpenter.sh/nodepool"
	labelKarpenterCap = "karpenter.sh/capacity-type"
	labelNodeGroup    = "eks.amazonaws.com/nodegroup"
	labelNodeGroupCap = "eks.amazonaws.com/capacityType"
	labelInstanceType = "node.kubernetes.io/instance-type"
	labelZone         = "topology.kubernetes.io/zone"
	labelRegion       = "topology.kubernetes.io/region"
	capacityOnDemand  = "on-demand"
	capacitySpot      = "spot"
	labelZoneID       = "topology.k8s.aws/zone-id"
	nodeGroupPrefix   = "ng:"
	unknown           = "-"
)

// NodeInfo is what kubed knows about the machine behind a node.
type NodeInfo struct {
	Pool         string // Karpenter NodePool, "ng:<name>" for a managed node group
	InstanceType string
	Capacity     string // "spot" or "on-demand"
	Zone         string
	ZoneID       string // same physical zone in every account, unlike Zone
	Region       string
	Hourly       float64
	PriceKnown   bool
}

// Node reads the node's labels and prices it.
func (e *Enricher) Node(node *unstructured.Unstructured) NodeInfo {
	l := node.GetLabels()
	info := NodeInfo{
		Pool:         l[labelNodePool],
		InstanceType: l[labelInstanceType],
		Capacity:     l[labelKarpenterCap],
		Zone:         l[labelZone],
		ZoneID:       l[labelZoneID],
		Region:       l[labelRegion],
	}
	if info.Pool == "" && l[labelNodeGroup] != "" {
		info.Pool = nodeGroupPrefix + l[labelNodeGroup]
	}
	if info.Capacity == "" {
		// Managed node groups use SPOT / ON_DEMAND.
		info.Capacity = strings.ReplaceAll(strings.ToLower(l[labelNodeGroupCap]), "_", "-")
	}
	if info.Capacity == "" && info.InstanceType != "" {
		info.Capacity = capacityOnDemand
	}
	zone := cmp.Or(info.ZoneID, info.Zone)
	if e.costs {
		info.Hourly, info.PriceKnown = e.price(info.Region, info.InstanceType, zone, info.Capacity)
	}
	return info
}

// price is what the node costs this account per hour: the live spot price
// in its zone, or the on-demand rate on the account's bill, else the list
// price less the account's discount (Savings Plans / RIs cover on-demand
// only).
func (e *Enricher) price(region, instanceType, zone, capacity string) (float64, bool) {
	if capacity == capacitySpot {
		if p, ok := e.spot[instanceType+"/"+zone]; ok {
			return p, true
		}
		return e.prices.Hourly(region, instanceType, capacity)
	}
	if p, ok := e.billed[instanceType]; ok {
		return p, true
	}
	list, ok := e.prices.Hourly(region, instanceType, capacity)
	return list * (1 - e.Discount()/100), ok
}

// PriceSource says where a node's price comes from, in words.
func (e *Enricher) PriceSource(info NodeInfo) string {
	zone := cmp.Or(info.ZoneID, info.Zone)
	switch {
	case info.Capacity == capacitySpot && e.spot != nil && e.spot[info.InstanceType+"/"+zone] > 0:
		return "live spot price in its zone"
	case info.Capacity == capacitySpot:
		return "recent average spot price"
	case e.billed[info.InstanceType] > 0:
		return "what your account paid for this type last week"
	case e.Discount() > 0:
		return "list price less your account's discount"
	}
	return "AWS list price"
}

// SpotTypes are the instance types of spot nodes, for a live price lookup.
func (e *Enricher) SpotTypes() []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range e.list(kube.Nodes, "") {
		info := e.Node(n)
		if info.Capacity == capacitySpot && info.InstanceType != "" && !seen[info.InstanceType] {
			seen[info.InstanceType] = true
			out = append(out, info.InstanceType)
		}
	}
	sort.Strings(out)
	return out
}

// PodNode returns info for the node a pod runs on.
func (e *Enricher) PodNode(pod *unstructured.Unstructured) (string, NodeInfo, bool) {
	name, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName")
	if name == "" {
		return "", NodeInfo{}, false
	}
	node, ok := e.get(kube.Nodes, "", name)
	if !ok {
		return name, NodeInfo{}, false
	}
	return name, e.Node(node), true
}

// PoolCost sums the hourly price of every node in each NodePool.
func (e *Enricher) PoolCost() map[string]PoolTotal {
	out := map[string]PoolTotal{}
	for _, n := range e.list(kube.Nodes, "") {
		info := e.Node(n)
		if info.Pool == "" {
			continue
		}
		t := out[info.Pool]
		t.Nodes++
		if info.PriceKnown {
			t.Hourly += info.Hourly
		} else {
			t.Unpriced++
		}
		out[info.Pool] = t
	}
	return out
}

// PoolTotal aggregates a NodePool's nodes.
type PoolTotal struct {
	Nodes    int
	Unpriced int
	Hourly   float64
}

// Or returns s, or a dash when s is empty.
func Or(s string) string {
	if s == "" {
		return unknown
	}
	return s
}
