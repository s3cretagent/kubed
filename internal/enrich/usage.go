package enrich

import (
	"fmt"
	"time"

	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// UsageSource is the read side of kube.Metrics.
type UsageSource interface {
	Available() bool
	Pod(ns, name string) (kube.Usage, bool)
	Node(name string) (kube.Usage, bool)
	Peak(ns, name string) (kube.Usage, time.Time, bool)
}

// SetUsage attaches live CPU/memory readings (nil = no metrics).
func (e *Enricher) SetUsage(u UsageSource) { e.usage = u }

// PodUse is a pod's usage next to what it asked for.
type PodUse struct {
	Use            kube.Usage
	Known          bool // metrics-server has a reading for this pod
	ReqCPU, LimCPU int64
	ReqMem, LimMem int64
}

// PodUsage sums the pod's container requests/limits and pairs them with
// its current usage.
func (e *Enricher) PodUsage(pod *unstructured.Unstructured) PodUse {
	var p PodUse
	containers, _, _ := unstructured.NestedSlice(pod.Object, "spec", "containers")
	// Helpers that run alongside (init containers with restartPolicy
	// Always) reserve their share for the pod's whole life, as kubectl
	// counts them.
	inits, _, _ := unstructured.NestedSlice(pod.Object, "spec", "initContainers")
	for _, c := range inits {
		if cm, ok := c.(map[string]any); ok && cm["restartPolicy"] == "Always" {
			containers = append(containers, c)
		}
	}
	for _, c := range containers {
		cm, _ := c.(map[string]any)
		p.ReqCPU += quantity(cm, true, "resources", "requests", "cpu")
		p.LimCPU += quantity(cm, true, "resources", "limits", "cpu")
		p.ReqMem += quantity(cm, false, "resources", "requests", "memory")
		p.LimMem += quantity(cm, false, "resources", "limits", "memory")
	}
	if e.usage != nil {
		p.Use, p.Known = e.usage.Pod(pod.GetNamespace(), pod.GetName())
	}
	return p
}

// NodeUse is a node's usage next to its allocatable capacity.
type NodeUse struct {
	Use                kube.Usage
	Known              bool
	AllocCPU, AllocMem int64
}

// NodeUsage pairs a node's usage with its allocatable resources.
func (e *Enricher) NodeUsage(node *unstructured.Unstructured) NodeUse {
	n := NodeUse{
		AllocCPU: quantity(node.Object, true, "status", "allocatable", "cpu"),
		AllocMem: quantity(node.Object, false, "status", "allocatable", "memory"),
	}
	if e.usage != nil {
		n.Use, n.Known = e.usage.Node(node.GetName())
	}
	return n
}

// Cluster summarises every node for the header.
type Cluster struct {
	Nodes          int
	Hourly         float64
	Unpriced       int
	Region         string // most common node region
	CPUPct, MemPct int
	UsageKnown     bool
}

// ClusterSummary adds up nodes, cost and usage.
func (e *Enricher) ClusterSummary() Cluster {
	var c Cluster
	regions := map[string]int{}
	var useCPU, useMem, allocCPU, allocMem int64
	for _, n := range e.list(kube.Nodes, "") {
		c.Nodes++
		info := e.Node(n)
		if info.PriceKnown {
			c.Hourly += info.Hourly
		} else {
			c.Unpriced++
		}
		if info.Region != "" {
			regions[info.Region]++
		}
		if u := e.NodeUsage(n); u.Known {
			useCPU, useMem = useCPU+u.Use.CPUMilli, useMem+u.Use.MemBytes
			allocCPU, allocMem = allocCPU+u.AllocCPU, allocMem+u.AllocMem
		}
	}
	for r, count := range regions {
		if count > regions[c.Region] {
			c.Region = r
		}
	}
	if allocCPU > 0 && allocMem > 0 {
		c.CPUPct, c.MemPct, c.UsageKnown = Pct(useCPU, allocCPU), Pct(useMem, allocMem), true
	}
	return c
}

// Pct is 100*used/total rounded down; -1 when total is unknown.
func Pct(used, total int64) int {
	if total <= 0 {
		return -1
	}
	return int(used * 100 / total)
}

// quantity reads a Kubernetes quantity string at path; milli selects
// millicores (CPU) instead of whole units (bytes).
func quantity(obj map[string]any, milli bool, path ...string) int64 {
	s, found, _ := unstructured.NestedString(obj, path...)
	if !found || s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	if milli {
		return q.MilliValue()
	}
	return q.Value()
}

// Headroom on top of the observed peak when suggesting requests.
const (
	headroom = 1.3
	minCPU   = 50       // millicores: below this, a busy moment has no room
	minMem   = 64 << 20 // bytes
)

// Waste is how much a workload asks for beyond what it uses.
type Waste struct {
	Workload               *unstructured.Unstructured // Deployment, StatefulSet… (or the pod)
	Pods                   int
	ReqCPU                 int64 // per pod, millicores
	PeakCPU                int64
	ReqMem                 int64 // per pod, bytes
	PeakMem                int64
	SuggestCPU, SuggestMem int64
	Monthly                float64 // ≈ $ saved per month across its pods
	Since                  time.Time
	// RanOutOfMemory is when one of its containers was last killed for
	// running out of memory (in the last week): never a reason to cut it.
	RanOutOfMemory time.Time
}

// oomWindow is how long a kill for running out of memory keeps a
// workload's memory off the list of things to cut.
const oomWindow = 7 * 24 * time.Hour

// lastOutOfMemory is when a pod's container was last killed for running
// out of memory, as its last termination says.
func lastOutOfMemory(p *unstructured.Unstructured) time.Time {
	var last time.Time
	for _, field := range []string{"containerStatuses", "initContainerStatuses"} {
		list, _, _ := unstructured.NestedSlice(p.Object, "status", field)
		for _, c := range list {
			m, _ := c.(map[string]any)
			// Its state now (killed, waiting to restart) and the one before.
			for _, which := range []string{"state", "lastState"} {
				t, _, _ := unstructured.NestedMap(m, which, "terminated")
				if t == nil || t["reason"] != "OOMKilled" {
					continue
				}
				if at, err := time.Parse(time.RFC3339, fmt.Sprint(t["finishedAt"])); err == nil && at.After(last) {
					last = at
				}
			}
		}
	}
	return last
}

// Wastes lists workloads whose requests exceed peak usage + headroom,
// most money first. Pods without usage readings are skipped.
func (e *Enricher) Wastes(pods []*unstructured.Unstructured) []Waste {
	if e.usage == nil {
		return nil
	}
	byWorkload := map[string]*Waste{}
	var order []string
	// Money is worked out per node: what the pods there could give back,
	// as a share of the node (the larger of CPU and memory), at most the
	// whole node. Each workload gets its part of that.
	type give struct {
		w        *Waste
		cpu, mem int64
	}
	type nodeGive struct {
		hourly             float64
		allocCPU, allocMem int64
		freeCPU, freeMem   int64
		gives              []give
	}
	nodes := map[string]*nodeGive{}
	for _, p := range pods {
		if str(p, "status", "phase") != "Running" {
			continue
		}
		peak, since, ok := e.usage.Peak(p.GetNamespace(), p.GetName())
		if !ok || peak.MemBytes == 0 { // no real reading yet: no advice
			continue
		}
		req := e.PodUsage(p)
		top := e.TopOwner(p)
		if top.GetKind() == "DaemonSet" && p.GetNamespace() == "kube-system" {
			continue // the cluster's own agents (kube-proxy…): sized by their maker, not by you
		}
		key := top.GetKind() + "/" + top.GetNamespace() + "/" + top.GetName()
		w, seen := byWorkload[key]
		if !seen {
			w = &Waste{Workload: top, Since: since}
			byWorkload[key] = w
			order = append(order, key)
		}
		w.Pods++
		oom := lastOutOfMemory(p)
		if time.Since(oom) < oomWindow && oom.After(w.RanOutOfMemory) {
			w.RanOutOfMemory = oom
		}
		w.ReqCPU, w.ReqMem = max(w.ReqCPU, req.ReqCPU), max(w.ReqMem, req.ReqMem)
		w.PeakCPU, w.PeakMem = max(w.PeakCPU, peak.CPUMilli), max(w.PeakMem, peak.MemBytes)
		w.SuggestCPU = max(int64(float64(w.PeakCPU)*headroom), minCPU)
		w.SuggestMem = max(int64(float64(w.PeakMem)*headroom), minMem)
		// This list is about giving back: never suggest more than it has.
		if w.ReqCPU > 0 {
			w.SuggestCPU = min(w.SuggestCPU, w.ReqCPU)
		}
		if w.ReqMem > 0 {
			w.SuggestMem = min(w.SuggestMem, w.ReqMem)
		}
		if !w.RanOutOfMemory.IsZero() && w.ReqMem > 0 {
			w.SuggestMem = w.ReqMem // it ran out of memory: never suggest less
		}

		name, node, ok := e.PodNode(p)
		if !ok || !node.PriceKnown {
			continue
		}
		ng := nodes[name]
		if ng == nil {
			nodeObj, found := e.NodeObject(name)
			if !found {
				continue
			}
			alloc := e.NodeUsage(nodeObj)
			ng = &nodeGive{hourly: node.Hourly, allocCPU: alloc.AllocCPU, allocMem: alloc.AllocMem}
			nodes[name] = ng
		}
		g := give{w: w,
			cpu: max(req.ReqCPU-max(int64(float64(peak.CPUMilli)*headroom), minCPU), 0),
			mem: max(req.ReqMem-max(int64(float64(peak.MemBytes)*headroom), minMem), 0)}
		if !w.RanOutOfMemory.IsZero() {
			g.mem = 0 // no money in memory it needs
		}
		ng.freeCPU, ng.freeMem = ng.freeCPU+g.cpu, ng.freeMem+g.mem
		ng.gives = append(ng.gives, g)
	}
	for _, ng := range nodes {
		frac := func(part, of int64) float64 {
			if of <= 0 {
				return 0
			}
			return float64(part) / float64(of)
		}
		nodeShare := min(max(frac(ng.freeCPU, ng.allocCPU), frac(ng.freeMem, ng.allocMem)), 1)
		var weights float64
		for _, g := range ng.gives {
			weights += max(frac(g.cpu, ng.allocCPU), frac(g.mem, ng.allocMem))
		}
		if weights == 0 {
			continue
		}
		for _, g := range ng.gives {
			wt := max(frac(g.cpu, ng.allocCPU), frac(g.mem, ng.allocMem))
			g.w.Monthly += ng.hourly * nodeShare * (wt / weights) * hoursPerMonth
		}
	}
	var out []Waste
	for _, k := range order {
		w := byWorkload[k]
		// Room to give back, or short of memory (a risk the list also shows).
		if w.ReqCPU > w.SuggestCPU || w.ReqMem > w.SuggestMem || (w.ReqMem > 0 && w.PeakMem > w.ReqMem) || !w.RanOutOfMemory.IsZero() {
			out = append(out, *w)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Monthly > out[j].Monthly })
	return out
}

const hoursPerMonth = 730
