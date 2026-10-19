package view

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

const hoursPerMonth = 730

func init() {
	register(kube.Nodes, View{
		Columns: []string{"NAME", "STATUS", "NODEPOOL", "TYPE", "PRICING", "ZONE", "PODS", "CPU", "CPU %", "MEM", "MEM %", "COST/MO", "AGE"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			n := e.Node(o)
			u := e.NodeUsage(o)
			status, tone := nodeStatus(o)
			if busy := NodeBusy(e, o); tone == ToneNormal && busy != "" {
				switch {
				case strings.Contains(busy, "memory ") && strings.Contains(busy, "of what it has"):
					// Memory can't be squeezed: past it, pods are pushed off.
					status, tone = "Nearly full", ToneWarn
					if u.Known && enrich.Pct(u.Use.MemBytes, u.AllocMem) >= 100 {
						status = "Full"
					}
				case strings.Contains(busy, "of what it has"):
					// Only CPU: busy, its pods slow down, nothing is pushed off
					// (and one reading jumps around); not a warning.
					status = "Busy"
				case strings.Contains(busy, "no room for new pods"):
					status = "No pod room" // the machine is fine; its pod places are used up, normal when packed
				case strings.Contains(busy, "pod places"):
					status = "Few pod places" // nearly all taken, a few left
				default: // only reserved: normal when an autoscaler packs nodes
					status = "Packed"
				}
			}
			return []string{ShortNode(o.GetName()), status, enrich.Or(n.Pool), enrich.Or(n.InstanceType), enrich.Or(n.Capacity),
				enrich.Or(n.Zone), fmt.Sprint(e.PodsOnNode(o.GetName())),
				cpu(u.Use.CPUMilli, u.Known), pctSign(pct(u.Use.CPUMilli, u.AllocCPU, u.Known)),
				mem(u.Use.MemBytes, u.Known), pctSign(pct(u.Use.MemBytes, u.AllocMem, u.Known)),
				monthly(n.Hourly, n.PriceKnown), age(o)}, tone
		},
		Detail:   nodeDetail,
		Optional: []string{"AGE", "ZONE", "PRICING", "NODEPOOL", "CPU", "MEM", "PODS"}, // NAME and STATUS stay
		Drill: func(o *unstructured.Unstructured) *Drill {
			name := o.GetName()
			return &Drill{GVR: kube.Pods, Title: "on node " + ShortNode(name) + " (all namespaces)", AllNamespaces: true, Match: func(p *unstructured.Unstructured) bool {
				return str(p, "spec", "nodeName") == name
			}}
		},
		Needs: []schema.GroupVersionResource{kube.Pods, kube.PDBs}, // budgets: D names the ones that hold a drain
		Caps:  CapCordon,
	})
}

// NodeBusy says what a node is nearly out of (90% or more): CPU, memory
// or room for pods; "" when it has room.
func NodeBusy(e *enrich.Enricher, o *unstructured.Unstructured) string {
	var out []string
	u := e.NodeUsage(o)
	if u.Known && u.AllocCPU > 0 && enrich.Pct(u.Use.CPUMilli, u.AllocCPU) >= 90 {
		out = append(out, fmt.Sprintf("CPU %d%% of what it has, so its pods may run slow", enrich.Pct(u.Use.CPUMilli, u.AllocCPU)))
	}
	if u.Known && u.AllocMem > 0 && enrich.Pct(u.Use.MemBytes, u.AllocMem) >= 90 {
		out = append(out, fmt.Sprintf("memory %d%% of what it has, so pods may be pushed off", enrich.Pct(u.Use.MemBytes, u.AllocMem)))
	}
	// What the scheduler counts: what pods reserve, not what they use.
	if rc, rm := e.NodeReserved(o.GetName()); u.AllocCPU > 0 && u.AllocMem > 0 {
		if p := enrich.Pct(rc, u.AllocCPU); p >= 90 {
			out = append(out, fmt.Sprintf("its pods reserve %d%% of its CPU, so new pods may not fit", p))
		}
		if p := enrich.Pct(rm, u.AllocMem); p >= 90 {
			out = append(out, fmt.Sprintf("its pods reserve %d%% of its memory, so new pods may not fit", p))
		}
	}
	limit := num(o, "status", "allocatable", "pods")
	if q, err := resource.ParseQuantity(str(o, "status", "allocatable", "pods")); limit == 0 && err == nil {
		limit = q.Value() // usually written as text: "29"
	}
	if pods := int64(e.PodsOnNode(o.GetName())); limit > 0 && pods*10 >= limit*9 {
		if pods >= limit {
			out = append(out, fmt.Sprintf("all %d pod places taken, so no room for new pods", limit))
		} else {
			out = append(out, fmt.Sprintf("%d of its %d pod places taken: room for %s", pods, limit, plural(int(limit-pods), "more pod")))
		}
	}
	return strings.Join(out, "; ")
}

func nodeStatus(o *unstructured.Unstructured) (string, Tone) {
	status, tone := "Not ready", ToneBad
	if c, ok := enrich.Condition(o, "Ready"); ok && c.Status == "True" {
		status, tone = "Ready", ToneNormal
	}
	if unsched, _, _ := unstructured.NestedBool(o.Object, "spec", "unschedulable"); unsched {
		status += ", cordoned"
		tone = ToneWarn
	}
	return status, tone
}

func nodeDetail(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	n := e.Node(o)
	machine := []string{enrich.Or(n.InstanceType)}
	if n.Capacity != "" {
		machine = append(machine, n.Capacity)
	}
	if n.Pool != "" {
		machine = append(machine, "pool "+n.Pool)
	}
	if n.Zone != "" {
		machine = append(machine, n.Zone)
	}
	lines := []string{"Machine  " + strings.Join(machine, " · ")}
	if busy := NodeBusy(e, o); busy != "" {
		if !strings.Contains(busy, "of what it has") && !strings.Contains(busy, "pod places") {
			busy += " (usual when an autoscaler packs nodes; it adds one when a pod can't fit)"
		}
		lines = append([]string{"Room     " + busy}, lines...)
	}
	// Not ready: why first, in the node's own words (it may be leaving).
	if c, ok := enrich.Condition(o, "Ready"); ok && c.Status != "True" {
		why := strings.Join(strings.Fields(c.Message), " ")
		if why == "" {
			why = c.Reason
		}
		if o.GetDeletionTimestamp() != nil {
			why = "it is being removed; " + why
		}
		lines = append([]string{"Problem  not ready: " + enrich.Or(why)}, lines...)
	}
	switch {
	case n.PriceKnown:
		lines = append(lines, fmt.Sprintf("Cost     $%.3f/hr ≈ $%.0f/month (%s)", n.Hourly, n.Hourly*hoursPerMonth, e.PriceSource(n)))
	case e.Costs(): // costs are on, but this type has no price
		lines = append(lines, "Cost     no price known for "+enrich.Or(n.InstanceType)+" in "+enrich.Or(n.Region))
	}
	u := e.NodeUsage(o)
	lines = append(lines, fmt.Sprintf("Usable   %s · %s memory · up to %s pods (what pods can use, after the system's share)",
		cores(u.AllocCPU), gibibytes(u.AllocMem), str(o, "status", "allocatable", "pods")))

	taints, _, _ := unstructured.NestedSlice(o.Object, "spec", "taints")
	var ts []string
	for _, t := range taints {
		if m, ok := t.(map[string]any); ok {
			effect := map[string]string{"NoSchedule": "others can't start here", "PreferNoSchedule": "others start here only if nothing else fits",
				"NoExecute": "others are moved off"}[fmt.Sprint(m["effect"])]
			ts = append(ts, fmt.Sprintf("%v=%v (%s)", m["key"], valueOr(m["value"]), enrich.Or(effect)))
		}
	}
	if len(ts) > 0 {
		lines = append(lines, "Taints   only pods that tolerate "+strings.Join(ts, ", "))
	}

	pressed, reported := false, true
	for _, t := range []string{"MemoryPressure", "DiskPressure", "PIDPressure"} {
		if _, ok := enrich.Condition(o, t); !ok {
			reported = false
		}
	}
	words := map[string]string{"MemoryPressure": "low on memory", "DiskPressure": "low on disk", "PIDPressure": "too many processes", "NetworkUnavailable": "network not set up"}
	for _, t := range nodePressure {
		if c, ok := enrich.Condition(o, t); ok && c.Status == "True" {
			lines = append(lines, fmt.Sprintf("Problem  %s: %s", words[t], c.Message))
			pressed = true
		}
	}
	switch {
	case pressed:
	case !reported && u.Known && u.AllocMem > 0 && enrich.Pct(u.Use.MemBytes, u.AllocMem) >= 90:
		// Room says it is nearly full and its kubelet reports no pressure
		// conditions: don't call it "enough". Reported False, it is none.
		lines = append(lines, "Pressure not reported yet: Kubernetes only reports it once it starts pushing pods off")
	default:
		lines = append(lines, "Pressure none: enough memory, disk and processes")
	}
	return lines
}

// nodePressure are the node conditions where True means trouble. Others
// (e.g. EKS node-monitoring's KernelReady) are True when healthy.
var nodePressure = []string{"MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"}

func valueOr(v any) any {
	if v == nil {
		return ""
	}
	return v
}
