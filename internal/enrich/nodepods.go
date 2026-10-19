package enrich

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// podCountTTL bounds how often pod counts per node are recomputed; the
// nodes view asks once per row on every redraw.
const podCountTTL = time.Second

// PodsOnNode counts non-finished pods scheduled on a node.
func (e *Enricher) PodsOnNode(node string) int {
	if time.Since(e.podCountsAt) > podCountTTL {
		counts, reqs := map[string]int{}, map[string][2]int64{}
		for _, p := range e.list(kube.Pods, "") {
			phase, _, _ := unstructured.NestedString(p.Object, "status", "phase")
			if phase == "Succeeded" || phase == "Failed" {
				continue
			}
			if n, _, _ := unstructured.NestedString(p.Object, "spec", "nodeName"); n != "" {
				counts[n]++
				cpu, mem := podReserved(p)
				r := reqs[n]
				reqs[n] = [2]int64{r[0] + cpu, r[1] + mem}
			}
		}
		e.podCounts, e.podReqs, e.podCountsAt = counts, reqs, time.Now()
	}
	return e.podCounts[node]
}

// NodeReserved is the CPU (millicores) and memory (bytes) the pods on a
// node reserve: what decides whether a new pod still fits there.
func (e *Enricher) NodeReserved(node string) (cpu, mem int64) {
	e.PodsOnNode(node) // fills the cache
	r := e.podReqs[node]
	return r[0], r[1]
}

// podReserved is the CPU (millicores) and memory (bytes) a pod reserves
// on its node, counted as the scheduler counts them: its containers and
// helpers that run alongside (init containers with restartPolicy Always),
// or more while a setup step (another init container) runs next to the
// helpers started before it, plus its overhead. Requests set for the
// whole pod (spec.resources) count instead of its containers'.
func podReserved(pod *unstructured.Unstructured) (cpu, mem int64) {
	req := func(c any) (int64, int64) {
		cm, _ := c.(map[string]any)
		return quantity(cm, true, "resources", "requests", "cpu"), quantity(cm, false, "resources", "requests", "memory")
	}
	var sideCPU, sideMem, initCPU, initMem int64
	inits, _, _ := unstructured.NestedSlice(pod.Object, "spec", "initContainers")
	for _, c := range inits {
		c1, m1 := req(c)
		if cm, ok := c.(map[string]any); ok && cm["restartPolicy"] == "Always" {
			sideCPU, sideMem = sideCPU+c1, sideMem+m1
			c1, m1 = sideCPU, sideMem
		} else {
			c1, m1 = c1+sideCPU, m1+sideMem
		}
		initCPU, initMem = max(initCPU, c1), max(initMem, m1)
	}
	containers, _, _ := unstructured.NestedSlice(pod.Object, "spec", "containers")
	for _, c := range containers {
		c1, m1 := req(c)
		cpu, mem = cpu+c1, mem+m1
	}
	cpu, mem = max(cpu+sideCPU, initCPU), max(mem+sideMem, initMem)
	if v := quantity(pod.Object, true, "spec", "resources", "requests", "cpu"); v > 0 {
		cpu = v
	}
	if v := quantity(pod.Object, false, "spec", "resources", "requests", "memory"); v > 0 {
		mem = v
	}
	return cpu + quantity(pod.Object, true, "spec", "overhead", "cpu"), mem + quantity(pod.Object, false, "spec", "overhead", "memory")
}
