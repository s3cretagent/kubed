package view

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Lists that are not a Kubernetes resource: a pod's containers, the home
// dashboard and Helm releases. Each row's Obj carries the name (and
// namespace) the UI acts on.

// Named wraps a name so these lists reuse the table code.
func Named(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

// ContainerHeader matches ContainerRows.
var ContainerHeader = []string{"NAME", "ROLE", "IMAGE", "STATE", "READY", "RESTARTS", "LAST STOP", "CPU RESERVED/MAX", "MEMORY RESERVED/MAX", "PORTS"}

// ContainerOptional hide first on narrow terminals.
var ContainerOptional = []string{"PORTS", "CPU RESERVED/MAX", "ROLE", "LAST STOP", "MEMORY RESERVED/MAX"}

// ContainerRows lists a pod's containers: init, sidecars, then app ones.
func ContainerRows(e *enrich.Enricher, o *unstructured.Unstructured) []Row {
	p, err := ToPod(o)
	if err != nil {
		return nil
	}
	status := map[string]corev1.ContainerStatus{}
	for _, s := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		status[s.Name] = s
	}
	var rows []Row
	add := func(c corev1.Container, kind string) {
		s := status[c.Name]
		state, tone := containerState(s)
		ready := "no"
		if s.Ready {
			ready = "yes"
		}
		var ports []string
		for _, port := range c.Ports {
			ports = append(ports, fmt.Sprintf("%d/%s", port.ContainerPort, port.Protocol))
		}
		lastExit := ""
		if t := newestStop(s); t != nil {
			lastExit = stopText(e, p, c.Name, t) + agoText(t.FinishedAt.Time)
		}
		cpuOf := func(q interface{ MilliValue() int64 }) string {
			if v := q.MilliValue(); v > 0 {
				return Cores(v)
			}
			return "-"
		}
		memOf := func(q interface{ Value() int64 }) string {
			if v := q.Value(); v > 0 {
				return mem(v, true)
			}
			return "-"
		}
		rows = append(rows, Row{Obj: Named(p.Namespace, c.Name), Tone: tone, Cells: []string{
			c.Name, kind, shortImage(c.Image), state, ready, fmt.Sprint(s.RestartCount), lastExit,
			cpuOf(c.Resources.Requests.Cpu()) + " / " + cpuOf(c.Resources.Limits.Cpu()),
			memOf(c.Resources.Requests.Memory()) + " / " + memOf(c.Resources.Limits.Memory()),
			joinOr(ports, ","),
		}})
	}
	for _, c := range p.Spec.InitContainers {
		kind := "runs first"
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			kind = "helper"
		}
		add(c, kind)
	}
	for _, c := range p.Spec.Containers {
		add(c, "main")
	}
	return rows
}

func containerState(s corev1.ContainerStatus) (string, Tone) {
	switch {
	case s.State.Running != nil:
		if s.Ready {
			return "Running", ToneNormal
		}
		return "Running", ToneWarn
	case s.State.Waiting != nil:
		return s.State.Waiting.Reason, ToneBad
	case s.State.Terminated != nil:
		if s.State.Terminated.ExitCode == 0 {
			return "Completed", ToneDim
		}
		return s.State.Terminated.Reason, ToneBad
	}
	return "-", ToneWarn
}

// exitWords says how a container stopped: "ran out of memory".
func exitWords(reason string, code int32) string {
	switch reason {
	case "OOMKilled":
		return "ran out of memory"
	case "Completed":
		return "finished"
	case "Error":
		return fmt.Sprintf("error (exit %d)", code)
	case "StartError", "ContainerCannotRun":
		return "never started (its command or a file it needs is missing or can't run)"
	case "":
		return fmt.Sprintf("exit code %d", code)
	}
	return fmt.Sprintf("%s (exit %d)", reason, code)
}

// HelmHeader matches HelmRows.
var HelmHeader = []string{"NAME", "NAMESPACE", "REVISION", "STATUS", "UPDATED"}

// HelmRows lists Helm releases (latest revision each).
func HelmRows(rels []kube.HelmRelease) []Row {
	rows := make([]Row, 0, len(rels))
	for _, r := range rels {
		tone := ToneNormal
		switch strings.ToLower(r.Status) {
		case "failed":
			tone = ToneBad
		case "pending-install", "pending-upgrade", "pending-rollback", "uninstalling":
			tone = ToneWarn
		case "superseded", "uninstalled":
			tone = ToneDim
		}
		rows = append(rows, Row{Obj: Named(r.Namespace, r.Name), Tone: tone,
			Cells: []string{r.Name, r.Namespace, fmt.Sprint(r.Revision), r.Status, Age(r.Updated)}})
	}
	return rows
}

// HelmTime is when a release row was last updated (for sort by age).
func HelmTime(rels []kube.HelmRelease) func(ns, name string) time.Time {
	idx := map[string]time.Time{}
	for _, r := range rels {
		idx[r.Namespace+"/"+r.Name] = r.Updated
	}
	return func(ns, name string) time.Time { return idx[ns+"/"+name] }
}

// WasteHeader matches WasteRows.
var WasteHeader = []string{"NAME", "KIND", "NAMESPACE", "PODS", "CPU RESERVED", "CPU PEAK", "MEM RESERVED", "MEM PEAK", "INSTEAD (CPU / MEM)", "SAVES/MO"}

// askedOr shows a request, or "not set" when there is none.
func askedOr(req int64, format func(int64, bool) string) string {
	if req <= 0 {
		return "not set"
	}
	return format(req, true)
}

// askFor is the request to suggest: lower when it asks for more than it
// uses, otherwise what it has now (this list is about giving back).
func askFor(suggest, req int64, format func(int64, bool) string) string {
	if req <= 0 { // nothing reserved: what to reserve, as the box says
		return "set " + format(suggest, true)
	}
	if suggest >= req { // nothing to give back here
		return "keep"
	}
	return format(suggest, true)
}

// WasteAdvice says, in words, what to reserve instead: the same choice the
// list's INSTEAD column shows, for the details box.
func WasteAdvice(w enrich.Waste) string {
	part := func(peak, req, suggest int64, raise string, words func(int64) string, floor int64) string {
		switch {
		case req <= 0: // nothing reserved yet: what to reserve, as the INSTEAD column says
			return "reserve " + words(suggest) + " (none is set now)"
		case req > 0 && peak > req:
			return raise + " " + words(peak*13/10) + " (it already uses more than it reserves)"
		case req > 0 && suggest >= req:
			return "keep " + words(req)
		case peak*13/10 < floor:
			return words(suggest) + " (kubed's smallest suggestion)"
		}
		return words(suggest)
	}
	memory := part(w.PeakMem, w.ReqMem, w.SuggestMem, "raise memory to", Mem, 64<<20)
	if !w.RanOutOfMemory.IsZero() {
		memory = "don't cut memory: it ran out of memory " + Age(w.RanOutOfMemory) + " ago"
	}
	return part(w.PeakCPU, w.ReqCPU, w.SuggestCPU, "raise CPU to", Cores, 50) + "; " + memory
}

// peakMem is the most memory seen, or "all of it" when it was killed for
// running out: readings miss the moment it hit the limit.
func peakMem(w enrich.Waste) string {
	if !w.RanOutOfMemory.IsZero() {
		return "all of it"
	}
	return mem(w.PeakMem, true)
}

// cpuAdvice is the CPU part: less, keep, or more when it already uses more
// than it reserves.
func cpuAdvice(w enrich.Waste) string {
	if w.ReqCPU > 0 && w.PeakCPU > w.ReqCPU {
		return "more: " + cpu(w.PeakCPU*13/10, true)
	}
	return askFor(w.SuggestCPU, w.ReqCPU, cpu)
}

// memAdvice is the memory part of RESERVE INSTEAD: less, keep, or raise
// when it already uses more than it reserves.
func memAdvice(w enrich.Waste) string {
	if !w.RanOutOfMemory.IsZero() {
		return "keep: ran out " + Age(w.RanOutOfMemory) + " ago"
	}
	if w.ReqMem > 0 && w.PeakMem > w.ReqMem {
		return "more: " + mem(w.PeakMem*13/10, true)
	}
	return askFor(w.SuggestMem, w.ReqMem, mem)
}

// WasteRows lists over-provisioned workloads, most money first.
// early: too little usage seen to act on; the rows are greyed.
func WasteRows(ws []enrich.Waste, early bool) []Row {
	rows := make([]Row, 0, len(ws))
	for _, w := range ws {
		o := w.Workload
		saved := "-"
		switch short := w.ReqMem > 0 && w.PeakMem > w.ReqMem; {
		case short: // short of memory: it needs more, not a saving
		case w.Monthly >= 1:
			saved = fmt.Sprintf("$%.0f", w.Monthly)
		case w.Monthly > 0:
			saved = "<$1"
		}
		tone := ToneNormal
		switch {
		case w.ReqMem > 0 && w.PeakMem > w.ReqMem, !w.RanOutOfMemory.IsZero(): // a risk, not a saving
			tone = ToneWarn
		case early:
			tone = ToneDim
		}
		rows = append(rows, Row{Obj: o, Tone: tone, Cells: []string{
			o.GetName(), strings.ToLower(o.GetKind()), o.GetNamespace(), fmt.Sprint(w.Pods),
			askedOr(w.ReqCPU, cpu), cpu(w.PeakCPU, true), askedOr(w.ReqMem, mem), peakMem(w),
			cpuAdvice(w) + " / " + memAdvice(w), saved,
		}})
	}
	return rows
}

// ContainerDetail is the box under the containers list: the selected
// container's own state, image, restarts and resources.
func ContainerDetail(e *enrich.Enricher, o *unstructured.Unstructured, name string) []string {
	p, err := ToPod(o)
	if err != nil {
		return nil
	}
	role := "main"
	var spec *corev1.Container
	for i := range p.Spec.InitContainers {
		if p.Spec.InitContainers[i].Name == name {
			spec, role = &p.Spec.InitContainers[i], "runs first"
			if c := p.Spec.InitContainers[i].RestartPolicy; c != nil && *c == corev1.ContainerRestartPolicyAlways {
				role = "helper"
			}
		}
	}
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].Name == name {
			spec = &p.Spec.Containers[i]
		}
	}
	if spec == nil {
		return nil
	}
	var st corev1.ContainerStatus
	for _, s := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		if s.Name == name {
			st = s
		}
	}
	state, _ := containerState(st)
	if t := st.State.Terminated; t != nil {
		state = stopText(e, p, name, t) + ", " + Age(t.FinishedAt.Time) + " ago"
	}
	lines := []string{
		fmt.Sprintf("State    %s (%s container)", state, role),
		"Image    " + spec.Image,
		"Restarts " + fmt.Sprint(st.RestartCount),
	}
	if t := st.LastTerminationState.Terminated; t != nil {
		lines = append(lines, "Stopped  "+stopText(e, p, name, t)+", "+Age(t.FinishedAt.Time)+" ago (p shows its logs from before)")
	}
	res := func(req, lim interface{ IsZero() bool }, f func() string, g func() string) string {
		out := "reserves " + f()
		if req.IsZero() {
			out = "reserves nothing"
		}
		if lim.IsZero() {
			return out + " · no limit"
		}
		return out + " · limit " + g()
	}
	r, l := spec.Resources.Requests, spec.Resources.Limits
	lines = append(lines,
		"Memory   "+res(r.Memory(), l.Memory(), func() string { return mem(r.Memory().Value(), true) }, func() string { return mem(l.Memory().Value(), true) }),
		"CPU      "+res(r.Cpu(), l.Cpu(), func() string { return Cores(r.Cpu().MilliValue()) }, func() string { return Cores(l.Cpu().MilliValue()) }))
	return lines
}

// stopText says how a container stopped: killed by its alive or startup
// check (which Kubernetes records as a plain exit), or as its exit says.
func stopText(e *enrich.Enricher, p *corev1.Pod, container string, t *corev1.ContainerStateTerminated) string {
	if e != nil {
		if check, _, killed := e.KilledByCheck(p.Namespace, p.Name, container, t.FinishedAt.Time, t.ExitCode); killed && t.Reason != "OOMKilled" {
			return "killed by its " + check
		}
	}
	return exitWords(t.Reason, t.ExitCode)
}
