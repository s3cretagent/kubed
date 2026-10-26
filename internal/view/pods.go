package view

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

func init() {
	register(kube.Pods, View{
		Columns:  []string{"NAME", "READY", "STATUS", "RESTARTS", "CPU", "MEM", "LIMIT USED", "NODE", "AGE"},
		Row:      podRow,
		Optional: []string{"NODE", "LIMIT USED", "AGE"},
		Detail:   podDetail,
		Needs: []schema.GroupVersionResource{kube.Nodes, kube.ServiceAccounts, kube.ReplicaSets, kube.Deployments,
			kube.StatefulSets, kube.DaemonSets, kube.Jobs, kube.ExternalSecrets, kube.Applications, kube.Events,
			kube.PDBs}, // budgets: ctrl-d says when one allows none down
		Caps: CapLogs | CapShell | CapPortForward | CapKill | CapXray | CapContainers,
	})
}

// ToPod converts an unstructured object to a typed Pod. Conversions are
// cached by UID + resourceVersion: the table redraws every second, but a
// pod only changes when its resourceVersion does. Callers must not modify
// the returned Pod.
func ToPod(o *unstructured.Unstructured) (*corev1.Pod, error) {
	key, rv := string(o.GetUID()), o.GetResourceVersion()
	if key != "" && rv != "" {
		podCache.mu.Lock()
		c, ok := podCache.m[key]
		podCache.mu.Unlock()
		if ok && c.rv == rv {
			return c.pod, nil
		}
	}
	var p corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &p); err != nil {
		return &p, err
	}
	if key != "" && rv != "" {
		podCache.mu.Lock()
		if len(podCache.m) > maxCachedPods { // deleted pods pile up; start over
			podCache.m = map[string]cachedPod{}
		}
		podCache.m[key] = cachedPod{rv: rv, pod: &p}
		podCache.mu.Unlock()
	}
	return &p, nil
}

// maxCachedPods bounds the conversion cache (entries for deleted pods are
// only dropped when it is cleared).
const maxCachedPods = 50000

type cachedPod struct {
	rv  string
	pod *corev1.Pod
}

var podCache = struct {
	mu sync.Mutex
	m  map[string]cachedPod
}{m: map[string]cachedPod{}}

func podRow(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
	p, err := ToPod(o)
	if err != nil {
		return []string{o.GetName(), "?", "?", "?", "", "", "", "", age(o)}, ToneBad
	}
	ready, total, restarts := 0, runningContainers(p), int32(0)
	var lastRestart time.Time
	for _, cs := range RunningStatuses(p) {
		if cs.Ready {
			ready++
		}
		restarts += cs.RestartCount
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(lastRestart) {
			lastRestart = t.FinishedAt.Time
		}
		// Stopped now, waiting to be started again: that stop is the
		// latest (the about box says the same).
		if t := cs.State.Terminated; t != nil && cs.LastTerminationState.Terminated != nil && t.FinishedAt.After(lastRestart) {
			lastRestart = t.FinishedAt.Time
		}
	}
	if n := SetupRestarts(p); n > 0 { // a setup step failing over and over, as kubectl counts it
		restarts += n
		for _, cs := range p.Status.InitContainerStatuses { // and when it last stopped
			for _, t := range []*corev1.ContainerStateTerminated{cs.LastTerminationState.Terminated, cs.State.Terminated} {
				if t != nil && t.ExitCode != 0 && t.FinishedAt.After(lastRestart) {
					lastRestart = t.FinishedAt.Time
				}
			}
		}
	}
	restartCell := fmt.Sprint(restarts)
	if restarts > 0 && !lastRestart.IsZero() { // as kubectl: "3 (12m ago)"
		restartCell += " (" + Age(lastRestart) + " ago)"
	}
	st := Explain(e, p)
	u := e.PodUsage(o)
	return []string{
		p.Name,
		fmt.Sprintf("%d/%d", ready, total),
		st.Short,
		restartCell,
		cpu(u.Use.CPUMilli, u.Known),
		mem(u.Use.MemBytes, u.Known),
		ofLimit(u.Use.MemBytes, u.LimMem, u.Known),
		ShortNode(p.Spec.NodeName),
		age(o),
	}, st.Tone
}

// ofLimit is memory used as a share of the limit ("48%"): near 100% the
// pod is about to be killed for running out of memory.
func ofLimit(used, limit int64, known bool) string {
	switch {
	case !known:
		return "-"
	case limit <= 0:
		return "no limit"
	}
	return fmt.Sprintf("%d%%", enrich.Pct(used, limit))
}

// ShortNode drops the cloud's DNS suffix from a node name:
// "ip-10-0-1-2.ap-south-1.compute.internal" → "ip-10-0-1-2".
func ShortNode(name string) string {
	for _, suffix := range []string{".compute.internal", ".ec2.internal"} {
		if strings.HasSuffix(name, suffix) {
			if i := strings.IndexByte(name, '.'); i > 0 {
				return name[:i]
			}
		}
	}
	return name
}

// podObject wraps a typed pod for enrichment lookups that only need its
// namespace, name and spec.
func podObject(p *corev1.Pod) *unstructured.Unstructured {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	if err != nil {
		return Named(p.Namespace, p.Name)
	}
	return &unstructured.Unstructured{Object: obj}
}

// AllStatuses are a pod's container statuses, then its init containers'.
func AllStatuses(p *corev1.Pod) []corev1.ContainerStatus {
	return slices.Concat(p.Status.ContainerStatuses, p.Status.InitContainerStatuses)
}

func isSidecar(p *corev1.Pod, name string) bool {
	for _, c := range p.Spec.InitContainers {
		if c.Name == name {
			return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
		}
	}
	return false
}

func podDetail(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	var lines []string
	if p, err := ToPod(o); err == nil {
		st := Explain(e, p)
		if st.Tone == ToneBad || st.Tone == ToneWarn {
			lines = append(lines, fmt.Sprintf("Problem  %s: %s", st.Short, withoutKeyHint(st.Why))) // the Fix line says which key
			top := e.TopOwner(o)
			owner, ownerName := top.GetKind(), top.GetName()
			if ref := metav1.GetControllerOf(p); owner == "Pod" && ref != nil { // what runs it isn't loaded: the owner it names
				owner, ownerName = ref.Kind, ref.Name
			}
			fix := neverStartedInGit(e, o, podFixFor(st, owner))
			if owner == "Node" { // a static pod: the node runs it from a file
				fix = strings.ReplaceAll(fix, " (on its deployment)", " in its static pod manifest on node "+ownerName)
			}
			if job := e.Controller(o); job != nil {
				fix = laterRunFix(e, job, fix)
			}
			if strings.HasPrefix(st.Short, "Can't pull image") && staleCronRun(e, o, p) {
				fix = readOnlyFix("its cron job runs another image now; this run still waits on the old one and never starts: ctrl-d deletes it")
			}
			if sts := e.Controller(o); sts != nil && st.Tone == ToneBad && waitsOnPod(sts, p) { // the app box's case: its set runs a good version now
				fix = "its stateful set now runs another version, but Kubernetes won't replace this pod by itself: delete it (ctrl-d) so it is made again with the current version"
				if ReadOnly {
					fix = "its stateful set now runs another version, but Kubernetes won't replace this pod by itself: delete it (without --readonly) so it is made again with the current version"
				}
			}
			if fix != "" {
				lines = append(lines, "Fix      "+fix)
			}
		} else {
			lines = append(lines, fmt.Sprintf("Status   %s: %s", st.Short, st.Why))
		}
		// A pod that ended never starts its containers again.
		ended := p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded
		for _, cs := range RunningStatuses(p) { // a native sidecar's restarts too, as RESTARTS counts them
			if l := lastTermination(cs, ended); l != "" {
				if t := newestStop(cs); t != nil {
					if check, _, ok := e.KilledByCheck(p.Namespace, p.Name, cs.Name, t.FinishedAt.Time, t.ExitCode); ok {
						// "finished" or "stopped": it was killed for failing its check
						l = fmt.Sprintf("%s container %q was killed by its %s%s", stopLabel(ended), cs.Name, check, agoText(t.FinishedAt.Time))
					}
				}
				lines = append(lines, l)
			}
		}
	}
	if u := e.PodUsage(o); u.Known {
		// Use, what it reserves (requests) and its limit, for both.
		amounts := func(used, req, lim int64, f func(int64) string) string {
			out := f(used) + " used"
			if req > 0 {
				out += " · reserves " + f(req)
			}
			if lim > 0 {
				out += fmt.Sprintf(" · limit %s (%d%% used)", f(lim), used*100/lim)
			} else {
				out += " · no limit"
			}
			return out
		}
		lines = append(lines,
			"Memory   "+amounts(u.Use.MemBytes, u.ReqMem, u.LimMem, Mem),
			"CPU      "+amounts(u.Use.CPUMilli, u.ReqCPU, u.LimCPU, Cores))
	}

	if name, n, ok := e.PodNode(o); ok {
		parts := []string{ShortNode(name)}
		if n.InstanceType != "" {
			parts = append(parts, strings.TrimSpace(n.InstanceType+" "+n.Capacity))
		}
		if n.Zone != "" {
			parts = append(parts, n.Zone)
		}
		if n.PriceKnown {
			parts = append(parts, monthly(n.Hourly, true)+"/month")
		}
		lines = append(lines, "Node     "+strings.Join(parts, " · "))
	} else if name != "" {
		lines = append(lines, "Node     "+ShortNode(name))
	} else {
		lines = append(lines, "Node     not placed on a node yet")
	}

	sa, role, via := e.PodRole(o)
	if role != "" {
		lines = append(lines, fmt.Sprintf("AWS      role %s (%s)", roleName(role), via))
	} else {
		lines = append(lines, fmt.Sprintf("AWS      no IAM role (service account %s)", sa))
	}

	lines = append(lines, argoLine(e, o))
	for _, s := range e.ExternalSecretsForPod(o) {
		lines = append(lines, secretLine(s))
	}
	return lines
}

// lastTermination explains why a container last restarted (e.g. OOMKilled).
// In a pod that ended (ended), nothing restarts: it says how it stopped.
func lastTermination(cs corev1.ContainerStatus, ended bool) string {
	t := cs.LastTerminationState.Terminated
	waiting := ""
	if now := cs.State.Terminated; now != nil && t != nil && now.FinishedAt.After(t.FinishedAt.Time) {
		// Stopped now after earlier restarts, waiting to be started again:
		// this stop is the latest. (A finished job's pod has none before.)
		t, waiting = now, "; waiting to restart"
	}
	if t == nil {
		return ""
	}
	if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
		waiting = "; waiting to restart" // Kubernetes waits longer before each try
	}
	how := fmt.Sprintf("stopped (%s, exit %d)", t.Reason, t.ExitCode)
	switch t.Reason {
	case "OOMKilled":
		how = "ran out of memory and was killed"
	case "Error":
		how = fmt.Sprintf("stopped with an error (exit %d)", t.ExitCode)
	case "Completed":
		how = "finished and was started again"
		if waiting != "" { // not started again yet
			how, waiting = "finished", "; waits to be started again"
		}
	case "":
		how = fmt.Sprintf("stopped (exit %d)", t.ExitCode)
	}
	if ended {
		if t.Reason == "Completed" {
			how = "finished"
		}
		waiting = ""
	}
	return fmt.Sprintf("%s container %q %s%s%s", stopLabel(ended), cs.Name, how, agoText(t.FinishedAt.Time), waiting)
}

// stopLabel heads a container's last stop: a restart, unless the pod ended.
func stopLabel(ended bool) string {
	if ended {
		return "Ended   "
	}
	return "Restart "
}

// agoText is ", 5m ago", or nothing when the time isn't known.
func agoText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return ", " + Age(t) + " ago"
}

// Containers lists a pod's container names (regular first, then init).
func Containers(o *unstructured.Unstructured) []string {
	p, err := ToPod(o)
	if err != nil {
		return nil
	}
	var names []string
	for _, c := range p.Spec.Containers {
		names = append(names, c.Name)
	}
	for _, c := range p.Spec.InitContainers {
		names = append(names, c.Name)
	}
	return names
}

// ContainerRestarts is how often each container has restarted.
func ContainerRestarts(o *unstructured.Unstructured) map[string]int32 {
	out := map[string]int32{}
	p, err := ToPod(o)
	if err != nil {
		return out
	}
	for _, cs := range AllStatuses(p) {
		out[cs.Name] = cs.RestartCount
	}
	return out
}

// RunningContainers are the containers running right now.
func RunningContainers(o *unstructured.Unstructured) map[string]bool {
	out := map[string]bool{}
	if p, err := ToPod(o); err == nil {
		for _, cs := range AllStatuses(p) {
			out[cs.Name] = cs.State.Running != nil
		}
	}
	return out
}

// InitContainers are the pod's init containers (they run before the rest).
func InitContainers(o *unstructured.Unstructured) map[string]bool {
	out := map[string]bool{}
	if p, err := ToPod(o); err == nil {
		for _, c := range p.Spec.InitContainers {
			out[c.Name] = true
		}
	}
	return out
}

// HelperContainers are init containers that keep running alongside the
// app (restartPolicy Always).
func HelperContainers(o *unstructured.Unstructured) map[string]bool {
	out := map[string]bool{}
	if p, err := ToPod(o); err == nil {
		for _, c := range p.Spec.InitContainers {
			if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
				out[c.Name] = true
			}
		}
	}
	return out
}

// ContainerPorts lists "container/port" for every declared port.
func ContainerPorts(o *unstructured.Unstructured) []int32 {
	p, err := ToPod(o)
	if err != nil {
		return nil
	}
	var ports []int32
	for _, c := range p.Spec.Containers {
		for _, port := range c.Ports {
			ports = append(ports, port.ContainerPort)
		}
	}
	return ports
}

// roleName is the name at the end of an IAM role ARN.
func roleName(arn string) string {
	if i := strings.LastIndex(arn, "/"); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

// shortImage drops the registry and digest: "auth:staging-82b2a9e9".
func shortImage(image string) string { return enrich.ShortImage(image) }

// podFixFor is the next step for a pod's problem: a kill by its alive
// check points at the check, the rest as podFix.
func podFixFor(st Status, owner string) string {
	return readOnlyFix(podFixWords(st, owner))
}

// ReadOnly is kubed started with --readonly: Fix lines don't offer keys
// that change things.
var ReadOnly bool

// readOnlyFix rewords a Fix line's change keys when kubed is read-only.
// changeKeyOnOwner is a Fix line's change key on what runs a pod ("(i on
// its deployment)"), which --readonly refuses.
var changeKeyOnOwner = regexp.MustCompile(`\(i on [^)]*\)`)

func readOnlyFix(fix string) string {
	if !ReadOnly {
		return fix
	}
	fix = changeKeyOnOwner.ReplaceAllString(fix, "(without --readonly)")
	return strings.NewReplacer(
		"(gC, then e on it)", "(without --readonly)",
		" (or t on its cron job runs it now)", "",
		"fix it (e), then enter opens its runs: ctrl-d on the stuck ones", "fix it, then delete the stuck runs (without --readonly)",
		"enter opens its runs, ctrl-d on them", "they need deleting (without --readonly)",
		"u to go back to the previous version, or i to fix the image name or tag", "go back to the previous version, or fix the image name or tag (without --readonly)",
		"ctrl-d on it ends this try (the job starts another if it has tries left)", "deleting it ends this try (the job starts another if it has tries left), without --readonly",
		"ctrl-d on it ends this try", "deleting it ends this try (without --readonly)",
		"delete this run (ctrl-d)", "delete this run (without --readonly)",
		"ctrl-d deletes it", "deleting it clears it (without --readonly)",
	).Replace(fix)
}

// laterRunFix: a failed cron run that a later run of its cron job has
// made good. "The next run is on schedule" is old news: say that a later
// run worked, and when.
func laterRunFix(e *enrich.Enricher, job *unstructured.Unstructured, fix string) string {
	if !strings.Contains(fix, "the next run is on schedule") {
		return fix
	}
	done, ok := e.LaterRunWorked(job)
	if !ok {
		return fix
	}
	worked := "a later run worked, " + Age(done) + " ago"
	fix = strings.Replace(fix, "the next run is on schedule (or t on its cron job runs it now)", worked, 1)
	return strings.Replace(fix, "the next run is on schedule", worked, 1)
}

func podFixWords(st Status, owner string) string {
	if fix := checkFix(st.Why); fix != "" {
		return fix
	}
	if fix := startFix(st.Short, st.Why); fix != "" {
		return onOwner(fix, owner)
	}
	switch {
	case (strings.HasPrefix(st.Short, "Crashing") || st.Short == "Restarting") && strings.Contains(st.Why, "keeps stopping"): // exits 0: no crash to read about
		return stoppingFix
	case st.Short == "Not ready" && heldBackFix(st.Why) != "":
		return heldBackFix(st.Why)
	case st.Short == "Failed" && strings.Contains(st.Why, "keeps the pod running"):
		fix := "its run failed, but another container keeps the pod running: ctrl-d on it ends this try (the job starts another if it has tries left); l shows why it failed"
		if strings.Contains(st.Why, "ran out of memory") {
			fix += "; then raise its memory limit where the job is defined"
		}
		return fix
	case strings.HasPrefix(st.Short, "Can't pull image") && owner == "Job":
		return "a job's image can't be changed once it exists: fix it where the job is defined, then create the job again"
	case strings.HasPrefix(st.Short, "Can't pull image") && owner == "CronJob":
		return "fix the image name or tag in its cron job (gC, then e on it): the next run uses it; delete this run (ctrl-d), which never starts"
	case strings.HasPrefix(st.Short, "Can't pull image") && owner == "Pod":
		return "fix the image name or tag where this pod is defined (nothing makes it again if it goes), or check registry access"
	case strings.HasPrefix(st.Short, "Can't pull image") && owner == "Node":
		return "a pod the node runs itself (static): fix the image in its manifest file on that node"
	case (strings.HasPrefix(st.Short, "Out of memory") || strings.HasPrefix(st.Short, "Hit memory limit") || st.Short == "Failed" && strings.Contains(st.Why, "ran out of memory")) && owner == "CronJob":
		return "raise its memory limit in its cron job (gC, then e on it): the next run uses it; p shows the logs from before"
	case (strings.HasPrefix(st.Short, "Out of memory") || strings.HasPrefix(st.Short, "Hit memory limit") || st.Short == "Failed" && strings.Contains(st.Why, "ran out of memory")) && owner == "Job":
		return "raise its memory limit where the job is defined (a job can't be changed once made), then create it again; p shows the logs from before"
	case st.Short == "Too big for any node": // as home's box says it
		return tooBigFix(podFixWhere(owner))
	case strings.HasPrefix(st.Short, "Waiting for a node") && nodeWaitFix(st.Why, podFixWhere(owner)) != "":
		return nodeWaitFix(st.Why, podFixWhere(owner))
	case st.Short == "Failed" && owner == "CronJob" && !strings.Contains(st.Why, "never started"):
		return "l shows its logs, up to where it failed; the next run is on schedule (or t on its cron job runs it now)"
	}
	if st.Short == "Bad config" {
		return badConfigFix(st.Why)
	}
	if st.Short == "Can't start" || st.Short == "Failed" && strings.Contains(st.Why, "never started") { // nothing ran, so no logs to read
		if owner == "Job" || owner == "CronJob" { // a job can't be changed once made: where its runs come from
			return neverStartedFix(podFixWhere(owner))
		}
		return onOwner(neverStartedFix("(on its deployment)"), owner)
	}
	return onOwner(podFix(st.Short), owner)
}

// neverStartedFix is the Fix for a container whose command is missing
// from its image, fixed where (fixWhere's words, or "(on its deployment)"
// for onOwner). A caller whose pod ArgoCD manages says "in Git".
func neverStartedFix(where string) string {
	return neverStartedWords + where + ", or use an image that has it"
}

const neverStartedWords = "it never started, so it has no logs: the command or a file it runs is missing from the image (or can't be run). Fix the command "

// neverStartedInGit: fix, a pod's Fix, says where its missing command is
// fixed in Git when ArgoCD manages o (it would undo an edit).
func neverStartedInGit(e *enrich.Enricher, o *unstructured.Unstructured, fix string) string {
	if !strings.HasPrefix(fix, neverStartedWords) {
		return fix
	}
	if _, ok := e.ManagedBy(o); !ok {
		return fix
	}
	return neverStartedFix("in Git")
}

// ownerWords name what runs a pod, for its Fix line.
var ownerWords = map[string]string{"StatefulSet": "its stateful set", "DaemonSet": "its daemon set", "Job": "its job", "CronJob": "its cron job"}

// onOwner is a pod's Fix with "(on its deployment)" naming where the fix
// goes: what runs it (owner, its top owner's kind), or, for a pod
// nothing makes, where it is defined. A static pod's (Node) is said
// with the node's name, by the caller.
func onOwner(fix, owner string) string {
	switch words, ok := ownerWords[owner]; {
	case owner == "Pod":
		return strings.ReplaceAll(fix, " (on its deployment)", " where this pod is defined (nothing makes it again if it goes)")
	case ok:
		return strings.ReplaceAll(fix, "its deployment", words)
	case owner != "" && owner != "Deployment" && owner != "ReplicaSet" && owner != "Node": // made by an operator: i doesn't change it (a ReplicaSet not loaded is almost always a deployment's)
		words = "its " + strings.ToLower(owner)
		return strings.ReplaceAll(strings.ReplaceAll(fix, "(i on its deployment)", "("+words+")"), "its deployment", words)
	}
	return fix
}

// podFixWhere is where a change to what a pod asks for goes, by what
// runs it (owner, its top owner's kind), in a pod box's words.
func podFixWhere(owner string) string {
	switch owner {
	case "Pod":
		return "where this pod is defined (nothing makes it again if it goes)"
	case "Node": // a static pod: the node runs it from a file
		return "in its static pod manifest on its node"
	case "Job":
		return "where the job is defined and create the job again (a job can't be changed once made)"
	case "CronJob":
		return "in its cron job (gC, then e on it)"
	}
	return onOwner("in its deployment", owner)
}

// badConfigFix is the Fix for a container that can't start for its
// config, from what the kubelet's message says is missing: a key in a
// secret or config map, or the object itself. Nothing when it says
// something else: its Why has the message.
func badConfigFix(why string) string {
	if kind, name, key, ok := enrich.MissingKey(why); ok {
		return "add key " + key + " to " + kind + " " + name + ", or fix the key name it refers to"
	}
	if enrich.MissingObject(why) {
		return "create the missing secret or config map, or fix the name it refers to"
	}
	return ""
}

// podFix is the next step for a pod's problem, in keys that work on it.
func podFix(short string) string {
	switch {
	case strings.HasPrefix(short, "Out of memory"): // it keeps happening: no "if"
		return "raise its memory limit (on its deployment); p shows the logs from before"
	case strings.HasPrefix(short, "Hit memory limit"):
		return "raise its memory limit (on its deployment) if this repeats; p shows the logs from before"
	case short == "Failed": // it ran once and ended: its own logs say why
		return "l shows its logs, up to where it failed"
	case strings.HasPrefix(short, "Crashing"):
		return "p shows the logs from before the crash; l the current ones"
	case strings.HasPrefix(short, "Can't pull image"):
		return "fix the image name or tag on what runs it (i on its deployment), or check registry access"
	case short == "Waiting for a node":
		return "E shows why no node fits"
	case short == "Not ready":
		return "E shows which health check fails; l the logs"
	case short == "Stuck stopping":
		return "d shows its finalizers; the node may be unreachable"
	}
	return ""
}
