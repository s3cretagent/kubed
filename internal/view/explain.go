package view

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Status is a pod's state in plain words, with the reason behind it.
type Status struct {
	Short string // "Crashing", "Out of memory", "Waiting for a node"…
	Why   string // one sentence: what happened and what it means
	Tone  Tone
}

// recentCrash: a container that failed this recently and isn't ready is
// still crashing, even caught between restarts.
const recentCrash = 10 * time.Minute

// recentlyStarted: a pod not ready yet but younger than this is "Starting".
const recentlyStarted = 2 * time.Minute

// Explain turns a pod's status into plain words. It replaces Kubernetes
// reasons like CrashLoopBackOff with what they mean for the app.
func Explain(e *enrich.Enricher, p *corev1.Pod) Status {
	// Native sidecars run, crash and restart as the app's containers do,
	// and the pods list counts their restarts: so do the words.
	running := RunningStatuses(p)
	restarts := int32(0)
	for _, cs := range running {
		restarts += cs.RestartCount
	}
	restarts += SetupRestarts(p)
	switch {
	case p.DeletionTimestamp != nil && stuckStopping(p):
		return Status{"Stuck stopping", fmt.Sprintf("asked to stop %s ago and still hasn't (%s)", Age(stopAsked(p)), stuckCause(e, p)), ToneWarn}
	case p.DeletionTimestamp != nil:
		return Status{"Stopping", stoppingWhy(p), ToneNormal}
	case p.Status.Phase == corev1.PodSucceeded:
		return Status{"Completed", "ran to completion", ToneDim}
	case p.Status.Reason == "Evicted":
		return Status{"Evicted", "removed from its node: " + oneLine(p.Status.Message), ToneBad}
	case p.Status.Phase == corev1.PodUnknown: // what its containers last said is stale
		return Status{"Unknown", "Kubernetes can't reach its node, so its state is unknown", ToneWarn}
	}

	// Containers that can't start, in the order they would block.
	for _, cs := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
		w := cs.State.Waiting
		if w == nil {
			continue
		}
		switch w.Reason {
		case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "RegistryUnavailable":
			return Status{"Can't pull image", fmt.Sprintf("%s can't download %s: %s%s", cs.Name, cs.Image, imagePullCause(w.Message), pullSecretMissing(e, p)), ToneBad}
		case "SignatureValidationFailed":
			return Status{"Can't start", fmt.Sprintf("container %q: image %s %s (%s)", cs.Name, cs.Image, sigRefused, MessageWords(oneLine(w.Message))), ToneBad}
		case "ErrImageNeverPull":
			return Status{"Can't start", fmt.Sprintf("container %q: image %s %s", cs.Name, cs.Image, neverPull), ToneBad}
		case "ImageInspectError":
			return Status{"Can't start", fmt.Sprintf("container %q: %s %s on its node: %s", cs.Name, cantReadImage, cs.Image, MessageWords(oneLine(w.Message))), ToneBad}
		case "PreStartHookError", "PreCreateHookError": // Kubernetes' own steps (CPU, devices), not the app's hooks
			return Status{"Can't start", fmt.Sprintf("container %q: %s: %s", cs.Name, cantPrepare, MessageWords(oneLine(w.Message))), ToneBad}
		case "CreateContainerConfigError", "CreateContainerError":
			return Status{"Bad config", fmt.Sprintf("%s can't start: %s", cs.Name, oneLine(w.Message)), ToneBad}
		case "CrashLoopBackOff", "RunContainerError", "PostStartHookError":
			return crashing(e, p, cs)
		}
	}

	// Between restarts a crashing container is briefly neither waiting nor
	// running: still crashing if it failed recently and isn't ready. A
	// native sidecar stopped once the work is done is Kubernetes ending
	// the pod, not a crash.
	workDone := len(p.Status.ContainerStatuses) > 0
	for _, cs := range p.Status.ContainerStatuses {
		workDone = workDone && cs.State.Terminated != nil
	}
	for i, cs := range running {
		if cs.Ready || cs.RestartCount == 0 || i >= len(p.Status.ContainerStatuses) && workDone {
			continue
		}
		t := newestStop(cs)
		// One that exits 0 is started again too where containers always
		// restart, and newer Kubernetes keeps it stopped (not waiting)
		// for its whole back-off.
		// Only in a pod still running: one ended (Failed or Succeeded, e.g.
		// stopped by a node shutting down) never starts it again.
		ended := p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded
		always := !ended && (p.Spec.RestartPolicy == "" || p.Spec.RestartPolicy == corev1.RestartPolicyAlways || i >= len(p.Status.ContainerStatuses))
		if t != nil && (t.ExitCode != 0 || always && cs.State.Terminated != nil) && time.Since(t.FinishedAt.Time) < recentCrash {
			return crashing(e, p, cs)
		}
	}

	if c := podCondition(p, corev1.PodScheduled); c != nil && c.Status == corev1.ConditionFalse {
		if placeholderPod(p) { // an overprovisioner's: waiting is its job
			return Status{"Placeholder", "a low-priority pod that holds spare room for others; it waits for a node by design", ToneNormal}
		}
		if why := tooBig(e, p); why != "" { // no node could ever take it: not a wait
			return Status{"Too big for any node", why, ToneBad}
		}
		return Status{"Waiting for a node", schedulingCause(c.Message), ToneWarn}
	}
	if p.Status.Phase == corev1.PodPending {
		// A setup step that already failed isn't "starting": it's failing
		// (kubectl says Init:Error or Init:CrashLoopBackOff).
		for _, cs := range p.Status.InitContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
				return crashing(e, p, cs)
			}
			if t := cs.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 && cs.RestartCount > 0 && !cs.Ready {
				return crashing(e, p, cs)
			}
		}
		for _, cs := range p.Status.InitContainerStatuses { // a setup step at work (waiting for a database, say)
			if r := cs.State.Running; r != nil && !isSidecar(p, cs.Name) {
				return Status{"Starting", fmt.Sprintf("setup step %q running for %s; the app's containers start after it", cs.Name, Age(r.StartedAt.Time)), ToneNormal}
			}
		}
		switch why, attaching := mountFailure(e, p); {
		case attaching:
			return Status{"Starting", "waiting for its disk to attach", ToneNormal}
		case why != "":
			return Status{"Can't mount", why, ToneBad}
		}
		return Status{"Starting", "pulling images and starting containers", ToneNormal}
	}
	if p.Status.Phase == corev1.PodFailed {
		if why, ok := startFailure(p); ok {
			return Status{"Can't start", why, ToneBad}
		}
		return Status{"Failed", lastExit(p), ToneBad}
	}
	// A container that failed in a pod that never restarts containers
	// (a job's, restartPolicy Never) while another one runs on (a
	// sidecar): the pod stays Running, but its work failed.
	if p.Spec.RestartPolicy == corev1.RestartPolicyNever && p.Status.Phase == corev1.PodRunning {
		var failed, running []string
		// ended: when the work's last container ended, well or not.
		var ended time.Time
		for _, cs := range p.Status.ContainerStatuses { // the work: a sidecar stopping (or restarting) isn't it failing
			if t := cs.State.Terminated; t != nil {
				if t.ExitCode != 0 {
					failed = append(failed, cs.Name)
				}
				if t.FinishedAt.After(ended) {
					ended = t.FinishedAt.Time
				}
			}
		}
		// Kubernetes stops native sidecars (init containers that keep
		// running) by itself once the work ends, within the grace period:
		// until then one still running is only on its way out.
		grace := 30 * time.Second
		if g := p.Spec.TerminationGracePeriodSeconds; g != nil {
			grace = time.Duration(*g) * time.Second
		}
		native := time.Since(ended) > grace+5*time.Second
		var stopping []string // native sidecars Kubernetes is stopping now
		for i, cs := range RunningStatuses(p) {
			switch {
			case cs.State.Running == nil:
			case i < len(p.Status.ContainerStatuses) || native:
				running = append(running, cs.Name)
			default:
				stopping = append(stopping, cs.Name)
			}
		}
		if len(failed) > 0 && len(running) > 0 {
			return Status{"Failed", fmt.Sprintf("%s; %s still running and keeps the pod running", lastExit(p), strings.Join(running, ", ")), ToneBad}
		}
		if len(failed) > 0 && len(stopping) > 0 { // the run failed: the pod ends once they stop
			return Status{"Failed", fmt.Sprintf("%s; Kubernetes is stopping %s, then the pod ends", lastExit(p), strings.Join(stopping, ", ")), ToneBad}
		}
		if done, _ := tryFailed(p); done && len(running) == 0 { // all stopped: the pod is marked Failed a moment later
			return Status{"Failed", lastExit(p) + ", then the pod ends", ToneBad}
		}
	}
	// A job's pod has no readiness to speak of: it runs, then finishes.
	// Calling it "not ready" for its few seconds flags every cron run. (It
	// can still keep running out of memory, with restartPolicy OnFailure.)
	jobRun := false
	if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "Job" && p.Status.Phase == corev1.PodRunning {
		jobRun = true
	}

	for _, cs := range running {
		if jobRun || cs.Ready || cs.State.Running == nil {
			continue
		}
		if started := cs.State.Running.StartedAt.Time; time.Since(started) < recentlyStarted {
			// Its ready check failed in this run, past its initial delay:
			// not ready yet, and why, rather than only "starting up".
			if msg, when, ok := readyCheckFailed(e, p, cs.Name, started); ok && strings.HasPrefix(msg, "Readiness probe") && time.Since(started) >= readinessDelay(p, cs.Name) {
				return Status{"Starting", fmt.Sprintf("%s isn't ready yet: %s (%s ago)", cs.Name, enrich.MessageWords(oneLine(msg)), Age(when)), ToneNormal}
			}
			return Status{"Starting", cs.Name + " is starting up", ToneNormal}
		}
		why := cs.Name + " is running but not ready for traffic"
		if msg, when, ok := readyCheckFailed(e, p, cs.Name, cs.State.Running.StartedAt.Time); ok {
			why = fmt.Sprintf("%s (%s ago)", enrich.MessageWords(oneLine(msg)), Age(when)) // "ready check failed (its command gave no output)", not "...failed: "
			// This run's: Kubernetes stops repeating it while the check goes on failing.
			if time.Since(when) > time.Minute && when.After(cs.State.Running.StartedAt.Time) {
				why = fmt.Sprintf("%s (still failing; last reported %s ago)", enrich.MessageWords(oneLine(msg)), Age(when))
			}
		}
		return Status{"Not ready", why, ToneWarn}
	}

	// Running and ready; say so if it ran out of memory today, or keeps
	// restarting: the worst of its containers, whichever comes first.
	var worst *Status
	for i, cs := range running {
		if i >= len(p.Status.ContainerStatuses) && workDone { // a sidecar Kubernetes stops as the pod ends
			continue
		}
		if st, ok := restartStatus(e, p, cs); ok && (worst == nil || severity(st.Tone) > severity(worst.Tone)) {
			worst = &st
		}
	}
	if worst != nil {
		return *worst
	}
	for _, cs := range running {
		if !cs.Ready && !jobRun {
			return Status{"Not ready", fmt.Sprintf("container %q isn't ready", cs.Name), ToneWarn}
		}
	}
	if jobRun {
		return Status{"Running", "a run in progress", ToneNormal}
	}
	// Its containers say ready, but Kubernetes doesn't send it traffic.
	if c := podCondition(p, corev1.PodReady); c != nil && c.Status != corev1.ConditionTrue && p.Status.Phase == corev1.PodRunning {
		return readyHeldBack(e, p)
	}
	why := "healthy"
	if restarts > 0 {
		why = "healthy now; " + plural(int(restarts), "restart") + " in total"
		var last time.Time
		for _, cs := range running {
			if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(last) {
				last = t.FinishedAt.Time
			}
		}
		if !last.IsZero() { // settled, but say when it last fell over
			why += ", the last " + Age(last) + " ago"
		}
	}
	return Status{"Running", why, ToneNormal}
}

// upAfterBackOff: the container is up again only after Kubernetes' wait
// between crashes (at most 5 minutes), and not for long yet: after many
// crashes its last stop is over 5 minutes old by the time it runs again.
func upAfterBackOff(cs corev1.ContainerStatus, t *corev1.ContainerStateTerminated) bool {
	if cs.State.Running == nil || cs.State.Running.StartedAt.IsZero() {
		return false
	}
	up := time.Since(cs.State.Running.StartedAt.Time)
	return up < 5*time.Minute && cs.State.Running.StartedAt.Sub(t.FinishedAt.Time) <= 5*time.Minute+30*time.Second
}

// restartStatus says what a running pod's container's last stop means
// now: a crash or a memory kill that keeps happening, or one to check.
func restartStatus(e *enrich.Enricher, p *corev1.Pod, cs corev1.ContainerStatus) (Status, bool) {
	t := newestStop(cs) // stopped now: that stop is the latest
	if t == nil || time.Since(t.FinishedAt.Time) > restartWindow {
		return Status{}, false
	}
	ago := time.Since(t.FinishedAt.Time)
	aliveKill := func() (Status, bool) {
		if e == nil {
			return Status{}, false
		}
		check, words, ok := e.KilledByCheck(p.Namespace, p.Name, cs.Name, t.FinishedAt.Time, t.ExitCode)
		if !ok {
			return Status{}, false
		}
		why := fmt.Sprintf("container %q is killed by its %s", cs.Name, check)
		if words != "" {
			why += " (" + checkAnswer(check, words) + ")"
		}
		return Status{"Crashing", fmt.Sprintf("%s; %s (running again now)", why, restartsAgo(cs.RestartCount, t, false)), ToneBad}, true
	}
	switch {
	case t.Reason != "OOMKilled" && t.Reason != "Completed" && t.ExitCode != 0 && cs.RestartCount >= restartsToCheck && (ago < 5*time.Minute || upAfterBackOff(cs, t)):
		// Up for a few seconds between crashes: still crashing, or the
		// lists flip between failing and fine every few seconds.
		if st, ok := aliveKill(); ok {
			return st, true
		}
		if hook, ok := hookKilled(e, p, cs, t); ok {
			return Status{"Crashing", fmt.Sprintf("%s; %s (running again now)", hook, restartsAgo(cs.RestartCount, t, false)), ToneBad}, true
		}
		return Status{"Crashing", fmt.Sprintf("container %q keeps crashing: %s (running again now); p shows the logs from before",
			cs.Name, restartsAgo(cs.RestartCount, t, false)), ToneBad}, true
	case t.Reason == "OOMKilled" && cs.RestartCount >= restartsToCheck && ago < restartRecent:
		// Running between kills: still a problem, not "to check", or it
		// flips between the two every run.
		return Status{"Out of memory", fmt.Sprintf("%s keeps running out of memory: limit %s; %s (running again now); p shows the logs from before",
			cs.Name, memLimit(p, cs.Name), restartsAgo(cs.RestartCount, t, true)), ToneBad}, true
	case t.Reason == "OOMKilled":
		return Status{"Hit memory limit", fmt.Sprintf("ran out of memory %s ago and restarted; running again now (container %q, limit %s); p shows the logs from before",
			Age(t.FinishedAt.Time), cs.Name, memLimit(p, cs.Name)), ToneWarn}, true
	case cs.RestartCount >= restartsToCheck && ago < restartRecent:
		if st, ok := aliveKill(); ok {
			return st, true
		}
		if t.ExitCode == 0 { // it ended without an error: no crash
			return Status{"Restarting", fmt.Sprintf("container %q keeps stopping and being started again: restarted %s, the last %s ago (exit code 0); p shows the logs from before",
				cs.Name, plural(int(cs.RestartCount), "time"), Age(t.FinishedAt.Time)), ToneWarn}, true
		}
		return Status{"Restarting", fmt.Sprintf("container %q restarted %s, the last %s ago; p shows the logs from before",
			cs.Name, plural(int(cs.RestartCount), "time"), Age(t.FinishedAt.Time)), ToneWarn}, true
	}
	return Status{}, false
}

// SetupRestarts counts the restarts of setup (init) containers that
// haven't finished yet: while one keeps failing, kubectl's RESTARTS counts
// them, and they are the restarts that matter.
func SetupRestarts(p *corev1.Pod) int32 {
	sidecar := map[string]bool{}
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecar[c.Name] = true // counted with the running containers
		}
	}
	n := int32(0)
	for _, cs := range p.Status.InitContainerStatuses {
		if sidecar[cs.Name] {
			continue
		}
		if t := cs.State.Terminated; t != nil && t.ExitCode == 0 {
			continue // done: its old tries are history
		}
		n += cs.RestartCount
	}
	return n
}

func crashing(e *enrich.Enricher, p *corev1.Pod, cs corev1.ContainerStatus) Status {
	restarts := cs.RestartCount // "keeps crashing" counts this container's own tries, not the pod's
	t := newestStop(cs)
	tries := "" // how often it tried, when Kubernetes counted any
	if restarts > 0 {
		tries = "; " + plural(int(restarts), "try")
	}
	if t != nil && isStartError(t.Reason) {
		return Status{"Can't start", fmt.Sprintf("container %q: %s%s", cs.Name, StartWords(t.Message), tries), ToneBad}
	}
	if w := cs.State.Waiting; t == nil && w != nil && w.Reason == "RunContainerError" && w.Message != "" {
		// Its first try failed to start, before any stop was recorded.
		return Status{"Can't start", fmt.Sprintf("container %q: %s%s", cs.Name, StartWords(w.Message), tries), ToneBad}
	}
	if hook, ok := hookKilled(e, p, cs, t); ok {
		return Status{"Crashing", fmt.Sprintf("%s; %s", hook, restartsAgo(restarts, t, false)), ToneBad}
	}
	if t != nil && t.Reason == "OOMKilled" {
		why := fmt.Sprintf("%s keeps running out of memory: limit %s", cs.Name, memLimit(p, cs.Name))
		// The reading is the whole pod's: it is this container's only when
		// it is the pod's one container, and running.
		if u := e.PodUsage(podObject(p)); u.Known && len(RunningStatuses(p)) == 1 && cs.State.Running != nil {
			why += ", using " + mem(u.Use.MemBytes, true) + " now"
		}
		return Status{"Out of memory", fmt.Sprintf("%s; %s", why, restartsAgo(restarts, t, true)), ToneBad}
	}
	if t != nil && e != nil {
		if check, words, ok := e.KilledByCheck(p.Namespace, p.Name, cs.Name, t.FinishedAt.Time, t.ExitCode); ok {
			why := fmt.Sprintf("container %q is killed by its %s", cs.Name, check)
			if words != "" {
				why += " (" + checkAnswer(check, words) + ")"
			}
			return Status{"Crashing", fmt.Sprintf("%s; %s", why, restartsAgo(restarts, t, false)), ToneBad}
		}
	}
	why := fmt.Sprintf("container %q keeps crashing; %s", cs.Name, plural(int(restarts), "restart"))
	short, tone, logs := "Crashing", ToneBad, "crashed run's"
	if t != nil && t.ExitCode == 0 {
		// It ends without an error each time: stopping, not crashing. The
		// same word stopped, waiting and running again, so it doesn't flip.
		why, logs = fmt.Sprintf("container %q keeps stopping and being started again; %s", cs.Name, plural(int(restarts), "restart")), "last run's"
		short, tone = "Restarting", ToneWarn
	}
	if t != nil {
		why += fmt.Sprintf("; last exit code %d", t.ExitCode)
		if t.Reason != "" && t.Reason != "Error" {
			why += " (" + t.Reason + ")"
		}
		why += fmt.Sprintf(", %s ago", Age(t.FinishedAt.Time))
	}
	return Status{short, why + ". p shows the " + logs + " logs", tone}
}

// newestStop is a container's latest stop: stopped now (waiting to
// restart, or a setup step that just failed) is newer than the one before.
func newestStop(cs corev1.ContainerStatus) *corev1.ContainerStateTerminated {
	t := cs.LastTerminationState.Terminated
	if now := cs.State.Terminated; now != nil && (t == nil || !now.FinishedAt.Before(&t.FinishedAt)) {
		t = now
	}
	return t
}

// endedHow says how a container ended: "stopped with an error (exit 3)",
// "ran out of memory and was killed", "never started (...)".
func endedHow(t *corev1.ContainerStateTerminated) string {
	how := exitWords(t.Reason, t.ExitCode)
	switch t.Reason {
	case "OOMKilled":
		return "ran out of memory and was killed"
	case "StartError", "ContainerCannotRun":
		return how
	case "Error":
		how = "an " + how
	}
	return "stopped with " + how
}

// placeholderPod: a low-priority pod that only runs the pause image, as an
// overprovisioner makes to hold spare room (a real low-priority workload
// that can't be placed is still a problem).
func placeholderPod(p *corev1.Pod) bool {
	if p.Spec.Priority == nil || *p.Spec.Priority >= 0 || len(p.Spec.Containers) == 0 {
		return false
	}
	for _, c := range p.Spec.Containers {
		if !strings.Contains(c.Image, "/pause:") && !strings.HasPrefix(c.Image, "pause:") {
			return false
		}
	}
	return true
}

func lastExit(p *corev1.Pod) string {
	for _, cs := range p.Status.ContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return fmt.Sprintf("container %q %s", cs.Name, endedHow(t))
		}
	}
	helper := map[string]bool{} // native sidecars: stopped by Kubernetes, not a failure
	for _, c := range p.Spec.InitContainers {
		helper[c.Name] = c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
	}
	for _, cs := range p.Status.InitContainerStatuses { // a setup step failed, so the app never ran
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 && !helper[cs.Name] {
			return fmt.Sprintf("setup step %q %s, so the app never ran", cs.Name, endedHow(t))
		}
	}
	return oneLine(p.Status.Message)
}

// restartWindow and restartsToCheck: a running pod that restarted this
// many times, the last one within the window, is worth a look.
const (
	restartWindow   = 24 * time.Hour // running out of memory counts for a day
	restartRecent   = 2 * time.Hour  // other restarts: only if the last one is this recent
	restartsToCheck = 3
)

func memLimit(p *corev1.Pod, container string) string {
	for _, c := range append(append([]corev1.Container(nil), p.Spec.Containers...), p.Spec.InitContainers...) { // a setup step or sidecar has its own
		if c.Name == container {
			if l := c.Resources.Limits.Memory(); !l.IsZero() {
				return mem(l.Value(), true)
			}
		}
	}
	return "none set"
}

func podCondition(p *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == t {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

// tooBig says why a pod can never be placed: it asks for more CPU or memory
// than the biggest node here has at all. "" when some node could hold it.
func tooBig(e *enrich.Enricher, p *corev1.Pod) string {
	if e == nil {
		return ""
	}
	var cpuAsk, memAsk int64
	for _, c := range p.Spec.Containers {
		cpuAsk += c.Resources.Requests.Cpu().MilliValue()
		memAsk += c.Resources.Requests.Memory().Value()
	}
	var maxCPU, maxMem int64
	nodes := e.List(kube.Nodes, "")
	for _, n := range nodes {
		u := e.NodeUsage(n)
		maxCPU, maxMem = max(maxCPU, u.AllocCPU), max(maxMem, u.AllocMem)
	}
	switch {
	case len(nodes) == 0:
	case maxCPU > 0 && cpuAsk > maxCPU:
		return fmt.Sprintf("asks for %s of CPU, more than any node here has (the biggest: %s); unless a bigger machine joins, it never starts", cores(cpuAsk), cores(maxCPU))
	case maxMem > 0 && memAsk > maxMem:
		return fmt.Sprintf("asks for %s of memory, more than any node here has (the biggest: %s); unless a bigger machine joins, it never starts", Mem(memAsk), Mem(maxMem))
	}
	return ""
}

// schedulingCause rewrites the scheduler's message (enrich.SchedulingWords),
// or keeps it on one line when it isn't the usual kind.
func schedulingCause(msg string) string {
	if words := enrich.SchedulingWords(msg); words != "" {
		return words
	}
	return oneLine(msg)
}

// imagePullCause shortens kubelet's pull error to the part that matters.
func imagePullCause(msg string) string {
	switch {
	case strings.Contains(msg, "not found") || strings.Contains(msg, "manifest unknown"):
		return "that image or tag doesn't exist"
	case strings.Contains(strings.ToLower(msg), "unauthorized") || strings.Contains(msg, "denied") || strings.Contains(msg, "403"):
		return "access denied by the registry"
	case strings.Contains(msg, "Back-off"):
		return "retrying after earlier failures (" + oneLine(msg) + ")"
	}
	return oneLine(msg)
}

// stuckStopping: a pod still there well after its grace period ran out.
// Kubernetes sets its deletion time to that deadline (when it was asked to
// stop, plus the grace period), not to when it was asked.
func stuckStopping(p *corev1.Pod) bool {
	return time.Since(p.DeletionTimestamp.Time) > 5*time.Minute
}

// stopAsked is when a pod was (last) asked to stop: its deletion time is
// that, plus its grace period.
func stopAsked(p *corev1.Pod) time.Time {
	at := p.DeletionTimestamp.Time
	if g := p.DeletionGracePeriodSeconds; g != nil {
		at = at.Add(-time.Duration(*g) * time.Second)
	}
	return at
}

// stuckCause says what may hold a pod that won't go: its finalizers when
// it has some (Kubernetes keeps it until they are removed), or its node.
func stuckCause(e *enrich.Enricher, p *corev1.Pod) string {
	var why []string
	if len(p.Finalizers) > 0 {
		why = append(why, fmt.Sprintf("it has %s %s: Kubernetes keeps it until they are removed", map[bool]string{true: "finalizer", false: "finalizers"}[len(p.Finalizers) == 1], strings.Join(p.Finalizers, ", ")))
	}
	switch {
	case nodeDown(e, p):
		why = append(why, "its node isn't reporting")
	case len(why) == 0:
		why = append(why, "its node may be unreachable")
	}
	return strings.Join(why, "; ")
}

// nodeDown: the pod's node is known and doesn't say it is ready.
func nodeDown(e *enrich.Enricher, p *corev1.Pod) bool {
	if e == nil || p.Spec.NodeName == "" {
		return false
	}
	n, ok := e.NodeObject(p.Spec.NodeName)
	if !ok {
		return false
	}
	c, ok := enrich.Condition(n, "Ready")
	return ok && c.Status != "True"
}

// stoppingWhy says a pod is shutting down. Why (replaced, scaled down,
// deleted by hand, its node drained) the pod doesn't tell, so it doesn't
// say; only that a pod of its own doesn't come back.
func stoppingWhy(p *corev1.Pod) string {
	if metav1.GetControllerOf(p) == nil {
		return "shutting down; nothing makes it again (a pod of its own)"
	}
	return "shutting down"
}

// readinessDelay is a container's ready check's initial delay (none when
// it has no check): until then, Kubernetes doesn't run the check.
func readinessDelay(p *corev1.Pod, container string) time.Duration {
	for _, c := range append(append([]corev1.Container{}, p.Spec.Containers...), p.Spec.InitContainers...) {
		if c.Name == container && c.ReadinessProbe != nil {
			return time.Duration(c.ReadinessProbe.InitialDelaySeconds) * time.Second
		}
	}
	return 0
}

// gateGrace: a readiness gate waits on something outside the pod (an AWS
// load balancer's health checks pass after 30 to 90 seconds or more), so
// on every rollout a new pod waits this long for it as part of starting.
const gateGrace = 3 * time.Minute

// readyHeldBack says why Kubernetes keeps a pod out of traffic though its
// containers last said they were ready: a readiness gate not passed yet
// (starting, for a while), or a node that stopped reporting (what its
// containers said is stale).
func readyHeldBack(e *enrich.Enricher, p *corev1.Pod) Status {
	for _, g := range p.Spec.ReadinessGates {
		if c := podCondition(p, g.ConditionType); c == nil || c.Status != corev1.ConditionTrue {
			what := ""
			if strings.Contains(string(g.ConditionType), "elbv2.k8s.aws") {
				what = " (its load balancer's health check)"
			}
			since := time.Time{} // since its containers became ready, else since it started
			if c := podCondition(p, corev1.ContainersReady); c != nil && c.Status == corev1.ConditionTrue {
				since = c.LastTransitionTime.Time
			}
			if since.IsZero() && p.Status.StartTime != nil {
				since = p.Status.StartTime.Time
			}
			if !since.IsZero() && time.Since(since) < gateGrace {
				return Status{"Starting", fmt.Sprintf("its containers are ready; waiting for its readiness gate %s%s", g.ConditionType, what), ToneNormal}
			}
			waited := ""
			if !since.IsZero() {
				waited = " after " + Age(since)
			}
			return Status{"Not ready", fmt.Sprintf("its containers are ready, but its readiness gate %s%s hasn't passed%s", g.ConditionType, what, waited), ToneWarn}
		}
	}
	if nodeDown(e, p) {
		return Status{"Not ready", "its node " + ShortNode(p.Spec.NodeName) + " isn't reporting, so Kubernetes took it out of traffic", ToneWarn}
	}
	return Status{"Not ready", "Kubernetes keeps it out of traffic, though its containers say they are ready", ToneWarn}
}

// heldBackFix is the next step for a pod its containers call ready that
// Kubernetes keeps out of traffic: no probe fails, so not the probe advice.
// "" for any other reason.
func heldBackFix(why string) string {
	switch {
	case strings.Contains(why, "readiness gate") && strings.Contains(why, "load balancer"):
		return "the AWS load balancer controller passes the gate once the target group finds the pod healthy: check the target group's health check (path and port) and that the load balancer can reach the pod"
	case strings.Contains(why, "readiness gate"):
		return "what sets that readiness gate (a controller) hasn't marked it passed: check that controller"
	case strings.Contains(why, "isn't reporting"):
		return "gn shows its node; if the node stays unreachable, Kubernetes usually removes its pods after about 5 minutes"
	case strings.Contains(why, "out of traffic"): // nothing known yet (its node still loading, a gate just passed): look again first
		return "this often clears within a few seconds; if it stays, E shows its events and gn its node"
	}
	return ""
}

// stoppingFix: a container that keeps exiting without an error has no
// crash to read about (the pod and its app say the same).
const stoppingFix = "p shows the logs of its last run; l shows the live ones"

// readyCheckFailed is the latest failure of a container's ready (or
// startup) check in this run: an alive check failing, another
// container's check, or a run before this one say nothing of why it
// isn't ready now.
func readyCheckFailed(e *enrich.Enricher, p *corev1.Pod, container string, since time.Time) (string, time.Time, bool) {
	var msg string
	var when time.Time
	if e == nil {
		return "", when, false
	}
	for _, ev := range e.List(kube.Events, p.Namespace) {
		inv, _, _ := unstructured.NestedMap(ev.Object, "involvedObject")
		m, _, _ := unstructured.NestedString(ev.Object, "message")
		reason, _, _ := unstructured.NestedString(ev.Object, "reason")
		if reason != "Unhealthy" || inv["kind"] != "Pod" || inv["name"] != p.Name {
			continue
		}
		// A check's event names its container; one that names none is
		// this container's only when the pod runs no other.
		if path, _ := inv["fieldPath"].(string); !strings.HasSuffix(path, "{"+container+"}") && (path != "" || len(RunningStatuses(p)) > 1) {
			continue
		}
		if !strings.HasPrefix(m, "Readiness probe") && !strings.HasPrefix(m, "Startup probe") {
			continue
		}
		if t := enrich.EventTime(ev); !t.Before(since.Add(-time.Second)) && t.After(when) {
			msg, when = m, t
		}
	}
	return msg, when, msg != ""
}

// isStartError: the container never ran its program (its command or a
// file it needs is missing, or can't be run).
func isStartError(reason string) bool {
	return reason == "StartError" || reason == "ContainerCannotRun"
}

// startFailure says why a finished pod never started its program.
func startFailure(p *corev1.Pod) (string, bool) {
	for _, cs := range p.Status.ContainerStatuses {
		if t := cs.State.Terminated; t != nil && isStartError(t.Reason) {
			return fmt.Sprintf("container %q: %s", cs.Name, StartWords(t.Message)), true
		}
	}
	return "", false
}

var (
	execMissing = regexp.MustCompile(`exec: "([^"]+)":[^:]*:? *(?:[^:]*: )*no such file or directory`)
	execDenied  = regexp.MustCompile(`exec: "([^"]+)":[^:]*:? *(?:[^:]*: )*permission denied`)
)

// StartWords puts a runtime's start error in words: the long "OCI runtime
// create failed: … exec: "/app/run.sh": stat …: no such file or directory"
// becomes "it never started: /app/run.sh isn't in the image".
func StartWords(msg string) string {
	msg = strings.Join(strings.Fields(msg), " ")
	if m := execMissing.FindStringSubmatch(msg); m != nil {
		return "it never started: " + m[1] + " isn't in the image"
	}
	if m := execDenied.FindStringSubmatch(msg); m != nil {
		return "it never started: " + m[1] + " can't be run (no permission to run it)"
	}
	if i := strings.LastIndex(msg, "unable to start container process: "); i >= 0 {
		msg = msg[i+len("unable to start container process: "):]
	}
	msg = strings.TrimSuffix(msg, ": unknown")
	if msg == "" {
		return "it never started"
	}
	return "it never started: " + shortText(msg, 200)
}

// StoppedNow reports that the container has stopped and not restarted yet. Its
// current logs are then the run that just ended; "previous" logs would be
// the run before (often gone already).
func StoppedNow(o *unstructured.Unstructured, container string) bool {
	statuses, _, _ := unstructured.NestedSlice(o.Object, "status", "containerStatuses")
	inits, _, _ := unstructured.NestedSlice(o.Object, "status", "initContainerStatuses")
	for _, c := range append(statuses, inits...) {
		m, _ := c.(map[string]any)
		if m["name"] != container {
			continue
		}
		_, stopped, _ := unstructured.NestedMap(m, "state", "terminated")
		return stopped
	}
	return false
}

// checkAnswer is what the check that killed it found, without repeating
// "alive check failed" after "killed by its alive check".
func checkAnswer(check, words string) string {
	words = strings.TrimPrefix(words, check+" failed: ")
	if inner, ok := strings.CutPrefix(words, check+" failed ("); ok {
		words = strings.TrimSuffix(inner, ")")
	}
	return words
}

// Words for containers Kubernetes can't start for reasons of its own or
// of the image's (their Fix lines find them by these words).
const (
	neverPull     = "isn't on its node, and its pull policy is Never (it never downloads)"
	cantReadImage = "Kubernetes can't read image"
	cantPrepare   = "Kubernetes couldn't prepare it on its node"
	sigRefused    = "was refused: its signature doesn't pass the node's policy"
)

// podEvents are the events kept about pod p since it was made (an
// earlier pod of the same name's are not its own).
func podEvents(e *enrich.Enricher, p *corev1.Pod) []*unstructured.Unstructured {
	if e == nil {
		return nil
	}
	var out []*unstructured.Unstructured
	made := p.CreationTimestamp.Truncate(time.Second)
	for _, ev := range e.List(kube.Events, p.Namespace) {
		inv, _, _ := unstructured.NestedMap(ev.Object, "involvedObject")
		if inv["kind"] != "Pod" || inv["name"] != p.Name {
			continue
		}
		if uid, _ := inv["uid"].(string); uid != "" && p.UID != "" && uid != string(p.UID) {
			continue
		}
		if enrich.EventTime(ev).Before(made) {
			continue
		}
		out = append(out, ev)
	}
	return out
}

func evStr(ev *unstructured.Unstructured, path ...string) string {
	s, _, _ := unstructured.NestedString(ev.Object, path...)
	return s
}

// pullSecrets is the kubelet's "Unable to retrieve some image pull
// secrets (a, b); attempting to pull the image may not succeed."
var pullSecrets = regexp.MustCompile(`Unable to retrieve some image pull secrets \(([^)]+)\)`)

// pullSecretMissing adds to a failed pull that a pull secret it names
// isn't there, when the kubelet said so for this pod; "" otherwise.
func pullSecretMissing(e *enrich.Enricher, p *corev1.Pod) string {
	var names string
	var when time.Time
	for _, ev := range podEvents(e, p) {
		if evStr(ev, "reason") != "FailedToRetrieveImagePullSecret" {
			continue
		}
		if m := pullSecrets.FindStringSubmatch(evStr(ev, "message")); m != nil && !enrich.EventTime(ev).Before(when) {
			names, when = m[1], enrich.EventTime(ev)
		}
	}
	switch {
	case names == "":
		return ""
	case strings.Contains(names, ","):
		return "; its pull secrets " + names + " aren't there"
	}
	return "; its pull secret " + names + " isn't there"
}

// pullSecretGone: a pull's Why says its pull secret isn't there.
func pullSecretGone(why string) bool {
	return strings.Contains(why, "; its pull secret")
}

// mountGrace: mounting a volume can fail once or twice as a pod starts
// (its secret just made); not a problem yet. A disk attaching takes
// longer (attachGrace), and one still attached to another node longest:
// Kubernetes waits about 6 minutes before it forces that node to let go
// (multiAttachGrace).
const (
	mountGrace       = 30 * time.Second
	attachGrace      = 3 * time.Minute
	multiAttachGrace = 7 * time.Minute
)

// attachGraceFor is how long a pod may wait on what a mount warning
// says: a disk attaching, or a secret or config map (mountGrace).
func attachGraceFor(reason, msg string) time.Duration {
	switch {
	case strings.Contains(msg, "Multi-Attach error"):
		return multiAttachGrace
	case reason == "FailedAttachVolume", strings.Contains(msg, "timed out waiting for the condition"), strings.Contains(msg, "unmounted volumes"):
		return attachGrace
	}
	return mountGrace
}

// mountWindow: a volume still failing is reported again within minutes
// (Kubernetes limits a pod's events to about one in 5 minutes once it
// has sent many); an older failure says nothing of now.
const mountWindow = 10 * time.Minute

var (
	mountVolume = regexp.MustCompile(`failed for volume "([^"]+)" : (.*)$`)
	mountNoKey  = regexp.MustCompile(`non-existent (secret|config) key: (\S+)`)
)

// mountFailure says why a pod waiting to create its containers can't
// mount a volume, from its newest FailedMount (or FailedAttachVolume)
// warning: "volume c: config map no-such-cm isn't there". "" when none
// is kept, or it is past. attaching: a disk is still attaching, within
// the time that normally takes (not a problem yet); a secret or config
// map mount younger than mountGrace says nothing.
func mountFailure(e *enrich.Enricher, p *corev1.Pod) (why string, attaching bool) {
	all := slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses)
	if len(all) == 0 {
		return "", false
	}
	for _, cs := range all {
		if w := cs.State.Waiting; w == nil || w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
			return "", false
		}
	}
	started := p.CreationTimestamp.Time
	if p.Status.StartTime != nil {
		started = p.Status.StartTime.Time
	}
	var msg, reason string
	var failed, progress time.Time
	for _, ev := range podEvents(e, p) {
		t := enrich.EventTime(ev)
		switch r := evStr(ev, "reason"); r {
		case "FailedMount", "FailedAttachVolume":
			if evStr(ev, "type") == "Warning" && !t.Before(failed) {
				msg, reason, failed = evStr(ev, "message"), r, t
			}
		// Its volumes were mounted: the kubelet went on (to the pod's
		// sandbox, its images, its containers).
		case "Pulling", "Pulled", "Created", "Started", "FailedCreatePodSandBox", "SandboxChanged":
			if t.After(progress) {
				progress = t
			}
		}
	}
	if msg == "" || time.Since(failed) > mountWindow || progress.After(failed) {
		return "", false
	}
	msg = oneLine(msg)
	if grace := attachGraceFor(reason, msg); time.Since(started) < grace {
		return "", grace > mountGrace
	}
	return mountWords(p, msg), false
}

// mountWords puts a mount warning in words: which volume, and what of it
// isn't there.
func mountWords(p *corev1.Pod, msg string) string {
	m := mountVolume.FindStringSubmatch(msg)
	if m == nil {
		return MessageWords(msg)
	}
	vol, rest := m[1], m[2]
	if kind, name, ok := enrich.MissingObjectName(rest); ok {
		return "volume " + vol + ": " + kind + " " + name + " isn't there"
	}
	if k := mountNoKey.FindStringSubmatch(rest); k != nil {
		kind := map[string]string{"secret": "secret", "config": "config map"}[k[1]]
		if name := volumeSource(p, vol, kind, k[2]); name != "" {
			return "volume " + vol + ": " + kind + " " + name + " has no key " + k[2]
		}
		return "volume " + vol + ": its " + kind + " has no key " + k[2]
	}
	return MessageWords(msg)
}

// volumeSource is the secret or config map (kind) of pod p's volume vol
// that lists key: "" when it isn't one only.
func volumeSource(p *corev1.Pod, vol, kind, key string) string {
	var names []string
	lists := func(name string, items []corev1.KeyToPath) {
		for _, it := range items {
			if it.Key == key && !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	for _, v := range p.Spec.Volumes {
		if v.Name != vol {
			continue
		}
		switch {
		case kind == "secret" && v.Secret != nil:
			lists(v.Secret.SecretName, v.Secret.Items)
		case kind == "config map" && v.ConfigMap != nil:
			lists(v.ConfigMap.Name, v.ConfigMap.Items)
		case v.Projected != nil:
			for _, src := range v.Projected.Sources {
				if kind == "secret" && src.Secret != nil {
					lists(src.Secret.Name, src.Secret.Items)
				}
				if kind == "config map" && src.ConfigMap != nil {
					lists(src.ConfigMap.Name, src.ConfigMap.Items)
				}
			}
		}
	}
	if len(names) != 1 {
		return ""
	}
	return names[0]
}

// mountNoKeyWhy is a Can't mount Why's "secret has-a has no key nope".
var mountNoKeyWhy = regexp.MustCompile(`(secret|config map) (\S+) has no key (\S+)$`)

// mountFix is the Fix for a volume that can't be mounted, worded as the
// bad config one: "" when the Why says nothing kubed knows to fix.
func mountFix(why string) string {
	if m := mountNoKeyWhy.FindStringSubmatch(why); m != nil {
		return "add key " + m[3] + " to " + m[1] + " " + m[2] + ", or fix the key name it refers to"
	}
	if strings.HasSuffix(why, " isn't there") && (strings.Contains(why, ": secret ") || strings.Contains(why, ": config map ")) {
		return "create the missing secret or config map, or fix the name it refers to"
	}
	return ""
}

// startFix is the Fix for a pod that can't start for a reason its Why
// names (not a command missing from its image): "" for the rest.
func startFix(short, why string) string {
	switch {
	case short == "Can't mount":
		return mountFix(why)
	case short == "Can't start" && strings.Contains(why, neverPull):
		return "set imagePullPolicy to IfNotPresent or put the image on the node"
	case short == "Can't start" && (strings.Contains(why, cantReadImage) || strings.Contains(why, cantPrepare) || strings.Contains(why, sigRefused)):
		return "E shows its events"
	case strings.HasPrefix(short, "Can't pull image") && pullSecretGone(why):
		return "create the pull secret, or fix imagePullSecrets"
	case strings.HasPrefix(short, "Crashing") && strings.Contains(why, hookFailed):
		return "fix its postStart hook (on its deployment)"
	}
	return ""
}

// hookFailed: Kubernetes killed the container because its postStart
// hook failed.
const hookFailed = "is killed because its postStart hook failed"

// KilledByHook reports that the stop of p's container that finished at finished
// (its stop now, or the one before) was Kubernetes killing it because its
// postStart hook failed, by hookKilled's evidence.
func KilledByHook(e *enrich.Enricher, p *corev1.Pod, container string, finished time.Time) bool {
	for _, cs := range AllStatuses(p) {
		if cs.Name != container {
			continue
		}
		for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if t != nil && t.FinishedAt.Unix() == finished.Unix() {
				_, ok := hookKilled(e, p, cs, t)
				return ok
			}
		}
	}
	return false
}

func init() { // :changes says a hook kill as home does
	enrich.HookKilledOf = func(e *enrich.Enricher, o *unstructured.Unstructured, container string, finished time.Time) bool {
		p, err := ToPod(o)
		return err == nil && KilledByHook(e, p, container, finished)
	}
}

// hookKilled says a container's latest stop was Kubernetes killing it
// because its postStart hook failed, with the hook's error when kept:
// from its waiting message, or its pod's events since that run began.
func hookKilled(e *enrich.Enricher, p *corev1.Pod, cs corev1.ContainerStatus, t *corev1.ContainerStateTerminated) (string, bool) {
	msg, ok := "", false
	if w := cs.State.Waiting; w != nil && w.Reason == "PostStartHookError" {
		msg, ok = w.Message, true
	}
	if !ok {
		var since, when time.Time
		if t != nil {
			since = t.StartedAt.Add(-2 * time.Second)
		}
		for _, ev := range podEvents(e, p) {
			at := enrich.EventTime(ev)
			if path := evStr(ev, "involvedObject", "fieldPath"); at.Before(since) || !strings.HasSuffix(path, "{"+cs.Name+"}") && (path != "" || len(RunningStatuses(p)) > 1) {
				continue
			}
			switch evStr(ev, "reason") {
			case "FailedPostStartHook":
				if at.After(when) {
					msg, when = evStr(ev, "message"), at
				}
				ok = true
			case "Killing":
				if strings.Contains(evStr(ev, "message"), "FailedPostStartHook") {
					ok = true
				}
			}
		}
	}
	if !ok {
		msg, ok = hookKilledEarlier(e, p, cs, t)
	}
	if !ok {
		return "", false
	}
	why := fmt.Sprintf("container %q %s", cs.Name, hookFailed)
	if words := hookWords(msg); words != "" {
		why += " (" + words + ")"
	}
	return why, true
}

// hookKilledEarlier: no event tells of this stop (the kubelet stops
// repeating an event that comes often), but an earlier run of the same
// crash loop, within the hour, was killed because its postStart hook
// failed, and this one ended alike: its container has that hook and was
// killed (exit 137), with nothing else killing it since. The hook's
// error, when kept.
func hookKilledEarlier(e *enrich.Enricher, p *corev1.Pod, cs corev1.ContainerStatus, t *corev1.ContainerStateTerminated) (string, bool) {
	if t == nil || t.Reason != "Error" || t.ExitCode != 137 || cs.RestartCount == 0 || !hasPostStart(p, cs.Name) {
		return "", false
	}
	msg := ""
	var hookAt, msgAt, otherAt time.Time
	for _, ev := range podEvents(e, p) {
		at := enrich.EventTime(ev)
		if path := evStr(ev, "involvedObject", "fieldPath"); !strings.HasSuffix(path, "{"+cs.Name+"}") && (path != "" || len(RunningStatuses(p)) > 1) {
			continue
		}
		reason, text := evStr(ev, "reason"), evStr(ev, "message")
		switch {
		case reason == "FailedPostStartHook" || reason == "Killing" && strings.Contains(text, "FailedPostStartHook"):
			if at.After(hookAt) {
				hookAt = at
			}
			if reason == "FailedPostStartHook" && at.After(msgAt) {
				msg, msgAt = text, at
			}
		case reason == "Killing" || reason == "Unhealthy" && !strings.HasPrefix(text, "Readiness"): // killed for something else
			if at.After(otherAt) {
				otherAt = at
			}
		}
	}
	if hookAt.IsZero() || t.StartedAt.Sub(hookAt) > time.Hour || !otherAt.Before(hookAt) {
		return "", false
	}
	return msg, true
}

// hasPostStart: p's container of that name has a postStart hook.
func hasPostStart(p *corev1.Pod, name string) bool {
	for _, c := range append(append([]corev1.Container(nil), p.Spec.Containers...), p.Spec.InitContainers...) {
		if c.Name == name {
			return c.Lifecycle != nil && c.Lifecycle.PostStart != nil
		}
	}
	return false
}

// hookWords is a hook's error without the kubelet's wrapping: from
// `Exec lifecycle hook ([...]) for Container "app" in Pod "x" failed -
// error: command 'x' exited with 3: , message: ""`, the part after
// "error: ". "" when it says no more than that the hook failed.
func hookWords(msg string) string {
	msg = oneLine(msg)
	if _, after, found := strings.Cut(msg, "failed - error: "); found {
		msg = after
	}
	msg = strings.TrimSuffix(msg, `, message: ""`)
	msg = strings.TrimRight(msg, ": ,")
	if msg == "" || msg == "PostStartHook failed" || msg == "FailedPostStartHook" {
		return ""
	}
	return shortText(MessageWords(msg), 200)
}

// restartsAgo is "3 restarts, the last 1m ago" (killed: "last killed 1m
// ago"). With none counted yet but a stop known, it says when it stopped
// ("stopped 16s ago", "killed 16s ago"): "0 restarts, the last" can't be.
func restartsAgo(n int32, t *corev1.ContainerStateTerminated, killed bool) string {
	stopped, last := "stopped ", ", the last "
	if killed {
		stopped, last = "killed ", ", last killed "
	}
	switch {
	case t == nil:
		return plural(int(n), "restart")
	case n == 0:
		return stopped + Age(t.FinishedAt.Time) + " ago"
	}
	return plural(int(n), "restart") + last + Age(t.FinishedAt.Time) + " ago"
}
