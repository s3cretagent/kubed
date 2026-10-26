package view

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

var argoNeeds = []schema.GroupVersionResource{kube.Applications}

// workloadNeeds also warms pods so Enter (drill to pods) is instant.
// workloadNeeds: events too, as the box explains a pod's problem from
// them (a kill by its alive check), as the pods list does.
var workloadNeeds = []schema.GroupVersionResource{kube.Applications, kube.Pods, kube.ReplicaSets, kube.Events}

func init() {
	register(kube.Deployments, View{
		Columns:  []string{"NAME", "READY", "UPDATED", "ARGOCD APP", "AGE"},
		Optional: []string{"ARGOCD APP"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			want, ready := num(o, "spec", "replicas"), num(o, "status", "readyReplicas")
			return []string{o.GetName(), fmt.Sprintf("%d/%d", ready, want),
				fmt.Sprintf("%d/%d", num(o, "status", "updatedReplicas"), want),
				argoCell(e, o), age(o)}, worseTone(worseTone(worseTone(readyTone(ready, want), podsTone(e, o)), pausedTone(o)), cantMakeTone(e, o))
		},
		Detail: workloadDetail,
		Drill:  func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector", "matchLabels") },
		Needs:  workloadNeeds,
		Caps:   CapScale | CapRestart | CapImage | CapXray | CapPodLogs,
	})

	register(kube.StatefulSets, View{
		Columns:  []string{"NAME", "READY", "ARGOCD APP", "AGE"},
		Optional: []string{"ARGOCD APP"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			want, ready := num(o, "spec", "replicas"), num(o, "status", "readyReplicas")
			return []string{o.GetName(), fmt.Sprintf("%d/%d", ready, want), argoCell(e, o), age(o)}, worseTone(readyTone(ready, want), podsTone(e, o))
		},
		Detail: workloadDetail,
		Drill:  func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector", "matchLabels") },
		Needs:  workloadNeeds,
		Caps:   CapScale | CapRestart | CapImage | CapXray | CapPodLogs,
	})

	register(kube.DaemonSets, View{
		Columns:  []string{"NAME", "READY", "UPDATED", "ARGOCD APP", "AGE"},
		Optional: []string{"ARGOCD APP"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			want, ready := num(o, "status", "desiredNumberScheduled"), num(o, "status", "numberReady")
			return []string{o.GetName(), fmt.Sprintf("%d/%d", ready, want),
				fmt.Sprintf("%d/%d", num(o, "status", "updatedNumberScheduled"), want), argoCell(e, o), age(o)}, readyTone(ready, want)
		},
		Detail: workloadDetail,
		Drill:  func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector", "matchLabels") },
		Needs:  workloadNeeds,
		Caps:   CapRestart | CapImage | CapXray | CapPodLogs,
	})

	register(kube.ReplicaSets, View{
		Columns: []string{"NAME", "DESIRED", "CURRENT", "READY", "AGE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			want, ready := num(o, "spec", "replicas"), num(o, "status", "readyReplicas")
			tone := readyTone(ready, want)
			if want == 0 {
				tone = ToneDim
			}
			return []string{o.GetName(), fmt.Sprint(want), fmt.Sprint(num(o, "status", "replicas")), fmt.Sprint(ready), age(o)}, tone
		},
		Detail: workloadDetail,
		Drill:  func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector", "matchLabels") },
		Needs:  []schema.GroupVersionResource{kube.Deployments, kube.Applications, kube.Pods},
		Caps:   CapScale | CapXray | CapPodLogs,
	})

	register(kube.Jobs, View{
		Columns: []string{"NAME", "STATUS", "DONE", "TOOK", "AGE"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			want := num(o, "spec", "completions")
			if want == 0 {
				want = 1
			}
			done := num(o, "status", "succeeded")
			status, tone := jobStatus(e, o, done >= want)
			held := (status == "Running" || strings.HasPrefix(status, "Retrying")) && heldOpenTry(e, o)
			if held { // as the app says: the try failed, the job hasn't
				status, tone = "Try failed (another container keeps it running)", ToneBad
			}
			if !held && (status == "Running" || strings.HasPrefix(status, "Retrying")) && jobRetrying(e, o) { // a try's pod ended, not counted yet: as the app says
				status, tone = fmt.Sprintf("Retrying (%s failed)", Plural(int(failedTries(e, o)), "try")), ToneWarn
				if liveTries(e, o) > 0 {
					status = fmt.Sprintf("Retrying (%s failed, another running)", Plural(int(failedTries(e, o)), "try"))
				}
				if triesUsedUp(e, o) {
					status, tone = "Failed (its last try just ended)", ToneBad
				}
			}
			return []string{o.GetName(), status, fmt.Sprintf("%d/%d", done, want), jobDuration(o), age(o)}, tone
		},
		Detail: workloadDetail,
		Drill:  func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector", "matchLabels") },
		Needs:  []schema.GroupVersionResource{kube.CronJobs, kube.Applications, kube.Pods},
		Caps:   CapXray | CapPodLogs,
	})

	register(kube.CronJobs, View{
		// Paused shows in SCHEDULE ("paused · every day…"), not a column of its own.
		Columns:  []string{"NAME", "SCHEDULE", "LAST RUN", "LAST SUCCESS", "RUNNING", "ARGOCD APP", "AGE"},
		Optional: []string{"ARGOCD APP", "AGE", "RUNNING"},
		Main:     "SCHEDULE",
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			active, _, _ := unstructured.NestedSlice(o.Object, "status", "active")
			suspend := "no"
			if str(o, "spec", "suspend") == "true" {
				suspend = "yes"
			}
			last := e.CronLastRun(o)
			ok, _ := time.Parse(time.RFC3339, str(o, "status", "lastSuccessfulTime"))
			// The last finished run failed: it stays flagged while the next
			// one runs, so the row doesn't jump around.
			tone := ToneNormal
			switch failed, kept := e.CronLastFinished(o); {
			case suspend == "yes":
				tone = ToneDim
			case kept && failed:
				tone = ToneBad // "failing", as the Watch line says
			case func() bool { n, _ := e.CronRunsStuck(o); return n > 0 }():
				tone = ToneBad // runs that can't start: failing too
			case e.CronLastRunGone(o):
				tone = ToneWarn // runs cleaned up: it started after the last success, and isn't running (home says it too)
			}
			sched := cronWords(str(o, "spec", "schedule"))
			if !strings.HasPrefix(sched, "never") { // a zone for a time that never comes says nothing
				sched += " " + cronZone(o)
			}
			if suspend == "yes" {
				sched = "paused · " + sched
			}
			return []string{o.GetName(), sched, agoOrNever(last), agoOrNever(ok),
				fmt.Sprint(max(len(active), e.CronRunning(o))), argoCell(e, o), age(o)}, tone
		},
		Detail: cronDetail,
		Drill: func(o *unstructured.Unstructured) *Drill {
			name := o.GetName()
			return &Drill{GVR: kube.Jobs, Title: "started by cron job " + name, Match: func(j *unstructured.Unstructured) bool {
				for _, ref := range j.GetOwnerReferences() {
					if ref.Kind == "CronJob" && ref.Name == name {
						return true
					}
				}
				return false
			}}
		},
		Needs: []schema.GroupVersionResource{kube.Applications, kube.Jobs}, // jobs: runs started by hand
		Caps:  CapTrigger | CapImage | CapXray,
	})
}

// podsTone is the worst of a workload's pods, as each pod's status says:
// all ready can still mean one keeps crashing between its short runs.
func podsTone(e *enrich.Enricher, o *unstructured.Unstructured) Tone {
	worst := ToneNormal
	if e == nil {
		return worst
	}
	for _, p := range e.OwnedPods(o) {
		if pod, err := ToPod(p); err == nil {
			worst = worseTone(worst, Explain(e, pod).Tone)
		}
	}
	if worst == ToneDim || worst == ToneOK {
		return ToneNormal
	}
	return worst
}

// worseTone is the more serious of two tones.
// cantMakeTone: a rollout stuck because Kubernetes can't make its new
// pods is bad, as home says, though its old pods are all ready.
func cantMakeTone(e *enrich.Enricher, o *unstructured.Unstructured) Tone {
	if e != nil && rolloutCantMake(e, o) != "" {
		return ToneBad
	}
	return ToneNormal
}

func worseTone(a, b Tone) Tone {
	if severity(b) > severity(a) {
		return b
	}
	return a
}

func readyTone(ready, want int64) Tone {
	switch {
	case want == 0:
		return ToneDim
	case ready == 0:
		return ToneBad
	case ready < want:
		return ToneWarn
	}
	return ToneNormal
}

func jobDuration(o *unstructured.Unstructured) string {
	start, err := time.Parse(time.RFC3339, str(o, "status", "startTime"))
	if err != nil {
		return "-"
	}
	end, err := time.Parse(time.RFC3339, str(o, "status", "completionTime"))
	if err != nil {
		if c, ok := enrich.Condition(o, "Failed"); ok && c.Status == "True" {
			end = conditionSince(o, "Failed") // a failed job stopped then
		} else {
			end = time.Now()
		}
	}
	return Age(time.Now().Add(-end.Sub(start))) // Age formats a span: "48s", "3h2m"
}

// jobStatus is a job's state in a word. Kubernetes 1.31+ sets
// FailureTarget or SuccessCriteriaMet on every job that ends, before Failed
// or Complete, and keeps them after: the final word wins, and "its last
// pods stopping" is said only while one really is.
func jobStatus(e *enrich.Enricher, o *unstructured.Unstructured, complete bool) (string, Tone) {
	if c, ok := enrich.Condition(o, "Failed"); ok && c.Status == "True" {
		return "Failed", ToneBad
	}
	if c, ok := enrich.Condition(o, "Complete"); ok && c.Status == "True" {
		return "Done", ToneDim
	}
	if c, ok := enrich.Condition(o, "FailureTarget"); ok && c.Status == "True" {
		if jobPodsStopping(e, o) {
			return "Failed (its last pods stopping)", ToneBad // failed already: Kubernetes finishes it once they stop
		}
		return "Failed", ToneBad
	}
	if c, ok := enrich.Condition(o, "SuccessCriteriaMet"); ok && c.Status == "True" {
		if jobPodsStopping(e, o) {
			return "Done (its last pods stopping)", ToneDim // its success policy is met: tries that failed before don't matter
		}
		return "Done", ToneDim
	}
	if suspended, _, _ := unstructured.NestedBool(o.Object, "spec", "suspend"); suspended {
		return "Paused", ToneDim
	}
	if complete {
		return "Done", ToneDim
	}
	if failed := num(o, "status", "failed"); failed > 0 {
		if num(o, "status", "active") > 0 {
			return fmt.Sprintf("Retrying (%s failed, another running)", Plural(int(failed), "try")), ToneWarn // as the app says
		}
		return fmt.Sprintf("Retrying (%s failed)", Plural(int(failed), "try")), ToneWarn // between tries
	}
	return "Running", ToneNormal // a run going on is normal, not something to check
}

// jobPodsStopping: a job has pods still shutting down, by its own count
// (status.terminating) or by its pods being deleted and not ended yet.
func jobPodsStopping(e *enrich.Enricher, o *unstructured.Unstructured) bool {
	if num(o, "status", "terminating") > 0 {
		return true
	}
	if e == nil {
		return false
	}
	for _, po := range e.OwnedPods(o) {
		phase := str(po, "status", "phase")
		if po.GetDeletionTimestamp() != nil && phase != "Succeeded" && phase != "Failed" {
			return true
		}
	}
	return false
}

// pausedTone: a deployment paused partway through a rollout is one to
// check, as home says.
func pausedTone(o *unstructured.Unstructured) Tone {
	if pausedPartwayWords(o) != "" {
		return ToneWarn
	}
	return ToneNormal
}

func workloadDetail(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	lines := workloadProblem(e, o)
	if words := pausedPartwayWords(o); words != "" {
		lines = append(lines, "Paused   "+words)
		if !slices.ContainsFunc(lines, func(l string) bool { return strings.HasPrefix(l, "Fix ") }) { // a problem's own Fix comes first
			lines = append(lines, "Fix      "+resumeFix(o, ReadOnly))
		}
	} else if paused, _, _ := unstructured.NestedBool(o.Object, "spec", "paused"); paused && o.GetKind() == "Deployment" {
		lines = append(lines, "Paused   "+pausedLine(o)) // nothing rolling out: nothing else says so
	}
	if words := noNodeWords(e, o); words != "" {
		lines = append(lines, "Nodes    "+words)
	}
	lines = append(lines, argoLine(e, o))
	spec := []string{"spec", "template", "spec"}
	if o.GetKind() == "CronJob" {
		spec = []string{"spec", "jobTemplate", "spec", "template", "spec"}
	}
	images := containerImages(o, spec...)
	if run, held := onDeleteImages(e, o); held != "" { // what its pods run, then what it is set to
		lines = append(lines, "Images   "+strings.Join(run, ", ")+"; "+held)
	} else if len(images) > 0 {
		lines = append(lines, "Images   "+strings.Join(images, ", "))
	}
	sa := str(o, "spec", "template", "spec", "serviceAccountName")
	if sa != "" {
		lines = append(lines, "Account  service account "+sa)
	}
	return lines
}

// containerImages are the images of the pod spec at path in o: the app's
// own first, then helpers that run alongside it, then setup that runs
// before it.
func containerImages(o *unstructured.Unstructured, path ...string) []string {
	var images, helpers, setup []string
	for _, list := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(o.Object, append(append([]string(nil), path...), list)...)
		for _, c := range containers {
			if m, ok := c.(map[string]any); ok {
				img := shortImage(fmt.Sprint(m["image"]))
				switch {
				case list == "containers":
					images = append(images, img)
				case m["restartPolicy"] == "Always":
					helpers = append(helpers, img+" (helper)")
				default:
					setup = append(setup, img+" (runs first)")
				}
			}
		}
	}
	return append(append(images, helpers...), setup...)
}

// onDeleteImages: a stateful set or daemon set that replaces a pod only
// once it is deleted (OnDelete), a pod of which doesn't run an image it
// is set to: the images its pods run, and what it is set to, in words.
// "" when its pods run what it is set to.
func onDeleteImages(e *enrich.Enricher, o *unstructured.Unstructured) (run []string, held string) {
	if e == nil || o.GetKind() != "StatefulSet" && o.GetKind() != "DaemonSet" || str(o, "spec", "updateStrategy", "type") != "OnDelete" {
		return nil, ""
	}
	set := containerImages(o, "spec", "template", "spec")
	var notRun []string
	for _, p := range e.OwnedPods(o) {
		if p.GetDeletionTimestamp() != nil { // on its way out: the next one takes what it is set to
			continue
		}
		has := containerImages(p, "spec")
		for _, img := range has {
			if !slices.Contains(run, img) {
				run = append(run, img)
			}
		}
		for _, img := range set {
			if !slices.Contains(has, img) && !slices.Contains(notRun, img) {
				notRun = append(notRun, img)
			}
		}
	}
	if len(notRun) == 0 {
		return nil, ""
	}
	return run, "set to " + strings.Join(notRun, ", ") + "; pods take it only once deleted (OnDelete)"
}

// workloadFixWhere is where a fix to a workload goes, as fixWhere says it
// for an app: Git for one ArgoCD manages, else an edit (e).
func workloadFixWhere(e *enrich.Enricher, o *unstructured.Unstructured) string {
	switch {
	case argoCell(e, o) != "":
		return "in Git"
	case ReadOnly:
		return "where it is defined"
	}
	return "(e)"
}

// noNodeWords: a daemon set that runs on no node (it wants a pod on none,
// as its current status says), in words; "" for any other. Its node
// selector "matches none" only when no node kubed knows has those labels:
// otherwise (taints, affinity) only that it runs on none.
func noNodeWords(e *enrich.Enricher, o *unstructured.Unstructured) string {
	if o.GetKind() != "DaemonSet" || num(o, "metadata", "generation") == 0 || num(o, "status", "observedGeneration") < num(o, "metadata", "generation") ||
		num(o, "status", "desiredNumberScheduled") != 0 {
		return ""
	}
	sel, _, _ := unstructured.NestedStringMap(o.Object, "spec", "template", "spec", "nodeSelector")
	nodes := e.List(kube.Nodes, "")
	if len(sel) == 0 || len(nodes) == 0 {
		return "it runs on no node now"
	}
	for _, n := range nodes {
		if labels.SelectorFromSet(sel).Matches(labels.Set(n.GetLabels())) {
			return "it runs on no node now"
		}
	}
	return "it runs on no node: its node selector matches none"
}

// workloadProblem is the Problem line of a workload that isn't well: the
// worst of its pods, said the way home says it, or why a job run failed.
func workloadProblem(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	if o.GetKind() == "CronJob" {
		if n, why := e.CronRunsStuck(o); n > 0 {
			fix := "Fix      fix it (e), then enter opens its runs: ctrl-d on the stuck ones"
			for _, po := range e.PodsOf(o) {
				if p, err := ToPod(po); err == nil && staleCronRun(e, po, p) {
					fix = "Fix      it runs another image now; the stuck runs still wait on the old one: enter opens its runs, ctrl-d on them"
					break
				}
			}
			return []string{fmt.Sprintf("Problem  %s %s and never %s", Plural(n, "run"), why, map[bool]string{true: "finishes", false: "finish"}[n == 1]), readOnlyFix(fix)}
		}
		return nil
	}
	worst, which := Status{Tone: ToneNormal}, ""
	var worstRestarts int32
	var worstContainer string
	var worstPod *corev1.Pod
	progressing, _ := enrich.Condition(o, "Progressing")
	stuck := o.GetKind() == "Deployment" && progressing.Status == "False"
	var copies []crashCopy // pods crashing: rivals for the one quoted
	var held []string      // failing pods it won't replace by itself (waitsOnPod)
	// Ranked as home ranks its headline: how bad, then one happening now
	// before one that has passed; a stuck rollout's reason by how bad only.
	rankOf := func(o *unstructured.Unstructured, pod *corev1.Pod, st Status) int {
		if stuck {
			return severity(st.Tone)
		}
		return versionRank(e, o, pod, st) // the new version's problem first, as home
	}
	worstRank := -1
	for _, p := range e.PodsOf(o) {
		pod, err := ToPod(p)
		if err != nil || leftover(pod) {
			continue
		}
		st := Explain(e, pod)
		if st.Tone != ToneBad && st.Tone != ToneWarn {
			continue
		}
		if st.Tone == ToneBad && waitsOnPod(o, pod) {
			held = append(held, pod.Name)
		}
		// Worse first; alike, the one that crashed most (as home and p say).
		name, restarts := crashingContainer(pod, st)
		if name != "" {
			copies = append(copies, crashCopy{rank: rankOf(p, pod, st), st: st, ns: pod.Namespace, pod: pod.Name, container: name, restarts: restarts})
		}
		crashes := strings.HasPrefix(st.Short, "Crashing") && strings.HasPrefix(worst.Short, "Crashing")
		// Two restarting: the one that restarted last, compared as home
		// compares them (as its stuck reason, or as its headline).
		later := st.Short == "Restarting" && worst.Short == "Restarting" && worstPod != nil &&
			(stuck || rankOf(p, pod, st) == worstRank) && restartedAt(pod, st).After(restartedAt(worstPod, worst))
		if rank := rankOf(p, pod, st); rank > worstRank || rank == worstRank && (crashes && restarts > worstRestarts+1 || later) { // ahead by more than one, as home
			worst, which, worstRestarts, worstContainer, worstPod, worstRank = st, p.GetName(), restarts, name, pod, rank
		}
	}
	if c, ok := keepNamed(e, copies, worstRank, worstContainer, which); ok { // the copy home names, as it does
		worst, which = c.st, c.pod
	}
	if which != "" { // the key to press goes on its own Fix line, as on home
		lines := []string{fmt.Sprintf("Problem  pod %s: %s: %s", which, worst.Short, withoutKeyHint(worst.Why))}
		if c, ok := enrich.Condition(o, "Progressing"); ok && c.Status == "False" && o.GetKind() == "Deployment" {
			why := "missed its deadline"
			if c.Reason != "ProgressDeadlineExceeded" { // ReplicaSetCreateError: a quota, a policy
				why = "can't make its new pods"
			}
			lines[0] = fmt.Sprintf("Problem  rollout stuck (%s): pod %s: %s: %s", why, which, worst.Short, withoutKeyHint(worst.Why)) // as home says
		}
		owner := o.GetKind()
		if top := e.TopOwner(o); top.GetKind() == "CronJob" {
			owner = "CronJob" // a cron job's run: fixed on its cron job
		}
		fix := neverStartedInGit(e, o, podFixFor(worst, owner))
		if worstPod != nil && !stuck && worstRank == (severity(ToneNormal)*2+1)*2+2 { // an older version's pod, as home says
			fix = oldVersionFix
		}
		if o.GetKind() == "Deployment" && strings.HasPrefix(worst.Short, "Can't pull image") && !pullSecretGone(worst.Why) && undoHelps(e, o) { // not back to a version that fails too
			fix = readOnlyFix("u to go back to the previous version, or i to fix the image name or tag")
			if maybe := undoState(e, o).maybe; maybe != "" {
				fix += " (" + maybe + ")"
			}
		} else if o.GetKind() == "Deployment" && fix != "" && fix != oldVersionFix { // why not u, as home says
			why := undoState(e, o).why
			if strings.HasPrefix(worst.Short, "Can't pull image") && !pullSecretGone(worst.Why) {
				fix += " (" + why + ")"
			} else {
				fix += undoNote(worst.Short, why)
			}
		}
		if worst.Tone == ToneBad && slices.Contains(held, which) { // the image is fine now: the one it waits for is the problem
			if first := waitsHeld(e, o); first != "" {
				fix = "it waits for " + waitsOnWords(first) + " (enter opens its pods, then ctrl-d on " + first + ")"
				if ReadOnly {
					fix = "it waits for " + waitsOnWords(first) + " (without --readonly)"
				}
			}
		}
		if fix != "" {
			lines = append(lines, "Fix      "+onItsOwner.ReplaceAllString(fix, "")) // this is the workload: no need to name it
		}
		return lines
	}
	if why := rolloutCantMake(e, o); why != "" { // as home says it, with its Fix
		return []string{"Problem  rollout stuck (can't make its new pods): " + why, "Fix      " + cantMakeFix + workloadFixWhere(e, o)}
	}
	if made, want, why, ok := cantMakeAll(e, o); ok { // as home says it, with its Fix
		text := fmt.Sprintf("Missing pods: Kubernetes can make only %d of %d copies: %s", made, want, why)
		if made == 0 {
			text = fmt.Sprintf("No pods: asks for %s but Kubernetes can't make them: %s", Plural(int(want), "copy"), why)
		}
		return []string{"Problem  " + text, "Fix      " + cantMakeFix + workloadFixWhere(e, o)}
	}
	if o.GetKind() == "Job" {
		c, ok := enrich.Condition(o, "Failed")
		if !ok || c.Status != "True" {
			c, ok = enrich.Condition(o, "FailureTarget") // failed already, its last pods stopping
		}
		if (!ok || c.Status != "True") && jobRetrying(e, o) && triesUsedUp(e, o) { // its last try failed; the job fails in a moment
			c, ok = enrich.Cond{Status: "True", Message: "its last try failed"}, true
		}
		if ok && c.Status == "True" {
			why := cmp.Or(c.Message, c.Reason)
			cause, causeFound := JobCause(e, o)
			if causeFound && cause != "" {
				why = cause // what each try ran into, not only that it gave up
			}
			lines := []string{"Problem  this run failed: " + MessageWords(why)}
			switch {
			case strings.Contains(why, "never ran: it couldn't download"): // no logs: its image is the fix
				lines = append(lines, "Fix      "+readOnlyFix("it never ran, so it has no logs: fix the image name or tag "+jobFixWhere(e, o)+", or check the registry access"))
			case strings.Contains(why, "never started"):
				lines = append(lines, "Fix      "+readOnlyFix(jobNeverStartedFix(e, o)))
			case strings.Contains(why, "ran out of memory"):
				fix := podFixFor(Status{Short: "Failed", Why: why}, map[bool]string{true: "CronJob", false: "Job"}[e.TopOwner(o).GetKind() == "CronJob"])
				lines = append(lines, "Fix      "+strings.Replace(fix, "p shows the logs from before", "enter opens its pods: l on the failed one shows why", 1)) // a run has no earlier logs: its pod's own do
			case e.TopOwner(o).GetKind() == "CronJob":
				lines = append(lines, "Fix      "+laterRunFix(e, o, readOnlyFix("enter opens its pods: l on the failed one shows why; the next run is on schedule (or t on its cron job runs it now)")))
			default:
				lines = append(lines, "Fix      enter opens its pods: l on the failed one shows why; fix it and create the job again")
			}
			if len(e.PodsOf(o)) == 0 { // its pods removed since (its cleanup, or by hand): as home says
				lines[1] = strings.Replace(lines[1], "enter opens its pods: l on the failed one shows why", "its failed pods are gone, and their logs with them (E shows its events)", 1)
				if !causeFound || cause == "" || cause == deletedGone { // nothing says a try failed by itself: as home says
					lines[1] = strings.Replace(lines[1], "fix it and create the job again", noCauseRunAgain, 1)
				}
			}
			if why == deletedStopping { // no try failed by itself: as home says
				lines[1] = strings.Replace(lines[1], "l on the failed one shows why", "l on the stopping one shows its logs", 1)
				lines[1] = strings.Replace(lines[1], "fix it and create the job again", "its pod was deleted, not failed by itself: create the job again once it has stopped", 1)
			}
			return lines
		}
		if jobRetrying(e, o) { // between tries or running another: what failed so far
			lines := []string{"Problem  " + Plural(int(failedTries(e, o)), "try") + " failed so far"}
			fix := "Fix      enter opens its pods: l on a failed one shows why; Kubernetes runs it again until its tries run out"
			if cause, ok := JobCause(e, o); ok && cause != "" {
				lines[0] += ": " + MessageWords(cause)
				if cause == deletedStopping { // no try failed by itself: as home says
					fix = strings.Replace(fix, "l on a failed one shows why", "l on the stopping one shows its logs", 1)
				}
			}
			return append(lines, fix)
		}
	}
	return nil
}

// heldOpenTry: a pod of the job whose try failed while another container
// keeps it running (so Kubernetes hasn't counted the try yet).
func heldOpenTry(e *enrich.Enricher, j *unstructured.Unstructured) bool {
	for _, po := range e.PodsOf(j) {
		p, err := ToPod(po)
		if err != nil || p.Spec.RestartPolicy != corev1.RestartPolicyNever || p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
			continue
		}
		failed := false // only a pod with a failed container pays for Explain
		for _, cs := range p.Status.ContainerStatuses {
			failed = failed || cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0
		}
		if failed && strings.Contains(Explain(e, p).Why, "keeps the pod running") {
			return true
		}
	}
	return false
}

// jobNeverStartedFix is the Fix for a job run whose command is missing:
// a job can't be changed once made, so the change goes where it comes from.
func jobNeverStartedFix(e *enrich.Enricher, j *unstructured.Unstructured) string {
	return neverStartedFix(jobFixWhere(e, j))
}

// jobFixWhere is where a job run's fix goes: a job can't be changed once
// made, so where it comes from.
func jobFixWhere(e *enrich.Enricher, j *unstructured.Unstructured) string {
	switch {
	case func() bool { _, ok := e.ManagedBy(j); return ok }():
		return "in Git"
	case e.TopOwner(j).GetKind() == "CronJob":
		return "in its cron job (gC, then e on it): the next run uses it"
	}
	return "where the job is defined and create the job again (a job can't be changed once made)"
}

// cronZone is the time zone a CronJob's schedule is in.
func cronZone(o *unstructured.Unstructured) string {
	if tz := str(o, "spec", "timeZone"); tz != "" {
		return "(" + tz + ")"
	}
	return "(UTC)"
}

// cronDetail is a CronJob's box: its schedule, when it runs next, then
// what every workload shows.
func cronDetail(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	expr := str(o, "spec", "schedule")
	loc := time.UTC // the control plane's clock, UTC on managed clusters
	if tz := str(o, "spec", "timeZone"); tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}
	sched := cronWords(expr) + " " + cronZone(o)
	if cronWords(expr) != expr {
		sched += " · written as " + expr
	}
	lines := []string{"Schedule " + sched}
	last := e.CronLastRun(o)
	ok, _ := time.Parse(time.RFC3339, str(o, "status", "lastSuccessfulTime"))
	// The run it started last hasn't finished (one failing or done with
	// its last pods stopping has), or it failed: then "it worked" is about
	// an older run that ended after it started (runs side by side).
	newestRunning, newestFailed, newestWorked := false, false, false
	if runs := e.CronRuns(o); len(runs) > 0 {
		newestRunning = !jobEnded(runs[0])
		for _, typ := range []string{"Failed", "FailureTarget"} {
			if c, found := enrich.Condition(runs[0], typ); found && c.Status == "True" {
				newestFailed = true
			}
		}
		// Complete, or SuccessCriteriaMet before Kubernetes records its
		// success; and it is the run the last run time is about (a newer one
		// may have failed and been removed since).
		newestWorked = !newestRunning && !newestFailed && !last.After(runs[0].GetCreationTimestamp().Add(2*time.Minute))
	}
	switch {
	case last.IsZero():
		lines = append(lines, "Last run never")
	case newestRunning && !ok.IsZero():
		lines = append(lines, "Last run "+Age(last)+" ago, still running; the last one that worked ended "+Age(ok)+" ago")
	case newestRunning:
		lines = append(lines, "Last run "+Age(last)+" ago, still running")
	case newestWorked || !ok.IsZero() && !last.After(ok.Add(2*time.Minute)) && !newestFailed:
		lines = append(lines, "Last run "+Age(last)+" ago, and it worked")
	case newestFailed && ok.After(last): // runs side by side: an older one finished after the newest started
		lines = append(lines, "Last run "+Age(last)+" ago failed; an earlier run finished fine "+Age(ok)+" ago")
	case ok.IsZero():
		lines = append(lines, "Last run "+Age(last)+" ago; it has never succeeded")
	default:
		lines = append(lines, "Last run "+Age(last)+" ago, but the last one that worked was "+Age(ok)+" ago (Enter shows its runs)")
	}
	// The newest finished run failed: say why, as its app's box does, and
	// first, so a short box keeps it.
	var problem []string
	for _, j := range e.CronRuns(o) {
		if c, found := enrich.Condition(j, "Complete"); found && c.Status == "True" {
			break
		}
		if c, found := enrich.Condition(j, "SuccessCriteriaMet"); found && c.Status == "True" {
			break // done, its last pods stopping: as the list's CronLastFinished says
		}
		c, found := enrich.Condition(j, "Failed")
		if !found || c.Status != "True" {
			c, found = enrich.Condition(j, "FailureTarget") // failed already, its last pods stopping
		}
		if found && c.Status == "True" {
			if cause, ok := JobCause(e, j); ok {
				which := "its last run"
				if newestRunning { // a newer run is going on: this one is the last that finished
					which = "the last run that finished"
				}
				problem = append(problem, "Problem  "+which+" failed: "+cause)
				if strings.Contains(cause, "it never started") {
					problem = append(problem, "Fix      "+neverStartedFix(workloadFixWhere(e, o))) // this cron job holds what its runs run
				}
			}
			break
		}
	}
	switch next, ok := NextRun(expr, time.Now(), loc); {
	case str(o, "spec", "suspend") == "true":
		lines = append(lines, "Paused   no runs start until it is resumed")
	case ok && len(sliceOf(o, "status", "active")) > 0 && str(o, "spec", "concurrencyPolicy") == "Forbid":
		lines = append(lines, "Next run in "+span(time.Until(next))+" ("+next.Local().Format("Mon 2 Jan 15:04")+" your time): skipped if this run is still going then (it allows one at a time)")
	case ok && len(sliceOf(o, "status", "active")) > 0 && str(o, "spec", "concurrencyPolicy") == "Replace":
		lines = append(lines, "Next run in "+span(time.Until(next))+" ("+next.Local().Format("Mon 2 Jan 15:04")+" your time): if this run is still going then, it is stopped and replaced")
	case ok:
		lines = append(lines, "Next run in "+span(time.Until(next))+" ("+next.Local().Format("Mon 2 Jan 15:04")+" your time)")
	}
	return append(append(problem, lines...), workloadDetail(e, o)...)
}

// onItsOwner is " (on its deployment)" and the like in a pod's Fix line.
var onItsOwner = regexp.MustCompile(` \(on its [a-z ]+\)`)
