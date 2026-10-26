package view

import (
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// App is what people mean by "my service": an ArgoCD Application, or a
// namespace where nothing is managed by ArgoCD. Home lists apps, worst first.
type App struct {
	Name, Namespace string
	Argo            *enrich.ArgoApp // nil when not managed by ArgoCD

	Status     Status // worst pod, ArgoCD and secret state, in plain words
	Ready      int
	Pods       int
	Restarts   int32
	LastDeploy time.Time
	// DeployBefore: Kubernetes no longer keeps the version that went out,
	// so LastDeploy is the oldest it has: it went out then or earlier.
	DeployBefore bool
	// DeploySince, with DeployBefore: its workload was made then, so it
	// went out no earlier (between DeploySince and LastDeploy).
	DeploySince time.Time
	DeployAfter bool      // it went out at LastDeploy or later (only a bound is known)
	ImageSince  time.Time // its images went out then; LastDeploy only changed settings
	// DeployUnknown: brought back at LastDeploy (or after it, with
	// DeployAfter), the version before it gone: when its settings first
	// went out is unknown.
	DeployUnknown bool
	// DeployMixed: DeployAfter because another of its workloads can only be
	// dated as a bound, which may be later than LastDeploy.
	DeployMixed bool
	// DeployPending: a newer version of a Deployment of it, with other
	// settings, has no ready pod yet (LastDeploy is still the old one's).
	DeployPending bool
	// DeployReturning: that version is an earlier one brought back.
	DeployReturning bool
	deploys         []enrich.DeployTime // each workload's, settled into the above
	// unranDeploys: bare pods that never ran. Their version never went
	// out, so they date the app only when nothing else does.
	unranDeploys []enrich.DeployTime
	// RunsNotPriced: it starts runs (Jobs) whose pods aren't in Monthly;
	// RunTTL, when set, is how long Kubernetes keeps a finished run (s).
	RunsNotPriced bool
	RunTTL        int64
	PodsSince     time.Time // its oldest running pod (not a job's) started: after a restart, later than LastDeploy
	Monthly       float64   // ≈ $ from each pod's share of its node
	Priced        bool
	Leftovers     int                        // evicted / finished pods still listed; not counted above
	Evicted       int                        // of Leftovers: pushed off a node and already replaced
	Stopping      int                        // of Leftovers: shutting down (a restart or scale-down), not finished yet
	CanUndo       bool                       // a deployment in trouble has an earlier version to go back to
	oldHead       bool                       // the headline is about a pod of an older version, mid-rollout
	noUndo        string                     // not CanUndo: why, for the headline's own deployment
	stuckNoUndo   string                     // the same for the stuck rollout's deployment
	headDep       *unstructured.Unstructured // the headline pod's deployment, if it has one
	Secrets       []FailingSecret            // ExternalSecrets of this app that can't sync
	Images        []string                   // what its pods run, the app's own first and helpers last
	// Crash names a container that keeps crashing, so its last run's
	// logs can say why (CrashRestarts tells one crash from the next).
	CrashNS, CrashPod, CrashContainer string
	// HeadNS/HeadPod: the pod the headline is about (p opens its earlier
	// logs, so the Fix line's "p shows" names the same pod).
	HeadNS, HeadPod string
	CrashRestarts   int32

	group string // how pods were matched: groupArgo, groupLabel, groupNamespace
	// stuckBy: its rollout is stuck because its pods have this problem.
	stuckBy Status
	// pausedJobs: finished pods of a paused (suspended) job, which runs
	// again once resumed.
	pausedJobs int
	// pausedJob: a job of it is paused (suspended), with or without pods.
	pausedJob bool
	// pausedPartway: a deployment of it paused partway through a rollout,
	// in words (pausedPartwayWords).
	pausedPartway string
	pausedDep     *unstructured.Unstructured // that deployment
	// pausedDeps: its paused deployments (spec.paused), partway or not.
	pausedDeps []*unstructured.Unstructured
	// rollingOut: a deployment of it partway through a rollout, old
	// pods still beside the new version's, in words (rollingOutWords).
	rollingOut string
	// noNode: a daemon set of it runs on no node, in words (noNodeWords).
	noNode string
	// heldImages: a workload of it (OnDelete) set to images its pods don't
	// run yet, in words (onDeleteImages).
	heldImages string
	// missing: copies its workloads ask for that Kubernetes refuses to
	// make (a quota, a policy); cantMake is why (cantMakeAll).
	missing  int
	cantMake string
	// stuckCreate: a deployment of it rolling out whose new version
	// Kubernetes can't make pods for, in words (rolloutCantMake), and
	// whether u helps it.
	stuckCreate     string
	stuckCreateUndo undoInfo
	// undoMaybe: u is offered, but may not help (undoInfo.maybe), for the
	// headline's deployment; stuckMaybe the same for the stuck one's.
	undoMaybe, stuckMaybe string
	// waitsOn: a failing pod of an older version that its stateful set
	// (waitsSet) waits for and won't replace by itself (waitsOnPod), the
	// first in turn; waitsHeld all of that set's pods held so.
	waitsOn, waitsSet string
	waitsHeld         []string
	// queuedJob: that paused job waits in a Kueue queue, not paused by hand.
	queuedJob bool
	// placeholders: pods that wait for a node by design (low priority).
	placeholders int
	// starting: copies starting up (not ready yet), not a job's runs.
	starting int
	// notReady: the status of its first pod starting up.
	notReady Status
	// jobKinds: Job (and CronJob) when a job of its failed or retries, for
	// the Fix line's wording only (kinds count its live pods).
	jobKinds map[string]bool
	// stuckNS/stuckPod: the pod whose problem stuckBy is.
	stuckNS, stuckPod string
	stuckRestarts     int32       // and its crashing container's restarts
	stuckContainer    string      // that container ("" when not crashing)
	headPod           *corev1.Pod // the pod HeadPod names, as read
	stuckPodObj       *corev1.Pod // and stuckPod
	stuckUndo         bool        // its deployment has an earlier version to go back to
	// headStatus: the headline HeadPod was set for; a headline that
	// replaces it (a failed job, a stuck rollout) is about something else.
	headStatus Status
	crashOld   bool // CrashPod is an older version's pod, mid-rollout
	// staleRuns: its cron job's runs can't pull an image the cron job no
	// longer uses (fixed since).
	staleRuns bool
	barePods  int // pods nothing runs (no owner at all)
	// podChains are what runs each of its pods (not stopping), up their
	// owner chains; wanted are the workloads whose replicas make up wants.
	podChains  [][]types.UID
	chainReady []bool // podChains' pods: ready or not
	wanted     map[types.UID]bool
	// evictedBare: evicted pods with no owner, which nothing makes again.
	evictedBare int
	// stoppingBare: of Stopping, pods nothing runs, which nothing makes again.
	stoppingBare int
	// doneNeverRan: of its finished pods (not evicted), those whose
	// containers never started (PodRan), so they have no logs.
	doneNeverRan int
	// fallback is how pods ArgoCD doesn't track are grouped on this
	// cluster (by label or by namespace), for joinsArgoApp.
	fallback string
	ownedNS  map[string]bool // namespaces whose untracked pods joined it
	// ownsUnmanaged: pods ArgoCD doesn't manage in its namespace belong to
	// this app (made by an operator it installs).
	ownsUnmanaged bool
	kinds         map[string]bool // workload kinds it has (Deployment, CronJob…), even with no pods
	crons         int             // its cron jobs, and how many of them are paused
	cronsPaused   int
	wants         int64                        // copies its Deployments and StatefulSets ask for
	firstCheck    bool                         // a pod not ready yet whose ready or startup check can't have passed yet
	wantObjs      []*unstructured.Unstructured // its workloads that ask for copies
	// podOwners: what runs each of its pods, running or stopping (the
	// pod's own UID when nothing known does); zeroed: its Deployments and
	// StatefulSets that ask for no copies now (scaled to 0, or deleted).
	podOwners, zeroed map[types.UID]bool
	neverRuns         []string // its cron jobs whose schedule never comes round
	// failedJob: the job its job headline (Job failed, Retrying, Try
	// failed) is about, when known: its Fix follows that job's owner (a
	// cron job's run or a one-off), not whatever else the app runs.
	failedJob *unstructured.Unstructured
	// failedNoCause: that job failed and kubed knows no cause from a try
	// (its pods gone, nothing in its events): Kubernetes' word only.
	failedNoCause bool
	// jobPodsGone: none of that job's own pods is left (the app may have
	// others, as a whole namespace does): its Fix can't point at one.
	jobPodsGone bool
	// cronGone: its cron job whose last run didn't succeed and isn't
	// kept (failed runs removed), in words, as gC flags it.
	cronGone string
}

// FailingSecret is an ExternalSecret that can't sync from its store.
type FailingSecret struct {
	Namespace, Name string
	Target          string    // the Kubernetes Secret it writes
	Since           time.Time // when it stopped syncing
	NeverSynced     bool      // it has never written its Secret's values
	UsedBy          int       // pods that read its Secret
}

// SecretCheck is what the UI looked up for a failing ExternalSecret: the
// store's error (from its events) and whether its Secret exists.
type SecretCheck struct {
	Error        string
	TargetExists bool
}

// How pods are matched to apps.
const (
	groupArgo      = "argo"      // ArgoCD Application that manages the pod
	groupLabel     = "label"     // app label or top owner (clusters without ArgoCD)
	groupNamespace = "namespace" // pods ArgoCD doesn't manage, on an ArgoCD cluster
)

// appLabels name a pod's app on clusters without ArgoCD, first match wins.
var appLabels = []string{"app.kubernetes.io/instance", "release", "app.kubernetes.io/name", "app"}

// labelApp is the app a pod belongs to by its labels, else its top owner
// (Deployment, CronJob…), else its namespace.
func labelApp(e *enrich.Enricher, pod *unstructured.Unstructured) string {
	l := pod.GetLabels()
	for _, k := range appLabels {
		if v := l[k]; v != "" {
			return v
		}
	}
	if top := e.TopOwner(pod); top != pod {
		return top.GetName()
	}
	if k := pod.GetKind(); k != "" && k != "Pod" { // a workload asked about itself (a Job): named as its pods are
		return workloadApp(pod) // its pods' label, or its own name
	}
	return pod.GetNamespace()
}

// OwnsWorkload reports whether a workload belongs to this app, the way its pods would
// (an app with no pods still owns the deployments it was listed for).
// ArgoCD apps say what they manage themselves.
func (a *App) OwnsWorkload(e *enrich.Enricher, w *unstructured.Unstructured) bool {
	if _, managed := e.ManagedBy(w); managed && a.group != groupArgo {
		return false
	}
	switch a.group {
	case groupLabel:
		return w.GetNamespace() == a.Namespace && workloadApp(w) == a.Name
	case groupNamespace:
		return w.GetNamespace() == a.Name
	}
	return false
}

// helperNames are sidecars that run next to an app, not the app itself.
var helperNames = []string{"nginx", "envoy", "istio", "linkerd", "proxy", "sidecar", "fluent", "datadog", "vault", "otel", "cloudsql", "git-sync", "tail-log", "log-shipper", "filebeat"}

// IsHelper reports whether a container runs next to the app (a proxy, a
// log shipper), judged by its name.
func IsHelper(container string) bool {
	for _, h := range helperNames {
		if strings.Contains(container, h) {
			return true
		}
	}
	return false
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}
	return 1
}

// workloadApp is the app a workload's pods would belong to: labelApp,
// read from its pod template.
func workloadApp(w *unstructured.Unstructured) string {
	path := []string{"spec", "template", "metadata", "labels"}
	if w.GetKind() == "CronJob" {
		path = []string{"spec", "jobTemplate", "spec", "template", "metadata", "labels"}
	}
	l, _, _ := unstructured.NestedStringMap(w.Object, path...)
	for _, k := range appLabels {
		if v := l[k]; v != "" {
			return v
		}
	}
	return w.GetName()
}

// leftover: a pod that already finished or was evicted. Its controller has
// replaced it; it says nothing about the app's health now.
func leftover(p *corev1.Pod) bool {
	switch p.Status.Reason {
	case "Evicted", "Shutdown", "NodeShutdown", "Terminated", "NodeAffinity", "OutOfcpu", "OutOfmemory":
		return true // pushed off or ended with its node, and already replaced
	}
	if p.Status.Phase == corev1.PodFailed {
		// A job's run that ended badly: the Job says whether that matters
		// ("Job failed"); a pod outliving its Job is history.
		if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "Job" {
			return true
		}
		// Its Job deleted without it (--cascade=orphan): the label stays.
		if p.Labels["batch.kubernetes.io/job-name"] != "" || p.Labels["job-name"] != "" {
			return true
		}
	}
	// A job's try that failed, its phase not yet saying so, nothing left
	// running in it: the same. (Sidecars still stopping: the pod's own
	// status says so until they have.)
	if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "Job" && p.Status.Phase != corev1.PodFailed {
		if failed, at := tryFailed(p); failed && !anyRunning(p) && time.Since(at) < 10*time.Minute { // not one stuck on a lost node
			return true
		}
	}
	if p.Status.Phase == corev1.PodSucceeded && p.DeletionTimestamp != nil && !stuckStopping(p) { // one stuck past its grace period stays a leftover, as before
		// A copy that exited cleanly as it was asked to stop (replaced, or
		// scaled down) reads Succeeded for its last moments: stopping, not
		// a finished run kept for its logs. A run (restart Never or
		// OnFailure: a job's, a bare one) is one.
		return p.Spec.RestartPolicy != corev1.RestartPolicyAlways && p.Spec.RestartPolicy != ""
	}
	return p.Status.Phase == corev1.PodSucceeded
}

// AppStages are the lists the apps home reads, in loading order: pods
// first (what the screen needs), then what names and prices apps, then the
// heavy owner lists used for "last deploy" and ArgoCD ownership.
var AppStages = [][]schema.GroupVersionResource{
	AppCore,
	{kube.ReplicaSets, kube.StatefulSets, kube.DaemonSets, kube.Jobs, kube.CronJobs, kube.Nodes, kube.ExternalSecrets, kube.ControllerRevisions},
	// Events and web routes last: events date a version brought back
	// after a go-back and a restart, routes give the boxes' Web lines (and,
	// against ingresses, which one gi opens; autoscalers, so + warns at once);
	// home shows without them.
	{kube.Events, kube.HTTPRoutes, kube.Ingresses, kube.HPAs},
}

// AppCore must be loaded before apps can be shown correctly: pods, and
// what decides which app a pod belongs to (ArgoCD apps and Deployments).
// ReplicaSets aren't needed for that: pods reach their Deployment through
// the pod-template-hash (enrich.controller). Events are never loaded for
// home.
var AppCore = []schema.GroupVersionResource{kube.Pods, kube.Applications, kube.Deployments}

const hoursPerMonthF = float64(hoursPerMonth)

// Apps groups every pod into its app and sums each app up. Workloads
// (Deployments, StatefulSets, CronJobs) give apps with no pods a row too:
// one scaled to 0 shows as Stopped instead of vanishing.
func Apps(e *enrich.Enricher, pods, apps, secrets, jobs, workloads []*unstructured.Unstructured) []*App {
	byKey := map[string]*App{}
	get := func(name, ns, group string, argo *enrich.ArgoApp) *App {
		// An ArgoCD app is one row whatever namespaces it touches; its
		// Application record often has no destination namespace at all.
		key := group + "\x00" + name + "\x00" + ns
		if group == groupArgo {
			key = group + "\x00" + name
		}
		if a, ok := byKey[key]; ok {
			if a.Namespace == "" || (a.Pods == 0 && a.Leftovers == 0 && ns != "") {
				a.Namespace = ns // where its pods actually run
			}
			return a
		}
		a := &App{Name: name, Namespace: ns, Argo: argo, group: group, Status: Status{"Running", "healthy", ToneNormal}}
		byKey[key] = a
		return a
	}

	// Every Argo app shows up, even with no pods (e.g. only a CronJob).
	argoByName := map[string]enrich.ArgoApp{}
	for _, o := range apps {
		a := enrich.Application(o)
		argoByName[a.Name] = a
		ns := str(o, "spec", "destination", "namespace")
		get(a.Name, ns, groupArgo, &a)
	}
	// With ArgoCD, pods it doesn't manage (kube-system…) group by namespace;
	// without ArgoCD, by app label so each service gets its own row.
	fallback := groupLabel
	if len(apps) > 0 {
		fallback = groupNamespace
	}

	type versionKey struct {
		app *App
		id  string
	}
	versionStart := map[versionKey]time.Time{}
	deployedAt := map[string]map[types.UID]enrich.DeployTime{} // per namespace, worked out once
	badPods := map[*App]int{}
	stuck := map[*App]string{} // apps with a rollout past its deadline → why
	recentCrashes := map[*App]int{}
	aliveKills := map[*App]int{}             // of recentCrashes: kills by its alive or startup check
	killChecks := map[*App]map[string]bool{} // which checks those were
	lastCrash := map[*App]time.Time{}
	canUndo := map[types.UID]undoInfo{} // per Deployment, worked out once a refresh
	undoOf := func(dep *unstructured.Unstructured) undoInfo {
		u, seen := canUndo[dep.GetUID()]
		if !seen {
			u = undoState(e, dep)
			canUndo[dep.GetUID()] = u
		}
		return u
	}
	crashers := map[*App][]string{}          // containers crashing now, each app's, as their headlines name them
	crashMost := map[*App]map[string]int32{} // and the most restarts of each, over its copies
	headRank := map[*App]int{}               // the headline's rank (see below)
	headCopies := map[*App][]crashCopy{}     // pods crashing, each with its rank: the headline's rivals
	stuckCopies := map[*App][]crashCopy{}    // and those a stuck rollout's reason could quote
	add := func(app *App, o *unstructured.Unstructured) {
		p, err := ToPod(o)
		if err != nil {
			return
		}
		if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "Job" {
			// A run, finished or not: say runs aren't priced and how long
			// they're kept, whether one is running now or not.
			app.RunsNotPriced = true
			if job := e.Controller(o); job != nil {
				if ttl := num(job, "spec", "ttlSecondsAfterFinished"); ttl > 0 && (app.RunTTL == 0 || ttl < app.RunTTL) {
					app.RunTTL = ttl
				}
			}
		}
		if leftover(p) {
			app.Leftovers++
			if p.Status.Reason == "Evicted" {
				app.Evicted++
				if len(p.OwnerReferences) == 0 {
					app.evictedBare++
				}
			} else if !PodRan(o) {
				app.doneNeverRan++
			}
			if job := e.Controller(o); job != nil && job.GetKind() == "Job" && str(job, "spec", "suspend") == "true" {
				app.pausedJobs++ // its owner is here and runs again once resumed: still an app
			}
			return
		}
		if app.podOwners == nil {
			app.podOwners = map[types.UID]bool{}
		}
		app.podOwners[e.TopOwner(o).GetUID()] = true
		if p.DeletionTimestamp != nil && Explain(e, p).Short == "Stopping" {
			// Shutting down on schedule: on its way out, not one of the
			// copies the app runs (kubectl's READY doesn't count it
			// either). One stuck long past its grace period still counts.
			app.Leftovers++
			app.Stopping++
			if metav1.GetControllerOf(p) == nil {
				app.stoppingBare++
			}
			if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "ReplicaSet" { // its version went out all the same (pushed off a node, replaced)
				app.deploys = append(app.deploys, deployTime(e, o, deployedAt))
			}
			return
		}
		app.Pods++
		var chain []types.UID
		for cur, i := e.Controller(o), 0; cur != nil && i < 4; cur, i = e.Controller(cur), i+1 {
			chain = append(chain, cur.GetUID())
		}
		if len(chain) > 0 || metav1.GetControllerOf(p) == nil { // what runs it isn't known yet: no claim
			app.podChains = append(app.podChains, chain)
			app.chainReady = append(app.chainReady, podFullyReady(p))
		}
		if metav1.GetControllerOf(p) == nil {
			app.barePods++
		}
		if ref := metav1.GetControllerOf(p); ref != nil && (ref.Kind == "DaemonSet" || ref.Kind == "Job") {
			// Copies nobody counts in replicas: READY counts them as they are.
			if app.kinds == nil {
				app.kinds = map[string]bool{}
			}
			app.kinds[ref.Kind] = true
		}
		for _, c := range p.Spec.Containers { // a helper is only what Kubernetes itself says runs alongside
			if img := shortImage(c.Image); !slices.Contains(app.Images, img) {
				app.Images = append(app.Images, img)
			}
		}
		for _, c := range p.Spec.InitContainers { // helpers that keep running, and setup that runs first
			img := shortImage(c.Image) + " (runs first)"
			if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
				img = shortImage(c.Image) + " (helper)"
			}
			if !slices.Contains(app.Images, img) {
				app.Images = append(app.Images, img)
			}
		}
		st := Explain(e, p)
		// An older version's pod names the crash only until the current
		// version's own does (its problem leads the headline).
		oldPod := oldVersionPod(e, o)
		if name, restarts := crashingContainer(p, st); name != "" && (app.CrashPod == "" || app.crashOld && !oldPod) { // the container its status names
			app.CrashNS, app.CrashPod, app.CrashContainer, app.CrashRestarts, app.crashOld = p.Namespace, p.Name, name, restarts, oldPod
		}
		if st.Short == "Crashing" && app.CrashPod == "" {
			for _, cs := range append(RunningStatuses(p), p.Status.InitContainerStatuses...) { // a failing setup step too
				if t := cs.LastTerminationState.Terminated; t != nil && t.Reason != "OOMKilled" && cs.RestartCount > 0 {
					app.CrashNS, app.CrashPod, app.CrashContainer, app.CrashRestarts = p.Namespace, p.Name, cs.Name, cs.RestartCount
					break
				}
			}
		}
		if st.Short == "Failed" && app.CrashPod == "" { // the Error line quotes the container that failed
			for _, cs := range p.Status.ContainerStatuses {
				if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
					app.CrashNS, app.CrashPod, app.CrashContainer = p.Namespace, p.Name, cs.Name
					break
				}
			}
		}
		if strings.HasPrefix(st.Short, "Can't pull image") && staleCronRun(e, o, p) {
			app.staleRuns = true
		}
		if st.Short == "Stopping" { // routine: counted, but says nothing about the app
			st = Status{"Running", "healthy", ToneNormal}
		}
		if top := e.TopOwner(o); top.GetKind() == "StatefulSet" || top.GetKind() == "DaemonSet" {
			if st.Tone == ToneBad && !app.CanUndo { // u goes back on them too, from their ControllerRevisions
				app.CanUndo = undoOf(top).helps
			}
			if st.Tone == ToneBad && (app.waitsOn == "" || app.waitsSet == top.GetName()) && waitsOnPod(top, p) {
				if app.waitsOn == "" {
					if held := waitsHeld(e, top); held != "" { // the one Kubernetes waits for, if held
						app.waitsOn, app.waitsSet = held, top.GetName()
					}
				}
				if app.waitsSet == top.GetName() {
					app.waitsHeld = append(app.waitsHeld, p.Name)
				}
			}
		}
		if top := e.TopOwner(o); top.GetKind() == "Deployment" {
			if st.Tone == ToneBad && !app.CanUndo { // a problem: can it go back? (before its rollout is marked stuck too)
				app.CanUndo = undoOf(top).helps
			}
			if c, ok := enrich.Condition(top, "Progressing"); ok && c.Status == "False" {
				// A pod of an older version isn't why the new one is stuck:
				// it counts as there, not for its own problem.
				ss := st
				if oldVersionPod(e, o) {
					ss = Status{Tone: ToneNormal}
				}
				// Several pods crashing alike: the one that crashed most says
				// why (as the headline does), not whichever came last.
				crashName, restarts := crashingContainer(p, ss)
				if crashName != "" { // a rival for the reason, picked or not (see keepNamed)
					which := map[bool]string{true: "new version", false: "first version"}[e.RolloutOf(top).Previous != nil]
					undo := undoOf(top).helps // not back to a version that fails too
					stuckCopies[app] = append(stuckCopies[app], crashCopy{severity(ss.Tone), ss, p.Namespace, p.Name, crashName, restarts,
						which + " of deployment/" + top.GetName() + " " + cantStartWhy(ss), undo})
				}
				fewer := app.stuckBy.Short != "" && severity(ss.Tone) == severity(app.stuckBy.Tone) && restarts <= app.stuckRestarts+1 // ahead by more than one, as the headline
				if fewer && ss.Short == "Restarting" && app.stuckBy.Short == "Restarting" && restartedAt(p, ss).After(restartedAt(app.stuckPodObj, app.stuckBy)) {
					fewer = false // restarted last, as the headline and the box say
				}
				if (stuck[app] == "" || ss.Tone == ToneBad || ss.Tone == ToneWarn) && !fewer {
					why := "missed its deadline"
					switch ss.Tone {
					case ToneBad:
						why = cantStartWhy(ss)
					case ToneWarn: // not ready, or waiting for a node: still the reason
						why = "missed its deadline: " + strings.ToLower(ss.Short) + ": " + ss.Why
					}
					which := "new version"
					if e.RolloutOf(top).Previous == nil {
						which = "first version" // nothing went out before it
					}
					app.CanUndo, app.stuckNoUndo, app.stuckMaybe = undoOf(top).helps, undoOf(top).why, undoOf(top).maybe // not back to a version that fails too
					stuck[app] = which + " of deployment/" + top.GetName() + " " + why
					if ss.Tone == ToneBad || ss.Tone == ToneWarn {
						app.stuckBy = ss // its pods' own problem: the Fix line's advice
						app.stuckNS, app.stuckPod, app.stuckRestarts, app.stuckPodObj = p.Namespace, p.Name, restarts, p
						app.stuckContainer, app.stuckUndo = crashName, app.CanUndo
					}
				}
			}
		}
		if st.Short == "Placeholder" {
			app.placeholders++ // not ready by design: not one it is short of
		}
		if !podFullyReady(p) && beforeFirstCheck(p) {
			app.firstCheck = true // a slow start, not a failure yet
		}
		if podFullyReady(p) {
			app.Ready++
		} else if ref := metav1.GetControllerOf(p); st.Short == "Starting" && (ref == nil || ref.Kind != "Job") {
			app.starting++ // a copy starting up (a job's run isn't one: it runs, then ends)
			if app.notReady == (Status{}) {
				app.notReady = st
			}
		}
		// How bad first, then one happening now (the pod isn't ready)
		// before one that has passed (killed for memory an hour ago and
		// running fine again, next to one crash-looping now).
		rank := versionRank(e, o, p, st)
		if _, seen := headRank[app]; !seen {
			headRank[app] = (severity(ToneNormal)*2+1)*2 + 1
		}
		name, restarts := crashingContainer(p, st)
		if name != "" {
			headCopies[app] = append(headCopies[app], crashCopy{rank: rank, st: st, ns: p.Namespace, pod: p.Name, container: name, restarts: restarts})
		}
		if name != "" && !slices.Contains(crashers[app], name) {
			crashers[app] = append(crashers[app], name)
		}
		if name != "" {
			if crashMost[app] == nil {
				crashMost[app] = map[string]int32{}
			}
			crashMost[app][name] = max(crashMost[app][name], restarts)
		}
		// Two crashing alike: the one that crashed more is the headline
		// (steady, unlike the latest, which swaps with every crash).
		// Copies of one container: more restarts than the headline's (ties
		// keep it, so it doesn't swap with each crash). Another container:
		// more than any copy of the headline's crashed.
		most := crashMost[app][app.CrashContainer]
		if name == app.CrashContainer {
			most = app.CrashRestarts + 1 // ahead by more than one: copies crashing alike would swap with each crash
		}
		moreCrashes := rank == headRank[app] && name != "" && strings.HasPrefix(app.Status.Short, "Crashing") && restarts > most
		// Two restarting now and then: the one that restarted last (it
		// stays so until either restarts again, so no swapping).
		later := rank == headRank[app] && st.Short == "Restarting" && app.Status.Short == "Restarting" &&
			restartedAt(p, st).After(restartedAt(app.headPod, app.Status))
		if rank > headRank[app] || moreCrashes || later {
			headRank[app] = rank
			app.Status = st
			app.HeadNS, app.HeadPod, app.headStatus, app.headPod = p.Namespace, p.Name, st, p
			app.oldHead = rank == (severity(ToneNormal)*2+1)*2+2 // an older version's pod: its Fix is the rollout's
			app.headDep = nil
			if top := e.TopOwner(o); top.GetKind() == "Deployment" || top.GetKind() == "StatefulSet" || top.GetKind() == "DaemonSet" {
				app.headDep = top // why going back is off is its own workload's (u goes back on each)
			}
			// The Error line and p: the pod the headline names.
			switch {
			case strings.HasPrefix(st.Short, "Crashing") && name != "": // the container its status names
				app.CrashNS, app.CrashPod, app.CrashContainer, app.CrashRestarts = p.Namespace, p.Name, name, restarts
			case strings.HasPrefix(st.Short, "Crashing"):
				for _, cs := range append(RunningStatuses(p), p.Status.InitContainerStatuses...) {
					if t := cs.LastTerminationState.Terminated; t != nil && t.Reason != "OOMKilled" && cs.RestartCount > 0 {
						app.CrashNS, app.CrashPod, app.CrashContainer, app.CrashRestarts = p.Namespace, p.Name, cs.Name, cs.RestartCount
						break
					}
				}
			case st.Short == "Failed":
				for _, cs := range p.Status.ContainerStatuses {
					if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
						app.CrashNS, app.CrashPod, app.CrashContainer, app.CrashRestarts = p.Namespace, p.Name, cs.Name, 0
						break
					}
				}
			}
			if app.CrashPod == p.Name && app.CrashNS == p.Namespace {
				app.crashOld = app.oldHead
			}
		}
		if st.Tone == ToneBad {
			badPods[app]++
		}
		app.Restarts += SetupRestarts(p) // a setup step failing over and over, as on the pod
		for _, cs := range RunningStatuses(p) {
			app.Restarts += cs.RestartCount
			if t := newestStop(cs); t != nil && t.Reason != "Completed" && time.Since(t.FinishedAt.Time) < 24*time.Hour {
				recentCrashes[app]++ // each pod alone may look fine; together they keep falling over
				if check, _, killed := e.KilledByCheck(p.Namespace, p.Name, cs.Name, t.FinishedAt.Time, t.ExitCode); killed && t.Reason != "OOMKilled" {
					aliveKills[app]++ // killed by its check, not crashed by itself
					if killChecks[app] == nil {
						killChecks[app] = map[string]bool{}
					}
					killChecks[app][check] = true
				}
				if t.FinishedAt.After(lastCrash[app]) {
					lastCrash[app] = t.FinishedAt.Time
				}
			}
		}
		ref := metav1.GetControllerOf(o)
		if (ref == nil || ref.Kind != "Job") && str(o, "status", "phase") == "Running" {
			if t := o.GetCreationTimestamp().Time; app.PodsSince.IsZero() || t.Before(app.PodsSince) {
				app.PodsSince = t
			}
		}
		switch owner := e.Controller(o); {
		case ref != nil && ref.Kind == "Job":
			// a job's run is not a deploy
		case ref == nil && !PodRan(o):
			app.unranDeploys = append(app.unranDeploys, deployTime(e, o, deployedAt))
		case owner != nil && owner.GetKind() != "ReplicaSet" && owner.GetKind() != "Deployment":
			// StatefulSets, DaemonSets: a version went out when its oldest
			// pod started. A pod replaced later (evicted, node gone) is not
			// a new version.
			if t, ok := e.RevisionDeployTime(owner); ok { // its revisions say, restarts aside
				app.deploys = append(app.deploys, enrich.DeployTime{At: t.At, Before: t.Before, Since: t.Since})
				break
			}
			key := versionKey{app, string(owner.GetUID()) + "/" + o.GetLabels()["controller-revision-hash"]}
			if t, seen := versionStart[key]; !seen || o.GetCreationTimestamp().Time.Before(t) {
				versionStart[key] = o.GetCreationTimestamp().Time
			}
		default:
			app.deploys = append(app.deploys, deployTime(e, o, deployedAt))
		}
		// A run (a Job's pod) comes and goes: priced while it runs, the
		// monthly figure jumps. Left out, and the box says so.
		if cost, ok := podMonthly(e, o); ok && (ref == nil || ref.Kind != "Job") {
			app.Monthly += cost
			app.Priced = true
		}
	}

	// Pods ArgoCD manages first; that tells us which namespace each app
	// runs in. Then the rest.
	argoNamespaces := map[string][]*App{}
	// An ArgoCD app also owns the namespace its resources are in, even
	// before (or without) any pod it tracks: its untracked job pods join it.
	for _, o := range apps {
		app := byKey[groupArgo+"\x00"+o.GetName()]
		if app == nil || app.Argo == nil {
			continue
		}
		count := map[string]int{}
		best := ""
		for _, r := range app.Argo.Resources {
			if r.Namespace != "" {
				count[r.Namespace]++
				if count[r.Namespace] > count[best] {
					best = r.Namespace
				}
			}
		}
		if best != "" && !slices.Contains(argoNamespaces[best], app) {
			argoNamespaces[best] = append(argoNamespaces[best], app)
		}
	}
	var unmanaged []*unstructured.Unstructured
	for _, o := range pods {
		argo, ok := e.ManagedBy(o)
		if !ok || argo.Name == "" {
			unmanaged = append(unmanaged, o)
			continue
		}
		if full, ok := argoByName[argo.Name]; ok {
			argo = full
		}
		app := get(argo.Name, o.GetNamespace(), groupArgo, &argo)
		if !slices.Contains(argoNamespaces[o.GetNamespace()], app) {
			argoNamespaces[o.GetNamespace()] = append(argoNamespaces[o.GetNamespace()], app)
		}
		add(app, o)
	}
	for _, o := range unmanaged {
		ns := o.GetNamespace()
		switch owner := namespaceOwner(argoNamespaces[ns], ns); {
		case owner != nil && joinsArgoApp(e, owner, o, fallback):
			// Created by an operator inside an ArgoCD app's namespace
			// (typesense, victoria-metrics…): it belongs to that app.
			owner.ownsUnmanaged, owner.fallback = true, fallback
			if owner.ownedNS == nil {
				owner.ownedNS = map[string]bool{}
			}
			owner.ownedNS[ns] = true
			if owner.Pods == 0 && owner.Namespace != ns { // its first pods: this is where it runs
				owner.Namespace = ns
			}
			add(owner, o)
		case fallback == groupLabel:
			add(get(labelApp(e, o), ns, groupLabel, nil), o)
		default:
			add(get(ns, ns, groupNamespace, nil), o)
		}
	}
	// Workloads ArgoCD doesn't manage: the app their pods would join.
	for _, w := range workloads {
		ns := w.GetNamespace()
		var app *App
		if argo, ok := e.ManagedBy(w); ok && argo.Name != "" {
			if full, ok := argoByName[argo.Name]; ok {
				argo = full
			}
			app = get(argo.Name, ns, groupArgo, &argo) // usually listed already, from its Application
		} else if owner := namespaceOwner(argoNamespaces[ns], ns); owner != nil {
			app = owner
		} else if fallback == groupLabel {
			app = get(workloadApp(w), ns, groupLabel, nil)
		} else {
			app = get(ns, ns, groupNamespace, nil)
		}
		if app == nil {
			continue
		}
		if app.kinds == nil {
			app.kinds = map[string]bool{}
		}
		app.kinds[w.GetKind()] = true
		if w.GetKind() == "CronJob" {
			app.crons++
			if str(w, "spec", "suspend") == "true" {
				app.cronsPaused++
			} else if app.cronGone == "" && e.CronLastRunGone(w) {
				app.cronGone = cronGoneWords(e, w)
			}
		}
		if words := pausedPartwayWords(w); words != "" && app.pausedPartway == "" {
			app.pausedPartway, app.pausedDep = words, w
		}
		if paused, _, _ := unstructured.NestedBool(w.Object, "spec", "paused"); paused && w.GetKind() == "Deployment" {
			app.pausedDeps = append(app.pausedDeps, w)
		}
		if words := rollingOutWords(w); words != "" && app.rollingOut == "" {
			app.rollingOut = words
		}
		if words := noNodeWords(e, w); words != "" && app.noNode == "" {
			app.noNode = words
		}
		if _, held := onDeleteImages(e, w); held != "" && app.heldImages == "" {
			app.heldImages = held
		}
		if w.GetKind() != "CronJob" {
			if w.GetDeletionTimestamp() == nil { // being deleted: its copies don't come back
				app.wants += max(num(w, "spec", "replicas"), 0)
				if app.wanted == nil {
					app.wanted = map[types.UID]bool{}
				}
				app.wanted[w.GetUID()] = true
			}
			_, set, _ := unstructured.NestedFieldNoCopy(w.Object, "spec", "replicas") // unset is 1
			// Deleted with orphan deletion (kubectl delete --cascade=orphan):
			// its pods stay, so they are not going away.
			gone := w.GetDeletionTimestamp() != nil && !slices.Contains(w.GetFinalizers(), "orphan")
			if (w.GetKind() == "Deployment" || w.GetKind() == "StatefulSet") && (set && num(w, "spec", "replicas") == 0 || gone) {
				if app.zeroed == nil {
					app.zeroed = map[types.UID]bool{}
				}
				app.zeroed[w.GetUID()] = true
			}
			if num(w, "spec", "replicas") > 0 {
				app.wantObjs = append(app.wantObjs, w) // why it can't make them is read only if none exist
			}
			if why := rolloutCantMake(e, w); why != "" && app.stuckCreate == "" {
				which := map[bool]string{true: "new version", false: "first version"}[e.RolloutOf(w).Previous != nil]
				app.stuckCreate = which + " of deployment/" + w.GetName() + " can't make its pods: " + why
				app.stuckCreateUndo = undoOf(w)
			}
			if made, want, why, ok := cantMakeAll(e, w); ok && (w.GetKind() != "ReplicaSet" || metav1.GetControllerOf(w) == nil) {
				app.missing += int(want - made)
				if app.cantMake == "" {
					app.cantMake = why
				}
			}
		} else if words := cronWords(str(w, "spec", "schedule")); strings.HasPrefix(words, "never") && str(w, "spec", "suspend") != "true" {
			app.neverRuns = append(app.neverRuns, "cron job "+w.GetName()+": its schedule names "+
				strings.TrimSuffix(strings.TrimPrefix(words, "never: "), " doesn't exist")+", a date that doesn't exist")
		}
	}
	// An ArgoCD app with no pods shows where its things are: the namespace
	// most of its resources use (its destination is often just "default").
	for _, a := range byKey {
		if a.group != groupArgo || a.Pods > 0 || a.Argo == nil {
			continue
		}
		count := map[string]int{}
		best := a.Namespace
		for _, r := range a.Argo.Resources {
			if r.Namespace != "" {
				count[r.Namespace]++
				if count[r.Namespace] > count[best] {
					best = r.Namespace
				}
			}
		}
		a.Namespace = best
	}
	for k, t := range versionStart {
		k.app.deploys = append(k.app.deploys, enrich.DeployTime{At: t})
	}
	for _, a := range byKey {
		settleDeployed(a)
	}

	// A failing ExternalSecret counts against the app that manages it, or
	// the app that owns its namespace, not every app sharing the namespace.
	failingSecrets := map[*App][]FailingSecret{}
	for _, es := range secrets {
		s := enrich.ExternalSecret(es)
		if s.Ready != "False" {
			continue
		}
		var owner *App
		if argo, ok := e.ManagedBy(es); ok {
			owner = byKey[groupArgo+"\x00"+argo.Name]
		}
		if owner == nil {
			owner = namespaceOwner(argoNamespaces[es.GetNamespace()], es.GetNamespace())
		}
		if owner == nil && fallback == groupLabel {
			owner = byKey[groupLabel+"\x00"+es.GetNamespace()+"\x00"+es.GetNamespace()]
		}
		if owner == nil { // nobody claims it: list it under its namespace, never drop it
			owner = get(es.GetNamespace(), es.GetNamespace(), groupNamespace, nil)
		}
		if owner != nil {
			used := 0
			for _, p := range pods {
				if p.GetNamespace() == es.GetNamespace() && enrich.PodSecretNames(p)[s.Target] {
					used++
				}
			}
			failingSecrets[owner] = append(failingSecrets[owner], FailingSecret{
				Namespace: es.GetNamespace(), Name: s.Name, Target: s.Target,
				Since: conditionSince(es, "Ready"), NeverSynced: s.RefreshedAt.IsZero() || s.RefreshedAt.Unix() <= 0,
				UsedBy: used,
			})
		}
	}

	// A job whose latest run failed in the last day counts against its app.
	failedJobs := map[*App]string{}
	noCause := map[*App]bool{}                            // for failedJobs: no try's cause known
	retrying := map[*App]string{}                         // a try failed, the next not started yet: not "Running"
	jobOf := map[*App]*unstructured.Unstructured{}        // the job those say, for the Error line
	jobAt := map[*App]time.Time{}                         // and when it failed: the newest wins
	jobOwner := func(j *unstructured.Unstructured) *App { // the app a job counts against
		var owner *App
		if argo, ok := e.ManagedBy(j); ok {
			owner = byKey[groupArgo+"\x00"+argo.Name]
		}
		if owner == nil {
			owner = namespaceOwner(argoNamespaces[j.GetNamespace()], j.GetNamespace())
		}
		if owner == nil && fallback == groupLabel {
			owner = get(labelApp(e, j), j.GetNamespace(), groupLabel, nil)
		}
		if owner == nil {
			owner = get(j.GetNamespace(), j.GetNamespace(), groupNamespace, nil)
		}
		return owner
	}
	for _, j := range latestRuns(e, jobs) {
		if str(j, "spec", "suspend") == "true" && !jobEnded(j) {
			if cj := e.Controller(j); cj == nil || cj.GetKind() != "CronJob" { // a cron job's run: its cron job says it
				owner := jobOwner(j)
				owner.pausedJob = true // listed (Paused) even before it ever ran
				if j.GetLabels()["kueue.x-k8s.io/queue-name"] != "" {
					owner.queuedJob = true // Kueue holds it until it is let in
				}
			}
			continue
		}
		c, ok := enrich.Condition(j, "Failed")
		when := conditionSince(j, "Failed")
		if ft, ftOK := enrich.Condition(j, "FailureTarget"); ftOK && ft.Status == "True" {
			switch {
			case !ok || c.Status != "True": // failed already, its last pods stopping
				c, ok, when = ft, true, conditionSince(j, "FailureTarget")
			default:
				// The moment it failed, kept once marked Failed (after its last
				// pods stopped): the time doesn't jump as they finish, and
				// :changes and home say the same.
				when = enrich.JobFailedAt(j)
			}
		}
		waiting := false
		if !ok || c.Status != "True" {
			// Between tries: one failed lately, none runs, and the job
			// hasn't ended, isn't ending (FailureTarget, SuccessCriteriaMet
			// come first in newer Kubernetes) and isn't paused.
			waiting = jobRetrying(e, j)
			if !waiting {
				continue
			}
		} else if time.Since(when) > failedJobWindow {
			continue
		} else if lr := lastTryEnd(e, j); !lr.IsZero() && lr.Before(when) && when.Sub(lr) < time.Minute {
			when = lr // when its last try ended, as the moment before the job marked it said
		}
		if cj := e.Controller(j); cj != nil && cj.GetKind() == "CronJob" && str(cj, "spec", "suspend") == "true" {
			continue // paused: its last failure isn't failing now (the cron jobs list agrees)
		}
		owner := jobOwner(j)
		if owner.jobKinds == nil { // its pods may all have finished (leftovers say nothing of kinds)
			owner.jobKinds = map[string]bool{}
		}
		owner.jobKinds["Job"] = true
		if cj := e.Controller(j); cj != nil && cj.GetKind() == "CronJob" {
			owner.jobKinds["CronJob"] = true
		}
		why := MessageWords(c.Message) // as the job's box and :changes say it
		if why == "" {
			why = c.Reason
		}
		cause, causeFound := JobCause(e, j)
		if causeFound {
			why = cause
		}
		if why == "" { // used up its tries a moment ago: no condition yet, and its pod gone
			why = "its last try failed"
		}
		if waiting && triesUsedUp(e, j) { // its last try failed: the job fails in a moment
			waiting, when = false, failedAt(e, j)
			if when.IsZero() { // no container ended with an error code (killed some other way)
				when = time.Now()
			}
		}
		if waiting {
			failed := failedTries(e, j)
			cause, ok := JobCause(e, j)
			if !ok || cause == "" {
				cause = "its failed pod is gone, and why with it" // deleted since
				if deletingPod(e, j) {
					cause = deletedStopping // counted failed as soon as it was asked to stop
				}
			}
			if at := failedAt(e, j); at.After(jobAt[owner]) || jobOf[owner] == nil {
				words := fmt.Sprintf("job %s: %s failed (%s); Kubernetes tries again after a pause", j.GetName(), Plural(int(failed), "try"), cause)
				if liveTries(e, j) > 0 {
					words = fmt.Sprintf("job %s: try %d running, %s failed before (%s)", j.GetName(), failed+1, Plural(int(failed), "try"), cause)
					if !oneAtATime(j) { // tries that succeeded or run side by side: no single try number
						words = fmt.Sprintf("job %s: running again, %s failed before (%s)", j.GetName(), Plural(int(failed), "try"), cause)
					}
				}
				retrying[owner] = words
				jobOf[owner], jobAt[owner] = j, at
			}
			continue
		}
		if when.After(jobAt[owner]) || failedJobs[owner] == "" {
			failedJobs[owner] = fmt.Sprintf("job %s failed %s ago: %s", j.GetName(), Age(when), why)
			noCause[owner] = !causeFound || cause == "" || cause == deletedGone // stopped by hand: nothing to fix
			jobOf[owner], jobAt[owner] = j, when
		}
	}

	out := make([]*App, 0, len(byKey))
	named := map[string]bool{}
	for _, a := range byKey {
		// Copies crashing alike: the one named last refresh stays while
		// within one of the most crashed (each pass above keeps the first
		// by name on near ties, so the two would swap as they crash).
		if c, ok := keepNamed(e, headCopies[a], headRank[a], a.CrashContainer, a.CrashPod); ok && strings.HasPrefix(a.Status.Short, "Crashing") && a.HeadPod == a.CrashPod {
			a.Status, a.HeadNS, a.HeadPod, a.headStatus = c.st, c.ns, c.pod, c.st
			a.CrashNS, a.CrashPod, a.CrashRestarts = c.ns, c.pod, c.restarts
		}
		if c, ok := keepNamed(e, stuckCopies[a], severity(a.stuckBy.Tone), a.stuckContainer, a.stuckPod); ok && stuck[a] != "" {
			stuck[a], a.stuckBy, a.stuckNS, a.stuckPod, a.stuckRestarts, a.stuckUndo = c.stuck, c.st, c.ns, c.pod, c.restarts, c.undo
		}
		if why := failedJobs[a]; why != "" && severity(a.Status.Tone) < severity(ToneBad) {
			a.Status = Status{"Job failed", why, ToneBad}
			a.failedNoCause = noCause[a]
			a.CrashNS, a.CrashPod, a.CrashContainer = failedRun(e, jobOf[a]) // the Error line quotes that run
			a.failedJob = topJob(e, jobOf[a])
			a.jobPodsGone = jobOf[a] != nil && len(e.PodsOf(jobOf[a])) == 0
		}
		if why := retrying[a]; why != "" && severity(a.Status.Tone) < severity(ToneWarn) {
			a.Status = Status{"Retrying", why, ToneWarn}
			a.CrashNS, a.CrashPod, a.CrashContainer = failedRun(e, jobOf[a])
			a.failedJob = topJob(e, jobOf[a])
			a.jobPodsGone = jobOf[a] != nil && len(e.PodsOf(jobOf[a])) == 0
		}
		if a.cronGone != "" && severity(a.Status.Tone) < severity(ToneWarn) { // as gC flags it, after what is going on now (a job retrying)
			a.Status = Status{"Not succeeding", a.cronGone, ToneWarn}
		}
		// Still at it: crashes earlier today that have since settled are history.
		if n := recentCrashes[a]; n >= 3 && time.Since(lastCrash[a]) < 2*time.Hour && severity(a.Status.Tone) < severity(ToneWarn) {
			what := "crashed"
			if aliveKills[a] == n { // all of them: else some crashed by themselves
				what = "were killed by their " + checksWords(killChecks[a])
			}
			a.Status = Status{"Restarting", fmt.Sprintf("%d of its containers %s in the last day, the last %s ago; :changes shows when", n, what, Age(lastCrash[a])), ToneWarn}
		} else if n > 0 && a.Status.Short == "Running" && a.Status.Tone == ToneNormal {
			// Fine now, but not all day: worth a line, not an alarm.
			what := "crashed"
			if aliveKills[a] == n {
				what = "killed by its " + checksWords(killChecks[a])
			}
			a.Status.Why = fmt.Sprintf("healthy now; %s in the last day, the last time %s ago; p shows why", what, Age(lastCrash[a]))
		}
		if a.Status.Short == "Failed" && (a.kinds["Job"] || a.kinds["CronJob"]) {
			a.Status.Short = "Job failed"
			if a.headPod != nil { // the run its headline pod is
				if ref := metav1.GetControllerOfNoCopy(a.headPod); ref != nil && ref.Kind == "Job" {
					if j, ok := e.Get(kube.Jobs, a.HeadNS, ref.Name); ok {
						a.failedJob = topJob(e, j)
					}
				}
			}
			if strings.Contains(a.Status.Why, "keeps the pod running") { // the job fails only once that stops (or tries again)
				a.Status.Short = "Try failed"
			} // a run that ended badly, not the app failing as it runs
			if j, ok := stoppingTryJob(e, a); ok && badPods[a] == 1 && failedJobs[a] == "" { // the try failed, but Kubernetes runs another (and nothing else is failing)
				a.Status = Status{"Retrying", fmt.Sprintf("job %s: %s; then Kubernetes tries again", j.GetName(), strings.TrimSuffix(a.Status.Why, ", then the pod ends")), ToneWarn}
			}
		}
		if strings.HasPrefix(a.Status.Short, "Crashing") { // others crashing too: say so, or fixing one looks like enough
			var others []string
			for _, n := range crashers[a] {
				if n != a.CrashContainer {
					others = append(others, n)
				}
			}
			if len(others) > 0 {
				same := a.Status == a.headStatus
				a.Status.Why = beforeKeyHint(a.Status.Why, "; "+strings.Join(others, ", ")+" crashing too")
				if same {
					a.headStatus = a.Status
				}
			}
		}
		if why := stuck[a]; why != "" { // the rollout is the headline; the pod problem is its reason
			a.Status = Status{"Rollout stuck", why, ToneBad}
			badPods[a] = 0
			a.HeadNS, a.HeadPod, a.headStatus = a.stuckNS, a.stuckPod, a.Status // the pod its reason quotes
			if a.stuckPod != "" {
				a.CanUndo = a.stuckUndo // the stuck deployment's own, not another's
			}
			if a.stuckContainer != "" { // its crash is the Error line's, so the two name one pod
				a.CrashNS, a.CrashPod, a.CrashContainer, a.CrashRestarts = a.stuckNS, a.stuckPod, a.stuckContainer, a.stuckRestarts
			}
		}
		if a.CrashPod != "" && a.CrashContainer != "" && (strings.HasPrefix(a.Status.Short, "Crashing") || a.Status.Short == "Rollout stuck") {
			named[a.CrashNS+"/"+a.CrashPod+"/"+a.CrashContainer] = true // it keeps its place next refresh
		}
		if failing := failingSecrets[a]; len(failing) > 0 {
			a.Secrets = failing
			if st := secretStatus(failing); severity(st.Tone) > severity(a.Status.Tone) {
				a.Status = st
			}
		}
		if a.Status.Short == "Not ready" && a.Status.Tone == ToneWarn && a.wants > 0 && a.Ready == 0 && !a.firstCheck {
			a.Status.Tone = ToneBad // it asks for copies and none serves: failing, as the deployments list says
		}
		if a.Argo != nil && severity(a.Status.Tone) < severity(ToneBad) {
			switch {
			case a.Argo.Health == "Degraded":
				a.Status = Status{"Failing", "ArgoCD marks part of it as failing", ToneBad}
			case a.Argo.BuildError != "":
				a.Status = Status{"Can't build", "ArgoCD can't build this app from Git, so new changes won't deploy: " + shortText(oneLine(a.Argo.BuildError), 200), ToneWarn}
			case a.Argo.Health == "Missing":
				a.Status = Status{"Missing parts", "some of what Git defines isn't in the cluster", ToneBad}
			case a.Argo.Sync == "OutOfSync" && a.Pods == 0 && onlyWorkloadsDiffer(*a.Argo) && severity(a.Status.Tone) < severity(ToneWarn):
				// Nothing running and out of sync: almost always scaled to 0
				// by hand or by a scheduled pause, not a problem to chase.
				a.Status = Status{"Stopped", "no pods; " + outOfSyncWhy(*a.Argo), ToneDim}
			case a.Argo.Sync == "OutOfSync" && severity(a.Status.Tone) < severity(ToneWarn):
				a.Status = Status{"Out of sync", outOfSyncWhy(*a.Argo), ToneWarn}
			}
		}
		if a.pausedPartway != "" && a.Pods > 0 && a.Status.Tone == ToneNormal && onlyDeployments(a) {
			a.Status = Status{"Paused", a.pausedPartway, ToneWarn} // not "healthy": its copies run two versions
		} else if st, ok := replacing(a); ok {
			a.Status = st
		} else if a.Pods > 0 && a.placeholders == a.Pods && a.Status == (Status{"Running", "healthy", ToneNormal}) {
			a.Status = Status{"Placeholder", "its pods hold spare room for others and wait for a node by design", ToneNormal} // nothing runs: not "healthy"
		} else if a.rollingOut != "" && a.Pods > 0 && a.Status == (Status{"Running", "healthy", ToneNormal}) && onlyDeployments(a) {
			a.Status = Status{"Rolling out", a.rollingOut, ToneNormal} // not done until its old pods are gone, as kubectl rollout status says
		}
		if (a.Pods > 0 || a.Stopping > 0) && a.Status.Tone <= ToneNormal && goingAway(a) {
			// Asked for no copies (scaled to 0, by hand or by a pauser), its
			// pods still going: a stateful set stops them one at a time, so
			// some still run, ready, for a while. It is stopping, not running.
			why := "it asks for 0 copies now, so its pods are going away: "
			switch {
			case a.Pods > 0 && a.Stopping > 0:
				why += plural(a.Pods, "pod") + " still running, " + plural(a.Stopping, "pod") + " shutting down"
			case a.Pods > 0:
				why += plural(a.Pods, "pod") + " still running"
			default:
				why += plural(a.Stopping, "pod") + " shutting down"
			}
			a.Status = Status{"Stopping", why, ToneNormal}
		}
		if a.Pods == 0 && a.Stopping > 0 && a.Status.Tone <= ToneNormal && a.Status.Short != "Stopping" {
			// Its pods are shutting down (a restart replaces each in turn):
			// the app is still here, its new pods come next.
			a.Status = Status{"Stopping", plural(a.Stopping, "pod") + " shutting down (a restart or scale-down); new ones follow if it asks for any", ToneNormal}
			if a.stoppingBare == a.Stopping { // as the pods list says: a pod of its own doesn't come back
				a.Status.Why = "1 pod shutting down; nothing makes it again (a pod of its own)"
				if a.Stopping > 1 {
					a.Status.Why = plural(a.Stopping, "pod") + " shutting down; nothing makes them again (pods of its own)"
				}
			}
			if a.wants > 0 { // it asks for copies: they come back (pushed off a node, or replaced)
				short := "Replacing a pod"
				if a.Stopping > 1 {
					short = "Replacing pods"
				}
				a.Status = Status{short, plural(a.Stopping, "pod") + " shutting down; it asks for " + plural(int(a.wants), "copy") + ", so new ones follow", ToneNormal}
			}
		}
		if a.stuckCreate != "" && a.Status.Tone <= ToneNormal && a.Status.Short != "Stopping" {
			// Its new version can't make pods; the old ones still serve.
			a.Status = Status{"Rollout stuck", a.stuckCreate, ToneBad}
			a.CanUndo, a.stuckNoUndo, a.stuckMaybe = a.stuckCreateUndo.helps, a.stuckCreateUndo.why, a.stuckCreateUndo.maybe
		}
		if a.Pods > 0 && a.missing > 0 && a.Status.Tone <= ToneNormal && a.Status.Short != "Stopping" {
			// Some made, the rest refused (a quota, a policy): not healthy.
			a.Status = Status{"Missing pods", fmt.Sprintf("Kubernetes can make only %d of %d copies: %s", a.Pods, a.Pods+a.missing, a.cantMake), ToneWarn}
		}
		if a.Pods == 0 && a.Stopping == 0 && a.Status.Tone <= ToneNormal {
			why := "nothing running right now"
			if a.Leftovers > 0 {
				why = "nothing running; only " + plural(a.Leftovers, "finished or evicted pod") + " left over"
			}
			short, tone := "No pods", ToneDim
			switch kinds := managedKinds(a); {
			case a.wants > 0 && cantMakeAny(e, a.wantObjs) != "":
				short, why, tone = "No pods", fmt.Sprintf("asks for %d %s but Kubernetes can't make them: %s", a.wants, map[bool]string{true: "copy", false: "copies"}[a.wants == 1], cantMakeAny(e, a.wantObjs)), ToneBad
			case a.wants > 0:
				short, why, tone = "No pods", fmt.Sprintf("asks for %d %s but none exist", a.wants, map[bool]string{true: "copy", false: "copies"}[a.wants == 1]), ToneBad
			case (kinds["Deployment"] || kinds["StatefulSet"]) && a.Argo != nil && a.Argo.Sync == "Synced":
				short, why = "Stopped", "turned off in Git (0 copies, and ArgoCD is in sync)"
			case kinds["Deployment"] || kinds["StatefulSet"]:
				short, why = "Stopped", "scaled to 0, so nothing runs"
			case a.noNode != "" && !kinds["CronJob"]:
				why = a.noNode // a daemon set its nodes don't take: as its box says
			case !kinds["Deployment"] && !kinds["StatefulSet"] && !kinds["DaemonSet"] && !kinds["CronJob"] &&
				(kinds["HTTPRoute"] || kinds["Ingress"]):
				short, why = "Routes only", "it sends web traffic on to other apps and runs nothing itself (Web lists its addresses)"
			case a.evictedBare > 0 && a.evictedBare == a.Leftovers && !a.pausedJob && !kinds["Deployment"] && !kinds["StatefulSet"] && !kinds["DaemonSet"] && !kinds["CronJob"] && !kinds["Job"]:
				// Only bare pods, evicted: nothing makes them again, so the
				// app is down.
				short, why, tone = "Evicted", "its pod was evicted (removed from its node), and nothing makes it again", ToneBad
				if a.evictedBare > 1 {
					why = fmt.Sprintf("its %d pods were evicted (removed from their nodes), and nothing makes them again", a.evictedBare)
				}
			case len(a.neverRuns) > 0:
				short, why, tone = "Never runs", a.neverRuns[0], ToneWarn
			case (a.pausedJobs > 0 || a.pausedJob) && !kinds["Deployment"] && !kinds["StatefulSet"] && !kinds["DaemonSet"] && !kinds["CronJob"]:
				short, why = "Paused", "its job is paused, so nothing runs until it is resumed"
				if a.queuedJob {
					short, why = "Queued", "its job waits in its queue (Kueue) until it is let in"
				}
				if a.pausedJobs > 0 {
					why += "; " + plural(a.pausedJobs, "finished pod") + " kept"
				}
			case kinds["CronJob"] && a.crons > 0 && a.cronsPaused == a.crons:
				short, why = "Paused", "nothing runs until its cron jobs are resumed (enter → Its workloads, then z on each cron job)"
			case kinds["CronJob"]:
				short, why = "Scheduled", "runs on a schedule (a cron job); nothing running between runs"
			}
			a.Status = Status{short, why, tone}
		}
		if a.Pods == 0 && a.Leftovers > 0 && a.Stopping == 0 && len(managedKinds(a)) == 0 && a.Argo == nil &&
			len(a.Secrets) == 0 && a.pausedJobs == 0 && !a.pausedJob && severity(a.Status.Tone) < severity(ToneWarn) {
			continue // only finished pods, whose owners are gone: not an app
		}
		switch {
		case a.Status.Short == "Rollout stuck":
			a.noUndo, a.undoMaybe = a.stuckNoUndo, a.stuckMaybe
		case a.Status == a.headStatus && a.headDep != nil:
			a.noUndo, a.undoMaybe = undoOf(a.headDep).why, undoOf(a.headDep).maybe
		}
		if a.Status != a.headStatus { // a later headline (a failed job, a secret) isn't about that pod
			a.HeadNS, a.HeadPod = "", ""
		}
		out = append(out, a)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if si, sj := severity(out[i].Status.Tone), severity(out[j].Status.Tone); si != sj {
			return si > sj // broken first, idle last
		}
		return out[i].Name < out[j].Name
	})
	e.SetNamed(named)
	return out
}

// severity orders statuses for home: broken, then needs a look, then
// running, then idle (no pods).
func severity(t Tone) int {
	switch t {
	case ToneBad:
		return 3
	case ToneWarn:
		return 2
	case ToneDim:
		return 0
	}
	return 1
}

// settleDeployed works out an app's Deployed from each workload's: the
// latest, said only as sure as every one of them allows. An exact time is
// exact only when no other workload's could be later (a bound after, or a
// time not known); otherwise it is a bound: the latest known, or later.
func settleDeployed(a *App) {
	var exact, after, before enrich.DeployTime // the latest of each kind
	var unknownAfter enrich.DeployTime
	if len(a.deploys) == 0 {
		a.deploys = a.unranDeploys
	}
	for _, t := range a.deploys {
		switch {
		case t.At.IsZero():
		case t.Unknown && t.After:
			if t.At.After(unknownAfter.At) {
				unknownAfter = t
			}
		case t.Unknown || t.Before: // then or earlier
			if t.At.After(before.At) {
				before = t
			}
		case t.After:
			if t.At.After(after.At) {
				after = t
			}
		default:
			if t.At.After(exact.At) {
				exact = t
			}
		}
	}
	a.LastDeploy, a.DeployBefore, a.DeployAfter, a.DeployUnknown, a.DeployMixed, a.ImageSince, a.DeploySince = time.Time{}, false, false, false, false, time.Time{}, time.Time{}
	a.DeployPending = slices.ContainsFunc(a.deploys, func(t enrich.DeployTime) bool { return t.Pending })
	a.DeployReturning = a.DeployPending && !slices.ContainsFunc(a.deploys, func(t enrich.DeployTime) bool { return t.Pending && !t.Returning })
	others := 0 // workloads of other kinds than the one said
	for _, t := range []enrich.DeployTime{exact, after, before, unknownAfter} {
		if !t.At.IsZero() {
			others++
		}
	}
	switch {
	case !exact.At.IsZero() && after.At.IsZero() && unknownAfter.At.IsZero() && !before.At.After(exact.At):
		a.LastDeploy, a.ImageSince = exact.At, exact.ImageSince // none can be later
	case !exact.At.IsZero() || !after.At.IsZero():
		// Some can be later than the latest known: that, or later.
		t := exact
		if after.At.After(t.At) {
			t = after
		}
		a.LastDeploy, a.DeployAfter, a.DeployMixed = t.At, true, others > 1
	case others == 1 && !before.At.IsZero():
		a.LastDeploy, a.DeployBefore, a.DeployUnknown = before.At, before.Before, before.Unknown
		if before.Before {
			a.DeploySince = before.Since
		}
	case !unknownAfter.At.IsZero():
		// Brought back after a time (its own deploy is that or later), next
		// to ones only known as "then or earlier": the latest is that or
		// later, whichever it is.
		a.LastDeploy, a.DeployAfter, a.DeployUnknown, a.DeployMixed = unknownAfter.At, true, true, others > 1
	}
	a.deploys, a.unranDeploys = nil, nil
}

// deployTime is when the pod's current version rolled out: its ReplicaSet's
// creation, or the pod's own for StatefulSets / DaemonSets.
func deployTime(e *enrich.Enricher, pod *unstructured.Unstructured, deployedAt map[string]map[types.UID]enrich.DeployTime) enrich.DeployTime {
	owner := e.Controller(pod)
	switch {
	case owner == nil:
	case owner.GetKind() == "ReplicaSet": // a version of a Deployment: when it went out
		// Restarts don't count; a version brought back counts when it came back.
		if ref := metav1.GetControllerOf(owner); ref != nil && ref.Kind == "Deployment" {
			times, ok := deployedAt[pod.GetNamespace()]
			if !ok {
				times = e.DeployTimes(pod.GetNamespace())
				deployedAt[pod.GetNamespace()] = times
			}
			if t, ok := times[ref.UID]; ok {
				return t
			}
		}
		return enrich.DeployTime{At: owner.GetCreationTimestamp().Time}
	case owner.GetKind() == "Deployment":
		// ReplicaSets not loaded yet: say nothing rather than guess (the
		// Deployment's last progress moves on restarts too); it fills in
		// a moment later.
		return enrich.DeployTime{}
	}
	return enrich.DeployTime{At: pod.GetCreationTimestamp().Time}
}

// podMonthly is a pod's share of its node's price: the larger of its CPU
// and memory requests as a fraction of the node, times the node's $/hr.
func podMonthly(e *enrich.Enricher, pod *unstructured.Unstructured) (float64, bool) {
	name, node, ok := e.PodNode(pod)
	if !ok || !node.PriceKnown {
		return 0, false
	}
	nodeObj := Named("", name)
	if n, found := e.NodeObject(name); found {
		nodeObj = n
	}
	alloc := e.NodeUsage(nodeObj)
	use := e.PodUsage(pod)
	share := 0.0
	if alloc.AllocCPU > 0 {
		share = float64(use.ReqCPU) / float64(alloc.AllocCPU)
	}
	if alloc.AllocMem > 0 {
		share = max(share, float64(use.ReqMem)/float64(alloc.AllocMem))
	}
	return node.Hourly * share * hoursPerMonthF, true
}

// AppHeader matches AppRows.
// WHY only shows on wide screens: the details box says the same.
var AppHeader = []string{"APP", "NAMESPACE", "STATUS", "READY", "RESTARTS", "DEPLOYED", "COST/MO", "WHY"}

// AppRows renders apps for the home table.
func AppRows(apps []*App) []Row {
	rows := make([]Row, 0, len(apps))
	for _, a := range apps {
		o := Named(a.Namespace, a.Name)
		o.SetKind("App")
		cost := "-"
		switch {
		case a.Priced && a.Monthly < 1:
			cost = "<$1"
		case a.Priced:
			cost = fmt.Sprintf("$%.0f", a.Monthly)
		}
		deployed := "-"
		if !a.LastDeploy.IsZero() {
			deployed = Age(a.LastDeploy) + " ago"
			switch {
			case a.DeployUnknown && a.DeployAfter:
				deployed = "?" // brought back after a time; its settings' first time out unknown
			case a.DeployBefore, a.DeployUnknown:
				deployed = Age(a.LastDeploy) + "+ ago" // then or earlier
			case a.DeployAfter:
				deployed = AgeAtMost(a.LastDeploy) + " ago" // then or later
			}
		}
		rows = append(rows, Row{Obj: o, Tone: a.Status.Tone, Cells: []string{
			a.Name, OrDash(a.Namespace), a.Status.Short, readyCell(a), fmt.Sprint(a.Restarts), deployed, cost,
			whyWords(a.Status),
		}})
	}
	return rows
}

// whyWords is the WHY cell: the first sentence of what's wrong, without
// the key hints the details box gives.
func whyWords(st Status) string {
	if st.Tone == ToneNormal || st.Tone == ToneOK || st.Why == "healthy" {
		return ""
	}
	why := st.Why
	if i := strings.Index(why, ". "); i > 0 {
		why = why[:i]
	}
	return strings.TrimSuffix(keyHint.ReplaceAllString(why, ""), ".")
}

// DateWords is a date in your time: "17 Sep 17:46", with the year when it
// isn't this year ("17 Sep 2025"), so an old date doesn't read as recent.
func DateWords(t time.Time, withTime bool) string {
	t = t.Local()
	if t.Year() != time.Now().Year() {
		return t.Format("2 Jan 2006")
	}
	if withTime {
		return t.Format("2 Jan 15:04")
	}
	return t.Format("2 Jan")
}

// shortText cuts plain text to n letters, ending in … when cut.
func shortText(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// keyHint is "; p shows the logs from before" at the end of a reason: the
// box says it, a column has no room for it.
var keyHint = regexp.MustCompile(`[;,]? *\b[a-zA-Z] (shows|opens|lists) .*$`)

// cantMakeAny is the first reason Kubernetes gives for not making the
// pods of any of these workloads.
func cantMakeAny(e *enrich.Enricher, ws []*unstructured.Unstructured) string {
	for _, w := range ws {
		if why := cantCreate(e, w); why != "" {
			return why
		}
	}
	return ""
}

// cantMakeFix is the Fix for pods Kubernetes can't make, before where.
const cantMakeFix = "E shows why Kubernetes can't make its pods (often a missing service account, a quota or a policy); fix that "

// cantMakeAll: a deployment, stateful set, replica set or daemon set, its status
// current, that has made fewer copies than it asks for, and Kubernetes
// says why (its FailedCreate warning, or its ReplicaFailure condition):
// how many it made, how many it asks for, and why. ok is false for any
// other.
func cantMakeAll(e *enrich.Enricher, w *unstructured.Unstructured) (made, want int64, why string, ok bool) {
	switch w.GetKind() {
	case "Deployment", "StatefulSet", "ReplicaSet", "DaemonSet":
	default:
		return 0, 0, "", false
	}
	if w.GetDeletionTimestamp() != nil || num(w, "status", "observedGeneration") < w.GetGeneration() {
		return 0, 0, "", false
	}
	want, made = wantedReplicas(w), num(w, "status", "replicas")
	if w.GetKind() == "DaemonSet" { // one on each node it runs on
		want, made = num(w, "status", "desiredNumberScheduled"), num(w, "status", "currentNumberScheduled")
	}
	if made >= want {
		return 0, 0, "", false
	}
	// A deployment or replica set says it in its condition while it lasts;
	// a stateful set or daemon set has only its warnings.
	c, found := enrich.Condition(w, "ReplicaFailure")
	failing := found && c.Status == "True"
	if (w.GetKind() == "Deployment" || w.GetKind() == "ReplicaSet") && !failing {
		return 0, 0, "", false // an earlier warning, since put right
	}
	why = cantCreate(e, w)
	if k := w.GetKind(); k == "StatefulSet" || k == "DaemonSet" {
		// Fewer than it asks for is normal while it scales up (one at a
		// time) or a node joins: its warning counts only while it is the
		// newest word, after any pod it made and any it said it made.
		_, warned, _ := e.LatestWarningFor(w, "FailedCreate")
		_, created, _ := e.LatestWarningFor(w, "SuccessfulCreate")
		for _, p := range e.OwnedPods(w) {
			if t := p.GetCreationTimestamp().Time; t.After(created) {
				created = t
			}
		}
		if !warned.After(created) {
			return 0, 0, "", false
		}
	}
	if why == "" && failing {
		why = enrich.MessageWords(oneLine(c.Message))
	}
	return made, want, why, why != ""
}

// RolloutCantMake is rolloutCantMake, for the rollout and versions views.
func RolloutCantMake(e *enrich.Enricher, w *unstructured.Unstructured) string {
	return rolloutCantMake(e, w)
}

// waitsOnPod: a stateful set that replaces its pods one at a time
// (OrderedReady, rolling updates), its status current, won't replace this
// pod by itself: it is of an older version than the one it updates to and
// isn't ready, and Kubernetes waits for it to be ready first (after u on a
// version that failed, its pod must be deleted by hand).
func waitsOnPod(sts *unstructured.Unstructured, p *corev1.Pod) bool {
	if !podsInTurn(sts) || num(sts, "status", "observedGeneration") < sts.GetGeneration() {
		return false
	}
	update, hash := str(sts, "status", "updateRevision"), p.Labels["controller-revision-hash"]
	return update != "" && hash != "" && hash != update && !podFullyReady(p)
}

// podsInTurn: a stateful set that replaces its pods one at a time, each
// only once the one before is ready (OrderedReady, rolling updates, no
// partition keeping some on an older version on purpose).
func podsInTurn(sts *unstructured.Unstructured) bool {
	if sts.GetKind() != "StatefulSet" {
		return false
	}
	if pol := str(sts, "spec", "podManagementPolicy"); pol != "" && pol != "OrderedReady" {
		return false
	}
	if t := str(sts, "spec", "updateStrategy", "type"); t != "" && t != "RollingUpdate" {
		return false
	}
	return num(sts, "spec", "updateStrategy", "rollingUpdate", "partition") == 0
}

// HeldAfterGoingBack names a failing pod of a stateful set that replaces
// its pods one at a time: going back to an earlier version leaves it as
// it is (Kubernetes waits for it to be ready first), so it must be
// deleted for the change to reach it. "" when there is none.
func HeldAfterGoingBack(e *enrich.Enricher, sts *unstructured.Unstructured) string {
	if !podsInTurn(sts) {
		return ""
	}
	if pod := firstInTurn(e, sts); pod != nil && Explain(e, pod).Tone == ToneBad {
		return pod.Name
	}
	return ""
}

// waitsHeld names the pod stateful set sts waits for when it is held
// (waitsOnPod) and failing; "" when the one it waits for is another (one
// starting, or of the version it updates to) or there is none.
func waitsHeld(e *enrich.Enricher, sts *unstructured.Unstructured) string {
	if pod := firstInTurn(e, sts); pod != nil && waitsOnPod(sts, pod) && Explain(e, pod).Tone == ToneBad {
		return pod.Name
	}
	return ""
}

// firstInTurn is the pod a stateful set that replaces its pods one at a
// time waits for: its lowest-numbered one not ready (by number, not name:
// web-2 before web-10). nil when all are ready.
func firstInTurn(e *enrich.Enricher, sts *unstructured.Unstructured) *corev1.Pod {
	var first *corev1.Pod
	lowest := 0
	for _, po := range e.PodsOf(sts) {
		pod, err := ToPod(po)
		if err != nil || leftover(pod) || podFullyReady(pod) {
			continue
		}
		n, err := strconv.Atoi(pod.Name[strings.LastIndex(pod.Name, "-")+1:])
		if err != nil {
			n = math.MaxInt
		}
		if first == nil || n < lowest {
			first, lowest = pod, n
		}
	}
	return first
}

// waitsOnWords says why a stateful set waits on its pod and what to do.
func waitsOnWords(pod string) string {
	return "its pod " + pod + ", still on another version and failing: Kubernetes won't replace it by itself. Delete it so it is made again with the current version"
}

// rolloutCantMake: a deployment partway through a rollout (old pods still
// there), its status current, whose new version Kubernetes can't make
// pods for (its ReplicaFailure condition): why. "" for any other.
func rolloutCantMake(e *enrich.Enricher, w *unstructured.Unstructured) string {
	if w.GetKind() != "Deployment" || w.GetDeletionTimestamp() != nil || num(w, "status", "observedGeneration") < w.GetGeneration() {
		return ""
	}
	if c, ok := enrich.Condition(w, "ReplicaFailure"); !ok || c.Status != "True" || num(w, "status", "replicas") <= num(w, "status", "updatedReplicas") {
		return ""
	}
	if why := cantCreate(e, w); why != "" {
		return why
	}
	c, _ := enrich.Condition(w, "ReplicaFailure")
	return enrich.MessageWords(oneLine(c.Message))
}

// cantCreate is why Kubernetes can't make a workload's pods, from its newest
// FailedCreate warning: on a StatefulSet itself, on a Deployment's
// ReplicaSets. "" when it says nothing.
func cantCreate(e *enrich.Enricher, w *unstructured.Unstructured) string {
	objs := []*unstructured.Unstructured{w}
	if w.GetKind() == "Deployment" {
		for _, rs := range e.List(kube.ReplicaSets, w.GetNamespace()) {
			if ref := metav1.GetControllerOf(rs); ref != nil && ref.UID == w.GetUID() {
				objs = append(objs, rs)
			}
		}
	}
	var newest time.Time
	why := ""
	for _, o := range objs {
		if msg, at, ok := e.LatestWarningFor(o, "FailedCreate"); ok && at.After(newest) {
			newest, why = at, msg
		}
	}
	return why
}

// withoutKeyHint drops "; p shows…" or ". p shows…" from a reason.
func withoutKeyHint(why string) string {
	return strings.TrimSuffix(keyHint.ReplaceAllString(strings.TrimSuffix(why, ". p shows the crashed run's logs"), ""), ".")
}

// AppWhy is the WHY cell for an app with a problem: the same words as the
// Problem line of its details box (which may know more than its status,
// such as which ArgoCD part fails), without the key hints.
func AppWhy(a *App, look Lookups) string {
	if rest, ok := strings.CutPrefix(a.Status.Why, "healthy now; crashed"); ok && a.Status.Tone == ToneNormal {
		return "crashed " + strings.TrimSpace(keyHint.ReplaceAllString(rest, "")) // fine now; worth knowing
	}
	if a.Status.Tone != ToneBad && a.Status.Tone != ToneWarn {
		switch a.Status.Short { // not a problem, but worth reading: why it isn't all ready
		case "Starting", "Replacing a pod", "Replacing pods", "Rolling out": // passing (a placeholder is for good: no WHY column for it)
			return a.Status.Why
		}
		return ""
	}
	look.Web = nil // not needed for the problem; reading it is slower
	for _, l := range AppLines(a, look) {
		text, ok := strings.CutPrefix(l, "Problem  ")
		if !ok {
			continue
		}
		text = strings.TrimPrefix(text, a.Status.Short+": ") // STATUS already says it
		if i := strings.Index(text, ". "); i > 0 {
			text = text[:i]
		}
		return strings.TrimSuffix(keyHint.ReplaceAllString(text, ""), ".")
	}
	return whyWords(a.Status)
}

// secretStatus is the app's headline when its secrets can't sync. A
// warning: pods keep the Secret's last values, and if the Secret doesn't
// exist at all the pods fail to start and say so themselves.
func secretStatus(failing []FailingSecret) Status {
	what := "ExternalSecret " + failing[0].Name + " can't fetch from its store"
	if len(failing) > 1 {
		what = fmt.Sprintf("%d ExternalSecrets can't fetch from their store", len(failing))
	}
	return Status{"Secret not syncing", what, ToneWarn}
}

// ArgoCheck is what the UI found in the resources of an app ArgoCD calls
// unhealthy.
type ArgoCheck struct {
	Broken  []string // "scaledobject/x: why"
	Missing []string // "deployment/y"
}

// ArgoLookup returns the check for an app (ok false while it runs).
type ArgoLookup func(app string) (ArgoCheck, bool)

// Lookups are the details the UI reads on demand for the selected app,
// and how kubed runs.
type Lookups struct {
	Secret   SecretLookup
	Argo     ArgoLookup
	ReadOnly bool // changes are off: fixes say what to do elsewhere
	// Web, when set, lists the web addresses that reach the app.
	Web func(a *App) []string
	// Full: the details screen, with room for every web address.
	Full bool
	// Crash, when set, gives the error a crashing container's last run
	// ended with; ok is false while it is being read.
	Crash func(a *App) (text string, ok bool)
}

// outOfSyncWhy names what differs from Git.
func outOfSyncWhy(a enrich.ArgoApp) string {
	var names []string
	for _, r := range a.Resources {
		if r.OutOfSync {
			names = append(names, r.String())
		}
	}
	switch {
	case len(names) == 0:
		return "what runs differs from Git"
	case len(names) <= 3:
		return strings.Join(names, ", ") + " differ from Git"
	}
	return fmt.Sprintf("%s and %d more differ from Git", strings.Join(names[:3], ", "), len(names)-3)
}

// SecretLookup returns what the UI found for an ExternalSecret (ok false
// while it hasn't).
type SecretLookup func(namespace, name string) (SecretCheck, bool)

// AppLines explains the selected app under the table: what's wrong, the
// real error, whether it hurts now, and what fixes it; then Git and cost.
func AppLines(a *App, look Lookups) []string {
	lookup := look.Secret
	var lines []string
	add := func(label, text string) { lines = append(lines, fmt.Sprintf("%-8s %s", label, text)) }
	problem := a.Status.Tone == ToneBad || a.Status.Tone == ToneWarn

	switch {
	case len(a.Secrets) > 0 && a.Status.Short == "Secret not syncing":
		f := a.Secrets[0]
		since := ""
		if !f.Since.IsZero() {
			since = " since " + DateWords(f.Since, false) + " (" + howLong(time.Since(f.Since)) + ")"
		}
		add("Problem", fmt.Sprintf("ExternalSecret %s can't update secret %s%s", f.Name, f.Target, since))
		var check SecretCheck
		ok := false
		if lookup != nil {
			check, ok = lookup(f.Namespace, f.Name)
		}
		switch {
		case !ok:
			add("Error", "reading the error…")
		case check.Error == "":
			add("Error", "no recent error event; E shows the app's events")
		default:
			add("Error", check.Error)
		}
		switch {
		case !ok:
		case check.TargetExists && f.NeverSynced && f.UsedBy == 0:
			add("Impact", "secret "+f.Target+" has never had its values: no pod uses it yet, but any that does will get none")
		case check.TargetExists && f.NeverSynced:
			add("Impact", fmt.Sprintf("secret %s has never had its values: %d pods read it and get none", f.Target, f.UsedBy))
		case check.TargetExists && f.UsedBy == 0:
			add("Impact", "none now: no pod uses secret "+f.Target+"; changes in the store won't reach it")
		case check.TargetExists:
			add("Impact", fmt.Sprintf("none yet: %d pods run with the secret's last synced values; changes in the store won't reach them", f.UsedBy))
		default:
			add("Impact", "secret "+f.Target+" doesn't exist: pods that use it can't start")
		}
		msg := check.Error
		add("Fix", SecretFix(msg, f.Name))
		if len(a.Secrets) > 1 {
			add("Also", fmt.Sprintf("%d more failing ExternalSecrets in this app", len(a.Secrets)-1))
		}
	case a.Argo != nil && (a.Status.Short == "Failing" || a.Status.Short == "Missing parts"):
		check, ok := ArgoCheck{}, false
		if look.Argo != nil {
			check, ok = look.Argo(a.Name)
		}
		switch {
		case !ok:
			add("Problem", "ArgoCD marks part of it as failing. Finding out which…")
		case len(check.Broken)+len(check.Missing) == 0:
			add("Problem", "ArgoCD marks part of it as failing, but everything kubed can read looks fine")
			add("Fix", "d shows ArgoCD's own record of the app")
		default:
			for i, b := range check.Broken {
				if i == 3 {
					add("", fmt.Sprintf("and %d more", len(check.Broken)-3))
					break
				}
				label := ""
				if i == 0 {
					label = "Problem"
				}
				// A plain sentence first, the controller's own words after.
				if plain := argoPlain(b); plain != "" {
					add(label, plain)
					add("Error", b)
				} else {
					add(label, b)
				}
			}
			if len(check.Missing) > 0 {
				add("Missing", strings.Join(check.Missing, ", "))
			}
			add("Fix", argoFix(check))
		}
	case problem:
		// "p shows…" works on a pod, not here; the Fix line says how to get there.
		add("Problem", a.Status.Short+": "+withoutKeyHint(a.Status.Why)) // the Fix line says which key
		if a.CrashPod != "" && look.Crash != nil {
			// The crash quoted is the app's newest anywhere: under a problem
			// that isn't a crash (an image that can't be pulled), say whose.
			crashy := crashyShort(a.Status.Short) || a.Status.Short == "Rollout stuck" && crashyShort(a.stuckBy.Short) // stuck because it crashes: the same crash
			label, prefix := "Error", ""
			if !crashy || a.HeadPod != "" && (a.HeadPod != a.CrashPod || a.HeadNS != a.CrashNS) { // not the headline's pod: say whose
				label, prefix = "Also", "pod "+a.CrashPod+" crashed: "
			}
			switch text, ok := look.Crash(a); {
			case !ok:
				add(label, prefix+"reading the end of the crashed run…")
			case text != "":
				add(label, prefix+text)
			}
		}
		if fix := appFix(a, look.ReadOnly); fix != "" {
			add("Fix", fix)
		}
	default:
		add("Status", a.Status.Short+": "+a.Status.Why)
	}
	// A failing secret that isn't the headline still needs saying.
	if len(a.Secrets) > 0 && a.Status.Short != "Secret not syncing" {
		f := a.Secrets[0]
		text := fmt.Sprintf("ExternalSecret %s can't update secret %s", f.Name, f.Target)
		if !f.Since.IsZero() {
			text += " since " + DateWords(f.Since, false) + " (" + howLong(time.Since(f.Since)) + ")"
		}
		if lookup != nil {
			if check, ok := lookup(f.Namespace, f.Name); ok && check.Error != "" {
				text += ": " + check.Error
			}
		}
		if n := len(a.Secrets); n > 1 {
			text += fmt.Sprintf(" (and %d more)", n-1)
		}
		add("Also", text)
	}

	ready, of, _ := strings.Cut(readyCell(a), "/") // the same count as the READY column
	pods := ready + " of " + of + " ready"
	if a.Pods == 0 {
		pods = "none running"
		if a.Stopping > 0 { // still running while they stop, as kubectl says
			pods += "; " + plural(a.Stopping, "pod") + " shutting down"
		}
	}
	if a.Restarts > 0 {
		pods += " · " + plural(int(a.Restarts), "restart") + " across its pods since they started"
	}
	add("Pods", pods)
	if look.Web != nil {
		if hosts := look.Web(a); len(hosts) > 0 {
			if !look.Full && len(hosts) > 4 {
				hosts = append(hosts[:4], fmt.Sprintf("and %d more (d lists them all)", len(hosts)-4))
			}
			add("Web", strings.Join(hosts, " · "))
		}
	}
	if !a.LastDeploy.IsZero() {
		deployed := Age(a.LastDeploy) + " ago (" + DateWords(a.LastDeploy, true) + ")"
		switch {
		case a.DeployUnknown && a.DeployAfter:
			deployed = "brought back after " + DateWords(a.LastDeploy, true) + "; when its settings first went out is unknown (Kubernetes no longer keeps the version before it)"
		case a.DeployUnknown:
			deployed = "brought back " + Age(a.LastDeploy) + " ago (" + DateWords(a.LastDeploy, true) + "); its settings may have gone out before that (Kubernetes no longer keeps the version before it)"
		case a.DeployBefore && !a.DeploySince.IsZero(): // made then: no earlier
			deployed = "between " + DateWords(a.DeploySince, true) + " and " + DateWords(a.LastDeploy, true) + " (Kubernetes no longer keeps that version; since then it was only restarted)"
		case a.DeployBefore:
			deployed = "before " + DateWords(a.LastDeploy, true) + " (Kubernetes no longer keeps that version; since then it was only restarted)"
		case a.DeployMixed:
			deployed = "after " + DateWords(a.LastDeploy, true) + " (the latest known; another of its workloads can't be dated exactly and may be later)"
		case a.DeployAfter:
			deployed = "after " + DateWords(a.LastDeploy, true) + " (brought back after that version went out; what Kubernetes keeps can't say exactly when)"
		}
		if !a.ImageSince.IsZero() && a.LastDeploy.Sub(a.ImageSince) > time.Hour {
			deployed += "; that changed settings only: its images have run since " + DateWords(a.ImageSince, false)
		}
		if a.PodsSince.Sub(a.LastDeploy) > 10*time.Minute {
			// Every pod is newer than the version: restarted or moved since.
			deployed += "; all its pods were replaced since, the oldest " + Age(a.PodsSince) + " ago"
		}
		if a.DeployPending { // the new images show above; the time waits for its first ready pod
			if a.DeployReturning {
				deployed += "; an earlier version is coming back (not ready yet)"
			} else {
				deployed += "; a newer version is rolling out (not ready yet)"
			}
		}
		add("Deployed", deployed)
	}
	for _, d := range a.pausedDeps {
		if d == a.pausedDep && a.Status.Short == "Paused" && a.Status.Why == a.pausedPartway {
			continue // its headline says so, and its Fix how to resume
		}
		add("Paused", "deployment "+d.GetName()+": "+pausedLine(d))
	}
	if len(a.Images) > 0 {
		imgs := slices.Clone(a.Images)
		slices.SortStableFunc(imgs, func(x, y string) int { // the app's own first
			side := func(s string) bool { return strings.HasSuffix(s, " (helper)") || strings.HasSuffix(s, " (runs first)") }
			return cmpBool(side(x), side(y))
		})
		if !look.Full && len(imgs) > 5 {
			imgs = append(imgs[:5], fmt.Sprintf("and %d more (d lists them all)", len(imgs)-5))
		}
		if a.heldImages != "" { // its pods run the old ones until deleted
			imgs[len(imgs)-1] += "; " + a.heldImages
		}
		add("Images", strings.Join(imgs, " · "))
		var moving []string
		for _, img := range a.Images {
			name, _, _ := strings.Cut(img, " (")
			if tag := name[strings.LastIndex(name, "/")+1:]; !strings.Contains(tag, ":") || strings.HasSuffix(tag, ":latest") {
				moving = append(moving, name)
			}
		}
		if len(moving) > 0 {
			verb := " use moving tags"
			if len(moving) == 1 {
				verb = " uses a moving tag"
			}
			add("Note", strings.Join(moving, ", ")+verb+": a restart can pull a different build")
		}
	}
	if a.Argo != nil {
		add("ArgoCD", argoWords(*a.Argo))
	}
	if !a.Priced && a.Pods == 0 {
		switch {
		case a.Runs("Deployment") || a.Runs("StatefulSet"):
			switch {
			case a.wants > 0: // it asks for copies; none could be made
				add("Cost", "nothing now: none of its pods could be made")
			case !a.Runs("CronJob") || (a.crons > 0 && a.cronsPaused == a.crons):
				add("Cost", "nothing now: it runs no pods (scaled to 0)")
			}
		case a.crons > 0 && a.cronsPaused == a.crons:
			add("Cost", "nothing now: its cron jobs are paused")
		case a.Runs("CronJob"):
			// "-" isn't free: its runs cost while they run.
			add("Cost", "not counted: it runs only on a schedule, and kubed prices what runs now")
		}
	}
	if a.Priced {
		cost := fmt.Sprintf("about $%.0f a month", a.Monthly)
		if a.Monthly < 1 {
			cost = "under $1 a month"
		}
		cost += " (its share of the nodes it runs on)"
		if a.RunsNotPriced {
			cost += "; its runs (jobs) come and go and aren't counted"
		}
		add("Cost", cost)
	}
	if a.RunTTL > 0 {
		// What failed overnight may already be gone: say how long runs stay.
		add("Runs", "Kubernetes deletes each run "+span(time.Duration(a.RunTTL)*time.Second)+" after it ends: older ones, failed or not, aren't here")
	}
	// A pod nothing owns isn't replaced: say so, apart from those that are.
	switch replaced := a.Evicted - a.evictedBare; {
	case replaced > 0 && a.evictedBare > 0:
		add("Evicted", plural(a.Evicted, "pod")+" pushed off a node: "+fmt.Sprint(replaced)+" already replaced, "+fmt.Sprint(a.evictedBare)+" that nothing replaces; safe to delete")
	case replaced > 0:
		add("Evicted", plural(a.Evicted, "pod")+" pushed off a node and already replaced; safe to delete")
	case a.evictedBare > 0:
		it := "it"
		if a.evictedBare > 1 {
			it = "them"
		}
		add("Evicted", plural(a.evictedBare, "pod")+" pushed off a node; nothing replaces "+it+", so deleting removes "+it+" for good")
	}
	if a.Stopping > 0 {
		gone := ", being replaced or removed"
		if a.stoppingBare == a.Stopping { // pods of its own: nothing replaces them
			gone = ", being removed"
		}
		add("Stopping", plural(a.Stopping, "pod")+" shutting down"+gone)
	}
	if done := a.Leftovers - a.Evicted - a.Stopping; done > 0 {
		kept := ", kept for their logs"
		if done == 1 {
			kept = ", kept for its logs"
		}
		headline := a.Status.Short == "Can't start" || strings.Contains(a.Status.Why, "it never started")
		switch {
		case headline && a.doneNeverRan >= done: // only those that never started a container
			kept = ", kept by Kubernetes (they never started, so they have no logs)"
			if done == 1 {
				kept = ", kept by Kubernetes (it never started, so it has no logs)"
			}
		case a.RunTTL > 0:
			kept = ", kept for their logs until Kubernetes deletes them (Runs says when)"
			if done == 1 {
				kept = ", kept for its logs until Kubernetes deletes it (Runs says when)"
			}
		}
		if headline && a.doneNeverRan > 0 && a.doneNeverRan < done { // some ran: theirs are there
			has := "it has"
			if a.doneNeverRan > 1 {
				has = "they have"
			}
			kept += "; " + fmt.Sprint(a.doneNeverRan) + " of them never started, so " + has + " no logs"
		}
		add("Leftover", plural(done, "finished pod")+kept)
	}
	return problemFirst(lines)
}

// problemFirst puts the fix right under the problem, before the raw error,
// so it is still on screen when the box is short.
func problemFirst(lines []string) []string {
	label := func(l string) string { f, _, _ := strings.Cut(l, " "); return f }
	fix := -1
	for i, l := range lines {
		if label(l) == "Fix" {
			fix = i
			break
		}
	}
	if fix < 0 {
		return lines
	}
	out := make([]string, 0, len(lines))
	for i, l := range lines {
		if i == fix {
			continue
		}
		out = append(out, l)
		if label(l) == "Problem" {
			out = append(out, lines[fix])
		}
	}
	if len(out) < len(lines) { // no Problem line: keep the order
		return lines
	}
	return out
}

// beforeFirstCheck: a container of p that isn't ready can't have passed
// its checks yet: its startup check hasn't, or its ready check's first
// run (its initial delay plus one period) hasn't come.
func beforeFirstCheck(p *corev1.Pod) bool {
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			continue
		}
		for _, c := range p.Spec.Containers {
			if c.Name != cs.Name {
				continue
			}
			if c.StartupProbe != nil && (cs.Started == nil || !*cs.Started) {
				return true
			}
			if c.ReadinessProbe != nil && cs.State.Running != nil {
				period := 10 * time.Second // Kubernetes' default
				if c.ReadinessProbe.PeriodSeconds > 0 {
					period = time.Duration(c.ReadinessProbe.PeriodSeconds) * time.Second
				}
				if time.Since(cs.State.Running.StartedAt.Time) < readinessDelay(p, c.Name)+period {
					return true
				}
			}
		}
	}
	return false
}

// onlyWorkloadsDiffer: what differs from Git is only workloads (their
// copy counts, from a scale to 0 or a scheduled pause). Anything else that
// differs (a secret, a config map) is a real drift worth a look.
func onlyWorkloadsDiffer(a enrich.ArgoApp) bool {
	for _, r := range a.Resources {
		if !r.OutOfSync {
			continue
		}
		switch r.Kind {
		case "Deployment", "StatefulSet", "HorizontalPodAutoscaler", "ScaledObject":
		default:
			return false
		}
	}
	return true
}

// JobCause is why a failed Job's tries failed, from its pods: "reached
// the specified backoff limit" only says it gave up. The newest try that
// failed says it (tries can fail for different reasons, and the Error
// line quotes the newest too); a run past its deadline was stopped for
// that, whatever its earlier tries ran into, unless its program never
// started: that stays first (it has no logs, and its fix is the command).
func JobCause(e *enrich.Enricher, j *unstructured.Unstructured) (string, bool) {
	why, found := jobPodCause(e, j)
	for _, typ := range []string{"Failed", "FailureTarget"} {
		if c, ok := enrich.Condition(j, typ); ok && c.Status == "True" && c.Reason == "DeadlineExceeded" && c.Message != "" {
			if found && NeverRan(why) { // not a setup step that failed: it ran
				return why + "; then the run passed its time limit, so it was stopped", true
			}
			return MessageWords(c.Message), true
		}
	}
	return why, found
}

func init() { enrich.JobCauseOf = JobCause } // :changes says a failed job's cause as home does

// NeverRan reports that a cause (a Job's, or an app's Why) says the
// program never ran (a command missing from the image, an image never
// pulled), so there are no logs. Every l and p asks this one check.
func NeverRan(why string) bool {
	return strings.Contains(why, "it never started") || strings.Contains(why, "never ran: it couldn't download")
}

// PodRan reports that a container of the pod has started at least once (running, or
// ended after it ran), so the pod has logs. One that couldn't start (exit
// 128, its command missing) hasn't. l on an app and the box's Leftover
// line both ask it.
func PodRan(p *unstructured.Unstructured) bool {
	statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
	inits, _, _ := unstructured.NestedSlice(p.Object, "status", "initContainerStatuses")
	for _, s := range append(statuses, inits...) {
		st, _ := s.(map[string]any)
		if _, ok, _ := unstructured.NestedMap(st, "state", "running"); ok {
			return true
		}
		for _, when := range []string{"state", "lastState"} {
			t, ok, _ := unstructured.NestedMap(st, when, "terminated")
			reason, _, _ := unstructured.NestedString(t, "reason")
			code, _, _ := unstructured.NestedInt64(t, "exitCode")
			if ok && reason != "StartError" && reason != "ContainerCannotRun" && code != 128 {
				return true
			}
		}
	}
	return false
}

// jobPodCause is why the newest of a Job's tries that failed failed, from
// its pod.
func jobPodCause(e *enrich.Enricher, j *unstructured.Unstructured) (string, bool) {
	why, found := "", false
	var newest *corev1.Pod
	for _, po := range e.PodsOf(j) {
		pod, err := ToPod(po)
		if err != nil || newest != nil && !pod.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			continue
		}
		if w, ok := startFailure(pod); ok {
			why, found, newest = w, true, pod
		} else if failed, _ := tryFailed(pod); failed { // its phase may not say Failed yet
			why, found, newest = lastExit(pod), true, pod
		} else if w, ok := neverPulled(pod); ok && jobEnded(j) { // the run ended (its deadline) while this try waited for its image
			why, found, newest = w, true, pod
		}
	}
	if !found && jobEnded(j) && len(e.PodsOf(j)) == 0 {
		if why, ok := pullFailedEvents(e, j); ok { // its pods are gone: their events may still say
			return why, ok
		}
		if stoppedByDelete(e, j) {
			return deletedGone, true
		}
		return "", false
	}
	if !found && deletingPod(e, j) { // counted failed as soon as it was asked to stop: no try failed by itself
		return deletedStopping, true
	}
	return why, found
}

// deletedStopping is a Job's cause when no try failed by itself, but a
// pod of it was deleted and still stops: its logs don't say why.
const deletedStopping = "a deleted pod is still stopping"

// deletedGone is a Job's cause once such a pod is gone: nothing says a
// try failed by itself, and something says each was stopped.
const deletedGone = "its pod was stopped (deleted), not failed by itself"

// sawDeleting remembers, per Job UID, its pods kubed saw being deleted
// (while they stopped) and when that was asked, in memory only: once they
// are gone, that is all that may say why its tries ended.
var (
	sawDeleting   = map[types.UID]map[string]time.Time{}
	sawDeletingMu sync.Mutex
)

// stoppedByDelete: a Job that gave up (BackoffLimitExceeded) with its pods
// gone, each try it counts failed one of its pods that was stopped before
// it gave up (a kept "Stopping container" Killing event, or kubed saw it
// asked to delete), and no kept event says a try failed by itself (a
// crash's BackOff). Stops once it gave up are its own cleanup of the
// tries still running.
func stoppedByDelete(e *enrich.Enricher, j *unstructured.Unstructured) bool {
	var gaveUp time.Time // the first time it said so
	for _, typ := range []string{"Failed", "FailureTarget"} {
		if c, ok := enrich.Condition(j, typ); ok && c.Status == "True" && c.Reason == "BackoffLimitExceeded" {
			if at := conditionSince(j, typ); !at.IsZero() && (gaveUp.IsZero() || at.Before(gaveUp)) {
				gaveUp = at
			}
		}
	}
	if gaveUp.IsZero() {
		return false
	}
	// Times are whole seconds: the delete that made it give up reads the
	// same second (or one off). Its own cleanup of the tries still running
	// reads the same, so a kept try that failed by itself rules it out below.
	cutoff := gaveUp.Add(time.Second)
	stopped := map[string]bool{}
	sawDeletingMu.Lock()
	for name, at := range sawDeleting[j.GetUID()] {
		if !at.After(cutoff) {
			stopped[name] = true
		}
	}
	sawDeletingMu.Unlock()
	made := j.GetCreationTimestamp().Time
	for _, ev := range e.List(kube.Events, j.GetNamespace()) {
		name := str(ev, "involvedObject", "name")
		if str(ev, "involvedObject", "kind") != "Pod" || enrich.EventTime(ev).Before(made) || !jobPodName(e, j, name) {
			continue
		}
		switch str(ev, "reason") {
		case "BackOff":
			return false // a try crashed
		case "Killing":
			if strings.HasPrefix(str(ev, "message"), "Stopping container") && !enrich.EventTime(ev).After(cutoff) {
				stopped[name] = true
			}
		}
	}
	for _, po := range e.PodsOf(j) { // a kept try that failed by itself: the stops were its cleanup
		if p, err := ToPod(po); err == nil && !stopped[p.Name] && failedByItself(p) {
			return false
		}
	}
	return len(stopped) > 0 && int64(len(stopped)) >= failedTries(e, j)
}

// failedByItself: a pod not being deleted that failed (phase Failed), is
// backing off a crash, or has a container that exited with an error.
func failedByItself(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	if p.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, cs := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
		if w := cs.State.Waiting; w != nil && strings.Contains(w.Reason, "BackOff") && cs.LastTerminationState.Terminated != nil {
			return true
		}
		for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if t != nil && t.ExitCode != 0 {
				return true
			}
		}
	}
	return false
}

// neverPulled: a pod whose container waits for an image it can't pull and
// has never run (no earlier run, no restart), in words.
func neverPulled(p *corev1.Pod) (string, bool) {
	for _, cs := range p.Status.ContainerStatuses {
		w := cs.State.Waiting
		if w == nil || cs.RestartCount > 0 || cs.LastTerminationState.Terminated != nil {
			continue
		}
		switch w.Reason {
		case "ImagePullBackOff", "ErrImagePull", "InvalidImageName":
			return fmt.Sprintf("container %q never ran: it couldn't download %s (%s)", cs.Name, cs.Image, imagePullCause(w.Message)), true
		}
	}
	return "", false
}

var (
	pullFailedMsg = regexp.MustCompile(`^Failed to pull image "([^"]+)": (.*)`)
)

// pullFailedEvents is why a Job's newest pod, gone now, never ran, from
// the events kept about its pods: an image it failed to pull, and no pod
// of the job started after that (one replaced while pulling may have run).
// Events from before the job was made are another job's of the same name
// (a hook or migration made again). ok is false when the events don't say so.
func pullFailedEvents(e *enrich.Enricher, j *unstructured.Unstructured) (string, bool) {
	made := j.GetCreationTimestamp().Time
	var pod, why string
	var at, startedAt time.Time
	for _, ev := range e.List(kube.Events, j.GetNamespace()) {
		name := str(ev, "involvedObject", "name")
		t := enrich.EventTime(ev)
		if str(ev, "involvedObject", "kind") != "Pod" || t.Before(made) || !jobPodName(e, j, name) {
			continue
		}
		if str(ev, "reason") == "Started" {
			if t.After(startedAt) {
				startedAt = t
			}
			continue
		}
		m := pullFailedMsg.FindStringSubmatch(strings.Join(strings.Fields(str(ev, "message")), " "))
		if m == nil {
			continue
		}
		if pod == "" || t.After(at) {
			pod, at = name, t
			why = fmt.Sprintf("pod %s never ran: it couldn't download %s (%s)", name, m[1], imagePullCause(m[2]))
		}
	}
	if pod == "" || !startedAt.IsZero() && !startedAt.Before(at) {
		return "", false
	}
	return why, true
}

// The 5 characters Kubernetes adds to a pod's name (generateName) have
// no vowels and no 0, 1 or 3; an indexed job's pods put the index first.
var (
	jobPodRandom  = regexp.MustCompile(`^[bcdfghjklmnpqrstvwxz2456789]{5}$`)
	jobPodIndexed = regexp.MustCompile(`^([0-9]+)-[bcdfghjklmnpqrstvwxz2456789]{5}$`)
)

// jobPodName: name is one of job j's pods, as Kubernetes names them once
// the pod itself is gone: <job>-<random>, or <job>-<index>-<random> for
// an indexed job, an index below its completions and no other job named
// <job>-<index> here (a cron job's run, a job run again as <job>-2).
func jobPodName(e *enrich.Enricher, j *unstructured.Unstructured, name string) bool {
	rest, ok := strings.CutPrefix(name, j.GetName()+"-")
	if !ok {
		return false
	}
	if jobPodRandom.MatchString(rest) {
		return true
	}
	m := jobPodIndexed.FindStringSubmatch(rest)
	if m == nil || str(j, "spec", "completionMode") != "Indexed" {
		return false
	}
	if i, err := strconv.ParseInt(m[1], 10, 64); err != nil || i >= num(j, "spec", "completions") {
		return false
	}
	_, other := e.Get(kube.Jobs, j.GetNamespace(), j.GetName()+"-"+m[1])
	return !other
}

// readyCell is READY: ready pods out of its pods, or, while some are being
// replaced, out of the copies it asks for (as kubectl counts, 1/2).
func readyCell(a *App) string {
	return fmt.Sprintf("%d/%d", a.Ready, readyOf(a))
}

// pausedPartwayWords: a deployment paused partway through a rollout (some
// copies of the new version made, not all, or old ones still beside
// them), from its status once that is current: "rolling out, 1 of 2
// updated; ...". "" for any other.
func pausedPartwayWords(w *unstructured.Unstructured) string {
	paused, _, _ := unstructured.NestedBool(w.Object, "spec", "paused")
	if w.GetKind() != "Deployment" || !paused || num(w, "status", "observedGeneration") < w.GetGeneration() {
		return ""
	}
	want := wantedReplicas(w)
	updated := num(w, "status", "updatedReplicas")
	if updated == 0 || updated >= want && num(w, "status", "replicas") <= updated {
		return "" // not started (a change waits for resume), or done
	}
	return fmt.Sprintf("rolling out, %d of %d updated; nothing more changes until it is resumed", updated, want)
}

// pausedLine says a paused deployment is paused, partway through a
// rollout or not, and how to resume it: a change to it (i) waits for that,
// with nothing else to say so once nothing is rolling out.
func pausedLine(dep *unstructured.Unstructured) string {
	words := pausedPartwayWords(dep)
	if words == "" {
		words = "a change to it rolls out only once it is resumed"
	}
	return words + " (kubectl rollout resume deployment/" + dep.GetName() + " -n " + dep.GetNamespace() + ")"
}

// resumeFix: how to resume a paused deployment, which kubed has no key
// for.
func resumeFix(dep *unstructured.Unstructured, readOnly bool) string {
	fix := "resume it: kubectl rollout resume deployment/" + dep.GetName() + " -n " + dep.GetNamespace()
	if readOnly {
		return fix + " (or set spec.paused to false), without --readonly"
	}
	return fix + " (or e and set spec.paused to false)"
}

// rollingOutWords: a deployment (not paused) partway through a rollout,
// new copies made and old ones still beside them, from its status once
// that is current: "rolling out, 2 of 2 updated; 1 pod of the old
// version still running". "" for any other.
func rollingOutWords(w *unstructured.Unstructured) string {
	paused, _, _ := unstructured.NestedBool(w.Object, "spec", "paused")
	if w.GetKind() != "Deployment" || paused || num(w, "status", "observedGeneration") < w.GetGeneration() {
		return ""
	}
	want := wantedReplicas(w)
	updated := num(w, "status", "updatedReplicas")
	old := num(w, "status", "replicas") - updated
	if updated == 0 || old <= 0 {
		return ""
	}
	return fmt.Sprintf("rolling out, %d of %d updated; %s of the old version still running", updated, want, plural(int(old), "pod"))
}

// onlyDeployments: everything the app runs is a Deployment.
func onlyDeployments(a *App) bool {
	for k := range a.kinds {
		if k != "Deployment" {
			return false
		}
	}
	return len(a.kinds) > 0
}

// replacing: a healthy app with a pod on its way out and fewer ready than
// READY counts out of says so, not "healthy" (none ready is worth a look).
// A rollout that surges (its new copy ready first) stays healthy.
func replacing(a *App) (Status, bool) {
	want := readyOf(a)
	if a.Pods == 0 || a.Ready+a.placeholders >= want || a.Status != (Status{"Running", "healthy", ToneNormal}) {
		return Status{}, false
	}
	tone := ToneNormal
	if a.Ready == 0 {
		tone = ToneWarn
	}
	if a.Stopping == 0 { // a copy starting up, nothing on its way out
		if a.starting == 0 {
			return Status{}, false // not ready for another reason (a job's run, a placeholder): its own status says so
		}
		why := fmt.Sprintf("%d of %d ready", a.Ready, want) // as READY counts
		if a.notReady.Why != "" && a.notReady.Why != "healthy" {
			why += "; " + a.notReady.Why
		}
		return Status{"Starting", why, tone}, true
	}
	short := "Replacing a pod"
	if a.Stopping > 1 {
		short = "Replacing pods"
	}
	return Status{short, fmt.Sprintf("%d of %d ready; %s shutting down, a new one follows", a.Ready, want, plural(a.Stopping, "pod")), tone}, true
}

// checksWords names the checks that killed an app's containers: "alive
// check", or "alive check or startup check" when both did.
func checksWords(checks map[string]bool) string {
	names := slices.Sorted(maps.Keys(checks))
	return strings.Join(names, " or ")
}

// goingAway: every pod of the app (running or stopping) belongs to one of
// its Deployments or StatefulSets that asks for no copies now, and
// nothing else it runs asks for any, so none come back.
func goingAway(a *App) bool {
	if len(a.zeroed) == 0 || len(a.podOwners) == 0 || a.wants > 0 { // another workload asks for copies: they follow
		return false
	}
	for uid := range a.podOwners {
		if !a.zeroed[uid] {
			return false
		}
	}
	return true
}

// readyOf is what READY counts ready pods out of.
func readyOf(a *App) int {
	of := a.Pods
	// Pods no workload here asks for (bare pods, others' pods) count as
	// they are, on top of what the workloads ask for. A pod is a
	// workload's when any owner up its chain is (an operator's stateful
	// set's pods are the stateful set's).
	wants, extraNotReady := int(a.wants), 0
	if a.wants > 0 {
		for i, chain := range a.podChains { // a pod none of whose owners is a workload counted in wants
			if !slices.ContainsFunc(chain, func(uid types.UID) bool { return a.wanted[uid] }) {
				wants++
				if !a.chainReady[i] {
					extraNotReady++
				}
			}
		}
	}
	switch {
	case a.Status.Short == "Missing pods":
		return a.Pods + a.missing // as kubectl counts: 2/3, the copies it can't make too
	case a.Status.Short == "Paused" && a.pausedPartway != "", a.Status.Short == "Rolling out":
		return wants // mid-rollout: out of what it asks for, as kubectl counts (3/2)
	case a.Pods == 0 && a.wants > 0:
		of = wants // none made (a deployment made paused, a quota): 0/1, as gd counts
	case a.Stopping > 0 && wants > of:
		of = wants // copies being replaced
	case a.wants > 0 && wants < of && a.Ready <= wants && !a.kinds["DaemonSet"] && !a.kinds["CronJob"] && !a.kinds["Job"] && a.Status.Short != "Rollout stuck":
		// Mid-rollout, old and new copies overlap: count what it asks
		// for, as kubectl's READY does (0/1, not 0/2). Not once the
		// rollout is stuck: its new copy that never gets ready stays, and
		// counts (3/4, next to that pod's restarts).
		of = wants
	}
	// Never more ready than there are: pods kubed doesn't know a workload
	// for (one per node, a job's runs) still count. Nor all ready while a
	// pod no workload asks for isn't: a rollout's extra ready copy doesn't
	// stand in for it (3/4, not 3/3).
	return max(of, a.Ready+extraNotReady)
}

// Runs says whether the app has workloads of this kind (Deployment,
// CronJob…), from the cluster or from ArgoCD's record.
func (a *App) Runs(kind string) bool { return managedKinds(a)[kind] }

// managedKinds are the kinds an ArgoCD app manages.
func managedKinds(a *App) map[string]bool {
	out := map[string]bool{}
	for k := range a.kinds {
		out[k] = true
	}
	if a.Argo != nil {
		for _, r := range a.Argo.Resources {
			out[r.Kind] = true
		}
	}
	return out
}

// syncWords is ArgoCD's sync state for a column: "in sync", "out of sync".
func syncWords(s string) string {
	switch s {
	case "Synced":
		return "in sync"
	case "OutOfSync":
		return "out of sync"
	case "Unknown":
		return "unknown"
	}
	return enrich.Or(s)
}

// healthWords is ArgoCD's health for a column.
func healthWords(h string) string {
	if w, ok := argoHealthWords[h]; ok {
		return w
	}
	return enrich.Or(h)
}

// argoHealthWords are ArgoCD's health states, the way people say them.
var argoHealthWords = map[string]string{
	"Healthy":     "healthy",
	"Progressing": "updating",
	"Degraded":    "failing",
	"Missing":     "parts missing",
	"Suspended":   "paused",
	"Unknown":     "health unknown",
}

// argoWords says what ArgoCD is doing with the app, in words.
func argoWords(a enrich.ArgoApp) string {
	if !a.Found { // only its tracking label is here: nothing says how it syncs
		return "its record can't be read here (another cluster, or no access), so how it syncs is unknown"
	}
	var parts []string
	switch a.Sync {
	case "Synced":
		parts = append(parts, "in sync with Git")
	case "OutOfSync":
		parts = append(parts, "out of sync with Git")
	}
	if h := argoHealthWords[a.Health]; h != "" {
		if a.Sync == "Synced" && (a.Health == "Degraded" || a.Health == "Missing") && len(parts) > 0 {
			parts[len(parts)-1] += ", but " + h // matching Git and failing are two facts; say how they go together
		} else {
			parts = append(parts, h)
		}
	} else if a.Health != "" {
		parts = append(parts, strings.ToLower(a.Health))
	}
	switch {
	case a.AutoSync && a.SelfHeal:
		parts = append(parts, "auto-sync, undoes manual edits")
	case a.AutoSync:
		parts = append(parts, "auto-sync")
	default:
		parts = append(parts, "you sync it")
	}
	return strings.Join(parts, " · ")
}

// appFix is the next step for an app's problem, in keys that work here.
func appFix(a *App, readOnly bool) string {
	fix := appFixFor(a, readOnly)
	if a.undoMaybe != "" && a.CanUndo && strings.Contains(fix, "u to go back") {
		fix += " (" + a.undoMaybe + ")"
	}
	// A job's failed pods already removed (by its cleanup or by hand): they
	// can't be opened, and their logs went with them.
	if s := a.Status.Short; (s == "Job failed" || s == "Retrying") && a.CrashPod == "" && (a.Pods == 0 && a.Leftovers == 0 || a.jobPodsGone) {
		fix = strings.Replace(fix, "enter → Open app (its pods), then l on the failed one shows why", "its failed pods are gone, and their logs with them (E shows its events)", 1)
		if a.failedNoCause { // nothing says a try failed by itself (one deleted by hand counts as failed)
			fix = strings.Replace(fix, "fix it and create the job again", noCauseRunAgain, 1)
		}
	}
	if s := a.Status.Short; (s == "Job failed" || s == "Retrying") && strings.Contains(a.Status.Why, deletedStopping) {
		fix = strings.Replace(fix, "l on the failed one shows why", "l on the stopping one shows its logs", 1)
		fix = strings.Replace(fix, "fix it and create the job again", "its pod was deleted, not failed by itself: create the job again once it has stopped", 1)
	}
	return fix
}

// noCauseRunAgain ends a failed job's Fix when its pods are gone and
// nothing says why: kubed can't tell that anything needs fixing.
const noCauseRunAgain = "create the job again if it should run"

// noUndoWhy says why a Fix doesn't offer going back: there is no earlier
// version, or the earlier one fails too.
func noUndoWhy(a *App) string {
	if a.noUndo != "" {
		return a.noUndo
	}
	return noEarlierVersion
}

// noEarlierVersion: why u is off for a first version.
const noEarlierVersion = "there is no earlier version to go back to"

// undoNote is why a Fix for a pod's own problem (a crash, running out of
// memory, a command that can't start) leaves out u, in brackets as the
// pull Fix says it: only when there is an earlier version that doesn't
// help ("" otherwise: a first version has nothing to go back to).
func undoNote(short, why string) string {
	mayFix := strings.HasPrefix(short, "Crashing") || short == "Restarting" || strings.HasPrefix(short, "Out of memory") || short == "Can't start"
	if !mayFix || why == "" || why == noEarlierVersion {
		return ""
	}
	return " (" + why + ")"
}

// oldVersionFix is the Fix for a problem of a pod of an older version
// while a newer one rolls out: nothing to change on it, and going back
// would return to it.
const oldVersionFix = "o to watch the rollout: that pod is of the old version, replaced as the new version's pods become ready"

func appFixFor(a *App, readOnly bool) string {
	where := fixWhere(a, readOnly)
	switch s := a.Status.Short; {
	case a.waitsOn != "" && slices.Contains(a.waitsHeld, a.HeadPod) && a.Status.Tone == ToneBad: // another one held named: Kubernetes waits for the first
		fix := "stateful set " + a.waitsSet + " waits for " + waitsOnWords(a.waitsOn)
		if readOnly {
			return fix + " (without --readonly)"
		}
		return fix + " (enter → Open app (its pods), then ctrl-d on " + a.waitsOn + ")"
	case s == "Rollout stuck" && strings.Contains(a.Status.Why, "can't make its pods: "):
		return cantMakeFix + where
	case (s == "Rollout stuck" || s == "Too big for any node") && strings.Contains(a.Status.Why, "more than any node here has"):
		return tooBigFix(where)
	case s == "Rollout stuck" && !a.CanUndo && a.stuckBy.Short != "":
		// Nothing to go back to: the pods' own problem says what to fix.
		inner := *a
		inner.Status = a.stuckBy
		if fix := appFix(&inner, readOnly); fix != "" {
			return fix
		}
		return "fix the image or settings " + where + ": " + noUndoWhy(a) + " (o shows the rollout)"
	case s == "Rollout stuck" && !a.CanUndo:
		return "fix the image or settings " + where + ": " + noUndoWhy(a) + " (o shows the rollout)"
	case s == "Rollout stuck" && readOnly:
		return "go back to the previous version, or fix it " + where + " (o shows the rollout; u goes back, without --readonly)"
	case s == "Rollout stuck":
		return "o to watch the rollout, or u to go back to the previous version"
	case s == "Paused" && a.pausedDep != nil && a.Status.Why == a.pausedPartway:
		return resumeFix(a.pausedDep, readOnly)
	case a.oldHead && a.Status == a.headStatus: // still the headline it was worked out for
		return oldVersionFix
	case strings.HasPrefix(s, "Crashing") && strings.Contains(a.Status.Why, hookFailed):
		return "fix its postStart hook " + where
	case startFix(s, a.Status.Why) != "":
		return startFix(s, a.Status.Why)
	case s == "Out of sync" && a.Argo != nil && !a.Argo.AutoSync:
		return "sync it in ArgoCD (this app syncs by hand). Open the ArgoCD apps (ga) and press o on this one to see what differs from Git"
	case s == "Out of sync":
		return "ArgoCD should apply it by itself. If it stays like this, open the ArgoCD apps (ga) and press o on this one to see what differs from Git"
	case strings.HasPrefix(s, "Out of memory"):
		// Killed again and again lately (the status says so): no "if". Not
		// by the app's restart count: other containers restart for other
		// reasons, and one kill a while ago reads "Hit memory limit".
		return "raise its memory limit: it keeps being killed for running out of memory; p shows the logs from before it was killed" + undoNote(s, a.noUndo)
	case strings.HasPrefix(s, "Hit memory limit"):
		return "raise its memory limit if this repeats; p shows the logs from before it was killed"
	case (strings.HasPrefix(s, "Crashing") || s == "Restarting") && checkFix(a.Status.Why) != "":
		return checkFix(a.Status.Why) + undoNote(s, a.noUndo)
	case (strings.HasPrefix(s, "Crashing") || s == "Restarting") && strings.Contains(a.Status.Why, "keeps stopping"):
		return stoppingFix + undoNote(s, a.noUndo)
	case strings.HasPrefix(s, "Crashing"), s == "Restarting":
		return "p shows the logs from before the last crash; l shows the live ones" + undoNote(s, a.noUndo)
	case s == "Not succeeding" && strings.Contains(a.Status.Why, "failedJobsHistoryLimit 0"):
		return "its failed runs are removed, and their logs with them: set failedJobsHistoryLimit above 0 " + where + " to keep the next one; gC, then E on the cron job shows its events"
	case s == "Not succeeding":
		return "its run is gone, and its logs with it: gC, then E on the cron job shows its events"
	case (s == "Job failed" || s == "Retrying") && strings.Contains(a.Status.Why, "never ran: it couldn't download"):
		return "it never ran, so it has no logs: fix the image name or tag " + where + ", or check the registry access"
	case s == "Can't start", (s == "Job failed" || s == "Retrying" || s == "Failed") && strings.Contains(a.Status.Why, "never started"):
		return "it never started, so it has no logs: the command or a file it runs is missing from the image (or can't be run). Fix the command " + where + ", or use an image that has it" + undoNote(s, a.noUndo)
	case strings.HasPrefix(s, "Failed"):
		return "l shows its logs, up to where it failed"
	case s == "Can't build":
		return "fix the error in Git (its chart or manifests); d shows ArgoCD's full message"
	case s == "Never runs" && readOnly:
		return "fix its schedule in Git, or remove the cron job if nothing needs it"
	case s == "Never runs":
		return "fix its schedule in Git (or e to edit it), or remove the cron job if nothing needs it"
	case s == "No pods", s == "Missing pods":
		return cantMakeFix + where
	case strings.HasPrefix(s, "Can't pull image") && (a.Pods > 0 && a.barePods == a.Pods || a.headBare()):
		return "fix the image name or tag " + bareWhere(a) + ", or check registry access"
	case strings.HasPrefix(s, "Can't pull image") && a.staleRuns && runsOnlyJobs(a) && readOnly:
		return "its cron job runs another image now; these runs still wait on the old one and never start: delete them, without --readonly"
	case strings.HasPrefix(s, "Can't pull image") && a.staleRuns && runsOnlyJobs(a):
		return "its cron job runs another image now; these runs still wait on the old one and never start: enter → Open app (its pods), then ctrl-d on them"
	case strings.HasPrefix(s, "Can't pull image") && runsOnlyJobs(a) && a.Runs("CronJob") && a.Argo == nil && readOnly:
		return "fix the image name or tag in its cron job, without --readonly: the next run uses it"
	case strings.HasPrefix(s, "Can't pull image") && runsOnlyJobs(a) && a.Runs("CronJob") && a.Argo == nil:
		return "fix the image name or tag in its cron job (gC, then e on it): the next run uses it"
	case strings.HasPrefix(s, "Can't pull image") && runsOnlyJobs(a) && a.Argo == nil:
		return "a job's image can't be changed once it exists: fix it where the job is defined, then create the job again"
	case strings.HasPrefix(s, "Can't pull image") && a.Argo == nil && readOnly && !a.CanUndo:
		return "fix the image name or tag, without --readonly (" + noUndoWhy(a) + ")"
	case strings.HasPrefix(s, "Can't pull image") && a.Argo == nil && readOnly:
		return "fix the image name or tag (or go back to the previous version), without --readonly"
	case strings.HasPrefix(s, "Can't pull image") && a.Argo == nil && !a.CanUndo:
		return "i to fix the image name or tag (" + noUndoWhy(a) + ")" // i works from the app itself
	case strings.HasPrefix(s, "Can't pull image") && a.Argo == nil:
		return "u to go back to the previous version, or i to fix the image"
	case strings.HasPrefix(s, "Can't pull image"):
		return "fix the image name or tag in Git, or check the registry access"
	case s == "Try failed" && readOnly:
		fix := "its run failed, but another container keeps the pod running: delete that pod to end this try (the job starts another if it has tries left), without --readonly"
		if strings.Contains(a.Status.Why, "ran out of memory") {
			fix += "; then raise its memory limit where the job is defined"
		}
		return fix
	case s == "Try failed":
		fix := "its run failed, but another container keeps the pod running: enter → Open app (its pods), then ctrl-d on that pod ends this try (the job starts another if it has tries left); l shows why it failed"
		if strings.Contains(a.Status.Why, "ran out of memory") {
			fix += "; then raise its memory limit where the job is defined"
		}
		return fix
	case (s == "Job failed" || s == "Retrying") && strings.Contains(a.Status.Why, "ran out of memory") && a.cronRun() && (a.Argo != nil || readOnly):
		return "raise its memory limit in its cron job " + where + ": the next run uses it; enter → Open app (its pods), then l on the failed one shows why"
	case (s == "Job failed" || s == "Retrying") && strings.Contains(a.Status.Why, "ran out of memory") && a.cronRun():
		return "raise its memory limit in its cron job (gC, then e on it): the next run uses it; enter → Open app (its pods), then l on the failed one shows why"
	case (s == "Job failed" || s == "Retrying") && strings.Contains(a.Status.Why, "ran out of memory"):
		return "raise its memory limit where the job is defined (a job can't be changed once made), then create it again; enter → Open app (its pods), then l on the failed one shows why"
	case s == "Job failed" && a.cronRun() && readOnly: // t runs it now: a change
		return "enter → Open app (its pods), then l on the failed one shows why; the next run is on schedule"
	case s == "Job failed" && a.cronRun():
		return "enter → Open app (its pods), then l on the failed one shows why; the next run is on schedule (or t on its cron job runs it now)"
	case s == "Job failed":
		return "enter → Open app (its pods), then l on the failed one shows why; fix it and create the job again"
	case s == "Retrying":
		return "enter → Open app (its pods), then l on the failed one shows why; Kubernetes runs it again until its tries run out"
	case s == "Too big for any node":
		return tooBigFix(where)
	case strings.HasPrefix(s, "Waiting for a node") && nodeWaitFix(a.Status.Why, where) != "":
		return nodeWaitFix(a.Status.Why, where)
	case strings.HasPrefix(s, "Waiting for a node"):
		return "E shows the scheduler's reason (often the cluster is adding a node; if it stays, nodes are full)"
	case strings.HasPrefix(s, "Not ready") && heldBackFix(a.Status.Why) != "":
		return heldBackFix(a.Status.Why)
	case strings.HasPrefix(s, "Not ready"):
		return "E shows which probe fails; l shows the logs"
	case s == "Bad config":
		return badConfigFix(a.Status.Why)
	}
	return ""
}

// tooBigFix: a pod that asks for more than any node has, its request
// lowered where (fixWhere's words) or a bigger node added.
func tooBigFix(where string) string {
	return "lower what it asks for (its CPU or memory request) " + where + ", or add a bigger machine type to the cluster"
}

// nodeWaitFix: a pod no node takes for its node selector or the nodes'
// taints (not for full nodes), fixed where (fixWhere's words). Nothing
// for any other wait.
func nodeWaitFix(why, where string) string {
	switch {
	case nodesShort(why):
	case strings.Contains(why, "match its node selector"):
		return "no node here has the labels it asks for: if a node of that kind is being added, wait a few minutes; if not, fix its node selector (or node affinity) " + where + ". E shows the scheduler's reason"
	case strings.Contains(why, "reserved for other workloads"):
		return "every node is reserved for other workloads (taints): if a node is still starting, wait a few minutes; if not, give it the toleration for the nodes it should run on " + where + ". E shows the scheduler's reason"
	}
	return ""
}

// aliveCheckFix: a container Kubernetes keeps killing because its
// liveness check fails, not because it crashes.
const aliveCheckFix = "E shows the alive check's answers: fix the check (its command, path or port) or why the app stops answering it; l shows the live logs"

// checkFix is the fix for containers killed by their checks, by which
// checks the status names ("" when none): both get advice for both.
func checkFix(why string) string {
	switch {
	case strings.Contains(why, "alive check or startup check"):
		return bothChecksFix
	case strings.Contains(why, "killed by its alive check"), strings.Contains(why, "killed by their alive check"):
		return aliveCheckFix
	case strings.Contains(why, "killed by its startup check"), strings.Contains(why, "killed by their startup check"):
		return startupCheckFix
	}
	return ""
}

// bothChecksFix: containers killed by their alive check, and others (or
// the same, at other times) by their startup check.
const bothChecksFix = "E shows the checks' answers: fix the alive check (its command, path or port) or why the app stops answering it, and fix the startup check or give the app longer to start (more tries); l shows the live logs"

// startupCheckFix: a container Kubernetes keeps killing because its
// startup check doesn't pass in the time the check allows.
const startupCheckFix = "E shows the startup check's answers: fix the check (its command, path or port), give the app longer to start (more tries), or find why it doesn't come up; l shows the live logs"

// topJob is what made job j: its cron job, else j itself.
func topJob(e *enrich.Enricher, j *unstructured.Unstructured) *unstructured.Unstructured {
	if j == nil {
		return nil
	}
	return e.TopOwner(j)
}

// cronRun: the job its headline is about is a cron job's run; when that
// job isn't known, whether the app runs a cron job at all.
func (a *App) cronRun() bool {
	if a.failedJob != nil {
		return a.failedJob.GetKind() == "CronJob"
	}
	return a.Runs("CronJob")
}

// fixWhere is where a fix goes: Git for an app ArgoCD manages (it would
// undo an edit), else an edit here.
func fixWhere(a *App, readOnly bool) string {
	aboutJob := a.Status.Short == "Job failed" || a.Status.Short == "Retrying" || a.Status.Short == "Try failed" // the headline is the job's
	switch {
	case a.Argo != nil:
		return "in Git"
	case readOnly:
		return "where it is defined"
	case aboutJob && a.failedJob != nil && a.cronRun(): // the job that failed: its cron job holds what it runs
		return "in its cron job (gC, then e on it)"
	case aboutJob && a.failedJob != nil: // a one-off job: made again by hand
		return "where the job is defined and create the job again (a job can't be changed once made)"
	case a.headBare(): // its workloads don't make that pod
		return bareWhere(a)
	case a.Runs("Deployment") || a.Runs("StatefulSet") || a.Runs("DaemonSet"):
	case a.Pods > 0 && a.barePods == a.Pods && !a.Runs("CronJob") && !a.Runs("Job"): // nothing makes its pods: no workload to edit
		return bareWhere(a)
	case a.Runs("CronJob") || a.jobKinds["CronJob"] && aboutJob: // only runs: its cron job holds what they run
		return "in its cron job (gC, then e on it)"
	case a.Runs("Job") || a.jobKinds["Job"] && aboutJob: // a job can't be changed once made, and Its workloads has none
		return "where the job is defined and create the job again (a job can't be changed once made)"
	}
	return "(enter → Its workloads, then e)"
}

// headBare: the pod its headline is about has nothing that makes it (no
// owner), next to pods its workloads make: i, e and u reach only those.
func (a *App) headBare() bool {
	return a.HeadPod != "" && a.headPod != nil && a.barePods < a.Pods && metav1.GetControllerOfNoCopy(a.headPod) == nil
}

// bareWhere is where a pod nothing makes is fixed: named when the app
// has other pods.
func bareWhere(a *App) string {
	if a.headBare() {
		return "where pod " + a.HeadPod + " is defined (nothing makes it again if it goes)"
	}
	return "where its pod is defined (nothing makes it again if it goes)"
}

// argoPlain says what a resource's failure means, when kubed knows.
func argoPlain(broken string) string {
	low := strings.ToLower(broken)
	switch {
	case strings.Contains(low, "already owned by another applicationset"):
		return "two ApplicationSets both create the same app"
	case strings.Contains(low, "falling back"):
		return "a KEDA scaler can't read its metric, so the app stays at its fallback size"
	case strings.Contains(low, "certificate") && strings.Contains(low, "not ready"):
		return "a certificate can't be issued"
	}
	return ""
}

// argoFix suggests the next step for what the check found.
func argoFix(c ArgoCheck) string {
	all := strings.ToLower(strings.Join(c.Broken, " "))
	switch {
	case strings.Contains(all, "already owned by another applicationset"):
		return "remove the app from one of the two ApplicationSets in Git"
	case strings.Contains(all, "falling back"):
		return "check the system the scaler's trigger reads from (Prometheus, SQS…)"
	case strings.Contains(all, "pods ready"):
		return "enter → Open app shows the pods and why they aren't ready"
	case strings.Contains(all, "certificate") || strings.Contains(all, "issuer"):
		return "the certificate can't be issued: E shows cert-manager's reason"
	case len(c.Missing) > 0 && len(c.Broken) == 0:
		return "sync the app in ArgoCD to create them; d shows ArgoCD's record"
	}
	return "E shows the events; d shows ArgoCD's record"
}

// esMissingKey: "key slackToken does not exist in secret /a/b".
var esMissingKey = regexp.MustCompile(`key (\S+) does not exist in secret (\S+)`)

// SecretFix suggests a fix for an ExternalSecret error.
func SecretFix(msg, name string) string {
	low := strings.ToLower(msg)
	switch {
	case esMissingKey.MatchString(msg):
		m := esMissingKey.FindStringSubmatch(msg)
		return "add " + m[1] + " to " + m[2] + " in the store, or remove it from ExternalSecret " + name
	case strings.Contains(low, "accessdenied") || strings.Contains(low, "not authorized"):
		return "the SecretStore's IAM role can't read it: allow it in that role's policy"
	case strings.Contains(low, "parameternotfound") || strings.Contains(low, "resourcenotfound") || strings.Contains(low, "not found"):
		return "create it in the store, or fix the key name in ExternalSecret " + name
	case msg == "":
		return "E shows the app's events; gx lists ExternalSecrets"
	}
	return "fix what the error says, in the store or in ExternalSecret " + name
}

// cronGoneWords says what is known of a cron job whose last run didn't
// succeed and isn't kept: when it ran, when one last worked, and why it
// isn't kept when its history limit says so.
func cronGoneWords(e *enrich.Enricher, cj *unstructured.Unstructured) string {
	last := e.CronLastRun(cj)
	words := "cron job " + cj.GetName() + ": its last run " + Age(last) + " ago didn't succeed"
	if ok, err := time.Parse(time.RFC3339, str(cj, "status", "lastSuccessfulTime")); err == nil {
		words += " (it last worked " + Age(ok) + " ago)"
	} else {
		words += " (it has never succeeded)"
	}
	if limit, found, _ := unstructured.NestedInt64(cj.Object, "spec", "failedJobsHistoryLimit"); found && limit == 0 {
		return words + ", and Kubernetes keeps no failed runs (failedJobsHistoryLimit 0)"
	}
	return words + ", and it isn't kept"
}

// failedJobWindow is how long a failed job keeps its app marked failing.
const failedJobWindow = 24 * time.Hour

// latestRuns keeps, for each CronJob, only its newest run (an older failure
// is old news once a later run exists), and every job that isn't scheduled.
func latestRuns(e *enrich.Enricher, jobs []*unstructured.Unstructured) []*unstructured.Unstructured {
	newest := map[string]*unstructured.Unstructured{}
	var out []*unstructured.Unstructured
	for _, j := range jobs {
		top := e.TopOwner(j)
		if top.GetKind() != "CronJob" {
			out = append(out, j)
			continue
		}
		key := j.GetNamespace() + "/" + top.GetName()
		if cur, ok := newest[key]; !ok || j.GetCreationTimestamp().After(cur.GetCreationTimestamp().Time) {
			newest[key] = j
		}
	}
	for _, j := range newest {
		out = append(out, j)
	}
	return out
}

// howLong says a duration the way people do: "3 hours", "12 days", "4 months".
func howLong(d time.Duration) string {
	const day = 24 * time.Hour
	switch {
	case d < 2*time.Hour:
		return plural(int(d.Minutes()), "minute")
	case d < 2*day:
		return plural(int(d.Hours()), "hour")
	case d < 60*day:
		return plural(int(d/day), "day")
	}
	return plural(int(d/(30*day)), "month")
}

// Plural is "1 pod", "3 pods".
func Plural(n int, what string) string { return plural(n, what) }

func plural(n int, what string) string {
	if n == 1 {
		return "1 " + what
	}
	if strings.HasSuffix(what, "y") && !strings.HasSuffix(what, "ay") && !strings.HasSuffix(what, "ey") && !strings.HasSuffix(what, "oy") {
		return fmt.Sprintf("%d %sies", n, strings.TrimSuffix(what, "y")) // try → tries, copy → copies
	}
	return fmt.Sprintf("%d %ss", n, what)
}

// conditionSince is when a condition last changed.
func conditionSince(o *unstructured.Unstructured, typ string) time.Time {
	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		if m["type"] == typ {
			t, _ := time.Parse(time.RFC3339, fmt.Sprint(m["lastTransitionTime"]))
			return t
		}
	}
	return time.Time{}
}

// AppPods says whether a pod belongs to an app (for the app page).
func AppPods(e *enrich.Enricher, a *App) func(*unstructured.Unstructured) bool {
	return func(p *unstructured.Unstructured) bool {
		switch a.group {
		case groupArgo:
			argo, ok := e.ManagedBy(p)
			if ok {
				return argo.Name == a.Name
			}
			return a.ownsUnmanaged && (p.GetNamespace() == a.Namespace || a.ownedNS[p.GetNamespace()]) && joinsArgoApp(e, a, p, a.fallback) // operator-made, or the same app untracked
		case groupLabel:
			return p.GetNamespace() == a.Namespace && labelApp(e, p) == a.Name
		}
		if p.GetNamespace() != a.Namespace {
			return false
		}
		_, managed := e.ManagedBy(p)
		return !managed
	}
}

// stoppingTryJob is the job of the app's headline pod when that pod's try
// failed and Kubernetes is stopping its sidecars, and the job has tries
// left (it counts the try once the pod ends, then starts another).
func stoppingTryJob(e *enrich.Enricher, a *App) (*unstructured.Unstructured, bool) {
	if a.HeadPod == "" || !strings.HasSuffix(a.Status.Why, ", then the pod ends") {
		return nil, false
	}
	p, ok := e.Get(kube.Pods, a.HeadNS, a.HeadPod)
	if !ok {
		return nil, false
	}
	ref := metav1.GetControllerOfNoCopy(p)
	if ref == nil || ref.Kind != "Job" {
		return nil, false
	}
	pod, err := ToPod(p)
	if err != nil || !anyRunning(pod) { // only while its sidecars stop (not a pod frozen on a lost node)
		return nil, false
	}
	j, ok := e.Get(kube.Jobs, a.HeadNS, ref.Name)
	if !ok {
		return nil, false
	}
	if !oneAtATime(j) || str(j, "spec", "suspend") == "true" { // other pods may count too; paused starts none
		return nil, false
	}
	for _, typ := range []string{"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet", "Suspended"} {
		if c, ok := enrich.Condition(j, typ); ok && c.Status == "True" {
			return nil, false
		}
	}
	for _, f := range []string{"backoffLimitPerIndex", "podFailurePolicy"} { // tries counted another way
		if _, found, _ := unstructured.NestedFieldNoCopy(j.Object, "spec", f); found {
			return nil, false
		}
	}
	limit, found, _ := unstructured.NestedInt64(j.Object, "spec", "backoffLimit")
	if !found {
		limit = 6 // Kubernetes' default
	}
	n := failedTries(e, j)
	if done, _ := tryFailed(pod); !done { // a main container yet to start: its try isn't counted yet
		n++
	}
	return j, n <= limit
}

// cantStartWhy is why a stuck rollout's pods can't start, from their
// problem: "can't start: crashing: ...", but "can't pull image: ..." once.
func cantStartWhy(st Status) string {
	if st.Short == "Bad config" { // its why says "can't start" already: a verb before it, as after "first version of deployment/x"
		return "has bad config: " + st.Why
	}
	if strings.HasPrefix(st.Short, "Can't") {
		return strings.ToLower(st.Short) + ": " + st.Why
	}
	return "can't start: " + strings.ToLower(st.Short) + ": " + st.Why
}

// beforeKeyHint puts more words before a Why's closing key hint ("p shows
// ..."), which the WHY cell and the Problem line cut off.
func beforeKeyHint(why, more string) string {
	loc := keyHint.FindStringIndex(why)
	if loc == nil {
		return why + more
	}
	cut := loc[0]
	if cut > 0 && why[cut-1] == '.' { // "... 1m ago. p shows": the words go before the full stop
		cut--
	}
	return why[:cut] + more + why[cut:]
}

// quotedContainer is the container a status names: `container "api" ...`.
var quotedContainer = regexp.MustCompile(`container "([^"]+)"`)

// crashingContainer is the container a "Crashing" status is about, and its
// restarts; "" for any other status.
func crashingContainer(p *corev1.Pod, st Status) (string, int32) {
	if !strings.HasPrefix(st.Short, "Crashing") {
		return "", 0
	}
	m := quotedContainer.FindStringSubmatch(st.Why)
	if m == nil {
		return "", 0
	}
	for _, cs := range append(RunningStatuses(p), p.Status.InitContainerStatuses...) {
		if cs.Name == m[1] {
			return cs.Name, cs.RestartCount
		}
	}
	return m[1], 0
}

// crashCopy: a pod crashing, as a pick between copies sees it. rank is
// the pick's own (how bad, and whether now); stuck and undo are a stuck
// rollout's reason when quoting it.
type crashCopy struct {
	rank      int
	st        Status
	ns, pod   string
	container string
	restarts  int32
	stuck     string
	undo      bool
}

// keepNamed: among copies at rank crashing container, the one home named
// last refresh, when it isn't picked (picked) and is within one of the
// most crashed. Without it a near tie goes to the first by name each
// refresh, and copies crashing alike swap as they crash in turn.
func keepNamed(e *enrich.Enricher, copies []crashCopy, rank int, container, picked string) (crashCopy, bool) {
	if container == "" {
		return crashCopy{}, false
	}
	most, keep, found := int32(-1), crashCopy{}, false
	for _, c := range copies {
		if c.rank != rank || c.container != container {
			continue
		}
		most = max(most, c.restarts)
		if !found && e.Named(c.ns, c.pod, c.container) {
			keep, found = c, true
		}
	}
	if !found || keep.pod == picked || keep.restarts+1 < most {
		return crashCopy{}, false
	}
	return keep, true
}

// headlineRank orders pods for an app's headline: how bad first, then one
// happening now (the pod isn't ready) before one that has passed (killed
// for memory an hour ago and running fine again, next to one crash-looping
// now).
// versionRank is headlineRank, made room for one more step: a pod of a
// Deployment's older version while a newer one exists has a problem of the
// version on its way out. It ranks below any problem of the current
// version (a fix rolling out that has its own trouble is what to read),
// above a pod that is fine.
func versionRank(e *enrich.Enricher, o *unstructured.Unstructured, p *corev1.Pod, st Status) int {
	if (st.Tone == ToneBad || st.Tone == ToneWarn) && oldVersionPod(e, o) {
		return (severity(ToneNormal)*2+1)*2 + 2
	}
	return headlineRank(p, st)*2 + 1
}

// oldVersionPod: a pod of a Deployment's version older than the one it
// runs now, mid-rollout (its revision isn't the Deployment's). Not one
// being deleted (its own status and Fix stand: stuck stopping), nor one
// of a paused or Recreate deployment: nothing replaces it as the new
// version's pods become ready.
func oldVersionPod(e *enrich.Enricher, o *unstructured.Unstructured) bool {
	if o.GetDeletionTimestamp() != nil {
		return false
	}
	rs := e.Controller(o)
	if rs == nil || rs.GetKind() != "ReplicaSet" {
		return false
	}
	dep := e.Controller(rs)
	if dep == nil || dep.GetKind() != "Deployment" {
		return false
	}
	if paused, _, _ := unstructured.NestedBool(dep.Object, "spec", "paused"); paused || str(dep, "spec", "strategy", "type") == "Recreate" {
		return false
	}
	const key = "deployment.kubernetes.io/revision"
	now, mine := dep.GetAnnotations()[key], rs.GetAnnotations()[key]
	return now != "" && mine != "" && now != mine
}

// undoInfo is whether u helps a Deployment in trouble, and why not.
type undoInfo struct {
	helps bool
	why   string
	// maybe, when it helps: why it may not (that version failed when it
	// last ran, from evidence that often passes), for the Fix line.
	maybe string
}

// undoState says whether going back helps, as u picks its target
// (undoTarget): the first older version that isn't only a restart of now.
// Not when there is none, or when that version's own pods fail too
// (going back to the image that can't be pulled fixes nothing).
func undoState(e *enrich.Enricher, dep *unstructured.Unstructured) undoInfo {
	r := e.RolloutOf(dep)
	if r.Previous == nil || len(r.All) == 0 {
		return undoInfo{why: noEarlierVersion}
	}
	var target *enrich.Version
	for _, v := range r.All[1:] {
		if v.Settings != r.All[0].Settings { // runs something else, not just a restart of now
			target = v
			break
		}
	}
	if target == nil {
		return undoInfo{why: "every earlier version kept runs the same as now"}
	}
	if _, fails := VersionFails(e, dep, target.Name); fails {
		return undoInfo{why: "going back won't help: the previous version fails too"}
	}
	if why, firm := VersionFailedBefore(e, dep, target, r.All[0]); firm {
		return undoInfo{why: "going back won't help: that version failed when it last ran: " + why}
	} else if why != "" { // often passing (a surge, a pull that timed out): u stays offered
		return undoInfo{helps: true, maybe: "going back may not help: that version failed when it last ran: " + why}
	}
	return undoInfo{helps: true}
}

// VersionFailedBefore says why a kept version failed when it last ran, from
// the events Kubernetes keeps (about an hour): its ReplicaSet couldn't make
// pods, or a pod of the workload couldn't download an image only that
// version runs. "" when they say nothing. A bound: it may work now. firm:
// the registry said that image or tag doesn't exist, or denied access
// (it won't work without a change); the rest often passes.
func VersionFailedBefore(e *enrich.Enricher, w *unstructured.Unstructured, v, current *enrich.Version) (why string, firm bool) {
	if w.GetKind() == "Deployment" {
		if rs, ok := e.Get(kube.ReplicaSets, w.GetNamespace(), v.Name); ok {
			if msg, _, ok := e.LatestWarningFor(rs, "FailedCreate"); ok {
				why = "Kubernetes couldn't make its pods: " + msg
			}
		}
	}
	bare := func(images []string) []string { // "api:1 (helper)" -> "api:1"
		var out []string
		for _, i := range images {
			out = append(out, strings.SplitN(i, " (", 2)[0])
		}
		return out
	}
	mine, now := bare(v.Images), bare(current.Images)
	for _, ev := range e.List(kube.Events, w.GetNamespace()) {
		if str(ev, "reason") != "Failed" || str(ev, "involvedObject", "kind") != "Pod" || !podOfVersion(w, v, str(ev, "involvedObject", "name")) {
			continue
		}
		m := pullFailedMsg.FindStringSubmatch(strings.Join(strings.Fields(str(ev, "message")), " "))
		if m == nil {
			continue
		}
		if img := enrich.ShortImage(m[1]); slices.Contains(mine, img) && !slices.Contains(now, img) {
			cause := imagePullCause(m[2])
			if f := cause == "that image or tag doesn't exist" || cause == "access denied by the registry"; f || why == "" {
				why, firm = "its image "+img+" couldn't be downloaded ("+cause+")", f
			}
			if firm {
				break
			}
		}
	}
	return why, firm
}

// podOfVersion: a pod name a workload's version gives its pods (the pod
// may be gone): a ReplicaSet's name and 5 letters, a daemon set's name and
// 5 letters, a stateful set's name and a number.
func podOfVersion(w *unstructured.Unstructured, v *enrich.Version, pod string) bool {
	prefix := w.GetName() + "-"
	if w.GetKind() == "Deployment" {
		prefix = v.Name + "-"
	}
	rest, ok := strings.CutPrefix(pod, prefix)
	if !ok || rest == "" || strings.Contains(rest, "-") {
		return false
	}
	if w.GetKind() == "StatefulSet" {
		_, err := strconv.Atoi(rest)
		return err == nil
	}
	return true
}

// VersionFails is the problem of a failing pod of one of a workload's
// kept versions (a Deployment's ReplicaSet, or a StatefulSet's or
// DaemonSet's ControllerRevision), as undoState checks the one u goes to.
func VersionFails(e *enrich.Enricher, dep *unstructured.Unstructured, rsName string) (Status, bool) {
	for _, o := range e.PodsOf(dep) {
		rs := e.Controller(o)
		// A StatefulSet's pods carry the revision's name, a DaemonSet's only its hash.
		h := o.GetLabels()["controller-revision-hash"]
		ofRevision := rs != nil && rs.GetUID() == dep.GetUID() && h != "" && (h == rsName || dep.GetName()+"-"+h == rsName)
		if !ofRevision && (rs == nil || rs.GetName() != rsName) {
			continue
		}
		if p, err := ToPod(o); err == nil && !leftover(p) {
			if st := Explain(e, p); st.Tone == ToneBad {
				return st, true
			}
		}
	}
	return Status{}, false
}

// undoHelps is undoState's answer alone.
func undoHelps(e *enrich.Enricher, dep *unstructured.Unstructured) bool {
	return undoState(e, dep).helps
}

func headlineRank(p *corev1.Pod, st Status) int {
	rank := severity(st.Tone) * 2
	if !podFullyReady(p) || stoppedWithin(p, 10*time.Minute) { // a crash loop between crashes is still now
		rank++
	}
	return rank
}

// deletingPod: one of job j's pods, not one that finished fine, is being
// deleted and still there (each read as it is now, as jobRetrying does).
func deletingPod(e *enrich.Enricher, j *unstructured.Unstructured) bool {
	for _, o := range e.OwnedPods(j) {
		now, ok := e.Get(kube.Pods, o.GetNamespace(), o.GetName())
		if !ok || now.GetDeletionTimestamp() == nil || str(now, "status", "phase") == string(corev1.PodSucceeded) {
			continue
		}
		sawDeletingMu.Lock() // once gone, that may be all that says why its try ended
		if sawDeleting[j.GetUID()] == nil {
			sawDeleting[j.GetUID()] = map[string]time.Time{}
		}
		asked := now.GetDeletionTimestamp().Time // the time it goes, its grace after it was asked
		if g := now.GetDeletionGracePeriodSeconds(); g != nil {
			asked = asked.Add(-time.Duration(*g) * time.Second)
		}
		sawDeleting[j.GetUID()][now.GetName()] = asked
		sawDeletingMu.Unlock()
		return true
	}
	return false
}

// restartedAt is when the container a "Restarting" status names last
// stopped; zero for any other status.
func restartedAt(p *corev1.Pod, st Status) time.Time {
	if p == nil || st.Short != "Restarting" {
		return time.Time{}
	}
	m := quotedContainer.FindStringSubmatch(st.Why)
	if m == nil {
		return time.Time{}
	}
	for _, cs := range RunningStatuses(p) { // a native sidecar's too, as restartStatus names it
		if t := newestStop(cs); cs.Name == m[1] && t != nil { // stopped now: that stop is the latest
			return t.FinishedAt.Time
		}
	}
	return time.Time{}
}

// crashyShort: a headline about a crash, whose Fix line sends you to the
// logs from before it (p).
func crashyShort(short string) bool {
	for _, w := range []string{"Crashing", "Out of memory", "Hit memory limit", "Restarting", "Retrying", "Can't start", "Job failed", "Try failed", "Failed"} {
		if strings.HasPrefix(short, w) {
			return true
		}
	}
	return false
}

// PrevLogsOfHead says p on the app opens the headline pod's earlier logs (its
// Fix line says p for that pod), not the crash its box quotes.
func (a *App) PrevLogsOfHead() bool {
	short := a.Status.Short
	if short == "Rollout stuck" {
		short = a.stuckBy.Short // its pods' own problem
	}
	return a.HeadPod != "" && crashyShort(short)
}

// oneAtATime: a job that runs a single pod at a time until one succeeds,
// so its tries can be numbered.
func oneAtATime(j *unstructured.Unstructured) bool {
	for _, f := range []string{"parallelism", "completions"} {
		if n, ok, _ := unstructured.NestedInt64(j.Object, "spec", f); ok && n > 1 {
			return false
		}
	}
	n, _, _ := unstructured.NestedInt64(j.Object, "status", "succeeded")
	return n == 0
}

// RunningStatuses are the statuses of the containers that run for the
// pod's whole life: its containers, plus helpers declared as init
// containers that keep running (restartPolicy Always), as kubectl counts.
func RunningStatuses(p *corev1.Pod) []corev1.ContainerStatus {
	out := append([]corev1.ContainerStatus(nil), p.Status.ContainerStatuses...)
	helper := map[string]bool{}
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			helper[c.Name] = true
		}
	}
	for _, cs := range p.Status.InitContainerStatuses {
		if helper[cs.Name] {
			out = append(out, cs)
		}
	}
	return out
}

// runningContainers counts what RunningStatuses covers, from the spec.
func runningContainers(p *corev1.Pod) int {
	n := len(p.Spec.Containers)
	for _, c := range p.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			n++
		}
	}
	return n
}

func podFullyReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning || len(p.Status.ContainerStatuses) == 0 {
		return false
	}
	// Kept out of traffic though its containers last said ready (a
	// readiness gate, or a node that stopped reporting): not ready, as
	// its status says.
	if c := podCondition(p, corev1.PodReady); c != nil && c.Status != corev1.ConditionTrue {
		return false
	}
	for _, cs := range RunningStatuses(p) {
		if !cs.Ready {
			return false
		}
	}
	return true
}

// namespaceOwner is the ArgoCD app that owns a namespace: the only one
// running there, or the one named after the namespace.
// joinsArgoApp: a pod ArgoCD doesn't track still belongs to the ArgoCD
// app in its namespace when an operator made it, or when it would form an
// app of the same name anyway (its app label, or the namespace).
func joinsArgoApp(e *enrich.Enricher, a *App, pod *unstructured.Unstructured, fallback string) bool {
	if operatorMade(e, pod) {
		return true
	}
	if fallback == groupLabel {
		return labelApp(e, pod) == a.Name
	}
	return pod.GetNamespace() == a.Name
}

// operatorMade: a pod made by an operator, not a plain workload. Its top
// owner is a custom resource (a Typesense or VictoriaMetrics cluster…), so
// it belongs to the ArgoCD app that installed that resource. A plain
// Deployment someone made by hand in the same namespace does not.
func operatorMade(e *enrich.Enricher, pod *unstructured.Unstructured) bool {
	top := e.TopOwner(pod)
	switch top.GetKind() {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job", "CronJob", "Pod":
		return len(top.GetOwnerReferences()) > 0 // owned by something kubed doesn't follow: a custom resource
	}
	return true
}

func namespaceOwner(apps []*App, ns string) *App {
	if len(apps) == 1 {
		return apps[0]
	}
	for _, a := range apps {
		if a.Name == ns {
			return a
		}
	}
	return nil
}

// wantedReplicas is a workload's spec.replicas; unset is 1.
func wantedReplicas(w *unstructured.Unstructured) int64 {
	if _, set, _ := unstructured.NestedFieldNoCopy(w.Object, "spec", "replicas"); set {
		return num(w, "spec", "replicas")
	}
	return 1
}

// OrDash shows an empty value as "-".
func OrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// jobEnded: a Job that finished, well or not.
func jobEnded(j *unstructured.Unstructured) bool {
	for _, typ := range []string{"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet"} {
		if c, ok := enrich.Condition(j, typ); ok && c.Status == "True" {
			return true
		}
	}
	return false
}

// jobRetrying: a Job with a try that failed in the last few minutes that
// hasn't ended, isn't ending and isn't paused: between tries, or running
// the next one.
func jobRetrying(e *enrich.Enricher, j *unstructured.Unstructured) bool {
	if str(j, "spec", "suspend") == "true" || j.GetDeletionTimestamp() != nil { // paused, or being removed (its pods first)
		return false
	}
	for _, typ := range []string{"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet", "Suspended"} {
		if c, ok := enrich.Condition(j, typ); ok && c.Status == "True" {
			return false
		}
	}
	pods := e.OwnedPods(j) // the index finds them; each read as it is now
	going := len(pods) > 0 // every pod it has is being deleted (a try stopped by hand, or pushed off its node)
	for _, po := range pods {
		if now, ok := e.Get(kube.Pods, po.GetNamespace(), po.GetName()); ok {
			po = now
		}
		going = going && po.GetDeletionTimestamp() != nil
		p, err := ToPod(po)
		if err != nil {
			continue
		}
		if failed, at := tryFailed(p); failed && time.Since(at) < 10*time.Minute {
			return failedTries(e, j) > 0 // Kubernetes waits at most about 6 minutes between tries
		}
	}
	// A try that failed may be gone (deleted, or pushed off its node): the
	// job's own count keeps it. Retrying while the next try runs, or while
	// none exists yet (between tries).
	if len(pods) == 0 && num(j, "status", "active") == 0 || going || liveTries(e, j) > 0 {
		return failedTries(e, j) > 0
	}
	return false
}

// failedAt is when a Job's newest try failed: its pod's end, or its main
// container's while a sidecar still stops (one clock for "failed N ago",
// before and after the job marks it).
func failedAt(e *enrich.Enricher, j *unstructured.Unstructured) time.Time {
	if at := lastTryEnd(e, j); !at.IsZero() {
		return at
	}
	return lastFailedRun(e, j)
}

// lastTryEnd is when a Job's newest failed try's pod ended (its last
// container stopped), the moment the job fails, give or take its count.
func lastTryEnd(e *enrich.Enricher, j *unstructured.Unstructured) time.Time {
	var end time.Time
	for _, po := range e.PodsOf(j) {
		p, err := ToPod(po)
		if err != nil || anyRunning(p) {
			continue
		}
		if failed, at := tryFailed(p); failed {
			for _, cs := range RunningStatuses(p) { // its sidecars stopped after the work
				if t := cs.State.Terminated; t != nil && t.FinishedAt.After(at) {
					at = t.FinishedAt.Time
				}
			}
			if at.After(end) {
				end = at
			}
		}
	}
	return end
}

// anyRunning: a container of the pod runs now (a native sidecar too).
func anyRunning(p *corev1.Pod) bool {
	for _, cs := range RunningStatuses(p) {
		if cs.State.Running != nil {
			return true
		}
	}
	return false
}

// tryFailed: a job's pod whose try failed, and when it ended. Its phase
// says Failed only a moment after its containers have all stopped (and
// after its sidecars are stopped too), so its containers say it first:
// every main container stopped, one with an error.
func tryFailed(p *corev1.Pod) (bool, time.Time) {
	var at time.Time
	if p.Status.Phase == corev1.PodFailed {
		for _, cs := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
			if t := cs.State.Terminated; t != nil && t.FinishedAt.After(at) {
				at = t.FinishedAt.Time
			}
		}
		return true, at
	}
	if p.Spec.RestartPolicy != corev1.RestartPolicyNever || p.DeletionTimestamp != nil || len(p.Status.ContainerStatuses) == 0 {
		return false, at
	}
	failed := false
	for _, cs := range p.Status.ContainerStatuses {
		t := cs.State.Terminated
		if t == nil {
			return false, at // still at work
		}
		failed = failed || t.ExitCode != 0
		if t.FinishedAt.After(at) {
			at = t.FinishedAt.Time
		}
	}
	return failed, at
}

// nodesShort: the scheduler's reason includes nodes too full for it (CPU,
// memory, a GPU, disk, or pod places), not only rules that keep it out.
func nodesShort(why string) bool {
	return strings.Contains(why, "not enough free") || strings.Contains(why, "Insufficient") || strings.Contains(why, "Too many pods")
}

// failedRun is a Job's newest run, when it failed, and the container that
// failed in it (its own containers first, as the Problem line names), for
// the box's Error line; empty when none is kept or the newest run didn't
// fail (it can't start: nothing to quote).
func failedRun(e *enrich.Enricher, j *unstructured.Unstructured) (ns, pod, container string) {
	if j == nil {
		return "", "", ""
	}
	var newest *corev1.Pod
	for _, po := range e.PodsOf(j) {
		if p, err := ToPod(po); err == nil && (newest == nil || p.CreationTimestamp.After(newest.CreationTimestamp.Time)) {
			newest = p
		}
	}
	failed := func(p *corev1.Pod) bool { f, _ := tryFailed(p); return f }             // its phase may not say so yet
	if newest != nil && !failed(newest) && newest.Status.Phase == corev1.PodRunning { // the next try runs: the newest failed one says why
		newest = nil
		for _, po := range e.PodsOf(j) {
			if p, err := ToPod(po); err == nil && failed(p) && (newest == nil || p.CreationTimestamp.After(newest.CreationTimestamp.Time)) {
				newest = p
			}
		}
	}
	if newest == nil || !failed(newest) {
		return "", "", ""
	}
	for _, statuses := range [][]corev1.ContainerStatus{newest.Status.ContainerStatuses, newest.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
				return newest.Namespace, newest.Name, cs.Name
			}
		}
	}
	return "", "", ""
}

// failedTries is how many of a Job's tries failed. The pods are current
// where the Job's own count can be seconds behind (kubed sees the two
// apart), so each failed pod counts, unless a pod failure policy may
// ignore some (a node taken away): then only the ones it counts or is
// still counting (listed as uncounted, or holding its tracking finalizer).
func failedTries(e *enrich.Enricher, j *unstructured.Unstructured) int64 {
	n, _, _ := unstructured.NestedInt64(j.Object, "status", "failed")
	uncounted, _, _ := unstructured.NestedStringSlice(j.Object, "status", "uncountedTerminatedPods", "failed")
	n += int64(len(uncounted))
	_, policy, _ := unstructured.NestedFieldNoCopy(j.Object, "spec", "podFailurePolicy")
	pods := int64(0)
	for _, po := range e.PodsOf(j) {
		p, err := ToPod(po)
		if err != nil {
			continue
		}
		if failed, _ := tryFailed(p); !failed {
			continue
		}
		pods++
		if slices.Contains(po.GetFinalizers(), "batch.kubernetes.io/job-tracking") && !slices.Contains(uncounted, string(po.GetUID())) {
			n++ // still being counted: on top of the tries it counted, some of them gone (deleted, pushed off a node)
		}
	}
	if policy { // it may ignore some failures (a node taken away): only what it counts
		return n
	}
	return max(n, pods)
}

// liveTries is how many of a Job's tries run now, from its pods (its own
// count can be seconds behind): waiting to start or running, and not one
// whose work has already ended (its sidecars stopping).
func liveTries(e *enrich.Enricher, j *unstructured.Unstructured) int64 {
	n := int64(0)
	for _, po := range e.PodsOf(j) {
		p, err := ToPod(po)
		if err != nil || p.DeletionTimestamp != nil || (p.Status.Phase != corev1.PodPending && p.Status.Phase != corev1.PodRunning) {
			continue
		}
		ended := len(p.Status.ContainerStatuses) > 0
		for _, cs := range p.Status.ContainerStatuses {
			ended = ended && cs.State.Terminated != nil
		}
		if !ended {
			n++
		}
	}
	return n
}

// triesUsedUp: a Job's failed tries are past its backoff limit, so it
// fails rather than tries again (unknown when it counts tries per index or
// by a policy).
func triesUsedUp(e *enrich.Enricher, j *unstructured.Unstructured) bool {
	for _, f := range []string{"backoffLimitPerIndex", "podFailurePolicy"} {
		if _, found, _ := unstructured.NestedFieldNoCopy(j.Object, "spec", f); found {
			return false
		}
	}
	limit, found, _ := unstructured.NestedInt64(j.Object, "spec", "backoffLimit")
	if !found {
		limit = 6 // Kubernetes' default
	}
	return failedTries(e, j) > limit
}

// lastFailedRun is when a Job's newest failed run stopped.
func lastFailedRun(e *enrich.Enricher, j *unstructured.Unstructured) time.Time {
	var at time.Time
	for _, po := range e.PodsOf(j) {
		p, err := ToPod(po)
		if err != nil {
			continue
		}
		for _, cs := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
			if t := cs.State.Terminated; t != nil && t.ExitCode != 0 && t.FinishedAt.After(at) {
				at = t.FinishedAt.Time
			}
		}
	}
	return at
}

// runsOnlyJobs: the app runs jobs (or cron jobs) and nothing that keeps
// running (no deployment, stateful set or daemon set).
func runsOnlyJobs(a *App) bool {
	k := managedKinds(a)
	return (k["Job"] || k["CronJob"]) && !k["Deployment"] && !k["StatefulSet"] && !k["DaemonSet"]
}

// imageKey is an image's last name part and tag ("busybox:1.36"), the same
// whether written plainly or rewritten to a mirror's address.
func imageKey(image string) string {
	short := shortImage(image)
	if i := strings.LastIndex(short, "/"); i >= 0 {
		short = short[i+1:]
	}
	if !strings.Contains(short, ":") {
		short += ":latest" // no tag is latest
	}
	return short
}

// staleCronRun: a cron job's run whose pod can't pull an image its cron job
// no longer uses (fixed since). The run never starts; deleting it is the
// fix. A mirror's rewritten address is the same image.
func staleCronRun(e *enrich.Enricher, o *unstructured.Unstructured, p *corev1.Pod) bool {
	top := e.TopOwner(o)
	if top.GetKind() != "CronJob" {
		return false
	}
	now, _, _ := unstructured.NestedSlice(top.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers")
	uses := map[string]bool{}
	for _, c := range now {
		if cm, ok := c.(map[string]any); ok {
			uses[imageKey(fmt.Sprint(cm["image"]))] = true
		}
	}
	for _, cs := range p.Status.ContainerStatuses { // the container that can't pull, not a sidecar added on the way
		stuck := cs.State.Waiting != nil && (cs.State.Waiting.Reason == "ImagePullBackOff" || cs.State.Waiting.Reason == "InvalidImageName" ||
			cs.State.Waiting.Reason == "ErrImagePull" && time.Since(p.CreationTimestamp.Time) > 30*time.Second) // stuck, as CronRunsStuck counts
		if stuck && len(uses) > 0 && !uses[imageKey(cs.Image)] {
			return true
		}
	}
	return false
}

// stoppedWithin: one of the pod's containers stopped (crashed, or was
// killed) within d.
func stoppedWithin(p *corev1.Pod, d time.Duration) bool {
	for _, cs := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
		if t := newestStop(cs); t != nil && time.Since(t.FinishedAt.Time) < d {
			return true
		}
	}
	return false
}
