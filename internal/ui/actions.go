package ui

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

const apiTimeout = 30 * time.Second

func apiCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), apiTimeout)
}

// readTry is how long one try of a read waits: a connection gone stale
// (after a sleep, or one the cluster dropped quietly) hangs rather than
// failing, and a second try on a fresh one usually answers at once.
const readTry = 10 * time.Second

// readYAML reads an object's YAML, trying once more when the first try
// hangs or the connection drops. Only for reads: a change is never sent
// twice.
func (m *Model) readYAML(ref kube.Ref) (string, error) {
	var out string
	var err error
	for try, wait := range []time.Duration{readTry, apiTimeout - readTry} { // the second try waits longer: a slow link, not a stale one
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		out, err = m.client.YAML(ctx, ref)
		cancel()
		if err == nil || !networkError(err) || try == 1 {
			return out, err
		}
	}
	return out, err
}

// networkError: it failed on the way, not because of what was asked.
func networkError(err error) bool {
	low := strings.ToLower(err.Error())
	for _, s := range []string{"context deadline exceeded", "i/o timeout", "unexpected eof", "connection reset", "tls handshake timeout", "http2: client connection lost"} {
		if strings.Contains(low, s) {
			return true
		}
	}
	return false
}

// --- read actions ---

func (m *Model) describe(row view.Row) tea.Cmd {
	ref := m.ref(row)
	m.busy = "describing " + ref.String()
	about := ""
	if m.loc.kind == locResource || m.loc.kind == locContainers {
		if lines := view.For(ref.Resource).Detail; lines != nil && row.Obj != nil {
			about = aboutBlock(lines(m.enr, row.Obj), m.pagerWidth())
		}
	}
	return func() tea.Msg {
		out, err := m.client.Describe(ref)
		return pagerMsg{title: "details of " + refWords(ref), content: about + out, err: err}
	}
}

// aboutBlock puts kubed's own summary (the box under the list, in full)
// above Kubernetes' description.
func aboutBlock(lines []string, width int) string {
	var kept []string
	for _, l := range lines {
		if l != "" { // long lines wrap under their text, as in the box
			kept = append(kept, hangWrapAt(l, width, detailIndent))
		}
	}
	if len(kept) == 0 {
		return ""
	}
	return strings.Join(kept, "\n") + "\n\n" + styleMuted.Render("── full description ──") + "\n\n"
}

// hideSecretValues blanks a Secret's values in YAML meant for reading or
// copying: values are shown only through x, which asks first. kubectl's
// last-applied note is dropped too, since it carries them in plain text.
// Edit keeps the real values: it must write them back.
func hideSecretValues(ref kube.Ref, text string, readOnly bool) string {
	if ref.Resource.Kind != "Secret" {
		return text
	}
	hidden := "hidden (x shows the values)"
	if readOnly { // x is off too: don't point at it
		hidden = "hidden (read-only)"
	}
	var obj map[string]any
	if err := yaml.Unmarshal([]byte(text), &obj); err != nil {
		return "(the values are " + hidden + ")"
	}
	for _, field := range []string{"data", "stringData"} {
		if vals, ok := obj[field].(map[string]any); ok {
			for k := range vals {
				vals[k] = hidden
			}
		}
	}
	if meta, ok := obj["metadata"].(map[string]any); ok {
		if ann, ok := meta["annotations"].(map[string]any); ok {
			delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
			if len(ann) == 0 {
				delete(meta, "annotations")
			}
		}
	}
	out, err := yaml.Marshal(obj)
	if err != nil {
		return "(the values are hidden; x shows them)"
	}
	return string(out)
}

func (m *Model) showYAML(row view.Row) tea.Cmd {
	ref := m.ref(row)
	m.busy = "fetching " + ref.String()
	return func() tea.Msg {
		out, err := m.readYAML(ref)
		out = hideSecretValues(ref, out, m.opts.ReadOnly) // x shows them, after asking
		return pagerMsg{title: "YAML of " + refWords(ref), content: out, err: err, editRow: &row}
	}
}

func (m *Model) decodeSecret(row view.Row) tea.Cmd {
	ref := m.ref(row)
	m.busy = "fetching " + ref.String()
	return func() tea.Msg {
		ctx, cancel := apiCtx()
		defer cancel()
		obj, err := m.client.Get(ctx, ref)
		if err != nil {
			return pagerMsg{err: err}
		}
		data, _, _ := unstructured.NestedStringMap(obj.Object, "data")
		var b strings.Builder
		for _, k := range slices.Sorted(maps.Keys(data)) {
			v, err := base64.StdEncoding.DecodeString(data[k])
			if err != nil {
				v = []byte("<not base64>")
			}
			fmt.Fprintf(&b, "%s:\n  %s\n\n", k, strings.ReplaceAll(string(v), "\n", "\n  "))
		}
		return pagerMsg{title: "values of " + refWords(ref), content: b.String(), secret: true}
	}
}

// withContainer calls fn with the pod and container to act on. In the
// containers view that is the selected container; on a pod row it asks when
// the pod has more than one.
func (m *Model) withContainer(row view.Row, fn func(pod *unstructured.Unstructured, container string) tea.Cmd) tea.Cmd {
	defer func() { m.pickRestarted, m.pickShell = false, false }()
	if m.loc.kind == locContainers {
		pod, ok := m.containersPod()
		if !ok {
			m.setFlash("", fmt.Errorf("pod %s is gone", m.loc.podName))
			return nil
		}
		return fn(pod, row.Obj.GetName())
	}
	pod := row.Obj
	containers := view.Containers(pod)
	switch len(containers) {
	case 0:
		m.setFlash("", fmt.Errorf("pod has no containers"))
		return nil
	case 1:
		return fn(pod, containers[0])
	}
	// The container that restarted most comes first when you asked for the
	// logs from before a restart; init containers are marked.
	restarts := view.ContainerRestarts(pod)
	init := view.InitContainers(pod)
	sidecar := view.HelperContainers(pod)
	restartedFirst, forShell := m.pickRestarted, m.pickShell
	m.pickRestarted, m.pickShell = false, false
	if forShell { // a shell needs a running container: offer only those, setup ones never
		running := view.RunningContainers(pod)
		kept := containers[:0]
		for _, c := range containers {
			if running[c] && (!init[c] || sidecar[c]) { // a helper that keeps running takes a shell too
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			m.setFlash("", fmt.Errorf("no container in %s is running, so there is nowhere to open a shell. p shows the logs from before it stopped", pod.GetName()))
			return nil
		}
		containers = kept
		if len(containers) == 1 {
			return fn(pod, containers[0])
		}
	}
	if restartedFirst { // only containers with a run that ended have logs from before
		kept := containers[:0:0]
		for _, c := range containers {
			if earlierRun(pod, c) {
				kept = append(kept, c)
			}
		}
		if len(kept) > 0 {
			containers = kept
		}
		if len(containers) == 1 {
			return fn(pod, containers[0])
		}
	}
	// The app's own container first: helpers (proxies, log shippers) last;
	// for logs from before a restart, the one that restarted most first.
	sort.SliceStable(containers, func(i, j int) bool {
		if restartedFirst && restarts[containers[i]] != restarts[containers[j]] {
			return restarts[containers[i]] > restarts[containers[j]]
		}
		if init[containers[i]] != init[containers[j]] {
			return !init[containers[i]] // setup containers last
		}
		return !isHelper(containers[i]) && isHelper(containers[j])
	})
	main := 0 // the app's own containers: not setup steps, not helpers
	for _, c := range containers {
		if !init[c] && !isHelper(c) {
			main++
		}
	}
	items := make([]menuItem, 0, len(containers))
	for _, c := range containers {
		about := "" // one of several: nothing to tell them apart by
		if main == 1 && !init[c] && !isHelper(c) {
			about = "The app's own container"
		}
		switch {
		case sidecar[c]:
			about = "A helper that runs alongside the app"
		case init[c]:
			about = "Runs before the others start"
		case restarts[c] > 0:
			about = "Restarted " + view.Plural(int(restarts[c]), "time")
		}
		label := c
		switch {
		case sidecar[c]:
			label += " (helper)"
		case init[c]:
			label += " (runs first)"
		}
		items = append(items, menuItem{label: label, about: about, run: func() tea.Cmd { return fn(pod, c) }})
	}
	m.prompt = menuPrompt("Which container of "+pod.GetName()+"?", items)
	m.mode = modePrompt
	return nil
}

func isHelper(container string) bool { return view.IsHelper(container) }

// logSource says which log stream a pager shows, so it can be reopened
// (e.g. with timestamps on). A workload's logs list several pods.
type logSource struct {
	ns, pod, container   string
	previous, timestamps bool
	// stoppedRun: the current logs of a container stopped right now (waiting
	// to restart), which are the run that ended: shown as such, l goes live.
	stoppedRun bool
	since      time.Duration // read from this long ago; 0 = the last lines
	allPods    int           // a workload: how many pods it has (streams may follow fewer)

	workload string      // "deployment/web" when showing several pods
	streams  [][2]string // [pod, container] for a workload
	// pods, for a workload, lists its pods now: r reads those, and a new
	// one (a replacement, a rollout) is noticed while following.
	pods func() (streams [][2]string, all int)
}

// oneStream: the pod when these logs are one pod's one container (a pod's,
// or a workload's with a single stream), with opts set for it.
func (m *Model) oneStream(src logSource, opts *kube.LogOptions) (string, bool) {
	pod := src.pod
	if src.workload != "" {
		if len(src.streams) != 1 {
			return "", false
		}
		pod, opts.Container = src.streams[0][0], src.streams[0][1]
	}
	if p, ok := m.client.Cache.Get(kube.Pods, src.ns, pod); ok {
		opts.Restarted = view.ContainerRestarts(p)[opts.Container] > 0 // it ran before: its earlier run's logs may be gone, and waiting isn't "not started yet"
	}
	return pod, true
}

// readFrom says where logs read this way start: kubed reads the last
// lines (or from a time with h), not a container's whole history.
func (src logSource) readFrom() string {
	switch {
	case src.previous:
		return "the last 500 lines of the run before the restart"
	case src.since > 0 && src.workload != "" && len(src.streams) > 1:
		// several pods share a size limit that far back
		return "from " + strings.TrimSuffix(view.Age(time.Now().Add(-src.since)), " ago") + " ago on, as much as fits in " + view.Mem(kube.SinceLimit)
	case src.since > 0:
		return "from " + strings.TrimSuffix(view.Age(time.Now().Add(-src.since)), " ago") + " ago on"
	case src.workload != "" && len(src.streams) > 1:
		return "the last 100 lines of each container, and newer"
	}
	return "the last 500 lines, and newer"
}

// heldWords says which part of the logs the screen holds: where kubed
// started reading, and only the newest lines once older ones were left out.
func (p *pager) heldWords() string {
	if p.dropped {
		return fmt.Sprintf("%s; only the newest %d lines are kept", p.source.readFrom(), maxLogLines)
	}
	return p.source.readFrom()
}

// keepReading carries your search into logs read again (h, r): they
// open at the end, following, and n / N find your matches again.
func keepReading(old, p *pager) {
	p.search = old.search
}

func (m *Model) logs(row view.Row, previous bool) tea.Cmd { return m.logsWith(row, previous, nil) }

// logsWith is logs, with the pod as just read from the cluster (nil: not
// read yet), so it isn't asked for twice.
func (m *Model) logsWith(row view.Row, previous bool, fresh *unstructured.Unstructured) tea.Cmd {
	m.pickRestarted = previous
	return m.withContainer(row, func(pod *unstructured.Unstructured, container string) tea.Cmd {
		src := logSource{ns: pod.GetNamespace(), pod: pod.GetName(), container: container}
		switch {
		case previous && fresh != nil && fresh.GetNamespace() == src.ns && fresh.GetName() == src.pod:
			src = endedRunOf(src, fresh)
		case previous:
			// Asked fresh, as p in logs does: a list's copy can be seconds old.
			// Stopped right now: its current logs are the run that ended.
			src = m.endedRun(src)
		}
		return m.openLogs(src)
	})
}

// endedRun points src at a container's run that ended: the one before
// the last restart, or, while the container is stopped (waiting to be
// restarted after a crash), its current logs, which are that run.
func (m *Model) endedRun(src logSource) logSource {
	src.previous = true
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return src
	}
	pod, found := m.freshPod(gvr, src.ns, src.pod)
	if !found {
		return src
	}
	return endedRunOf(src, pod)
}

// endedRunOf is endedRun with the pod already read.
func endedRunOf(src logSource, pod *unstructured.Unstructured) logSource {
	src.previous = true
	if stoppedRunNow(pod, src.container) {
		src.previous, src.stoppedRun = false, true
	}
	return src
}

// freshPod is the pod as the cluster has it now, else as listed: a
// container that died a second ago is still running in a list a few
// seconds old.
func (m *Model) freshPod(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if pod, err := m.client.Dynamic.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return pod, true
	}
	return m.client.Cache.Get(gvr, ns, name)
}

// podGoneErr says a pod no longer exists, so p has no earlier logs to
// read (nil: it exists, or kubed can't tell).
func (m *Model) podGoneErr(ns, name string) error {
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := m.client.Dynamic.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return fmt.Errorf("pod %s no longer exists, so there are no earlier logs to read", name)
	}
	return nil
}

// mayBeGone: src reads one pod's live logs, whose end may be the pod's.
func (m *Model) mayBeGone(src logSource) bool {
	return src.workload == "" && !src.previous && m.client.Dynamic != nil
}

// podDeleted: the one pod src reads the live logs of no longer exists.
func (m *Model) podDeleted(src logSource) bool {
	return m.mayBeGone(src) && m.podGoneErr(src.ns, src.pod) != nil
}

// podGoneMsg says whether the pod of the logs on ch was deleted; held
// are the stream's last lines about a lost connection.
type podGoneMsg struct {
	ch   chan string
	held []string
	gone bool
}

// checkPodGone asks the cluster whether the open logs' pod is gone (or
// being deleted).
func (m *Model) checkPodGone(ch chan string, held []string) tea.Cmd {
	src, dyn := m.pager.source, m.client.Dynamic
	gvr, ok := m.client.Resources.Served(kube.Pods)
	return func() tea.Msg {
		if !ok {
			return podGoneMsg{ch: ch, held: held}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		pod, err := dyn.Resource(gvr).Namespace(src.ns).Get(ctx, src.pod, metav1.GetOptions{})
		deleting := err == nil && pod.GetDeletionTimestamp() != nil // killed at the end of its grace, it is still there
		return podGoneMsg{ch: ch, held: held, gone: apierrors.IsNotFound(err) || deleting}
	}
}

// maxLogPods bounds how many pods a workload's logs follow at once.
const maxLogPods = 25

// maxLogStreams bounds the streams (one per container) read at once.
const maxLogStreams = 75

// workloadLogs follows the logs of every pod a workload owns (its selector),
// each line prefixed with the pod (and container when there are several).
// neverStarted: a run whose pods never started its program has no logs;
// say so, as the app does, instead of showing an empty stream.
func (m *Model) neverStarted(job *unstructured.Unstructured) bool {
	if m.jobNeverRan(job) {
		m.flashNeverRan(job.GetName())
		return true
	}
	return false
}

// jobNeverRan: a Job whose cause says its program never ran, so it has
// no logs; neverStarted without the flash, for a menu line.
func (m *Model) jobNeverRan(job *unstructured.Unstructured) bool {
	if job == nil || job.GetKind() != "Job" {
		return false
	}
	cause, ok := view.JobCause(m.enr, job)
	return ok && view.NeverRan(cause) // a missing command, or an image it never pulled (a failed setup step ran: it has logs)
}

func (m *Model) workloadLogs(row view.Row) tea.Cmd {
	if m.neverStarted(row.Obj) {
		return nil
	}
	v := view.For(m.loc.res)
	d := v.Drill(row.Obj)
	if d == nil {
		m.setFlash("", fmt.Errorf("%s has no pod selector", row.Obj.GetName()))
		return nil
	}
	gvr, _ := m.client.Resources.Served(kube.Pods)
	ns := row.Obj.GetNamespace()
	podsNow := func() ([][2]string, int) {
		var streams [][2]string
		all := 0
		pods := m.client.Cache.List(gvr, ns)
		stopping := true // only pods on their way out (a restart of each in turn): read those
		for _, p := range pods {
			if d.Match(p) && p.GetDeletionTimestamp() == nil {
				stopping = false
			}
		}
		for _, p := range pods {
			if !d.Match(p) || (p.GetDeletionTimestamp() != nil && !stopping) {
				continue
			}
			all++
			if podCount(streams) >= maxLogPods || len(streams) >= maxLogStreams { // pods, not containers: several per pod is usual
				continue
			}
			for _, c := range view.Containers(p) {
				streams = append(streams, [2]string{p.GetName(), c})
			}
		}
		return streams, all
	}
	streams, _ := podsNow()
	if len(streams) == 0 {
		m.setFlash("", podsGoneWhy(row.Obj))
		return nil
	}
	return m.openLogs(logSource{ns: ns, streams: streams, pods: podsNow,
		workload: strings.ToLower(row.Obj.GetKind()) + "/" + row.Obj.GetName()})
}

// podsGoneWhy says why a workload has no pod to read logs from.
func podsGoneWhy(o *unstructured.Unstructured) error {
	if done, _, _ := unstructured.NestedString(o.Object, "status", "completionTime"); done != "" {
		return fmt.Errorf("%s finished and its pods were cleaned up, so its logs are gone", o.GetName())
	}
	if c, ok := enrich.Condition(o, "Failed"); ok && c.Status == "True" {
		return fmt.Errorf("%s failed and its pods are gone, so its logs are too (E shows its events)", o.GetName())
	}
	return fmt.Errorf("no pod of %s exists right now, so there are no logs to show", o.GetName()) // scaled to 0, not started yet, never made
}

// noLogPods says why l on a workload's row has no logs to show, as
// workloadLogs refuses (nil: it has pods).
func (m *Model) noLogPods(o *unstructured.Unstructured) error {
	if m.jobNeverRan(o) { // as l says first
		return fmt.Errorf("%s never started its program, so it has no logs", o.GetName())
	}
	d := view.For(m.loc.res).Drill(o)
	if d == nil {
		return fmt.Errorf("%s has no pod selector", o.GetName())
	}
	gvr, _ := m.client.Resources.Served(kube.Pods)
	for _, p := range m.client.Cache.List(gvr, o.GetNamespace()) {
		if d.Match(p) {
			return nil
		}
	}
	return podsGoneWhy(o)
}

func (m *Model) openLogs(src logSource) tea.Cmd {
	title := fmt.Sprintf("logs of pod %s · %s container · in %s", src.pod, src.container, src.ns)
	if src.workload != "" {
		title = fmt.Sprintf("logs of %s in %s (%s)", strings.Replace(src.workload, "/", " ", 1), src.ns, podsWords(podCount(src.streams), src.allPods, len(src.streams) >= maxLogStreams))
	}
	if src.previous {
		title += " · before its last restart"
	}
	if src.since > 0 {
		title += " · from " + view.Age(time.Now().Add(-src.since)) + " ago"
	}
	p := newPager(title, "", m.pagerWidth(), m.pagerHeight())
	p.openedAt = time.Now()
	p.short = "logs of " + src.pod
	if src.workload != "" {
		p.short = "logs of " + strings.Replace(src.workload, "/", " ", 1)
		if n := podCount(src.streams); src.allPods > n { // narrow screens keep that some pods aren't followed
			p.short += fmt.Sprintf(" (%d of %d pods)", n, src.allPods)
		}
	}
	if src.previous {
		p.short += " · before restart"
	}
	if src.since > 0 {
		p.short += " · last " + view.Age(time.Now().Add(-src.since))
	}
	if old := m.pager; old != nil && old.isLogs { // t, h reopen the stream: keep how you were reading it
		p.raw, p.wrap, p.errorsOnly = old.raw, old.wrap, old.errorsOnly
	} else if m.width < 100 { // narrow: unwrapped lines are cut before their message
		p.wrap = true
	}
	// Logs from before a restart open at the end, where it stopped.
	p.lines, p.isLogs, p.follow, p.source = nil, true, true, src
	if old := m.pager; old != nil && old.isLogs && m.keepPlace { // t, h, r: the same logs read again
		keepReading(old, p)
	}
	m.keepPlace = false
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.logs = make(chan string, 1000)
	m.openPager(p)
	// Always with Kubernetes' timestamps: t only shows or hides them, so
	// the logs aren't read again and you stay where you are.
	opts := kube.LogOptions{Container: src.container, Previous: src.previous, Follow: !src.previous, TailLines: 500, Timestamps: true, Since: src.since}
	if pod, ok := m.oneStream(src, &opts); ok { // one pod, one container: no need to say on every line which
		go m.client.StreamLogs(ctx, src.ns, pod, opts, p.logs)
		return waitLogs(p.logs)
	}
	opts.TailLines = 100
	if src.since > 0 && len(src.streams) > 0 { // many pods far back: share one budget
		opts.LimitBytes = max(kube.SinceLimit/int64(len(src.streams)), 1<<20)
	}
	p.tagNames = map[string]string{}
	for i, t := range logTags(src.streams) {
		p.tagNames[t] = src.streams[i][0] + "/" + src.streams[i][1]
	}
	go m.mergeLogs(ctx, src, opts, p.logs)
	return waitLogs(p.logs)
}

// lost: the stream ended because the connection dropped, not the pod.
func lost(lines []string) bool {
	for _, l := range lines {
		plain := ansi.Strip(l)
		if e := logTagEnd(plain); e > 0 && e <= len(plain) {
			plain = plain[e:] // after a merged line's pod tag
		}
		if strings.HasPrefix(plain, "kubed:") && (strings.Contains(l, "lost the connection") || strings.Contains(l, "connection lost") || strings.Contains(l, "connection reset") ||
			strings.Contains(l, "EOF") || strings.Contains(l, "timeout")) {
			return true
		}
	}
	return false
}

// mergeLogs streams every [pod, container] of a workload into out, each line
// prefixed with a coloured tag, and closes out when all streams end.
func (m *Model) mergeLogs(ctx context.Context, src logSource, opts kube.LogOptions, out chan<- string) {
	tags := logTags(src.streams)
	width := 0
	for _, t := range tags {
		width = max(width, len([]rune(t)))
	}
	type stamped struct {
		at   time.Time
		line string
	}
	lines := make(chan stamped, 1000)
	var wg sync.WaitGroup
	for i, s := range src.streams {
		// Same width for every tag, each pod in its own colour, then a bar.
		tag := logTagStyle(i).Render(padRight(tags[i], width)) + styleMuted.Render(" │")
		in := make(chan string, 100)
		o := opts
		o.Container = s[1]
		o.Timestamps = true // to put the pods' lines in time order
		go m.client.StreamLogs(ctx, src.ns, s[0], o, in)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range in {
				at := time.Now()
				if ts, _, ok := strings.Cut(line, " "); ok {
					if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
						at = t // kept on the line: t shows or hides it
					}
				}
				select {
				case lines <- stamped{at, tag + " " + line}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(lines) }()

	// Each pod's lines arrive in a block. Gather them for a moment (longer
	// at first, when every pod sends its backlog), then send them in time
	// order, so the merged view tells what happened when.
	var batch []stamped
	flush := func() bool {
		sort.SliceStable(batch, func(i, j int) bool { return batch[i].at.Before(batch[j].at) })
		for _, l := range batch {
			select {
			case out <- l.line:
			case <-ctx.Done():
				return false
			}
		}
		batch = batch[:0]
		return true
	}
	wait := 1500 * time.Millisecond
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				flush()
				close(out)
				return
			}
			batch = append(batch, l)
		case <-timer.C:
			if !flush() {
				close(out)
				return
			}
			timer.Reset(300 * time.Millisecond)
		case <-ctx.Done():
			close(out)
			return
		}
	}
}

// logTags names each [pod, container] stream as briefly as tells them
// apart: the end of the pod name ("55wcn"), with the workload when the
// pods come from several, and the container when a pod has several.
func logTags(streams [][2]string) []string {
	perPod := map[string]int{}
	bases := map[string]bool{}
	for _, s := range streams {
		perPod[s[0]]++
		bases[workloadOf(s[0])] = true // every pod counts, DaemonSet and job pods too
	}
	// Workloads of one app share a start ("agent-data-gateway-"): drop it,
	// so the tag keeps the part that tells them apart ("tableau·9jhlq").
	shared := ""
	if len(bases) > 1 {
		var names []string
		for b := range bases {
			names = append(names, b)
		}
		shared = commonDashPrefix(names)
	}
	tags := make([]string, len(streams))
	for i, s := range streams {
		tag := podTag(s[0])
		if m := podName.FindStringSubmatch(s[0]); m != nil && shared != "" && len(m[1]) > len(shared) && m[3] != "" {
			tag = podTag(strings.TrimPrefix(m[1], shared) + "-" + m[2] + m[3])
		}
		if m := podName.FindStringSubmatch(s[0]); m != nil && len(bases) == 1 && m[3] != "" {
			tag = strings.TrimPrefix(m[3], "-") // one workload: the title already names it
		} else if m == nil && shared != "" && strings.HasPrefix(s[0], shared) {
			tag = podTag(strings.TrimPrefix(s[0], shared)) // a DaemonSet's or a job's pod
		}
		if perPod[s[0]] > 1 {
			// The end tells containers apart ("container-1" … "-6"): a
			// long name is cut in the middle, with the room the tag has.
			c, room := []rune(s[1]), max(10, 30-len([]rune(tag))-1)
			if len(c) > room {
				tail := room / 2
				c = append(append(c[:room-1-tail:room-1-tail], '…'), c[len(c)-tail:]...)
			}
			tag += "/" + string(c)
		}
		tags[i] = tag
	}
	return tags
}

// workloadOf is the workload part of a pod's name: "web" of
// "web-5b76dd6597-fkd44", "ebs-csi-node" of "ebs-csi-node-76v62".
func workloadOf(pod string) string {
	if m := podName.FindStringSubmatch(pod); m != nil {
		return m[1]
	}
	if i := strings.LastIndex(pod, "-"); i > 0 {
		return pod[:i]
	}
	return pod
}

// commonDashPrefix is the start several names share, up to a dash
// ("agent-data-gateway-" of "agent-data-gateway-tableau" and
// "agent-data-gateway-server").
func commonDashPrefix(names []string) string {
	if len(names) == 0 {
		return ""
	}
	p := names[0]
	for _, n := range names[1:] {
		for !strings.HasPrefix(n, p) {
			p = p[:len(p)-1]
		}
	}
	if i := strings.LastIndex(p, "-"); i >= 0 {
		return p[:i+1]
	}
	return ""
}

// podTag shortens "web-5b76dd6597-fkd44" to "fkd44" for log prefixes.
// podTag names a pod briefly for merged logs: its workload and the end of
// its name ("celery·nsxxx", "db-0").
func podTag(pod string) string {
	if m := podName.FindStringSubmatch(pod); m != nil && strings.ContainsAny(m[2], "0123456789") && m[3] != "" {
		base := []rune(m[1])
		if len(base) > 18 { // the end tells workloads apart ("…-consumer-carts"), the start is the app
			base = append([]rune{'…'}, base[len(base)-17:]...)
		}
		return string(base) + "·" + strings.TrimPrefix(m[3], "-")
	}
	if r := []rune(pod); len(r) > 24 {
		return string(r[:12]) + "…" + string(r[len(r)-11:])
	}
	return pod
}

// shellStartedMsg carries a new in-panel shell session.
type shellStartedMsg struct {
	title string
	sess  *kube.Session
	err   error
}

// shell opens a shell inside kubed's body box.
func (m *Model) shell(row view.Row) tea.Cmd {
	m.pickShell = true
	return m.withContainer(row, func(pod *unstructured.Unstructured, container string) tea.Cmd {
		ns, name := pod.GetNamespace(), pod.GetName()
		title := fmt.Sprintf("shell in pod %s · %s container · in %s", name, container, ns)
		m.busy = "connecting to pod " + name
		cols, rows := m.pagerWidth(), m.pagerHeight()
		return func() tea.Msg {
			sess, err := m.client.StartSession(ns, name, container, cols, rows)
			return shellStartedMsg{title: title, sess: sess, err: err}
		}
	})
}

// fullScreenShell hands the whole terminal to the shell (for vim, htop…).
func (m *Model) fullScreenShell(row view.Row) tea.Cmd {
	m.pickShell = true // only running containers can take a shell
	return m.withContainer(row, func(pod *unstructured.Unstructured, container string) tea.Cmd {
		sh := m.client.NewShell(pod.GetNamespace(), pod.GetName(), container)
		return tea.Exec(sh, func(err error) tea.Msg {
			if err != nil {
				if strings.Contains(err.Error(), "command terminated with exit code") {
					// The last command's code, passed on by exit: you closed it.
					return resultMsg{text: "shell closed", shellEnd: true}
				}
				return resultMsg{err: fmt.Errorf("shell: %w", err), shellEnd: true}
			}
			return resultMsg{text: "shell closed", shellEnd: true}
		})
	})
}

func (m *Model) portForward(row view.Row) tea.Cmd {
	target := row.Obj
	if m.loc.kind == locContainers {
		p, ok := m.containersPod()
		if !ok {
			return nil
		}
		target = p
	}
	ns, pod := target.GetNamespace(), target.GetName()
	if phase, _, _ := unstructured.NestedString(target.Object, "status", "phase"); phase != "Running" || target.GetDeletionTimestamp() != nil {
		// Nothing listens in a pod that isn't running: say so before asking for ports.
		why := fmt.Sprintf("pod %s isn't running, so there is nothing to reach", pod)
		if ref := metav1.GetControllerOf(target); ref != nil && ref.Kind != "Job" { // a job's pod has no service to go through
			why += ". F on its deployment or service follows whichever pod is running"
		}
		m.setFlash("", errors.New(why))
		return nil
	}
	suggestion := ""
	if ports := view.ContainerPorts(target); len(ports) > 0 {
		suggestion = fmt.Sprintf("%d:%d", suggestLocalPort(int(ports[0])), ports[0])
	}
	m.prompt = inputPrompt(fmt.Sprintf("Reach a port of %s on a local port (localhost).\nType <local port>:<port in the pod>, e.g. 8080:80", pod), suggestion, func(v string) tea.Cmd {
		local, remote, _ := parsePorts(v)
		client := m.client // this cluster's, even if you switch before it starts
		return func() tea.Msg {
			f, err := m.forwards.Start(client, ns, pod, local, remote)
			if err != nil && strings.Contains(err.Error(), "not found") {
				return resultMsg{err: fmt.Errorf("pod %s is gone (replaced a moment ago?). F on its deployment or service follows whichever pod is running", pod)}
			}
			if err != nil {
				return resultMsg{err: fmt.Errorf("port-forward: %w", err)}
			}
			return resultMsg{text: f.Short() + " · :pf to see or stop it"}
		}
	})
	m.prompt.validate = func(v string) error {
		local, _, err := parsePorts(v)
		if err == nil {
			err = m.portMoving(local)
		}
		return err
	}
	m.mode = modePrompt
	return nil
}

// suggestLocalPort is the local port to suggest for a port in the cluster: the
// same number, but local ports under 1024 need admin rights, so
// 80 becomes 8080 and 443 becomes 8443.
func suggestLocalPort(p int) int {
	if p < 1024 {
		return p + 8000
	}
	return p
}

func parsePorts(v string) (local, remote int, err error) {
	l, r, found := strings.Cut(v, ":")
	if !found {
		r = l
	}
	local, err1 := strconv.Atoi(strings.TrimSpace(l))
	remote, err2 := strconv.Atoi(strings.TrimSpace(r))
	if err1 != nil || err2 != nil || local <= 0 || remote <= 0 || local > 65535 || remote > 65535 {
		return 0, 0, fmt.Errorf("type two port numbers from 1 to 65535, like 8080:80")
	}
	if local < 1024 && os.Geteuid() != 0 {
		return 0, 0, fmt.Errorf("local ports under 1024 need admin rights. Use %d:%d instead", suggestLocalPort(local), remote)
	}
	return local, remote, nil
}

// stopForwards closes the ports of the rows (each row is named by its
// local port, so picks stay right as others close).
func (m *Model) stopForwards(rows []view.Row) tea.Cmd {
	var stopped []string
	for _, row := range rows {
		port, _ := strconv.Atoi(row.Obj.GetName())
		for _, f := range m.forwards.List() {
			if f.LocalPort == port {
				f.Again = nil // you stopped it: don't start it again elsewhere
				m.forwards.Stop(f)
				stopped = append(stopped, fmt.Sprintf("localhost:%d", port))
			}
		}
		if f := m.moving[port]; f != nil { // finding another pod: it stops looking, and what it found is stopped
			f.Dropped.Store(true)
			delete(m.moving, port)
			if m.stopping == nil {
				m.stopping = map[int]*kube.Forward{}
			}
			m.stopping[port] = f
			stopped = append(stopped, fmt.Sprintf("localhost:%d", port))
		}
		held := m.endedHeld[:0:0] // ended a moment ago: you stopped it, so it isn't started again
		for _, e := range m.endedHeld {
			if e.f.LocalPort == port {
				stopped = append(stopped, fmt.Sprintf("localhost:%d", port))
				continue
			}
			held = append(held, e)
		}
		m.endedHeld = held
	}
	cleared := 0
	for _, row := range rows { // a stopped one's line: cleared
		port, _ := strconv.Atoi(row.Obj.GetName())
		kept := m.stoppedForwards[:0:0]
		for _, st := range m.stoppedForwards {
			if st.port == port {
				cleared++
				continue
			}
			kept = append(kept, st)
		}
		m.stoppedForwards = kept
	}
	switch {
	case len(stopped) > 0:
		m.setFlash("stopped "+strings.Join(slices.Compact(stopped), ", "), nil)
	case cleared > 0:
		m.setFlash(fmt.Sprintf("cleared %s", view.Plural(cleared, "stopped port")), nil)
	}
	m.tbl.clearMarks()
	m.refreshRows()
	return nil
}

// --- write actions ---

func (m *Model) scale(row view.Row) tea.Cmd {
	ref := m.ref(row)
	if hpa, ok := m.client.Resources.Served(kube.HPAs); ok {
		m.client.Cache.Watch(hpa) // so the confirm can warn about an autoscaler
	}
	current, _, _ := unstructured.NestedInt64(row.Obj.Object, "spec", "replicas")
	m.prompt = inputPrompt(fmt.Sprintf("How many copies of %s should run? (now %d)", refWords(ref), current), fmt.Sprint(current), func(v string) tea.Cmd {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > maxReplicas { // validReplicas already refused it; never pass it on
			return nil
		}
		if int64(n) == current { // nothing to change, nothing to confirm
			m.setFlash(fmt.Sprintf("%s already runs %d; nothing changed", refWords(ref), n), nil)
			return nil
		}
		question := fmt.Sprintf("Scale %s from %d to %d?", refWords(ref), current, n)
		switch {
		case n >= 20 && current == 0:
			question += fmt.Sprintf("\nThat starts %d copies at once. Make sure the cluster has room for them.", n)
		case n >= 20 && int64(n) > 5*current:
			question += fmt.Sprintf("\nThat's %dx as many copies as now. Make sure the cluster has room for them.", int64(n)/current)
		}
		if n == 0 {
			question += "\nWith 0 copies the app stops until you scale it up again."
		}
		m.confirm(question, row, enrich.ActionScale, func() tea.Cmd {
			return func() tea.Msg {
				ctx, cancel := apiCtx()
				defer cancel()
				if err := m.client.Scale(ctx, ref, int32(n)); err != nil {
					return resultMsg{err: err}
				}
				return resultMsg{text: fmt.Sprintf("scaled %s to %d", refWords(ref), n)}
			}
		})
		return nil
	})
	m.prompt.validate = validReplicas
	m.prompt.warnings = m.enr.Warnings(row.Obj, enrich.ActionScale) // ArgoCD, an autoscaler: said up front
	if hpa, ok := m.client.Resources.Served(kube.HPAs); ok && !m.client.Cache.Synced(hpa) {
		// Not read yet: don't let silence read as "no autoscaler".
		m.prompt.warnings = append(m.prompt.warnings, "still checking whether an autoscaler manages it (one would undo this)")
	}
	m.mode = modePrompt
	return nil
}

// maxReplicas guards against a slip of the finger (an extra zero).
const maxReplicas = 1000

// validReplicas accepts a whole number from 0 to maxReplicas.
func validReplicas(v string) error {
	n, err := strconv.Atoi(v)
	switch {
	case err != nil || n < 0:
		return fmt.Errorf("type a whole number, 0 or more")
	case n > maxReplicas:
		return fmt.Errorf("that's more than %d copies; type a smaller number (or use kubectl if you really mean it)", maxReplicas)
	}
	return nil
}

// refWords names an object the way people say it: "deployment web in payments".
func refWords(r kube.Ref) string {
	kind := strings.ToLower(r.Resource.Kind)
	if kind == "" {
		kind = strings.TrimSuffix(r.Resource.GVR.Resource, "s")
	}
	if r.Namespace == "" {
		return kind + " " + r.Name
	}
	return kind + " " + r.Name + " in " + r.Namespace
}

// Edit: fetch YAML → $EDITOR → confirm (with warnings) → dry-run → update.

type editReadyMsg struct {
	row  view.Row
	ref  kube.Ref
	path string
	orig []byte
	err  error
	// sentBack: the file as kubed wrote it when sending you back to fix
	// it. Left as it was (quit without saving): cancelled.
	sentBack []byte
}

type editDoneMsg struct {
	editReadyMsg
}

func (m *Model) edit(row view.Row) tea.Cmd {
	if m.ref(row).Resource.Kind == "Secret" {
		// Its values open in the editor (and kubectl's last-applied note
		// may hold them in plain text): asked first, as x does.
		q := "Edit secret " + row.Obj.GetName() + "? Its values show in your editor (some in plain text)."
		m.prompt = confirmPrompt(q, nil, "", func() tea.Cmd { return m.editNow(row) })
		m.mode = modePrompt
		return nil
	}
	return m.editNow(row)
}

// editNow fetches the YAML and opens the editor.
func (m *Model) editNow(row view.Row) tea.Cmd {
	ref := m.ref(row)
	warnings := m.enr.Warnings(row.Obj, enrich.ActionEdit)
	m.busy = "fetching " + ref.String()
	return func() tea.Msg {
		out, err := m.readYAML(ref)
		if err != nil {
			return editReadyMsg{err: err}
		}
		out = withoutStatus(out, ref.Resource.Kind)
		// Say up front, in the file, what will undo an edit.
		var head strings.Builder
		for _, w := range warnings {
			head.WriteString("# " + w + "\n")
		}
		if head.Len() > 0 {
			out = head.String() + "#\n" + out
		}
		path, err := writeTemp(out)
		if err != nil {
			return editReadyMsg{err: err}
		}
		return editReadyMsg{row: row, ref: ref, path: path, orig: []byte(out)}
	}
}

func writeTemp(content string) (string, error) {
	f, err := os.CreateTemp("", "kubed-edit-*.yaml")
	if err != nil {
		return "", err
	}
	_, werr := f.WriteString(content)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(f.Name())
		return "", werr
	}
	return f.Name(), nil
}

func (m *Model) onEditReady(msg editReadyMsg) tea.Cmd {
	if msg.err != nil {
		m.setFlash("", msg.err)
		return nil
	}
	editor := cmp.Or(os.Getenv("KUBE_EDITOR"), os.Getenv("EDITOR"), "vi")
	args := append(strings.Fields(editor), msg.path)
	cmd := exec.Command(args[0], args[1:]...)
	return tea.ExecProcess(cmd, func(err error) tea.Msg {
		msg.err = err
		return editDoneMsg{msg}
	})
}

// yamlLine is the line number in a YAML parse error ("line 3").
var yamlLine = regexp.MustCompile(`line \d+`)

// yamlProblem is a YAML parse error with its line number as the file you
// get back shows it: below the notes put above it, and on the bad line
// itself (the parser names the line before it when a key or a '-' is
// missing, and the line after it when a ':' is).
func yamlProblem(msg string, notes int) string {
	msg = strings.ReplaceAll(msg, "\n", " ")
	shift := notes
	switch {
	case strings.Contains(msg, "did not find expected key"), strings.Contains(msg, "did not find expected '-' indicator"):
		shift++
	case strings.Contains(msg, "could not find expected ':'"):
		shift--
	}
	return yamlLine.ReplaceAllStringFunc(msg, func(s string) string {
		n, _ := strconv.Atoi(strings.TrimPrefix(s, "line "))
		return fmt.Sprintf("around line %d", max(n+shift, 1))
	})
}

// editNote starts the file when kubed sends you back to fix an edit.
const editNote = "# kubed: "

func (m *Model) onEditDone(msg editDoneMsg) tea.Cmd {
	edited, err := os.ReadFile(msg.path)
	if msg.err != nil {
		os.Remove(msg.path)
		m.setFlash("", fmt.Errorf("editor: %w", msg.err))
		return nil
	}
	if err != nil {
		os.Remove(msg.path)
		m.setFlash("", err)
		return nil
	}
	if msg.sentBack != nil && bytes.Equal(edited, msg.sentBack) {
		os.Remove(msg.path)
		m.setFlash("edit cancelled: you left without saving, so nothing changed", nil)
		return nil
	}
	edited = dropEditNotes(edited)
	switch {
	case len(bytes.TrimSpace(edited)) == 0:
		os.Remove(msg.path)
		m.setFlash("edit cancelled", nil)
		return nil
	case bytes.Equal(bytes.TrimSpace(edited), bytes.TrimSpace(msg.orig)):
		os.Remove(msg.path)
		m.setFlash("no changes", nil)
		return nil
	}
	// Broken YAML: back to the editor with the problem written on top,
	// like kubectl edit, instead of losing what you typed.
	if _, err := yaml.YAMLToJSON(edited); err != nil {
		why := yamlProblem(err.Error(), 2)
		note := editNote + "this isn't valid YAML: " + why + "\n" +
			editNote + "fix it and save, or quit without saving (or delete everything) to cancel.\n"
		back := append([]byte(note), edited...)
		if werr := os.WriteFile(msg.path, back, 0o600); werr != nil {
			os.Remove(msg.path)
			m.setFlash("", werr)
			return nil
		}
		again := msg.editReadyMsg
		again.sentBack = back
		return m.onEditReady(again)
	}
	os.Remove(msg.path)
	ref := msg.ref
	question := "Apply your edit to " + refWords(ref) + "?\n" + strings.Join(firstLines(contextDiff(string(msg.orig), string(edited), 2), maxDiffLines), "\n") // with lines around, to show where
	// What makes the pod, read here, not while the edit is sent.
	podFix := ""
	if msg.row.Obj != nil && ref.Resource.GVR == kube.Pods {
		podFix = m.podEditFix(msg.row.Obj)
	}
	m.confirm(question, msg.row, enrich.ActionEdit, func() tea.Cmd {
		m.busy = "applying your edit to " + refWords(ref) // a slow cluster can take a while
		return func() tea.Msg {
			ctx, cancel := apiCtx()
			defer cancel()
			if err := m.client.Update(ctx, ref, msg.orig, edited); err != nil {
				if strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "timeout") {
					if strings.HasPrefix(err.Error(), "dry run") { // the check itself: nothing was sent
						return resultMsg{err: fmt.Errorf("the cluster didn't answer in time, so nothing changed. Try e again")}
					}
					// The change may have landed before the answer was lost.
					return resultMsg{err: fmt.Errorf("the cluster didn't answer in time: your edit may or may not have been applied. y shows what %s holds now; check before trying e again", refWords(ref))}
				}
				if !strings.HasPrefix(err.Error(), "dry run") || strings.Contains(err.Error(), "the object has been modified") {
					return resultMsg{err: err}
				}
				// The cluster refused it (a bad value): back to the editor
				// with why written on top, so your edit isn't lost.
				why := plainEditError(err.Error())
				if strings.HasPrefix(why, podSpecRefused) && podFix != "" {
					why = podSpecRefused + "; " + podFix
				}
				note := editNote + "the cluster refused this: " + why + "\n" +
					editNote + "fix it and save, or quit without saving (or delete everything) to cancel.\n"
				path, werr := writeTemp(note + string(edited))
				if werr != nil {
					return resultMsg{err: err}
				}
				again := msg.editReadyMsg
				again.path, again.sentBack = path, []byte(note+string(edited))
				return again
			}
			return resultMsg{text: "updated " + refWords(ref)}
		}
	})
	return nil
}

// dropEditNotes removes kubed's notes from the top of an edited file.
func dropEditNotes(b []byte) []byte {
	for bytes.HasPrefix(b, []byte(editNote)) {
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			b = b[i+1:]
		} else {
			return nil
		}
	}
	return b
}

// maxDiffLines bounds the diff shown before applying an edit.
const maxDiffLines = 14

// savePager writes the pager's text to ~/Downloads (or the home directory
// when there is no Downloads folder).
func (m *Model) savePager() {
	// Read again a moment ago (t, h, r…) and nothing in yet: a file now
	// would hold only its header.
	switch {
	case m.pager.isLogs && !m.pager.gotLines && !m.pager.ended && time.Since(m.pager.openedAt) < 3*time.Second:
		m.setFlash("", fmt.Errorf("still reading the logs; ctrl-s again in a moment"))
		return
	case m.pager.isLogs && len(m.pager.logLines(m.pager.lines)) == 0:
		m.setFlash("", fmt.Errorf("nothing to save: no lines logged yet"))
		return
	}
	ext := "log"
	if m.pager.csv {
		ext = "csv"
	}
	text := m.pager.plainText()
	if m.pager.isLogs { // a file outlives the screen: say what it holds, for a ticket
		filter := "every line kubed read: " + m.pager.heldWords()
		if m.pager.errorsOnly {
			filter = "errors only, of what kubed read: " + m.pager.heldWords()
		}
		text = fmt.Sprintf("# kubed: %s\n# cluster %s · saved %s · %s\n\n", m.pager.title, shortContext(m.context),
			time.Now().Format("2 Jan 2006 15:04 MST"), filter) + text
	}
	m.saveFile(m.pager.title, ext, []byte(text))
}

// saveList writes the list on screen (as filtered and sorted) as CSV, to
// open in a spreadsheet or share.
func (m *Model) saveList() {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(append(append([]string(nil), m.tbl.header...), "CLUSTER")) // a file outlives the screen: say where it came from
	rows := m.tbl.rows
	if picked := m.tbl.marked(); len(picked) > 0 { // rows you picked, or all of them
		rows = picked
	}
	timeCol := -1 // the list's own time (DEPLOYED, AGE): the one it knows exactly
	for i, h := range m.tbl.header {
		switch strings.TrimRight(h, "↑↓") {
		case "DEPLOYED", "AGE", "AGO": // what the list's own time means (not LAST RUN and the like)
			if timeCol < 0 {
				timeCol = i
			}
		}
	}
	for _, r := range rows {
		cells := make([]string, len(r.Cells))
		for i, c := range r.Cells {
			cells[i] = strings.TrimPrefix(ansi.Strip(c), markPrefix)
			// "37s ago" is wrong by the time someone reads it: write the
			// row's real time, or none, never one guessed from the words.
			if when, ok := restartedAgo(cells[i]); ok { // "7 (3m ago)" goes stale in a file
				cells[i] = when
			}
			if i == timeCol && m.tbl.timeOf != nil { // "37s ago", or "8m" in AGE columns
				if sec := m.tbl.timeOf(r); sec > 0 {
					at := time.Unix(sec, 0).Local().Format("2006-01-02 15:04 MST")
					switch { // a bound stays a bound in the file
					case strings.HasPrefix(cells[i], "≤"):
						at = "at or after " + at
					case strings.Contains(cells[i], "+ ago"):
						at = "at or before " + at
					}
					cells[i] = at
				}
			}
		}
		_ = w.Write(append(cells, shortContext(m.context)))
	}
	w.Flush()
	// Named for the list and where, not the whole title (trail, counts,
	// filter): "kubed-pods-in-shop-<time>.csv".
	m.saveFile(locName(m.loc)+" in "+nsWords(m.namespace), "csv", buf.Bytes())
}

// restartedAgo rewrites a RESTARTS cell ("7 (3m ago)") with the real time
// of the last restart, about to the minute: "7 (last 2026-10-12 13:41 IST)".
func restartedAgo(cell string) (string, bool) {
	count, rest, ok := strings.Cut(cell, " (")
	if !ok || !strings.HasSuffix(rest, " ago)") {
		return "", false
	}
	if _, err := strconv.Atoi(count); err != nil {
		return "", false
	}
	at, ok := agoToTime(strings.TrimSuffix(rest, ")"))
	if !ok {
		return "", false
	}
	return count + " (last " + at.Local().Format("2006-01-02 15:04 MST") + ")", true
}

// agoToTime reads "3h2m ago" (as Age writes it) back into a time.
func agoToTime(cell string) (time.Time, bool) {
	spanText, ok := strings.CutSuffix(strings.TrimSpace(cell), " ago")
	if !ok || spanText == "" {
		return time.Time{}, false
	}
	units := map[byte]time.Duration{'y': 365 * 24 * time.Hour, 'd': 24 * time.Hour, 'h': time.Hour, 'm': time.Minute, 's': time.Second}
	var total time.Duration
	n := 0
	for i := 0; i < len(spanText); i++ {
		c := spanText[i]
		switch {
		case c >= '0' && c <= '9':
			n = n*10 + int(c-'0')
		case units[c] > 0:
			total += time.Duration(n) * units[c]
			n = 0
		default:
			return time.Time{}, false
		}
	}
	return time.Now().Add(-total), total > 0
}

// saveFile writes to your Downloads folder (or home) and says where.
func (m *Model) saveFile(title, ext string, data []byte) {
	dir, err := os.UserHomeDir()
	if err != nil {
		m.setFlash("", err)
		return
	}
	if d := filepath.Join(dir, "Downloads"); isDir(d) {
		dir = d
	}
	// "kubed-logs-of-pod-web-1-in-shop": words and dashes only.
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(title) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			dash = false
		} else if !dash && b.Len() > 0 {
			b.WriteByte('-')
			dash = true
		}
	}
	name := strings.Trim(b.String(), "-")
	if len(name) > 100 { // long enough for "logs of app x in namespace y"
		name = strings.Trim(name[:100], "-")
	}
	name = "kubed-" + name
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.%s", name, time.Now().Format("20060102-150405"), ext))
	if err := os.WriteFile(path, data, 0o600); err != nil { // only you can read it: logs and secret values can be private
		m.setFlash("", err)
		return
	}
	m.setFlash("saved to "+filepath.Base(filepath.Dir(path))+": "+filepath.Base(path), nil) // one line: the folder and the file
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// podsWords is "3 pods", or "25 of 31 pods" when only some are followed.
func podsWords(followed, all int, streamsFull bool) string {
	switch {
	case all > followed && streamsFull:
		return fmt.Sprintf("%d of %d pods: the first %d, %d containers at most", followed, all, followed, maxLogStreams)
	case all > followed && followed >= maxLogPods:
		return fmt.Sprintf("%d of %d pods: the first %d", followed, all, maxLogPods)
	case all > followed: // say why, or it reads as pods left out by mistake
		return fmt.Sprintf("%d of %d pods: the others have finished or aren't running", followed, all)
	}
	return view.Plural(followed, "pod")
}

// podCount is how many pods a set of [pod, container] streams covers.
func podCount(streams [][2]string) int {
	seen := map[string]bool{}
	for _, s := range streams {
		seen[s[0]] = true
	}
	return len(seen)
}

// statusIgnored are kinds whose status the API keeps apart: an edit can't
// change it, so the edit file leaves it out (it only made diffs noisy).
var statusIgnored = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "ReplicaSet": true,
	"Job": true, "CronJob": true, "Pod": true, "Service": true, "PersistentVolumeClaim": true, "HorizontalPodAutoscaler": true}

func withoutStatus(doc, kind string) string {
	if !statusIgnored[kind] {
		return doc
	}
	var obj map[string]any
	if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
		return doc
	}
	delete(obj, "status")
	out, err := yaml.Marshal(obj)
	if err != nil {
		return doc
	}
	return string(out)
}

// typeErr is Go's wording for a value of the wrong type in an edit.
var typeErr = regexp.MustCompile(`cannot unmarshal (\w+) into Go struct field \S*?\.(\S+) of type (\w+)`)

// plainEditError says why the cluster refused an edit in words: "replicas
// must be a whole number, not text".
func plainEditError(msg string) string {
	msg = strings.ReplaceAll(msg, "\n", " ")
	if words, ok := immutableWords(msg); ok {
		return words
	}
	if m := typeErr.FindStringSubmatch(msg); m != nil {
		want := map[string]string{"int32": "a whole number", "int64": "a whole number", "int": "a whole number",
			"bool": "true or false", "string": "text"}[m[3]]
		if want == "" {
			want = m[3]
		}
		got := map[string]string{"string": "text", "number": "a number", "bool": "true or false", "object": "a block", "array": "a list"}[m[1]]
		if got == "" {
			got = m[1]
		}
		return fmt.Sprintf("%s must be %s, not %s", m[2], want, got)
	}
	if m := unknownField.FindAllStringSubmatch(msg, -1); m != nil {
		var names []string
		for _, f := range m {
			names = append(names, f[1])
		}
		return strings.Join(names, ", ") + ": not a field Kubernetes knows here (a typo?). Nothing was changed"
	}
	return strings.TrimPrefix(msg, "dry run: ")
}

// invalidField reads the field of each "<field>: Invalid value: <its
// value>" in a refusal, the value often a whole spec dumped as Go values.
var invalidField = regexp.MustCompile(`(?:^|[\s\[,])([A-Za-z][\w.\[\]-]*): Invalid value: `)

// podSpecRefused starts the words for a refused pod edit; podEditFix
// says what to do instead.
const podSpecRefused = "a pod's spec can't be changed once it runs, except its images and a few fields"

// podEditFix: where a pod's refused edit goes instead: what makes it,
// from its owners, or, for a pod nothing makes, making it again.
func (m *Model) podEditFix(pod *unstructured.Unstructured) string {
	top := pod
	if m.enr != nil {
		top = m.enr.TopOwner(pod)
	}
	kind := ""
	if top.GetUID() != pod.GetUID() || top.GetName() != pod.GetName() {
		kind = top.GetKind()
	}
	if ref := metav1.GetControllerOf(top); ref != nil { // its owner isn't loaded: the owner it names
		kind = ref.Kind
	}
	words := map[string]string{"Deployment": "deployment", "StatefulSet": "stateful set", "DaemonSet": "daemon set",
		"ReplicaSet": "replica set", "Job": "job", "CronJob": "cron job"}[kind]
	switch {
	case kind == "":
		return "create it again with the change (delete it, then apply the new spec)"
	case words == "":
		words = strings.ToLower(kind)
	}
	return "change what makes it (its " + words + ") instead, or delete the pod"
}

// immutableWords says a refusal of a change to a field that can't change
// once made ("field is immutable") in one line, naming the field and
// never its value; ok is false for any other error.
func immutableWords(msg string) (string, bool) {
	if strings.Contains(msg, "pod updates may not change fields other than") { // the API's dump follows, a diff and all
		return podSpecRefused + "; change what makes it instead, or delete the pod", true // which owner isn't known here
	}
	if !strings.Contains(msg, "field is immutable") {
		return "", false
	}
	locs := invalidField.FindAllStringSubmatchIndex(msg, -1)
	var fields []string
	for i, l := range locs {
		end := len(msg)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		if f := msg[l[2]:l[3]]; strings.Contains(msg[l[1]:end], "field is immutable") && !slices.Contains(fields, f) {
			fields = append(fields, f)
		}
	}
	job := strings.Contains(msg, `Job.batch "`)
	switch {
	case job && slices.Contains(fields, "spec.template"):
		return "a job's pod template can't be changed once it exists; create the job again (or edit its cron job)", true
	case len(fields) == 0:
		return "a field you changed can't be changed once it exists. Nothing was changed", true
	case job:
		return strings.Join(fields, ", ") + " can't be changed once the job exists; create the job again (or edit its cron job)", true
	}
	return strings.Join(fields, ", ") + " can't be changed once it exists. Nothing was changed", true
}

// unknownField reads strict validation's refusals: `unknown field "spec.3eplicas"`.
var unknownField = regexp.MustCompile(`unknown field "([^"]+)"`)

// contextDiff is a unified diff of two texts, old to new: changed lines in
// red (-, the old) and green (+, the new), with a few unchanged lines
// around each change so you can tell which env var or container it is.
func contextDiff(old, cur string, context int) []string {
	x, y := strings.Split(old, "\n"), strings.Split(cur, "\n")
	lcs := make([][]int, len(x)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(y)+1)
	}
	for i := len(x) - 1; i >= 0; i-- {
		for j := len(y) - 1; j >= 0; j-- {
			if x[i] == y[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	type op struct {
		kind byte // ' ', '-', '+'
		text string
	}
	var ops []op
	i, j := 0, 0
	for i < len(x) || j < len(y) {
		switch {
		case i < len(x) && j < len(y) && x[i] == y[j]:
			ops = append(ops, op{' ', x[i]})
			i, j = i+1, j+1
		case i < len(x) && (j == len(y) || lcs[i+1][j] >= lcs[i][j+1]):
			ops = append(ops, op{'-', x[i]})
			i++
		default:
			ops = append(ops, op{'+', y[j]})
			j++
		}
	}
	show := make([]bool, len(ops))
	for k, o := range ops {
		if o.kind != ' ' {
			for d := max(k-context, 0); d <= min(k+context, len(ops)-1); d++ {
				show[d] = true
			}
		}
	}
	var out []string
	gap := false
	for k, o := range ops {
		if !show[k] {
			gap = true
			continue
		}
		if gap && len(out) > 0 {
			out = append(out, styleMuted.Render("  ⋯"))
		}
		gap = false
		line := strings.TrimRight(o.text, " ")
		switch o.kind {
		case '-':
			out = append(out, styleErr.Render("- "+line))
		case '+':
			out = append(out, styleOK.Render("+ "+line))
		default:
			out = append(out, styleMuted.Render("  "+line))
		}
	}
	return out
}

// firstLines keeps at most n lines, saying how many more there were.
func firstLines(lines []string, n int) []string {
	if len(lines) <= n {
		return lines
	}
	return append(lines[:n:n], fmt.Sprintf("… and %d more lines", len(lines)-n))
}
