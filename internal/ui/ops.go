package ui

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// --- bulk writes: delete, restart, kill act on marked rows ---

// confirmRows asks once for a write on one or more rows. Warnings are the
// union for all rows. Production contexts make you type the name (one
// row) or the number of rows.
func (m *Model) confirmRows(verb, why string, rows []view.Row, action enrich.Action, run func() tea.Cmd) {
	seen := map[string]bool{}
	var warnings []string
	for _, r := range rows {
		if m.enr == nil || r.Obj == nil {
			continue
		}
		for _, w := range m.enr.Warnings(r.Obj, action) {
			if !seen[w] {
				seen[w] = true
				warnings = append(warnings, w)
			}
		}
	}
	title := fmt.Sprintf("%s %s?", verb, refWords(m.ref(rows[0])))
	if len(rows) > 1 {
		title = fmt.Sprintf("%s these %d %s?\n%s", verb, len(rows), m.loc.res.GVR.Resource, listNames(rows))
	}
	if why != "" {
		title += "\n" + why
	}
	m.confirmTitled(title, rows, warnings, run)
}

// confirmTitled shows the confirm prompt. Production contexts make you type
// the name (one row) or the number of rows instead of pressing y.
func (m *Model) confirmTitled(title string, rows []view.Row, warnings []string, run func() tea.Cmd) {
	typed := ""
	if m.typedConfirm() {
		typed = rows[0].Obj.GetName()
		if rows[0].Obj.GetKind() == "Node" {
			typed = view.ShortNode(typed) // the name as the list shows it
		}
		if len(rows) > 1 {
			// Several rows: the action and how many ("delete 3"), so a
			// stray number can't confirm it.
			typed = strings.ToLower(strings.Fields(title)[0]) + " " + fmt.Sprint(len(rows))
		}
	}
	m.prompt = confirmPrompt(title, warnings, typed, run)
	m.mode = modePrompt
}

// confirmNamed is confirmTitled for a change to one named thing made of
// several (an app): production asks for that name, like everywhere else.
func (m *Model) confirmNamed(title, name string, warnings []string, run func() tea.Cmd) {
	typed := ""
	if m.typedConfirm() {
		typed = name
	}
	m.prompt = confirmPrompt(title, warnings, typed, run)
	m.mode = modePrompt
}

func listNames(rows []view.Row) string {
	const show = 8
	var names []string
	for i, r := range rows {
		if i == show {
			names = append(names, fmt.Sprintf("  … and %d more", len(rows)-show))
			break
		}
		names = append(names, "  "+strings.TrimPrefix(r.Cells[0], markPrefix))
	}
	return strings.Join(names, "\n")
}

// eachRow runs op for every row and reports "<verb> 3 of 3" or the errors.
func (m *Model) eachRow(verb string, rows []view.Row, op func(ctx context.Context, ref kube.Ref) error) tea.Cmd {
	refs := make([]kube.Ref, len(rows))
	for i, r := range rows {
		refs[i] = m.ref(r)
	}
	m.tbl.clearMarks()
	return func() tea.Msg {
		var failed []string
		for _, ref := range refs {
			ctx, cancel := apiCtx()
			if err := op(ctx, ref); err != nil {
				failed = append(failed, ref.Name+": "+err.Error())
			}
			cancel()
		}
		if len(failed) > 0 {
			return resultMsg{err: fmt.Errorf("%s %d of %d; failed: %s", verb, len(refs)-len(failed), len(refs), strings.Join(failed, "; "))}
		}
		if len(refs) == 1 {
			return resultMsg{text: verb + " " + refWords(refs[0])}
		}
		return resultMsg{text: fmt.Sprintf("%s %d %s", verb, len(refs), refs[0].Resource.GVR.Resource)}
	}
}

// goneAfterDrop: a delete whose answer the network lost went through when
// a fresh look finds it gone. Still there (or no answer again): the error.
func goneAfterDrop(c *kube.Client, op func(context.Context, kube.Ref) error) func(context.Context, kube.Ref) error {
	return func(ctx context.Context, ref kube.Ref) error {
		err := op(ctx, ref)
		if err == nil || !lostConnection(strings.ToLower(err.Error())) {
			return err
		}
		look, cancel := context.WithTimeout(context.Background(), readTry)
		defer cancel()
		if _, gerr := c.Get(look, ref); apierrors.IsNotFound(gerr) {
			return nil
		}
		return err
	}
}

func (m *Model) deleteRows(rows []view.Row) tea.Cmd {
	why := "It is removed from the cluster; whatever manages it may create it again."
	phase, _, _ := unstructured.NestedString(rows[0].Obj.Object, "status", "phase")
	if len(rows) == 1 && rows[0].Obj.GetKind() == "Pod" && (phase == "Succeeded" || phase == "Failed") {
		why = "It has already stopped; this removes it and its logs."
	} else if len(rows) == 1 && rows[0].Obj.GetKind() == "Pod" {
		switch top := m.enr.TopOwner(rows[0].Obj); {
		case top.GetKind() == "Pod":
			why = "It gets time to finish its work, then is removed. Nothing runs it, so it is gone for good."
		case m.enr.Controller(rows[0].Obj) != nil && m.enr.Controller(rows[0].Obj).GetKind() == "Job":
			why = jobPodDeleteWords(m.enr.Controller(rows[0].Obj))
		default:
			why = "It gets time to finish its work, then is removed; " + strings.ToLower(top.GetKind()) + " " + top.GetName() + " starts a new one."

		}
	}
	pods := " Its pods stop too."
	if len(rows) > 1 {
		why, pods = "They are removed from the cluster; whatever manages them may create them again.", " Their pods stop too."
		if rows[0].Obj.GetKind() == "Pod" { // pods of one workload: name it, as for one pod
			top := m.enr.TopOwner(rows[0].Obj)
			same := top.GetKind() != "Pod"
			for _, r := range rows[1:] {
				if t := m.enr.TopOwner(r.Obj); t.GetKind() != top.GetKind() || t.GetName() != top.GetName() {
					same = false
				}
			}
			finished := true // all already stopped: nothing to finish, nothing replaces them
			for _, r := range rows {
				if ph, _, _ := unstructured.NestedString(r.Obj.Object, "status", "phase"); ph != "Succeeded" && ph != "Failed" {
					finished = false
				}
			}
			switch {
			case finished:
				why = "They have already stopped; this removes them and their logs."
			case same:
				why = "They get time to finish their work, then are removed; " + strings.ToLower(top.GetKind()) + " " + top.GetName() + " starts new ones."
			}
		}
	}
	jobsDone := rows[0].Obj.GetKind() == "Job" // finished runs: nothing runs, nothing makes them again
	for _, r := range rows {
		if c, ok := enrich.Condition(r.Obj, "Complete"); !ok || c.Status != "True" {
			if c, ok := enrich.Condition(r.Obj, "Failed"); !ok || c.Status != "True" {
				jobsDone = false
			}
		}
	}
	switch kind := rows[0].Obj.GetKind(); {
	case jobsDone && len(rows) == 1:
		why = "It has already finished; this removes it, its pods and their logs."
	case jobsDone:
		why = "They have already finished; this removes them, their pods and their logs."
	case kind == "Deployment", kind == "StatefulSet", kind == "DaemonSet", kind == "ReplicaSet", kind == "Job", kind == "CronJob":
		why += pods
	}
	why += m.lastCopiesWarning(rows)
	m.confirmRows("Delete", why, rows, enrich.ActionDelete, func() tea.Cmd { return m.eachRow("deleted", rows, goneAfterDrop(m.client, m.client.Delete)) })
	return nil
}

// restartOwner rollout-restarts what runs a pod.
func (m *Model) restartOwner(row view.Row) tea.Cmd {
	pod := m.podRowIfContainer(row).Obj
	top := m.enr.TopOwner(pod)
	switch top.GetKind() {
	case "Deployment", "StatefulSet", "DaemonSet":
	default:
		m.setFlash("", fmt.Errorf("nothing runs %s (no deployment, statefulset or daemonset), so there's nothing to restart", pod.GetName()))
		return nil
	}
	res, ok := m.client.Resources.ByKind("apps", top.GetKind())
	if !ok {
		return nil
	}
	ownerRow := view.Row{Obj: top, Cells: []string{top.GetName()}}
	ref := kube.RefOf(res, top)
	m.confirmTitled(fmt.Sprintf("Restart %s?\n%s", refWords(ref), restartWhy(top)), []view.Row{ownerRow},
		m.enr.Warnings(top, enrich.ActionRestart), func() tea.Cmd {
			return func() tea.Msg {
				ctx, cancel := apiCtx()
				defer cancel()
				if err := m.client.RolloutRestart(ctx, ref); err != nil {
					return resultMsg{err: err}
				}
				if pausedDeploy(top) {
					return resultMsg{text: "restart set for " + refWords(ref) + ": it happens once it is resumed"}
				}
				return resultMsg{text: "restarted " + refWords(ref)}
			}
		})
	return nil
}

// restartRowsWhy is what r's confirm says a restart of rows does.
func restartRowsWhy(rows []view.Row) string {
	if len(rows) == 1 {
		return restartWhy(rows[0].Obj)
	}
	// The one-at-a-time lead-in is only about the rows that do that; the
	// rest each get a line of their own.
	var plain, paused, own []string
	for _, r := range rows {
		switch {
		case ownLine(r.Obj): // its line says it is paused too
			own = append(own, "For "+r.Obj.GetName()+": "+restartWhy(r.Obj))
		case pausedDeploy(r.Obj):
			paused = append(paused, r.Obj.GetName())
		default:
			plain = append(plain, r.Obj.GetName())
		}
	}
	var lines []string
	switch {
	case len(plain) == len(rows):
		lines = append(lines, "New pods replace the old ones one pod at a time.")
	case len(plain) > 3:
		lines = append(lines, fmt.Sprintf("For the other %d: new pods replace the old ones one pod at a time.", len(plain)))
	case len(plain) > 0:
		lines = append(lines, "For "+strings.Join(plain, ", ")+": new pods replace the old ones one pod at a time.")
	}
	lines = append(lines, own...)
	if len(paused) > 0 {
		lines = append(lines, "Paused, so not until resumed: "+strings.Join(paused, ", ")+".")
	}
	return strings.Join(lines, "\n")
}

// replacesNow: a restart of o replaces its pods without anything more:
// not OnDelete (only pods you delete), not held back whole by a
// partition, not a paused deployment (only once resumed).
func replacesNow(o *unstructured.Unstructured) bool {
	key := "updateStrategy"
	if o.GetKind() == "Deployment" {
		key = "strategy"
	}
	strategy, _, _ := unstructured.NestedString(o.Object, "spec", key, "type")
	if strategy == "OnDelete" && (o.GetKind() == "StatefulSet" || o.GetKind() == "DaemonSet") {
		return false
	}
	if o.GetKind() == "StatefulSet" {
		copies, found, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")
		if !found {
			copies = 1
		}
		if partition, _, _ := unstructured.NestedInt64(o.Object, "spec", key, "rollingUpdate", "partition"); partition > 0 && partition >= copies {
			return false
		}
	}
	return !pausedDeploy(o)
}

func (m *Model) restartRows(rows []view.Row) tea.Cmd {
	var paused []string
	for _, r := range rows {
		if pausedDeploy(r.Obj) {
			paused = append(paused, r.Obj.GetName())
		}
	}
	m.confirmRows("Restart", restartRowsWhy(rows), rows, enrich.ActionRestart, func() tea.Cmd {
		cmd := m.eachRow("restarted", rows, m.client.RolloutRestart)
		if len(paused) == 0 || cmd == nil {
			return cmd
		}
		return func() tea.Msg { // paused ones restart only once resumed: say so, not "restarted"
			msg := cmd()
			r, ok := msg.(resultMsg)
			if !ok || r.err != nil {
				return msg
			}
			r.text = pausedRestartText(r.text, paused, len(rows), refWords(m.ref(rows[0])))
			return r
		}
	})
	return nil
}

// goneForGood: a pod nothing starts again once it is stopped: a bare pod
// (no controller), or a finished job's.
func goneForGood(pod *unstructured.Unstructured) bool {
	phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
	owner := metav1.GetControllerOf(pod)
	return owner == nil || owner.Kind == "Job" && (phase == "Succeeded" || phase == "Failed")
}

// podEnded: a pod whose containers have all finished (Succeeded, Failed).
func podEnded(pod *unstructured.Unstructured) bool {
	phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
	return phase == "Succeeded" || phase == "Failed"
}

func (m *Model) killRows(rows []view.Row) tea.Cmd {
	// Which come back: a pod run by a ReplicaSet, a stateful set or a
	// running job is replaced; a bare pod, or a finished job's, is not.
	var gone []string
	replacer, jobPods := "", 0
	for _, r := range rows {
		if c := m.enr.Controller(r.Obj); c != nil && c.GetKind() == "Job" && !goneForGood(r.Obj) {
			jobPods++ // its job starts another only while it has tries left
		}
		switch {
		case goneForGood(r.Obj):
			gone = append(gone, r.Obj.GetName())
		case replacer == "":
			if top := m.enr.TopOwner(r.Obj); top.GetKind() != "Pod" {
				replacer = strings.ToLower(top.GetKind()) + " " + top.GetName()
			}
		}
	}
	// A pod that has ended (Succeeded, Failed) has no work left to cut short.
	ended := 0
	for _, r := range rows {
		if podEnded(r.Obj) {
			ended++
		}
	}
	why := "It stops right away, without time to finish its work. "
	switch {
	case len(rows) == 1 && ended == 1:
		why = "It has already ended; this removes it at once. "
	case ended == len(rows):
		why = "They have already ended; this removes them at once. "
	case ended > 0:
		why = "They stop right away; the ones still running get no time to finish their work. "
	case len(rows) > 1:
		why = "They stop right away, without time to finish their work. "
	}
	switch {
	case ended == len(rows) && len(gone) == 0:
		// Whether its owner starts another for an ended pod depends on the
		// owner (most already have): no claim either way.
	case len(rows) == 1 && jobPods == 1: // as Delete says it
		why += "Its " + jobNext(m.enr.Controller(rows[0].Obj)) + "."
	case len(gone) == 0 && len(rows) == 1 && replacer != "":
		why += replacer + " starts a new one."
	case jobPods > 0 && len(gone) == 0:
		why += "A job's pod is replaced only while its job has tries left; pods of other workloads are replaced."
	case jobPods > 0:
		why += "Nothing starts " + strings.Join(gone, ", ") + " again (gone for good); a job's pod is replaced only while its job has tries left."
	case len(gone) == 0:
		why += "Whatever runs them starts new ones."
	case len(gone) == len(rows):
		why += "Nothing starts " + strings.Join(gone, ", ") + " again: gone for good."
	default:
		why += "Nothing starts " + strings.Join(gone, ", ") + " again (gone for good); the others are replaced."
	}
	why = strings.TrimSpace(why) + m.lastCopiesWarning(rows)
	m.confirmRows("Stop", why, rows, enrich.ActionDelete, func() tea.Cmd { return m.eachRow("stopped", rows, goneAfterDrop(m.client, m.client.Kill)) })
	return nil
}

// --- nodes ---

// cordonRows cordons the rows, or uncordons them when the first is cordoned.
func (m *Model) cordonRows(rows []view.Row) tea.Cmd {
	unsched, _, _ := unstructured.NestedBool(rows[0].Obj.Object, "spec", "unschedulable")
	verb, why, done, want := "Stop new pods on", "Pods already there keep running (this is called cordoning).", "stopped new pods on", true
	if unsched {
		verb, why, done, want = "Allow new pods on", "New pods can be placed on it again (uncordon).", "allowed new pods on", false
	}
	m.confirmRows(verb, why, rows, enrich.ActionEdit, func() tea.Cmd {
		return m.eachRow(done, rows, func(ctx context.Context, ref kube.Ref) error { return m.client.Cordon(ctx, ref.Name, want) })
	})
	return nil
}

// movablePods counts the pods a drain moves off a node (all but the ones
// a DaemonSet runs on every node), names those nothing runs (a drain
// deletes them for good), and the disruption budgets that hold them now.
func (m *Model) movablePods(node string) (n int, orphans, held []string) {
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return 0, nil, nil
	}
	for _, p := range m.client.Cache.List(gvr, "") {
		if name, _, _ := unstructured.NestedString(p.Object, "spec", "nodeName"); name != node {
			continue
		}
		if phase, _, _ := unstructured.NestedString(p.Object, "status", "phase"); phase == "Succeeded" || phase == "Failed" {
			continue
		}
		if _, mirror := p.GetAnnotations()["kubernetes.io/config.mirror"]; mirror {
			continue // a static pod: the node itself runs it, a drain leaves it (as kube.Evictable)
		}
		if ref := metav1.GetControllerOf(p); ref != nil && ref.Kind == "DaemonSet" {
			continue // runs on every node, stays; known even before DaemonSets load
		}
		switch top := m.enr.TopOwner(p); {
		case top.GetKind() == "DaemonSet":
			continue
		case top == p && len(p.GetOwnerReferences()) == 0:
			orphans = append(orphans, p.GetName())
		}
		for _, b := range m.enr.BudgetsAllowingNone(p) {
			if !slices.Contains(held, b) {
				held = append(held, b)
			}
		}
		n++
	}
	return n, orphans, held
}

// drainTimeout bounds a drain waiting on PodDisruptionBudgets.
const drainTimeout = 10 * time.Minute

type drainMsg struct {
	node     string
	ch       chan drainMsg
	progress kube.DrainProgress
	done     bool
	err      error
}

// drainWhy says what draining a node would do.
func (m *Model) drainWhy(node string) string {
	n, orphans, held := m.movablePods(node)
	moves := fmt.Sprintf("Its %d pods move", n)
	if n == 1 {
		moves = "Its 1 pod moves"
	}
	why := moves + " to other nodes, respecting each app's limit on how many may be down at once. No new pods will be placed on it. (Pods that run on every node stay.)"
	for _, b := range held {
		why += "\n⚠ The drain will wait on budget " + b + ": no pod of it may be taken down now."
	}
	if len(orphans) > 0 {
		why += fmt.Sprintf("\n⚠ Nothing runs %s, so %s deleted for good, not moved.", strings.Join(orphans, ", "),
			map[bool]string{true: "it is", false: "they are"}[len(orphans) == 1])
	}
	return why
}

// drainPreview, in --readonly, says what a drain would do, and does nothing.
func (m *Model) drainPreview(row view.Row) tea.Cmd {
	text := m.drainWhy(row.Obj.GetName()) + "\n\nkubed is read-only, so nothing moves."
	title := "if node " + row.Obj.GetName() + " were drained"
	return func() tea.Msg { return pagerMsg{title: title, content: text, hang: 2} }
}

func (m *Model) drain(row view.Row) tea.Cmd {
	node := row.Obj.GetName()
	why := m.drainWhy(node)
	m.confirmRows("Drain", why, []view.Row{row}, enrich.ActionEdit, func() tea.Cmd {
		ch := make(chan drainMsg, 16)
		m.busy = drainStatus(node, kube.DrainProgress{})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			defer cancel()
			last := kube.DrainProgress{}
			err := m.client.Drain(ctx, node, func(p kube.DrainProgress) {
				last = p
				select {
				case ch <- drainMsg{node: node, ch: ch, progress: p}:
				default:
				}
			})
			ch <- drainMsg{node: node, ch: ch, progress: last, done: true, err: err}
		}()
		return waitDrain(ch)
	})
	return nil
}

func waitDrain(ch chan drainMsg) tea.Cmd { return func() tea.Msg { return <-ch } }

func drainStatus(node string, p kube.DrainProgress) string {
	s := fmt.Sprintf("draining %s: %d of %d pods evicted", node, p.Evicted, p.Total)
	if p.Waiting != "" {
		s += ", waiting on a PodDisruptionBudget for " + p.Waiting
	}
	return s
}

// --- workloads ---

func (m *Model) triggerCronJob(row view.Row) tea.Cmd {
	ns, name := row.Obj.GetNamespace(), row.Obj.GetName()
	m.confirmRows("Run", "Starts a run now, outside its schedule.", []view.Row{row}, enrich.ActionRestart, func() tea.Cmd {
		return func() tea.Msg {
			ctx, cancel := apiCtx()
			defer cancel()
			job, err := m.client.TriggerCronJob(ctx, ns, name)
			if err != nil {
				return resultMsg{err: err}
			}
			return resultMsg{text: "started job " + job + "  (gj to watch)"}
		}
	})
	return nil
}

// heldWords: why a change to o replaces no pod now (OnDelete, a
// partition holding all back), as its confirm says it; "" when it rolls
// out (or waits on a resume, said on its own).
func heldWords(o *unstructured.Unstructured) string {
	if pausedDeploy(o) || o.GetKind() == "CronJob" || replacesNow(o) {
		return ""
	}
	return restartWhy(o)
}

// imageSetWords is what is said once an image is set. A paused
// deployment runs the old one until it is resumed; one that replaces no
// pod now (OnDelete, a partition holding all back) says why (held).
func imageSetWords(ref kube.Ref, image, container string, paused, atOnce bool, held string) string {
	if atOnce {
		return fmt.Sprintf("%s now runs %s in container %s: a version with it already runs all its copies, so it takes over at once. %s", refWords(ref), image, container, stillPaused)
	}
	if paused {
		return fmt.Sprintf("%s is set to run %s in container %s: it rolls out once it is resumed (kubectl rollout resume)", refWords(ref), image, container)
	}
	if held != "" {
		return fmt.Sprintf("%s is set to run %s in container %s, but no pod runs it yet. %s", refWords(ref), image, container, held)
	}
	watch := " (o to watch the rollout)"
	if ref.Resource.Kind != "Deployment" { // o works on deployments only
		watch = ""
	}
	return fmt.Sprintf("%s now runs %s in container %s%s", refWords(ref), image, container, watch)
}

// setImage: pick a container, type the new image, confirm.
func (m *Model) setImage(row view.Row) tea.Cmd {
	ref := m.ref(row)
	containers := templateContainers(row.Obj)
	if len(containers) == 0 {
		m.setFlash("", fmt.Errorf("no containers in %s", ref))
		return nil
	}
	ask := func(c [3]string) {
		m.prompt = inputPrompt(fmt.Sprintf("Which image should container %s of %s run?", c[0], refWords(ref)), c[1], func(image string) tea.Cmd {
			if image == "" || image == c[1] {
				return func() tea.Msg { return resultMsg{text: "image unchanged"} }
			}
			after := "New pods with this image replace the old ones."
			paused := pausedDeploy(row.Obj)
			atOnce := takesAtOnce(row.Obj, m.imageVersion(row.Obj, c[0], image, c[2] != ""))
			held := heldWords(row.Obj)
			switch {
			case atOnce:
				after = "It is paused, but a version with this image already runs all its copies: it takes over at once, and the other version's pods stop. " + stillPaused
			case paused:
				after = pausedWords + " Then new pods with this image replace the old ones."
			case row.Obj.GetKind() == "CronJob":
				after = "Its next run uses this image; a run going now keeps the old one."
			}
			if why := restartWhy(row.Obj); !atOnce && (strings.Contains(why, "down for a moment") || strings.Contains(why, "OnDelete") || strings.Contains(why, "partition")) {
				after += " " + why // one copy (or a node's only pod), all at once, or held back: said as restart says it
			}
			m.confirmRows(fmt.Sprintf("Run image %s in container %s of", image, c[0]), after, []view.Row{row}, enrich.ActionEdit, func() tea.Cmd {
				return func() tea.Msg {
					ctx, cancel := apiCtx()
					defer cancel()
					if err := m.client.SetImage(ctx, ref, c[0], image, c[2] != ""); err != nil { // init and helper both live under initContainers
						return resultMsg{err: err}
					}
					return resultMsg{text: imageSetWords(ref, image, c[0], paused, atOnce, held)}
				}
			})
			return nil
		})
		m.mode = modePrompt
	}
	if len(containers) == 1 {
		ask(containers[0])
		return nil
	}
	rank := func(c [3]string) int {
		switch {
		case c[2] == "init":
			return 2
		case c[2] == "helper", isHelper(c[0]):
			return 1
		}
		return 0
	}
	sort.SliceStable(containers, func(i, j int) bool { return rank(containers[i]) < rank(containers[j]) })
	items := make([]menuItem, 0, len(containers))
	for _, c := range containers {
		about := "Runs " + c[1]
		switch c[2] {
		case "init":
			about = "Runs first, before the app starts; runs " + c[1]
		case "helper":
			about = "A helper that runs alongside the app; runs " + c[1]
		}
		label := c[0]
		switch c[2] {
		case "init":
			label += " (runs first)"
		case "helper":
			label += " (helper)"
		}
		items = append(items, menuItem{label: label, about: about, run: func() tea.Cmd { ask(c); return nil }})
	}
	m.prompt = menuPrompt("Which container's image?", items)
	m.mode = modePrompt
	return nil
}

// templateContainers returns [name, image, "init", "helper" or ""] for each
// container of a workload's pod template, init containers too.
func templateContainers(o *unstructured.Unstructured) [][3]string {
	spec := []string{"spec", "template", "spec"}
	if o.GetKind() == "CronJob" {
		spec = []string{"spec", "jobTemplate", "spec", "template", "spec"}
	}
	var out [][3]string
	for _, list := range []string{"containers", "initContainers"} {
		items, _, _ := unstructured.NestedSlice(o.Object, append(spec, list)...)
		for _, c := range items {
			cm, _ := c.(map[string]any)
			name, _ := cm["name"].(string)
			image, _ := cm["image"].(string)
			kind := ""
			switch {
			case list == "initContainers" && cm["restartPolicy"] == "Always":
				kind = "helper" // an init container that keeps running alongside the app
			case list == "initContainers":
				kind = "init"
			}
			out = append(out, [3]string{name, image, kind})
		}
	}
	return out
}

// --- xray ---

// xrayWait bounds how long an xray waits for its lists to load.
const xrayWait = 15 * time.Second

func (m *Model) xray(row view.Row) tea.Cmd {
	obj := row.Obj
	if m.loc.kind == locContainers {
		p, ok := m.containersPod()
		if !ok {
			return nil
		}
		obj = p
	}
	m.busy = "finding what " + obj.GetName() + " uses"
	c, enr := m.client, m.enr
	var gvrs []kube.Resource
	for _, g := range enrich.XrayNeeds {
		if res, ok := c.Resources.ByGVR(g); ok {
			gvrs = append(gvrs, res)
			c.Cache.Watch(res.GVR)
		}
	}
	return func() tea.Msg {
		deadline := time.Now().Add(xrayWait)
		for _, res := range gvrs {
			for !c.Cache.Synced(res.GVR) && !cantRead(c.Cache.Err(res.GVR)) && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond) // a list kubed can't read won't load: don't wait for it
			}
		}
		tree := enr.Xray(obj)
		// Long lines (AWS roles, secret names) wrap rather than being cut.
		return pagerMsg{title: fmt.Sprintf("what %s %s uses", strings.ToLower(tree.Kind), tree.Name), content: renderXray(tree), tree: true}
	}
}

// cantRead: a list kubed isn't allowed to read (it won't load however
// long you wait), as opposed to a timeout or a busy server.
func cantRead(err error) bool {
	return err != nil && (apierrors.IsForbidden(err) || errors.Is(err, kube.ErrPickNamespace))
}

// renderXray draws the tree with ├─ └─ connectors and ✓ ⚠ ✗ marks.
func renderXray(root *enrich.XNode) string {
	var b strings.Builder
	var walk func(n *enrich.XNode, prefix string, last, top bool)
	walk = func(n *enrich.XNode, prefix string, last, top bool) {
		connector, next := "├─ ", prefix+"│  "
		if last {
			connector, next = "└─ ", prefix+"   "
		}
		if top {
			connector, next = "", ""
		}
		line := prefix + connector + healthMark(n.Health)
		if n.Name == "" {
			line += styleMuted.Render(n.Kind)
		} else {
			line += styleInfoLabel.Render(n.Kind) + " " + styleValue.Render(n.Name)
		}
		if n.Note != "" {
			line += "  " + styleMuted.Render(n.Note)
		}
		b.WriteString(line + "\n")
		for i, c := range n.Children {
			walk(c, next, i == len(n.Children)-1, false)
		}
	}
	walk(root, "", true, true)
	return b.String()
}

func healthMark(h enrich.Health) string {
	switch h {
	case enrich.HealthOK:
		return styleOK.Render("✓ ")
	case enrich.HealthWarn:
		return styleWarn.Render("⚠ ")
	case enrich.HealthBad:
		return styleErr.Render("✗ ")
	}
	return "  "
}

// --- helm (read-only) ---

// helmRefresh is how often the Helm list is re-read while it is open.
const helmRefresh = 30 * time.Second

type helmMsg struct {
	rels []kube.HelmRelease
	err  error
	ns   string // the namespace it was read for
}

func (m *Model) openHelm() tea.Cmd {
	m.jumpTo(location{kind: locHelm})
	return m.fetchHelm()
}

func (m *Model) fetchHelm() tea.Cmd {
	m.helmLoading, m.helmErr = true, nil // an old error isn't the answer for this read
	c, ns := m.client, m.namespace
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 2*apiTimeout)
		defer cancel()
		rels, err := c.HelmReleases(ctx, ns)
		return helmMsg{rels: rels, err: err, ns: ns}
	}
}

func (m *Model) helmRelease(row view.Row) (kube.HelmRelease, bool) {
	for _, r := range m.helm {
		if r.Namespace == row.Obj.GetNamespace() && r.Name == row.Obj.GetName() {
			return r, true
		}
	}
	return kube.HelmRelease{}, false
}

// helmShow opens values (what) = "values" or "manifest" of a release.
func (m *Model) helmShow(row view.Row, what string) tea.Cmd {
	rel, ok := m.helmRelease(row)
	if !ok {
		return nil
	}
	m.busy = "reading helm release " + rel.Name
	return func() tea.Msg {
		ctx, cancel := apiCtx()
		defer cancel()
		d, err := m.client.HelmDetail(ctx, rel)
		if err != nil {
			return pagerMsg{err: err}
		}
		head := fmt.Sprintf("Release:  %s/%s  revision %d\nChart:    %s-%s  (app %s)\nStatus:   %s  %s\n\n",
			rel.Namespace, rel.Name, rel.Revision, d.Chart, d.ChartVersion, d.AppVersion, d.Status, d.Description)
		if what == "manifest" {
			return pagerMsg{title: "helm manifest " + rel.Name, content: head + d.Manifest}
		}
		values := "(no user-supplied values)\n"
		if len(d.Values) > 0 {
			out, _ := yaml.Marshal(d.Values)
			values = string(out)
		}
		return pagerMsg{title: "helm values " + rel.Name, content: head + "User-supplied values:\n\n" + values}
	}
}

func (m *Model) helmHistory(row view.Row) tea.Cmd {
	rel, ok := m.helmRelease(row)
	if !ok {
		return nil
	}
	m.busy = "reading the history of " + rel.Name
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		revs, err := m.client.HelmHistory(ctx, rel.Namespace, rel.Name)
		if err != nil {
			return pagerMsg{err: err}
		}
		var b strings.Builder
		fmt.Fprintf(&b, "%-9s %-10s %-16s %-28s %-12s %s\n", "REVISION", "AGO", "STATUS", "CHART", "APP VERSION", "WHAT HAPPENED")
		for i, r := range revs {
			chart, app, what := "", "", ""
			if i < 10 { // reading a revision means fetching its whole Secret
				if d, err := m.client.HelmDetail(ctx, r); err == nil {
					chart, app, what = d.Chart+"-"+d.ChartVersion, d.AppVersion, d.Description
				}
			}
			fmt.Fprintf(&b, "%-9d %-10s %-16s %-28s %-12s %s\n", r.Revision, view.Age(r.Updated)+" ago", r.Status,
				truncate(chart, 28), truncate(view.OrDash(app), 12), what)
		}
		return pagerMsg{title: "Helm history of " + rel.Name, content: b.String(), hang: 9 + 1 + 10 + 1 + 16 + 1 + 28 + 1 + 12 + 1}
	}
}

// --- RBAC ---

func (m *Model) canI() tea.Cmd {
	ns := cmp.Or(m.namespace, "default")
	m.busy = "checking your permissions in " + ns
	return func() tea.Msg {
		ctx, cancel := apiCtx()
		defer cancel()
		checks, err := m.client.AccessMatrix(ctx, ns)
		if err != nil {
			return pagerMsg{err: err}
		}
		// One line per resource: ✓ get  ✓ list  ✗ delete …
		var order []string
		byRes := map[string][]string{}
		for _, a := range checks {
			if _, ok := byRes[a.Resource]; !ok {
				order = append(order, a.Resource)
			}
			mark := styleErr.Render("✗ " + a.Verb)
			if a.Allowed {
				mark = styleOK.Render("✓ " + a.Verb)
			}
			byRes[a.Resource] = append(byRes[a.Resource], mark)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "What you can do in namespace %q on %s\n\n", ns, shortContext(m.context))
		for _, r := range order {
			fmt.Fprintf(&b, "%-28s %s\n", r, strings.Join(byRes[r], "   "))
		}
		b.WriteString("\nAsked one by one (SelfSubjectAccessReview), so EKS access entries are included.\n")
		return pagerMsg{title: "can-i " + ns, content: b.String()}
	}
}

func (m *Model) serviceAccountRBAC(row view.Row) tea.Cmd {
	ns, sa := row.Obj.GetNamespace(), row.Obj.GetName()
	m.busy = "reading what " + sa + " is allowed to do"
	role := enrich.ServiceAccountRole(row.Obj)
	return func() tea.Msg {
		ctx, cancel := apiCtx()
		defer cancel()
		bindings, err := m.client.ServiceAccountBindings(ctx, ns, sa)
		if err != nil {
			return pagerMsg{err: err}
		}
		var b strings.Builder
		fmt.Fprintf(&b, "ServiceAccount %s/%s\n", ns, sa)
		if role != "" {
			fmt.Fprintf(&b, "AWS role: %s\n", role)
		}
		if len(bindings) == 0 {
			b.WriteString("\nNo RoleBindings or ClusterRoleBindings name this ServiceAccount or a group it is in: it has no Kubernetes permissions.\n")
		}
		for _, bd := range bindings {
			where := ""
			if bd.Namespace != "" && bd.Namespace != ns {
				where = " (in namespace " + bd.Namespace + ")"
			}
			via := ""
			if bd.Via != "" {
				via = "   because it is in group " + bd.Via
			}
			fmt.Fprintf(&b, "\n%s %s%s → %s %s%s\n", bd.Kind, bd.Name, where, bd.RoleKind, bd.RoleName, styleMuted.Render(via))
			for _, r := range bd.Rules {
				fmt.Fprintf(&b, "  %-36s %s\n", strings.Join(r.Verbs, ","), strings.Join(append(r.Resources, r.NonResourceURLs...), ","))
			}
		}
		return pagerMsg{title: "what service account " + sa + " may do in " + ns, content: b.String()}
	}
}

// --- events of one object ---

// objectEvents lists the events about an object, newest first.
func (m *Model) objectEvents(row view.Row) tea.Cmd {
	obj := row.Obj
	title := "events of " + view.KindWords(strings.ToLower(obj.GetKind())+" "+obj.GetName())
	// A workload's problems are often told on its pods (no node fits, a
	// failing check) or its ReplicaSets (can't make pods): theirs too, as
	// E on home lists an app's.
	related := map[string]bool{}
	switch obj.GetKind() {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job":
		for _, p := range m.enr.OwnedPods(obj) {
			related["Pod/"+p.GetName()] = true
		}
		if obj.GetKind() == "Deployment" {
			for _, rs := range m.enr.List(kube.ReplicaSets, obj.GetNamespace()) {
				if ref := metav1.GetControllerOf(rs); ref != nil && ref.UID == obj.GetUID() {
					related["ReplicaSet/"+rs.GetName()] = true
				}
			}
		}
	}
	return m.eventsWhere(obj.GetNamespace(), title, func(ev *unstructured.Unstructured) bool {
		uid, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "uid")
		name, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
		kind, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "kind")
		return (uid != "" && uid == string(obj.GetUID())) || (name == obj.GetName() && kind == obj.GetKind()) || related[kind+"/"+name]
	})
}

// eventAbout is "Pod/web-1": what an event is about.
func eventAbout(ev *unstructured.Unstructured) string {
	kind, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "kind")
	name, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
	return kind + "/" + name
}

// eventsWhere lists the events in a namespace that match, newest first.
func (m *Model) eventsWhere(ns, title string, match func(*unstructured.Unstructured) bool) tea.Cmd {
	gvr, ok := m.client.Resources.Served(kube.Events)
	if !ok {
		return nil
	}
	c, width := m.client, m.width
	c.Cache.Watch(gvr)
	m.busy = "reading events"
	return func() tea.Msg {
		deadline := time.Now().Add(xrayWait)
		for !c.Cache.Synced(gvr) && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		var evs []*unstructured.Unstructured
		for _, ev := range c.Cache.List(gvr, ns) {
			if match(ev) {
				evs = append(evs, ev)
			}
		}
		sort.Slice(evs, func(i, j int) bool { return enrich.EventTime(evs[i]).After(enrich.EventTime(evs[j])) })
		if len(evs) == 0 {
			return pagerMsg{title: title, content: "No events. Kubernetes keeps events for about an hour; w shows what changed over the last 24 hours."}
		}
		// Events about one thing (one pod) needn't name it on every line.
		oneThing := true
		for _, ev := range evs[1:] {
			if eventAbout(ev) != eventAbout(evs[0]) {
				oneThing = false
				break
			}
		}
		var b strings.Builder
		narrow := width < 110 // too narrow for columns: each event on two lines
		// No TYPE column: warnings show in colour, and "Normal" says nothing.
		switch {
		case narrow:
		case oneThing:
			fmt.Fprintf(&b, "%-8s %-22s %s\n", "AGO", "WHAT", "MESSAGE")
		default:
			fmt.Fprintf(&b, "%-8s %-22s %-32s %s\n", "AGO", "WHAT", "ABOUT", "MESSAGE")
		}
		for _, ev := range evs {
			typ, _, _ := unstructured.NestedString(ev.Object, "type")
			reason, _, _ := unstructured.NestedString(ev.Object, "reason")
			msg, _, _ := unstructured.NestedString(ev.Object, "message")
			kind, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "kind")
			name, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
			ago, what, about := view.Age(enrich.EventTime(ev)), view.EventWords(reason, kind, msg), view.KindWords(strings.ToLower(kind)+" "+name)
			message := view.MessageWords(strings.Join(strings.Fields(msg), " "))
			if n, _, _ := unstructured.NestedInt64(ev.Object, "count"); n > 1 { // as the events list's COUNT says
				message += fmt.Sprintf(" (%d times)", n)
			} else if n, _, _ := unstructured.NestedInt64(ev.Object, "series", "count"); n > 1 {
				message += fmt.Sprintf(" (%d times)", n)
			}
			var line string
			lw := strings.Fields(strings.ToLower(what))
			switch {
			case strings.EqualFold(message, what): // "container created": Container created
				message = ""
			case narrow && len(lw) > 1 && strings.HasPrefix(strings.ToLower(message), lw[0]+" "):
				what, message = message, "" // "placed on a node": placed on node ip-10-1-2-3
			}
			switch {
			case narrow && oneThing:
				line = strings.TrimRight(fmt.Sprintf("%-4s %s · %s", ago, what, message), " ·")
			case narrow:
				line = fmt.Sprintf("%-4s %s · %s", ago, what, about)
				if message != "" {
					line += "\n     " + message
				}
			case oneThing:
				line = fmt.Sprintf("%-8s %-22s %s", ago, truncate(what, 22), message)
			default:
				line = fmt.Sprintf("%-8s %-22s %-32s %s", ago, truncate(what, 22), truncate(about, 32), message)
			}
			if typ == "Warning" && !enrich.PolicyPassed(reason, msg) {
				if noColor { // flagged without colour, as lists do: /warnings finds it
					line += " !"
				}
				line = styleWarn.Render(line)
			}
			b.WriteString(line + "\n")
		}
		hang := 8 + 1 + 22 + 1 + 32 + 1 // under MESSAGE
		switch {
		case narrow:
			hang = 5
		case oneThing:
			hang = 8 + 1 + 22 + 1
		}
		return pagerMsg{title: title, content: b.String(), hang: hang}
	}
}

// --- what changed ---

// maxChanges bounds the timeline.
const maxChanges = 500

// whatChanged shows the last 24h for the selected app's namespace (home),
// or the namespace you are in (all namespaces = the whole cluster).
func (m *Model) whatChanged(row view.Row) tea.Cmd {
	ns, app, scope := m.namespace, "", nsWords(m.namespace)
	var own map[string]bool
	appRow := row
	if m.loc.app != "" { // a list of one app's things: what changed for that app
		for _, a := range m.apps {
			if a.Name == m.loc.app && a.Namespace == m.namespace {
				appRow = view.Row{Obj: view.Named(a.Namespace, a.Name)}
			}
		}
	}
	if m.loc.kind == locHome || m.loc.app != "" {
		if a, ok := m.appFor(appRow); ok && a.Namespace != "" {
			ns, scope = a.Namespace, "app "+a.Name
			own = m.appObjects(a)
			if a.Argo != nil {
				app = a.Name
				own["argocd/"+a.Name] = true
			}
		}
	}
	if own == nil && m.loc.kind == locResource && row.Obj != nil && m.client != nil {
		switch k := row.Obj.GetKind(); k {
		case "Deployment", "StatefulSet", "DaemonSet", "CronJob", "Job": // just this workload, not its whole namespace
			ns, scope = row.Obj.GetNamespace(), strings.ToLower(k)+" "+row.Obj.GetName()
			own = m.workloadObjects(row.Obj)
		}
	}
	return m.changesIn(ns, app, scope, own)
}

// workloadObjects are a workload and what it makes ("kind/name"): its
// ReplicaSets or runs, their pods, and its autoscaler.
func (m *Model) workloadObjects(w *unstructured.Unstructured) map[string]bool {
	key := func(o *unstructured.Unstructured) string { return strings.ToLower(o.GetKind()) + "/" + o.GetName() }
	own := map[string]bool{key(w): true, "horizontalpodautoscaler/" + w.GetName(): true}
	for _, p := range m.enr.PodsOf(w) {
		own[key(p)] = true
		for c := m.enr.Controller(p); c != nil && c.GetUID() != w.GetUID(); c = m.enr.Controller(c) {
			own[key(c)] = true // the ReplicaSet or run between them
		}
	}
	for _, g := range []schema.GroupVersionResource{kube.ReplicaSets, kube.Jobs} { // old versions and runs with no pods left
		served, ok := m.client.Resources.Served(g)
		if !ok {
			continue
		}
		for _, o := range m.client.Cache.List(served, w.GetNamespace()) {
			if ref := metav1.GetControllerOf(o); ref != nil && ref.UID == w.GetUID() {
				own[key(o)] = true
			}
		}
	}
	return own
}

// changesIn shows the timeline for a namespace ("" = whole cluster) or app.
// own, when set, keeps only changes to those objects ("kind/name").
// shipped lists the new versions of the last week, one line each, with
// the date, the workload and what it now runs: "what did we ship?".
func (m *Model) shipped() tea.Cmd {
	c, enr, ns, width := m.client, m.enr, m.namespace, m.width
	var gvrs []schema.GroupVersionResource
	for _, g := range enrich.ChangeNeeds {
		if served, ok := c.Resources.Served(g); ok {
			c.Cache.Watch(served)
			gvrs = append(gvrs, served)
		}
	}
	m.busy = "collecting this week's new versions in " + nsWords(ns)
	return func() tea.Msg {
		deadline := time.Now().Add(xrayWait)
		for _, g := range gvrs {
			for !c.Cache.Synced(g) && c.Cache.Err(g) == nil && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}
		}
		list := enr.Shipped(ns, time.Now(), enrich.ShipWindow)
		title := fmt.Sprintf("shipped · %s · last 7 days (%d)", nsWords(ns), len(list))
		if len(list) == 0 {
			return pagerMsg{title: title, content: "No new versions went out in the last 7 days (restarts aren't counted)."}
		}
		// Columns sized to what they hold; NAMESPACE only when it varies;
		// "deployment" goes without saying.
		type row struct{ when, ns, what, full, runs string }
		rows := make([]row, 0, len(list))
		nsSeen := map[string]bool{}
		whatW, nsW, whenW := len("WHAT"), len("NAMESPACE"), 17
		for _, ch := range list {
			what := shippedWhat(ch.Object, 44)
			runs := ch.Detail
			if _, after, ok := strings.Cut(runs, ": "); ok { // "revision 7: api:v42" -> "api:v42"
				runs = after
			}
			if neverRan(enr, ch) {
				runs += " (no pod of it ran: Kubernetes can't make its pods)"
			}
			when := ch.When.Local().Format("Mon 2 Jan 15:04")
			if ch.After { // a go-back nothing dates: then or later, as Deployed says
				when = "after " + when
			}
			rows = append(rows, row{when, ch.Namespace, what, shippedWhat(ch.Object, 0), runs})
			nsSeen[ch.Namespace] = true
			whatW, nsW, whenW = max(whatW, min(len(what), 44)), max(nsW, min(len(ch.Namespace), 26)), max(whenW, len(when))
		}
		showNS := len(nsSeen) > 1 && width >= 100
		var b, plain strings.Builder
		hang := whenW + 1 + whatW + 2
		if showNS {
			hang += nsW + 1
			fmt.Fprintf(&b, "%-*s %-*s %-*s  %s\n", whenW, "WHEN", nsW, "NAMESPACE", whatW, "WHAT", "IMAGES")
		} else {
			fmt.Fprintf(&b, "%-*s %-*s  %s\n", whenW, "WHEN", whatW, "WHAT", "IMAGES")
		}
		plain.WriteString("when,namespace,what,images,cluster\n")
		for i, r := range rows {
			if showNS {
				fmt.Fprintf(&b, "%-*s %-*s %-*s  %s\n", whenW, r.when, nsW, truncate(r.ns, nsW), whatW, truncate(r.what, whatW), r.runs)
			} else {
				fmt.Fprintf(&b, "%-*s %-*s  %s\n", whenW, r.when, whatW, truncate(r.what, whatW), r.runs)
			}
			when := list[i].When.Local().Format("2006-01-02 15:04 MST")
			if list[i].After {
				when = "at or after " + when // a bound stays a bound in the file
			}
			fmt.Fprintf(&plain, "%s,%s,%q,%q,%s\n", when, r.ns, r.full, r.runs, shortContext(m.context)) // the export: names in full
		}
		return pagerMsg{title: title, content: b.String(), hang: hang, plain: plain.String(), csv: true}
	}
}

// neverRan: a shipped row that is a deployment's newest version, which
// has no pods because Kubernetes can't make them (its rollout is stuck).
func neverRan(enr *enrich.Enricher, ch enrich.Change) bool {
	name, ok := strings.CutPrefix(ch.Object, "deployment/")
	if !ok || ch.After {
		return false
	}
	dep, ok := enr.Get(kube.Deployments, ch.Namespace, name)
	if !ok {
		return false
	}
	r := enr.RolloutOf(dep)
	return r.Current != nil && r.Current.Replicas == 0 && r.Current.Created.Equal(ch.When) && view.RolloutCantMake(enr, dep) != ""
}

func (m *Model) changesIn(ns, app, scope string, own map[string]bool) tea.Cmd {
	c, enr := m.client, m.enr
	var gvrs []schema.GroupVersionResource
	needs := enrich.ChangeNeeds
	if own != nil { // an autoscaler of another name: its scaleTargetRef says what it scales
		needs = append(slices.Clone(needs), kube.HPAs)
	}
	for _, g := range needs {
		if served, ok := c.Resources.Served(g); ok {
			c.Cache.Watch(served)
			gvrs = append(gvrs, served)
		}
	}
	m.busy = "collecting changes in " + scope
	width := m.pagerWidth()
	return func() tea.Msg {
		deadline := time.Now().Add(xrayWait)
		for _, g := range gvrs {
			for !c.Cache.Synced(g) && c.Cache.Err(g) == nil && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}
		}
		var keep func(string) bool
		if own != nil {
			// A pod that's gone (evicted, replaced) isn't in own any more,
			// but its name still starts with what made it: keep its events.
			var makers []string
			for o := range own {
				for _, kind := range []string{"replicaset/", "statefulset/", "daemonset/", "job/"} {
					if name, ok := strings.CutPrefix(o, kind); ok {
						makers = append(makers, "pod/"+name+"-")
					}
				}
			}
			keep = func(object string) bool {
				if own[object] {
					return true
				}
				if name, ok := strings.CutPrefix(object, "horizontalpodautoscaler/"); ok && ns != "" {
					// An autoscaler (hpa-real) of one of these (deployment/hpadep).
					h, ok := enr.Get(kube.HPAs, ns, name)
					if !ok {
						return false
					}
					target, _, _ := unstructured.NestedString(h.Object, "spec", "scaleTargetRef", "name")
					kind, _, _ := unstructured.NestedString(h.Object, "spec", "scaleTargetRef", "kind")
					return own[strings.ToLower(kind)+"/"+target]
				}
				for _, p := range makers {
					if strings.HasPrefix(object, p) {
						return true
					}
				}
				return false
			}
		}
		changes := enr.Changes(ns, app, time.Now(), keep)
		title := fmt.Sprintf("what changed · %s · last 24h", scope)
		if len(changes) == 0 {
			msg := "Nothing changed in the last 24 hours."
			if strings.HasPrefix(scope, "app ") {
				msg = "Nothing changed for this app in the last 24 hours.\n:changes shows everything in its namespace (or the whole cluster, after :ns all)."
			}
			return pagerMsg{title: title, content: msg}
		}
		// One namespace (an app, or :ns set): no NAMESPACE column.
		oneNS := strings.HasPrefix(scope, "app ") || ns != ""
		if len(changes) > maxChanges {
			changes = changes[:maxChanges]
		}
		ons, full := make([]string, len(changes)), make([]string, len(changes))
		onW, nsW := len("ON"), len("NAMESPACE")
		for i, ch := range changes {
			kind, name, _ := strings.Cut(ch.Object, "/")
			on := view.KindWords(kind + " " + strings.ReplaceAll(name, "/", " ")) // "deployment web"
			switch kind {
			case "argocd":
				on = "ArgoCD app " + name
			case "node":
				on = "node " + view.ShortNode(name)
			}
			on = strings.ReplaceAll(on, ", deployment/", ", deployment ")
			full[i] = on
			// Whole names when the screen has room (DETAIL keeps at least
			// 50 columns); cut, the start stays, which says what it is.
			ons[i] = truncate(on, max(30, width-6-1-8-1-2-50))
			onW = max(onW, len([]rune(ons[i])))
			nsW = max(nsW, min(len(ch.Namespace), 28))
		}
		// DETAIL must start early enough to wrap under itself (the pager
		// hangs wrapped text there only with 30 columns to its right):
		// long names give way.
		left := 6 + 1 + 8 + 1 + 2
		if !oneNS {
			left += nsW + 2
		}
		if limit := width - 40 - left; onW > limit && limit >= 20 {
			onW = limit
			for i := range ons {
				ons[i] = truncate(ons[i], onW)
			}
		}
		var b strings.Builder
		row := func(ago, what, nsCell, on, detail string) string {
			ago = padRight(ago, 6) // "≤5m" is one letter wider than its bytes say
			if oneNS {
				return fmt.Sprintf("%s %-8s %-*s  %s", ago, what, onW, on, detail)
			}
			return fmt.Sprintf("%s %-8s %-*s  %-*s  %s", ago, what, nsW, truncate(nsCell, nsW), onW, on, detail)
		}
		b.WriteString(row("AGO", "CHANGE", "NAMESPACE", "ON", "DETAIL") + "\n")
		for i, ch := range changes {
			ago := view.Age(ch.When)
			if ch.After { // a go-back nothing dates: then or later, as the versions list says
				ago = view.AgeAtMost(ch.When)
			}
			line := row(ago, ch.What, ch.Namespace, ons[i], ch.Detail)
			switch ch.Health {
			case enrich.HealthBad:
				line = styleErr.Render(line)
			case enrich.HealthWarn:
				line = styleWarn.Render(line)
			case enrich.HealthOK:
				line = styleOK.Render(line)
			}
			b.WriteString(line + "\n")
		}
		hang := 6 + 1 + 8 + 1 + onW + 2 // where DETAIL starts: wrapped text lines up under it
		if !oneNS {
			hang += nsW + 2
		}
		title += " · as of " + time.Now().Format("15:04")
		// Saved and copied: whole names, one change a line, for Slack or a file.
		var plain strings.Builder
		for i, ch := range changes {
			// A real time, not "29m ago": it is read later, somewhere else.
			when := ch.When.Local().Format("2 Jan 15:04 MST")
			if ch.After {
				when = "at or after " + when // a bound stays a bound in the file
			}
			fmt.Fprintf(&plain, "%s\t%s\t%s\t%s\t%s\n", when, ch.What, ch.Namespace, full[i], ch.Detail)
		}
		return pagerMsg{title: title, content: b.String(), hang: hang, plain: plain.String(), plainFrom: 1}
	}
}

// --- rollouts ---

func isDeployments(m *Model) bool {
	return onResource(m) && m.loc.res.GVR.Group == "apps" && m.loc.res.GVR.Resource == "deployments"
}

// isVersioned: lists whose objects keep versions to go back to.
func isVersioned(m *Model) bool {
	if !onResource(m) || m.loc.res.GVR.Group != "apps" {
		return false
	}
	switch m.loc.res.GVR.Resource {
	case "deployments", "statefulsets", "daemonsets":
		return true
	}
	return false
}

// goingBackWords is what is said once a rollback has started. A paused
// deployment takes the version but rolls nothing out until resumed.
func goingBackWords(ref kube.Ref, rev int, paused, atOnce bool, held string) string {
	if atOnce {
		return fmt.Sprintf("%s goes back to revision %d at once: that version already runs all its copies. %s", refWords(ref), rev, stillPaused)
	}
	if paused {
		return fmt.Sprintf("%s is set to go back to revision %d: it rolls out once it is resumed (kubectl rollout resume)", refWords(ref), rev)
	}
	if held != "" {
		return fmt.Sprintf("%s is set to go back to revision %d, but no pod runs it yet. %s", refWords(ref), rev, held)
	}
	text := fmt.Sprintf("%s is going back to revision %d", refWords(ref), rev)
	if ref.Resource.GVR.Resource == "deployments" {
		text += " (o to watch)"
	}
	return text
}

// heldPodWords is the go-back confirm's line for a stateful set waiting
// on a failing pod: going back doesn't replace it, so nothing else stops.
func heldPodWords(pod string) string {
	return "\n  Its pod " + pod + " is failing now: going back doesn't replace it (Kubernetes waits for it to be ready first). Delete it (ctrl-d on " + pod + ") for this to take effect."
}

// pausedWords is the confirm's line for a change to a paused deployment.
const pausedWords = "It is paused: nothing rolls out until it is resumed (kubectl rollout resume)."

// pausedBackWords is the confirm's line for going back on a paused
// deployment to v, which takes over at once when it already runs all
// its copies (takesAtOnce).
func pausedBackWords(o *unstructured.Unstructured, v *enrich.Version) string {
	if takesAtOnce(o, v) {
		return "It is paused, but that version already runs all its copies: it takes over at once, and the other version's pods stop. " + stillPaused
	}
	return pausedWords
}

// stillPaused: a paused deployment that took a version at once stays
// paused all the same.
const stillPaused = "It stays paused: later changes wait until it is resumed (kubectl rollout resume)."

// takesAtOnce: on a paused deployment o, going to version v changes over
// at once all the same. Kubernetes still scales a paused deployment: when
// its pod template is a version whose ReplicaSet already runs (and has
// available) all its copies, it scales the other versions to 0 then.
func takesAtOnce(o *unstructured.Unstructured, v *enrich.Version) bool {
	if v == nil || !pausedDeploy(o) {
		return false
	}
	want, found, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")
	if !found {
		want = 1
	}
	return v.Saturated(want)
}

// imageVersion is the kept version of deployment o whose pod template is
// o's with container's image set to image (nil when none is), as
// Kubernetes matches them: the restart stamp too.
func (m *Model) imageVersion(o *unstructured.Unstructured, container, image string, init bool) *enrich.Version {
	if o.GetKind() != "Deployment" {
		return nil
	}
	t, ok, _ := unstructured.NestedMap(o.Object, "spec", "template")
	if !ok {
		return nil
	}
	list := map[bool]string{false: "containers", true: "initContainers"}[init]
	items, _, _ := unstructured.NestedSlice(t, "spec", list)
	set := false
	for _, c := range items {
		if cm, _ := c.(map[string]any); cm != nil && cm["name"] == container {
			cm["image"], set = image, true
		}
	}
	if !set || unstructured.SetNestedSlice(t, items, "spec", list) != nil {
		return nil
	}
	restarted, _, _ := unstructured.NestedString(t, "metadata", "annotations", "kubectl.kubernetes.io/restartedAt")
	settings := kube.TemplateSettings(t)
	for _, v := range m.enr.RolloutOf(o).All {
		if v.Settings == settings && v.Restarted == restarted {
			return v
		}
	}
	return nil
}

// rolloutView shows a Deployment's update: versions, image change, pods,
// refreshed every second until you leave.
func (m *Model) rolloutView(row view.Row) tea.Cmd {
	ns, name := row.Obj.GetNamespace(), row.Obj.GetName()
	render := func() string {
		dep, ok := m.client.Cache.Get(m.loc.res.GVR, ns, name)
		if !ok {
			return "deployment " + name + " is gone"
		}
		r := m.enr.RolloutOf(dep)
		if crash := m.newPodsCrashing(dep, r); crash != "" {
			r.Stuck, r.Message = true, crash
		} else if why := view.RolloutCantMake(m.enr, dep); why != "" { // it never finishes: say so now, and why
			r.Stuck, r.Message = true, "Kubernetes can't make the new version's pods: "+why
		}
		return renderRollout(r, m.enr.Warnings(dep, enrich.ActionEdit), m.opts.ReadOnly)
	}
	if rs, ok := m.client.Resources.Served(kube.ReplicaSets); ok {
		m.client.Cache.Watch(rs) // versions come from ReplicaSets; the view refreshes as they load
	}
	p := newPager("rollout of deployment "+name, render(), m.pagerWidth(), m.pagerHeight())
	p.wrap, p.hang = true, 9 // image tags are what this screen is for: never cut them off
	p.refresh()
	p.live = render
	p.undo = func() tea.Cmd { // u here does what u does on the deployment
		if m.writesLocked() {
			m.setFlash("", m.lockedReason())
			return nil
		}
		m.closePager()
		return m.undoRollout(row)
	}
	p.versions = func() tea.Cmd {
		m.closePager()
		cmd := m.versions(row)
		if m.prompt != nil && m.prompt.kind == promptMenu { // esc on it comes back here
			m.prompt.backTo = func() tea.Cmd { return m.rolloutView(row) }
		}
		return cmd
	}
	m.openPager(p)
	return nil
}

// newPodsCrashing says why the new version's pods don't come up, when they
// keep crashing or can't start, so a rollout that will never finish says so
// before Kubernetes' own ten-minute deadline.
func (m *Model) newPodsCrashing(dep *unstructured.Unstructured, r enrich.Rollout) string {
	if r.Current == nil {
		return ""
	}
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return ""
	}
	for _, p := range m.client.Cache.List(gvr, dep.GetNamespace()) {
		owned := false
		for _, ref := range p.GetOwnerReferences() {
			owned = owned || ref.Name == r.Current.Name
		}
		if !owned {
			continue
		}
		statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "containerStatuses")
		for _, s := range statuses {
			sm, _ := s.(map[string]any)
			reason, _, _ := unstructured.NestedString(sm, "state", "waiting", "reason")
			name, _ := sm["name"].(string)
			switch reason {
			case "CrashLoopBackOff":
				return fmt.Sprintf("the new pods keep crashing (container %q). l shows the logs, p the ones from before the crash", name)
			case "ImagePullBackOff", "ErrImagePull":
				return fmt.Sprintf("the new image can't be pulled (container %q): check the image name and tag", name)
			case "CreateContainerConfigError":
				msg, _, _ := unstructured.NestedString(sm, "state", "waiting", "message")
				if kind, obj, key, ok := enrich.MissingKey(msg); ok { // the object is there, its key isn't
					return fmt.Sprintf("container %q can't start: %s %s has no key %s", name, kind, obj, key)
				}
				return fmt.Sprintf("container %q can't start: a secret or config map it needs is missing", name)
			}
		}
	}
	return ""
}

func renderRollout(r enrich.Rollout, warnings []string, readOnly bool) string {
	var b strings.Builder
	state := styleOK.Render("✓ " + r.Summary())
	switch {
	case r.Stuck:
		state = styleErr.Render("✗ " + r.Summary())
	case !r.Done():
		state = styleWarn.Render("… " + r.Summary())
	}
	b.WriteString(state + "\n")
	if r.Stuck && r.Message != "" {
		b.WriteString(styleMuted.Render("  "+r.Message) + "\n")
	}
	if r.Paused {
		b.WriteString(styleWarn.Render("  paused") + "\n")
	}
	bar := func(ready, total int64) string {
		const width = 20
		if total <= 0 {
			return strings.Repeat("░", width)
		}
		filled := int(ready * width / total)
		return styleOK.Render(strings.Repeat("█", filled)) + styleMuted.Render(strings.Repeat("░", width-filled))
	}
	b.WriteString("\n")
	if c := r.Current; c != nil {
		fmt.Fprintf(&b, "%s revision %-4d %s %-12s %s\n", styleInfoLabel.Render("Now     "), c.Revision,
			bar(c.Ready, max(r.Desired, 1)), fmt.Sprintf("%d/%d ready", c.Ready, r.Desired), strings.Join(c.Images, ", "))
	}
	if p := r.Previous; p != nil {
		fmt.Fprintf(&b, "%s revision %-4d %s %-12s %s\n", styleInfoLabel.Render("Before  "), p.Revision,
			bar(p.Ready, max(p.Replicas, 1)), view.Plural(int(p.Replicas), "pod")+" left", strings.Join(p.Images, ", "))
		fmt.Fprintf(&b, "%s %s\n", styleInfoLabel.Render("Changed "), r.ImageChange())
	}
	if n := len(r.All) - 1; n > 0 {
		fmt.Fprintf(&b, "%s %s to go back to (v lists them)\n", styleInfoLabel.Render("Kept    "), view.Plural(n, "earlier version"))
	}
	target, _ := undoTarget(r)
	switch {
	case r.Previous == nil:
		b.WriteString("\n" + styleMuted.Render("There is no earlier version to go back to."))
	case target == nil:
		b.WriteString("\n" + styleMuted.Render("Every kept version runs the same as now (they differ only by restarts): u has nowhere to go."))
	case readOnly:
		b.WriteString("\n" + styleMuted.Render(fmt.Sprintf("u would go back to revision %d (%s); it needs kubed without --readonly.", target.Revision, strings.Join(target.Images, ", "))))
	default:
		b.WriteString("\n" + styleMuted.Render(fmt.Sprintf("Press u to go back to revision %d (%s).", target.Revision, strings.Join(target.Images, ", "))))
	}
	for _, w := range warnings {
		b.WriteString("\n" + styleWarn.Render("⚠ "+w))
	}
	return b.String()
}

// undoRollout goes back to the previous version after a confirm.
// undoTarget is where u goes: the version before is often the same thing
// restarted, which wouldn't change anything, so the last version that ran
// something else. nil when every kept version is a restart of now.
func undoTarget(r enrich.Rollout) (*enrich.Version, int) {
	skipped := 0
	for i := 1; i < len(r.All); i++ {
		if r.All[i].Settings != r.Current.Settings { // runs something else, not just a restart of now
			return r.All[i], skipped
		}
		skipped++
	}
	return nil, skipped
}

// undoTargetFails says a pod of the version u goes back to is failing
// (as home's Fix checks it).
func (m *Model) undoTargetFails(o *unstructured.Unstructured) bool {
	if k := o.GetKind(); k != "Deployment" && k != "StatefulSet" && k != "DaemonSet" {
		return false
	}
	r := m.enr.RolloutOf(o)
	if target, _ := undoTarget(r); r.Previous != nil && target != nil {
		_, fails := view.VersionFails(m.enr, o, target.Name)
		return fails
	}
	return false
}

// goBackWarnings are the lines both go-back confirms (u, and b in the
// versions list) add about the version they go back to: how it differs
// from now, and whether its pods fail now or it failed when it last ran.
// diffHint ends the "only settings differ" line.
func (m *Model) goBackWarnings(o *unstructured.Unstructured, target, current *enrich.Version, diffHint string) string {
	var out string
	sameImages := strings.Join(target.Images, ",") == strings.Join(current.Images, ",")
	switch {
	case target.Settings == current.Settings:
		out += "\n  Same image: the two differ only by a restart."
	case sameImages && enrich.OtherBuild(target.Refs, current.Refs):
		out += "\n  Same tag, but a different build of it (the image digest differs)."
	case sameImages:
		out += "\n  Same image: only settings differ (env, config, resources)." + diffHint
	}
	if st, fails := view.VersionFails(m.enr, o, target.Name); fails {
		out += "\n  That version's pods are failing now: " + strings.ToLower(st.Short)
	} else if why, firm := view.VersionFailedBefore(m.enr, o, target, current); firm {
		out += "\n  That version failed when it last ran, so going back won't help: " + why
	} else if why != "" {
		out += "\n  That version failed when it last ran, so going back may not help: " + why
	}
	return out
}

// undoFailsAbout ends u's menu line when the version it goes back to fails too.
const undoFailsAbout = ". Going back won't help: the previous version fails too"

// undoFailedAbout ends it when that version failed when it last ran, as
// home says it: won't help when the registry said its image doesn't exist
// or denied access, may not help otherwise. "" when nothing says so.
func (m *Model) undoFailedAbout(o *unstructured.Unstructured) string {
	if k := o.GetKind(); k != "Deployment" && k != "StatefulSet" && k != "DaemonSet" {
		return ""
	}
	r := m.enr.RolloutOf(o)
	target, _ := undoTarget(r)
	if r.Previous == nil || target == nil {
		return ""
	}
	switch why, firm := view.VersionFailedBefore(m.enr, o, target, r.Current); {
	case firm:
		return ". Going back won't help: that version failed when it last ran"
	case why != "":
		return ". Going back may not help: that version failed when it last ran"
	}
	return ""
}

// undoOff is why u can't go back on a workload, once its versions have
// loaded (before, u waits for them): the menu's note and line, and the
// error u gives. All empty when it can, or while they load.
func (m *Model) undoOff(o *unstructured.Unstructured) (off, about string, err error) {
	ref := m.ref(view.Row{Obj: o})
	if gvr, ok := m.versionsList(ref); !ok || !m.client.Cache.Synced(gvr) {
		return "", "", nil
	}
	r := m.enr.RolloutOf(o)
	if r.Previous == nil {
		return "no earlier version", "Can't: it keeps no earlier version to go back to", fmt.Errorf("%s has no earlier version to go back to", ref.Name)
	}
	if target, _ := undoTarget(r); target == nil {
		return "only restarts", "Can't: every kept version runs the same as now (they differ only by restarts). v lists them",
			fmt.Errorf("every kept version of %s runs the same as now (they differ only by restarts). v lists them", ref.Name)
	}
	return "", "", nil
}

func (m *Model) undoRollout(row view.Row) tea.Cmd {
	ref := m.ref(row)
	if served, ok := m.versionsList(ref); ok && !m.client.Cache.Synced(served) {
		// "no earlier version" from a list still loading would be wrong
		return m.whenLoaded(served, ref.Name+"'s versions", func() tea.Cmd { return m.undoRolloutNow(row) })
	}
	return m.undoRolloutNow(row)
}

// undoRolloutNow goes back with the versions as loaded, without waiting.
func (m *Model) undoRolloutNow(row view.Row) tea.Cmd {
	ref := m.ref(row)
	r := m.enr.RolloutOf(row.Obj)
	if r.Previous == nil {
		m.setFlash("", fmt.Errorf("%s has no earlier version to go back to", ref.Name))
		return nil
	}
	target, skipped := undoTarget(r)
	if target == nil { // every older version is a restart of this one
		m.setFlash("", fmt.Errorf("every kept version of %s runs the same as now (they differ only by restarts). v lists them", ref.Name))
		return nil
	}
	title := fmt.Sprintf("Go back to revision %d of %s?\n  now:     %s\n  back to: %s", target.Revision, refWords(ref),
		strings.Join(r.Current.Images, ", "), strings.Join(target.Images, ", "))
	if skipped > 0 {
		title += fmt.Sprintf("\n  (Skipping %s that only restarted the same thing.)", view.Plural(skipped, "newer version"))
	}
	title += m.goBackWarnings(row.Obj, target, r.Current, " v shows what differs.")
	held := view.HeldAfterGoingBack(m.enr, row.Obj)
	if held != "" {
		title += heldPodWords(held)
	}
	switch {
	case pausedDeploy(row.Obj):
		title += "\n  " + pausedBackWords(row.Obj, target)
	case held != "": // nothing is replaced until that pod is deleted: no "down for a moment" now
	case row.Obj.GetKind() != "Deployment" || strings.Contains(restartWhy(row.Obj), "down for a moment"): // a Deployment stays up, unless it stops all its pods first (Recreate)
		title += "\n  " + restartWhy(row.Obj)
	}
	m.confirmTitled(title, []view.Row{row}, m.enr.Warnings(row.Obj, enrich.ActionEdit), func() tea.Cmd {
		return func() tea.Msg {
			ctx, cancel := apiCtx()
			defer cancel()
			rev, err := m.client.RolloutTo(ctx, ref, target.Revision)
			if err != nil {
				return resultMsg{err: err}
			}
			return resultMsg{text: goingBackWords(ref, rev, pausedDeploy(row.Obj), takesAtOnce(row.Obj, target), heldWords(row.Obj))}
		}
	})
	return nil
}

// versionWhen says when a version went out ("made 8m ago") or came back
// ("back 8m ago"; "back ≤8m ago" when only the versions made before it
// date the go-back, the same bound what changed and Deployed say).
// unknown: brought back, and nothing dates it any more.
func versionWhen(v *enrich.Version, all []*enrich.Version) (made, ago string, unknown bool) {
	after := v.BackAfter // the versions it came back over were made by then
	if after.IsZero() {
		for _, w := range all { // a version with a lower number was made before it came back
			if w.Revision < v.Revision && w.Created.After(after) {
				after = w.Created
			}
		}
	}
	switch {
	case v.Reused && !v.LiveSince.IsZero():
		return "back", view.Age(v.LiveSince) + " ago", false
	case v.Reused && !after.IsZero():
		return "back", view.AgeAtMost(after) + " ago", false
	case v.Reused:
		return "first", view.Age(v.Created) + " ago", true // events last about an hour
	}
	// Made by a restart of the version just before it (same settings, a
	// new restart stamp): "restarted", as what changed says.
	var prev *enrich.Version
	for _, w := range all {
		if w.Revision < v.Revision && (prev == nil || w.Revision > prev.Revision) {
			prev = w
		}
	}
	if prev != nil && prev.Settings == v.Settings && prev.Restarted != v.Restarted {
		return "restarted", view.Age(v.Created) + " ago", false
	}
	return "made", view.Age(v.Created) + " ago", false
}

// versionsAt reopens the versions with the cursor on one of them.
func (m *Model) versionsAt(row view.Row, revision int) tea.Cmd {
	open := func() tea.Cmd {
		cmd := m.versionsNow(row)
		if m.prompt != nil {
			for i, it := range m.prompt.items {
				if strings.HasPrefix(it.label, fmt.Sprintf("revision %-4d", revision)) {
					m.prompt.choice = i
				}
			}
		}
		return cmd
	}
	ref := m.ref(row)
	if served, ok := m.versionsList(ref); ok && !m.client.Cache.Synced(served) {
		return m.whenLoaded(served, ref.Name+"'s versions", open) // the cursor goes on it once they're in
	}
	return open()
}

// versions lists a Deployment's kept versions; picking one goes back to it.
func (m *Model) versions(row view.Row) tea.Cmd {
	ref := m.ref(row)
	if served, ok := m.versionsList(ref); ok && !m.client.Cache.Synced(served) {
		// where its versions are: wait for them
		return m.whenLoaded(served, ref.Name+"'s versions", func() tea.Cmd { return m.versionsNow(row) })
	}
	return m.versionsNow(row)
}

// versionsList is the list a workload's kept versions are in: ReplicaSets
// for a Deployment, ControllerRevisions for a StatefulSet or DaemonSet.
// Until it has loaded, "no versions" would be a guess.
func (m *Model) versionsList(ref kube.Ref) (schema.GroupVersionResource, bool) {
	if ref.Resource.GVR.Resource == kube.Deployments.Resource {
		return m.client.Resources.Served(kube.ReplicaSets)
	}
	return m.client.Resources.Served(kube.ControllerRevisions)
}

// noVersionsWhy says why a workload shows no kept versions: still
// loading, can't be read, or truly none listed.
func (m *Model) noVersionsWhy(ref kube.Ref) error {
	served, ok := m.versionsList(ref)
	switch {
	case ok && m.client.Cache.Err(served) != nil:
		return fmt.Errorf("kubed can't read %s's versions: %w", ref.Name, m.client.Cache.Err(served))
	case ok && m.client.Cache.Synced(served):
		return fmt.Errorf("%s has no kept versions listed", ref.Name)
	}
	return fmt.Errorf("still loading %s's versions; try again in a moment", ref.Name)
}

// versionsNow lists the versions as loaded, without waiting.
func (m *Model) versionsNow(row view.Row) tea.Cmd {
	ref := m.ref(row)
	items, r := m.versionItems(row)
	if len(r.All) == 0 {
		m.setFlash("", m.noVersionsWhy(ref))
		return nil
	}
	m.prompt = menuPrompt("versions of "+refWords(ref)+" (newest first)", items)
	m.prompt.maxItems = m.menuRoom()
	menu := m.prompt
	// While it is open, each version is read again from the live cache:
	// its age, and its ready counts as the rollout goes on ("2 of 3 ready"
	// becomes "running now"), the cursor kept on its version.
	menu.tick = func() {
		fresh := row
		if o, ok := m.enr.Get(ref.Resource.GVR, row.Obj.GetNamespace(), row.Obj.GetName()); ok {
			if o.GetUID() != row.Obj.GetUID() {
				return // deleted and made again under the same name: another workload's versions
			}
			fresh.Obj = o
		}
		again, now := m.versionItems(fresh)
		if len(now.All) == 0 {
			return // gone for a moment: keep what is shown
		}
		picked := -1
		if menu.choice < len(r.All) {
			picked = r.All[menu.choice].Revision
		}
		menu.items, r = again, now
		for i, v := range r.All {
			if v.Revision == picked {
				menu.choice = i
			}
		}
		menu.choice = min(menu.choice, len(menu.items)-1)
	}
	m.prompt.choice = min(1, len(items)-1)
	m.mode = modePrompt
	return nil
}

// servingBefore is the newest older version with pods ready (0 when none).
func servingBefore(all []*enrich.Version) int {
	for _, v := range all[1:] {
		if v.Ready > 0 {
			return v.Revision
		}
	}
	return 0
}

// versionItems are the versions list's lines for a workload, as it is
// now, and the versions they are read from.
func (m *Model) versionItems(row view.Row) ([]menuItem, enrich.Rollout) {
	ref := m.ref(row)
	r := m.enr.RolloutOf(row.Obj)
	if len(r.All) == 0 {
		return nil, r
	}
	var items []menuItem
	wordW := 5 // "made", "back", "first": wider when one says "restarted"
	for _, v := range r.All {
		made, _, _ := versionWhen(v, r.All)
		wordW = max(wordW, len(made))
	}
	when := func(v *enrich.Version) string { // "revision 7    made  8m ago     "
		made, ago, _ := versionWhen(v, r.All)
		return fmt.Sprintf("revision %-4d %-*s %-11s", v.Revision, wordW, made, ago)
	}
	now := strings.Join(r.All[0].Images, ", ")
	for i, v := range r.All {
		images := strings.Join(v.Images, ", ")
		// A version brought back keeps its first creation time: say so, so
		// "revision 13 made 35m ago" above revision 12 isn't a puzzle.
		_, _, unknown := versionWhen(v, r.All)
		// What it is ("running now", "same image") before its images: on a
		// narrow screen the images get cut, never the part that says which.
		label := when(v)
		imgs := "  " + imageTags(v.Images) // the tags tell versions apart; "Runs" below names the images
		if unknown {
			imgs = "  (brought back since)" + imgs
		}
		about := "Runs " + images
		switch {
		case i == 0:
			now := "  (running now)"
			switch reps, found, _ := unstructured.NestedInt64(row.Obj.Object, "spec", "replicas"); {
			case found && reps == 0:
				now = "  (current, scaled to 0)"
			case v.Replicas == 0 && (!found || reps > 0) && servingBefore(r.All) > 0:
				// None of its pods made (a quota, a policy): an older one serves.
				now = fmt.Sprintf("  (newest, no pods; revision %d serves)", servingBefore(r.All))
			case v.Replicas > 0 && v.Ready < v.Replicas && !found:
				// The newest, but its pods aren't serving yet (or crash):
				// an older version may still be the one answering.
				now = fmt.Sprintf("  (newest, %d of %d ready)", v.Ready, v.Replicas)
			case found && (v.Ready < reps || servingBefore(r.All) > 0):
				// Not all the workload's copies ready (mid-rollout its
				// ReplicaSet wants fewer so far), or an older one still serves.
				now = fmt.Sprintf("  (newest, %d of %d ready)", v.Ready, reps)
			}
			said := "this is the version running now: pick an older one to go back to"
			if now != "  (running now)" { // set to run, but not (all) running: say only what is sure
				said = "this is the newest version (what it is set to run): pick an older one to go back to"
			}
			current := func() tea.Cmd { // enter or b: nothing to compare with or go back to
				m.setFlash(said, nil)
				return nil
			}
			items = append(items, menuItem{label: label + now + imgs, about: about, also: "b", alsoRun: current, run: current})
			continue
		case v.Settings == r.All[0].Settings:
			label += "  (same image, before a restart)"
			about += ", like now. It differs only by a restart"
		case images == now && enrich.OtherBuild(v.Refs, r.All[0].Refs):
			label += "  (same tag, other build)"
			about += ": the same tag as now, but a different build of it (the image digest differs)"
		case images == now:
			label += "  (same image, other settings)"
			about += ", like now: only its settings differ (env, config, resources)"
		}
		if v.Ready > 0 { // an older version still answering (mid-rollout, or the new one is stuck)
			label += fmt.Sprintf("  · %d serving", v.Ready)
		}
		label += imgs
		goBack := func() tea.Cmd { //nolint:unparam // a menu item's run: the confirm prompt does the work
			if m.writesLocked() {
				m.setFlash("", m.lockedReason())
				return nil
			}
			title := fmt.Sprintf("Go back to revision %d of %s?\n  now:     %s\n  back to: %s", v.Revision, refWords(ref),
				strings.Join(r.Current.Images, ", "), strings.Join(v.Images, ", "))
			title += m.goBackWarnings(row.Obj, v, r.Current, "") // as u says it
			held := view.HeldAfterGoingBack(m.enr, row.Obj)
			if held != "" {
				title += heldPodWords(held)
			}
			switch {
			case pausedDeploy(row.Obj):
				title += "\n  " + pausedBackWords(row.Obj, v)
			case held != "": // nothing is replaced until that pod is deleted
			case row.Obj.GetKind() != "Deployment" || strings.Contains(restartWhy(row.Obj), "down for a moment"):
				title += "\n  " + restartWhy(row.Obj)
			}
			m.confirmTitled(title, []view.Row{row}, m.enr.Warnings(row.Obj, enrich.ActionEdit), func() tea.Cmd {
				return func() tea.Msg {
					ctx, cancel := apiCtx()
					defer cancel()
					rev, err := m.client.RolloutTo(ctx, ref, v.Revision)
					if err != nil {
						return resultMsg{err: err}
					}
					return resultMsg{text: goingBackWords(ref, rev, pausedDeploy(row.Obj), takesAtOnce(row.Obj, v), heldWords(row.Obj))}
				}
			})
			return nil
		}
		// Picking a version asks what to do: see how it differs from now,
		// or go back to it.
		hint := ". Pick it to see how it differs from now; b goes back to it."
		if m.writesLocked() {
			hint = ". Pick it to see how it differs from now (going back is off: read-only)."
		}
		items = append(items, menuItem{label: label, about: about + hint,
			also: "b", alsoRun: goBack,
			run: func() tea.Cmd {
				back := menuItem{label: "Go back to this version", key: "b", about: "Run revision " + fmt.Sprint(v.Revision) + " again (asks first)", run: goBack}
				if m.writesLocked() {
					back.disabled, back.off, back.about = true, "read-only", "Can't: kubed is read-only (started with --readonly)"
				}
				m.prompt = menuPrompt(fmt.Sprintf("revision %d of %s", v.Revision, refWords(ref)), []menuItem{
					{label: "What differs from now", key: "d", about: "Its pod settings next to the running version's, line by line", run: func() tea.Cmd {
						return m.versionDiff(ref, r.Current, v, func() tea.Cmd { return m.versionsAt(row, v.Revision) })
					}},
					back,
				})
				m.mode = modePrompt
				return nil
			}})
	}
	return items, r
}

// shortImages is images without their registry: "auth:v2".
// imageTags is just the tags of images: what differs from one version of
// the same app to the next ("production-c4e4220a"). An image without a
// tag keeps its name.
func imageTags(images []string) string {
	out := make([]string, len(images))
	for i, img := range images {
		name, note, _ := strings.Cut(img, " (")
		short := enrich.ShortImage(name)
		if j := strings.LastIndex(short, ":"); j >= 0 && !strings.Contains(short[j:], "/") {
			short = short[j+1:]
		}
		out[i] = short
		if note != "" {
			out[i] += " (" + note
		}
	}
	return strings.Join(out, ", ")
}

// lastCopiesWarning warns when the pods picked are every copy that is
// serving something: it stops answering until new ones are ready. With
// none ready (all crashing) or others left, nothing more stops.
func (m *Model) lastCopiesWarning(rows []view.Row) string {
	type owner struct {
		obj   *unstructured.Unstructured
		ready int64
	}
	picked := map[string]*owner{}
	var order []string
	for _, r := range rows {
		if r.Obj.GetKind() != "Pod" || !podReady(r.Obj) {
			continue
		}
		top := m.enr.TopOwner(r.Obj)
		if top.GetKind() == "Pod" || top.GetKind() == "DaemonSet" { // a DaemonSet's pods are one per node
			continue
		}
		key := top.GetKind() + "/" + top.GetName()
		if picked[key] == nil {
			picked[key] = &owner{obj: top}
			order = append(order, key)
		}
		picked[key].ready++
	}
	for _, k := range order {
		o := picked[k]
		if ready, _, _ := unstructured.NestedInt64(o.obj.Object, "status", "readyReplicas"); ready > 0 && o.ready >= ready {
			what := strings.ToLower(o.obj.GetKind()) + " " + o.obj.GetName()
			if ready == 1 {
				return "\n⚠ It is the only running copy of " + what + ": what it serves stops until the new one is ready."
			}
			return fmt.Sprintf("\n⚠ These are all %d running copies of %s: what it serves stops until new ones are ready.", ready, what)
		}
	}
	return ""
}

// podReady: the pod's Ready condition is true (it answers traffic).
func podReady(pod *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(pod.Object, "status", "conditions")
	for _, c := range conds {
		if m, _ := c.(map[string]any); m["type"] == "Ready" && m["status"] == "True" {
			return true
		}
	}
	return false
}

// suspendCronJob pauses a CronJob's schedule, or resumes it.
func (m *Model) suspendCronJob(row view.Row) tea.Cmd {
	ns, name := row.Obj.GetNamespace(), row.Obj.GetName()
	paused, _, _ := unstructured.NestedBool(row.Obj.Object, "spec", "suspend")
	verb, why, done := "Pause", "No new runs start until you resume it. A run going on now finishes.", "paused"
	if paused {
		verb, why, done = "Resume", "Runs start again on its schedule.", "resumed"
	}
	m.confirmRows(verb, why, []view.Row{row}, enrich.ActionScale, func() tea.Cmd {
		return func() tea.Msg {
			ctx, cancel := apiCtx()
			defer cancel()
			if err := m.client.SuspendCronJob(ctx, ns, name, !paused); err != nil {
				return resultMsg{err: err}
			}
			return resultMsg{text: done + " cronjob " + name}
		}
	})
	return nil
}

// lastRunLogs opens the logs of a CronJob's newest run.
func (m *Model) lastRunLogs(row view.Row) tea.Cmd {
	name := row.Obj.GetName()
	jobs, _ := m.client.Resources.Served(kube.Jobs)
	pods, _ := m.client.Resources.Served(kube.Pods)
	m.client.Cache.Watch(jobs)
	m.client.Cache.Watch(pods)
	var newest *unstructured.Unstructured
	for _, j := range m.client.Cache.List(jobs, row.Obj.GetNamespace()) {
		for _, ref := range j.GetOwnerReferences() {
			if ref.Kind == "CronJob" && ref.Name == name &&
				(newest == nil || j.GetCreationTimestamp().After(newest.GetCreationTimestamp().Time)) {
				newest = j
			}
		}
	}
	if newest == nil {
		hint := " t runs it now"
		if m.writesLocked() {
			hint = ""
		}
		m.setFlash("", fmt.Errorf("%s hasn't run yet (or its runs were cleaned up).%s", name, hint))
		return nil
	}
	if m.neverStarted(newest) {
		return nil
	}
	var pod *unstructured.Unstructured
	for _, p := range m.client.Cache.List(pods, row.Obj.GetNamespace()) {
		for _, ref := range p.GetOwnerReferences() {
			if ref.Kind == "Job" && ref.Name == newest.GetName() &&
				(pod == nil || p.GetCreationTimestamp().After(pod.GetCreationTimestamp().Time)) {
				pod = p
			}
		}
	}
	if pod == nil {
		m.setFlash("", fmt.Errorf("the last run, %s, has no pod left to read logs from", newest.GetName()))
		return nil
	}
	return m.logs(view.Row{Obj: pod, Cells: []string{pod.GetName()}}, false)
}

// argoDiff lists what an ArgoCD app manages, the parts that differ from
// Git first, with each one's health.
func (m *Model) argoDiff(row view.Row) tea.Cmd {
	o := row.Obj
	res, _, _ := unstructured.NestedSlice(o.Object, "status", "resources")
	if len(res) == 0 {
		m.setFlash("", fmt.Errorf("ArgoCD hasn't listed what %s manages yet", o.GetName()))
		return nil
	}
	type item struct{ sync, health, what, ns string }
	var items []item
	for _, r := range res {
		rm, _ := r.(map[string]any)
		s := func(k string) string { v, _ := rm[k].(string); return v }
		h, _, _ := unstructured.NestedString(rm, "health", "status")
		sync := "matches Git"
		if s("status") == "OutOfSync" {
			sync = "differs from Git"
		}
		health := map[string]string{"Healthy": "healthy", "Degraded": "failing", "Progressing": "updating", "Missing": "missing", "Suspended": "paused"}[h]
		if health == "" && s("kind") == "ExternalSecret" { // ArgoCD gives these no health: read it
			if gvr, ok := m.client.Resources.Served(kube.ExternalSecrets); ok {
				if es, found := m.client.Cache.Get(gvr, s("namespace"), s("name")); found && enrich.ExternalSecret(es).Ready == "False" {
					health = "failing"
				}
			}
		}
		items = append(items, item{sync, health, view.KindWords(strings.ToLower(s("kind")) + " " + s("name")), s("namespace")})
	}
	rank := func(it item) int { // what differs from Git, then what fails, then the rest
		switch {
		case it.sync == "differs from Git":
			return 0
		case it.health == "failing" || it.health == "missing":
			return 1
		}
		return 2
	}
	sort.SliceStable(items, func(i, j int) bool { return rank(items[i]) < rank(items[j]) })
	var b strings.Builder
	fmt.Fprintf(&b, "%-17s %-9s %-44s %s\n", "GIT", "HEALTH", "WHAT", "NAMESPACE")
	differs := 0
	for _, it := range items {
		line := fmt.Sprintf("%-17s %-9s %-44s %s", it.sync, view.OrDash(it.health), truncate(it.what, 44), view.OrDash(it.ns))
		switch {
		case it.sync == "differs from Git":
			differs++
			line = styleWarn.Render(line)
		case it.health == "failing" || it.health == "missing":
			line = styleErr.Render(line)
		}
		b.WriteString(line + "\n")
	}
	verb := "differ"
	if differs == 1 {
		verb = "differs"
	}
	title := fmt.Sprintf("what ArgoCD app %s manages · %d %s from Git", o.GetName(), differs, verb)
	return func() tea.Msg { return pagerMsg{title: title, content: b.String()} }
}

// openNamed jumps to one object's list with it selected, so esc comes
// back here.
func (m *Model) openNamed(gvr schema.GroupVersionResource, ns, name string) tea.Cmd {
	res, ok := m.client.Resources.ByGVR(gvr)
	if !ok {
		return nil
	}
	if _, found := m.client.Cache.Get(gvr, ns, name); !found {
		if m.client.Cache.Synced(gvr) {
			m.setFlash("", fmt.Errorf("%s %s isn't there", strings.TrimSuffix(res.GVR.Resource, "s"), name))
			return nil
		}
		// Not loaded yet (secrets load when first opened): open the list,
		// and pick the row once it arrives.
		m.pendingSelect, m.pendingGVR = name, res.GVR
	}
	m.enterNamespace(ns)
	m.navigate(location{kind: locResource, res: res}, true) // selected, not filtered: one esc goes back
	m.tbl.touched = true
	m.selectNamed(name)
	return nil
}

// showConfig shows a ConfigMap's keys and values. Its binaryData keys
// are listed with their size, never their bytes.
func (m *Model) showConfig(row view.Row) tea.Cmd {
	data, _, _ := unstructured.NestedStringMap(row.Obj.Object, "data")
	bin := view.BinaryKeys(row.Obj)
	if len(data) == 0 && len(bin) == 0 {
		m.setFlash("", fmt.Errorf("%s has no values", row.Obj.GetName()))
		return nil
	}
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(data)) {
		v := data[k]
		if strings.Contains(v, "\n") {
			b.WriteString(styleInfoLabel.Render(k) + "\n" + v + "\n\n")
			continue
		}
		b.WriteString(styleInfoLabel.Render(k) + "  " + v + "\n")
	}
	for _, k := range bin {
		b.WriteString(styleInfoLabel.Render(k.Name) + "  binary data, " + k.Size + " (not shown)\n")
	}
	title := "contents of config map " + row.Obj.GetName() + " in " + row.Obj.GetNamespace()
	return func() tea.Msg { return pagerMsg{title: title, content: b.String()} }
}

// versionDiff shows how an older version's pod settings differ from the
// running one: image, env, resources, probes, anything in the template.
func (m *Model) versionDiff(ref kube.Ref, now, then *enrich.Version, back func() tea.Cmd) tea.Cmd {
	from, path := kube.ReplicaSets, []string{"spec", "template"}
	if ref.Resource.GVR.Resource != "deployments" { // StatefulSets and DaemonSets keep ControllerRevisions
		from, path = kube.ControllerRevisions, []string{"data", "spec", "template"}
	}
	gvr, ok := m.client.Resources.Served(from)
	if !ok || now == nil {
		return nil
	}
	template := func(name string) (string, bool) {
		rs, found := m.client.Cache.Get(gvr, ref.Namespace, name)
		if !found {
			return "", false
		}
		t, _, _ := unstructured.NestedMap(rs.Object, path...)
		t = runtime.DeepCopyJSON(t)
		delete(t, "$patch") // how a ControllerRevision says "replace", not a setting
		if labels, ok := t["metadata"].(map[string]any); ok {
			if l, ok := labels["labels"].(map[string]any); ok {
				delete(l, "pod-template-hash") // differs on every version, says nothing
			}
		}
		out, err := yaml.Marshal(t)
		return string(out), err == nil
	}
	a, ok1 := template(now.Name)
	b, ok2 := template(then.Name)
	if !ok1 || !ok2 {
		m.setFlash("", fmt.Errorf("those versions aren't loaded yet; try again in a moment"))
		return nil
	}
	diff := contextDiff(b, a, 3) // old to new, as in git
	title := fmt.Sprintf("revision %d → revision %d (running now) of %s", then.Revision, now.Revision, refWords(ref))
	content := fmt.Sprintf("Lines with - are revision %d (red); lines with + are what runs now (green).\n\n", then.Revision) + strings.Join(diff, "\n")
	if len(diff) == 0 {
		content = "Their pod settings are the same."
	}
	// esc comes back to the versions, where b goes back to this one.
	return func() tea.Msg { return pagerMsg{title: title, content: content, back: back, hang: 2, diff: true} } // wrapped: a cut line could hide what changed
}

// pausedRestartText is what a restart of rows says when some are paused
// Deployments: they restart only once resumed, so not "restarted".
// one names the row when there is only one ("deployment web in shop").
func pausedRestartText(text string, paused []string, rows int, one string) string {
	switch {
	case len(paused) == 0:
		return text
	case rows == 1:
		return "restart set for " + one + ": it happens once it is resumed"
	case len(paused) == rows: // none restarts yet
		return fmt.Sprintf("restart set for %d deployments: each happens once it is resumed (%s)", rows, strings.Join(paused, ", "))
	}
	// Some restarted now, the paused ones only once resumed: count apart.
	return fmt.Sprintf("restarted %s; restart set for %s: %s once resumed", view.Plural(rows-len(paused), "deployment"), strings.Join(paused, ", "),
		map[bool]string{true: "it happens", false: "each happens"}[len(paused) == 1])
}

// pausedDeploy: a Deployment whose rollouts are paused: a restart waits
// until it is resumed.
func pausedDeploy(o *unstructured.Unstructured) bool {
	paused, _, _ := unstructured.NestedBool(o.Object, "spec", "paused")
	return o.GetKind() == "Deployment" && paused
}

// restartWhy says what a restart does to the app's uptime, by kind, copies
// and update strategy (a set image or a go-back rolls out the same way).
func restartWhy(o *unstructured.Unstructured) string {
	copies, found, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")
	if !found {
		copies = 1
	}
	key := "updateStrategy"
	if o.GetKind() == "Deployment" {
		key = "strategy"
	}
	strategy, _, _ := unstructured.NestedString(o.Object, "spec", key, "type")
	// scaled is a rolling update's setting (a count or a percent of n),
	// as Kubernetes reads it, or def when unset.
	scaled := func(field string, n int64, roundUp bool, def string) int64 {
		v := intstr.FromString(def)
		switch x, _, _ := unstructured.NestedFieldNoCopy(o.Object, "spec", key, "rollingUpdate", field); x := x.(type) {
		case int64:
			v = intstr.FromInt32(int32(x))
		case string:
			v = intstr.FromString(x)
		}
		got, err := intstr.GetScaledValueFromIntOrPercent(&v, int(n), roundUp)
		if err != nil {
			return -1 // unreadable: say nothing it could break
		}
		return int64(got)
	}
	if strategy == "OnDelete" && (o.GetKind() == "StatefulSet" || o.GetKind() == "DaemonSet") {
		return "It replaces a pod only once that pod is deleted (its update strategy is OnDelete): nothing changes until you delete its pods."
	}
	switch o.GetKind() {
	case "Deployment":
		// "Stays up" only when it is up: with no copy ready (crashing,
		// none made yet) a rollout can't keep up what isn't.
		ready, _, _ := unstructured.NestedInt64(o.Object, "status", "readyReplicas")
		how := "a new pod starts before each old one stops, so it stays up."
		// As Kubernetes rolls a deployment: maxSurge rounds up, maxUnavailable
		// down, and both 0 means 1 may be unavailable.
		surge, unavail := scaled("maxSurge", copies, true, "25%"), scaled("maxUnavailable", copies, false, "25%")
		if surge == 0 && unavail == 0 {
			unavail = 1
		}
		switch {
		case strategy == "Recreate":
			how = "it stops all its pods before new ones start (its update strategy is Recreate), so it is down for a moment."
		case surge < 0 || unavail < 0:
			how = "new pods replace the old ones."
		case unavail >= copies && surge > 0: // new pods start too: they may be ready first
			how = "its update settings (maxUnavailable) let all its pods stop before new ones are ready, so it can be down for a moment."
		case unavail >= copies:
			how = "its update settings (maxUnavailable) let all its pods stop before new ones are ready, so it is down for a moment."
		case unavail > 0:
			how = fmt.Sprintf("up to %s may stop before new ones are ready, but never all, so it stays up.", view.Plural(int(unavail), "old pod"))
		}
		switch {
		case found && copies == 0:
			how = "it runs no copies, so no pod stops or starts until it is scaled up."
		case ready == 0:
			how = "no copy of it is ready now, so it isn't up; new pods replace the old ones."
		}
		if pausedDeploy(o) {
			return "It is paused: nothing restarts until it is resumed (kubectl rollout resume); then " + how
		}
		return strings.ToUpper(how[:1]) + how[1:]
	case "StatefulSet":
		partition, _, _ := unstructured.NestedInt64(o.Object, "spec", key, "rollingUpdate", "partition")
		// maxUnavailable: 1 unless set (rounded down, at least 1). A
		// cluster without the feature ignores it, so more than 1 is a bound.
		most := max(scaled("maxUnavailable", copies, false, "1"), 1)
		pace := "one at a time"
		if most > 1 {
			pace = fmt.Sprintf("one at a time (up to %d at once where the cluster honours its maxUnavailable setting)", min(most, copies))
		}
		switch {
		case found && copies == 0:
			return "It runs no copies, so no pod stops or starts until it is scaled up."
		case partition >= copies && partition > 0:
			return fmt.Sprintf("Its update partition (%d) holds back all its pods: none is replaced until it is lowered.", partition)
		case partition > 0:
			return fmt.Sprintf("Only its pods numbered %d and up are replaced (its update partition holds back the rest), %s, each stopping before its new one starts; the others keep running.", partition, pace)
		case copies <= 1:
			return "It runs 1 copy, which stops before its new one starts: it is down for a moment."
		case most >= copies:
			return "Pods are replaced, each stopping before its new one starts; where the cluster honours its maxUnavailable setting, all may stop at once, so it can be down for a moment."
		}
		return "Pods are replaced " + pace + ", each stopping before its new one starts; the others keep running."
	case "DaemonSet":
		if n, known, _ := unstructured.NestedInt64(o.Object, "status", "desiredNumberScheduled"); known && n == 0 {
			return "It runs on no node now, so no pod stops or starts."
		}
		if surge := scaled("maxSurge", 1, true, "0"); surge > 0 { // a percent of any node count rounds up to at least 1
			return "On each node a new pod starts before the old one stops, so it stays up."
		}
		return "Each node's pod stops before its new one starts: on that node it is down for a moment."
	}
	return "New pods replace the old ones one pod at a time."
}

// replaceWords says how a restart of several workloads replaces their
// pods: one at a time, unless one of them stops all its pods first
// (Recreate) or replaces them only when deleted (OnDelete); the confirm
// says which.
func replaceWords(rows []view.Row) string {
	if !slices.ContainsFunc(rows, func(r view.Row) bool { return replacesNow(r.Obj) }) {
		return "none of them replaces a pod now (the confirm says why for each)"
	}
	for _, r := range rows {
		if ownLine(r.Obj) {
			return "new pods replace the old ones (some differ or go down for a moment: the confirm says which)"
		}
	}
	return "new pods replace the old ones one pod at a time"
}

// ownLine: a restart of o needs its own words beside the rest: it
// stops all its pods first, replaces them only when deleted, holds some
// back (a partition), or goes down. Said on a line of its own.
func ownLine(o *unstructured.Unstructured) bool {
	why := restartWhy(o)
	return !oneAtATime(o) || strings.Contains(why, "partition") || strings.Contains(why, "down for a moment")
}

// oneAtATime: a restart of o replaces its pods in turn, not all at once
// (Recreate) or only when deleted by hand (OnDelete).
func oneAtATime(o *unstructured.Unstructured) bool {
	key := "updateStrategy"
	if o.GetKind() == "Deployment" {
		key = "strategy"
	}
	strategy, _, _ := unstructured.NestedString(o.Object, "spec", key, "type")
	return strategy != "Recreate" && strategy != "OnDelete"
}

// jobPodDeleteWords say what deleting a job's running pod does: the job
// counts it as a failed try and starts another only if it has tries left.
// Its logs (often the only record of why it failed) go with it.
func jobPodDeleteWords(job *unstructured.Unstructured) string {
	return "It is stopped and removed; " + jobNext(job) + ". Its logs go with it: l first to keep what it said."
}

// jobNext says whether a job starts another pod for one of its pods
// that is stopped: not once it has ended or while it is paused, and only
// while it has tries left (a stopped pod counts as a failed try).
func jobNext(job *unstructured.Unstructured) string {
	for _, typ := range []string{"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet"} {
		if c, ok := enrich.Condition(job, typ); ok && c.Status == "True" {
			return "job " + job.GetName() + " has already ended, so none starts again"
		}
	}
	if suspended, _, _ := unstructured.NestedBool(job.Object, "spec", "suspend"); suspended {
		return "job " + job.GetName() + " is paused, so none starts until it resumes"
	}
	if _, perIndex, _ := unstructured.NestedInt64(job.Object, "spec", "backoffLimitPerIndex"); perIndex {
		return "job " + job.GetName() + " may start it again if its index has tries left"
	}
	limit, found, _ := unstructured.NestedInt64(job.Object, "spec", "backoffLimit")
	if !found {
		limit = 6 // Kubernetes' default
	}
	failed, _, _ := unstructured.NestedInt64(job.Object, "status", "failed")
	if failed+1 <= limit {
		return "job " + job.GetName() + " counts it as a failed try and starts another (it has tries left)"
	}
	return "job " + job.GetName() + " has no tries left, so none starts again"
}

// shippedWhat is a :shipped row's WHAT: "deployment web" goes without
// saying, and a group reads "web (and 8 more)", as :changes says it, its
// name cut to fit width (0: never), never the count.
func shippedWhat(object string, width int) string {
	what := strings.TrimPrefix(strings.ReplaceAll(object, "/", " "), "deployment ")
	if i := strings.LastIndex(what, " and "); i > 0 && strings.HasSuffix(what, " more") {
		more := " (and " + what[i+5:] + ")"
		name := what[:i]
		if width > 0 {
			name = truncate(name, max(width-len(more), 8))
		}
		what = name + more
	}
	return what
}
