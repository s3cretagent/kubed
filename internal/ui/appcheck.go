package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// When ArgoCD calls an app unhealthy, the reason is in one of the resources
// it manages (often not a pod: a KEDA ScaledObject, a certificate…), and
// newer ArgoCD doesn't keep it in the Application record. kubed reads those
// resources for the selected app only, once every argoCheckTTL.
const (
	argoCheckTTL     = 2 * time.Minute
	maxArgoResources = 80 // reads per check
)

type argoCheck struct {
	check view.ArgoCheck
	at    time.Time
}

type argoCheckMsg struct {
	context, app string
	check        view.ArgoCheck
}

// argoLookup is a view.ArgoLookup over what has been read.
func (m *Model) argoLookup(app string) (view.ArgoCheck, bool) {
	c, ok := m.argoChecks[app]
	if !ok || c.at.IsZero() {
		return view.ArgoCheck{}, false
	}
	return c.check, true
}

// maxArgoChecks bounds how many failing apps are looked into at once.
const maxArgoChecks = 3

// argoCheckCmds reads the resources of apps ArgoCD calls unhealthy: the
// selected one first, then the others, so the WHY column and every box
// can name the part that fails.
func (m *Model) argoCheckCmds() tea.Cmd {
	if m.loc.kind != locHome || m.client == nil {
		return nil
	}
	var todo []*view.App
	if row, ok := m.tbl.selected(); ok {
		if a, ok := m.appFor(row); ok {
			todo = append(todo, a)
		}
	}
	todo = append(todo, m.apps...)
	var cmds []tea.Cmd
	for _, a := range todo {
		if len(cmds) == maxArgoChecks {
			break
		}
		if cmd := m.argoCheckCmd(a); cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	return tea.Batch(cmds...)
}

// argoCheckCmd looks into one app, unless it is healthy or was just checked.
func (m *Model) argoCheckCmd(a *view.App) tea.Cmd {
	if a.Argo == nil || (a.Argo.Health != "Degraded" && a.Argo.Health != "Missing") {
		return nil
	}
	if m.argoChecks == nil {
		m.argoChecks = map[string]argoCheck{}
	}
	if c, seen := m.argoChecks[a.Name]; seen && (c.at.IsZero() || time.Since(c.at) < argoCheckTTL) {
		return nil
	}
	m.argoChecks[a.Name] = argoCheck{} // in flight
	dyn, res, ctx, name := m.client.Dynamic, m.client.Resources, m.context, a.Name
	resources := a.Argo.Resources
	return func() tea.Msg {
		cctx, cancel := context.WithTimeout(context.Background(), 2*apiTimeout)
		defer cancel()
		var check view.ArgoCheck
		for i, r := range resources {
			if i == maxArgoResources {
				break
			}
			kind, ok := res.ByKind(r.Group, r.Kind)
			if !ok {
				continue
			}
			get := dyn.Resource(kind.GVR).Get
			if kind.Namespaced {
				get = dyn.Resource(kind.GVR).Namespace(r.Namespace).Get
			}
			o, err := get(cctx, r.Name, metav1.GetOptions{})
			msg, bad := "", false
			if err == nil {
				msg, bad = enrich.ObjectProblem(o)
			}
			switch {
			case apierrors.IsNotFound(err):
				check.Missing = append(check.Missing, r.String())
			case bad:
				check.Broken = append(check.Broken, r.String()+": "+msg)
			}
		}
		return argoCheckMsg{context: ctx, app: name, check: check}
	}
}

func (m *Model) onArgoCheck(msg argoCheckMsg) {
	if msg.context == m.context {
		m.argoChecks[msg.app] = argoCheck{check: msg.check, at: time.Now()}
	}
}

// A crashing app's box names the error its last run ended with: kubed
// reads the end of that run's logs for the selected app, once per crash.
type crashCheckMsg struct {
	context, key, text string
	failed             bool // the logs couldn't be read: try again later
}

// crashKey is one crash: a later restart is a new one to read.
func crashKey(a *view.App) string {
	return fmt.Sprintf("%s/%s/%s/%d", a.CrashNS, a.CrashPod, a.CrashContainer, a.CrashRestarts)
}

// crashLookup is a view.Lookups.Crash over what has been read.
func (m *Model) crashLookup(a *view.App) (string, bool) {
	text, ok := m.crashChecks[crashKey(a)]
	return text, ok
}

// crashCheckCmd reads the selected app's crashed run, if it has one not
// read yet.
func (m *Model) crashCheckCmd() tea.Cmd {
	if m.loc.kind != locHome || m.client == nil {
		return nil
	}
	row, ok := m.tbl.selected()
	if !ok {
		return nil
	}
	a, ok := m.appFor(row)
	if !ok || a.CrashPod == "" {
		return nil
	}
	key := crashKey(a)
	if t, busy := m.crashInFlight[key]; busy && time.Since(t) < crashRetry {
		return nil // being read, or read and found nothing a moment ago
	}
	if _, done := m.crashChecks[key]; done {
		return nil
	}
	if m.crashInFlight == nil {
		m.crashInFlight = map[string]time.Time{}
	}
	m.crashInFlight[key] = time.Now()
	client, ctx := m.client, m.context
	ns, pod, container := a.CrashNS, a.CrashPod, a.CrashContainer
	stopped := false
	if po, ok := client.Cache.Get(kube.Pods, ns, pod); ok {
		stopped = view.StoppedNow(po, container)
	}
	return func() tea.Msg {
		read := func(previous bool) []string {
			cctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
			defer cancel()
			out := make(chan string, 128)
			go client.StreamLogs(cctx, ns, pod, kube.LogOptions{Container: container, Previous: previous, TailLines: 100}, out)
			var lines []string
			for l := range out {
				lines = append(lines, l)
			}
			return lines
		}
		// Just stopped: the run that ended is its current log. Waiting to
		// restart: it's the previous one. Either way, try the other if
		// the first has nothing.
		lines := read(!stopped)
		text, ok := crashError(lines)
		if !ok || text == "" {
			t2, ok2 := crashError(read(stopped))
			if t2 != "" {
				text = t2
			}
			ok = ok || ok2
		}
		return crashCheckMsg{context: ctx, key: key, text: text, failed: !ok}
	}
}

// crashRetry: a crash whose logs couldn't be read is tried again after this.
const crashRetry = 20 * time.Second

func (m *Model) onCrashCheck(msg crashCheckMsg) {
	if msg.context != m.context || msg.failed {
		return // crashInFlight keeps when it was tried: crashRetry later, again
	}
	delete(m.crashInFlight, msg.key)
	if m.crashChecks == nil {
		m.crashChecks = map[string]string{}
	}
	m.crashChecks[msg.key] = msg.text
}

// exceptionLine is how most languages print the error that ended a run:
// "ValueError: …", "django.core.exceptions.ImproperlyConfigured: …",
// "panic: …", "Error: …", "Exception in thread …".
var exceptionLine = regexp.MustCompile(`^(?:[A-Za-z_][\w.$]*(?:Error|Exception|Exit|Configured|Failure)\b|panic|fatal error|Error|ERROR|FATAL|Exception in thread)[:\s]`)

// crashError picks the error a run ended with from its last lines: the
// last exception line, else the last line that reads as an error.
// Tracebacks written into JSON fields ("\\n  File …") are unpacked first.
func crashError(lines []string) (string, bool) {
	var flat []string
	jsonErr := "" // the message of the last JSON line logged at error level
	for _, l := range lines {
		if strings.HasPrefix(l, "kubed: ") { // couldn't read them
			return "", false
		}
		if msg, ok := jsonErrorMessage(l); ok {
			jsonErr = msg
			continue
		}
		// Escaped once per layer of JSON: undo a layer at a time.
		unescape := strings.NewReplacer(`\\`, `\`, `\n`, "\n", `\"`, `"`)
		for range 4 {
			next := unescape.Replace(l)
			if next == l {
				break
			}
			l = next
		}
		for _, part := range strings.Split(l, "\n") {
			p := strings.Trim(strings.TrimSpace(part), ",[]{} ")
			if strings.HasPrefix(p, `"`) { // a JSON string's quotes, not the message's
				p = strings.Trim(p, `", `)
			}
			if p != "" {
				flat = append(flat, p)
			}
		}
	}
	pick := ""
	for i := len(flat) - 1; i >= 0 && pick == ""; i-- {
		if exceptionLine.MatchString(flat[i]) {
			pick = flat[i]
		}
	}
	if pick == "" {
		pick = jsonErr
	}
	for i := len(flat) - 1; i >= 0 && pick == ""; i-- {
		if l := flat[i]; errorLine.MatchString(l) && !infoLine.MatchString(l) && !strings.HasPrefix(l, "File ") {
			pick = l
		}
	}
	if strings.Count(pick, `"`)%2 == 1 && pick != jsonErr { // a JSON string's closing quote left at the end
		pick = strings.TrimSuffix(pick, `"`)
	}
	if len(pick) > 240 {
		pick = pick[:240] + "…"
	}
	return pick, true
}

// jsonErrorMessage is the message of a JSON log line written at error
// level ({"level":"error","msg":"…"}), read as JSON rather than by trimming
// quotes. ok is false for other lines, and for ones carrying a traceback,
// which crashError unpacks itself.
func jsonErrorMessage(line string) (string, bool) {
	var obj map[string]any
	if !strings.HasPrefix(strings.TrimSpace(line), "{") || json.Unmarshal([]byte(line), &obj) != nil {
		return "", false
	}
	for _, k := range []string{"exc", "exc_info", "stack", "stacktrace", "traceback"} {
		if _, ok := obj[k]; ok {
			return "", false
		}
	}
	level := ""
	for _, k := range []string{"level", "severity", "lvl", "levelname"} {
		if v, ok := obj[k].(string); ok {
			level = strings.ToLower(v)
			break
		}
	}
	if level != "error" && level != "fatal" && level != "critical" && level != "panic" && level != "err" {
		return "", false
	}
	for _, k := range []string{"msg", "message", "error", "err"} {
		if v, ok := obj[k].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}
