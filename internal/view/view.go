// Package view turns Kubernetes objects into table rows and detail lines.
// Each resource kubed knows about has one View; anything else falls back to
// the generic view.
package view

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Tone colours a row.
type Tone int

// Row tones, from plain to muted.
const (
	ToneNormal Tone = iota
	ToneOK
	ToneWarn
	ToneBad
	ToneDim
)

// Cap is an action a view supports beyond describe/yaml/edit/delete.
type Cap int

// Capabilities a view can declare.
const (
	CapLogs Cap = 1 << iota
	CapShell
	CapPortForward
	CapScale
	CapRestart
	CapKill       // force delete (pods)
	CapCordon     // cordon / uncordon / drain (nodes)
	CapTrigger    // run now (cronjobs)
	CapImage      // set container image (workloads)
	CapXray       // resource tree
	CapContainers // enter opens the pod's containers
	CapPodLogs    // logs of every pod the workload owns
)

// Row is one rendered table row.
type Row struct {
	Obj   *unstructured.Unstructured
	Cells []string
	Tone  Tone
}

// Drill is where Enter on a row leads: another resource, narrowed down.
type Drill struct {
	GVR   schema.GroupVersionResource
	Title string // shown in the header, e.g. "pods of deploy/payment-api"
	Match func(*unstructured.Unstructured) bool
	// AllNamespaces: the drill ignores the namespace you're showing (a
	// node's pods come from every namespace).
	AllNamespaces bool
}

// View describes how to show one resource.
type View struct {
	Columns []string
	// Row renders the cells (the NAMESPACE column is added by the UI).
	Row func(e *enrich.Enricher, obj *unstructured.Unstructured) ([]string, Tone)
	// Detail returns the context lines shown under the table.
	Detail func(e *enrich.Enricher, obj *unstructured.Unstructured) []string
	// Drill is optional.
	Drill func(obj *unstructured.Unstructured) *Drill
	// Time is what "sort by age" uses; defaults to creationTimestamp.
	Time func(obj *unstructured.Unstructured) time.Time
	// Optional columns hide, in this order, when the terminal is too narrow
	// for every heading (like k9s "wide" columns).
	Optional []string
	// Main is the column that keeps its width after the name, such as a
	// message or a status.
	Main string
	// Needs lists resources whose informers should be warm for enrichment.
	Needs []schema.GroupVersionResource
	Caps  Cap
}

// Has reports whether the view supports an action.
func (v View) Has(c Cap) bool { return v.Caps&c != 0 }

// For returns the view for a resource.
func For(res kube.Resource) View {
	key := res.GVR.GroupResource()
	if v, ok := registry[key]; ok {
		return v
	}
	return generic
}

var registry = map[schema.GroupResource]View{}

func register(gvr schema.GroupVersionResource, v View) {
	if v.Time == nil {
		v.Time = created
	}
	registry[gvr.GroupResource()] = v
}

// Rows renders objects; namespaced resources get a NAMESPACE column when
// allNamespaces is set.
func Rows(v View, e *enrich.Enricher, objs []*unstructured.Unstructured, showNamespace bool) []Row {
	rows := make([]Row, 0, len(objs))
	for _, o := range objs {
		cells, tone := v.Row(e, o)
		if showNamespace && len(cells) > 0 { // second: the name stays first
			cells = append([]string{cells[0], o.GetNamespace()}, cells[1:]...)
		}
		rows = append(rows, Row{Obj: o, Cells: cells, Tone: tone})
	}
	return rows
}

// Optional returns the view's hideable columns in the order they hide;
// NAMESPACE, when shown, hides last.
func Optional(v View, showNamespace bool) []string {
	if showNamespace {
		return append(append([]string(nil), v.Optional...), "NAMESPACE")
	}
	return v.Optional
}

// Header returns the column names matching Rows.
func Header(v View, showNamespace bool) []string {
	if showNamespace && len(v.Columns) > 0 {
		return append([]string{v.Columns[0], "NAMESPACE"}, v.Columns[1:]...)
	}
	return v.Columns
}

// --- helpers shared by views ---

func created(o *unstructured.Unstructured) time.Time { return o.GetCreationTimestamp().Time }

// Age formats the time since t like kubectl does ("3d4h", "12m").
func Age(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return span(time.Since(t))
}

// AgeAtMost is an upper bound on how long ago t was ("≤11m" for 10m53s,
// "≤1m" for 59s): its age rounded up to the unit Age shows it in, so
// "at most" holds.
// Kubernetes' times are whole seconds: less than one past is none.
func AgeAtMost(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t).Truncate(time.Second)
	unit := time.Minute // as span under 8h; under a minute too: "≤1m", not a bound to the second
	switch {
	case d >= 8*365*24*time.Hour:
		unit = 365 * 24 * time.Hour
	case d >= 8*24*time.Hour:
		unit = 24 * time.Hour
	case d >= 8*time.Hour:
		unit = time.Hour
	}
	if r := d % unit; r != 0 {
		d += unit - r
	}
	return "≤" + span(d)
}

// span says a duration briefly, in the largest units that read well:
// "45s", "12m", "2h3m", "13h", "3d".
func span(d time.Duration) string {
	switch {
	case d < 0:
		d = 0
		fallthrough
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 8*time.Hour: // from 8h, whole hours, as kubectl writes them
		if m := int(d.Minutes()) % 60; m != 0 {
			return fmt.Sprintf("%dh%dm", int(d.Hours()), m)
		}
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours())) // whole hours passed, as kubectl says
	case d >= 8*365*24*time.Hour: // eight years and up: years alone, as kubectl
		return fmt.Sprintf("%dy", int(d.Hours()/24/365))
	case d >= 730*24*time.Hour: // two years and up: years and days ("3y90d"), as kubectl
		days := int(d.Hours() / 24)
		if rest := days % 365; rest != 0 {
			return fmt.Sprintf("%dy%dd", days/365, rest)
		}
		return fmt.Sprintf("%dy", days/365)
	case d < 8*24*time.Hour: // a few days: the hours still matter ("2d5h", as kubectl)
		if h := int(d.Hours()) % 24; h != 0 {
			return fmt.Sprintf("%dd%dh", int(d.Hours())/24, h)
		}
	}
	return fmt.Sprintf("%dd", int(d.Hours())/24)
}

func age(o *unstructured.Unstructured) string { return Age(created(o)) }

func str(o *unstructured.Unstructured, fields ...string) string {
	s, _, _ := unstructured.NestedFieldNoCopy(o.Object, fields...)
	switch v := s.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func num(o *unstructured.Unstructured, fields ...string) int64 {
	v, _, _ := unstructured.NestedFieldNoCopy(o.Object, fields...)
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

func strs(o *unstructured.Unstructured, fields ...string) []string {
	v, _, _ := unstructured.NestedStringSlice(o.Object, fields...)
	return v
}

func joinOr(items []string, sep string) string {
	if len(items) == 0 {
		return "-"
	}
	return strings.Join(items, sep)
}

// cpu shows CPU in cores ("0.25", "2"): "m" would read like minutes.
func cpu(milli int64, known bool) string {
	switch {
	case !known:
		return "-"
	case milli == 0:
		return "0"
	case milli < 10:
		return "<0.01"
	case milli%1000 == 0:
		return fmt.Sprint(milli / 1000)
	}
	if milli < 10000 { // the same two decimals down a column: 0.20 next to 0.05
		return fmt.Sprintf("%.2f", float64(milli)/1000)
	}
	return fmt.Sprintf("%.1f", float64(milli)/1000)
}

// mem shows memory in Mi or Gi, as kubectl writes it.
func mem(bytes int64, known bool) string {
	switch {
	case !known:
		return "-"
	case bytes >= 10<<30:
		return fmt.Sprintf("%.0fGi", float64(bytes)/(1<<30))
	case bytes >= 1<<30: // 1Gi and up read better in Gi
		return fmt.Sprintf("%.1fGi", float64(bytes)/(1<<30))
	}
	return fmt.Sprintf("%dMi", bytes/(1<<20)) // Kubernetes' own units, as kubectl writes them
}

// pct is usage as a percent of a request, limit or allocatable; "n/a" when
// that amount is not set.
func pct(used, of int64, known bool) string {
	switch {
	case !known:
		return "-"
	case of <= 0:
		return "n/a"
	}
	return fmt.Sprint(enrich.Pct(used, of))
}

// cores formats millicores as cores ("7.9 cores"); gibibytes formats bytes
// as "14.3Gi". Used for node capacity, where Ki/m values are unreadable.
func cores(milli int64) string {
	if milli < 0 { // never expected; keep the plain float form
		return fmt.Sprintf("%.1f cores", float64(milli)/1000)
	}
	tenths := (milli + 50) / 100 // round half up in whole numbers: 850m is 0.9, not a float's 0.8
	return fmt.Sprintf("%d.%d cores", tenths/10, tenths%10)
}

func gibibytes(bytes int64) string { return fmt.Sprintf("%.1fGi", float64(bytes)/(1<<30)) }

// pctSign adds % to a percentage cell ("-" and "n/a" stay as they are).
func pctSign(s string) string {
	if s == "-" || s == "n/a" {
		return s
	}
	return s + "%"
}

// monthly is an hourly price as dollars a month: "$54".
func monthly(hourly float64, known bool) string {
	if !known {
		return "-"
	}
	return "$" + fmt.Sprintf("%.0f", hourly*hoursPerMonth)
}

// argoCell is the ARGO column for workloads: app name, or blank.
func argoCell(e *enrich.Enricher, o *unstructured.Unstructured) string {
	app, ok := e.ManagedBy(o)
	if !ok {
		return ""
	}
	return app.Name
}

// argoLine is the detail line about ArgoCD ownership.
func argoLine(e *enrich.Enricher, o *unstructured.Unstructured) string {
	if e.OwnersLoading(o) {
		return "ArgoCD   loading…"
	}
	app, ok := e.ManagedBy(o)
	switch {
	case !ok:
		return "" // most things aren't: saying so on every box is noise
	case !app.Found:
		return fmt.Sprintf("ArgoCD   app %s (its record can't be read here: another cluster, or no access; so how it syncs is unknown)", app.Name)
	default:
		return fmt.Sprintf("ArgoCD   app %s · %s", app.Name, argoWords(app))
	}
}

func syncPolicy(a enrich.ArgoApp) string {
	switch {
	case !a.AutoSync:
		return "manual sync"
	case a.SelfHeal:
		return "auto-sync, undoes manual edits"
	default:
		return "auto-sync"
	}
}

// storeWords names a secret store, or says "its store" when unknown.
func storeWords(store string) string {
	if store == "" {
		return "its store"
	}
	return store
}

func secretLine(s enrich.SecretSync) string {
	switch s.Ready {
	case "True":
		return fmt.Sprintf("Secret   %s ✓ last checked against %s %s ago (ExternalSecret %s; a routine check, not a change)", s.Target, storeWords(s.Store), Age(s.RefreshedAt), s.Name)
	case "False":
		line := fmt.Sprintf("Secret   %s ✗ can't update from %s (ExternalSecret %s)", s.Target, storeWords(s.Store), s.Name)
		if s.Message != "" {
			line += ": " + s.Message
		}
		return line
	}
	return fmt.Sprintf("Secret   %s from %s (ExternalSecret %s, not synced yet)", s.Target, storeWords(s.Store), s.Name)
}

// selectorMatch builds a Drill matcher from spec.selector.matchLabels.
func selectorMatch(ns string, labels map[string]string) func(*unstructured.Unstructured) bool {
	return func(o *unstructured.Unstructured) bool {
		if o.GetNamespace() != ns || len(labels) == 0 {
			return false
		}
		l := o.GetLabels()
		for k, v := range labels {
			if l[k] != v {
				return false
			}
		}
		return true
	}
}

func podsOf(o *unstructured.Unstructured, labelPath ...string) *Drill {
	labels, _, _ := unstructured.NestedStringMap(o.Object, labelPath...)
	if len(labels) == 0 {
		return nil
	}
	return &Drill{
		GVR:   kube.Pods,
		Title: fmt.Sprintf("of %s/%s", strings.ToLower(o.GetKind()), o.GetName()),
		Match: selectorMatch(o.GetNamespace(), labels),
	}
}

// agoOrNever is a past time as "3h ago", or "never".
func agoOrNever(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return Age(t) + " ago"
}

// Cores formats CPU (millicores) for people: "0.25 cores".
func Cores(milli int64) string {
	if milli == 1000 {
		return "1 core"
	}
	return cpu(milli, true) + " cores"
}

// Mem formats memory (bytes) in Kubernetes' units: "256Mi", "1.5Gi".
func Mem(bytes int64) string { return mem(bytes, true) }
