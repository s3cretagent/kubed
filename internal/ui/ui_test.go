package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

func rows(names ...string) []view.Row {
	out := make([]view.Row, len(names))
	for i, n := range names {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetName(n)
		out[i] = view.Row{Obj: u, Cells: []string{n}}
	}
	return out
}

func TestFilterRows(t *testing.T) {
	in := rows("payment-api", "auth-api", "payment-worker")
	if got := filterRows(in, "PAYMENT"); len(got) != 2 {
		t.Errorf("case-insensitive filter kept %d rows", len(got))
	}
	if got := filterRows(in, "!payment"); len(got) != 1 || got[0].Cells[0] != "auth-api" {
		t.Errorf("inverted filter = %v", got)
	}
}

func TestColumnWidthsFit(t *testing.T) {
	header := []string{"NAME", "MESSAGE"}
	r := []view.Row{{Cells: []string{"a-very-long-pod-name-that-goes-on-and-on", "an even longer message that will not fit anywhere at all"}}}
	w := columnWidths(header, r, 40, -1)
	if total := w[0] + w[1] + colGap; total > 40 {
		t.Errorf("widths %v exceed 40", w)
	}
}

func TestColumnWidthsKeepHeadings(t *testing.T) {
	header := []string{"NAMESPACE", "NAME", "NODE"}
	r := []view.Row{{Cells: []string{"auth", "auth-app-server-7f6bcc6f86-762n4", "ip-10-0-133-237.ap-south-1.compute.internal"}}}
	w := columnWidths(header, r, 50, -1)
	if w[0] < len("NAMESPACE") {
		t.Errorf("NAMESPACE column cut to %d while wide cells could shrink: %v", w[0], w)
	}
	if total := w[0] + w[1] + w[2] + 2*colGap; total > 50 {
		t.Errorf("widths %v exceed 50", w)
	}
}

func TestColumnWidthsKeepStatusReadable(t *testing.T) {
	header := []string{"NAME", "READY", "STATUS", "AGE"}
	r := []view.Row{{Cells: []string{"agent-data-gateway-mcp-server-6d4d4f6c9f-abcde", "1/1", "Running", "4h40m"}}}
	w := columnWidths(header, r, 50, -1)
	if w[2] < len("Running") {
		t.Errorf("STATUS cut to %d before NAME shrank: %v", w[2], w)
	}
}

func TestSetRowsKeepsSelection(t *testing.T) {
	var tb table
	tb.setRows([]string{"NAME"}, rows("a", "b", "c"), nil)
	tb.cursor = 1
	tb.setRows([]string{"NAME"}, rows("new", "a", "b", "c"), nil)
	if r, _ := tb.selected(); r.Obj.GetName() != "b" {
		t.Errorf("cursor moved to %q, want b", r.Obj.GetName())
	}
}

func TestParsePorts(t *testing.T) {
	cases := map[string][2]int{"8080:80": {8080, 80}, "9000": {9000, 9000}, "2000:2": {2000, 2}}
	for in, want := range cases {
		l, r, err := parsePorts(in)
		if err != nil || l != want[0] || r != want[1] {
			t.Errorf("parsePorts(%q) = %d, %d, %v", in, l, r, err)
		}
	}
	for _, bad := range []string{"", "a:b", "0:80", "70000:1", "80:80"} { // local ports under 1024 need admin rights
		if _, _, err := parsePorts(bad); err == nil {
			t.Errorf("parsePorts(%q) should fail", bad)
		}
	}
}

func TestShortContext(t *testing.T) {
	if got := shortContext("arn:aws:eks:ap-south-1:123456789012:cluster/acme-staging"); got != "acme-staging" {
		t.Errorf("shortContext = %q", got)
	}
	if got := shortContext("kind-dev"); got != "kind-dev" {
		t.Errorf("shortContext = %q", got)
	}
}

func TestVisibleColumnsDropOptionalFirst(t *testing.T) {
	tb := table{header: []string{"NAME", "READY", "STATUS", "IP", "NODE", "AGE"}, optional: []string{"IP", "NODE"},
		rows: []view.Row{{Cells: []string{"auth-app-server-7f6bcc6f86-762n4", "1/1", "Running", "10.0.134.235",
			"ip-10-0-133-237.ap-south-1.compute.internal", "114m"}}}}
	all := tb.visibleColumns(250)
	if len(all) != 6 {
		t.Fatalf("wide terminal hid columns: %v", all)
	}
	narrow := pick(tb.header, tb.visibleColumns(80))
	for _, h := range narrow {
		if h == "IP" {
			t.Errorf("IP should hide first on a narrow terminal: %v", narrow)
		}
	}
	if narrow[0] != "NAME" || narrow[len(narrow)-1] != "AGE" {
		t.Errorf("required columns dropped: %v", narrow)
	}
}

func TestDrawBoxHeightWithMultilineText(t *testing.T) {
	out := drawBox(40, 6, panel{title: "x", lines: []string{"error: line one\nline two\nline three\nfour\nfive\nsix"}})
	if got := len(strings.Split(out, "\n")); got != 6 {
		t.Fatalf("box is %d rows tall, want 6:\n%s", got, out)
	}
}

func TestSortArrowCountsTowardWidth(t *testing.T) {
	tb := table{sort: sortName} // the arrow only shows for a sort you chose
	tb.setRows([]string{"NAME", "IMAGE"}, []view.Row{{Obj: rows("web")[0].Obj, Cells: []string{"web", "busybox"}}}, nil)
	out := tb.render(60, 3)
	if !strings.Contains(out, "NAME↑") {
		t.Errorf("heading cut:\n%s", out)
	}
}

func TestWrapLines(t *testing.T) {
	long := "Secret shop-backend-app-secrets updated from aws-parameters-store 29m ago (ExternalSecret shop-backend-external-secret-0)"
	got := wrapLines(long, 40, 3)
	if len(got) < 3 || !strings.Contains(strings.Join(got, " "), "ExternalSecret") {
		t.Errorf("long line must wrap, not lose its end: %q", got)
	}
	for _, l := range got {
		if len(l) > 40 {
			t.Errorf("row wider than 40: %q", l)
		}
	}
	if got := wrapLines("short", 40, 3); len(got) != 1 {
		t.Errorf("short text wrapped: %q", got)
	}
}

func TestErrorsOnlyLogs(t *testing.T) {
	p := newPager("logs", strings.Join([]string{
		`GET /health 200`,
		`ERROR db timeout`,
		`{"level":"error","msg":"boom"}`,
		`level=info msg=ok`,
		`Traceback (most recent call last):`,
		`10.0.0.1 - "POST /pay HTTP/1.1" 502 12`,
		`warning: slow`,
	}, "\n"), 80, 10)
	p.isLogs = true
	p.errorsOnly = true
	got := p.visible()
	if len(got) != 4 {
		t.Fatalf("errors-only kept %d lines: %q", len(got), got)
	}
}

func TestARNParts(t *testing.T) {
	arn := "arn:aws:eks:ap-south-1:210987654321:cluster/prod"
	if arnAccount(arn) != "210987654321" || arnRegion(arn) != "ap-south-1" {
		t.Errorf("got %q %q", arnAccount(arn), arnRegion(arn))
	}
	if arnAccount("my-cluster") != "" || arnRegion("kind-kind") != "" {
		t.Error("non-ARN names have no account or region")
	}
}

func TestFmtPct(t *testing.T) {
	for in, want := range map[float64]string{27.699999999999996: "27.7%", 30: "30%", 0.04: "0%"} {
		if got := fmtPct(in); got != want {
			t.Errorf("fmtPct(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestFitHintsKeepsHelpAndWholeHints(t *testing.T) {
	h := []hint{{"enter", "Actions menu"}, {"w", "What changed"}, {"l", "Logs"}, {"?", "Help"}}
	got := fitHints(h, 30)
	if !strings.HasSuffix(ansi.Strip(got), "? help") || strings.Contains(ansi.Strip(got), "what") && !strings.Contains(ansi.Strip(got), "what changed") {
		t.Errorf("got %q", ansi.Strip(got))
	}
	if w := len([]rune(ansi.Strip(got))); w > 30 {
		t.Errorf("width %d > 30: %q", w, ansi.Strip(got))
	}
}

func TestMenuShowsWhatTheItemDoes(t *testing.T) {
	p := menuPrompt("app", []menuItem{{label: "Logs", key: "l", about: "Logs of every pod in this app, merged"}})
	if !strings.Contains(ansi.Strip(p.render(120)), "Logs of every pod in this app") {
		t.Error("the highlighted item's description is missing")
	}
}

func TestEveryActionSaysWhatItDoes(t *testing.T) {
	for _, a := range actions {
		if a.about == "" {
			t.Errorf("action %q (%s) has no description for the menu", a.label, a.key)
		}
	}
}

func TestShortESError(t *testing.T) {
	in := "error processing spec.data[0] (key: /a/b), err: key slackToken does not exist in secret /a/b"
	if got := shortESError(in); got != "key slackToken does not exist in secret /a/b" {
		t.Errorf("got %q", got)
	}
}

func TestNoCostsWithoutAnAWSAccount(t *testing.T) {
	off, on := strings.ToLower(helpTextFor(false)), strings.ToLower(helpTextFor(true))
	for _, money := range []string{"$", "cost", "save"} {
		if strings.Contains(off, money) {
			t.Errorf("help without costs mentions %q", money)
		}
	}
	if !strings.Contains(on, "save") {
		t.Error("help with costs should say what :waste saves")
	}
	h, rows := dropColumns(
		[]string{"APP", "READY", "COST/MO"},
		[]view.Row{{Cells: []string{"auth", "2/2", "$8"}}},
		moneyColumns)
	if strings.Join(h, ",") != "APP,READY" || strings.Join(rows[0].Cells, ",") != "auth,2/2" {
		t.Errorf("money columns must go: %v %v", h, rows[0].Cells)
	}
}

// A match on a long line that wraps over several rows must be on screen.
func TestSearchFindsWrappedMatch(t *testing.T) {
	var lines []string
	for i := range 40 {
		lines = append(lines, fmt.Sprintf("%02d %s", i, strings.Repeat("x", 120)))
	}
	lines = append(lines, "40 "+strings.Repeat("y", 150)+" Magrath")
	p := newPager("logs", strings.Join(lines, "\n"), 60, 10)
	p.setSearch("Magrath")
	if !p.wrap {
		t.Error("a match past the right edge should turn wrapping on")
	}
	if !strings.Contains(ansi.Strip(p.vp.View()), "Magrath") {
		t.Errorf("match not on screen:\n%s", ansi.Strip(p.vp.View()))
	}
}

func TestScaleRejectsSlips(t *testing.T) {
	for _, v := range []string{"abc", "-1", "", "4294967298", "1001"} {
		if validReplicas(v) == nil {
			t.Errorf("%q must be refused", v)
		}
	}
	for _, v := range []string{"0", "3", "1000"} {
		if err := validReplicas(v); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
	p := inputPrompt("How many?", "2", func(string) tea.Cmd { return nil })
	p.validate = validReplicas
	p.input.SetValue("abc")
	if _, done := p.update(tea.KeyMsg{Type: tea.KeyEnter}); done || p.problem == "" {
		t.Error("a bad number keeps the prompt open and says why")
	}
}

func TestLineDiff(t *testing.T) {
	a := "kind: Deployment\nspec:\n  replicas: 2\n  paused: false\n"
	b := "kind: Deployment\nspec:\n  replicas: 3\n  paused: false\n"
	got := lineDiff(a, b, 10)
	if strings.Join(got, "|") != "  -   replicas: 2|  +   replicas: 3" {
		t.Errorf("got %q", got)
	}
	if notes := dropEditNotes([]byte("# kubed: bad\n# kubed: fix it\nkind: Pod\n")); string(notes) != "kind: Pod\n" {
		t.Errorf("notes not dropped: %q", notes)
	}
}

func TestKindCounts(t *testing.T) {
	d := func(kind string) view.Row { o := view.Named("ns", "x"); o.SetKind(kind); return view.Row{Obj: o} }
	if got := kindCounts([]view.Row{d("Deployment")}); got != "1 deployment" {
		t.Errorf("got %q", got)
	}
	if got := kindCounts([]view.Row{d("Deployment"), d("Deployment"), d("StatefulSet")}); got != "2 deployments and 1 statefulset" {
		t.Errorf("got %q", got)
	}
}

func TestPrettyLog(t *testing.T) {
	got := ansi.Strip(prettyLog(`[dslpm] {"level":"info","msg":"Start processing","resource":"argocd/vector","time":"2026-10-06T14:45:12Z"}`))
	if !strings.HasPrefix(got, "[dslpm] ") || !strings.Contains(got, "INFO     Start processing  resource=argocd/vector") {
		t.Errorf("got %q", got)
	}
	if plain := "GET /health 200"; prettyLog(plain) != plain {
		t.Error("plain lines must stay as they are")
	}
}

func TestPrettyLogfmt(t *testing.T) {
	got := ansi.Strip(prettyLog(`[76nn8/repo-server] time="2026-10-06T14:46:13Z" level=info msg="started call" grpc.code=OK`))
	if !strings.HasPrefix(got, "[76nn8/repo-server] ") || !strings.Contains(got, "INFO     started call  grpc.code=OK") {
		t.Errorf("got %q", got)
	}
	if odd := `level=info this is not logfmt`; prettyLog(odd) != odd {
		t.Error("half key=value lines stay as they are")
	}
}

func TestPrettyLogfmtTrailingSpace(t *testing.T) {
	got := ansi.Strip(prettyLog("[0] level=info msg=\"done\" dest-name= ms=3 \r"))
	if !strings.Contains(got, "INFO     done") {
		t.Errorf("got %q", got)
	}
}

func TestShortenName(t *testing.T) {
	for _, c := range []struct {
		in   string
		w    int
		want string
	}{
		{"shop-backend-celery-69bcfc747d-nsxxx", 30, "shop-backend-celery-…-nsxxx"},
		{"auth", 30, "auth"},
	} {
		got := shortenName(c.in, c.w)
		if got != c.want || len([]rune(got)) > c.w {
			t.Errorf("shortenName(%q, %d) = %q (want %q)", c.in, c.w, got, c.want)
		}
	}
	if got := shortenName("shop-backend-catalogue-service-consumer-product-images-597f49cf6-xcpzq", 40); !strings.HasSuffix(got, "…-xcpzq") || strings.Count(got, "…") != 1 {
		t.Errorf("hash must go first, with one cut: %q", got)
	}
}

func TestPrettyLogPythonJSON(t *testing.T) {
	line := `{"@timestamp": "2026-10-06T14:19:13.241Z", "@version": 1, "source_host": "celery-x", "name": "worker.go", "levelname": "INFO", "levelno": 20, "message": "batch processed", "time": 18}`
	got := ansi.Strip(prettyLog(line))
	if !strings.Contains(got, "INFO     batch processed") || strings.Contains(got, "@version") || strings.Contains(got, "levelno") {
		t.Errorf("got %q", got)
	}
	if !strings.Contains(got, "time=18") || strings.HasPrefix(got, "18 ") {
		t.Errorf("a duration called time must stay a field: %q", got)
	}
}

func TestMergedLogLinesAreTidied(t *testing.T) {
	tag := logTagStyle(0).Render("argocd-server·kzsxg") + styleMuted.Render(" │")
	got := ansi.Strip(prettyLog(tag + ` time="2026-10-06T14:42:49Z" level=info msg="invalidated cache"`))
	if !strings.Contains(got, "argocd-server·kzsxg │") || !strings.Contains(got, "INFO     invalidated cache") {
		t.Errorf("got %q", got)
	}
	if got := podTag("shop-backend-celery-69bcfc747d-nsxxx"); got != "…op-backend-celery·nsxxx" { // the end tells workloads apart
		t.Errorf("podTag = %q", got)
	}
	if got := podTag("db-0"); got != "db-0" {
		t.Errorf("podTag = %q", got)
	}
}

func TestPrettyLogTimestamps(t *testing.T) {
	got := ansi.Strip(prettyLog("2026-10-06T14:42:49.169370923Z GET /health 200"))
	if strings.Contains(got, "2026-10-06T") || !strings.HasSuffix(got, " GET /health 200") {
		t.Errorf("got %q", got)
	}
}

// With t on, a line that has its own time doesn't show the time twice.
func TestPrettyLogTimestampOnce(t *testing.T) {
	ts := time.Date(2026, 10, 6, 14, 42, 49, 0, time.UTC)
	line := ts.Format(time.RFC3339Nano) + ` {"time":"` + ts.Format(time.RFC3339) + `","level":"info","msg":"hi"}`
	got := ansi.Strip(prettyLog(line))
	if n := strings.Count(got, ts.Local().Format("15:04:05")); n != 1 {
		t.Errorf("time shown %d times: %q", n, got)
	}
}

// Python logging's template ("%s: %s") gives way to the formatted message.
func TestPrettyLogTemplate(t *testing.T) {
	got := ansi.Strip(prettyLog(`{"levelname":"WARNING","msg":"%s: %s","message":"Forbidden: /shopauth/x"}`))
	if strings.Contains(got, "%s") || !strings.Contains(got, "Forbidden: /shopauth/x") {
		t.Errorf("got %q", got)
	}
}

// A plain line with its own leading time doesn't get the time twice with t.
func TestPrettyLogOwnLeadingTime(t *testing.T) {
	got := ansi.Strip(prettyLog("2026-10-06T17:50:09.123Z 2026-10-06T17:50:09Z INFO plain iso line"))
	if strings.Count(got, "Oct") > 1 {
		t.Errorf("date twice: %q", got)
	}
}

func TestPlainEditError(t *testing.T) {
	in := `dry run: Deployment in version "v1" cannot be handled as a Deployment: json: cannot unmarshal string into Go struct field DeploymentSpec.spec.replicas of type int32`
	if got := plainEditError(in); got != "spec.replicas must be a whole number, not text" {
		t.Errorf("got %q", got)
	}
}

// Cutting a huge text is quick (it once froze kubed when copying logs).
func TestTruncateHugeText(t *testing.T) {
	huge := strings.Repeat("a log line that goes on\n", 200000)
	start := time.Now()
	if got := truncate(huge, 60); lipgloss.Width(got) > 60 {
		t.Errorf("too wide: %d", lipgloss.Width(got))
	}
	if time.Since(start) > time.Second {
		t.Errorf("took %v", time.Since(start))
	}
}

// Coloured text is never cut by bytes: a short line with many colour codes
// stays whole, and a long one ends in "…" at exactly the width.
func TestTruncateColouredText(t *testing.T) {
	grey := func(s string) string { return "\x1b[38;2;158;158;158m" + s + "\x1b[0m" }
	var short strings.Builder
	for range 60 { // about 1500 bytes of codes, 60 cells of text
		short.WriteString(grey("a"))
	}
	if got := truncate(short.String(), 100); got != short.String() {
		t.Errorf("a 60-cell line was cut to fit 100: %q", ansi.Strip(got))
	}
	var long strings.Builder
	for i := range 400 {
		long.WriteString(grey(fmt.Sprintf("key%d=", i)) + "v ")
	}
	got := truncate(long.String(), 80)
	if w := ansi.StringWidth(got); w != 80 || !strings.HasSuffix(ansi.Strip(got), "…") {
		t.Errorf("long coloured line: width %d, ends %q; want 80 ending in …", w, ansi.Strip(got)[len(ansi.Strip(got))-5:])
	}
}

// Wrapping breaks at spaces, not inside hyphenated names.
func TestWrapWordsKeepsHyphenatedNames(t *testing.T) {
	got := wrapWords("moved to nodepool system-critical-spot-arm64 today", 30)
	for _, l := range strings.Split(got, "\n") {
		if strings.HasSuffix(l, "-") {
			t.Fatalf("broke at a hyphen:\n%s", got)
		}
	}
	if !strings.Contains(got, "system-critical-spot-arm64") {
		t.Fatalf("name split:\n%s", got)
	}
}

// A line marked info is not an error, whatever words it carries.
func TestErrorsOnlySkipsInfoLines(t *testing.T) {
	p := newPager("logs", strings.Join([]string{
		`time="2026-10-06T21:00:00Z" level=info msg="Trigger 'on-sync-failed' FAILED"`,
		`time="2026-10-06T21:00:01Z" level=error msg="sync failed"`,
		`INFO notifier: delivery failed for 0 recipients`,
		`2026-10-07 12:28:01,447 INFO Received API request: POST /payment/failed-callback/`,
	}, "\n"), 80, 10)
	p.isLogs, p.errorsOnly = true, true
	if got := p.visible(); len(got) != 1 || !strings.Contains(got[0], "level=error") {
		t.Fatalf("errors only kept %q", got)
	}
}

// A Secret's YAML for reading or copying never shows its values.
func TestHideSecretValues(t *testing.T) {
	in := `apiVersion: v1
kind: Secret
metadata:
  name: creds
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: '{"stringData":{"password":"s3cr3t"}}'
data:
  password: czNjcjN0
`
	got := hideSecretValues(kube.Ref{Resource: kube.Resource{Kind: "Secret"}, Name: "creds"}, in, false)
	if strings.Contains(got, "czNjcjN0") || strings.Contains(got, "s3cr3t") || !strings.Contains(got, "password: hidden (x shows the values)") {
		t.Fatalf("values leak:\n%s", got)
	}
}

// A wrapped tree line keeps the tree's lines and starts under its text.
func TestTreeWrapKeepsLines(t *testing.T) {
	got := treeWrap("│  ├─   Secret argocd-dex-server-tls  not there, and that's fine: it's marked optional", 50)
	lines := strings.Split(ansi.Strip(got), "\n")
	if len(lines) < 2 || !strings.HasPrefix(lines[1], "│  │    ") {
		t.Fatalf("continuation lost the tree:\n%s", ansi.Strip(got))
	}
}

func TestAgoToTime(t *testing.T) {
	at, ok := agoToTime("3h2m ago")
	if !ok || time.Since(at) < 3*time.Hour || time.Since(at) > 3*time.Hour+3*time.Minute {
		t.Fatalf("3h2m ago -> %v, %v", at, ok)
	}
	if _, ok := agoToTime("Running"); ok {
		t.Error("a status is not a time")
	}
}

// A 5xx answer is an error even on a line marked INFO.
func TestErrorsOnlyKeepsServerErrors(t *testing.T) {
	p := newPager("logs", strings.Join([]string{
		`2026-10-07 13:01:22,123 INFO request done path=/paynow status=500`,
		`2026-10-07 13:01:23,123 INFO Ignoring razorpay payment.failed callback`,
	}, "\n"), 80, 10)
	p.isLogs, p.errorsOnly = true, true
	if got := p.visible(); len(got) != 1 || !strings.Contains(got[0], "status=500") {
		t.Fatalf("errors only kept %q", got)
	}
}

// A typo in a field name is refused in words, not dropped.
func TestPlainEditErrorUnknownField(t *testing.T) {
	got := plainEditError(`dry run: Deployment in version "v1" cannot be handled as a Deployment: strict decoding error: unknown field "spec.3eplicas"`)
	if !strings.Contains(got, "spec.3eplicas: not a field Kubernetes knows") {
		t.Errorf("got %q", got)
	}
}

// Every path a route rule matches counts, not only the first.
func TestRoutePaths(t *testing.T) {
	rule := map[string]any{"matches": []any{
		map[string]any{"path": map[string]any{"value": "/rest/v1/joinussave"}},
		map[string]any{"path": map[string]any{"value": "/api"}},
		map[string]any{"path": map[string]any{"value": "/"}},
	}}
	got := routePaths(rule)
	if len(got) != 3 || got[0] != "/rest/v1/joinussave" || got[1] != "/api" || got[2] != "" {
		t.Errorf("got %q", got)
	}
	if got := routePaths(map[string]any{}); len(got) != 1 || got[0] != "" {
		t.Errorf("no matches is the whole site: %q", got)
	}
}

// A saved list writes when the last restart was, not "3m ago".
func TestRestartedAgoInFiles(t *testing.T) {
	got, ok := restartedAgo("7 (3m ago)")
	if !ok || !strings.HasPrefix(got, "7 (last ") || strings.Contains(got, "ago") {
		t.Errorf("restarts cell %q", got)
	}
	for _, cell := range []string{"7", "Running", "3m ago", "x (3m ago)"} {
		if _, ok := restartedAgo(cell); ok {
			t.Errorf("%q rewritten", cell)
		}
	}
}
