package ui

import (
	"regexp"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/s3cretagent/kubed/internal/kube"
)

// onProblemKey: while connecting or when the cluster can't be shown, only
// r (try again), c (pick another cluster), q and ? do anything.
func (m *Model) onProblemKey(msg tea.KeyMsg) (tea.Cmd, bool) {
	if m.loc.kind == locContexts {
		return nil, false
	}
	_, bad := m.problem()
	if !m.connecting && !bad {
		return nil, false
	}
	switch msg.String() {
	case "r":
		if bad {
			m.lastErr = ""
			return m.switchContext(m.context), true
		}
	case "c":
		m.flash = ""                                  // the problem's message belongs to the screen we leave
		m.navigate(location{kind: locContexts}, true) // esc comes back here
	case "q", "?":
		return nil, false
	}
	return nil, true
}

// problemShown: the connection-problem screen is what you see.
func (m *Model) problemShown() bool {
	if m.loc.kind == locContexts {
		return false
	}
	_, bad := m.problem()
	return bad
}

// slowConnect is when the connecting screen starts saying what may be wrong.
const slowConnect = 8 * time.Second

// connProblem is why kubed can't show the cluster, in words: what happened
// and what to do. ok is false when there is no problem.
type connProblem struct {
	what, fix, detail string
}

// problem returns the connection problem to show instead of a screen, if
// any: connecting failed, or the cluster refuses the first list.
func (m *Model) problem() (connProblem, bool) {
	if m.connErr != nil {
		return explainConnErr(m.connErr.Error(), m.lastErr, m.awsProfile()), true
	}
	if m.client == nil || m.loc.kind == locContexts {
		return connProblem{}, false
	}
	// Discovery can come from the disk cache, so a dead login only shows
	// when the first list fails.
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok || m.client.Cache.Synced(gvr) {
		return connProblem{}, false
	}
	if err := m.client.Cache.Err(gvr); err != nil {
		return explainConnErr(err.Error(), m.lastErr, m.awsProfile()), true
	}
	return connProblem{}, false
}

// awsProfile is the AWS profile the kubeconfig logs in with, if any.
func (m *Model) awsProfile() string {
	if m.client == nil {
		return ""
	}
	env, _, ok := m.client.AWSLogin()
	if !ok {
		return ""
	}
	for _, e := range env {
		if p, found := strings.CutPrefix(e, "AWS_PROFILE="); found {
			return p
		}
	}
	return ""
}

// missingProfile finds the name in "The config profile (x) could not be found".
var missingProfile = regexp.MustCompile(`config profile \(([^)]+)\) could not be found`)

// explainConnErr turns a client error (and the credential helper's last
// stderr line) into what happened and what to do.
func explainConnErr(err, stderr, profile string) connProblem {
	all := strings.ToLower(err + " " + stderr)
	login := "aws sso login"
	if profile != "" {
		login += " --profile " + profile
	}
	detail := strings.TrimSpace(stderr)
	if detail == "" {
		detail = err
	}
	switch {
	case strings.Contains(all, "context was not found") || strings.Contains(all, "does not exist"):
		return connProblem{what: "There is no such cluster in your kubeconfig.", fix: "Press c and pick one.", detail: err}
	case strings.Contains(all, "token has expired") || strings.Contains(all, "sso") && strings.Contains(all, "expired") ||
		strings.Contains(all, "expiredtoken") || strings.Contains(all, "refresh failed"):
		return connProblem{what: "Your AWS login has expired.", fix: "Run  " + login + "  in another terminal, then press r.", detail: detail}
	case strings.Contains(all, "config profile") && strings.Contains(all, "could not be found"):
		what := "The AWS profile your kubeconfig uses isn't in ~/.aws/config on this machine."
		if m := missingProfile.FindStringSubmatch(err + " " + stderr); m != nil {
			what = "The AWS profile " + m[1] + " (which your kubeconfig uses) isn't in ~/.aws/config on this machine."
		}
		return connProblem{what: what,
			fix: "Add it to ~/.aws/config (or fix the kubeconfig), then press r.", detail: detail}
	case strings.Contains(all, "executable") && strings.Contains(all, "not found"):
		return connProblem{what: "The login tool your kubeconfig needs isn't installed.", fix: "Install it (for EKS: the aws CLI), then press r.", detail: detail}
	case strings.Contains(all, "getting credentials") || strings.Contains(all, "unauthorized") || strings.Contains(all, "you must be logged in"):
		return connProblem{what: "The cluster didn't accept your login.", fix: "Log in again (" + login + "), then press r.", detail: detail}
	case strings.Contains(all, "forbidden"):
		return connProblem{what: "Your login can't list pods in this cluster.", fix: "Pick a namespace you can see with :ns <name>, or ask for access.", detail: detail}
	case strings.Contains(all, "no such host") || strings.Contains(all, "i/o timeout") || strings.Contains(all, "connection refused") ||
		strings.Contains(all, "context deadline exceeded") || strings.Contains(all, "network is unreachable"):
		if strings.Contains(all, "127.0.0.1") || strings.Contains(all, "localhost") {
			return connProblem{what: "Nothing is answering at the local address this cluster uses.",
				fix: "It's usually a tunnel or port-forward that isn't running. Start it, then press r.", detail: detail}
		}
		return connProblem{what: "Can't reach the cluster.", fix: "Check your VPN or network, then press r.", detail: detail}
	}
	return connProblem{what: "Can't connect to the cluster.", fix: "Press r to try again, or c to pick another cluster.", detail: detail}
}

// problemLines renders a problem for the main box, wrapped to width.
func problemLines(p connProblem, width int) []string {
	// The keys are in the footer; the box says what is wrong and what to do.
	var lines []string
	for _, l := range wrapLines(p.what, max(width, 20), 4) {
		lines = append(lines, styleErr.Render(l))
	}
	lines = append(lines, "")
	for _, l := range wrapLines(p.fix, max(width, 20), 6) {
		lines = append(lines, styleValue.Render(l))
	}
	lines = append(lines, "")
	for _, l := range wrapLines("Details: "+p.detail, max(width, 20), 6) {
		lines = append(lines, styleMuted.Render(l))
	}
	return lines
}

// connectingLines is the connecting screen; after a while it says what is
// usually wrong.
func (m *Model) connectingLines() []string {
	lines := []string{styleMuted.Render("Connecting to " + shortContext(m.context) + "…")}
	if time.Since(m.connectStart) > slowConnect {
		lines = append(lines, "", styleWarn.Render("This is taking long. Usually it's the VPN, or an AWS login that needs renewing (aws sso login)."),
			styleMuted.Render("Keep waiting, or press c to pick another cluster, q to quit."))
	}
	return lines
}
