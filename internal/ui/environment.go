package ui

import (
	"context"
	"regexp"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// environment is what kubed believes the cluster is, and why.
//
//	cluster marker (ConfigMap kube-public/kubed)  →  names (context, cluster)
//	→  your earlier answer  →  ask once
type environment struct {
	known  bool
	prod   bool
	source string // "cluster", "name", "you"
}

// Answers saved in state.json.
const (
	answerProd    = "production"
	answerNonProd = "non-production"
)

// Words in a cluster's name that say what it is. Names are split into words
// at every non-letter, non-digit, so "devops-prod" is {devops, prod}: prod,
// not dev, and "acme-preprod" is preprod.
var (
	prodWords    = map[string]bool{"prod": true, "production": true, "prd": true}
	nonProdWords = map[string]bool{
		"stag": true, "stage": true, "staging": true, "stg": true, "preprod": true, "dev": true, "develop": true,
		"development": true, "test": true, "testing": true, "qa": true, "uat": true, "sandbox": true, "sbx": true,
		"nonprod": true, "demo": true, "local": true, "kind": true, "minikube": true,
	}
)

// envFromName decides from the context name and the kubeconfig cluster
// name (for EKS the ARN, which stays the same when the context is renamed).
//
// Patterns you set (--prod-pattern, $KUBED_PROD_PATTERN, config) come first
// and always win. Then the words in the names; a name with both kinds of
// word counts as production, the safe side.
func envFromName(prod, nonProd *regexp.Regexp, names ...string) environment {
	for _, n := range names {
		if n != "" && prod != nil && prod.MatchString(n) {
			return environment{known: true, prod: true, source: "setting"}
		}
	}
	for _, n := range names {
		if n != "" && nonProd != nil && nonProd.MatchString(n) {
			return environment{known: true, source: "setting"}
		}
	}
	isProd, isNonProd := false, false
	for _, n := range names {
		for _, w := range nameWords(n) {
			isProd = isProd || prodWords[w]
			isNonProd = isNonProd || nonProdWords[w]
		}
	}
	switch {
	case isProd:
		return environment{known: true, prod: true, source: "name"}
	case isNonProd:
		return environment{known: true, source: "name"}
	}
	return environment{}
}

// nameWords splits a name into lower-case words: "arn:aws:eks:…/acme-staging"
// → arn, aws, eks, …, acme, staging.
func nameWords(name string) []string {
	return strings.FieldsFunc(strings.ToLower(name), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
}

func envFromAnswer(answer, source string) environment {
	switch answer {
	case answerProd, "prod":
		return environment{known: true, prod: true, source: source}
	case "":
		return environment{}
	}
	return environment{known: true, source: source}
}

// markerMsg carries the cluster's own label, if it has one.
type markerMsg struct {
	context string
	env     string
	ok      bool
}

func (m *Model) readMarker() tea.Cmd {
	c, ctx := m.client, m.context
	return func() tea.Msg {
		cctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
		defer cancel()
		env, ok := c.MarkedEnvironment(cctx)
		return markerMsg{context: ctx, env: env, ok: ok}
	}
}

// resolveEnvironment runs after connecting: names and earlier answers now,
// the cluster marker when it arrives (it wins).
func (m *Model) resolveEnvironment() {
	m.env = envFromName(m.opts.ProdPattern, m.opts.NonProdPattern, m.context, m.client.Cluster)
	if !m.env.known && m.opts.State != nil {
		m.env = envFromAnswer(m.opts.State.Environments[m.client.Server], "you")
	}
}

func (m *Model) onMarker(msg markerMsg) {
	if msg.context != m.context || m.client == nil {
		return
	}
	if msg.ok {
		m.env = envFromAnswer(msg.env, "cluster")
		return
	}
	// Read-only can't change anything, so there is nothing to protect.
	if !m.env.known && !m.asked && !m.opts.ReadOnly {
		m.askEnvironment()
	}
}

// askEnvironment asks once which kind of cluster this is and remembers it.
func (m *Model) askEnvironment() {
	m.asked = true
	remember := func(answer string) func() tea.Cmd {
		return func() tea.Cmd {
			m.env = envFromAnswer(answer, "you")
			if m.opts.State != nil {
				m.opts.State.Environments[m.client.Server] = answer
				m.saveState()
			}
			return nil
		}
	}
	m.prompt = menuPrompt("Is "+shortContext(m.context)+" a production cluster?", []menuItem{
		{label: "Yes, production: ask me to type names before changes", run: remember(answerProd)},
		{label: "No, staging or dev: a y/n confirm is enough", run: remember(answerNonProd)},
		{label: "Not sure: ask me next time", run: func() tea.Cmd { return nil }},
	})
	m.prompt.warnings = []string{"kubed can't tell from the cluster or its name. Your answer is saved for this cluster only."}
	m.mode = modePrompt
}

// isProd: the cluster is production (red frame, PROD badge).
func (m *Model) isProd() bool { return m.env.known && m.env.prod }

// typedConfirm: changes need the name typed on production, and on
// clusters kubed couldn't identify (the safe side).
func (m *Model) typedConfirm() bool { return m.isProd() || !m.env.known }

// envBadge is shown in the header: PROD (cluster) / staging (name) / env?
func (m *Model) envBadge() string {
	switch {
	case m.client == nil:
		return ""
	case !m.env.known:
		return styleWarnBadge.Render("env?")
	case m.env.prod:
		return styleProd.Render("PRODUCTION")
	}
	return styleOK.Render("not production")
}

// sourceWords says how kubed decided the environment.
func sourceWords(source string) string {
	switch source {
	case "name":
		return "(judged by its name)"
	case "cluster":
		return "(set in the cluster)"
	case "you":
		return "(you said)"
	case "setting":
		return "(your setting)"
	}
	return "(" + source + ")"
}
