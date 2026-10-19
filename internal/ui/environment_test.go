package ui

import (
	"regexp"
	"testing"

	"github.com/s3cretagent/kubed/internal/config"
)

func TestEnvironmentDetection(t *testing.T) {
	arn := "arn:aws:eks:ap-south-1:210987654321:cluster/acme-production"
	cases := []struct {
		name             string
		context, cluster string
		known, isProd    bool
	}{
		{"default context", arn, arn, true, true},
		{"renamed with --alias: the cluster ARN still says production", "acme-prd", arn, true, true},
		{"preprod is not prod", "acme-preprod", "", true, false},
		{"staging", "arn:aws:eks:ap-south-1:1:cluster/acme-staging", "", true, false},
		{"devops is not dev", "devops-prod", "", true, true},
		{"both kinds of word: production, to be safe", "prod-qa-mirror", "", true, true},
		{"globex-p is not guessed", "globex-p-1234-aps1-01-context", "p-1234-aps1-01", false, false},
	}
	for _, c := range cases {
		e := envFromName(nil, nil, c.context, c.cluster)
		if e.known != c.known || e.prod != c.isProd {
			t.Errorf("%s: got known=%v prod=%v", c.name, e.known, e.prod)
		}
	}
	// A pattern you set wins over kubed's own rules.
	if e := envFromName(regexp.MustCompile("staging"), nil, "acme-staging"); !e.prod {
		t.Error("--prod-pattern staging must make staging production")
	}
	if e := envFromName(nil, regexp.MustCompile("-p-"), "globex-p-1234"); !e.known || e.prod {
		t.Error("a non-prod pattern you set must apply")
	}
}

func TestUnknownClusterIsAskedOnceAndRemembered(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	m := testModel()
	defer m.client.Cache.Stop()
	m.opts.State = config.LoadState()
	m.context, m.client.Cluster, m.client.Server = "globex-p-1234", "p-1234", "https://abc.eks"

	m.resolveEnvironment()
	if m.env.known || !m.typedConfirm() {
		t.Fatal("unknown cluster must be unknown and still ask for typed names")
	}
	m.onMarker(markerMsg{context: m.context, ok: false})
	if m.prompt == nil || m.prompt.kind != promptMenu {
		t.Fatal("expected the ask-once prompt")
	}
	m.prompt.items[0].run() // "Yes, production"
	if !m.isProd() || m.env.source != "you" {
		t.Fatalf("answer not applied: %+v", m.env)
	}

	m2 := testModel()
	defer m2.client.Cache.Stop()
	m2.opts = m.opts
	m2.opts.State = config.LoadState() // fresh run reads the saved answer
	m2.context, m2.client.Cluster, m2.client.Server = m.context, m.client.Cluster, m.client.Server
	m2.resolveEnvironment()
	if !m2.isProd() {
		t.Fatal("answer was not remembered for the next run")
	}
}

func TestClusterMarkerWins(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	m.opts.ProdPattern = regexp.MustCompile("prod")
	m.context = "looks-like-prod"
	m.resolveEnvironment()
	m.onMarker(markerMsg{context: m.context, env: "staging", ok: true})
	if m.isProd() || m.env.source != "cluster" {
		t.Fatalf("marker should override the name guess: %+v", m.env)
	}
}
