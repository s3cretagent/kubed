package ui

import (
	"slices"
	"sort"
	"strings"

	"github.com/s3cretagent/kubed/internal/kube"
)

// maxSuggestions is how many completions the ":" box shows.
const maxSuggestions = 6

// suggestion is one completion for the ":" command bar.
type suggestion struct {
	name   string // what gets typed
	show   string // how to show it, when shorter than name
	detail string // short names / what it does
}

// commands are kubed's own ":" commands.
var commands = []suggestion{
	{name: "apps", detail: "your apps, problems first  (gh)"},
	{name: "ns", detail: "show one namespace: :ns <name>, or :ns all"},
	{name: "ctx", detail: "switch to another cluster"},
	{name: "changes", detail: "what changed in the last day, in the namespace you're in"},
	{name: "shipped", detail: "new versions that went out in the last 7 days (restarts left out)"},
	{name: "waste", detail: "what workloads reserve vs what they use"},
	{name: "pf", detail: "local ports you opened (port-forwards)"},
	{name: "helm", detail: "Helm releases  (gH)"},
	{name: "help", detail: "every key  (?)"},
	{name: "mouse", detail: "turn clicking rows on or off (off: select text freely)"},
	{name: "q", detail: "quit"},
}

// commonKinds are offered when nothing is typed yet, after the commands,
// so the lists people want most are there without knowing their names.
var commonKinds = []suggestion{
	{name: "pods", detail: "every pod  (gp)"},
	{name: "deployments", detail: "what runs your pods  (gd)"},
	{name: "services", detail: "how pods are reached inside the cluster  (gs)"},
	{name: "ingresses", detail: "web addresses  (gi, unless routes are used more)"},
	{name: "nodes", detail: "the machines  (gn)"},
	{name: "cronjobs", detail: "jobs on a schedule  (gC)"},
	{name: "secrets", detail: "passwords and keys  (gS)"},
	{name: "configmaps", detail: "settings  (gc)"},
}

// synonyms are words people type for a list kubed calls something else.
var synonyms = map[string]string{
	"cron":     "cronjobs",
	"cordon":   "nodes",
	"drain":    "nodes",
	"node":     "nodes",
	"top":      "pods",
	"history":  "changes",
	"releases": "helm",
	"ingress":  "ingresses",
	"urls":     "ingresses",
	"hosts":    "ingresses",
	"rollout":  "deployments",
	"rollback": "deployments",
	"scale":    "deployments",
	"logs":     "pods",
	"shell":    "pods",
	"exec":     "pods",
	"cost":     "waste",
	"costs":    "waste",
}

// jumps are the "g" + key shortcuts, keyed by the second key.
var jumps = map[string]string{
	"p": "pods",
	"d": "deployments",
	"s": "services",
	"n": "nodes",
	"e": "events",
	"a": "applications.argoproj.io",
	"x": "externalsecrets.external-secrets.io",
	"N": "nodepools.karpenter.sh",
	"c": "configmaps",
	"S": "secrets",
	"i": "ingresses",
	"j": "jobs",
	"C": "cronjobs",
}

// resourceName is how a resource is typed: its plural, or plural.group when
// another group serves the same plural (events vs events.k8s.io).
func resourceName(res kube.Resource, all []kube.Resource) string {
	for _, o := range all {
		if o.GVR.Resource == res.GVR.Resource && o.GVR.Group != res.GVR.Group && res.GVR.Group != "" {
			return res.GVR.Resource + "." + res.GVR.Group
		}
	}
	return res.GVR.Resource
}

// resourceAliases are the other names a resource answers to.
func resourceAliases(res kube.Resource) []string {
	var out []string
	for alias, target := range aliases {
		if target == res.GVR.Resource+"."+res.GVR.Group || (res.GVR.Group == "" && target == res.GVR.Resource) {
			out = append(out, alias)
		}
	}
	out = append(out, res.ShortNames...)
	sort.Strings(out)
	return slices.Compact(out)
}

// argSuggestions completes the argument of a command (":ns pay" → payments).
func (m *Model) argSuggestions(cmd, typed string, names []string, what string) []suggestion {
	var out []suggestion
	for _, n := range names {
		if strings.Contains(strings.ToLower(n), typed) {
			out = append(out, suggestion{name: cmd + n, detail: what})
			if len(out) == maxSuggestions {
				break
			}
		}
	}
	return out
}

// namespaceNames lists the namespaces kubed has loaded (all namespaces first).
func (m *Model) namespaceNames() []string {
	names := []string{"all"}
	if m.client == nil {
		return names
	}
	gvr, ok := m.client.Resources.Served(kube.Namespaces)
	if !ok {
		return names
	}
	m.client.Cache.Watch(gvr)
	for _, o := range m.client.Cache.List(gvr, "") {
		names = append(names, o.GetName())
	}
	sort.Strings(names[1:])
	return names
}

// suggest returns completions for what has been typed after ":". Only the
// first word completes; once there is a space the user is typing an
// argument.
func (m *Model) suggest(typed string) []suggestion {
	typed = strings.ToLower(typed)
	switch {
	case typed == "": // nothing typed yet: kubed's commands, then common lists
		// Commands, then common lists, quit last.
		out := append(append([]suggestion(nil), commands[:len(commands)-1]...), commonKinds...)
		if room := m.height/2 - inputBoxLines - 2; room > 0 && len(out) > room {
			out = out[:room]
		}
		out = append(out, suggestion{name: "q", detail: "quit   ·   ctrl-a lists every kind your cluster has"})
		return out
	case strings.HasPrefix(typed, "ns "):
		out := m.argSuggestions("ns ", strings.TrimPrefix(typed, "ns "), m.namespaceNames(), "")
		for i := range out {
			switch strings.TrimPrefix(out[i].name, "ns ") {
			case "all":
				out[i].detail = "every namespace"
			case m.namespace:
				out[i].detail = "✓ showing now"
			}
		}
		return out
	case strings.HasPrefix(typed, "ctx ") && m.kc != nil:
		names, _ := m.kc.Contexts()
		out := m.argSuggestions("ctx ", strings.TrimPrefix(typed, "ctx "), names, "")
		for i := range out {
			name := strings.TrimPrefix(out[i].name, "ctx ")
			out[i].show = "ctx " + strings.TrimSuffix(shortContext(name), "-context")
			if env := m.contextEnv(name); env.known && env.prod { // as the contexts list says it
				out[i].detail = "production"
			}
			if name == m.context {
				out[i].detail = strings.TrimSpace(out[i].detail + "  ✓ you're here")
			}
		}
		return out
	case strings.Contains(typed, " "):
		return nil
	}
	type ranked struct {
		s    suggestion
		rank int
	}
	var found []ranked
	add := func(s suggestion, names ...string) {
		best := -1
		for _, n := range names {
			n = strings.ToLower(n)
			switch {
			case n == typed:
				best = 0
			case strings.HasPrefix(n, typed) && (best < 0 || best > 1):
				best = 1
			}
		}
		if best >= 0 {
			found = append(found, ranked{s, best})
		}
	}
	for _, c := range commands {
		add(c, c.name)
	}
	for word, target := range synonyms {
		if strings.HasPrefix(word, typed) && (len(typed) >= 3 || (len(typed) == 2 && (word == "urls" || word == "hosts"))) {
			add(suggestion{name: target, detail: "for \"" + word + "\""}, word)
		}
	}
	// Web addresses live in ingresses or in Gateway routes: offer routes
	// too, first when the cluster has more of them.
	if m.client != nil && len(typed) >= 2 && (startOf(typed, "urls") || startOf(typed, "hosts")) {
		if gvr, ok := m.client.Resources.Served(kube.HTTPRoutes); ok {
			m.client.Cache.Watch(gvr) // to count them; opening either list needs it anyway
			if ing, ok := m.client.Resources.Served(kube.Ingresses); ok {
				m.client.Cache.Watch(ing)
			}
			rank := 1
			if m.routesFirst() {
				rank = -1
			}
			found = append(found, ranked{suggestion{name: "httproutes", detail: "for \"" + typed + "\": web addresses through the gateway"}, rank})
		}
	}
	if m.client != nil {
		all := m.client.Resources.All()
		for _, res := range all {
			name := resourceName(res, all)
			al := resourceAliases(res)
			add(suggestion{name: name, detail: strings.Join(al, " ")}, append([]string{name, res.Singular}, al...)...)
		}
	}
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].rank != found[j].rank {
			return found[i].rank < found[j].rank
		}
		return len(found[i].s.name) < len(found[j].s.name)
	})
	out := make([]suggestion, 0, maxSuggestions)
	seen := map[string]bool{}
	for _, f := range found {
		if len(out) == maxSuggestions {
			break
		}
		if seen[f.s.name] {
			continue
		}
		seen[f.s.name] = true
		out = append(out, f.s)
	}
	return out
}

// startOf: what you typed is the start of word ("ur" of "urls").
func startOf(typed, word string) bool { return strings.HasPrefix(word, typed) }

// routesFirst: web addresses here are Gateway routes rather than
// ingresses (the namespace's own kind when it has only one, else what the
// cluster mostly uses), so gi and :urls open routes.
func (m *Model) routesFirst() bool {
	if m.client == nil {
		return false
	}
	routes, ok := m.client.Resources.Served(kube.HTTPRoutes)
	if !ok {
		return false
	}
	ing, ok := m.client.Resources.Served(kube.Ingresses)
	if !ok {
		return true
	}
	// The namespace web addresses open in: one that has only one kind
	// gets that kind, whatever the cluster mostly uses.
	if ns := m.chosenNS; ns != "" {
		r, i := len(m.client.Cache.List(routes, ns)), len(m.client.Cache.List(ing, ns))
		switch {
		case r > 0 && i == 0:
			return true
		case i > 0 && r == 0:
			return false
		}
	}
	return len(m.client.Cache.List(routes, "")) > len(m.client.Cache.List(ing, ""))
}
