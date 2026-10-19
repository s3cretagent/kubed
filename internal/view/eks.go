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

func init() {
	register(kube.NodePools, View{
		Columns: []string{"NAME", "NODES", "COST/MO", "MAX CPU", "READY", "AGE"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			t := e.PoolCost()[o.GetName()]
			ready, tone := readyCond(o)
			cost := fmt.Sprintf("$%.0f", t.Hourly*hoursPerMonth)
			if t.Unpriced > 0 {
				cost += fmt.Sprintf(" (%d not priced)", t.Unpriced)
			}
			return []string{o.GetName(), fmt.Sprint(t.Nodes), cost,
				enrich.Or(str(o, "spec", "limits", "cpu")), ready, age(o)}, tone
		},
		Detail: func(_ *enrich.Enricher, o *unstructured.Unstructured) []string {
			reqs, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "requirements")
			var lines []string
			for _, r := range reqs {
				m, _ := r.(map[string]any)
				lines = append(lines, requirementWords(fmt.Sprint(m["key"]), fmt.Sprint(m["operator"]), m["values"]))
			}
			if policy := str(o, "spec", "disruption", "consolidationPolicy"); policy != "" {
				lines = append(lines, fmt.Sprintf("Shrinks  nodes that are %s, after %s", consolidationWords(policy), enrich.Or(str(o, "spec", "disruption", "consolidateAfter"))))
			}
			return lines
		},
		Drill: func(o *unstructured.Unstructured) *Drill {
			pool := o.GetName()
			return &Drill{GVR: kube.Nodes, Title: "in node pool " + pool, Match: func(n *unstructured.Unstructured) bool {
				return n.GetLabels()["karpenter.sh/nodepool"] == pool
			}}
		},
		Needs: []schema.GroupVersionResource{kube.Nodes},
	})

	register(kube.NodeClaims, View{
		Columns: []string{"NAME", "TYPE", "CAPACITY", "ZONE", "NODE", "READY", "AGE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			l := o.GetLabels()
			ready, tone := readyCond(o)
			return []string{o.GetName(), enrich.Or(l["node.kubernetes.io/instance-type"]), enrich.Or(l["karpenter.sh/capacity-type"]),
				enrich.Or(l["topology.kubernetes.io/zone"]), enrich.Or(str(o, "status", "nodeName")), ready, age(o)}, tone
		},
		Detail: conditionDetail,
	})

	register(kube.ExternalSecrets, View{
		Columns:  []string{"NAME", "STATUS", "LAST SYNC", "SECRET", "STORE", "EVERY"},
		Optional: []string{"EVERY", "STORE", "SECRET"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			s := enrich.ExternalSecret(o)
			tone := ToneNormal
			status := "Unknown"
			switch s.Ready {
			case "True":
				status = "Up to date"
			case "False":
				status, tone = "Error", ToneBad
			default:
				tone = ToneWarn
			}
			if s.Ready == "False" {
				status = "Not syncing"
			}
			return []string{s.Name, status, agoOrNever(s.RefreshedAt), s.Target, enrich.Or(s.Store), enrich.Or(shortInterval(s.Interval))}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			s := enrich.ExternalSecret(o)
			var lines []string
			if s.Ready == "False" {
				since := ""
				if t := conditionSince(o, "Ready"); !t.IsZero() {
					since = " since " + DateWords(t, false) + " (" + howLong(time.Since(t)) + ")"
				}
				lines = append(lines, "Problem  can't update secret "+s.Target+since)
				// The event says what is wrong ("key x does not exist"); the
				// status only that something is.
				if msg, _, ok := e.LatestWarning(o); ok {
					lines = append(lines, "Error    "+msg)
				} else if s.Message != "" {
					lines = append(lines, "Error    "+s.Message)
				}
			} else if s.Message != "" {
				lines = append(lines, "Message  "+s.Message)
			}
			return append(lines, argoLine(e, o))
		},
		Time:  func(o *unstructured.Unstructured) time.Time { return enrich.ExternalSecret(o).RefreshedAt },
		Needs: []schema.GroupVersionResource{kube.Applications, kube.Events}, // events: the real error
	})

	register(kube.Applications, View{
		Columns:  []string{"NAME", "PROJECT", "SYNC", "HEALTH", "POLICY", "REVISION", "AGE"},
		Optional: []string{"PROJECT", "REVISION", "POLICY"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			a := enrich.Application(o)
			tone := ToneNormal
			switch {
			case a.Health == "Degraded" || a.Health == "Missing":
				tone = ToneBad
			case a.Sync != "Synced" || a.Health != "Healthy":
				tone = ToneWarn
			}
			rev := a.Revision
			if len(rev) > 8 {
				rev = rev[:8]
			}
			return []string{a.Name, a.Project, syncWords(a.Sync), healthWords(a.Health), syncPolicy(a), enrich.Or(rev), age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			var lines []string
			for _, src := range enrich.Application(o).Sources {
				what := "path " + enrich.Or(src.Path)
				switch {
				case src.Chart != "":
					what = "chart " + src.Chart
				case src.Ref != "" && src.Path == "":
					what = "ref " + src.Ref
				}
				lines = append(lines, fmt.Sprintf("Source   %s  %s  @ %s", enrich.Or(src.RepoURL), what, enrich.Or(src.TargetRevision)))
			}
			lines = append(lines, fmt.Sprintf("Deploys  to %s, namespace %s",
				clusterWords(str(o, "spec", "destination", "server")+str(o, "spec", "destination", "name")),
				enrich.Or(str(o, "spec", "destination", "namespace"))))
			app := enrich.Application(o)
			if app.Sync == "OutOfSync" { // what differs, as home's WHY says it
				lines = append(lines, "Sync     out of sync: "+outOfSyncWhy(app))
			}
			if msg := str(o, "status", "operationState", "message"); msg != "" {
				// When the last sync ran, so "succeeded" can't read as "in sync now".
				when := "Last sync"
				if t, err := time.Parse(time.RFC3339, str(o, "status", "operationState", "finishedAt")); err == nil {
					when += " " + Age(t) + " ago (" + DateWords(t, true) + ")"
				}
				lines = append(lines, when+": "+strings.ToLower(str(o, "status", "operationState", "phase"))+": "+msg)
			}
			conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
			for _, c := range conds {
				m, _ := c.(map[string]any)
				lines = append(lines, fmt.Sprintf("Problem  %v: %v", m["type"], m["message"]))
			}
			// The parts that fail, as ArgoCD says, so "failing" says why.
			res, _, _ := unstructured.NestedSlice(o.Object, "status", "resources")
			shown := 0
			for _, r := range res {
				rm, _ := r.(map[string]any)
				h, _, _ := unstructured.NestedString(rm, "health", "status")
				if h == "" && rm["kind"] == "ExternalSecret" && e != nil { // ArgoCD gives these no health
					if es, ok := e.Get(kube.ExternalSecrets, fmt.Sprint(rm["namespace"]), fmt.Sprint(rm["name"])); ok {
						if sync := enrich.ExternalSecret(es); sync.Ready == "False" {
							h = "Degraded"
							rm = map[string]any{"kind": rm["kind"], "name": rm["name"], "health": map[string]any{"message": sync.Message}}
						}
					}
				}
				if h != "Degraded" && h != "Missing" {
					continue
				}
				if shown == 3 {
					lines = append(lines, "         … more: o lists everything it manages")
					break
				}
				msg, _, _ := unstructured.NestedString(rm, "health", "message")
				line := fmt.Sprintf("Problem  %s %v: %s", strings.ToLower(fmt.Sprint(rm["kind"])), rm["name"], strings.ToLower(healthWords(h)))
				if msg != "" {
					line += ": " + MessageWords(msg)
				}
				lines = append(lines, line)
				shown++
			}
			if h := app.Health; shown == 0 && (h == "Degraded" || h == "Missing" || h == "Unknown") {
				// No part marked failing: ArgoCD's own reason, else the
				// first part that isn't healthy.
				if msg := str(o, "status", "health", "message"); msg != "" {
					return append(lines, "Problem  "+MessageWords(msg))
				}
				for _, r := range res {
					rm, _ := r.(map[string]any)
					rh, _, _ := unstructured.NestedString(rm, "health", "status")
					if rh == "" || rh == "Healthy" {
						continue
					}
					line := fmt.Sprintf("Problem  %s %v: %s", strings.ToLower(fmt.Sprint(rm["kind"])), rm["name"], strings.ToLower(healthWords(rh)))
					if msg, _, _ := unstructured.NestedString(rm, "health", "message"); msg != "" {
						line += ": " + MessageWords(msg)
					}
					return append(lines, line)
				}
			}
			return lines
		},
	})
}

func readyCond(o *unstructured.Unstructured) (string, Tone) {
	c, ok := enrich.Condition(o, "Ready")
	switch {
	case !ok:
		return "-", ToneNormal
	case c.Status == "True":
		return "yes", ToneNormal
	case c.Status == "False":
		return "no", ToneWarn
	default:
		return "unknown", ToneWarn
	}
}

func conditionDetail(_ *enrich.Enricher, o *unstructured.Unstructured) []string {
	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	var lines []string
	for _, c := range conds {
		m, _ := c.(map[string]any)
		typ, status := fmt.Sprint(m["type"]), fmt.Sprint(m["status"])
		// Only what kubed knows is bad: "ScalingLimited is false" is fine,
		// and d still lists every condition as written.
		if bad, _ := enrich.ConditionBad(typ, status); bad {
			lines = append(lines, fmt.Sprintf("Problem  %s is %s: %v", typ, strings.ToLower(status), valueOr(m["message"])))
		}
	}
	return lines
}

// consolidationWords says which nodes Karpenter removes or replaces.
func consolidationWords(policy string) string {
	switch policy {
	case "WhenEmpty":
		return "empty"
	case "WhenEmptyOrUnderutilized", "WhenUnderutilized":
		return "empty or under-used"
	}
	return policy
}

// clusterWords names an ArgoCD destination: the in-cluster address is
// "this cluster".
func clusterWords(dest string) string {
	if dest == "https://kubernetes.default.svc" || dest == "in-cluster" {
		return "this cluster"
	}
	return enrich.Or(dest)
}

// requirementWords says a node pool rule plainly: "Buys     spot, on-demand".
func requirementWords(key, op string, values any) string {
	var vals []string
	if vs, ok := values.([]any); ok {
		for _, v := range vs {
			vals = append(vals, fmt.Sprint(v))
		}
	}
	list := strings.Join(vals, ", ")
	if op == "NotIn" {
		list = "anything but " + list
	}
	switch key {
	case "karpenter.sh/capacity-type":
		return "Buys     " + list
	case "kubernetes.io/arch":
		return "Chip     " + list
	case "node.kubernetes.io/instance-type":
		return "Machines " + list
	case "karpenter.k8s.aws/instance-family":
		return "Families " + list
	case "karpenter.k8s.aws/instance-category":
		return "Kinds    " + list
	case "topology.kubernetes.io/zone":
		return "Zones    " + list
	case "kubernetes.io/os":
		return "System   " + list
	}
	return fmt.Sprintf("Only     %s %s %s", key, strings.ToLower(op), list)
}

// shortInterval writes a Go duration the short way: "1h0m0s" is "1h",
// "30m0s" is "30m". Anything else is kept as written.
func shortInterval(s string) string {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return s
	}
	out := d.String()
	if strings.HasSuffix(out, "m0s") {
		out = strings.TrimSuffix(out, "0s")
	}
	if strings.HasSuffix(out, "h0m") {
		out = strings.TrimSuffix(out, "0m")
	}
	return out
}
