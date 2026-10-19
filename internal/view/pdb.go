package view

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Disruption budgets: how many of their pods must stay up, and how many
// may be taken down now, as `kubectl get pdb` lays them out.
func init() {
	register(kube.PDBs, View{
		Columns:  []string{"NAME", "MIN AVAILABLE", "MAX UNAVAILABLE", "ALLOWED DOWN", "AGE"},
		Optional: []string{"AGE", "MAX UNAVAILABLE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			tone := ToneNormal
			if pdbBlocks(o) {
				tone = ToneWarn
			}
			allowed := "-"
			if _, found, _ := unstructured.NestedFieldNoCopy(o.Object, "status", "disruptionsAllowed"); found {
				allowed = fmt.Sprint(num(o, "status", "disruptionsAllowed"))
			}
			return []string{o.GetName(), enrich.Or(str(o, "spec", "minAvailable")), enrich.Or(str(o, "spec", "maxUnavailable")), allowed, age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			var lines []string
			if pdbBlocks(o) {
				fix := "it allows none down while all its pods are healthy: lower minAvailable (or raise maxUnavailable)"
				if !pdbNoneBySetting(o) { // more copies leave room only when it doesn't ask for all of them
					fix += ", or run more copies"
				}
				lines = append(lines, "Problem  no pod of it may be taken down now (drains wait on it)", "Fix      "+fix)
			}
			lines = append(lines, fmt.Sprintf("Covers   %s (%s)", plural(int(num(o, "status", "expectedPods")), "pod"), pdbSelector(o)))
			if _, found, _ := unstructured.NestedFieldNoCopy(o.Object, "status", "currentHealthy"); found {
				lines = append(lines, fmt.Sprintf("Healthy  %d of %d (needs %d)", num(o, "status", "currentHealthy"), num(o, "status", "expectedPods"), num(o, "status", "desiredHealthy")))
			}
			return append(lines, argoLine(e, o))
		},
		Needs: argoNeeds,
	})
}

// pdbBlocks: a budget, its status current, that allows no pod down though
// all the pods it expects are healthy: every drain of their nodes waits
// on it. (None allowed because a pod is down is it doing its job.)
func pdbBlocks(o *unstructured.Unstructured) bool {
	_, found, _ := unstructured.NestedFieldNoCopy(o.Object, "status", "disruptionsAllowed")
	return found && num(o, "status", "observedGeneration") >= o.GetGeneration() && num(o, "status", "disruptionsAllowed") == 0 &&
		num(o, "status", "expectedPods") > 0 && num(o, "status", "currentHealthy") >= num(o, "status", "desiredHealthy")
}

// pdbSelector is a budget's selector in words: "app=db", or "its
// selector" when it uses expressions.
func pdbSelector(o *unstructured.Unstructured) string {
	labels, _, _ := unstructured.NestedStringMap(o.Object, "spec", "selector", "matchLabels")
	exprs, _, _ := unstructured.NestedSlice(o.Object, "spec", "selector", "matchExpressions")
	if len(labels) == 0 || len(exprs) > 0 {
		return "by its selector"
	}
	var out []string
	for k, v := range labels {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// pdbNoneBySetting: a budget that allows no pod down however many it
// covers: maxUnavailable 0, or minAvailable 100%. (A minAvailable count
// equal to its pods allows one down once there is one more.)
func pdbNoneBySetting(o *unstructured.Unstructured) bool {
	if _, found, _ := unstructured.NestedFieldNoCopy(o.Object, "spec", "maxUnavailable"); found {
		return str(o, "spec", "maxUnavailable") == "0" || str(o, "spec", "maxUnavailable") == "0%"
	}
	return str(o, "spec", "minAvailable") == "100%"
}
