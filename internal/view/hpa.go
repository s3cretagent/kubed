package view

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Autoscalers: what each one watches, how far it can go, and where it is
// now, the way `kubectl get hpa` lays it out.
func init() {
	register(kube.HPAs, View{
		Columns:  []string{"NAME", "SCALES", "TARGETS", "MIN", "MAX", "NOW", "AGE"},
		Optional: []string{"AGE", "SCALES"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			minR := num(o, "spec", "minReplicas")
			if minR == 0 {
				minR = 1 // Kubernetes' default
			}
			maxR, now := num(o, "spec", "maxReplicas"), num(o, "status", "currentReplicas")
			tone := ToneNormal
			if maxR > 0 && now >= maxR && hpaWantsMore(o) || hpaProblem(o) != "" {
				tone = ToneWarn // at its ceiling and still short, or not scaling at all: to check
			}
			scales := strings.ToLower(str(o, "spec", "scaleTargetRef", "kind")) + "/" + str(o, "spec", "scaleTargetRef", "name")
			return []string{o.GetName(), scales, hpaTargets(o), fmt.Sprint(minR), fmt.Sprint(maxR), fmt.Sprint(now), age(o)}, tone
		},
		Detail: hpaDetail,
	})
}

// hpaGone is the Kubernetes message for a scale target that doesn't exist.
var hpaGone = regexp.MustCompile(`"[^"]+" not found`)

// hpaProblem says, in plain words, why an autoscaler can't do its job: it
// can't read or change what it scales, or it isn't scaling (its metrics
// can't be read). "" when neither (a target scaled to 0 on purpose isn't
// a problem).
func hpaProblem(o *unstructured.Unstructured) string {
	if c, ok := enrich.Condition(o, "AbleToScale"); ok && c.Status == "False" {
		kind, name := strings.ToLower(str(o, "spec", "scaleTargetRef", "kind")), str(o, "spec", "scaleTargetRef", "name")
		switch {
		case c.Reason == "FailedGetScale" && hpaGone.MatchString(c.Message) && name != "":
			return kind + " " + name + ", which it scales, doesn't exist, so it scales nothing"
		case c.Reason == "FailedUpdateScale":
			return "it can't change the size of what it scales: " + oneLine(c.Message)
		}
		return "it can't read what it scales: " + oneLine(c.Message)
	}
	if c, ok := enrich.Condition(o, "ScalingActive"); ok && c.Status == "False" && c.Reason != "ScalingDisabled" {
		if strings.HasPrefix(c.Reason, "FailedGet") && strings.HasSuffix(c.Reason, "Metric") {
			return "it isn't scaling: it can't read its metrics: " + oneLine(c.Message)
		}
		return "it isn't scaling: " + oneLine(c.Message)
	}
	return ""
}

// hpaDetail is the box: the plain problem in place of the conditions it
// comes from, then any other condition kubed knows is bad.
func hpaDetail(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	lines := conditionDetail(e, o)
	plain := hpaProblem(o)
	if plain == "" {
		return lines
	}
	out := []string{"Problem  " + plain}
	for _, l := range lines {
		if !strings.HasPrefix(l, "Problem  AbleToScale is") && !strings.HasPrefix(l, "Problem  ScalingActive is") {
			out = append(out, l)
		}
	}
	return out
}

// hpaTargets reads like kubectl's TARGETS: "cpu: 78%/300%", with "?" for a
// reading not in yet.
func hpaTargets(o *unstructured.Unstructured) string {
	specs, _, _ := unstructured.NestedSlice(o.Object, "spec", "metrics")
	current, _, _ := unstructured.NestedSlice(o.Object, "status", "currentMetrics")
	var out []string
	for i, sm := range specs {
		m, _ := sm.(map[string]any)
		typ, _ := m["type"].(string)
		if typ == "" {
			continue
		}
		key := strings.ToLower(typ[:1]) + typ[1:] // "Resource" -> "resource"
		spec, _ := m[key].(map[string]any)
		name := metricName(spec)
		target, _ := spec["target"].(map[string]any)
		want := targetText(target)
		have := "?"
		if i < len(current) {
			if cm, ok := current[i].(map[string]any); ok {
				part, _ := cm[key].(map[string]any)
				if cur, ok := part["current"].(map[string]any); ok {
					have = targetText(cur)
				}
			}
		}
		out = append(out, name+": "+have+"/"+want)
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ", ")
}

func metricName(spec map[string]any) string {
	if n, ok := spec["name"].(string); ok { // Resource, ContainerResource
		return n
	}
	if m, ok := spec["metric"].(map[string]any); ok { // Pods, Object, External (KEDA)
		if n, ok := m["name"].(string); ok {
			return n
		}
	}
	return "metric"
}

// targetText is one side of a target: a percentage, an average or a value.
func targetText(t map[string]any) string {
	switch {
	case t["averageUtilization"] != nil:
		return fmt.Sprintf("%v%%", t["averageUtilization"])
	case t["averageValue"] != nil:
		return fmt.Sprint(t["averageValue"])
	case t["value"] != nil:
		return fmt.Sprint(t["value"])
	}
	return "?"
}

// hpaWantsMore: the autoscaler says it is held back by its maximum.
func hpaWantsMore(o *unstructured.Unstructured) bool {
	c, ok := enrich.Condition(o, "ScalingLimited")
	return ok && c.Status == "True" && strings.Contains(c.Reason, "TooMany")
}
