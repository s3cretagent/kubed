package enrich

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Cond is one entry of status.conditions.
type Cond struct {
	Type, Status, Reason, Message string
	Updated                       time.Time // lastUpdateTime, when the condition has one
}

// Condition finds a status condition by type.
func Condition(obj *unstructured.Unstructured, condType string) (Cond, bool) {
	conds, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != condType {
			continue
		}
		str := func(k string) string { s, _ := m[k].(string); return s }
		updated, _ := time.Parse(time.RFC3339, str("lastUpdateTime"))
		return Cond{Type: condType, Status: str("status"), Reason: str("reason"), Message: str("message"), Updated: updated}, true
	}
	return Cond{}, false
}

// IsWarning reports an event that warns of something. Kubernetes types Kyverno's
// passing checks as warnings too ("... pass: validation rule ... passed").
func IsWarning(ev *unstructured.Unstructured) bool {
	return str(ev, "type") == "Warning" && !PolicyPassed(str(ev, "reason"), str(ev, "message"))
}

// PolicyPassed reports a Kyverno PolicyViolation event that is a pass.
func PolicyPassed(reason, message string) bool {
	return reason == "PolicyViolation" && strings.Contains(message, " pass: ") && !strings.Contains(message, " fail: ")
}
