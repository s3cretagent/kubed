package enrich

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ObjectProblem says what is wrong with any object, from the conditions most
// controllers write (Ready/Available False, Degraded/Failed/Stalled True)
// and a few built-in kinds' own status. ok is false when nothing is.
//
// ArgoCD's health checks look at the same things, so this finds why ArgoCD
// calls an app unhealthy when its own record doesn't say.
func ObjectProblem(o *unstructured.Unstructured) (string, bool) {
	switch o.GetKind() {
	case "Deployment", "StatefulSet":
		want, found := number(o, "spec", "replicas")
		if !found {
			want = 1
		}
		ready, _ := number(o, "status", "readyReplicas")
		if ready < want {
			msg := fmt.Sprintf("%d of %d pods ready", ready, want)
			if c, ok := Condition(o, "Progressing"); ok && c.Reason == "ProgressDeadlineExceeded" {
				msg += "; the rollout gave up: " + c.Message
			}
			return msg, true
		}
		return "", false
	case "DaemonSet":
		want, _ := number(o, "status", "desiredNumberScheduled")
		ready, _ := number(o, "status", "numberReady")
		if ready < want {
			return fmt.Sprintf("%d of %d pods ready", ready, want), true
		}
		return "", false
	case "Application": // app of apps: a child app's own health
		if h := str(o, "status", "health", "status"); h == "Degraded" || h == "Missing" {
			return "ArgoCD app " + o.GetName() + " is " + strings.ToLower(h), true
		}
		return "", false
	case "PersistentVolumeClaim":
		if phase := str(o, "status", "phase"); phase == "Pending" || phase == "Lost" {
			return "volume claim is " + strings.ToLower(phase), true
		}
		return "", false
	}

	conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		typ, status := fmt.Sprint(m["type"]), fmt.Sprint(m["status"])
		if bad, _ := ConditionBad(typ, status); !bad {
			continue
		}
		msg := strings.Join(strings.Fields(fmt.Sprint(m["message"])), " ")
		if msg == "" || msg == "<nil>" {
			msg = fmt.Sprint(m["reason"])
		}
		return msg, true
	}
	return "", false
}

// ConditionBad says whether a condition in this state is a problem; known
// is false for a type kubed doesn't know, where "False" may well be fine
// (ScalingLimited, Reconciling).
func ConditionBad(typ, status string) (bad, known bool) {
	switch typ {
	case "Ready", "Available", "Synced", "Healthy", "Established", "Complete", "ResourcesUpToDate",
		"AbleToScale", "ScalingActive", "Active":
		return status == "False", true
	case "Degraded", "Failed", "Stalled", "Fallback", "ReplicaFailure", "InvalidSpec", "ErrorOccurred":
		return status == "True", true
	}
	return false, false
}

// number reads a whole number however it was decoded (int64 from the API,
// float64 from plain JSON).
func number(o *unstructured.Unstructured, path ...string) (int64, bool) {
	v, found, _ := unstructured.NestedFieldNoCopy(o.Object, path...)
	switch n := v.(type) {
	case int64:
		return n, found
	case int:
		return int64(n), found
	case float64:
		return int64(n), found
	}
	return 0, false
}
