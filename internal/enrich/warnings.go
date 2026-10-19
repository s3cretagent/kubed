package enrich

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Action is a write kubed can perform.
type Action string

// Write actions that can trigger a warning.
const (
	ActionDelete  Action = "delete"
	ActionScale   Action = "scale"
	ActionEdit    Action = "edit"
	ActionRestart Action = "restart"
)

// Warnings explains what a controller will do to undo an action. They are
// shown in the confirm prompt; the action is still allowed.
func (e *Enricher) Warnings(obj *unstructured.Unstructured, action Action) []string {
	var out []string
	if e.OwnersLoading(obj) && obj.GetKind() != "Node" { // ArgoCD never manages a node
		out = append(out, "Still loading owners: kubed cannot tell yet whether ArgoCD manages this.")
	} else if app, ok := e.ManagedBy(obj); ok {
		if w := argoWarning(app, obj, action); w != "" {
			out = append(out, w)
		}
	}
	if action == ActionScale || action == ActionEdit {
		// An autoscaler (an HPA; KEDA makes one too) sets the count itself:
		// a scale, or an edit of replicas, is undone.
		for _, h := range e.list(kube.HPAs, obj.GetNamespace()) {
			kind, _, _ := unstructured.NestedString(h.Object, "spec", "scaleTargetRef", "kind")
			name, _, _ := unstructured.NestedString(h.Object, "spec", "scaleTargetRef", "name")
			if kind == obj.GetKind() && name == obj.GetName() {
				lo, _, _ := unstructured.NestedInt64(h.Object, "spec", "minReplicas")
				hi, _, _ := unstructured.NestedInt64(h.Object, "spec", "maxReplicas")
				msg := fmt.Sprintf("Autoscaler %s sets the count between %d and %d by itself: it will change this back, except a scale to 0, which it leaves alone until you scale up again.", h.GetName(), max(lo, 1), hi)
				if action == ActionEdit {
					msg = fmt.Sprintf("Autoscaler %s sets its count (replicas) between %d and %d by itself: a change to replicas here is undone.", h.GetName(), max(lo, 1), hi)
				}
				if why, stuck := hpaCantAct(h); stuck { // as its box says: it isn't scaling now
					msg = fmt.Sprintf("Autoscaler %s can't act now%s, so it won't change this until it can.", h.GetName(), why)
				}
				out = append(out, msg)
			}
		}
	}
	if action == ActionDelete && obj.GetKind() == "Pod" {
		for _, b := range e.BudgetsAllowingNone(obj) {
			out = append(out, "Its disruption budget "+b+" allows none down now; a delete isn't held by it (only drains are), so this takes it below its minimum.")
		}
	}
	if obj.GetKind() == "Secret" {
		if es, ok := e.ExternalSecretOwner(obj); ok && (action == ActionEdit || action == ActionDelete) {
			out = append(out, fmt.Sprintf("ExternalSecret %s owns this Secret: it will rewrite it on the next refresh.", es.Name))
		}
	}
	return out
}

// hpaCantAct: an autoscaler that isn't scaling now (it can't read or
// change what it scales, or its metrics), so it leaves a count alone; why
// is " (it can't read its metrics)" when that is the reason. A target
// scaled to 0 (ScalingDisabled) isn't this: it stays off until scaled up.
func hpaCantAct(h *unstructured.Unstructured) (why string, ok bool) {
	if c, found := Condition(h, "AbleToScale"); found && c.Status == "False" {
		return "", true
	}
	c, found := Condition(h, "ScalingActive")
	if !found || c.Status != "False" || c.Reason == "ScalingDisabled" {
		return "", false
	}
	if strings.HasPrefix(c.Reason, "FailedGet") && strings.HasSuffix(c.Reason, "Metric") {
		return " (it can't read its metrics)", true
	}
	return "", true
}

func argoWarning(app ArgoApp, obj *unstructured.Unstructured, action Action) string {
	// Argo only reverts fields Git declares; a restart annotation is not one.
	// A deleted Pod is recreated by its controller, not by Argo.
	if action == ActionRestart || (action == ActionDelete && obj.GetKind() == "Pod") {
		return ""
	}
	switch {
	case !app.Found:
		return fmt.Sprintf("Managed by ArgoCD app %s (its record can't be read here: another cluster, or no access; so how it syncs is unknown).", app.Name)
	case !app.AutoSync:
		return fmt.Sprintf("Managed by ArgoCD app %s (manual sync): the next sync will undo this.", app.Name)
	case !app.SelfHeal:
		// Auto-sync without selfHeal only syncs again on a new Git commit.
		return fmt.Sprintf("ArgoCD app %s applies Git automatically: the next change in Git will undo this.", app.Name)
	case action == ActionDelete:
		return fmt.Sprintf("ArgoCD app %s undoes manual changes: it will create this again within seconds.", app.Name)
	case action == ActionScale:
		return fmt.Sprintf("ArgoCD app %s undoes manual changes: if Git sets the replica count, it will put it back.", app.Name)
	default:
		return fmt.Sprintf("ArgoCD app %s undoes manual changes: anything Git sets will be put back.", app.Name)
	}
}
