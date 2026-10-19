package view

import (
	"strings"

	"github.com/s3cretagent/kubed/internal/enrich"
)

// reasonWords says common event reasons in plain words. Others are shown as
// Kubernetes wrote them.
var reasonWords = map[string]string{
	"BackOff":                      "crashing",
	"Unhealthy":                    "health check failed",
	"FailedMount":                  "can't attach volume",
	"FailedAttachVolume":           "can't attach volume",
	"FailedScheduling":             "no node can take it",
	"Scheduled":                    "placed on a node",
	"Pulling":                      "pulling image",
	"Pulled":                       "image pulled",
	"Failed":                       "failed",
	"ErrImagePull":                 "can't pull image",
	"Created":                      "container created",
	"Started":                      "container started",
	"Killing":                      "stopping",
	"Evicted":                      "pushed off its node",
	"OOMKilling":                   "out of memory",
	"SuccessfulCreate":             "created a pod",
	"SuccessfulDelete":             "removed a pod",
	"ScalingReplicaSet":            "scaled",
	"SuccessfulRescale":            "autoscaled",
	"FailedGetResourceMetric":      "no usage numbers",
	"SawCompletedJob":              "run ended",
	"InvalidSchedule":              "bad schedule",
	"Completed":                    "finished",
	"NodeNotReady":                 "node not answering",
	"NodeReady":                    "node ready",
	"RegisteredNode":               "node joined",
	"RemovingNode":                 "node leaving",
	"Sync":                         "synced",
	"UpdatedLoadBalancer":          "load balancer updated",
	"ProvisioningSucceeded":        "volume created",
	"ProvisioningFailed":           "volume not created",
	"FailedComputeMetricsReplicas": "autoscaler can't read usage",
	"FailedGetExternalMetric":      "autoscaler can't read its metric",
	"PolicyViolation":              "policy check failed",
	"BackoffLimitExceeded":         "job gave up",
	"DeadlineExceeded":             "job took too long",
	"NodeHasSufficientMemory":      "node memory fine",
	"NodeHasDiskPressure":          "node disk full",
	"NodeHasNoDiskPressure":        "node disk fine",
	"Rebooted":                     "node restarted",
	"NodeShutdown":                 "stopped: node shut down",
	"TriggeredScaleUp":             "asked for a new node",
	"NotTriggerScaleUp":            "no node pool can fit it",
	"ScaleDown":                    "node being removed",
	"SuccessfullyReconciled":       "reconciled",
	"ResourceUpdated":              "updated",
	"NoPods":                       "no pods",
	"SuccessfulAttachVolume":       "volume attached",
	"TaintManagerEviction":         "moved off a node",
	"Preempted":                    "pushed out by a higher priority pod",
	"FailedCreatePodSandBox":       "can't set up the pod network",
	"FailedKillPod":                "can't stop",
	"ExceededGracePeriod":          "didn't stop in time",
	"Nominated":                    "waiting for a node",
	"DisruptionBlocked":            "can't be moved now",
	"Unconsolidatable":             "kept (can't be merged)",
	"DisruptionTerminating":        "node being removed",
}

// ReasonWordsFor is ReasonWords for an event about kind: a CronJob creates
// jobs, not pods.
func ReasonWordsFor(reason, kind string) string {
	if kind == "CronJob" {
		switch reason {
		case "SuccessfulCreate":
			return "started a run"
		case "SuccessfulDelete":
			return "removed an old run"
		}
	}
	if kind == "Job" && reason == "SuccessfulCreate" {
		return "started a pod"
	}
	return ReasonWords(reason)
}

// EventWords is what an event says happened, in plain words: its reason,
// unless its message says otherwise. Kyverno files every check under
// PolicyViolation, the ones that pass too ("... pass: validation rule ...").
func EventWords(reason, kind, message string) string {
	if enrich.PolicyPassed(reason, message) {
		return "policy check passed"
	}
	if reason == "TaintManagerEviction" && strings.HasPrefix(message, "Cancelling deletion") {
		return "deletion called off" // the taint went before the pod was deleted: it stayed on its node
	}
	if reason == "BackOff" && strings.Contains(message, "pulling image") {
		return "can't download image" // waiting to try again: it never started, so not crashing
	}
	return ReasonWordsFor(reason, kind)
}

// ReasonWords is an event reason in plain words.
func ReasonWords(reason string) string {
	if w, ok := reasonWords[reason]; ok {
		return w
	}
	return reason
}

// KindWords shortens kind names that read badly in a sentence:
// "horizontalpodautoscaler auth" becomes "autoscaler auth".
func KindWords(s string) string {
	for long, short := range map[string]string{
		"horizontalpodautoscaler ": "autoscaler ",
		"persistentvolumeclaim ":   "volume claim ",
		"poddisruptionbudget ":     "disruption budget ",
	} {
		if strings.HasPrefix(s, long) {
			return short + strings.TrimPrefix(s, long)
		}
	}
	return s
}

// MessageWords says common event messages in plain words.
func MessageWords(msg string) string { return enrich.MessageWords(msg) }
