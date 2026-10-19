package enrich

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	digest      = regexp.MustCompile(`@sha256:[0-9a-f]{12,}`)
	probeFailed = regexp.MustCompile(`^(Readiness|Liveness|Startup) probe failed: (?:Get|HTTP probe failed).*?"https?://[^:/"]+:(\d+)([^"]*)":\s*(.*)$`)
	probeTCP    = regexp.MustCompile(`^(Readiness|Liveness|Startup) probe failed: dial tcp [^ ]+:(\d+): (?:connect: |i/o )?(.*)$`)
	probeStatus = regexp.MustCompile(`^(Readiness|Liveness|Startup) probe failed: HTTP probe failed with statuscode: (\d+)`)
	newSize     = regexp.MustCompile(`New size: (\d+); reason: (.*)$`)
	assigned    = regexp.MustCompile(`^Successfully assigned \S+ to (\S+)`)
	noRoom      = regexp.MustCompile(`^0/(\d+) nodes are available`)
	scaledRS    = regexp.MustCompile(`^Scaled (?:up|down) replica set (\S+) from (\d+) to (\d+)`)
	present     = regexp.MustCompile(`^Container image "([^"]+)" already present on machine`)
	pulled      = regexp.MustCompile(`^Successfully pulled image "([^"]+)"`)
	lowOn       = regexp.MustCompile(`^The node was low on resource: \[?([a-z-]+)`)
	sawJob      = regexp.MustCompile(`^Saw completed job: (\S+), condition: (\w+)`)
	createdPod  = regexp.MustCompile(`^Created pod: (\S+)`)
	createdJob  = regexp.MustCompile(`^Created job (\S+)`)
	pulling     = regexp.MustCompile(`^Pulling image "([^"]+)"`)
	backOff     = regexp.MustCompile(`^Back-off restarting failed container (\S+) in pod`)
	pullBackOff = regexp.MustCompile(`^Back-off pulling image "([^"]+)"`)
)

// MessageWords says the event messages people see most in plain words,
// and leaves the rest as Kubernetes wrote them.
func MessageWords(msg string) string {
	msg = strings.TrimPrefix(msg, "(combined from similar events): ") // Kubernetes merged repeats: what they said follows
	msg = digest.ReplaceAllString(msg, "")                            // image digests are noise next to the tag
	checks := map[string]string{"Readiness": "ready check", "Liveness": "alive check", "Startup": "startup check"}
	if m := probeFailed.FindStringSubmatch(msg); m != nil {
		why := "no answer in time"
		switch {
		case strings.Contains(m[4], "connection refused"):
			why = "nothing listening"
		case strings.Contains(m[4], "connection reset"):
			why = "connection dropped"
		}
		path := m[3]
		if path == "" {
			path = "/"
		}
		return fmt.Sprintf("%s failed: %s on %s, port %s", checks[m[1]], why, path, m[2])
	}
	if m := probeTCP.FindStringSubmatch(msg); m != nil {
		why := "no answer in time"
		if strings.Contains(m[3], "refused") {
			why = "nothing listening"
		}
		return fmt.Sprintf("%s failed: %s on port %s", checks[m[1]], why, m[2])
	}
	if m := probeStatus.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("%s failed: answered %s", checks[m[1]], m[2])
	}
	for kind, words := range checks { // a command check that failed and said nothing
		if strings.TrimSpace(strings.TrimPrefix(msg, kind+" probe failed:")) == "" && strings.HasPrefix(msg, kind+" probe failed:") {
			return words + " failed (its command gave no output)"
		}
	}
	if m := newSize.FindStringSubmatch(msg); m != nil {
		load := ""
		switch {
		case strings.Contains(m[2], "below target"):
			load = " (load is low)"
		case strings.Contains(m[2], "above target"):
			load = " (load is high)"
		}
		return "scaled to " + m[1] + load
	}
	if m := assigned.FindStringSubmatch(msg); m != nil {
		return "placed on node " + strings.Split(m[1], ".")[0]
	}
	if m := noRoom.FindStringSubmatch(msg); m != nil {
		if words := SchedulingWords(msg); words != "" {
			return words // why each node is out: taints, selectors, space
		}
		return fmt.Sprintf("no node can take it (none of the %s)", m[1])
	}
	if m := scaledRS.FindStringSubmatch(msg); m != nil { // one version of it, not the whole deployment
		return fmt.Sprintf("version %s now runs %s (was %s)", m[1], m[3], m[2])
	}
	if strings.HasPrefix(msg, "Readiness probe errored") || strings.HasPrefix(msg, "Liveness probe errored") {
		return "a health check couldn't run (often while the pod is stopping)"
	}
	// Images in full, as the cluster pulls them (a mirror's path too): the
	// same in every row, and what a failed pull needs to be fixed.
	if m := present.FindStringSubmatch(msg); m != nil {
		return "image " + m[1] + " already on the node"
	}
	if m := pulled.FindStringSubmatch(msg); m != nil {
		return "pulled " + m[1]
	}
	if m := lowOn.FindStringSubmatch(msg); m != nil {
		return "pushed off its node: the node ran low on " + m[1]
	}
	if strings.HasPrefix(msg, "Evicted pod") || msg == "Evicted" {
		return "pushed off its node"
	}
	if strings.HasPrefix(msg, "Job has reached the specified backoff limit") {
		return "it failed, and so did any retries it was allowed"
	}
	if strings.HasPrefix(msg, "Job was active longer than specified deadline") {
		return "it ran longer than it is allowed to, so it was stopped"
	}
	if m := sawJob.FindStringSubmatch(msg); m != nil {
		if m[2] == "Failed" {
			return "run " + m[1] + " failed"
		}
		return "run " + m[1] + " finished"
	}
	if m := createdPod.FindStringSubmatch(msg); m != nil {
		return "started pod " + m[1]
	}
	if m := createdJob.FindStringSubmatch(msg); m != nil {
		return "started run " + m[1]
	}
	switch msg {
	case "Job completed":
		return "finished"
	case "Created container", "Container created":
		return "container created"
	case "Started container", "Container started":
		return "container started"
	}
	if m := pulling.FindStringSubmatch(msg); m != nil {
		return "downloading image " + m[1]
	}
	if m := backOff.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("container %q keeps crashing; Kubernetes waits longer before each restart", m[1])
	}
	if m := pullBackOff.FindStringSubmatch(msg); m != nil {
		return "can't download image " + m[1] + "; trying again"
	}
	return msg
}

// schedPart is one reason in the scheduler's list: "35 node(s) had …".
var schedPart = regexp.MustCompile(`(\d+) (.+)`)

// SchedulingWords rewrite the scheduler's message:
// "0/38 nodes are available: 3 Insufficient cpu, 35 node(s) had untolerated taint…"
// → "no node fits: not enough free CPU on 3 nodes; 35 nodes are reserved for
// other workloads (taints)". "" when it isn't that kind of message.
func SchedulingWords(msg string) string {
	_, detail, found := strings.Cut(msg, "available:")
	if !found {
		return ""
	}
	detail, _, _ = strings.Cut(detail, "preemption:")
	var parts []string
	for _, part := range strings.Split(detail, ",") {
		m := schedPart.FindStringSubmatch(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(part), ".")))
		if m == nil {
			continue
		}
		n, what := m[1], m[2]
		switch {
		case strings.HasPrefix(what, "Insufficient cpu"):
			parts = append(parts, "not enough free CPU on "+nodesWord(n))
		case strings.HasPrefix(what, "Insufficient memory"):
			parts = append(parts, "not enough free memory on "+nodesWord(n))
		case strings.Contains(what, "untolerated taint"):
			parts = append(parts, nodesWord(n)+are(n)+" reserved for other workloads (taints)")
		case strings.Contains(what, "didn't match Pod's node affinity") || strings.Contains(what, "node selector"):
			parts = append(parts, nodesWord(n)+map[bool]string{true: " doesn't", false: " don't"}[n == "1"]+" match its node selector")
		case strings.Contains(what, "volume node affinity"):
			parts = append(parts, nodesWord(n)+are(n)+" in the wrong zone for its disk")
		default:
			parts = append(parts, n+" "+what)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "no node fits: " + strings.Join(parts, "; ")
}

// nodesWord is "1 node" or "3 nodes", from the scheduler's count.
func nodesWord(n string) string {
	if n == "1" {
		return "1 node"
	}
	return n + " nodes"
}

// are is "is" for one node, "are" for more.
func are(n string) string {
	if n == "1" {
		return " is"
	}
	return " are"
}

// missingKey is the kubelet's "couldn't find key apitoken in Secret
// ns/dbcreds" (or ConfigMap): the object is there, the key isn't.
var missingKey = regexp.MustCompile(`couldn't find key (\S+) in (Secret|ConfigMap) [^/\s]+/(\S+)`)

// missingObject is the kubelet's `secret "dbcreds" not found` (or
// configmap): the object itself isn't there.
var missingObject = regexp.MustCompile(`\b(secret|configmap)s? "([^"]+)" not found`)

// MissingKey is the key a container waits on and where it should be,
// from the kubelet's message: kind ("secret" or "config map"), name and
// key. ok is false when the message doesn't say a key is missing.
func MissingKey(msg string) (kind, name, key string, ok bool) {
	m := missingKey.FindStringSubmatch(msg)
	if m == nil {
		return "", "", "", false
	}
	kind = "secret"
	if m[2] == "ConfigMap" {
		kind = "config map"
	}
	return kind, m[3], m[1], true
}

// MissingObject says whether the kubelet's message says the secret or
// config map a container needs isn't there.
func MissingObject(msg string) bool { return missingObject.MatchString(msg) }

// MissingObjectName is the secret or config map the kubelet's message
// says isn't there: kind ("secret" or "config map") and name.
func MissingObjectName(msg string) (kind, name string, ok bool) {
	m := missingObject.FindStringSubmatch(msg)
	if m == nil {
		return "", "", false
	}
	kind = "secret"
	if m[1] == "configmap" {
		kind = "config map"
	}
	return kind, m[2], true
}
