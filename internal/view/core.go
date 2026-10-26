package view

import (
	"cmp"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

func init() {
	register(kube.Namespaces, View{
		Columns: []string{"NAME", "STATUS", "AGE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			phase := str(o, "status", "phase")
			tone := ToneNormal
			if phase != "Active" {
				tone = ToneWarn
			}
			return []string{o.GetName(), phase, age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string { return []string{argoLine(e, o)} },
		Needs:  argoNeeds,
	})

	register(kube.Events, View{
		// No TYPE column: warnings show in colour, and "Normal" says nothing.
		Columns:  []string{"AGO", "WHAT", "ABOUT", "COUNT", "MESSAGE"},
		Optional: []string{"COUNT"},
		Main:     "MESSAGE",
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			tone := ToneNormal
			if enrich.IsWarning(o) {
				tone = ToneWarn
			}
			obj := KindWords(strings.ToLower(str(o, "involvedObject", "kind")) + " " + str(o, "involvedObject", "name"))
			return []string{Age(eventTime(o)), EventWords(str(o, "reason"), str(o, "involvedObject", "kind"), str(o, "message")), obj, fmt.Sprint(enrich.EventCount(o)),
				MessageWords(oneLine(str(o, "message")))}, tone
		},
		Time: eventTime,
	})

	register(kube.Services, View{
		Columns:  []string{"NAME", "REACHED FROM", "PORTS", "INSIDE IP", "OUTSIDE ADDRESS", "AGE"},
		Optional: []string{"INSIDE IP", "OUTSIDE ADDRESS", "PORTS"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			tone := ToneNormal
			if serviceProblem(e, o) != "" {
				tone = ToneWarn // nothing answers on it
			}
			ports, _, _ := unstructured.NestedSlice(o.Object, "spec", "ports")
			var ps []string
			for _, p := range ports {
				m, _ := p.(map[string]any)
				p := fmt.Sprint(m["port"])
				if proto := fmt.Sprint(m["protocol"]); proto != "TCP" && proto != "<nil>" {
					p += " " + proto
				}
				ps = append(ps, p)
			}
			var ext []string
			lbs, _, _ := unstructured.NestedSlice(o.Object, "status", "loadBalancer", "ingress")
			for _, lb := range lbs {
				m, _ := lb.(map[string]any)
				ext = append(ext, fmt.Sprint(valueOr(m["hostname"]), valueOr(m["ip"])))
			}
			return []string{o.GetName(), serviceReach(str(o, "spec", "type")), joinOr(ps, ", "), str(o, "spec", "clusterIP"), joinOr(ext, ","), age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			lines := []string{argoLine(e, o)}
			if problem := serviceProblem(e, o); problem != "" {
				lines = append([]string{"Problem  " + problem}, lines...)
			}
			return lines
		},
		Drill: func(o *unstructured.Unstructured) *Drill { return podsOf(o, "spec", "selector") },
		Needs: argoNeeds,
		Caps:  CapXray,
	})

	register(kube.Ingresses, View{
		Columns:  []string{"NAME", "HOSTS", "CLASS", "ADDRESS", "AGE"},
		Optional: []string{"ADDRESS", "CLASS"},
		Main:     "HOSTS",
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "rules")
			var hosts []string
			for _, r := range rules {
				m, _ := r.(map[string]any)
				hosts = append(hosts, fmt.Sprint(valueOr(m["host"])))
			}
			lbs, _, _ := unstructured.NestedSlice(o.Object, "status", "loadBalancer", "ingress")
			var addr []string
			for _, lb := range lbs {
				m, _ := lb.(map[string]any)
				addr = append(addr, fmt.Sprint(valueOr(m["hostname"]), valueOr(m["ip"])))
			}
			class := str(o, "spec", "ingressClassName")
			if class == "" { // older ingresses name it in an annotation
				class = o.GetAnnotations()["kubernetes.io/ingress.class"]
			}
			tone := ToneNormal
			if len(missingServices(e, o)) > 0 || len(ingressProblems(e, o)) > 0 {
				tone = ToneWarn // what it sends to isn't there, or nothing takes it
			}
			return []string{o.GetName(), joinOr(hosts, ", "), enrich.Or(class), joinOr(addr, ","), age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			// Where each address goes: host and path to a service and port.
			var lines []string
			if problem := missingServicesWords(e, o); problem != "" {
				lines = append(lines, "Problem  "+problem)
			}
			for _, problem := range ingressProblems(e, o) {
				lines = append(lines, "Problem  "+problem)
			}
			rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "rules")
			for _, r := range rules {
				rm, _ := r.(map[string]any)
				host := fmt.Sprint(valueOr(rm["host"]))
				paths, _, _ := unstructured.NestedSlice(rm, "http", "paths")
				for _, p := range paths {
					pm, _ := p.(map[string]any)
					svc, _, _ := unstructured.NestedString(pm, "backend", "service", "name")
					port, _, _ := unstructured.NestedFieldNoCopy(pm, "backend", "service", "port", "number")
					if port == nil {
						port, _, _ = unstructured.NestedFieldNoCopy(pm, "backend", "service", "port", "name")
					}
					path, _ := pm["path"].(string)
					lines = append(lines, fmt.Sprintf("Sends    %s%s to service %s, port %v", host, path, svc, valueOr(port)))
				}
			}
			if len(lines) > 6 {
				lines = append(lines[:5], fmt.Sprintf("Sends    and %d more (d shows them all)", len(lines)-5))
			}
			return append(lines, argoLine(e, o))
		},
		Needs: append([]schema.GroupVersionResource{kube.IngressClasses}, routeNeeds...), // classes: which one is the default
	})

	register(kube.HTTPRoutes, View{
		Columns:  []string{"NAME", "HOSTNAMES", "PARENTS", "ACCEPTED", "AGE"},
		Optional: []string{"PARENTS", "AGE"},
		Main:     "HOSTNAMES",
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			parents, _, _ := unstructured.NestedSlice(o.Object, "spec", "parentRefs")
			var ps []string
			for _, p := range parents {
				m, _ := p.(map[string]any)
				ps = append(ps, fmt.Sprint(m["name"]))
			}
			accepted, tone := routeAccepted(o)
			if tone == ToneNormal && len(missingServices(e, o)) > 0 {
				tone = ToneWarn // what it sends to isn't there
			}
			return []string{o.GetName(), joinOr(strs(o, "spec", "hostnames"), ", "), joinOr(ps, ","), accepted, age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			var lines []string
			// Why it's flagged first: a route no gateway takes reaches nothing.
			switch accepted, _ := routeAccepted(o); {
			case accepted == "-" && len(strs(o, "spec", "parentRefs")) == 0 && len(sliceOf(o, "spec", "parentRefs")) == 0:
				lines = append(lines, "Problem  it names no gateway, so no traffic reaches it",
					"Fix      add the gateway it belongs to under parentRefs (in Git)")
			case accepted == "-":
				lines = append(lines, "Problem  no gateway has taken it yet: nothing reaches it until one does")
			case accepted != "True":
				lines = append(lines, "Problem  its gateway refused it: "+accepted+routeRefusal(o))
			}
			if problem := missingServicesWords(e, o); problem != "" {
				lines = append(lines, "Problem  "+problem)
			}
			if hosts := strs(o, "spec", "hostnames"); len(hosts) > 0 {
				web := make([]string, len(hosts))
				for i, h := range hosts {
					web[i] = "https://" + h
				}
				lines = append(lines, "Web      "+strings.Join(web, " · "))
			}
			if sends := routeSends(o); sends != "" {
				lines = append(lines, "Sends    "+sends)
			}
			return append(lines, argoLine(e, o))
		},
		Needs: routeNeeds,
	})

	register(kube.ConfigMaps, View{
		Columns: []string{"NAME", "KEYS", "AGE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			data, _, _ := unstructured.NestedMap(o.Object, "data")
			bin, _, _ := unstructured.NestedMap(o.Object, "binaryData")
			return []string{o.GetName(), fmt.Sprint(len(data) + len(bin)), age(o)}, ToneNormal
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			data, _, _ := unstructured.NestedMap(o.Object, "data")
			keys := slices.Sorted(maps.Keys(data))
			for _, k := range BinaryKeys(o) {
				keys = append(keys, k.Name+" (binary, "+k.Size+")")
			}
			if len(keys) == 0 {
				return []string{"Keys     none: it is empty", argoLine(e, o)}
			}
			return []string{"Keys     " + strings.Join(keys, ", ") + "  (x shows them)", argoLine(e, o)}
		},
		Needs: argoNeeds,
	})

	register(kube.Secrets, View{
		Columns:  []string{"NAME", "KIND", "KEYS", "FILLED BY", "AGE"},
		Optional: []string{"FILLED BY", "KIND"},
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			data, _, _ := unstructured.NestedMap(o.Object, "data")
			source, tone := "", ToneNormal
			if es, ok := e.ExternalSecretOwner(o); ok {
				source = "ExternalSecret " + es.Name
				if es.Ready == "False" {
					tone = ToneBad
				}
			}
			return []string{o.GetName(), secretKind(str(o, "type")), fmt.Sprint(len(data)), source, age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			lines := []string{argoLine(e, o)}
			if es, ok := e.ExternalSecretOwner(o); ok {
				lines = append(lines, secretLine(es))
			}
			data, _, _ := unstructured.NestedMap(o.Object, "data")
			keys := slices.Sorted(maps.Keys(data))
			lines = append(lines, "Keys     "+joinOr(keys, ", "))
			return lines
		},
		Needs: []schema.GroupVersionResource{kube.ExternalSecrets, kube.Applications},
	})

	register(kube.PVCs, View{
		Columns:  []string{"NAME", "STATUS", "SIZE", "STORAGE CLASS", "AGE"},
		Optional: []string{"STORAGE CLASS"},
		Main:     "STATUS",
		Row: func(e *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			status, tone := claimStatus(e, o)
			size := cmp.Or(str(o, "status", "capacity", "storage"),
				str(o, "spec", "resources", "requests", "storage")) // asked for, not made yet
			return []string{o.GetName(), status, size, str(o, "spec", "storageClassName"), age(o)}, tone
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
			status, _ := claimStatus(e, o)
			lines := []string{"Status   " + status}
			if v := str(o, "spec", "volumeName"); v != "" {
				lines = append(lines, "Volume   "+v)
			}
			if msg, at, ok := e.LatestWarning(o); ok && str(o, "status", "phase") != "Bound" {
				lines = append(lines, fmt.Sprintf("Problem  %s (%s ago)", msg, Age(at)))
			}
			return append(lines, argoLine(e, o))
		},
		Needs: argoNeeds,
	})

	register(kube.ServiceAccounts, View{
		Columns: []string{"NAME", "AWS ROLE", "AGE"},
		Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
			return []string{o.GetName(), roleName(enrich.ServiceAccountRole(o)), age(o)}, ToneNormal
		},
		Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string { return []string{argoLine(e, o)} },
		Needs:  argoNeeds,
	})
}

// eventTime to the second, as the list sorts by: a Kyverno event at
// 06:37:00.142 and a kubelet one at 06:37:00 then read the same age, not
// "22s" below "23s".
func eventTime(o *unstructured.Unstructured) time.Time {
	return enrich.EventTime(o).Truncate(time.Second)
}

func routeAccepted(o *unstructured.Unstructured) (string, Tone) {
	parents, _, _ := unstructured.NestedSlice(o.Object, "status", "parents")
	for _, p := range parents {
		m, _ := p.(map[string]any)
		conds, _ := m["conditions"].([]any)
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "Accepted" {
				if cm["status"] == "True" {
					return "True", ToneNormal
				}
				return fmt.Sprint(cm["reason"]), ToneBad
			}
		}
	}
	return "-", ToneWarn
}

func sliceOf(o *unstructured.Unstructured, fields ...string) []any {
	v, _, _ := unstructured.NestedSlice(o.Object, fields...)
	return v
}

// routeRefusal is the gateway's own words on why it refused a route.
func routeRefusal(o *unstructured.Unstructured) string {
	for _, p := range sliceOf(o, "status", "parents") {
		m, _ := p.(map[string]any)
		conds, _ := m["conditions"].([]any)
		for _, c := range conds {
			if cm, _ := c.(map[string]any); cm["type"] == "Accepted" && cm["status"] != "True" {
				if msg := oneLine(fmt.Sprint(cm["message"])); msg != "" && msg != "<nil>" {
					return ": " + msg
				}
			}
		}
	}
	return ""
}

// routeSends lists where a route's rules send traffic: "/api → shop-backend".
// missingServices are the services an ingress or HTTPRoute sends to that
// aren't there ("name", or "name (in ns)" in another namespace), only once
// the services of that namespace have loaded: one still loading isn't
// proof that it doesn't exist.
func missingServices(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	if e == nil {
		return nil
	}
	type ref struct{ ns, name string }
	var refs []ref
	switch o.GetKind() {
	case "Ingress":
		if n, _, _ := unstructured.NestedString(o.Object, "spec", "defaultBackend", "service", "name"); n != "" {
			refs = append(refs, ref{o.GetNamespace(), n})
		}
		for _, r := range sliceOf(o, "spec", "rules") {
			rm, _ := r.(map[string]any)
			paths, _, _ := unstructured.NestedSlice(rm, "http", "paths")
			for _, p := range paths {
				pm, _ := p.(map[string]any)
				if n, _, _ := unstructured.NestedString(pm, "backend", "service", "name"); n != "" {
					refs = append(refs, ref{o.GetNamespace(), n})
				}
			}
		}
	case "HTTPRoute":
		for _, r := range sliceOf(o, "spec", "rules") {
			rm, _ := r.(map[string]any)
			backends, _, _ := unstructured.NestedSlice(rm, "backendRefs")
			for _, b := range backends {
				bm, _ := b.(map[string]any)
				kind, _ := bm["kind"].(string)
				group, _ := bm["group"].(string)
				name, _ := bm["name"].(string)
				if name == "" || kind != "" && kind != "Service" || group != "" {
					continue // not a service: kubed doesn't look for it
				}
				ns, _ := bm["namespace"].(string)
				if ns == "" {
					ns = o.GetNamespace()
				}
				refs = append(refs, ref{ns, name})
			}
		}
	}
	var out []string
	seen := map[ref]bool{}
	for _, r := range refs {
		if seen[r] || !e.SyncedIn(kube.Services, r.ns) {
			continue
		}
		seen[r] = true
		if _, ok := e.Get(kube.Services, r.ns, r.name); !ok {
			name := r.name
			if r.ns != o.GetNamespace() {
				name += " (in " + r.ns + ")"
			}
			out = append(out, name)
		}
	}
	return out
}

// TLSSecret says, when set (the ui sets it), whether secret ns/name
// exists, as read on its own by name without its values; known is false
// when it hasn't been read yet or can't be (no access).
var TLSSecret func(ns, name string) (exists, known bool)

// TLSSecretNames are the TLS secrets an ingress names.
func TLSSecretNames(o *unstructured.Unstructured) []string {
	var out []string
	for _, t := range sliceOf(o, "spec", "tls") {
		tm, _ := t.(map[string]any)
		if name, _ := tm["secretName"].(string); name != "" {
			out = append(out, name)
		}
	}
	return out
}

// ingressProblems are what keeps an ingress from working that kubed can
// know: a TLS secret that isn't there (once its namespace's secrets
// have loaded), and no class while no ingress class is the default (once
// the classes have loaded) and no controller has given it an address.
func ingressProblems(e *enrich.Enricher, o *unstructured.Unstructured) []string {
	if e == nil {
		return nil
	}
	var out []string
	synced := e.SyncedIn(kube.Secrets, o.GetNamespace()) // another screen loaded them
	if synced || TLSSecret != nil {
		seen := map[string]bool{}
		for _, name := range TLSSecretNames(o) {
			if seen[name] {
				continue
			}
			seen[name] = true
			_, there := e.Get(kube.Secrets, o.GetNamespace(), name)
			if !synced {
				var known bool
				if there, known = TLSSecret(o.GetNamespace(), name); !known {
					continue // not read yet, or not allowed to: say nothing
				}
			}
			if !there {
				out = append(out, "TLS secret "+name+" isn't there")
			}
		}
	}
	class := cmp.Or(str(o, "spec", "ingressClassName"), o.GetAnnotations()["kubernetes.io/ingress.class"])
	lbs, _, _ := unstructured.NestedSlice(o.Object, "status", "loadBalancer", "ingress")
	if class == "" && len(lbs) == 0 && e.Synced(kube.IngressClasses) { // with an address, a controller took it all the same
		def := false
		for _, c := range e.List(kube.IngressClasses, "") {
			def = def || c.GetAnnotations()["ingressclass.kubernetes.io/is-default-class"] == "true"
		}
		if !def {
			out = append(out, "it has no class and none is the default, so a controller takes it only if it is set to watch ingresses without one")
		}
	}
	return out
}

// missingServicesWords: "service x isn't there" ("" when all are).
func missingServicesWords(e *enrich.Enricher, o *unstructured.Unstructured) string {
	switch missing := missingServices(e, o); len(missing) {
	case 0:
		return ""
	case 1:
		return "service " + missing[0] + " isn't there"
	default:
		return "services " + strings.Join(missing, ", ") + " aren't there"
	}
}

func routeSends(o *unstructured.Unstructured) string {
	var out []string
	for _, r := range sliceOf(o, "spec", "rules") {
		rm, _ := r.(map[string]any)
		var to []string
		refs, _, _ := unstructured.NestedSlice(rm, "backendRefs")
		for _, ref := range refs {
			fm, _ := ref.(map[string]any)
			name := fmt.Sprint(fm["name"])
			if ns, _ := fm["namespace"].(string); ns != "" && ns != o.GetNamespace() {
				name += " (in " + ns + ")"
			}
			if kind, _ := fm["kind"].(string); kind != "" && kind != "Service" {
				name = strings.ToLower(kind) + " " + name
			}
			to = append(to, name)
		}
		if len(to) == 0 {
			continue
		}
		path := "/"
		matches, _, _ := unstructured.NestedSlice(rm, "matches")
		if len(matches) > 0 {
			if v, _, _ := unstructured.NestedString(matches[0].(map[string]any), "path", "value"); v != "" {
				path = v
			}
		}
		out = append(out, path+" → "+strings.Join(to, ", "))
	}
	return strings.Join(out, " · ")
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// secretKind says what a Secret's type is for.
func secretKind(t string) string {
	switch t {
	case "Opaque", "":
		return "values"
	case "kubernetes.io/tls":
		return "TLS certificate"
	case "kubernetes.io/service-account-token":
		return "service account token"
	case "kubernetes.io/dockerconfigjson", "kubernetes.io/dockercfg":
		return "registry login"
	case "kubernetes.io/basic-auth":
		return "user and password"
	case "kubernetes.io/ssh-auth":
		return "SSH key"
	case "bootstrap.kubernetes.io/token":
		return "node join token"
	}
	return t
}

// serviceReach says who can reach a Service, from its type.
func serviceReach(t string) string {
	switch t {
	case "ClusterIP", "":
		return "in cluster"
	case "LoadBalancer":
		return "a load balancer"
	case "NodePort":
		return "each node's port"
	case "ExternalName":
		return "points outside"
	}
	return t
}

// serviceProblem: a service with a selector that no running pod matches,
// or whose running pods are none of them ready, so connections to it go
// nowhere ("" when some pod answers). Services with no selector (fed by
// hand) and ExternalName ones are left alone, and not-ready pods for one
// that sends to them too (publishNotReadyAddresses).
func serviceProblem(e *enrich.Enricher, o *unstructured.Unstructured) string {
	sel, _, _ := unstructured.NestedStringMap(o.Object, "spec", "selector")
	if e == nil || len(sel) == 0 || str(o, "spec", "type") == "ExternalName" || !e.Synced(kube.Pods) {
		return ""
	}
	want := labels.SelectorFromSet(sel)
	running := 0
	for _, p := range e.List(kube.Pods, o.GetNamespace()) {
		if str(p, "status", "phase") != "Running" || !want.Matches(labels.Set(p.GetLabels())) {
			continue
		}
		running++
		if c, ok := enrich.Condition(p, "Ready"); ok && c.Status == "True" {
			return ""
		}
	}
	switch publish, _, _ := unstructured.NestedBool(o.Object, "spec", "publishNotReadyAddresses"); {
	case running == 0:
		return "no running pod matches it, so nothing answers on it (check its selector, or the pods it should reach)"
	case publish:
		return ""
	case running == 1:
		return "its 1 pod isn't ready, so nothing answers on it"
	}
	return fmt.Sprintf("its %d pods aren't ready, so nothing answers on it", running)
}

// claimStatus is a volume claim's phase in words: Bound is fine, Pending
// for more than a minute means no volume could be made for it, and why
// when Kubernetes said (a storage class that doesn't exist, or none).
func claimStatus(e *enrich.Enricher, o *unstructured.Unstructured) (string, Tone) {
	switch phase := str(o, "status", "phase"); {
	case phase == "Bound":
		return "Bound", ToneNormal
	case phase == "Lost":
		return "Lost: its volume is gone", ToneBad
	case phase == "Pending" && time.Since(o.GetCreationTimestamp().Time) > time.Minute:
		msg, waiting := "", ""
		if e != nil {
			msg, _, _ = e.LatestWarning(o)
			// "no storage class is set" comes as a normal event, not a warning
			if m, _, ok := e.LatestEvent(o.GetNamespace(), "PersistentVolumeClaim", o.GetName(), "FailedBinding"); ok {
				msg += " " + m
			}
			// Waiting for a pod, only while no pod has been placed to use it
			// (then a failure to make the volume is the news, not the wait).
			if _, placed := o.GetAnnotations()["volume.kubernetes.io/selected-node"]; !placed {
				waiting, _, _ = e.LatestEvent(o.GetNamespace(), "PersistentVolumeClaim", o.GetName(), "WaitForFirstConsumer")
			}
		}
		// Events last an hour: a reason counts only while the claim still
		// fits it (no class named; the class it names not found).
		class := str(o, "spec", "storageClassName")
		switch low := strings.ToLower(msg); {
		case class == "" && strings.Contains(low, "no storage class is set"):
			return "Pending: it names no storage class and none is the default, so no volume is made", ToneBad
		case class != "" && strings.Contains(low, "storageclass") && strings.Contains(low, "not found") && strings.Contains(low, `"`+strings.ToLower(class)+`"`):
			return "Pending: its storage class doesn't exist, so no volume is made", ToneBad
		case class != "" && waiting != "" && strings.TrimSpace(msg) == "":
			return "Pending: its volume is made once a pod uses it (its storage class waits for one)", ToneNormal
		}
		return "Pending: no volume made for it yet", ToneWarn
	case phase == "":
		return "-", ToneNormal
	default:
		return phase, ToneNormal
	}
}

// routeNeeds: ingresses and routes say when the service they send to
// isn't there, from the services list.
var routeNeeds = []schema.GroupVersionResource{kube.Applications, kube.Services}

// BinaryKey is a ConfigMap's binaryData key and how big its value is
// ("1.2 KB"): the bytes themselves aren't text to show.
type BinaryKey struct{ Name, Size string }

// BinaryKeys are a ConfigMap's binaryData keys, sorted.
func BinaryKeys(o *unstructured.Unstructured) []BinaryKey {
	bin, _, _ := unstructured.NestedStringMap(o.Object, "binaryData")
	out := make([]BinaryKey, 0, len(bin))
	for k, v := range bin {
		size := "size unknown"
		if b, err := base64.StdEncoding.DecodeString(v); err == nil {
			size = byteSize(len(b))
		}
		out = append(out, BinaryKey{Name: k, Size: size})
	}
	slices.SortFunc(out, func(a, b BinaryKey) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// byteSize is a size in bytes as people read it: "300 bytes", "1.2 KB", "3.4 MB".
func byteSize(n int) string {
	switch {
	case n == 1:
		return "1 byte"
	case n < 1<<10:
		return fmt.Sprintf("%d bytes", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
}
