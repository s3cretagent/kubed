package enrich

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
)

// Health is how an xray / dashboard item is doing.
type Health int

// Health levels, best to worst.
const (
	HealthNone Health = iota // nothing to judge (e.g. a ConfigMap)
	HealthOK
	HealthWarn
	HealthBad
)

// XNode is one line of an xray tree.
type XNode struct {
	Kind, Name string
	Note       string // short status, e.g. "2/2 ready" or "role-auth"
	Health     Health
	Children   []*XNode
}

// XrayNeeds are the resources an xray reads; the UI warms them first.
var XrayNeeds = []schema.GroupVersionResource{
	kube.Pods, kube.ReplicaSets, kube.Deployments, kube.StatefulSets, kube.DaemonSets, kube.Jobs, kube.CronJobs,
	kube.Services, kube.Ingresses, kube.HTTPRoutes, kube.ConfigMaps, kube.Secrets, kube.ExternalSecrets,
	kube.ServiceAccounts, kube.PVCs, kube.HPAs, kube.PDBs, kube.Nodes,
}

// Xray builds the tree of what an object is made of and what it uses:
// workload → ReplicaSets → Pods → containers, plus Services, routes,
// config, identity, storage and scaling. A Pod shows its own node and role,
// and what made it.
func (e *Enricher) Xray(obj *unstructured.Unstructured) *XNode {
	switch obj.GetKind() {
	case "Service":
		return e.xrayService(obj)
	case "Pod":
		return e.xrayPod(obj)
	}
	root := e.TopOwner(obj)
	node := &XNode{Kind: root.GetKind(), Name: root.GetName()}
	node.Note, node.Health = workloadHealth(root)

	node.Children = append(node.Children, e.xrayOwned(root)...)
	if tmpl := podTemplate(root); tmpl != nil {
		ns := root.GetNamespace()
		labels, _, _ := unstructured.NestedStringMap(tmpl, "metadata", "labels")
		spec, _, _ := unstructured.NestedMap(tmpl, "spec")
		if uses := e.xrayUses(ns, spec, e.PodsOf(root)); len(uses) > 0 {
			node.Children = append(node.Children, &XNode{Kind: "uses", Children: uses})
		}
		if exposed := e.xrayExposedBy(ns, labels); len(exposed) > 0 {
			node.Children = append(node.Children, &XNode{Kind: "exposed by", Children: exposed})
		}
		if guards := e.xrayGuards(root, labels); len(guards) > 0 {
			node.Children = append(node.Children, &XNode{Kind: "scaling & disruption", Children: guards})
		}
	}
	return node
}

// xrayPod is one pod: its containers, the node it runs on, the AWS role
// it runs as, what it uses, and the workload that made it.
func (e *Enricher) xrayPod(pod *unstructured.Unstructured) *XNode {
	node := &XNode{Kind: "Pod", Name: pod.GetName()}
	node.Note, node.Health = workloadHealth(pod)
	node.Children = containerNodes(pod)
	if top := e.TopOwner(pod); top != pod && top.GetKind() != "Pod" {
		owner := &XNode{Kind: top.GetKind(), Name: top.GetName()}
		owner.Note, owner.Health = workloadHealth(top)
		node.Children = append(node.Children, &XNode{Kind: "made by", Children: []*XNode{owner}})
	}
	if name := str(pod, "spec", "nodeName"); name != "" {
		n := &XNode{Kind: "Node", Name: strings.Split(name, ".")[0], Health: HealthOK} // as lists show it
		if nd, ok := e.get(kube.Nodes, "", name); ok {
			n.Note = nd.GetLabels()[labelInstanceType]
		}
		node.Children = append(node.Children, &XNode{Kind: "runs on", Children: []*XNode{n}})
	}
	ns := pod.GetNamespace()
	spec, _, _ := unstructured.NestedMap(pod.Object, "spec")
	uses := e.xrayUses(ns, spec, []*unstructured.Unstructured{pod})
	if _, role, via := e.PodRole(pod); role != "" && strings.HasPrefix(via, "set on the pod") {
		uses = append([]*XNode{{Kind: "AWS role", Name: role, Note: via, Health: HealthNone}}, uses...)
	}
	if len(uses) > 0 {
		node.Children = append(node.Children, &XNode{Kind: "uses", Children: uses})
	}
	if exposed := e.xrayExposedBy(ns, pod.GetLabels()); len(exposed) > 0 {
		node.Children = append(node.Children, &XNode{Kind: "exposed by", Children: exposed})
	}
	return node
}

// podTemplate returns the pod template (metadata + spec) of a workload, or
// the pod itself.
func podTemplate(o *unstructured.Unstructured) map[string]any {
	var path []string
	switch o.GetKind() {
	case "Pod":
		return o.Object
	case "CronJob":
		path = []string{"spec", "jobTemplate", "spec", "template"}
	default:
		path = []string{"spec", "template"}
	}
	t, found, _ := unstructured.NestedMap(o.Object, path...)
	if !found {
		return nil
	}
	return t
}

// xrayOwned lists what the object controls, recursively down to pods.
func (e *Enricher) xrayOwned(o *unstructured.Unstructured) []*XNode {
	var childGVRs []schema.GroupVersionResource
	switch o.GetKind() {
	case "Deployment":
		childGVRs = []schema.GroupVersionResource{kube.ReplicaSets}
	case "CronJob":
		childGVRs = []schema.GroupVersionResource{kube.Jobs}
	case "ReplicaSet", "StatefulSet", "DaemonSet", "Job":
		childGVRs = []schema.GroupVersionResource{kube.Pods}
	case "Pod":
		return containerNodes(o)
	default:
		return nil
	}
	var out []*XNode
	for _, gvr := range childGVRs {
		for _, child := range e.list(gvr, o.GetNamespace()) {
			if !ownedBy(child, o) {
				continue
			}
			// Old ReplicaSets scaled to zero are history, not structure.
			if child.GetKind() == "ReplicaSet" && num(child, "spec", "replicas") == 0 {
				continue
			}
			n := &XNode{Kind: child.GetKind(), Name: child.GetName()}
			n.Note, n.Health = workloadHealth(child)
			n.Children = e.xrayOwned(child)
			out = append(out, n)
		}
	}
	return out
}

func ownedBy(child, owner *unstructured.Unstructured) bool {
	for _, ref := range child.GetOwnerReferences() {
		if ref.UID == owner.GetUID() || (ref.Kind == owner.GetKind() && ref.Name == owner.GetName()) {
			return true
		}
	}
	return false
}

func containerNodes(pod *unstructured.Unstructured) []*XNode {
	statuses := runningStatuses(pod)
	var out []*XNode
	for _, s := range statuses {
		m, _ := s.(map[string]any)
		n := &XNode{Kind: "container", Name: fmt.Sprint(m["name"])}
		ready, _ := m["ready"].(bool)
		restarts, _ := m["restartCount"].(int64)
		n.Note, n.Health = "ready", HealthOK
		if !ready {
			n.Note, n.Health = "not ready", HealthBad
			if w, ok, _ := unstructured.NestedString(m, "state", "waiting", "reason"); ok {
				n.Note = w
			}
			if t, _, _ := unstructured.NestedMap(m, "state", "terminated"); t != nil && t["reason"] == "OOMKilled" {
				n.Note = "ran out of memory and was killed; waiting to restart"
				if at, err := time.Parse(time.RFC3339, fmt.Sprint(t["finishedAt"])); err == nil {
					n.Note = "ran out of memory " + ageWords(time.Since(at)) + " ago; waiting to restart"
				}
			}
		}
		switch {
		case restarts == 1:
			n.Note += ", 1 restart"
		case restarts > 1:
			n.Note += fmt.Sprintf(", %d restarts", restarts)
		}
		// Ready now, but killed for running out of memory before: not a
		// tick's worth of fine.
		if t, _, _ := unstructured.NestedMap(m, "lastState", "terminated"); t != nil && t["reason"] == "OOMKilled" && n.Health == HealthOK {
			n.Health = HealthWarn
			if at, err := time.Parse(time.RFC3339, fmt.Sprint(t["finishedAt"])); err == nil {
				n.Note += ", last ran out of memory " + ageWords(time.Since(at)) + " ago"
			} else {
				n.Note += ", last ran out of memory"
			}
		}
		out = append(out, n)
	}
	return out
}

// xrayUses lists the config, identity and storage a pod spec references.
// pods are the pods made from it: a key their containers wait on marks
// the secret or config map it should be in.
func (e *Enricher) xrayUses(ns string, spec map[string]any, pods []*unstructured.Unstructured) []*XNode {
	var out []*XNode
	sa, _, _ := unstructured.NestedString(spec, "serviceAccountName")
	if sa == "" {
		sa = "default"
	}
	saNode := &XNode{Kind: "ServiceAccount", Name: sa, Health: HealthNone}
	if obj, ok := e.get(kube.ServiceAccounts, ns, sa); ok {
		if role := ServiceAccountRole(obj); role != "" {
			saNode.Note = "AWS role " + role
		}
	} else {
		saNode.Note, saNode.Health = e.missingNote(kube.ServiceAccounts)
	}
	out = append(out, saNode)

	pod := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	optional := optionalRefs(spec)
	asked, waits := keyRefs(spec), waitedKeys(pods)
	e.mountWaits(spec, pods, waits)
	for _, name := range SortedKeys(PodSecretNames(pod)) {
		n := &XNode{Kind: "Secret", Name: name}
		s, found := e.get(kube.Secrets, ns, name)
		if !found {
			n.Note, n.Health = e.missingNote(kube.Secrets)
			if optional["secret/"+name] && n.Health == HealthBad {
				n.Note, n.Health = "missing, but optional: the pod starts without it", HealthNone
			}
		}
		for _, es := range e.list(kube.ExternalSecrets, ns) {
			if s := ExternalSecret(es); s.Target == name {
				n.Note = "← ExternalSecret " + s.Name
				n.Health = HealthOK
				if s.Ready == "False" {
					n.Note += ": " + s.Reason
					n.Health = HealthBad
				}
			}
		}
		noKeys(n, s, found, asked["secret/"+name], waits["secret/"+name])
		out = append(out, n)
	}
	for _, name := range podConfigMaps(spec) {
		n := &XNode{Kind: "ConfigMap", Name: name}
		cm, found := e.get(kube.ConfigMaps, ns, name)
		if !found {
			n.Note, n.Health = e.missingNote(kube.ConfigMaps)
			if optional["configmap/"+name] && n.Health == HealthBad {
				n.Note, n.Health = "missing, but optional: the pod starts without it", HealthNone
			}
		}
		noKeys(n, cm, found, asked["configmap/"+name], waits["configmap/"+name])
		out = append(out, n)
	}
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, v := range volumes {
		vm, _ := v.(map[string]any)
		if path, _, _ := unstructured.NestedString(vm, "hostPath", "path"); path != "" {
			out = append(out, &XNode{Kind: "folder on the node", Name: path, Health: HealthNone})
			continue
		}
		claim, _, _ := unstructured.NestedString(vm, "persistentVolumeClaim", "claimName")
		if claim == "" {
			continue
		}
		n := &XNode{Kind: "PVC", Name: claim}
		n.Note, n.Health = e.missingNote(kube.PVCs)
		if pvc, ok := e.get(kube.PVCs, ns, claim); ok {
			phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
			n.Note, n.Health = phase, HealthOK
			if phase != "Bound" {
				n.Health = HealthWarn
			}
		}
		out = append(out, n)
	}
	return out
}

// keyRefs are the keys a pod spec's env reads from secrets and config maps
// ("secret/x", "configmap/x"), leaving out the optional ones: a container
// doesn't start without them.
func keyRefs(spec map[string]any) map[string][]string {
	out := map[string][]string{}
	for _, field := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(spec, field)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			env, _, _ := unstructured.NestedSlice(cm, "env")
			for _, ev := range env {
				evm, _ := ev.(map[string]any)
				for kind, ref := range map[string]string{"secret": "secretKeyRef", "configmap": "configMapKeyRef"} {
					r, _, _ := unstructured.NestedMap(evm, "valueFrom", ref)
					name, _, _ := unstructured.NestedString(r, "name")
					key, _, _ := unstructured.NestedString(r, "key")
					if opt, _, _ := unstructured.NestedBool(r, "optional"); name != "" && key != "" && !opt {
						out[kind+"/"+name] = append(out[kind+"/"+name], key)
					}
				}
			}
		}
	}
	// Volumes that list keys (items) of a secret or config map: not
	// mounted without them.
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, v := range volumes {
		vm, _ := v.(map[string]any)
		for kind, src := range volumeKeySources(vm) {
			for _, ks := range src {
				out[kind+"/"+ks[0]] = append(out[kind+"/"+ks[0]], ks[1])
			}
		}
	}
	return out
}

// volumeKeySources are the non-optional (name, key) pairs a volume's
// items list, by kind ("secret", "configmap"): its own source's, or each
// of a projected volume's.
func volumeKeySources(vm map[string]any) map[string][][2]string {
	out := map[string][][2]string{}
	add := func(kind string, src map[string]any, nameField string) {
		if src == nil {
			return
		}
		name, _, _ := unstructured.NestedString(src, nameField)
		if opt, _, _ := unstructured.NestedBool(src, "optional"); name == "" || opt {
			return
		}
		items, _, _ := unstructured.NestedSlice(src, "items")
		for _, it := range items {
			im, _ := it.(map[string]any)
			if key, _ := im["key"].(string); key != "" {
				out[kind] = append(out[kind], [2]string{name, key})
			}
		}
	}
	sec, _, _ := unstructured.NestedMap(vm, "secret")
	add("secret", sec, "secretName")
	cm, _, _ := unstructured.NestedMap(vm, "configMap")
	add("configmap", cm, "name")
	sources, _, _ := unstructured.NestedSlice(vm, "projected", "sources")
	for _, src := range sources {
		sm, _ := src.(map[string]any)
		sec, _, _ := unstructured.NestedMap(sm, "secret")
		add("secret", sec, "name")
		cm, _, _ := unstructured.NestedMap(sm, "configMap")
		add("configmap", cm, "name")
	}
	return out
}

// mountNoKey is the kubelet's FailedMount for a key a volume lists that
// isn't there: `MountVolume.SetUp failed for volume "s" : references
// non-existent secret key: nope`.
var mountNoKey = regexp.MustCompile(`failed for volume "([^"]+)" : .*non-existent (secret|config) key: (\S+)`)

// mountWaits adds to waits the keys pods can't mount, from their
// FailedMount warnings: the volume's secret or config map that lists the
// key (one only, or it says nothing).
func (e *Enricher) mountWaits(spec map[string]any, pods []*unstructured.Unstructured, waits map[string][]string) {
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, p := range pods {
		for _, ev := range e.list(kube.Events, p.GetNamespace()) {
			if str(ev, "reason") != "FailedMount" || str(ev, "involvedObject", "kind") != "Pod" || str(ev, "involvedObject", "name") != p.GetName() {
				continue
			}
			if uid := str(ev, "involvedObject", "uid"); uid != "" && p.GetUID() != "" && uid != string(p.GetUID()) {
				continue // an earlier pod of the same name's
			}
			m := mountNoKey.FindStringSubmatch(strings.Join(strings.Fields(str(ev, "message")), " "))
			if m == nil {
				continue
			}
			kind := map[string]string{"secret": "secret", "config": "configmap"}[m[2]]
			var names []string
			for _, v := range volumes {
				vm, _ := v.(map[string]any)
				if vm["name"] != m[1] {
					continue
				}
				for _, ks := range volumeKeySources(vm)[kind] {
					if ks[1] == m[3] && !slices.Contains(names, ks[0]) {
						names = append(names, ks[0])
					}
				}
			}
			if len(names) == 1 && !slices.Contains(waits[kind+"/"+names[0]], m[3]) {
				waits[kind+"/"+names[0]] = append(waits[kind+"/"+names[0]], m[3])
			}
		}
	}
}

// waitedKeys are the keys pods' containers wait on, by what the kubelet
// says ("couldn't find key k in Secret ns/x"), as keyRefs keys them.
func waitedKeys(pods []*unstructured.Unstructured) map[string][]string {
	out := map[string][]string{}
	for _, p := range pods {
		for _, field := range []string{"initContainerStatuses", "containerStatuses"} {
			statuses, _, _ := unstructured.NestedSlice(p.Object, "status", field)
			for _, st := range statuses {
				sm, _ := st.(map[string]any)
				msg, _, _ := unstructured.NestedString(sm, "state", "waiting", "message")
				if kind, name, key, ok := MissingKey(msg); ok {
					ref := strings.ReplaceAll(kind, " ", "") + "/" + name
					out[ref] = append(out[ref], key)
				}
			}
		}
	}
	return out
}

// noKeys marks a secret or config map that lacks a key a container needs:
// by its own keys (names only) when kubed has it, else when a pod's
// container waits on that key. One kubed doesn't have is left as it is
// (missing, or not known) when no pod says so.
func noKeys(n *XNode, o *unstructured.Unstructured, found bool, asked, waited []string) {
	lacks := map[string]bool{}
	if found {
		have := map[string]bool{}
		for _, field := range []string{"data", "binaryData"} {
			m, _, _ := unstructured.NestedMap(o.Object, field)
			for k := range m {
				have[k] = true
			}
		}
		for _, k := range append(append([]string{}, asked...), waited...) {
			if !have[k] {
				lacks[k] = true
			}
		}
	} else if n.Health != HealthBad {
		for _, k := range waited {
			lacks[k] = true
		}
	}
	if len(lacks) == 0 {
		return
	}
	keys := SortedKeys(lacks)
	words := "has no key " + keys[0]
	if len(keys) > 1 {
		words = "has no keys " + strings.Join(keys, ", ")
	}
	if found && n.Note != "" {
		words = n.Note + "; " + words
	}
	n.Note, n.Health = words, HealthBad
}

// optionalRefs are the secrets and config maps ("secret/x", "configmap/x")
// a pod spec only reads as optional: the pod starts without them.
func optionalRefs(spec map[string]any) map[string]bool {
	optional, required := map[string]bool{}, map[string]bool{}
	note := func(kind string, ref map[string]any, nameKey string) {
		name, _, _ := unstructured.NestedString(ref, nameKey)
		if name == "" {
			return
		}
		if opt, _, _ := unstructured.NestedBool(ref, "optional"); opt {
			optional[kind+"/"+name] = true
		} else {
			required[kind+"/"+name] = true
		}
	}
	sub := func(m map[string]any, path ...string) map[string]any {
		v, _, _ := unstructured.NestedMap(m, path...)
		return v
	}
	for _, key := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(spec, key)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			envFrom, _, _ := unstructured.NestedSlice(cm, "envFrom")
			for _, ef := range envFrom {
				efm, _ := ef.(map[string]any)
				note("secret", sub(efm, "secretRef"), "name")
				note("configmap", sub(efm, "configMapRef"), "name")
			}
			env, _, _ := unstructured.NestedSlice(cm, "env")
			for _, ev := range env {
				evm, _ := ev.(map[string]any)
				note("secret", sub(evm, "valueFrom", "secretKeyRef"), "name")
				note("configmap", sub(evm, "valueFrom", "configMapKeyRef"), "name")
			}
		}
	}
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, v := range volumes {
		vm, _ := v.(map[string]any)
		note("secret", sub(vm, "secret"), "secretName")
		note("configmap", sub(vm, "configMap"), "name")
		sources, _, _ := unstructured.NestedSlice(vm, "projected", "sources")
		for _, src := range sources {
			sm, _ := src.(map[string]any)
			note("secret", sub(sm, "secret"), "name")
			note("configmap", sub(sm, "configMap"), "name")
		}
	}
	for k := range required {
		delete(optional, k)
	}
	return optional
}

func podConfigMaps(spec map[string]any) []string {
	names := map[string]bool{}
	for _, key := range []string{"containers", "initContainers"} {
		containers, _, _ := unstructured.NestedSlice(spec, key)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			envFrom, _, _ := unstructured.NestedSlice(cm, "envFrom")
			for _, ef := range envFrom {
				if n, _, _ := unstructured.NestedString(ef.(map[string]any), "configMapRef", "name"); n != "" {
					names[n] = true
				}
			}
			env, _, _ := unstructured.NestedSlice(cm, "env")
			for _, ev := range env {
				if n, _, _ := unstructured.NestedString(ev.(map[string]any), "valueFrom", "configMapKeyRef", "name"); n != "" {
					names[n] = true
				}
			}
		}
	}
	volumes, _, _ := unstructured.NestedSlice(spec, "volumes")
	for _, v := range volumes {
		vm, _ := v.(map[string]any)
		if n, _, _ := unstructured.NestedString(vm, "configMap", "name"); n != "" {
			names[n] = true
		}
		sources, _, _ := unstructured.NestedSlice(vm, "projected", "sources")
		for _, src := range sources {
			if sm, ok := src.(map[string]any); ok {
				if n, _, _ := unstructured.NestedString(sm, "configMap", "name"); n != "" && n != "kube-root-ca.crt" {
					names[n] = true
				}
			}
		}
	}
	return SortedKeys(names)
}

// xrayExposedBy lists Services selecting these pod labels and the
// Ingresses / HTTPRoutes sending traffic to them.
func (e *Enricher) xrayExposedBy(ns string, labels map[string]string) []*XNode {
	var out []*XNode
	for _, svc := range e.list(kube.Services, ns) {
		sel, _, _ := unstructured.NestedStringMap(svc.Object, "spec", "selector")
		if len(sel) == 0 || !subset(sel, labels) {
			continue
		}
		n := &XNode{Kind: "Service", Name: svc.GetName(), Note: str(svc, "spec", "type")}
		n.Children = e.routesTo(ns, svc.GetName())
		out = append(out, n)
	}
	return out
}

func (e *Enricher) xrayService(svc *unstructured.Unstructured) *XNode {
	ns := svc.GetNamespace()
	node := &XNode{Kind: "Service", Name: svc.GetName(), Note: str(svc, "spec", "type")}
	sel, _, _ := unstructured.NestedStringMap(svc.Object, "spec", "selector")
	var pods []*XNode
	if len(sel) > 0 {
		for _, p := range e.list(kube.Pods, ns) {
			if subset(sel, p.GetLabels()) {
				n := &XNode{Kind: "Pod", Name: p.GetName()}
				n.Note, n.Health = workloadHealth(p)
				n.Children = containerNodes(p)
				pods = append(pods, n)
			}
		}
	}
	if len(pods) == 0 {
		pods = []*XNode{{Kind: "no pods match", Name: labelString(sel), Health: HealthBad}}
	}
	node.Children = append(node.Children, &XNode{Kind: "pods", Children: pods})
	if routes := e.routesTo(ns, svc.GetName()); len(routes) > 0 {
		node.Children = append(node.Children, &XNode{Kind: "exposed by", Children: routes})
	}
	return node
}

func (e *Enricher) routesTo(ns, svc string) []*XNode {
	var out []*XNode
	for _, ing := range e.list(kube.Ingresses, ns) {
		if ingressBackends(ing)[svc] {
			var hosts []string // its web addresses, as a route's hostnames are shown
			rules, _, _ := unstructured.NestedSlice(ing.Object, "spec", "rules")
			for _, r := range rules {
				if h, _ := r.(map[string]any)["host"].(string); h != "" && !slices.Contains(hosts, h) {
					hosts = append(hosts, h)
				}
			}
			out = append(out, &XNode{Kind: "Ingress", Name: ing.GetName(), Note: strings.Join(hosts, ",")})
		}
	}
	// Routes in any namespace: one elsewhere may send here (allowed by a
	// ReferenceGrant), as the app's box lists them. The service's own
	// namespace too, for who can't read routes cluster-wide.
	routes, seen := e.list(kube.HTTPRoutes, ""), map[types.UID]bool{}
	for _, r := range routes {
		seen[r.GetUID()] = true
	}
	for _, r := range e.list(kube.HTTPRoutes, ns) {
		if !seen[r.GetUID()] {
			routes = append(routes, r)
		}
	}
	for _, r := range routes {
		rules, _, _ := unstructured.NestedSlice(r.Object, "spec", "rules")
		hit := false
		for _, rule := range rules {
			refs, _, _ := unstructured.NestedSlice(rule.(map[string]any), "backendRefs")
			for _, ref := range refs {
				rm, _ := ref.(map[string]any)
				to, _ := rm["namespace"].(string)
				if to == "" {
					to = r.GetNamespace()
				}
				if kind, _ := rm["kind"].(string); rm["name"] == svc && to == ns && (kind == "" || kind == "Service") {
					hit = true
				}
			}
		}
		if hit {
			name := r.GetName()
			if r.GetNamespace() != ns {
				name = r.GetNamespace() + "/" + name
			}
			out = append(out, &XNode{Kind: "HTTPRoute", Name: name, Note: strings.Join(strs(r, "spec", "hostnames"), ",")})
		}
	}
	return out
}

// ingressBackends are the Service names an Ingress sends traffic to.
func ingressBackends(ing *unstructured.Unstructured) map[string]bool {
	out := map[string]bool{}
	if n := str(ing, "spec", "defaultBackend", "service", "name"); n != "" {
		out[n] = true
	}
	rules, _, _ := unstructured.NestedSlice(ing.Object, "spec", "rules")
	for _, r := range rules {
		paths, _, _ := unstructured.NestedSlice(r.(map[string]any), "http", "paths")
		for _, p := range paths {
			if n, _, _ := unstructured.NestedString(p.(map[string]any), "backend", "service", "name"); n != "" {
				out[n] = true
			}
		}
	}
	return out
}

// xrayGuards lists HPAs targeting the workload and PDBs covering its pods.
func (e *Enricher) xrayGuards(root *unstructured.Unstructured, labels map[string]string) []*XNode {
	ns := root.GetNamespace()
	var out []*XNode
	for _, h := range e.list(kube.HPAs, ns) {
		if str(h, "spec", "scaleTargetRef", "name") == root.GetName() && str(h, "spec", "scaleTargetRef", "kind") == root.GetKind() {
			out = append(out, &XNode{Kind: "Autoscaler", Name: h.GetName(), Note: fmt.Sprintf("%d–%d copies, now %d",
				num(h, "spec", "minReplicas"), num(h, "spec", "maxReplicas"), num(h, "status", "currentReplicas"))})
		}
	}
	for _, p := range e.list(kube.PDBs, ns) {
		sel, _, _ := unstructured.NestedStringMap(p.Object, "spec", "selector", "matchLabels")
		if len(sel) > 0 && subset(sel, labels) {
			n := &XNode{Kind: "Disruption budget", Name: p.GetName(), Note: podsMayBeDown(num(p, "status", "disruptionsAllowed"))}
			if num(p, "status", "disruptionsAllowed") == 0 {
				n.Health = HealthWarn
			}
			out = append(out, n)
		}
	}
	return out
}

// BudgetsAllowingNone names the disruption budgets that cover a ready pod
// (their selector matches it) and allow no pod down now, by their current
// status: an eviction (a drain) of it waits on them; a delete doesn't.
func (e *Enricher) BudgetsAllowingNone(pod *unstructured.Unstructured) []string {
	if pod.GetKind() != "Pod" || !podReadyNow(pod) {
		return nil
	}
	var out []string
	for _, p := range e.list(kube.PDBs, pod.GetNamespace()) {
		allowed, found := number(p, "status", "disruptionsAllowed")
		if !found || allowed != 0 || num(p, "status", "observedGeneration") < p.GetGeneration() {
			continue
		}
		raw, found, _ := unstructured.NestedMap(p.Object, "spec", "selector")
		if !found {
			continue // no selector: it covers no pod
		}
		var ls metav1.LabelSelector
		if runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &ls) != nil {
			continue
		}
		if sel, err := metav1.LabelSelectorAsSelector(&ls); err == nil && sel.Matches(labels.Set(pod.GetLabels())) {
			out = append(out, p.GetName())
		}
	}
	sort.Strings(out)
	return out
}

// podReadyNow: the pod's Ready condition is true.
func podReadyNow(pod *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(pod.Object, "status", "conditions")
	for _, c := range conds {
		if m, _ := c.(map[string]any); m["type"] == "Ready" && m["status"] == "True" {
			return true
		}
	}
	return false
}

// workloadHealth summarises readiness of a workload or pod.
func workloadHealth(o *unstructured.Unstructured) (string, Health) {
	switch o.GetKind() {
	case "Pod":
		phase := str(o, "status", "phase")
		if reason := podProblem(o); reason != "" {
			return reason, HealthBad
		}
		if phase == "Succeeded" {
			return "Completed", HealthNone
		}
		ready, total := podReady(o)
		if phase == "Running" && ready == total {
			return fmt.Sprintf("Running %d/%d", ready, total), HealthOK
		}
		return fmt.Sprintf("%s %d/%d", phase, ready, total), HealthWarn
	case "DaemonSet":
		return readyNote(num(o, "status", "numberReady"), num(o, "status", "desiredNumberScheduled"))
	case "Job":
		if num(o, "status", "failed") > 0 && num(o, "status", "succeeded") == 0 {
			return "failed", HealthBad
		}
		if num(o, "status", "succeeded") > 0 {
			return "succeeded", HealthOK
		}
		return "running", HealthWarn
	case "CronJob":
		if s, _, _ := unstructured.NestedBool(o.Object, "spec", "suspend"); s {
			return "suspended", HealthNone
		}
		return str(o, "spec", "schedule"), HealthNone
	}
	return readyNote(num(o, "status", "readyReplicas"), num(o, "spec", "replicas"))
}

func readyNote(ready, want int64) (string, Health) {
	note := fmt.Sprintf("%d/%d ready", ready, want)
	switch {
	case want == 0:
		return "scaled to 0", HealthNone
	case ready == 0:
		return note, HealthBad
	case ready < want:
		return note, HealthWarn
	}
	return note, HealthOK
}

func podReady(o *unstructured.Unstructured) (ready, total int) {
	statuses := runningStatuses(o)
	for _, s := range statuses {
		total++
		if m, _ := s.(map[string]any); m["ready"] == true {
			ready++
		}
	}
	return ready, total
}

// runningStatuses are the container statuses of a pod's containers plus
// its helpers declared as init containers that keep running.
func runningStatuses(pod *unstructured.Unstructured) []any {
	statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
	inits, _, _ := unstructured.NestedSlice(pod.Object, "spec", "initContainers")
	helper := map[string]bool{}
	for _, c := range inits {
		if m, ok := c.(map[string]any); ok && m["restartPolicy"] == "Always" {
			helper[fmt.Sprint(m["name"])] = true
		}
	}
	initStatuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
	for _, st := range initStatuses {
		if m, ok := st.(map[string]any); ok && helper[fmt.Sprint(m["name"])] {
			statuses = append(statuses, st)
		}
	}
	return statuses
}

// badWaiting are container waiting reasons that mean the pod is broken.
var badWaiting = map[string]bool{
	"CrashLoopBackOff": true, "ImagePullBackOff": true, "ErrImagePull": true,
	"CreateContainerConfigError": true, "CreateContainerError": true, "InvalidImageName": true,
}

// podProblem returns why a pod is broken, or "".
func podProblem(o *unstructured.Unstructured) string {
	if r := str(o, "status", "reason"); r == "Evicted" {
		return r
	}
	for _, key := range []string{"initContainerStatuses", "containerStatuses"} {
		statuses, _, _ := unstructured.NestedSlice(o.Object, "status", key)
		for _, s := range statuses {
			m, _ := s.(map[string]any)
			if w, _, _ := unstructured.NestedString(m, "state", "waiting", "reason"); badWaiting[w] {
				return w
			}
		}
	}
	if str(o, "status", "phase") == "Failed" {
		return "Failed"
	}
	return ""
}

func subset(sel, labels map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func labelString(l map[string]string) string {
	parts := make([]string, 0, len(l))
	for k, v := range l {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func str(o *unstructured.Unstructured, fields ...string) string {
	v, _, _ := unstructured.NestedFieldNoCopy(o.Object, fields...)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func strs(o *unstructured.Unstructured, fields ...string) []string {
	v, _, _ := unstructured.NestedStringSlice(o.Object, fields...)
	return v
}

func num(o *unstructured.Unstructured, fields ...string) int64 {
	v, _, _ := unstructured.NestedFieldNoCopy(o.Object, fields...)
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

// podsMayBeDown says what a PodDisruptionBudget allows right now.
func podsMayBeDown(n int64) string {
	switch n {
	case 0:
		return "no pod may be taken down right now"
	case 1:
		return "1 pod may be taken down at a time"
	}
	return fmt.Sprintf("%d pods may be taken down at a time", n)
}

// PodConfigRefs are the ConfigMaps and Secrets a pod uses, as
// "configmap/name" and "secret/name" (plus "externalsecret/name" for the
// ExternalSecret that usually shares a Secret's name).
func PodConfigRefs(pod *unstructured.Unstructured) []string {
	var out []string
	spec, _, _ := unstructured.NestedMap(pod.Object, "spec")
	for _, n := range podConfigMaps(spec) {
		out = append(out, "configmap/"+n)
	}
	for _, n := range SortedKeys(PodSecretNames(pod)) {
		out = append(out, "secret/"+n, "externalsecret/"+n)
	}
	return out
}

// ageWords is a duration in short words, as lists write ages: 28s, 45m, 5h, 3d.
func ageWords(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", max(int(d.Seconds()), 1))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// missingNote is what X says of something a pod uses that isn't in the
// cache: missing once that kind has loaded, else not known yet (a list
// still loading isn't proof that it doesn't exist).
func (e *Enricher) missingNote(gvr schema.GroupVersionResource) (string, Health) {
	if _, ok := e.served(gvr); !ok {
		return "kubed can't see these on this cluster", HealthNone
	}
	if es, ok := e.store.(interface {
		Err(schema.GroupVersionResource) error
	}); ok {
		// Not allowed, or pick a namespace (a timeout or a busy server
		// passes, and the list loads after all)
		if err := es.Err(gvr); err != nil && !e.Synced(gvr) && (apierrors.IsForbidden(err) || errors.Is(err, kube.ErrPickNamespace)) {
			return "kubed can't read these here, so it can't tell", HealthNone
		}
	}
	if !e.Synced(gvr) {
		return "not loaded yet; X again in a moment", HealthNone
	}
	return "missing", HealthBad
}
