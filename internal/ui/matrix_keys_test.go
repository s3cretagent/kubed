package ui

// Case map for area D: every key on every screen.
//
// Dimensions that change what a key must do or what kubed must say:
//
//  1. Screen (location kind): home (apps), each resource list kubed has a
//     view for (pods, deployments, statefulsets, daemonsets, replicasets,
//     jobs, cronjobs, services, configmaps, secrets, serviceaccounts,
//     events, ingresses, httproutes, nodes, namespaces, nodepools,
//     nodeclaims, externalsecrets, ArgoCD applications, pvcs, hpas, pdbs),
//     a pod's containers, find results (typing, and back on them), :pf,
//     the ctrl-a kind picker, :waste, Helm, text screens (help, YAML-like).
//  2. List state: rows; empty (the cluster has none); still loading (the
//     list never answers); a filter that keeps rows; a filter that keeps
//     none.
//  3. Picks: none; exactly the cursor row; another row; every row.
//  4. How you got there: jumped (no trail); drilled (a trail); opened
//     from a search result (esc goes back to the results).
//  5. Access: read-write; --readonly. Cluster: production (typed names)
//     or not.
//  6. Screen size: 80x24 and 200x50.
//
// Keys: every key the guide lists (moving, enter, esc, q, ?, f, /, :, g
// and each jump, 0-9, sorts, ctrl-s, space, *, ctrl-a) and every action
// key in registry.go.
//
// For each screen x state x key the truth checked is:
//   - kubed never panics or hangs.
//   - an action key either acts or says why not (an error line); never
//     silence.
//   - a key the footer offers works (no refusal) when pressed.
//   - the footer's esc hint names what esc does (back, clear the filter,
//     clear the picks, back to the results, home), and enter's hint is
//     there only when enter does something.
//   - the enter menu: an item shown enabled is one whose key doesn't
//     refuse; a key that refuses is greyed with a reason ("Can't…") or
//     not offered; every greyed item says why.
//   - with rows picked, a one-row key refuses and names the picks; a bulk
//     key acts on the picks, not the cursor row.
//   - read-only: a change key, a shell, secret values and ports refuse
//     with the read-only reason, and the footer never offers them.
//
// Cases: 31 screens x 11 states x 2 sizes x ~70 keys, plus the guide's
// promises (keys table, picks paragraph, find/esc paragraph, read-only
// list) as separate tests below.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// mxKinds is every list kind kubed has, registered with the fake cluster
// so no reflector panics on an unknown list.
var mxKinds = []kube.Resource{
	{GVR: kube.Pods, Kind: "Pod", Namespaced: true, ShortNames: []string{"po"}, Singular: "pod"},
	{GVR: kube.Nodes, Kind: "Node", ShortNames: []string{"no"}, Singular: "node"},
	{GVR: kube.Namespaces, Kind: "Namespace", ShortNames: []string{"ns"}, Singular: "namespace"},
	{GVR: kube.Events, Kind: "Event", Namespaced: true, ShortNames: []string{"ev"}, Singular: "event"},
	{GVR: kube.Services, Kind: "Service", Namespaced: true, ShortNames: []string{"svc"}, Singular: "service"},
	{GVR: kube.ConfigMaps, Kind: "ConfigMap", Namespaced: true, ShortNames: []string{"cm"}, Singular: "configmap"},
	{GVR: kube.Secrets, Kind: "Secret", Namespaced: true, Singular: "secret"},
	{GVR: kube.ServiceAccounts, Kind: "ServiceAccount", Namespaced: true, ShortNames: []string{"sa"}, Singular: "serviceaccount"},
	{GVR: kube.Deployments, Kind: "Deployment", Namespaced: true, ShortNames: []string{"deploy"}, Singular: "deployment"},
	{GVR: kube.StatefulSets, Kind: "StatefulSet", Namespaced: true, ShortNames: []string{"sts"}, Singular: "statefulset"},
	{GVR: kube.DaemonSets, Kind: "DaemonSet", Namespaced: true, ShortNames: []string{"ds"}, Singular: "daemonset"},
	{GVR: kube.ReplicaSets, Kind: "ReplicaSet", Namespaced: true, ShortNames: []string{"rs"}, Singular: "replicaset"},
	{GVR: kube.ControllerRevisions, Kind: "ControllerRevision", Namespaced: true, Singular: "controllerrevision"},
	{GVR: kube.Jobs, Kind: "Job", Namespaced: true, Singular: "job"},
	{GVR: kube.CronJobs, Kind: "CronJob", Namespaced: true, ShortNames: []string{"cj"}, Singular: "cronjob"},
	{GVR: kube.PVCs, Kind: "PersistentVolumeClaim", Namespaced: true, ShortNames: []string{"pvc"}, Singular: "persistentvolumeclaim"},
	{GVR: kube.HPAs, Kind: "HorizontalPodAutoscaler", Namespaced: true, ShortNames: []string{"hpa"}, Singular: "horizontalpodautoscaler"},
	{GVR: kube.PDBs, Kind: "PodDisruptionBudget", Namespaced: true, ShortNames: []string{"pdb"}, Singular: "poddisruptionbudget"},
	{GVR: kube.Ingresses, Kind: "Ingress", Namespaced: true, ShortNames: []string{"ing"}, Singular: "ingress"},
	{GVR: kube.HTTPRoutes, Kind: "HTTPRoute", Namespaced: true, Singular: "httproute"},
	{GVR: kube.NodePools, Kind: "NodePool", Singular: "nodepool"},
	{GVR: kube.NodeClaims, Kind: "NodeClaim", Singular: "nodeclaim"},
	{GVR: kube.ExternalSecrets, Kind: "ExternalSecret", Namespaced: true, ShortNames: []string{"es"}, Singular: "externalsecret"},
	{GVR: kube.Applications, Kind: "Application", Namespaced: true, ShortNames: []string{"app"}, Singular: "application"},
}

func mxObj(apiVersion, kind, ns, name string, fields map[string]any) *unstructured.Unstructured {
	md := map[string]any{"name": name, "uid": kind + "-" + name, "creationTimestamp": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)}
	if ns != "" {
		md["namespace"] = ns
	}
	o := map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": md}
	for k, v := range fields {
		if k == "metadata" {
			for mk, mv := range v.(map[string]any) {
				md[mk] = mv
			}
			continue
		}
		o[k] = v
	}
	return &unstructured.Unstructured{Object: o}
}

func mxOwner(apiVersion, kind, name string) map[string]any {
	return map[string]any{"ownerReferences": []any{map[string]any{"apiVersion": apiVersion, "kind": kind, "name": name, "uid": kind + "-" + name, "controller": true}}}
}

func mxPod(name, ownerAPI, ownerKind, owner, phase string, restarts int64, labels map[string]any) *unstructured.Unstructured {
	md := map[string]any{"labels": labels}
	if owner != "" {
		md["ownerReferences"] = mxOwner(ownerAPI, ownerKind, owner)["ownerReferences"]
	}
	ready := phase == "Running"
	state := map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339)}}
	if phase == "Succeeded" {
		state = map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}}
	}
	return mxObj("v1", "Pod", "shop", name, map[string]any{"metadata": md,
		"spec": map[string]any{"nodeName": "node-1", "serviceAccountName": "default",
			"containers": []any{map[string]any{"name": "app", "image": "shop/" + name + ":1", "ports": []any{map[string]any{"containerPort": int64(8080)}}}}},
		"status": map[string]any{"phase": phase, "containerStatuses": []any{map[string]any{"name": "app", "ready": ready, "restartCount": restarts,
			"image": "shop/" + name + ":1", "state": state}}}})
}

// mxCS is one container's status: its restarts, and how it is now
// ("running", "done" for exit 0, "crashed" for exit 1).
type mxCS struct {
	name     string
	restarts int64
	state    string
}

func (c mxCS) status() map[string]any {
	state := map[string]any{"running": map[string]any{"startedAt": time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)}}
	switch c.state {
	case "done":
		state = map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed"}}
	case "crashed":
		state = map[string]any{"terminated": map[string]any{"exitCode": int64(1), "reason": "Error"}}
	}
	return map[string]any{"name": c.name, "restartCount": c.restarts, "ready": c.state == "running", "image": "shop/" + c.name + ":1", "state": state}
}

// mxPodWith is a running pod of web with setup steps and several
// containers, for the keys that pick a container (p, l, s).
func mxPodWith(name string, inits, containers []mxCS) *unstructured.Unstructured {
	var specInit, specC, statInit, statC []any
	for _, c := range inits {
		specInit = append(specInit, map[string]any{"name": c.name, "image": "shop/" + c.name + ":1"})
		statInit = append(statInit, c.status())
	}
	for _, c := range containers {
		specC = append(specC, map[string]any{"name": c.name, "image": "shop/" + c.name + ":1"})
		statC = append(statC, c.status())
	}
	return mxObj("v1", "Pod", "shop", name, map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "web"},
		"ownerReferences": mxOwner("apps/v1", "ReplicaSet", "web-abc")["ownerReferences"]},
		"spec":   map[string]any{"nodeName": "node-1", "initContainers": specInit, "containers": specC},
		"status": map[string]any{"phase": "Running", "initContainerStatuses": statInit, "containerStatuses": statC}})
}

// mxEarlierRun is the truth p is checked against: a container has a run
// that ended to show when it restarted (the run before), or when it is
// stopped right now and isn't a setup step that finished as it should.
func mxEarlierRun(pod *unstructured.Unstructured, container string) bool {
	for _, field := range []string{"initContainerStatuses", "containerStatuses"} {
		statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", field)
		for _, st := range statuses {
			c := st.(map[string]any)
			if c["name"] != container {
				continue
			}
			if n, _, _ := unstructured.NestedInt64(c, "restartCount"); n > 0 {
				return true
			}
			code, stopped, _ := unstructured.NestedInt64(c, "state", "terminated", "exitCode")
			if _, ok, _ := unstructured.NestedMap(c, "state", "terminated"); ok {
				stopped = true
			}
			return stopped && (field != "initContainerStatuses" || code != 0)
		}
	}
	return false
}

func mxTemplate(labels map[string]any) map[string]any {
	return map[string]any{"metadata": map[string]any{"labels": labels},
		"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "shop/web:1", "ports": []any{map[string]any{"containerPort": int64(8080)}}}}}}
}

// mxFixtures is a small shop: one app of each workload kind, and one of
// everything around it.
func mxFixtures() []runtime.Object {
	web := map[string]any{"app": "web"}
	db := map[string]any{"app": "db"}
	agent := map[string]any{"app": "agent"}
	return []runtime.Object{
		mxObj("v1", "Namespace", "", "shop", nil),
		mxObj("v1", "Node", "", "node-1", map[string]any{"status": map[string]any{
			"conditions":  []any{map[string]any{"type": "Ready", "status": "True"}},
			"allocatable": map[string]any{"cpu": "4", "memory": "16Gi", "pods": "110"}, "capacity": map[string]any{"cpu": "4", "memory": "16Gi", "pods": "110"}}}),
		mxObj("apps/v1", "Deployment", "shop", "web", map[string]any{"metadata": map[string]any{"generation": int64(2), "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"spec":   map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": web}, "template": mxTemplate(web)},
			"status": map[string]any{"replicas": int64(2), "readyReplicas": int64(2), "updatedReplicas": int64(2), "availableReplicas": int64(2), "observedGeneration": int64(2)}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "web-abc", map[string]any{"metadata": map[string]any{"labels": web, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": web}, "template": mxTemplate(web)}, "status": map[string]any{"replicas": int64(2), "readyReplicas": int64(2)}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "web-old", map[string]any{"metadata": map[string]any{"labels": web, "annotations": map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(0), "selector": map[string]any{"matchLabels": web},
				"template": map[string]any{"metadata": map[string]any{"labels": web}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "shop/web:0"}}}}}}),
		mxObj("apps/v1", "ControllerRevision", "shop", "db-1", map[string]any{"metadata": mxOwner("apps/v1", "StatefulSet", "db"), "revision": int64(1)}),
		mxObj("apps/v1", "ControllerRevision", "shop", "agent-1", map[string]any{"metadata": mxOwner("apps/v1", "DaemonSet", "agent"), "revision": int64(1)}),
		mxPod("web-abc-1", "apps/v1", "ReplicaSet", "web-abc", "Running", 2, web),
		mxPod("web-abc-2", "apps/v1", "ReplicaSet", "web-abc", "Running", 0, web),
		// A mesh's setup step finished, nothing restarted: no earlier logs.
		mxPodWith("web-mesh", []mxCS{{"istio-init", 0, "done"}}, []mxCS{{"app", 0, "running"}, {"istio-proxy", 0, "running"}}),
		// Only the helper restarted; a setup step finished.
		mxPodWith("web-multi", []mxCS{{"migrate", 0, "done"}}, []mxCS{{"app", 0, "running"}, {"log-shipper", 3, "running"}}),
		// Crashed just now, before its first restart: the run that ended.
		mxPodWith("web-crash", nil, []mxCS{{"app", 0, "crashed"}, {"istio-proxy", 0, "running"}}),
		// A setup step that failed and is retried: its earlier attempt.
		mxPodWith("web-initfail", []mxCS{{"migrate", 2, "crashed"}}, []mxCS{{"app", 0, "running"}}),
		mxObj("apps/v1", "Deployment", "shop", "api", map[string]any{"metadata": map[string]any{"annotations": map[string]any{"deployment.kubernetes.io/revision": "1"}},
			"spec":   map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "api"}}, "template": mxTemplate(map[string]any{"app": "api"})},
			"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(0), "updatedReplicas": int64(1)}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "api-1", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "api"}, "annotations": map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "api")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "api"}}, "template": mxTemplate(map[string]any{"app": "api"})}}),
		mxObj("v1", "Pod", "shop", "api-1-x", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "api"}, "ownerReferences": mxOwner("apps/v1", "ReplicaSet", "api-1")["ownerReferences"]},
			"spec":   map[string]any{"nodeName": "node-1", "containers": []any{map[string]any{"name": "app", "image": "shop/api:1"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{mxCS{"app", 4, "crashed"}.status()}}}),
		// Paused after two versions: a go-back or a new image waits for resume.
		mxObj("apps/v1", "Deployment", "shop", "held", map[string]any{"metadata": map[string]any{"annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"spec":   map[string]any{"paused": true, "replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "held"}}, "template": mxTemplate(map[string]any{"app": "held"})},
			"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1), "updatedReplicas": int64(1)}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "held-2", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "held"}, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "held")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "held"}}, "template": mxTemplate(map[string]any{"app": "held"})}}),
		mxObj("apps/v1", "ReplicaSet", "shop", "held-1", map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "held"}, "annotations": map[string]any{"deployment.kubernetes.io/revision": "1"},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "held")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(0), "selector": map[string]any{"matchLabels": map[string]any{"app": "held"}},
				"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "held"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "shop/web:0"}}}}}}),
		// Made paused: Kubernetes makes no ReplicaSet, so no pod, until resumed.
		mxObj("apps/v1", "Deployment", "shop", "paused", map[string]any{
			"spec":   map[string]any{"paused": true, "replicas": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "paused"}}, "template": mxTemplate(map[string]any{"app": "paused"})},
			"status": map[string]any{}}),
		mxObj("apps/v1", "StatefulSet", "shop", "db", map[string]any{"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": db}, "template": mxTemplate(db)},
			"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}}),
		mxPod("db-0", "apps/v1", "StatefulSet", "db", "Running", 0, db),
		mxObj("apps/v1", "DaemonSet", "shop", "agent", map[string]any{"spec": map[string]any{"selector": map[string]any{"matchLabels": agent}, "template": mxTemplate(agent)},
			"status": map[string]any{"desiredNumberScheduled": int64(1), "numberReady": int64(1)}}),
		mxPod("agent-x", "apps/v1", "DaemonSet", "agent", "Running", 0, agent),
		mxObj("batch/v1", "CronJob", "shop", "nightly", map[string]any{"spec": map[string]any{"schedule": "0 1 * * *",
			"jobTemplate": map[string]any{"spec": map[string]any{"template": mxTemplate(map[string]any{"app": "nightly"})}}}}),
		mxObj("batch/v1", "Job", "shop", "nightly-1", map[string]any{"metadata": mxOwner("batch/v1", "CronJob", "nightly"),
			"spec": map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "nightly"}}, "template": mxTemplate(map[string]any{"app": "nightly"})}, "status": map[string]any{"succeeded": int64(1)}}),
		mxPod("nightly-1-p", "batch/v1", "Job", "nightly-1", "Succeeded", 0, map[string]any{"app": "nightly"}),
		mxObj("v1", "Service", "shop", "web", map[string]any{"spec": map[string]any{"selector": web, "ports": []any{map[string]any{"port": int64(80), "targetPort": int64(8080)}}}}),
		mxObj("v1", "ConfigMap", "shop", "web-config", map[string]any{"data": map[string]any{"a": "b"}}),
		mxObj("v1", "Secret", "shop", "web-secret", map[string]any{"data": map[string]any{"k": "dg=="}}),
		mxObj("v1", "ServiceAccount", "shop", "default", nil),
		mxObj("v1", "Event", "shop", "web-abc-1.1", map[string]any{"type": "Warning", "reason": "BackOff", "message": "Back-off restarting failed container",
			"involvedObject": map[string]any{"apiVersion": "v1", "kind": "Pod", "name": "web-abc-1", "namespace": "shop"},
			"lastTimestamp":  time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}),
		mxObj("networking.k8s.io/v1", "Ingress", "shop", "web", map[string]any{"spec": map[string]any{"rules": []any{map[string]any{"host": "web.example.com",
			"http": map[string]any{"paths": []any{map[string]any{"path": "/", "backend": map[string]any{"service": map[string]any{"name": "web"}}}}}}}}}),
		mxObj("gateway.networking.k8s.io/v1", "HTTPRoute", "shop", "web-route", map[string]any{"spec": map[string]any{"hostnames": []any{"r.example.com"},
			"rules": []any{map[string]any{"backendRefs": []any{map[string]any{"name": "web", "port": int64(80)}}}}}}),
		mxObj("external-secrets.io/v1", "ExternalSecret", "shop", "web-es", map[string]any{"spec": map[string]any{"target": map[string]any{"name": "web-secret"}}}),
		mxObj("argoproj.io/v1alpha1", "Application", "shop", "web", map[string]any{"spec": map[string]any{"destination": map[string]any{"namespace": "shop"}},
			"status": map[string]any{"sync": map[string]any{"status": "Synced"}, "health": map[string]any{"status": "Healthy"},
				"resources": []any{map[string]any{"kind": "Deployment", "name": "web", "namespace": "shop", "status": "Synced"}}}}),
		mxObj("karpenter.sh/v1", "NodePool", "", "default", nil),
		mxObj("karpenter.sh/v1", "NodeClaim", "", "default-x", nil),
	}
}

// mxCluster is one fake cluster, its cache and enricher, shared by every
// model built on it (each key press gets a model of its own).
type mxCluster struct {
	client *kube.Client
	enr    *enrich.Enricher
}

var (
	mxOnce                  sync.Once
	mxFull, mxEmpty, mxWait *mxCluster
)

// hangAll is a cluster where no list ever answers: every screen loads.
type hangAll struct{ dynamic.Interface }

func (h hangAll) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return hangingList{h.Interface.Resource(gvr)}
}

func mxNewCluster(objs []runtime.Object, hang bool) *mxCluster {
	return mxNewClusterHanging(objs, hang, nil)
}

// mxNewClusterHanging: hangs lists never answer (all of them with hang).
func mxNewClusterHanging(objs []runtime.Object, hang bool, hangs []schema.GroupVersionResource) *mxCluster {
	listKinds := map[schema.GroupVersionResource]string{}
	for _, r := range mxKinds {
		listKinds[r.GVR] = r.Kind + "List"
	}
	res := make([]kube.Resource, len(mxKinds))
	for i, r := range mxKinds {
		r.Verbs = []string{"get", "list", "watch", "create", "update", "patch", "delete"}
		res[i] = r
	}
	resources := kube.NewResources(res)
	var dyn dynamic.Interface = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
	if hang {
		dyn = hangAll{dyn}
	}
	for _, g := range hangs {
		dyn = slowList{dyn, g}
	}
	c := &mxCluster{client: &kube.Client{Resources: resources, Cache: kube.NewCache(dyn), Dynamic: dyn, Typed: kubefake.NewSimpleClientset()}}
	c.enr = enrich.New(c.client.Cache, resources, nil)
	for _, r := range mxKinds {
		c.client.Cache.Watch(r.GVR)
	}
	if !hang {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			all := true
			for _, r := range mxKinds {
				all = all && (c.client.Cache.Synced(r.GVR) || slices.Contains(hangs, r.GVR))
			}
			if all {
				break
			}
		}
	}
	return c
}

func mxClusters() {
	mxOnce.Do(func() {
		mxFull = mxNewCluster(mxFixtures(), false)
		mxEmpty = mxNewCluster(nil, false)
		mxWait = mxNewCluster(nil, true)
	})
}

// mxModel is a fresh model on a shared cluster, in the namespace-wide view.
func mxModel(c *mxCluster, w, h int) *Model {
	c.client.Cache.SetNamespace("")
	m := &Model{client: c.client, enr: c.enr, forwards: &kube.Forwards{}, width: w, height: h,
		env: environment{known: true}, connectedAt: time.Now().Add(-time.Hour)}
	return m
}

// mxScreen opens one screen.
type mxScreen struct {
	name string
	open func(m *Model)
}

func mxScreens() []mxScreen {
	s := []mxScreen{
		{"home", func(m *Model) { m.jumpTo(location{kind: locHome}) }},
		{"containers", func(m *Model) {
			m.jumpTo(location{kind: locHome})
			m.openResource("pods", true)
			m.navigate(location{kind: locContainers, podNS: "shop", podName: "web-abc-1"}, true)
		}},
		{"find-results", func(m *Model) {
			m.jumpTo(location{kind: locHome})
			m.startFind()
			for _, r := range "web" {
				m.onFindKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
			}
			m.mode = modeTable // back on the results, as after esc from one
		}},
		{"pf", func(m *Model) { m.jumpTo(location{kind: locHome}); m.jumpTo(location{kind: locForwards}) }},
		{"kinds", func(m *Model) { m.jumpTo(location{kind: locHome}); m.jumpTo(location{kind: locResources}) }},
		{"waste", func(m *Model) { m.jumpTo(location{kind: locHome}); m.jumpTo(location{kind: locWaste}) }},
		{"helm", func(m *Model) {
			m.jumpTo(location{kind: locHome})
			m.helm = []kube.HelmRelease{{Name: "web", Namespace: "shop", Status: "deployed", Revision: 3}}
			m.helmAt, m.helmNS = time.Now(), m.namespace
			m.jumpTo(location{kind: locHelm})
		}},
	}
	for _, r := range mxKinds {
		if r.GVR == kube.ControllerRevisions {
			continue
		}
		r := r
		s = append(s, mxScreen{r.GVR.Resource, func(m *Model) {
			m.jumpTo(location{kind: locHome})
			m.openResource(r.GVR.Resource+"."+r.GVR.Group, false)
		}})
	}
	return s
}

// mxState sets the list's state after the screen opened.
type mxState struct {
	name    string
	cluster func() *mxCluster
	set     func(m *Model)
}

func mxStates() []mxState {
	full := func() *mxCluster { return mxFull }
	return []mxState{
		{"rows", full, func(*Model) {}},
		{"rows-last", full, func(m *Model) { m.tbl.jump(len(m.tbl.rows) - 1) }},
		{"empty", func() *mxCluster { return mxEmpty }, func(*Model) {}},
		{"loading", func() *mxCluster { return mxWait }, func(*Model) {}},
		{"filter", full, func(m *Model) { m.tbl.filter = "web"; m.refreshRows() }},
		{"filter-none", full, func(m *Model) { m.tbl.filter = "zzzz"; m.refreshRows() }},
		{"pick-cursor", full, func(m *Model) { m.tbl.toggleMark(); m.refreshRows() }},
		{"pick-other", full, func(m *Model) {
			if len(m.tbl.rows) > 1 {
				m.tbl.jump(1)
				m.tbl.toggleMark()
				m.tbl.jump(0)
				m.refreshRows()
			}
		}},
		{"pick-all", full, func(m *Model) { m.tbl.markAll(); m.refreshRows() }},
		{"readonly", full, func(m *Model) { m.opts.ReadOnly = true }},
		{"prod", full, func(m *Model) { m.env = environment{known: true, prod: true} }},
	}
}

// mxKeys: every key the guide lists, and every action key.
func mxKeys() []string {
	keys := []string{"up", "down", "j", "k", "pgup", "pgdown", "ctrl+b", "ctrl+f", "home", "end", "G", "enter", "esc", "?",
		"f", "/", ":", "0", "1", "N", "A", "R", "C", "M", "$", "ctrl+s", " ", "*", "ctrl+a", "b"}
	for _, j := range []string{"g", "h", "p", "d", "s", "n", "e", "a", "x", "N", "c", "S", "i", "j", "C", "H", "z"} {
		keys = append(keys, "g"+j)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	for _, a := range actions {
		if a.key != "" && !seen[a.key] {
			seen[a.key] = true
			keys = append(keys, a.key)
		}
	}
	return keys
}

func mxKeyMsg(k string) tea.KeyMsg {
	switch k {
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "pgup":
		return tea.KeyMsg{Type: tea.KeyPgUp}
	case "pgdown":
		return tea.KeyMsg{Type: tea.KeyPgDown}
	case "home":
		return tea.KeyMsg{Type: tea.KeyHome}
	case "end":
		return tea.KeyMsg{Type: tea.KeyEnd}
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	case " ":
		return tea.KeyMsg{Type: tea.KeySpace, Runes: []rune{' '}}
	case "ctrl+b":
		return tea.KeyMsg{Type: tea.KeyCtrlB}
	case "ctrl+f":
		return tea.KeyMsg{Type: tea.KeyCtrlF}
	case "ctrl+s":
		return tea.KeyMsg{Type: tea.KeyCtrlS}
	case "ctrl+a":
		return tea.KeyMsg{Type: tea.KeyCtrlA}
	case "ctrl+k":
		return tea.KeyMsg{Type: tea.KeyCtrlK}
	case "ctrl+d":
		return tea.KeyMsg{Type: tea.KeyCtrlD}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(k)}
}

// mxPress presses a key (g jumps as two presses) and reports a panic or
// a hang as an error.
func mxPress(m *Model, k string) (cmd tea.Cmd, err error) {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v\n%s", r, debug.Stack())
			}
		}()
		if len(k) == 2 && k[0] == 'g' {
			m.onKey(mxKeyMsg("g"))
			cmd = m.onKey(mxKeyMsg(k[1:]))
		} else {
			cmd = m.onKey(mxKeyMsg(k))
		}
		_ = m.View()
		done <- nil
	}()
	select {
	case err = <-done:
		return cmd, err
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("hung")
	}
}

// mxSnap is what a key press can visibly change.
type mxSnap struct {
	where, filter, flash string
	mode                 mode
	hist, cursor, marks  int
	sort                 sortMode
	desc, prompt, pager  bool
	ns                   string
}

func mxSnapOf(m *Model) mxSnap {
	return mxSnap{where: fmt.Sprintf("%d/%s/%s/%s", m.loc.kind, m.loc.res.GVR, m.loc.podName, m.loc.query), filter: m.tbl.filter, flash: m.flash,
		mode: m.mode, hist: len(m.history), cursor: m.tbl.cursor, marks: len(m.tbl.marked()), sort: m.tbl.sort, desc: m.tbl.desc,
		prompt: m.prompt != nil, pager: m.pager != nil, ns: m.namespace}
}

// mxCheckP: what p opened (or offers to open) is a run that ended,
// never one the API has no earlier logs for ("previous terminated
// container not found").
func mxCheckP(m *Model, where string, fail func(string, ...any)) {
	switch {
	case m.pager != nil && m.pager.isLogs:
		src := m.pager.source
		pod, ok := m.client.Cache.Get(kube.Pods, src.ns, src.pod)
		if !ok {
			return
		}
		if !mxEarlierRun(pod, src.container) {
			fail("%s: p opened %s's %s, which has no run that ended (previous=%v, stopped=%v)", where, src.pod, src.container, src.previous, src.stoppedRun)
		}
		if src.previous && view.ContainerRestarts(pod)[src.container] == 0 {
			fail("%s: p asked for %s's %s before a restart, and it never restarted", where, src.pod, src.container)
		}
	case m.prompt != nil && strings.HasPrefix(m.prompt.title, "Which container of "):
		name := strings.TrimSuffix(strings.TrimPrefix(m.prompt.title, "Which container of "), "?")
		pod, ok := m.client.Cache.Get(kube.Pods, "shop", name)
		if !ok {
			return
		}
		for _, it := range m.prompt.items {
			if c := strings.Fields(it.label)[0]; !mxEarlierRun(pod, c) {
				fail("%s: p offers %s's %s, which has no run that ended", where, name, c)
			}
		}
	}
}

// mxCheckClaims: a menu item's description is true of the rows it acts
// on. A bulk item with rows picked names the picks, not "it"; "stays up"
// only where every deployment it restarts has a copy ready and isn't
// paused.
func mxCheckClaims(m *Model, it menuItem, where string, fail func(string, ...any)) {
	row, ok := m.tbl.selected()
	if !ok || it.disabled {
		return
	}
	var a action
	for _, x := range actions {
		if x.key == it.key && x.key != "" && x.when(m) {
			a = x
		}
	}
	if a.key == "" {
		return
	}
	rows := []view.Row{row}
	if picks := m.tbl.marked(); a.bulk && len(picks) > 0 {
		rows = picks
	}
	if a.bulk && (len(rows) > 1 || rowKey(rows[0]) != rowKey(row)) && a.key != "Y" && !strings.Contains(it.about, "picked") && !strings.Contains(it.label, "picked") {
		fail("%s: %s acts on %d picked rows, its menu line says %q", where, it.label, len(rows), it.about)
	}
	if !strings.Contains(it.about, "stays up") {
		return
	}
	for _, r := range rows {
		o := r.Obj
		if m.loc.kind == locContainers || o.GetKind() == "Pod" {
			o = m.enr.TopOwner(m.podRowIfContainer(r).Obj)
		}
		if o.GetKind() == "Deployment" && !strings.Contains(restartWhy(o), "stays up") {
			fail("%s: %s says it stays up; %s %s isn't up or is paused: %s", where, it.label, o.GetKind(), o.GetName(), restartWhy(o))
		}
	}
}

// mxOpenLabel: the menu's first item (open the row), as defaultOpen names it.
func mxOpenLabel(m *Model) (string, bool) {
	row, ok := m.tbl.selected()
	if !ok {
		return "", false
	}
	label, _, ok := m.defaultOpen(row)
	return label, ok
}

func mxOpenLabelOf(m *Model) string { l, _ := mxOpenLabel(m); return l }

// mxReason sorts a refusal into the first check that gave it, as
// runAction checks them: read-only, then rows picked, then the row's own.
func mxReason(s string) string {
	switch {
	case s == "read-only" || strings.Contains(s, "read-only"):
		return "read-only"
	case s == "one row" || strings.Contains(s, "picked"):
		return "picks"
	}
	return "own"
}

// mxOthersPicked: rows other than the cursor's are picked.
func mxOthersPicked(m *Model) bool {
	cur, ok := m.tbl.selected()
	picks := m.tbl.marked()
	return ok && (len(picks) > 1 || len(picks) == 1 && rowKey(picks[0]) != rowKey(cur))
}

// mxRefused: the key said it can't (or didn't) do what you asked.
func mxRefused(m *Model) bool { return m.flashErr && m.flash != "" }

func hintKeys(h []hint) map[string]string {
	out := map[string]string{}
	for _, x := range h {
		out[x.key] = x.label
	}
	return out
}

// mxBuild opens a screen in a state, or says it can't be built.
func mxBuild(sc mxScreen, st mxState, w, h int) *Model {
	m := mxModel(st.cluster(), w, h)
	sc.open(m)
	m.refreshRows()
	if strings.HasPrefix(st.name, "pick") && m.loc.kind != locResource && m.loc.kind != locHome && m.loc.kind != locForwards {
		return nil // rows can't be picked here (space says so)
	}
	st.set(m)
	m.flash, m.flashErr = "", false
	return m
}

func TestMatrixEveryKeyEveryScreen(t *testing.T) {
	if testing.Short() {
		t.Skip("walks every key on every screen")
	}
	t.Setenv("HOME", t.TempDir()) // ctrl-s saves here, not in your Downloads
	t.Setenv("EDITOR", "true")
	t.Setenv("KUBE_EDITOR", "true")
	t.Cleanup(func() { view.ReadOnly = false })
	mxClusters()
	var fails []string
	fail := func(f string, a ...any) { fails = append(fails, fmt.Sprintf(f, a...)) }
	for _, size := range [][2]int{{80, 24}, {200, 50}} {
		for _, sc := range mxScreens() {
			for _, st := range mxStates() {
				where := fmt.Sprintf("%s/%s/%dx%d", sc.name, st.name, size[0], size[1])
				base := mxBuild(sc, st, size[0], size[1])
				if base == nil {
					continue
				}
				footer := hintKeys(base.hints())
				// The enter menu, as it is for this row: keyed items only.
				menu := map[string]menuItem{}
				if mm := mxBuild(sc, st, size[0], size[1]); mm.loc.kind != locFind && mm.loc.kind != locContexts && mm.loc.kind != locResources && !isNamespaces(mm) {
					mm.menu()
					if mm.prompt != nil {
						for i, it := range mm.prompt.items {
							if it.key != "" {
								menu[it.key] = it
							}
							if _, isOpen := mxOpenLabel(base); isOpen && it.key == "" && it.label == mxOpenLabelOf(base) && mxOthersPicked(base) && (!it.disabled || it.off != "one row") {
								fail("%s: other rows picked, the menu's %q (item %d) still opens the cursor row", where, it.label, i)
							}
							mxCheckClaims(mm, it, where, fail)
							if it.disabled && (it.off == "" || !strings.HasPrefix(it.about, "Can't")) {
								fail("%s: menu greys %q without saying why (off %q, about %q)", where, it.label, it.off, it.about)
							}
						}
					}
				}
				for _, k := range mxKeys() {
					m := mxBuild(sc, st, size[0], size[1])
					before := mxSnapOf(m)
					_, rowBefore := m.tbl.selected()
					cmd, err := mxPress(m, k)
					if err != nil {
						fail("%s key %q: %v", where, k, err)
						continue
					}
					after := mxSnapOf(m)
					dk := displayKey(k)
					if lbl, offered := footer[dk]; offered && mxRefused(m) && dk != "esc" {
						fail("%s: footer offers %s (%s), pressing it refused: %s", where, dk, lbl, m.flash)
					}
					if _, isAction := base.actionFor(k); isAction && after == before && rowBefore && cmd == nil {
						fail("%s: %s did nothing and said nothing", where, dk)
					}
					if a, isAction := base.actionFor(k); isAction && rowBefore && st.name == "readonly" && (a.write || a.sensitive) {
						if _, inFooter := footer[dk]; inFooter {
							fail("%s: read-only, the footer offers %s (%s)", where, dk, a.label)
						}
						draining := k == "D" && (cmd != nil || m.pager != nil || m.busy != "") // read-only, D shows what draining would move
						if !draining && (!mxRefused(m) || !strings.Contains(m.flash, "read-only")) {
							fail("%s: read-only, %s (%s) didn't refuse as read-only: %q", where, dk, a.label, m.flash)
						}
					}
					if a, isAction := base.actionFor(k); isAction && rowBefore && !a.bulk && base.usesRow(a) && mxOthersPicked(base) {
						if !mxRefused(m) || !strings.Contains(m.flash, "picked") {
							fail("%s: %s (%s) works on one row, rows are picked, and it didn't say so: %q", where, dk, a.label, m.flash)
						}
					}
					if a, isAction := base.actionFor(k); isAction && a.bulk && a.write && st.name == "pick-other" && before.marks > 0 && m.prompt != nil && sc.name != "pf" {
						// The picked row (row 1), not the cursor's (row 0), is what it acts on.
						picked := strings.TrimPrefix(base.tbl.rows[1].Obj.GetName(), markPrefix)
						if !strings.Contains(m.prompt.title, picked) {
							fail("%s: %s with row %s picked asks %q", where, dk, picked, m.prompt.title)
						}
					}
					if k == "p" && rowBefore && (base.loc.res.GVR == kube.Pods || base.loc.kind == locContainers) {
						mxCheckP(m, where, fail)
					}
					if it, ok := menu[k]; ok {
						switch {
						case !it.disabled && mxRefused(m):
							fail("%s: menu offers %s (%s), pressing it refused: %s", where, dk, it.label, m.flash)
						case it.disabled && !mxRefused(m):
							fail("%s: menu greys %s (%s: %s), pressing it worked", where, dk, it.label, it.off)
						case it.disabled && mxReason(it.off) != mxReason(m.flash):
							fail("%s: menu greys %s for %q (%s), the key says %q", where, dk, it.off, it.about, m.flash)
						}
					}
					if _, offered := footer["enter"]; offered && k == "enter" && after == before && cmd == nil {
						fail("%s: footer offers enter, pressing it did nothing", where)
					}
					if k == "esc" {
						want := ""
						switch {
						case before.where != after.where && strings.HasPrefix(after.where, fmt.Sprint(int(locFind))+"/"):
							want = "Back to the results"
						case before.filter != after.filter:
							want = "Clear filter"
						case before.marks != after.marks:
							want = "Clear picks"
						case before.hist == 0 && after.where != before.where && strings.HasPrefix(after.where, fmt.Sprint(int(locHome))+"/"):
							want = "Home"
						case after.where != before.where || after.hist != before.hist:
							want = "Back"
						}
						if got := footer["esc"]; got != want && base.mode == modeTable {
							fail("%s: footer says esc %q, esc did %q", where, got, want)
						}
					}
				}
			}
		}
	}
	for _, f := range fails {
		t.Error(f)
	}
}

// The guide's promises about keys, checked against the code.
func TestMatrixGuideKeyPromises(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "GUIDE.md"))
	if err != nil {
		t.Fatal(err)
	}
	guide := string(raw)

	// The g table: every jump it lists goes where it says, and every jump
	// kubed has is in it (and in the footer's g list and the help).
	listed := map[string]bool{}
	for _, line := range strings.Split(guide, "\n") {
		cells := strings.Split(line, "|")
		for i := 1; i+1 < len(cells); i += 2 {
			if k := strings.TrimSpace(cells[i]); len(k) == 2 && k[0] == 'g' && strings.TrimSpace(cells[i+1]) != "" {
				listed[k[1:]] = true
			}
		}
	}
	if len(listed) < 10 {
		t.Fatalf("read only %d g keys from the guide's table", len(listed))
	}
	want := map[string]bool{"h": true, "H": true}
	for k := range jumps {
		want[k] = true
	}
	for k := range want {
		if !listed[k] {
			t.Errorf("g%s jumps somewhere, the guide's g table doesn't list it", k)
		}
		if !strings.Contains(helpText, "g"+k+" ") {
			t.Errorf("g%s isn't in the help", k)
		}
		inFooter := false
		for _, p := range gPlaces {
			inFooter = inFooter || p[0] == k
		}
		if !inFooter {
			t.Errorf("g%s isn't in the footer's g list", k)
		}
	}
	for k := range listed {
		if !want[k] && k != "g" {
			t.Errorf("the guide lists g%s, kubed has no such jump", k)
		}
	}

	// The picks paragraph: exactly these keys act on every picked row.
	bulk := map[string]bool{}
	for _, a := range actions {
		if a.bulk {
			bulk[displayKey(a.key)] = true
		}
	}
	for _, k := range []string{"r", "c", "ctrl-k", "ctrl-d", "Y"} {
		if !bulk[k] {
			t.Errorf("the guide says %s acts on every picked row; it doesn't", k)
		}
		delete(bulk, k)
	}
	for k := range bulk {
		t.Errorf("%s acts on every picked row; the guide's picks paragraph doesn't say so", k)
	}

	// The read-only list: every change, shells (s, S), secret values and
	// local ports (F) are off; nothing else that only looks.
	for _, a := range actions {
		off := a.write || a.sensitive
		switch a.key {
		case "s", "S", "F":
			if !off {
				t.Errorf("%s (%s) works with --readonly; the guide says it's off", a.key, a.label)
			}
		case "x":
			if off != (a.label == "Show values") { // a config map's contents are not secret
				t.Errorf("x (%s): off with --readonly = %v", a.label, off)
			}
		case "w", "l", "p", "E", "d", "y", "X", "o", "v":
			if off {
				t.Errorf("%s (%s) is off with --readonly; looking works, says the guide", a.key, a.label)
			}
		}
	}

	// The "Every key" table names only keys kubed has.
	_, section, found := strings.Cut(guide, "## Every key")
	if !found {
		t.Fatal("the guide has no Every key section")
	}
	for _, k := range []string{"ctrl-a", "ctrl-s", "ctrl-k", "ctrl-d", "ctrl-n, ctrl-p", "space, *", "0, 1–9"} {
		if !strings.Contains(section, "| "+k+" |") {
			t.Errorf("the keys table has no row for %s", k)
		}
	}
}

// Text screens: every key the footer offers works, esc leaves, and no key
// panics, on help and on a YAML-like screen, read-write and read-only.
func TestMatrixTextScreenKeys(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mxClusters()
	keys := []string{"/", "n", "N", "e", "t", "r", "l", "h", "v", "u", "j", "k", "p", "ctrl+s", "c", "C", "w", "f", "G", "gg", "?", "up", "down", "pgup", "pgdown", "home", "end", "esc", "q"}
	opens := map[string]func(m *Model){
		"help": func(m *Model) { m.openHelp() },
		"yaml": func(m *Model) {
			row, _ := m.tbl.selected()
			p := newPager("deployment web", "apiVersion: apps/v1\nkind: Deployment\n", m.pagerWidth(), m.pagerHeight())
			p.editRow = &row
			m.openPager(p)
		},
	}
	for name, open := range opens {
		for _, ro := range []bool{false, true} {
			build := func() *Model {
				m := mxModel(mxFull, 80, 24)
				m.opts.ReadOnly = ro
				m.jumpTo(location{kind: locHome})
				m.openResource("deployments", true)
				open(m)
				m.flash, m.flashErr = "", false
				return m
			}
			footer := hintKeys(build().hints())
			for _, k := range keys {
				m := build()
				where := fmt.Sprintf("%s/readonly=%v", name, ro)
				if _, err := mxPress(m, k); err != nil {
					t.Errorf("%s key %q: %v", where, k, err)
					continue
				}
				if lbl, ok := footer[displayKey(k)]; ok && mxRefused(m) {
					t.Errorf("%s: footer offers %s (%s), pressing it refused: %s", where, k, lbl, m.flash)
				}
				if k == "esc" && m.pager != nil {
					t.Errorf("%s: esc left the text screen open", where)
				}
				if ro && k == "e" && name == "yaml" && !strings.Contains(m.flash, "read-only") {
					t.Errorf("%s: e on a read-only YAML screen said %q", where, m.flash)
				}
			}
		}
	}
}

// Home in a namespace for this screen only (:apps shop, chosen: all): esc
// goes back to the namespace you chose, and the footer says so.
func TestMatrixEscHintHomeForThisScreen(t *testing.T) {
	mxClusters()
	m := mxModel(mxFull, 120, 40)
	m.runCommand("apps shop")
	if m.namespace != "shop" || len(m.history) != 0 {
		t.Fatalf("on %q with trail %v", m.namespace, m.history)
	}
	if got := hintKeys(m.hints())["esc"]; got != "Back to all namespaces" {
		t.Errorf("footer says esc %q", got)
	}
	m.onKey(tea.KeyMsg{Type: tea.KeyEsc})
	if m.namespace != "" || m.loc.kind != locHome {
		t.Errorf("esc went to %q", m.namespace)
	}
}

// p on every pod and on every container of it, menu and key alike: it
// opens only a run that ended, and greys (and refuses) when there is none.
func TestMatrixPOnEveryPodAndContainer(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	mxClusters()
	var fails []string
	fail := func(f string, a ...any) { fails = append(fails, fmt.Sprintf(f, a...)) }
	pods := mxFull.client.Cache.List(kube.Pods, "")
	for _, pod := range pods {
		containers := view.Containers(pod)
		anyRun := false
		for _, c := range containers {
			anyRun = anyRun || mxEarlierRun(pod, c)
		}
		// From the pods list, then from each of its containers.
		places := []string{""}
		places = append(places, containers...)
		for _, c := range places {
			open := func() *Model {
				m := mxModel(mxFull, 120, 40)
				m.jumpTo(location{kind: locHome})
				m.openResource("pods", true)
				if c != "" {
					m.navigate(location{kind: locContainers, podNS: pod.GetNamespace(), podName: pod.GetName()}, true)
				}
				for i, r := range m.tbl.rows {
					if name := strings.TrimPrefix(r.Obj.GetName(), markPrefix); (c == "" && name == pod.GetName()) || (c != "" && name == c) {
						m.tbl.jump(i)
						m.tbl.touched = true
					}
				}
				return m
			}
			where := pod.GetName()
			truth := anyRun
			if c != "" {
				where += "/" + c
				truth = mxEarlierRun(pod, c)
			}
			m := open()
			m.menu()
			offered := false
			for _, it := range m.prompt.items {
				if it.key == "p" {
					offered = !it.disabled
				}
			}
			if offered != truth {
				fail("%s: menu offers p = %v, it has a run that ended = %v", where, offered, truth)
			}
			m = open()
			if _, err := mxPress(m, "p"); err != nil {
				fail("%s: %v", where, err)
				continue
			}
			if truth == mxRefused(m) {
				fail("%s: p refused = %v (%q), it has a run that ended = %v", where, mxRefused(m), m.flash, truth)
			}
			// A refusal says what's true: nothing restarted (review: on the
			// containers screen it said "app has restarted").
			if !truth && (!strings.Contains(m.flash, "restarted or stopped") || strings.Contains(m.flash, " has restarted") && !strings.Contains(m.flash, "nothing in")) {
				fail("%s: p's refusal %q doesn't say nothing restarted", where, m.flash)
			}
			mxCheckP(m, where, fail)
		}
	}
	for _, f := range fails {
		t.Error(f)
	}
}

// Rows shown, their versions still loading (ReplicaSets, ControllerRevisions
// hang): u and v wait for them, never saying there are none, and the menu
// doesn't grey them.
func TestMatrixVersionsStillLoading(t *testing.T) {
	c := mxNewClusterHanging(mxFixtures(), false, []schema.GroupVersionResource{kube.ReplicaSets, kube.ControllerRevisions})
	defer c.client.Cache.Stop()
	for _, kind := range []string{"deployments", "statefulsets", "daemonsets"} {
		for _, k := range []string{"u", "v"} {
			m := mxModel(c, 120, 40)
			m.jumpTo(location{kind: locHome})
			m.openResource(kind, false)
			if _, ok := m.tbl.selected(); !ok {
				t.Fatalf("%s: no rows", kind)
			}
			m.menu()
			for _, it := range m.prompt.items {
				if it.key == k && it.disabled {
					t.Errorf("%s: menu greys %s while its versions load: %s", kind, k, it.about)
				}
			}
			m.prompt, m.mode = nil, modeTable
			if _, err := mxPress(m, k); err != nil {
				t.Fatalf("%s %s: %v", kind, k, err)
			}
			if mxRefused(m) || m.loadWaiting == 0 || !strings.Contains(m.busy, "versions") {
				t.Errorf("%s: %s while its versions load said %q (busy %q), want it to wait", kind, k, m.flash, m.busy)
			}
		}
	}
}

// p in logs of a setup step that finished as it should (istio-init), and
// of a container that never restarted: no earlier logs, said plainly,
// never "waiting to restart".
func TestMatrixPInLogsOfFinishedSetupStep(t *testing.T) {
	mxClusters()
	for _, tc := range []struct{ container, want string }{
		{"istio-init", "only run"},
		{"app", "hasn't restarted"},
	} {
		m := mxModel(mxFull, 120, 40)
		m.jumpTo(location{kind: locHome})
		m.openResource("pods", true)
		m.openLogs(logSource{ns: "shop", pod: "web-mesh", container: tc.container})
		m.mode = modePager
		m.flash = ""
		if _, err := mxPress(m, "p"); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(m.flash, tc.want) || strings.Contains(m.flash, "waiting to restart") || m.pager.source.previous {
			t.Errorf("%s: p said %q, previous=%v", tc.container, m.flash, m.pager.source.previous)
		}
		m.pager.close()
	}
}

// What r, u and i say a rollout does to uptime must be true: a deployment
// with no copy ready isn't up, so "it stays up" is false there.
func TestMatrixRolloutUptimeWords(t *testing.T) {
	mxClusters()
	for _, name := range []string{"api", "paused", "web"} {
		o, ok := mxFull.client.Cache.Get(kube.Deployments, "shop", name)
		if !ok {
			t.Fatalf("no deployment %s", name)
		}
		ready, _, _ := unstructured.NestedInt64(o.Object, "status", "readyReplicas")
		why := restartWhy(o)
		if up := strings.Contains(why, "stays up"); up != (ready > 0) {
			t.Errorf("%s (%d ready): %q", name, ready, why)
		}
	}
}

// A paused deployment takes a go-back or a new image, and rolls nothing
// out until it is resumed: the confirm warns, and the result says so.
func TestMatrixPausedDeploymentChanges(t *testing.T) {
	mxClusters()
	open := func() *Model {
		m := mxModel(mxFull, 160, 50)
		m.jumpTo(location{kind: locHome})
		m.openResource("deployments", true)
		for i, r := range m.tbl.rows {
			if r.Obj.GetName() == "held" {
				m.tbl.jump(i)
				m.tbl.touched = true
			}
		}
		if r, _ := m.tbl.selected(); r.Obj.GetName() != "held" {
			t.Fatalf("no deployment held")
		}
		return m
	}
	said := func(p *prompt) string { return p.title + "\n" + strings.Join(p.warnings, "\n") }
	m := open()
	if _, err := mxPress(m, "u"); err != nil || m.prompt == nil {
		t.Fatalf("u: %v, flash %q", err, m.flash)
	}
	if !strings.Contains(said(m.prompt), "paused") {
		t.Errorf("u on a paused deployment asks %q", said(m.prompt))
	}
	m = open()
	if _, err := mxPress(m, "i"); err != nil || m.prompt == nil {
		t.Fatalf("i: %v, flash %q", err, m.flash)
	}
	m.prompt.onSubmit("shop/web:9")
	if m.prompt == nil || !strings.Contains(said(m.prompt), "paused") || strings.Contains(said(m.prompt), "stays up") {
		t.Errorf("i on a paused deployment asks %q", said(m.prompt))
	}
	ref := kube.Ref{Resource: kube.Resource{GVR: kube.Deployments, Kind: "Deployment"}, Namespace: "shop", Name: "held"}
	for _, words := range []string{goingBackWords(ref, 1, true, false, ""), imageSetWords(ref, "shop/web:9", "app", true, false, "")} {
		if !strings.Contains(words, "once it is resumed") || strings.Contains(words, "is going back") || strings.Contains(words, "now runs") || strings.Contains(words, "o to watch") {
			t.Errorf("result on a paused deployment: %q", words)
		}
	}
	// Restart's menu line, from its facts: paused, or no copy ready.
	for _, name := range []string{"held", "api", "paused"} {
		m := mxModel(mxFull, 160, 50)
		m.jumpTo(location{kind: locHome})
		m.openResource("deployments", true)
		for i, r := range m.tbl.rows {
			if r.Obj.GetName() == name {
				m.tbl.jump(i)
			}
		}
		m.menu()
		for _, it := range m.prompt.items {
			if it.key == "r" && strings.Contains(it.about, "stays up") && !strings.HasPrefix(it.about, "Can't") {
				o, _ := m.tbl.selected()
				if !strings.Contains(restartWhy(o.Obj), "stays up") || pausedDeploy(o.Obj) && !strings.Contains(it.about, "paused") {
					t.Errorf("%s: restart's menu line %q", name, it.about)
				}
			}
		}
	}
}

// Review of round 127: the home restart line counts what r restarts: a
// deployment scaled to 0 has no pods to replace, and r leaves it out, so
// the line does too.
func TestAppRestartLineCountsWhatRuns(t *testing.T) {
	web := map[string]any{"app": "web"}
	deploy := func(name string, replicas int64) *unstructured.Unstructured {
		return mxObj("apps/v1", "Deployment", "shop", name, map[string]any{
			"spec":   map[string]any{"replicas": replicas, "selector": map[string]any{"matchLabels": web}, "template": mxTemplate(web)},
			"status": map[string]any{"replicas": replicas, "readyReplicas": replicas}})
	}
	rs := mxObj("apps/v1", "ReplicaSet", "shop", "web-abc", map[string]any{"metadata": map[string]any{"labels": web,
		"ownerReferences": mxOwner("apps/v1", "Deployment", "web")["ownerReferences"]},
		"spec": map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": web}, "template": mxTemplate(web)}})
	base := []runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), deploy("web", 2), deploy("web-canary", 0), rs,
		mxPod("web-abc-1", "apps/v1", "ReplicaSet", "web-abc", "Running", 0, web), mxPod("web-abc-2", "apps/v1", "ReplicaSet", "web-abc", "Running", 0, web)}
	db := []runtime.Object{mxObj("apps/v1", "StatefulSet", "shop", "web-db", map[string]any{"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": web}, "template": mxTemplate(web)},
		"status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}}),
		mxPod("web-db-0", "apps/v1", "StatefulSet", "web-db", "Running", 0, web)}
	for _, c := range []struct {
		name string
		objs []runtime.Object
		has  string
	}{
		{"one runs, one at 0", base, "Rollout restart of its deployment web. "},
		{"two run, one at 0", append(slices.Clone(base), db...), "Rollout restart of its 1 deployment and 1 statefulset"},
	} {
		cl := mxNewCluster(c.objs, false)
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		found := false
		for i, r := range m.tbl.rows {
			if a, ok := m.appFor(r); ok && a.Name == "web" {
				m.tbl.jump(i)
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no app web", c.name)
		}
		m.menu()
		about := ""
		for _, it := range m.prompt.items {
			if it.key == "r" {
				about = it.about
			}
		}
		if !strings.Contains(about, c.has) {
			t.Errorf("%s: restart line %q", c.name, about)
		}
		cl.client.Cache.Stop()
	}
}

// Review of round 127: Force stop's line says a finished job's pod is
// gone for good, as its confirm does: no new one comes.
func TestForceStopLineFinishedJobPod(t *testing.T) {
	mxClusters()
	m := mxModel(mxFull, 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("pods", true)
	found := false
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == "nightly-1-p" {
			m.tbl.jump(i)
			found = true
		}
	}
	if !found {
		t.Fatal("no pod nightly-1-p")
	}
	m.menu()
	about := ""
	for _, it := range m.prompt.items {
		if it.key == "ctrl+k" {
			about = it.about
		}
	}
	if !strings.Contains(about, "gone for good") || strings.Contains(about, "starts a new one") {
		t.Errorf("Force stop line %q", about)
	}
	// QA round 128: a pod that has ended has no work left to cut short.
	if strings.Contains(about, "without time to finish") {
		t.Errorf("Force stop line on an ended pod: %q", about)
	}
	row, _ := m.tbl.selected()
	m.mode = modeTable
	m.killRows([]view.Row{row})
	if m.prompt == nil {
		t.Fatal("no Force stop confirm")
	}
	if title := m.prompt.title; strings.Contains(title, "without time to finish") || !strings.Contains(title, "gone for good") {
		t.Errorf("Force stop confirm on an ended pod: %q", title)
	}
}

// Review of round 127: what r says a restart does follows the update
// strategy: Recreate stops every pod first, OnDelete replaces nothing
// until pods are deleted, a partition holds pods back, maxSurge 0 stops
// an old pod before its new one starts, and a daemon set with a surge
// starts the new pod first.
func TestRestartWhyFollowsStrategy(t *testing.T) {
	obj := func(kind string, replicas int64, ready int64, strategy map[string]any) *unstructured.Unstructured {
		spec := map[string]any{"replicas": replicas}
		key := "updateStrategy"
		if kind == "Deployment" {
			key = "strategy"
		}
		if kind == "DaemonSet" {
			delete(spec, "replicas")
		}
		if strategy != nil {
			spec[key] = strategy
		}
		return &unstructured.Unstructured{Object: map[string]any{"kind": kind, "spec": spec, "status": map[string]any{"readyReplicas": ready}}}
	}
	rolling := func(surge, unavail any) map[string]any {
		ru := map[string]any{}
		if surge != nil {
			ru["maxSurge"] = surge
		}
		if unavail != nil {
			ru["maxUnavailable"] = unavail
		}
		return map[string]any{"type": "RollingUpdate", "rollingUpdate": ru}
	}
	for _, c := range []struct {
		name  string
		o     *unstructured.Unstructured
		has   []string
		never []string
	}{
		{"deployment, defaults, 2 copies", obj("Deployment", 2, 2, nil), []string{"new pod starts before each old one stops", "stays up"}, nil},
		{"deployment, Recreate", obj("Deployment", 3, 3, map[string]any{"type": "Recreate"}), []string{"Recreate", "down for a moment"}, []string{"stays up", "one at a time"}},
		{"deployment, maxSurge 0, 1 copy", obj("Deployment", 1, 1, rolling(int64(0), int64(1))), []string{"down for a moment"}, []string{"stays up", "starts before"}},
		{"deployment, maxSurge 0, 3 copies", obj("Deployment", 3, 3, rolling(int64(0), int64(1))), []string{"stays up"}, []string{"starts before"}},
		{"deployment, defaults, 8 copies (2 may stop at once)", obj("Deployment", 8, 8, nil), []string{"stays up"}, []string{"starts before each old one stops"}},
		{"deployment, maxUnavailable 100%", obj("Deployment", 2, 2, rolling("25%", "100%")), []string{"down for a moment"}, []string{"stays up"}},
		{"stateful set, OnDelete", obj("StatefulSet", 3, 3, map[string]any{"type": "OnDelete"}), []string{"OnDelete", "deleted"}, []string{"one at a time", "down for a moment"}},
		{"stateful set, 1 copy, OnDelete", obj("StatefulSet", 1, 1, map[string]any{"type": "OnDelete"}), []string{"OnDelete"}, []string{"down for a moment"}},
		{"stateful set, partition 2 of 3", obj("StatefulSet", 3, 3, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"partition": int64(2)}}),
			[]string{"numbered 2 and up"}, nil},
		{"stateful set, partition holds all", obj("StatefulSet", 3, 3, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"partition": int64(3)}}),
			[]string{"partition"}, []string{"one at a time", "down for a moment"}},
		{"daemon set, OnDelete", obj("DaemonSet", 0, 0, map[string]any{"type": "OnDelete"}), []string{"OnDelete", "deleted"}, []string{"down for a moment"}},
		{"daemon set, maxSurge 1", obj("DaemonSet", 0, 0, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": int64(1), "maxUnavailable": int64(0)}}),
			[]string{"new pod starts before the old one stops"}, []string{"down for a moment"}},
		{"daemon set, defaults", obj("DaemonSet", 0, 0, nil), []string{"down for a moment"}, nil},
	} {
		why := restartWhy(c.o)
		for _, h := range c.has {
			if !strings.Contains(why, h) {
				t.Errorf("%s: %q lacks %q", c.name, why, h)
			}
		}
		for _, n := range c.never {
			if strings.Contains(why, n) {
				t.Errorf("%s: %q says %q", c.name, why, n)
			}
		}
	}
}

// Review of round 127: i says how the new image rolls out as r does: a
// stateful set that replaces pods only when deleted, and a deployment
// that stops all its pods first.
func TestSetImageSaysStrategy(t *testing.T) {
	db := map[string]any{"app": "db"}
	web := map[string]any{"app": "web"}
	cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil),
		mxObj("apps/v1", "StatefulSet", "shop", "db", map[string]any{"spec": map[string]any{"replicas": int64(3), "selector": map[string]any{"matchLabels": db},
			"template": mxTemplate(db), "updateStrategy": map[string]any{"type": "OnDelete"}}, "status": map[string]any{"replicas": int64(3), "readyReplicas": int64(3)}}),
		mxObj("apps/v1", "Deployment", "shop", "web", map[string]any{"spec": map[string]any{"replicas": int64(2), "selector": map[string]any{"matchLabels": web},
			"template": mxTemplate(web), "strategy": map[string]any{"type": "Recreate"}}, "status": map[string]any{"replicas": int64(2), "readyReplicas": int64(2)}}),
	}, false)
	defer cl.client.Cache.Stop()
	for _, c := range []struct{ list, name, has string }{
		{"statefulsets", "db", "OnDelete"},
		{"deployments", "web", "Recreate"},
	} {
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		m.openResource(c.list, true)
		for i, r := range m.tbl.rows {
			if r.Obj.GetName() == c.name {
				m.tbl.jump(i)
			}
		}
		if _, err := mxPress(m, "i"); err != nil || m.prompt == nil {
			t.Fatalf("%s i: %v, flash %q", c.name, err, m.flash)
		}
		m.prompt.onSubmit("shop/web:9")
		if m.prompt == nil || !strings.Contains(m.prompt.title, c.has) {
			t.Errorf("%s: i asks %+v", c.name, m.prompt)
		}
	}
}

// Second review of round 127: a stateful set's maxUnavailable defaults to
// 1 (nothing to hint); set above 1 it may stop more at once, and at all
// its copies it may stop them all. Any kind scaled to 0 runs nothing to
// stop. maxUnavailable at all copies with a surge can be down, not is.
func TestRestartWhyEdges(t *testing.T) {
	sts := func(replicas int64, ru map[string]any) *unstructured.Unstructured {
		spec := map[string]any{"replicas": replicas}
		if ru != nil {
			spec["updateStrategy"] = map[string]any{"type": "RollingUpdate", "rollingUpdate": ru}
		}
		return &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "spec": spec}}
	}
	for _, c := range []struct {
		name  string
		o     *unstructured.Unstructured
		has   []string
		never []string
	}{
		{"stateful set, maxUnavailable 1 (the default)", sts(3, map[string]any{"maxUnavailable": int64(1)}), []string{"one at a time"}, []string{"maxUnavailable", "more at once"}},
		{"stateful set, maxUnavailable 2", sts(4, map[string]any{"maxUnavailable": int64(2)}), []string{"up to 2"}, nil},
		{"stateful set, maxUnavailable 100%", sts(3, map[string]any{"maxUnavailable": "100%"}), []string{"all", "down for a moment"}, []string{"the others keep running"}},
		{"stateful set at 0", sts(0, nil), []string{"no copies"}, []string{"1 copy", "down for a moment"}},
		{"deployment at 0", &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "spec": map[string]any{"replicas": int64(0)}}}, []string{"no copies"}, nil},
		{"daemon set on no node", &unstructured.Unstructured{Object: map[string]any{"kind": "DaemonSet", "spec": map[string]any{}, "status": map[string]any{"desiredNumberScheduled": int64(0)}}},
			[]string{"no node"}, []string{"down for a moment"}},
		{"deployment, all may stop, with a surge", &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment",
			"spec":   map[string]any{"replicas": int64(2), "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": int64(1), "maxUnavailable": "100%"}}},
			"status": map[string]any{"readyReplicas": int64(2)}}}, []string{"can be down for a moment"}, []string{"is down"}},
	} {
		why := restartWhy(c.o)
		for _, h := range c.has {
			if !strings.Contains(why, h) {
				t.Errorf("%s: %q lacks %q", c.name, why, h)
			}
		}
		for _, n := range c.never {
			if strings.Contains(why, n) {
				t.Errorf("%s: %q says %q", c.name, why, n)
			}
		}
	}
}

// Second review of round 127: picked rows that hold pods back (a
// partition) or go down (all may stop) aren't "one pod at a time": the
// menu line says not all are, and the confirm gives each its own line. A
// paused one with its own line is listed once.
func TestRestartPickedRowsOwnLines(t *testing.T) {
	part := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "db"},
		"spec": map[string]any{"replicas": int64(3), "updateStrategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"partition": int64(3)}}}}}
	allStop := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "api"},
		"spec":   map[string]any{"replicas": int64(3), "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": int64(0), "maxUnavailable": "100%"}}},
		"status": map[string]any{"readyReplicas": int64(3)}}}
	plain := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"replicas": int64(2)}, "status": map[string]any{"readyReplicas": int64(2)}}}
	pausedRecreate := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "held"},
		"spec": map[string]any{"replicas": int64(2), "paused": true, "strategy": map[string]any{"type": "Recreate"}}, "status": map[string]any{"readyReplicas": int64(2)}}}
	for _, c := range []struct {
		name string
		rows []view.Row
		says []string
	}{
		{"partition", []view.Row{{Obj: part}, {Obj: plain}}, []string{"For db:"}},
		{"all may stop", []view.Row{{Obj: allStop}, {Obj: plain}}, []string{"For api:"}},
	} {
		if w := replaceWords(c.rows); strings.HasSuffix(w, "one pod at a time") {
			t.Errorf("%s: menu line %q", c.name, w)
		}
		why := restartRowsWhy(c.rows)
		for _, s := range c.says {
			if !strings.Contains(why, s) {
				t.Errorf("%s: confirm %q lacks %q", c.name, why, s)
			}
		}
	}
	if why := restartRowsWhy([]view.Row{{Obj: pausedRecreate}, {Obj: plain}}); strings.Count(why, "held") != 1 {
		t.Errorf("paused Recreate listed %d times: %q", strings.Count(why, "held"), why)
	}
}

// Third review of round 127: a default DaemonSet and a one-copy
// StatefulSet do replace one pod at a time; they get their own line only
// because they go down for a moment. Neither the menu nor the confirm
// may say they aren't one at a time.
func TestRestartPickedRowsDownNotDifferent(t *testing.T) {
	ds := &unstructured.Unstructured{Object: map[string]any{"kind": "DaemonSet", "metadata": map[string]any{"name": "agent"},
		"spec": map[string]any{}, "status": map[string]any{"desiredNumberScheduled": int64(3)}}}
	one := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "db"},
		"spec": map[string]any{"replicas": int64(1)}}}
	plain := &unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"replicas": int64(2)}, "status": map[string]any{"readyReplicas": int64(2)}}}
	for _, c := range []struct {
		name string
		obj  *unstructured.Unstructured
	}{{"agent", ds}, {"db", one}} {
		rows := []view.Row{{Obj: plain}, {Obj: c.obj}}
		if w := replaceWords(rows); strings.Contains(w, "not all one pod at a time") {
			t.Errorf("%s: menu line %q", c.name, w)
		}
		why := restartRowsWhy(rows)
		if strings.Contains(why, "Not "+c.name) {
			t.Errorf("%s: confirm %q", c.name, why)
		}
		if !strings.Contains(why, "down for a moment") {
			t.Errorf("%s: confirm %q lacks the downtime", c.name, why)
		}
	}
}

// QA round 128: picked rows that all replace nothing (OnDelete, a
// partition holding back every pod) get no "one pod at a time" lead-in,
// and each row's line is whole and on a line of its own. With a plain row
// beside them, the lead-in names the rows it is about.
func TestRestartPickedRowsNoneReplaced(t *testing.T) {
	ondel := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "ondel"},
		"spec": map[string]any{"replicas": int64(2), "updateStrategy": map[string]any{"type": "OnDelete"}}}}
	parted := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "parted"},
		"spec": map[string]any{"replicas": int64(2), "updateStrategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"partition": int64(2)}}}}}
	plain := &unstructured.Unstructured{Object: map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"replicas": int64(3)}}}
	why := restartRowsWhy([]view.Row{{Obj: ondel}, {Obj: parted}})
	if strings.Contains(why, "one pod at a time") || strings.Contains(why, "one at a time") {
		t.Errorf("none replaced, but: %q", why)
	}
	for _, r := range []*unstructured.Unstructured{ondel, parted} {
		want := "For " + r.GetName() + ": " + restartWhy(r)
		if !slices.Contains(strings.Split(why, "\n"), want) {
			t.Errorf("no line %q in %q", want, why)
		}
	}
	if w := replaceWords([]view.Row{{Obj: ondel}, {Obj: parted}}); strings.Contains(w, "new pods replace") {
		t.Errorf("menu line for none replaced: %q", w)
	}
	mixed := restartRowsWhy([]view.Row{{Obj: plain}, {Obj: ondel}})
	if !strings.HasPrefix(mixed, "For web: ") || strings.HasPrefix(mixed, "New pods replace the old ones") {
		t.Errorf("mixed lead-in: %q", mixed)
	}
	// The confirm box wraps a long line instead of cutting it with "…".
	p := confirmPrompt("Restart these 2 statefulsets?\n  ondel\n  parted\n"+why, nil, "", nil)
	out := p.render(70)
	if strings.Contains(out, "…") {
		t.Errorf("confirm cut short:\n%s", out)
	}
	for _, w := range []string{"pods.", "lowered."} {
		if !strings.Contains(out, w) {
			t.Errorf("confirm lacks %q:\n%s", w, out)
		}
	}
}

// QA round 128: r on an app whose only workload replaces no pod (OnDelete,
// or a partition holding back every pod) doesn't head the confirm with
// "gets new pods, replacing the old ones"; each line says what happens.
func TestAppRestartHeadlineNoneReplaced(t *testing.T) {
	lab := map[string]any{"app": "db"}
	for _, c := range []struct {
		name     string
		strategy map[string]any
		says     string
	}{
		{"OnDelete", map[string]any{"type": "OnDelete"}, "nothing changes until you delete its pods"},
		{"partition", map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"partition": int64(1)}}, "none is replaced"},
	} {
		sts := mxObj("apps/v1", "StatefulSet", "shop", "db", map[string]any{"spec": map[string]any{"replicas": int64(1), "updateStrategy": c.strategy,
			"selector": map[string]any{"matchLabels": lab}, "template": mxTemplate(lab)}, "status": map[string]any{"replicas": int64(1), "readyReplicas": int64(1)}})
		cl := mxNewCluster([]runtime.Object{mxObj("v1", "Namespace", "", "shop", nil), sts, mxPod("db-0", "apps/v1", "StatefulSet", "db", "Running", 0, lab)}, false)
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		var row view.Row
		for _, r := range m.tbl.rows {
			if a, ok := m.appFor(r); ok && a.Name == "db" {
				row = r
			}
		}
		m.appRestart(row)
		title := ""
		if m.prompt != nil {
			title = m.prompt.title
		}
		if strings.Contains(title, "new pods, replacing") || !strings.Contains(title, c.says) {
			t.Errorf("%s: confirm %q", c.name, title)
		}
		cl.client.Cache.Stop()
	}
}

// QA round 129: badimg's image couldn't be pulled; i set busybox:1.37,
// whose pod keeps exiting 0, while the old pod still can't pull. u stays
// offered, but its menu line says the previous version fails too, and its
// confirm says that version's pods are failing now (u's own target).
func TestUndoSaysTheTargetFailsToo(t *testing.T) {
	lbl := map[string]any{"app": "badimg"}
	tmpl := func(image string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": lbl}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": image}}}}
	}
	rs := func(name, rev, image string) *unstructured.Unstructured {
		return mxObj("apps/v1", "ReplicaSet", "shop", name, map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": rev},
			"ownerReferences": mxOwner("apps/v1", "Deployment", "badimg")["ownerReferences"]},
			"spec": map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl(image)}, "status": map[string]any{"replicas": int64(1)}})
	}
	pod := func(name, rsName, image string, status map[string]any) *unstructured.Unstructured {
		return mxObj("v1", "Pod", "shop", name, map[string]any{"metadata": map[string]any{"labels": lbl, "ownerReferences": mxOwner("apps/v1", "ReplicaSet", rsName)["ownerReferences"]},
			"spec": map[string]any{"nodeName": "node-1", "containers": []any{map[string]any{"name": "app", "image": image}}}, "status": status})
	}
	ended := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339)
	objs := []runtime.Object{mxObj("v1", "Namespace", "", "shop", nil),
		mxObj("apps/v1", "Deployment", "shop", "badimg", map[string]any{"metadata": map[string]any{"labels": lbl, "annotations": map[string]any{"deployment.kubernetes.io/revision": "2"}},
			"spec":   map[string]any{"replicas": int64(1), "selector": map[string]any{"matchLabels": lbl}, "template": tmpl("busybox:1.37")},
			"status": map[string]any{"replicas": int64(2), "updatedReplicas": int64(1)}}),
		rs("badimg-new", "2", "busybox:1.37"), rs("badimg-old", "1", "busybox:no-such-tag"),
		pod("badimg-old-a", "badimg-old", "busybox:no-such-tag", map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "app", "image": "busybox:no-such-tag",
			"state": map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff", "message": `Back-off pulling image "busybox:no-such-tag"`}}}}}),
		pod("badimg-new-a", "badimg-new", "busybox:1.37", map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "app", "image": "busybox:1.37", "restartCount": int64(2),
			"state":     map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off 20s restarting failed container"}},
			"lastState": map[string]any{"terminated": map[string]any{"exitCode": int64(0), "reason": "Completed", "finishedAt": ended}}}}}),
	}
	cl := mxNewCluster(objs, false)
	defer cl.client.Cache.Stop()
	for _, where := range []string{"home", "deployments"} {
		m := mxModel(cl, 160, 50)
		m.jumpTo(location{kind: locHome})
		if where == "deployments" {
			m.openResource("deployments", true)
		}
		found := false
		for i, r := range m.tbl.rows {
			if a, ok := m.appFor(r); ok && a.Name == "badimg" || where == "deployments" && r.Obj.GetName() == "badimg" {
				m.tbl.jump(i)
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no badimg", where)
		}
		m.menu()
		about := "no u"
		for _, it := range m.prompt.items {
			if it.key == "u" {
				about = it.about
				if it.disabled {
					t.Errorf("%s: u greyed: %q", where, it.about)
				}
			}
		}
		if !strings.Contains(about, "the previous version fails too") {
			t.Errorf("%s: u's menu line doesn't say the previous version fails too: %q", where, about)
		}
		m.prompt, m.mode = nil, modeTable
		if _, err := mxPress(m, "u"); err != nil || m.prompt == nil {
			t.Fatalf("%s u: %v, flash %q", where, err, m.flash)
		}
		if title := m.prompt.title; !strings.Contains(title, "busybox:no-such-tag") || !strings.Contains(title, "That version's pods are failing now: can't pull image") {
			t.Errorf("%s: u asks %q", where, title)
		}
	}
}

// QA round 131: ctrl-k on a running pod of a job with no tries left said
// "job jb0 starts a new one", while ctrl-d said none starts again. Force
// stop and its menu line now say what the job does, as Delete does.
func TestForceStopJobPodNoTriesLeft(t *testing.T) {
	lbl := map[string]any{"app": "jb0"}
	objs := append(mxFixtures(),
		mxObj("batch/v1", "Job", "shop", "jb0", map[string]any{"spec": map[string]any{"backoffLimit": int64(0),
			"selector": map[string]any{"matchLabels": lbl}, "template": mxTemplate(lbl)}, "status": map[string]any{"active": int64(1)}}),
		mxPod("jb0-p", "batch/v1", "Job", "jb0", "Running", 0, lbl))
	m := mxModel(mxNewCluster(objs, false), 160, 50)
	m.jumpTo(location{kind: locHome})
	m.openResource("pods", true)
	found := false
	for i, r := range m.tbl.rows {
		if r.Obj.GetName() == "jb0-p" {
			m.tbl.jump(i)
			found = true
		}
	}
	if !found {
		t.Fatal("no pod jb0-p")
	}
	m.menu()
	about := ""
	for _, it := range m.prompt.items {
		if it.key == "ctrl+k" {
			about = it.about
		}
	}
	if strings.Contains(about, "starts a new one") || !strings.Contains(about, "no tries left") {
		t.Errorf("Force stop line %q", about)
	}
	row, _ := m.tbl.selected()
	m.mode = modeTable
	m.killRows([]view.Row{row})
	if m.prompt == nil {
		t.Fatal("no Force stop confirm")
	}
	if title := m.prompt.title; strings.Contains(title, "starts a new one") || !strings.Contains(title, "no tries left") {
		t.Errorf("Force stop confirm %q", title)
	}
}
