package kube

import "k8s.io/apimachinery/pkg/runtime/schema"

// Well-known resources kubed has dedicated views or enrichments for. CRD
// versions match what the clusters it was built against serve; Served() resolves the
// version a given cluster actually prefers.
var (
	Pods            = schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	Nodes           = schema.GroupVersionResource{Version: "v1", Resource: "nodes"}
	Namespaces      = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	Events          = schema.GroupVersionResource{Version: "v1", Resource: "events"}
	Services        = schema.GroupVersionResource{Version: "v1", Resource: "services"}
	ConfigMaps      = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	Secrets         = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	ServiceAccounts = schema.GroupVersionResource{Version: "v1", Resource: "serviceaccounts"}

	Deployments  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	StatefulSets = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	DaemonSets   = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	ReplicaSets  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	// ControllerRevisions are the versions of StatefulSets and DaemonSets.
	ControllerRevisions = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "controllerrevisions"}
	Jobs                = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	CronJobs            = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}

	PVCs = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	HPAs = schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}
	PDBs = schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}

	Ingresses = schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"}
	// IngressClasses say which controller takes an ingress, and which is the default.
	IngressClasses = schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingressclasses"}
	HTTPRoutes     = schema.GroupVersionResource{Group: "gateway.networking.k8s.io", Version: "v1", Resource: "httproutes"}

	NodePools       = schema.GroupVersionResource{Group: "karpenter.sh", Version: "v1", Resource: "nodepools"}
	NodeClaims      = schema.GroupVersionResource{Group: "karpenter.sh", Version: "v1", Resource: "nodeclaims"}
	ExternalSecrets = schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1", Resource: "externalsecrets"}
	Applications    = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}
)

// Served returns gvr with the version this cluster prefers, and whether the
// cluster serves it at all (false when a CRD is not installed).
func (r *Resources) Served(gvr schema.GroupVersionResource) (schema.GroupVersionResource, bool) {
	res, ok := r.ByGVR(gvr)
	if !ok {
		return gvr, false
	}
	return res.GVR, true
}
