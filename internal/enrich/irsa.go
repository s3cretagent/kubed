package enrich

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
)

const annotationRoleARN = "eks.amazonaws.com/role-arn"

// IRSA returns the pod's ServiceAccount and the IAM role ARN annotated on it
// (empty when the SA has no IRSA role).
func (e *Enricher) IRSA(pod *unstructured.Unstructured) (sa, roleARN string) {
	sa, _, _ = unstructured.NestedString(pod.Object, "spec", "serviceAccountName")
	if sa == "" {
		sa = "default"
	}
	obj, ok := e.get(kube.ServiceAccounts, pod.GetNamespace(), sa)
	if !ok {
		return sa, ""
	}
	return sa, obj.GetAnnotations()[annotationRoleARN]
}

// annotationKube2IAM is the pod annotation kube2iam and kiam read.
const annotationKube2IAM = "iam.amazonaws.com/role"

// PodRole is the AWS role a pod runs as: from its service account (IRSA),
// or else from the pod's own kube2iam/kiam annotation. via says which.
func (e *Enricher) PodRole(pod *unstructured.Unstructured) (sa, role, via string) {
	sa, role = e.IRSA(pod)
	if role != "" {
		return sa, role, "through service account " + sa
	}
	if r := pod.GetAnnotations()[annotationKube2IAM]; r != "" {
		return sa, r, "set on the pod, for kube2iam"
	}
	return sa, "", ""
}

// ServiceAccountRole returns the IRSA role annotated on a ServiceAccount.
func ServiceAccountRole(sa *unstructured.Unstructured) string {
	return sa.GetAnnotations()[annotationRoleARN]
}
