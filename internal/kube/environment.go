package kube

import (
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Clusters can say what they are with a ConfigMap every authenticated user
// may read:
//
//	kubectl -n kube-public create configmap kubed --from-literal=environment=production
const (
	markerNamespace = "kube-public"
	markerName      = "kubed"
	markerKey       = "environment"
)

// MarkedEnvironment reads the cluster's own label ("production",
// "staging"…). ok is false when there is no marker or it can't be read.
func (c *Client) MarkedEnvironment(ctx context.Context) (env string, ok bool) {
	cm, err := c.Typed.CoreV1().ConfigMaps(markerNamespace).Get(ctx, markerName, metav1.GetOptions{})
	if err != nil {
		return "", false
	}
	env = strings.ToLower(strings.TrimSpace(cm.Data[markerKey]))
	return env, env != ""
}
