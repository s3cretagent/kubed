package kube

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
)

// Cordon marks a node unschedulable (or schedulable again).
func (c *Client) Cordon(ctx context.Context, node string, unschedulable bool) error {
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, unschedulable)
	_, err := c.Typed.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// DrainProgress reports how a drain is going.
type DrainProgress struct {
	Evicted, Total int
	Waiting        string // pod blocked by a PodDisruptionBudget, if any
}

// DrainRetry is how long to wait before retrying an eviction a
// PodDisruptionBudget refused.
var DrainRetry = 5 * time.Second

// Drain cordons a node and evicts its pods through the Eviction API, so
// PodDisruptionBudgets are respected. DaemonSet pods and static (mirror)
// pods are left alone, as `kubectl drain --ignore-daemonsets` does.
func (c *Client) Drain(ctx context.Context, node string, progress func(DrainProgress)) error {
	if err := c.Cordon(ctx, node, true); err != nil {
		return fmt.Errorf("cordon: %w", err)
	}
	list, err := c.Typed.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(),
	})
	if err != nil {
		return err
	}
	pods := Evictable(list.Items)
	p := DrainProgress{Total: len(pods)}
	progress(p)
	for _, pod := range pods {
		for {
			err := c.Typed.PolicyV1().Evictions(pod.Namespace).Evict(ctx, &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			})
			switch {
			case err == nil, apierrors.IsNotFound(err):
				p.Evicted++
				p.Waiting = ""
				progress(p)
			case apierrors.IsTooManyRequests(err): // PDB says not yet
				p.Waiting = pod.Namespace + "/" + pod.Name
				progress(p)
				select {
				case <-ctx.Done():
					return fmt.Errorf("drain stopped with %d of %d pods evicted: %w", p.Evicted, p.Total, ctx.Err())
				case <-time.After(DrainRetry):
				}
				continue
			default:
				return fmt.Errorf("evict %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			break
		}
	}
	return nil
}

// Evictable drops DaemonSet-owned, mirror (static) and finished pods.
func Evictable(pods []corev1.Pod) []corev1.Pod {
	var out []corev1.Pod
	for _, p := range pods {
		if _, mirror := p.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		daemon := false
		for _, o := range p.OwnerReferences {
			if o.Kind == "DaemonSet" {
				daemon = true
			}
		}
		if !daemon {
			out = append(out, p)
		}
	}
	return out
}
