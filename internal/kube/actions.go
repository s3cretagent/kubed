package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

// Ref identifies one object.
type Ref struct {
	Resource  Resource
	Namespace string
	Name      string
}

func (r Ref) String() string {
	if r.Namespace == "" {
		return fmt.Sprintf("%s/%s", r.Resource.GVR.Resource, r.Name)
	}
	return fmt.Sprintf("%s/%s/%s", r.Resource.GVR.Resource, r.Namespace, r.Name)
}

// RefOf builds a Ref for an object of a resource.
func RefOf(res Resource, obj *unstructured.Unstructured) Ref {
	return Ref{Resource: res, Namespace: obj.GetNamespace(), Name: obj.GetName()}
}

func (c *Client) resource(r Ref) dynamic.ResourceInterface {
	if r.Resource.Namespaced {
		return c.Dynamic.Resource(r.Resource.GVR).Namespace(r.Namespace)
	}
	return c.Dynamic.Resource(r.Resource.GVR)
}

// Get fetches the live object from the API server (not the cache).
func (c *Client) Get(ctx context.Context, r Ref) (*unstructured.Unstructured, error) {
	return c.resource(r).Get(ctx, r.Name, metav1.GetOptions{})
}

// YAML returns the live object as YAML without managedFields.
func (c *Client) YAML(ctx context.Context, r Ref) (string, error) {
	obj, err := c.Get(ctx, r)
	if err != nil {
		return "", err
	}
	obj.SetManagedFields(nil)
	out, err := yaml.Marshal(obj.Object)
	return string(out), err
}

// SuspendCronJob pauses (or resumes) a CronJob's schedule.
func (c *Client) SuspendCronJob(ctx context.Context, ns, name string, suspend bool) error {
	patch := fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)
	_, err := c.Typed.BatchV1().CronJobs(ns).Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// Delete removes an object with background propagation, like kubectl.
func (c *Client) Delete(ctx context.Context, r Ref) error {
	policy := metav1.DeletePropagationBackground
	return c.resource(r).Delete(ctx, r.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

// Scale sets replicas through the scale subresource.
func (c *Client) Scale(ctx context.Context, r Ref, replicas int32) error {
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := c.resource(r).Patch(ctx, r.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}, "scale")
	return err
}

// RolloutRestart does what `kubectl rollout restart` does: bump a pod
// template annotation so the controller rolls every pod.
func (c *Client) RolloutRestart(ctx context.Context, r Ref) error {
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`,
		time.Now().Format(time.RFC3339))
	_, err := c.resource(r).Patch(ctx, r.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// Update replaces an object edited by the user. The object's resourceVersion
// makes this fail if someone else changed it meanwhile (same as kubectl
// edit). Changes the cluster makes on its own (status, the revision
// number during a rollout) don't count: when only those moved since orig,
// the edit goes on top. A server dry-run runs first so validation errors
// surface before anything is written.
func (c *Client) Update(ctx context.Context, r Ref, orig, editedYAML []byte) error {
	var obj unstructured.Unstructured
	if err := yaml.Unmarshal(editedYAML, &obj.Object); err != nil {
		return fmt.Errorf("parse edited YAML: %w", err)
	}
	if obj.GetName() != r.Name || obj.GetNamespace() != r.Namespace {
		return fmt.Errorf("name and namespace cannot be changed while editing")
	}
	ri := c.resource(r)
	update := func(opts metav1.UpdateOptions) error {
		_, err := ri.Update(ctx, &obj, opts)
		if !apierrors.IsConflict(err) {
			return err
		}
		cur, gerr := ri.Get(ctx, r.Name, metav1.GetOptions{})
		var was unstructured.Unstructured
		if gerr != nil || yaml.Unmarshal(orig, &was.Object) != nil || !sameIntent(&was, cur) {
			return err // a real change by someone else
		}
		obj.SetResourceVersion(cur.GetResourceVersion())
		_, err = ri.Update(ctx, &obj, opts)
		return err
	}
	// Strict: a field the cluster doesn't know ("3eplicas" from a slip of
	// the keyboard) is refused, not silently dropped (as kubectl edit does).
	if err := update(metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}, FieldValidation: "Strict"}); err != nil {
		return fmt.Errorf("dry run: %w", err)
	}
	return update(metav1.UpdateOptions{FieldValidation: "Strict"})
}

// sameIntent: two copies of an object differ only in what the cluster
// writes on its own.
func sameIntent(a, b *unstructured.Unstructured) bool {
	strip := func(o *unstructured.Unstructured) map[string]any {
		c := o.DeepCopy().Object
		delete(c, "status")
		if md, ok := c["metadata"].(map[string]any); ok {
			for _, k := range []string{"resourceVersion", "managedFields", "generation"} {
				delete(md, k)
			}
			if ann, ok := md["annotations"].(map[string]any); ok {
				delete(ann, "deployment.kubernetes.io/revision")
				if len(ann) == 0 {
					delete(md, "annotations")
				}
			}
		}
		return c
	}
	// Through JSON, so 2 read from YAML (float64) equals 2 from the API (int64).
	norm := func(m map[string]any) any {
		var out any
		if b, err := json.Marshal(m); err == nil && json.Unmarshal(b, &out) == nil {
			return out
		}
		return m
	}
	return equality.Semantic.DeepEqual(norm(strip(a)), norm(strip(b)))
}

// Kill deletes an object immediately (grace period 0), like
// `kubectl delete --grace-period=0 --force`.
func (c *Client) Kill(ctx context.Context, r Ref) error {
	zero := int64(0)
	policy := metav1.DeletePropagationBackground
	return c.resource(r).Delete(ctx, r.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
}

// SetImage changes one container's image in a workload's pod template.
// Strategic merge matches the container by name, so other fields stay.
func (c *Client) SetImage(ctx context.Context, r Ref, container, image string, initContainer bool) error {
	list := "containers"
	if initContainer {
		list = "initContainers"
	}
	spec := map[string]any{list: []any{map[string]any{"name": container, "image": image}}}
	template := map[string]any{"spec": spec}
	var patch map[string]any
	if r.Resource.Kind == "CronJob" {
		patch = map[string]any{"spec": map[string]any{"jobTemplate": map[string]any{"spec": map[string]any{"template": template}}}}
	} else {
		patch = map[string]any{"spec": map[string]any{"template": template}}
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.resource(r).Patch(ctx, r.Name, types.StrategicMergePatchType, body, metav1.PatchOptions{})
	return err
}

// TriggerCronJob starts a Job from a CronJob's template now, like
// `kubectl create job --from=cronjob/<name>`. It returns the Job's name.
func (c *Client) TriggerCronJob(ctx context.Context, ns, name string) (string, error) {
	cj, err := c.Typed.BatchV1().CronJobs(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	jobName := fmt.Sprintf("%s-manual-%d", name, time.Now().Unix())
	if len(jobName) > 63 {
		jobName = jobName[len(jobName)-63:]
	}
	isController := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   ns,
			Labels:      cj.Spec.JobTemplate.Labels,
			Annotations: map[string]string{"cronjob.kubernetes.io/instantiate": "manual"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "CronJob", Name: cj.Name, UID: cj.UID, Controller: &isController,
			}},
		},
		Spec: cj.Spec.JobTemplate.Spec,
	}
	_, err = c.Typed.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{})
	return jobName, err
}

// revisionTo is RolloutTo for a StatefulSet or DaemonSet: it applies a
// kept ControllerRevision's data, as `kubectl rollout undo` does for them.
func (c *Client) revisionTo(ctx context.Context, r Ref, revision int) (int, error) {
	var owner metav1.Object
	var err error
	if r.Resource.GVR.Resource == "daemonsets" {
		owner, err = c.Typed.AppsV1().DaemonSets(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	} else {
		owner, err = c.Typed.AppsV1().StatefulSets(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	}
	if err != nil {
		return 0, err
	}
	list, err := c.Typed.AppsV1().ControllerRevisions(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	var current int64
	var mine []*appsv1.ControllerRevision
	for i := range list.Items {
		if cr := &list.Items[i]; metav1.IsControlledBy(cr, owner) {
			mine = append(mine, cr)
			current = max(current, cr.Revision)
		}
	}
	var target *appsv1.ControllerRevision
	for _, cr := range mine {
		switch {
		case revision > 0 && cr.Revision == int64(revision):
			target = cr
		case revision == 0 && cr.Revision < current && (target == nil || cr.Revision > target.Revision):
			target = cr
		}
	}
	if target == nil {
		if revision > 0 {
			return 0, fmt.Errorf("%s no longer keeps revision %d", r.Name, revision)
		}
		return 0, fmt.Errorf("%s has no earlier version to go back to", r.Name)
	}
	// The revision's data is a patch that replaces the pod template.
	if r.Resource.GVR.Resource == "daemonsets" {
		_, err = c.Typed.AppsV1().DaemonSets(r.Namespace).Patch(ctx, r.Name, types.StrategicMergePatchType, target.Data.Raw, metav1.PatchOptions{})
	} else {
		_, err = c.Typed.AppsV1().StatefulSets(r.Namespace).Patch(ctx, r.Name, types.StrategicMergePatchType, target.Data.Raw, metav1.PatchOptions{})
	}
	return int(target.Revision), err
}

// RolloutUndo puts a Deployment back on its previous ReplicaSet's pod
// template, as `kubectl rollout undo` does. It returns the revision it
// went back to.
func (c *Client) RolloutUndo(ctx context.Context, r Ref) (int, error) {
	return c.RolloutTo(ctx, r, 0)
}

// RolloutTo puts a Deployment on the pod template of one of its kept
// versions (revision 0: the one before the current), as `kubectl rollout
// undo --to-revision` does. It returns the revision it went to.
func (c *Client) RolloutTo(ctx context.Context, r Ref, revision int) (int, error) {
	if res := r.Resource.GVR.Resource; res == "statefulsets" || res == "daemonsets" {
		return c.revisionTo(ctx, r, revision)
	}
	dep, err := c.Typed.AppsV1().Deployments(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	sets, err := c.Typed.AppsV1().ReplicaSets(r.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	current, _ := strconv.Atoi(dep.Annotations["deployment.kubernetes.io/revision"])
	var prev *appsv1.ReplicaSet
	prevRev := 0
	for i := range sets.Items {
		rs := &sets.Items[i]
		if !metav1.IsControlledBy(rs, dep) {
			continue
		}
		rev, _ := strconv.Atoi(rs.Annotations["deployment.kubernetes.io/revision"])
		switch {
		case revision > 0 && rev == revision:
			prev, prevRev = rs, rev
		case revision == 0 && rev < current && rev > prevRev:
			prev, prevRev = rs, rev
		}
	}
	if prev == nil {
		if revision > 0 {
			return 0, fmt.Errorf("%s no longer keeps revision %d", r.Name, revision)
		}
		return 0, fmt.Errorf("%s has no earlier version to go back to", r.Name)
	}
	tmpl := prev.Spec.Template.DeepCopy()
	delete(tmpl.Labels, appsv1.DefaultDeploymentUniqueLabelKey) // pod-template-hash belongs to the ReplicaSet
	// Replace the whole template, as kubectl rollout undo does. A merge
	// would keep what the old version never had (a newer restart stamp),
	// making a new version instead of bringing the old one back.
	patch, err := json.Marshal([]map[string]any{{"op": "replace", "path": "/spec/template", "value": tmpl}})
	if err != nil {
		return 0, err
	}
	_, err = c.Typed.AppsV1().Deployments(r.Namespace).Patch(ctx, r.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	return prevRev, err
}
