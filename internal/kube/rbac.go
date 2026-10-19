package kube

import (
	"context"

	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Binding grants a role to a subject.
type Binding struct {
	Kind, Name, Namespace string // RoleBinding or ClusterRoleBinding
	RoleKind, RoleName    string
	Via                   string // a group the ServiceAccount is in, when the binding names that group
	Rules                 []rbacv1.PolicyRule
}

// ServiceAccountBindings lists the RoleBindings (in the SA's namespace) and
// ClusterRoleBindings that name the ServiceAccount, with their rules.
func (c *Client) ServiceAccountBindings(ctx context.Context, ns, sa string) ([]Binding, error) {
	// Named directly, or through a group every service account is in.
	groups := map[string]bool{"system:serviceaccounts": true, "system:serviceaccounts:" + ns: true, "system:authenticated": true}
	match := func(subjects []rbacv1.Subject) (bool, string) {
		for _, s := range subjects {
			if s.Kind == rbacv1.ServiceAccountKind && s.Name == sa && s.Namespace == ns {
				return true, ""
			}
		}
		for _, s := range subjects {
			if s.Kind == rbacv1.GroupKind && groups[s.Name] {
				return true, s.Name
			}
		}
		return false, ""
	}
	isSA := func(subjects []rbacv1.Subject) bool { ok, _ := match(subjects); return ok }
	rbac := c.Typed.RbacV1()
	var out []Binding

	// RoleBindings anywhere can name it (leader election in kube-system…);
	// if listing them everywhere isn't allowed, its own namespace.
	rbs, err := rbac.RoleBindings("").List(ctx, metav1.ListOptions{})
	if err != nil {
		if rbs, err = rbac.RoleBindings(ns).List(ctx, metav1.ListOptions{}); err != nil {
			return nil, err
		}
	}
	for _, b := range rbs.Items {
		if ok, via := match(b.Subjects); ok {
			out = append(out, c.withRules(ctx, Binding{Kind: "RoleBinding", Name: b.Name, Namespace: b.Namespace, RoleKind: b.RoleRef.Kind, RoleName: b.RoleRef.Name, Via: via}))
		}
	}
	crbs, err := rbac.ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, b := range crbs.Items {
		if isSA(b.Subjects) {
			_, via := match(b.Subjects)
			out = append(out, c.withRules(ctx, Binding{Kind: "ClusterRoleBinding", Name: b.Name, RoleKind: b.RoleRef.Kind, RoleName: b.RoleRef.Name, Via: via}))
		}
	}
	return out, nil
}

func (c *Client) withRules(ctx context.Context, b Binding) Binding {
	if b.RoleKind == "ClusterRole" {
		if r, err := c.Typed.RbacV1().ClusterRoles().Get(ctx, b.RoleName, metav1.GetOptions{}); err == nil {
			b.Rules = r.Rules
		}
		return b
	}
	if r, err := c.Typed.RbacV1().Roles(b.Namespace).Get(ctx, b.RoleName, metav1.GetOptions{}); err == nil {
		b.Rules = r.Rules
	}
	return b
}

// Access is a yes/no answer for one verb on one resource.
type Access struct {
	Resource string // e.g. "pods", "pods/exec", "deployments.apps"
	Verb     string
	Allowed  bool
}

// accessChecks are the questions :can-i asks. SelfSubjectRulesReview can't
// see permissions granted by webhook authorizers (EKS access entries), so
// each one is asked directly with a SelfSubjectAccessReview.
var accessChecks = []struct {
	group, resource, sub string
	verbs                []string
}{
	{"", "pods", "", []string{"get", "list", "delete"}},
	{"", "pods", "log", []string{"get"}},
	{"", "pods", "exec", []string{"create"}},
	{"", "pods", "portforward", []string{"create"}},
	{"apps", "deployments", "", []string{"get", "list", "patch", "delete"}},
	{"apps", "deployments", "scale", []string{"patch"}},
	{"apps", "statefulsets", "", []string{"get", "patch"}},
	{"batch", "jobs", "", []string{"create", "delete"}},
	{"batch", "cronjobs", "", []string{"get", "patch"}},
	{"", "secrets", "", []string{"get", "list"}},
	{"", "configmaps", "", []string{"get", "patch"}},
	{"", "services", "", []string{"get", "patch"}},
	{"", "nodes", "", []string{"get", "patch"}},
	{"", "pods", "eviction", []string{"create"}},
	{"", "events", "", []string{"list"}},
}

// AccessMatrix asks every accessChecks question for ns, in parallel.
func (c *Client) AccessMatrix(ctx context.Context, ns string) ([]Access, error) {
	var out []Access
	type job struct {
		i                          int
		group, resource, sub, verb string
	}
	var jobs []job
	for _, q := range accessChecks {
		name := q.resource
		if q.group != "" {
			name += "." + q.group
		}
		if q.sub != "" {
			name += "/" + q.sub
		}
		for _, v := range q.verbs {
			jobs = append(jobs, job{len(out), q.group, q.resource, q.sub, v})
			out = append(out, Access{Resource: name, Verb: v})
		}
	}
	errs := make(chan error, len(jobs))
	sem := make(chan struct{}, 8)
	for _, j := range jobs {
		sem <- struct{}{}
		go func(j job) {
			defer func() { <-sem }()
			review, err := c.Typed.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authv1.SelfSubjectAccessReview{
				Spec: authv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authv1.ResourceAttributes{
					Namespace: ns, Verb: j.verb, Group: j.group, Resource: j.resource, Subresource: j.sub,
				}},
			}, metav1.CreateOptions{})
			if err != nil {
				errs <- err
				return
			}
			out[j.i].Allowed = review.Status.Allowed
			errs <- nil
		}(j)
	}
	var first error
	for range jobs {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return out, first
}
