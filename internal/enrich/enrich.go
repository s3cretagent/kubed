// Package enrich derives the EKS context kubed shows next to resources:
// Karpenter node details and price, a pod's IRSA role, ExternalSecret sync
// state and ArgoCD ownership. Everything is read from the informer cache.
package enrich

import (
	"regexp"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/pricing"
)

// Store is the read side of kube.Cache.
type Store interface {
	List(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured
	Get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool)
	Synced(gvr schema.GroupVersionResource) bool
}

// Enricher answers enrichment questions for one cluster.
type Enricher struct {
	store Store
	// ownedPods indexes pods by every owner up their chain (ReplicaSet,
	// Deployment…), rebuilt at most once a second: a list row asking for
	// its pods is a lookup, not a walk over every pod.
	podIndexMu sync.Mutex
	podIndexAt time.Time
	ownedPods  map[types.UID][]*unstructured.Unstructured
	res        *kube.Resources
	prices     *pricing.Table
	// What this account pays (nil when it can't be read): billed on-demand
	// rates per instance type, the discount they imply for types not on
	// the bill, and live spot prices per "type/zone".
	billed         map[string]float64
	billedDiscount float64
	spot           map[string]float64
	// discount is a percent off on-demand list prices set in config, used
	// when the bill can't be read.
	discount float64
	// costs: kubed found the cluster's AWS account and can log in to it.
	// Until then (and on clusters where it can't) nothing is priced.
	costs bool
	usage UsageSource

	podCounts   map[string]int
	podReqs     map[string][2]int64 // node -> CPU millicores and memory bytes its pods reserve
	podCountsAt time.Time

	// named: the crashing pods (and containers) home named last refresh,
	// so copies crashing alike don't swap each refresh (see Named).
	namedMu sync.Mutex
	named   map[string]bool
}

// Named says home named this pod's container last refresh.
func (e *Enricher) Named(ns, pod, container string) bool {
	e.namedMu.Lock()
	defer e.namedMu.Unlock()
	return e.named[ns+"/"+pod+"/"+container]
}

// SetNamed replaces the pods home names ("ns/pod/container").
func (e *Enricher) SetNamed(named map[string]bool) {
	e.namedMu.Lock()
	defer e.namedMu.Unlock()
	e.named = named
}

// New creates an Enricher. prices may be nil (prices then show as unknown).
func New(store Store, res *kube.Resources, prices *pricing.Table) *Enricher {
	return &Enricher{store: store, res: res, prices: prices}
}

// SetPrices swaps in a refreshed price table.
func (e *Enricher) SetPrices(t *pricing.Table) { e.prices = t }

// SetDiscount sets the percent off on-demand list prices, used when the
// account's bill can't be read.
func (e *Enricher) SetDiscount(pct float64) { e.discount = pct }

// SetBilled sets the account's billed on-demand rates per instance type.
// Types not on the bill get the average discount of those that are.
func (e *Enricher) SetBilled(b *pricing.Billed) {
	e.billed, e.billedDiscount = nil, 0
	if b == nil {
		return
	}
	e.billed = b.Rates
	var paid, list float64
	for t, r := range b.Rates {
		if p, ok := e.prices.Lookup(b.Region, t); ok && p.OnDemand > 0 {
			paid, list = paid+r, list+p.OnDemand
		}
	}
	if list > 0 && paid < list {
		e.billedDiscount = (1 - paid/list) * 100
	}
}

// SetCosts turns prices on once the cluster's AWS account is known.
func (e *Enricher) SetCosts(on bool) { e.costs = on }

// Costs reports whether kubed shows prices for this cluster.
func (e *Enricher) Costs() bool { return e != nil && e.costs }

// SetSpot sets live spot prices, keyed "type/zone".
func (e *Enricher) SetSpot(p map[string]float64) { e.spot = p }

// Discount is the percent below list price on-demand nodes are priced at:
// from the bill when it was read, else from config.
func (e *Enricher) Discount() float64 {
	if e.billed != nil {
		return e.billedDiscount
	}
	return e.discount
}

// Billed reports whether prices come from the account's own bill.
func (e *Enricher) Billed() bool { return e.billed != nil }

// LiveSpot reports whether spot prices are live from EC2.
func (e *Enricher) LiveSpot() bool { return e.spot != nil }

// Prices returns the current price table (may be nil).
func (e *Enricher) Prices() *pricing.Table { return e.prices }

// served resolves a well-known GVR on this cluster; ok is false when the
// CRD is not installed.
func (e *Enricher) served(gvr schema.GroupVersionResource) (schema.GroupVersionResource, bool) {
	if e.res == nil {
		return gvr, true
	}
	return e.res.Served(gvr)
}

// list and get return nothing for resources the cluster does not serve, so
// callers need no CRD checks of their own.
func (e *Enricher) list(gvr schema.GroupVersionResource, ns string) []*unstructured.Unstructured {
	if g, ok := e.served(gvr); ok {
		return e.store.List(g, ns)
	}
	return nil
}

func (e *Enricher) get(gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, bool) {
	if g, ok := e.served(gvr); ok {
		return e.store.Get(g, ns, name)
	}
	return nil, false
}

// TopOwner follows controller owner references (Pod → ReplicaSet →
// Deployment) and returns the outermost object found in the cache.
func (e *Enricher) TopOwner(obj *unstructured.Unstructured) *unstructured.Unstructured {
	cur := obj
	for range 4 {
		next := e.controller(cur)
		if next == nil {
			return cur
		}
		cur = next
	}
	return cur
}

func (e *Enricher) controller(obj *unstructured.Unstructured) *unstructured.Unstructured {
	if e.res == nil {
		return nil
	}
	ref, ok := controllerRef(obj)
	if !ok {
		return nil
	}
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil
	}
	res, ok := e.res.ByKind(gv.Group, ref.Kind)
	if !ok {
		return nil
	}
	ns := obj.GetNamespace()
	if !res.Namespaced {
		ns = ""
	}
	owner, found := e.store.Get(res.GVR, ns, ref.Name)
	if !found {
		// A CronJob isn't loaded for most screens; its name is all the
		// grouping needs. A run's Job is named <cronjob>-<schedule number>.
		switch {
		case ref.Kind == "CronJob":
			return stub("batch/v1", "CronJob", ns, ref.Name)
		case ref.Kind == "Job" && cronRun.MatchString(ref.Name):
			return stub("batch/v1", "CronJob", ns, cronRun.ReplaceAllString(ref.Name, ""))
		}
	}
	if !found && ref.Kind == "ReplicaSet" {
		// ReplicaSets load late on big clusters (thousands of old ones). A
		// Deployment names its ReplicaSets <deployment>-<pod-template-hash>
		// and stamps that hash on every pod, so go to the Deployment
		// directly.
		if hash := obj.GetLabels()["pod-template-hash"]; hash != "" && strings.HasSuffix(ref.Name, "-"+hash) {
			if gvr, ok := e.served(kube.Deployments); ok {
				d, _ := e.store.Get(gvr, ns, strings.TrimSuffix(ref.Name, "-"+hash))
				return d
			}
		}
	}
	return owner
}

// cronRun matches the number a CronJob appends to each run's Job name.
var cronRun = regexp.MustCompile(`-\d{8,}$`)

// stub is a stand-in for an owner that isn't loaded: kind and name only.
func stub(apiVersion, kind, ns, name string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetAPIVersion(apiVersion)
	o.SetKind(kind)
	o.SetNamespace(ns)
	o.SetName(name)
	return o
}

func controllerRef(obj *unstructured.Unstructured) (metav1.OwnerReference, bool) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return ref, true
		}
	}
	return metav1.OwnerReference{}, false
}

// Controller returns the object's controlling owner from the cache, if any.
func (e *Enricher) Controller(obj *unstructured.Unstructured) *unstructured.Unstructured {
	return e.controller(obj)
}

// NodeObject returns a node from the cache.
func (e *Enricher) NodeObject(name string) (*unstructured.Unstructured, bool) {
	return e.get(kube.Nodes, "", name)
}
