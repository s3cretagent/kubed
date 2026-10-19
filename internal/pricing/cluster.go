package pricing

// The whole cluster's bill: nodes are only part of it. Disks, NAT and data
// transfer, load balancers, logs and the EKS fee are billed too. AWS knows
// which lines belong to a cluster by their tags, so kubed asks Cost
// Explorer for everything tagged with this cluster's name, and for the tax
// rate on the invoice.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// EKSCluster finds the EKS cluster whose API endpoint is server, and its tags.
func (a AWS) EKSCluster(ctx context.Context, server string) (string, map[string]string, error) {
	out, err := a.run(ctx, "eks", "list-clusters", "--region", a.Region)
	if err != nil {
		return "", nil, err
	}
	var list struct{ Clusters []string }
	if err := json.Unmarshal(out, &list); err != nil {
		return "", nil, fmt.Errorf("eks list-clusters: %w", err)
	}
	want := strings.TrimSuffix(server, "/")
	for _, name := range list.Clusters {
		out, err := a.run(ctx, "eks", "describe-cluster", "--region", a.Region, "--name", name)
		if err != nil {
			continue
		}
		var d struct {
			Cluster struct {
				Endpoint string
				Tags     map[string]string
			}
		}
		if json.Unmarshal(out, &d) == nil && strings.TrimSuffix(d.Cluster.Endpoint, "/") == want {
			return name, d.Cluster.Tags, nil
		}
	}
	return "", nil, errors.New("no EKS cluster in this account has this API endpoint")
}

// ClusterTags are the tag pairs that mark a cluster's resources: the ones
// AWS and its controllers set, and the cluster's own tags whose value is
// its name (how a team tags "everything of cluster X").
func ClusterTags(name string, tags map[string]string) map[string][]string {
	out := map[string][]string{
		"aws:eks:cluster-name":          {name},
		"eks:cluster-name":              {name},
		"elbv2.k8s.aws/cluster":         {name},
		"karpenter.sh/discovery":        {name},
		"kubernetes.io/cluster/" + name: {"owned", "shared"},
	}
	for k, v := range tags {
		if v == name || strings.Contains(v, name) || (len(v) >= 6 && strings.Contains(name, v)) {
			if !contains(out[k], v) {
				out[k] = append(out[k], v)
			}
		}
	}
	return out
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// KubernetesServices are the AWS services that run a cluster. Only these
// count: databases, buckets and the like that share the cluster's tags are
// the apps' own infrastructure, not the cluster's.
var KubernetesServices = map[string]string{
	"Amazon Elastic Compute Cloud - Compute":          "nodes",
	"EC2 - Other":                                     "disks & network",
	"Amazon Elastic Load Balancing":                   "load balancers",
	"AmazonCloudWatch":                                "logs & metrics",
	"Amazon Elastic Container Service for Kubernetes": "EKS fee",
	"Amazon Virtual Private Cloud":                    "VPC",
}

// ClusterBill is what a cluster costs per day, from the bill.
type ClusterBill struct {
	FetchedAt time.Time          `json:"fetched_at"`
	Name      string             `json:"name"` // EKS cluster name
	Days      int                `json:"days"`
	Daily     map[string]float64 `json:"daily"`    // service → $/day, amortized, before tax
	TaxRate   float64            `json:"tax_rate"` // tax on the last full month's invoice, e.g. 0.18
}

// Kubernetes returns the cluster's infrastructure cost per day, by part.
func (b *ClusterBill) Kubernetes() (total float64, parts []Part) {
	byLabel := map[string]float64{}
	for svc, v := range b.Daily {
		if label, ok := KubernetesServices[svc]; ok {
			byLabel[label] += v
			total += v
		}
	}
	for l, v := range byLabel {
		parts = append(parts, Part{l, v})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Daily > parts[j].Daily })
	return total, parts
}

// Part is one line of a bill.
type Part struct {
	What  string
	Daily float64
}

// ClusterBill reads the last week's cost of everything tagged with the
// cluster, and last month's tax rate (two Cost Explorer requests).
func (a AWS) ClusterBill(ctx context.Context, name string, tags map[string][]string, now time.Time) (*ClusterBill, error) {
	end := now.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -billedDays)
	// The filter is built as values and encoded once, so a tag key or value
	// can hold any character without breaking the JSON.
	type values struct {
		Key    string
		Values []string
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var exprs []any
	for _, k := range keys {
		exprs = append(exprs, map[string]values{"Tags": {Key: k, Values: tags[k]}})
	}
	tagged := exprs[0]
	if len(exprs) > 1 {
		tagged = map[string][]any{"Or": exprs}
	}
	services := make([]string, 0, len(KubernetesServices))
	for svc := range KubernetesServices {
		services = append(services, svc)
	}
	sort.Strings(services)
	encoded, err := json.Marshal(map[string][]any{"And": {tagged, map[string]values{"Dimensions": {Key: "SERVICE", Values: services}}}})
	if err != nil {
		return nil, err
	}
	filter := string(encoded)
	out, err := a.run(ctx, "ce", "get-cost-and-usage", "--region", costExplorerRegion,
		"--time-period", "Start="+start.Format("2006-01-02")+",End="+end.Format("2006-01-02"),
		"--granularity", "MONTHLY", "--metrics", "AmortizedCost",
		"--group-by", "Type=DIMENSION,Key=SERVICE", "--filter", filter)
	if err != nil {
		return nil, err
	}
	totals, err := serviceTotals(out, "AmortizedCost")
	if err != nil {
		return nil, err
	}
	b := &ClusterBill{FetchedAt: now.UTC(), Name: name, Days: billedDays, Daily: map[string]float64{}}
	for svc, v := range totals {
		b.Daily[svc] = v / billedDays
	}

	// Tax is billed as its own service and lands in lumps (a Savings Plan's
	// tax on the 1st), so its rate comes from a whole month.
	monthEnd := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	out, err = a.run(ctx, "ce", "get-cost-and-usage", "--region", costExplorerRegion,
		"--time-period", "Start="+monthEnd.AddDate(0, -1, 0).Format("2006-01-02")+",End="+monthEnd.Format("2006-01-02"),
		"--granularity", "MONTHLY", "--metrics", "UnblendedCost", "--group-by", "Type=DIMENSION,Key=SERVICE")
	if err == nil {
		if all, err := serviceTotals(out, "UnblendedCost"); err == nil {
			b.TaxRate = taxRate(all)
		}
	}
	return b, nil
}

func serviceTotals(data []byte, metric string) (map[string]float64, error) {
	var resp struct {
		ResultsByTime []struct {
			Groups []struct {
				Keys    []string
				Metrics map[string]struct{ Amount string }
			}
		}
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cost explorer: %w", err)
	}
	out := map[string]float64{}
	for _, r := range resp.ResultsByTime {
		for _, g := range r.Groups {
			if len(g.Keys) == 0 {
				continue
			}
			v, _ := strconv.ParseFloat(g.Metrics[metric].Amount, 64)
			out[g.Keys[0]] += v
		}
	}
	return out, nil
}

// taxRate is the invoice's tax as a fraction of everything else.
func taxRate(byService map[string]float64) float64 {
	var tax, rest float64
	for svc, v := range byService {
		if svc == "Tax" {
			tax += v
		} else {
			rest += v
		}
	}
	if rest <= 0 || tax <= 0 {
		return 0
	}
	return tax / rest
}

// ClusterBill returns the cluster's bill from a cache under a day old, or
// from AWS. It checks the login is in the cluster's account first.
func (s *Store) ClusterBill(ctx context.Context, account, server string, a AWS) (*ClusterBill, error) {
	login, err := a.Account(ctx)
	if err != nil {
		return nil, err
	}
	if login != account {
		return nil, fmt.Errorf("your AWS login is account %s but the cluster is in %s", login, account)
	}
	path := filepath.Join(filepath.Dir(s.cachePath), "cluster-"+safeName(account+"-"+a.Region+"-"+server)+".json")
	if data, err := os.ReadFile(path); err == nil {
		var b ClusterBill
		if json.Unmarshal(data, &b) == nil && time.Since(b.FetchedAt) < MaxAge {
			return &b, nil
		}
	}
	name, tags, err := a.EKSCluster(ctx, server)
	if err != nil {
		return nil, err
	}
	b, err := a.ClusterBill(ctx, name, ClusterTags(name, tags), time.Now())
	if err != nil {
		return nil, err
	}
	if data, err := json.Marshal(b); err == nil {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		_ = os.WriteFile(path, data, 0o644)
	}
	return b, nil
}
