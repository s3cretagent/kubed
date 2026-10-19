package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/pricing"
)

// spotRefresh is how often live spot prices are read again.
const spotRefresh = 15 * time.Minute

// rates tracks reading what this AWS account really pays (see
// pricing.AWS). Everything runs in the background; until it answers, or
// when it can't, nodes are priced from the public list.
type rates struct {
	aws     pricing.AWS
	viaAWS  bool   // the cluster is on EKS, so AWS prices apply
	account string // the AWS account that owns the cluster

	// Before reading prices, kubed finds the cluster's account and an aws
	// CLI login for it: the kubeconfig's own (aws eks get-token), else the
	// current AWS environment or a matching profile in ~/.aws/config.
	fromKubeconfig bool
	resolving      bool
	resolved       bool
	resolveErr     error
	login          string // how kubed logs in, for ? help

	billed        *pricing.Billed
	billedErr     error
	billedStarted bool

	cluster        *pricing.ClusterBill // everything tagged with the cluster
	clusterErr     error
	clusterDone    bool // the cluster bill came back, with or without one
	clusterStarted bool

	spotErr     error
	spotLoading bool
	spotAt      time.Time
	spotTypes   string // the spot instance types last priced
}

type resolvedMsg struct {
	context, account, login string
	env                     []string
	err                     error
}

type clusterBillMsg struct {
	context string
	bill    *pricing.ClusterBill
	err     error
}

type billedMsg struct {
	context string
	billed  *pricing.Billed
	err     error
}

type spotMsg struct {
	context string
	types   string
	prices  map[string]float64
	err     error
}

// resetRates starts over for a new connection.
func (m *Model) resetRates(c *kube.Client) {
	m.rates = rates{}
	env, region, fromKubeconfig := c.AWSLogin()
	if !fromKubeconfig && c.EKSRegion() == "" {
		return // not EKS: list prices don't apply either way
	}
	for _, r := range []string{region, arnRegion(m.context), arnRegion(c.Cluster), c.EKSRegion()} {
		if region == "" {
			region = r
		}
	}
	account := arnAccount(m.context)
	if account == "" {
		account = arnAccount(c.Cluster)
	}
	m.rates = rates{aws: pricing.AWS{Env: env, Region: region}, viaAWS: true, account: account, fromKubeconfig: fromKubeconfig}
}

// resolveLogin finds the cluster's account and how to log in to it.
func (m *Model) resolveLogin() tea.Cmd {
	r := &m.rates
	r.resolving = true
	c, ctx, account, region, env, fromKubeconfig := m.client, m.context, r.account, r.aws.Region, r.aws.Env, r.fromKubeconfig
	return func() tea.Msg {
		cctx, cancel := context.WithTimeout(context.Background(), 2*apiTimeout)
		defer cancel()
		if account == "" {
			a, err := c.AWSAccount(cctx)
			if err != nil {
				return resolvedMsg{context: ctx, err: err}
			}
			account = a
		}
		if fromKubeconfig {
			// Check the kubeconfig's login really is in the cluster's account.
			got, err := (pricing.AWS{Env: env, Region: region}).Account(cctx)
			if err == nil && got != account {
				err = fmt.Errorf("your AWS login is account %s but the cluster is in %s", got, account)
			}
			return resolvedMsg{context: ctx, account: account, env: env, login: "your kubeconfig's aws login", err: err}
		}
		env, err := pricing.LoginFor(cctx, account, region)
		login := "your AWS environment"
		if len(env) > 0 {
			login = "profile " + strings.TrimPrefix(env[0], "AWS_PROFILE=")
		}
		return resolvedMsg{context: ctx, account: account, env: env, login: login, err: err}
	}
}

func (m *Model) onResolved(msg resolvedMsg) {
	if msg.context != m.context {
		return
	}
	r := &m.rates
	r.resolving, r.resolved, r.resolveErr = false, true, msg.err
	if msg.account != "" {
		r.account = msg.account // known even when the login failed, for ? help
	}
	if msg.err == nil {
		r.account, r.aws.Env, r.login = msg.account, msg.env, msg.login
	}
}

// arnAccount reads the account from an EKS ARN.
func arnAccount(s string) string {
	parts := strings.Split(s, ":")
	if len(parts) >= 6 && parts[0] == "arn" && parts[2] == "eks" {
		return parts[4]
	}
	return ""
}

// arnRegion reads the region from an EKS ARN context
// ("arn:aws:eks:<region>:<account>:cluster/<name>").
func arnRegion(ctx string) string {
	parts := strings.Split(ctx, ":")
	if len(parts) >= 6 && parts[0] == "arn" && parts[2] == "eks" {
		return parts[3]
	}
	return ""
}

// rateCmds starts whatever account-price reads are due. Called every tick.
func (m *Model) rateCmds() tea.Cmd {
	r := &m.rates
	if !r.viaAWS || m.enr == nil || m.client == nil {
		return nil
	}
	if r.aws.Region == "" {
		if !m.client.Cache.Synced(kube.Nodes) {
			return nil
		}
		r.aws.Region = m.enr.ClusterSummary().Region
		if r.aws.Region == "" {
			r.viaAWS = false
			return nil
		}
	}
	if !r.resolved {
		if !r.resolving {
			return m.resolveLogin()
		}
		return nil
	}
	if r.resolveErr != nil {
		return nil // list prices; ? says why
	}
	var cmds []tea.Cmd
	if !r.billedStarted {
		r.billedStarted = true
		aws, ctx, store, account := r.aws, m.context, m.prices, r.account
		cmds = append(cmds, func() tea.Msg {
			b, err := store.Billed(context.Background(), account, aws)
			return billedMsg{context: ctx, billed: b, err: err}
		})
	}
	if !r.clusterStarted && r.billedStarted && !m.rates.billedPending() {
		r.clusterStarted = true
		aws, ctx, store, account, server := r.aws, m.context, m.prices, r.account, m.client.Server
		cmds = append(cmds, func() tea.Msg {
			b, err := store.ClusterBill(context.Background(), account, server, aws)
			return clusterBillMsg{context: ctx, bill: b, err: err}
		})
	}
	if !r.spotLoading && m.client.Cache.Synced(kube.Nodes) {
		types := m.enr.SpotTypes()
		key := strings.Join(types, " ")
		if key != r.spotTypes || time.Since(r.spotAt) > spotRefresh {
			r.spotLoading = true
			aws, ctx := r.aws, m.context
			cmds = append(cmds, func() tea.Msg {
				p, err := aws.SpotPrices(context.Background(), types)
				return spotMsg{context: ctx, types: key, prices: p, err: err}
			})
		}
	}
	return tea.Batch(cmds...)
}

// billedPending: the node rates are still being read (the cluster bill
// waits for them, so the two don't race to write the cache).
func (r *rates) billedPending() bool { return r.billedStarted && r.billed == nil && r.billedErr == nil }

func (m *Model) onClusterBill(msg clusterBillMsg) {
	if msg.context != m.context {
		return
	}
	m.rates.cluster, m.rates.clusterErr, m.rates.clusterDone = msg.bill, msg.err, true
}

// allIn is the cluster's monthly cost from its bill, before tax, and the
// parts of it; ok is false when there is no usable bill. A bill whose nodes
// come to well under what the nodes cost means the cluster's tags aren't
// cost allocation tags yet, so it isn't shown.
func (m *Model) allIn() (monthly float64, parts []pricing.Part, ok bool) {
	b := m.rates.cluster
	if b == nil || m.enr == nil {
		return 0, nil, false
	}
	daily, parts := b.Kubernetes()
	nodes := m.enr.ClusterSummary().Hourly * 24
	if nodes > 0 && b.Daily["Amazon Elastic Compute Cloud - Compute"] < nodes/2 {
		return 0, nil, false
	}
	return daily * hoursPerMonth / 24, parts, true
}

// hoursPerMonth: 730 hours, the month AWS prices use.
const hoursPerMonth = 730

func (m *Model) onBilled(msg billedMsg) {
	if msg.context != m.context || m.enr == nil {
		return
	}
	m.rates.billed, m.rates.billedErr = msg.billed, msg.err
	m.enr.SetBilled(msg.billed)
	// Costs appear once, with the bill's rates (or list prices when the
	// bill can't be read), instead of changing a moment after they show.
	m.enr.SetCosts(true)
}

func (m *Model) onSpot(msg spotMsg) {
	if msg.context != m.context || m.enr == nil {
		return
	}
	r := &m.rates
	r.spotLoading, r.spotAt, r.spotTypes, r.spotErr = false, time.Now(), msg.types, msg.err
	if msg.err == nil {
		m.enr.SetSpot(msg.prices)
	}
}

// priceSource says, in a few words, where node prices come from.
func (m *Model) priceSource() (short, long string) {
	if m.enr == nil {
		return "AWS list", ""
	}
	var on, spot string
	switch {
	case m.enr.Billed():
		on = "on-demand: your bill"
		if d := m.enr.Discount(); d >= 0.5 {
			on += " (" + fmtPct(d) + " below list)"
		}
	case m.enr.Discount() > 0:
		on = "on-demand: list − " + fmtPct(m.enr.Discount()) + " (config)"
	default:
		on = "on-demand: AWS list"
	}
	if m.enr.LiveSpot() {
		spot = "spot: live"
	} else {
		spot = "spot: recent average"
	}
	short = on + " · " + spot

	var why []string
	if m.rates.login != "" {
		why = append(why, "AWS: account "+m.rates.account+" via "+m.rates.login)
	}
	if m.rates.resolveErr != nil {
		why = append(why, "can't read your AWS prices: "+m.rates.resolveErr.Error())
	}
	if m.rates.billedErr != nil {
		why = append(why, "bill not read: "+m.rates.billedErr.Error()+" (needs ce:GetCostAndUsage)")
	}
	if m.rates.clusterErr != nil {
		why = append(why, "cluster bill not read: "+m.rates.clusterErr.Error())
	} else if b := m.rates.cluster; b != nil {
		if monthly, parts, ok := m.allIn(); ok {
			why = append(why, fmt.Sprintf("All-in ≈ %s/month: EKS cluster %s, its tagged costs over the last %d days", dollars(monthly), b.Name, b.Days))
			for _, p := range parts {
				why = append(why, fmt.Sprintf("  %-16s %8s/month", p.What, dollars(p.Daily*hoursPerMonth/24)))
			}
			why = append(why, "These are what the bill says for the last days. The header's Nodes figure is today's nodes at today's prices, so the two can differ.")
		} else {
			why = append(why, "the all-in cost needs this cluster's AWS tags turned on for billing (AWS Billing, Cost allocation tags)")
		}
		if b.TaxRate > 0 {
			why = append(why, "Tax: last month's invoice added "+fmtPct(b.TaxRate*100)+"; kubed's figures are before tax")
		}
	}
	if m.rates.spotErr != nil {
		why = append(why, "live spot not read: "+m.rates.spotErr.Error()+" (needs ec2:DescribeSpotPriceHistory)")
	}
	return short, strings.Join(why, "\n")
}
