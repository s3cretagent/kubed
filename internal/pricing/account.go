package pricing

// What this AWS account really pays, read with the aws CLI using the same
// credentials the kubeconfig logs in with (`aws eks get-token`). Spot comes
// live from EC2 (free); on-demand comes from Cost Explorer: what the account
// paid per hour for each instance type last week, so Savings Plans, Reserved
// Instances and private discounts are in it, and an account without any gets
// list price. Without access, kubed keeps the public list prices.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AWS runs the aws CLI as the kubeconfig does.
type AWS struct {
	Env    []string // extra environment, e.g. AWS_PROFILE=prod
	Region string
}

// awsTimeout bounds one aws CLI call.
const awsTimeout = 30 * time.Second

// costExplorerRegion is where the Cost Explorer API lives.
const costExplorerRegion = "us-east-1"

// runAWS runs the aws CLI; tests replace it.
var runAWS = func(ctx context.Context, a AWS, args ...string) ([]byte, error) { return a.exec(ctx, args...) }

func (a AWS) run(ctx context.Context, args ...string) ([]byte, error) { return runAWS(ctx, a, args...) }

func (a AWS) exec(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, awsTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "aws", append(args, "--output", "json")...)
	cmd.Env = append(append(os.Environ(), a.Env...), "AWS_PAGER=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if i := strings.IndexByte(msg, '\n'); i > 0 {
			msg = msg[:i]
		}
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("aws %s %s: %s", args[0], args[1], msg)
	}
	return out, nil
}

// Account returns the AWS account the CLI is logged in to.
func (a AWS) Account(ctx context.Context) (string, error) {
	out, err := a.run(ctx, "sts", "get-caller-identity")
	if err != nil {
		return "", err
	}
	var id struct{ Account string }
	if err := json.Unmarshal(out, &id); err != nil || id.Account == "" {
		return "", fmt.Errorf("aws sts get-caller-identity: no account in the answer")
	}
	return id.Account, nil
}

// SpotPrices returns the current spot price of each instance type in each
// zone of the region, keyed "type/zone-id" ("t3a.medium/aps1-az1"). Spot
// prices are public: the same whichever account asks.
func (a AWS) SpotPrices(ctx context.Context, types []string) (map[string]float64, error) {
	if len(types) == 0 {
		return map[string]float64{}, nil
	}
	args := []string{"ec2", "describe-spot-price-history", "--region", a.Region,
		"--product-descriptions", "Linux/UNIX", "--start-time", time.Now().UTC().Format("2006-01-02T15:04:05"),
		"--instance-types"}
	out, err := a.run(ctx, append(args, types...)...)
	if err != nil {
		return nil, err
	}
	return parseSpot(out)
}

func parseSpot(data []byte) (map[string]float64, error) {
	var resp struct {
		SpotPriceHistory []struct {
			InstanceType       string
			AvailabilityZone   string
			AvailabilityZoneID string `json:"AvailabilityZoneId"`
			SpotPrice          string
		}
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("spot prices: %w", err)
	}
	out := map[string]float64{}
	for _, p := range resp.SpotPriceHistory {
		// Keyed by zone ID: zone names ("ap-south-1a") map to different
		// physical zones in different accounts, zone IDs don't.
		zone := p.AvailabilityZoneID
		if zone == "" {
			zone = p.AvailabilityZone
		}
		key := p.InstanceType + "/" + zone
		if _, seen := out[key]; seen {
			continue // newest first
		}
		if v, err := strconv.ParseFloat(p.SpotPrice, 64); err == nil && v > 0 {
			out[key] = v
		}
	}
	return out, nil
}

// Billed is what an account paid per hour for on-demand Linux instances.
type Billed struct {
	FetchedAt time.Time          `json:"fetched_at"`
	Region    string             `json:"region"`
	Days      int                `json:"days"`
	Rates     map[string]float64 `json:"rates"` // instance type → $/hr
}

// billedDays is how far back the bill is read. Today is left out: Cost
// Explorer fills it in over the day.
const billedDays = 7

// minBilledHours: a day with less usage of a type says little about its rate.
const minBilledHours = 1

// Billed reads the last week's bill from Cost Explorer (one request, which
// AWS charges $0.01 for; Store caches the answer for a day).
func (a AWS) Billed(ctx context.Context, now time.Time) (*Billed, error) {
	end := now.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -billedDays)
	filter := fmt.Sprintf(`{"And":[{"Dimensions":{"Key":"SERVICE","Values":["Amazon Elastic Compute Cloud - Compute"]}},`+
		`{"Dimensions":{"Key":"REGION","Values":[%q]}},{"Dimensions":{"Key":"OPERATION","Values":["RunInstances"]}}]}`, a.Region)
	out, err := a.run(ctx, "ce", "get-cost-and-usage", "--region", costExplorerRegion,
		"--time-period", "Start="+start.Format("2006-01-02")+",End="+end.Format("2006-01-02"),
		"--granularity", "DAILY", "--metrics", "AmortizedCost", "UsageQuantity",
		"--group-by", "Type=DIMENSION,Key=USAGE_TYPE", "--filter", filter)
	if err != nil {
		return nil, err
	}
	rates, err := parseBilled(out)
	if err != nil {
		return nil, err
	}
	return &Billed{FetchedAt: now.UTC(), Region: a.Region, Days: billedDays, Rates: rates}, nil
}

// boxUsage matches on-demand instance hours: "APS3-BoxUsage:t4g.small", or
// "BoxUsage:t3.medium" in us-east-1.
var boxUsage = regexp.MustCompile(`(?:^|-)BoxUsage:([a-z0-9-]+\.[a-z0-9]+)$`)

// parseBilled turns daily cost and hours per usage type into one rate per
// instance type: the median of its daily rates, so a day the bill shows
// oddly (credits, a late Savings Plan charge) doesn't skew it.
func parseBilled(data []byte) (map[string]float64, error) {
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
	daily := map[string][]float64{}
	for _, day := range resp.ResultsByTime {
		for _, g := range day.Groups {
			if len(g.Keys) == 0 {
				continue
			}
			m := boxUsage.FindStringSubmatch(g.Keys[0])
			if m == nil {
				continue
			}
			cost, _ := strconv.ParseFloat(g.Metrics["AmortizedCost"].Amount, 64)
			hours, _ := strconv.ParseFloat(g.Metrics["UsageQuantity"].Amount, 64)
			if hours < minBilledHours {
				continue
			}
			daily[m[1]] = append(daily[m[1]], cost/hours)
		}
	}
	rates := map[string]float64{}
	for t, r := range daily {
		if med := median(r); med > 0 {
			rates[t] = med
		}
	}
	return rates, nil
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// Billed returns the bill of the AWS account that owns the cluster, from a
// cache under a day old or from Cost Explorer. It refuses when the aws CLI
// is logged in to a different account (a role in account B that can reach a
// cluster in account A would otherwise read B's bill).
func (s *Store) Billed(ctx context.Context, clusterAccount string, a AWS) (*Billed, error) {
	if clusterAccount == "" {
		return nil, errors.New("can't tell which AWS account the cluster is in (its kubeconfig name isn't an EKS ARN)")
	}
	login, err := a.Account(ctx)
	if err != nil {
		return nil, err
	}
	if login != clusterAccount {
		return nil, fmt.Errorf("your AWS login is account %s but the cluster is in %s", login, clusterAccount)
	}
	path := filepath.Join(filepath.Dir(s.cachePath), "billed-"+safeName(clusterAccount+"-"+a.Region)+".json")
	if data, err := os.ReadFile(path); err == nil {
		var b Billed
		if json.Unmarshal(data, &b) == nil && b.Region == a.Region && time.Since(b.FetchedAt) < MaxAge {
			return &b, nil
		}
	}
	b, err := a.Billed(ctx, time.Now())
	if err != nil {
		return nil, err
	}
	if len(b.Rates) == 0 {
		return nil, errors.New("no on-demand instances on last week's bill")
	}
	if data, err := json.Marshal(b); err == nil {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		_ = os.WriteFile(path, data, 0o644)
	}
	return b, nil
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func safeName(s string) string { return unsafeChars.ReplaceAllString(s, "_") }
