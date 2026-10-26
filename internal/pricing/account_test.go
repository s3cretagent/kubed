package pricing

import (
	"bufio"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseSpotKeysByZoneID(t *testing.T) {
	got, err := parseSpot([]byte(`{"SpotPriceHistory":[
		{"InstanceType":"t3a.medium","AvailabilityZone":"ap-south-1a","AvailabilityZoneId":"aps1-az1","SpotPrice":"0.012300"},
		{"InstanceType":"t3a.medium","AvailabilityZone":"ap-south-1a","AvailabilityZoneId":"aps1-az1","SpotPrice":"0.099"},
		{"InstanceType":"t3a.medium","AvailabilityZone":"ap-south-1b","SpotPrice":"0.0097"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got["t3a.medium/aps1-az1"] != 0.0123 {
		t.Errorf("newest price per zone ID: got %v", got)
	}
	if got["t3a.medium/ap-south-1b"] != 0.0097 {
		t.Errorf("zone name when no ID: got %v", got)
	}
}

func TestParseBilledMedianOfDays(t *testing.T) {
	day := func(groups string) string { return `{"Groups":[` + groups + `]}` }
	g := func(key, cost, hours string) string {
		return `{"Keys":["` + key + `"],"Metrics":{"AmortizedCost":{"Amount":"` + cost + `"},"UsageQuantity":{"Amount":"` + hours + `"}}}`
	}
	data := `{"ResultsByTime":[` +
		day(g("APS3-BoxUsage:t4g.small", "1.944", "240")+","+g("APS3-SpotUsage:r8gd.large", "0.65", "24")) + "," +
		day(g("APS3-BoxUsage:t4g.small", "0", "240")+","+g("BoxUsage:t3.medium", "0.5", "0.5")) + "," +
		day(g("APS3-BoxUsage:t4g.small", "1.944", "240")+","+g("BoxUsage:t3.medium", "0.9984", "24")) + `]}`
	rates, err := parseBilled([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if rates["t4g.small"] != 0.0081 {
		t.Errorf("t4g.small: a $0 day must not drag the rate down; got %v", rates["t4g.small"])
	}
	if rates["t3.medium"] != 0.0416 {
		t.Errorf("t3.medium (us-east-1 key, ignore <1h days): got %v", rates["t3.medium"])
	}
	if _, ok := rates["r8gd.large"]; ok {
		t.Error("spot usage is not on-demand")
	}
}

func TestBilledRefusesAnotherAccountsBill(t *testing.T) {
	calls := []string{}
	runAWS = func(_ context.Context, _ AWS, args ...string) ([]byte, error) {
		calls = append(calls, args[0])
		return []byte(`{"Account":"222222222222"}`), nil
	}
	defer func() {
		runAWS = func(ctx context.Context, a AWS, args ...string) ([]byte, error) { return a.exec(ctx, args...) }
	}()
	s := &Store{cachePath: t.TempDir() + "/prices.json"}
	_, err := s.Billed(context.Background(), "111111111111", AWS{Region: "ap-south-1"})
	if err == nil || !strings.Contains(err.Error(), "222222222222") {
		t.Fatalf("want an account mismatch error, got %v", err)
	}
	if len(calls) != 1 || calls[0] != "sts" {
		t.Errorf("must not ask Cost Explorer after a mismatch; calls %v", calls)
	}
	if _, err := s.Billed(context.Background(), "", AWS{Region: "ap-south-1"}); err == nil {
		t.Error("unknown cluster account must not read a bill")
	}
}

func TestBilledUsesCacheForADay(t *testing.T) {
	n := 0
	runAWS = func(_ context.Context, _ AWS, args ...string) ([]byte, error) {
		if args[0] == "sts" {
			return []byte(`{"Account":"111111111111"}`), nil
		}
		n++
		return []byte(`{"ResultsByTime":[{"Groups":[{"Keys":["APS3-BoxUsage:t4g.small"],"Metrics":{"AmortizedCost":{"Amount":"1.944"},"UsageQuantity":{"Amount":"240"}}}]}]}`), nil
	}
	defer func() {
		runAWS = func(ctx context.Context, a AWS, args ...string) ([]byte, error) { return a.exec(ctx, args...) }
	}()
	s := &Store{cachePath: t.TempDir() + "/prices.json"}
	for range 2 {
		b, err := s.Billed(context.Background(), "111111111111", AWS{Region: "ap-south-1"})
		if err != nil || b.Rates["t4g.small"] != 0.0081 {
			t.Fatalf("got %+v, %v", b, err)
		}
	}
	if n != 1 {
		t.Errorf("Cost Explorer asked %d times; the second start must use the cache", n)
	}
}

func TestProfilesFor(t *testing.T) {
	cfg := `[default]
region = ap-south-1
[profile globex-prod]
sso_start_url = https://x.awsapps.com/start
sso_account_id = 444455556666
sso_role_name = AdministratorAccess
[profile globex-ro]
role_arn = arn:aws:iam::444455556666:role/ReadOnly
source_profile = default
[profile other]
sso_account_id = 111111111111
[sso-session corp]
sso_account_id = 444455556666
`
	got := profilesFor(bufio.NewScanner(strings.NewReader(cfg)), "444455556666")
	if strings.Join(got, ",") != "globex-prod,globex-ro" {
		t.Errorf("got %v", got)
	}
}

func TestClusterBillOnlyKubernetes(t *testing.T) {
	var filter, tagFilter string
	runAWS = func(_ context.Context, _ AWS, args ...string) ([]byte, error) {
		filter = ""
		for i, a := range args {
			if a == "--filter" {
				filter = args[i+1]
			}
		}
		if strings.Contains(filter, "Tags") {
			tagFilter = filter
			return []byte(`{"ResultsByTime":[{"Groups":[
				{"Keys":["Amazon Elastic Compute Cloud - Compute"],"Metrics":{"AmortizedCost":{"Amount":"718.89"}}},
				{"Keys":["EC2 - Other"],"Metrics":{"AmortizedCost":{"Amount":"128.07"}}},
				{"Keys":["Amazon Elastic Container Service for Kubernetes"],"Metrics":{"AmortizedCost":{"Amount":"16.7"}}}]}]}`), nil
		}
		return []byte(`{"ResultsByTime":[{"Groups":[
			{"Keys":["Tax"],"Metrics":{"UnblendedCost":{"Amount":"1800.9"}}},
			{"Keys":["Amazon Relational Database Service"],"Metrics":{"UnblendedCost":{"Amount":"10005.06"}}}]}]}`), nil
	}
	defer func() {
		runAWS = func(ctx context.Context, a AWS, args ...string) ([]byte, error) { return a.exec(ctx, args...) }
	}()

	tags := ClusterTags("p-1234-aps1-01-k8s-cluster", map[string]string{"Cluster": "p-1234-aps1-01", "Team": "core", "clusterid": "62e8"})
	if !slices.Contains(tags["Cluster"], "p-1234-aps1-01") || tags["Team"] != nil || tags["clusterid"] != nil {
		t.Errorf("only tags naming the cluster count: %v", tags)
	}
	b, err := AWS{Region: "ap-south-1"}.ClusterBill(context.Background(), "p-1234-aps1-01-k8s-cluster", tags, time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tagFilter, `"Amazon Elastic Container Service for Kubernetes"`) || strings.Contains(tagFilter, "Relational") {
		t.Errorf("the query must ask only for Kubernetes services: %s", tagFilter)
	}
	total, parts := b.Kubernetes()
	if d := total*7 - 863.66; d > 0.01 || d < -0.01 {
		t.Errorf("7-day total %v, want 863.66", total*7)
	}
	if parts[0].What != "nodes" {
		t.Errorf("biggest part first: %v", parts)
	}
	if b.TaxRate < 0.1799 || b.TaxRate > 0.1801 {
		t.Errorf("tax rate %v, want 18%%", b.TaxRate)
	}
}

func TestLoginErrorWords(t *testing.T) {
	err := loginError("globex-prod", errors.New("aws sts get-caller-identity: The SSO session associated with this profile has expired or is otherwise invalid"))
	if err.Error() != "your AWS login for profile globex-prod has expired. Run aws sso login --profile globex-prod" {
		t.Errorf("got %q", err)
	}
}
