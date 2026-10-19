package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// FeedURL is the public instance price feed maintained by Vantage.
const FeedURL = "https://instances.vantage.sh/instances.json"

// feedInstance is the subset of one feed entry kubed reads. The feed entry is
// much larger (reserved pricing, other OSes); unknown fields are skipped.
type feedInstance struct {
	InstanceType string `json:"instance_type"`
	Pricing      map[string]struct {
		Linux struct {
			OnDemand string `json:"ondemand"`
			SpotAvg  string `json:"spot_avg"`
		} `json:"linux"`
	} `json:"pricing"`
}

// Fetch downloads the feed and reduces it to a Table.
func Fetch(ctx context.Context, client *http.Client) (*Table, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, FeedURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch price feed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch price feed: HTTP %d", resp.StatusCode)
	}
	return ParseFeed(resp.Body, time.Now().UTC())
}

// ParseFeed stream-decodes the feed (a JSON array of instances) one entry at a
// time, so the raw ~300 MB document is never held in memory.
func ParseFeed(r io.Reader, fetchedAt time.Time) (*Table, error) {
	dec := json.NewDecoder(r)
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return nil, fmt.Errorf("price feed: expected JSON array")
	}

	t := &Table{FetchedAt: fetchedAt, Source: FeedURL, Regions: map[string]map[string][2]float64{}}
	for dec.More() {
		var inst feedInstance
		if err := dec.Decode(&inst); err != nil {
			return nil, fmt.Errorf("price feed: %w", err)
		}
		for region, p := range inst.Pricing {
			onDemand := parsePrice(p.Linux.OnDemand)
			spot := parsePrice(p.Linux.SpotAvg)
			if onDemand == 0 && spot == 0 {
				continue
			}
			if t.Regions[region] == nil {
				t.Regions[region] = map[string][2]float64{}
			}
			t.Regions[region][inst.InstanceType] = [2]float64{onDemand, spot}
		}
	}
	if len(t.Regions) == 0 {
		return nil, fmt.Errorf("price feed: no prices found")
	}
	return t, nil
}

func parsePrice(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}
