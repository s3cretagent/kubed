// Package pricing gives EC2 Linux hourly prices (on-demand and average spot)
// without AWS credentials.
//
// Prices come from, in order: a local cache refreshed at most once a day from
// instances.vantage.sh, or the list compiled into the binary.
package pricing

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

//go:generate go run ./gen -out builtin.json.gz

//go:embed builtin.json.gz
var builtin []byte

// Price is an hourly USD price. Zero means unknown.
type Price struct {
	OnDemand float64
	Spot     float64
}

// Table holds prices per region and instance type.
type Table struct {
	FetchedAt time.Time `json:"fetched_at"`
	Source    string    `json:"source"`
	// Regions maps region -> instance type -> [on-demand, spot].
	Regions map[string]map[string][2]float64 `json:"regions"`
}

// Lookup returns the price of an instance type in a region.
func (t *Table) Lookup(region, instanceType string) (Price, bool) {
	if t == nil {
		return Price{}, false
	}
	p, ok := t.Regions[region][instanceType]
	return Price{OnDemand: p[0], Spot: p[1]}, ok
}

// Hourly returns the price for the node's capacity type ("spot" or anything
// else for on-demand). Spot falls back to on-demand when no spot price exists.
func (t *Table) Hourly(region, instanceType, capacityType string) (float64, bool) {
	p, ok := t.Lookup(region, instanceType)
	if !ok {
		return 0, false
	}
	if capacityType == "spot" && p.Spot > 0 {
		return p.Spot, true
	}
	return p.OnDemand, p.OnDemand > 0
}

// Builtin returns the price list compiled into the binary.
func Builtin() (*Table, error) {
	zr, err := gzip.NewReader(bytes.NewReader(builtin))
	if err != nil {
		return nil, fmt.Errorf("read builtin prices: %w", err)
	}
	defer zr.Close()
	return decodeTable(zr)
}

func decodeTable(r io.Reader) (*Table, error) {
	var t Table
	if err := json.NewDecoder(r).Decode(&t); err != nil {
		return nil, fmt.Errorf("decode prices: %w", err)
	}
	return &t, nil
}

// Plausible rejects a refreshed table that looks broken (the feed is run by
// a third party), so a bad download never replaces good prices.
func Plausible(t *Table) error {
	const minRegions = 10
	if len(t.Regions) < minRegions {
		return fmt.Errorf("price feed looks wrong: only %d regions", len(t.Regions))
	}
	// m5.large on-demand in Mumbai has been ~$0.10/hr for years.
	p, ok := t.Lookup("ap-south-1", "m5.large")
	if !ok || p.OnDemand < 0.01 || p.OnDemand > 1 {
		return fmt.Errorf("price feed looks wrong: ap-south-1 m5.large = %v", p.OnDemand)
	}
	return nil
}
