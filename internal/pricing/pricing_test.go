package pricing

import (
	"strings"
	"testing"
	"time"
)

const feed = `[
  {"instance_type": "m7i.large", "pricing": {
    "ap-south-1": {"linux": {"ondemand": "0.10605", "spot_avg": "0.051867", "reserved": {"x": "1"}}, "mswin": {"ondemand": "0.2"}},
    "us-east-1":  {"linux": {"ondemand": "0.1008"}}
  }},
  {"instance_type": "mac1.metal", "pricing": {"ap-south-1": {"mswin": {"ondemand": "9"}}}}
]`

func TestParseFeed(t *testing.T) {
	tbl, err := ParseFeed(strings.NewReader(feed), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := tbl.Lookup("ap-south-1", "m7i.large")
	if !ok || p.OnDemand != 0.10605 || p.Spot != 0.051867 {
		t.Fatalf("ap-south-1 m7i.large = %+v, %v", p, ok)
	}
	if _, ok := tbl.Lookup("ap-south-1", "mac1.metal"); ok {
		t.Error("instance with no linux price should be skipped")
	}
}

func TestHourly(t *testing.T) {
	tbl, _ := ParseFeed(strings.NewReader(feed), time.Unix(0, 0))
	cases := []struct {
		region, capacity string
		want             float64
		ok               bool
	}{
		{"ap-south-1", "spot", 0.051867, true},
		{"ap-south-1", "on-demand", 0.10605, true},
		{"us-east-1", "spot", 0.1008, true}, // no spot price: fall back to on-demand
		{"eu-west-1", "spot", 0, false},
	}
	for _, c := range cases {
		got, ok := tbl.Hourly(c.region, "m7i.large", c.capacity)
		if got != c.want || ok != c.ok {
			t.Errorf("Hourly(%s, %s) = %v, %v; want %v, %v", c.region, c.capacity, got, ok, c.want, c.ok)
		}
	}
	var nilTable *Table
	if _, ok := nilTable.Hourly("ap-south-1", "m7i.large", "spot"); ok {
		t.Error("nil table must report unknown")
	}
}

func TestParseFeedRejectsNonArray(t *testing.T) {
	if _, err := ParseFeed(strings.NewReader(`{"a":1}`), time.Now()); err == nil {
		t.Fatal("expected error")
	}
}

func TestBuiltin(t *testing.T) {
	tbl, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Lookup("ap-south-1", "m7i.large"); !ok {
		t.Error("builtin list should cover ap-south-1 m7i.large")
	}
}

func TestPlausible(t *testing.T) {
	if err := Plausible(&Table{Regions: map[string]map[string][2]float64{"ap-south-1": {"m5.large": {0.1, 0.04}}}}); err == nil {
		t.Error("one region should be rejected")
	}
	good, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	if err := Plausible(good); err != nil {
		t.Errorf("builtin list rejected: %v", err)
	}
	good.Regions["ap-south-1"]["m5.large"] = [2]float64{999, 0}
	if err := Plausible(good); err == nil {
		t.Error("absurd price accepted")
	}
}
