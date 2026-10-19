package view

import "testing"

func TestUsageFormatting(t *testing.T) {
	cases := []struct{ got, want string }{
		{cpu(250, true), "0.25"},
		{cpu(3000, true), "3"},
		{cpu(1, true), "<0.01"},
		{cpu(100, true), "0.10"}, // two decimals, so a column lines up
		{cpu(250, false), "-"},   // no metrics-server
		{mem(167<<20, true), "167Mi"},
		{mem(3<<29, true), "1.5Gi"},
		{mem(1, false), "-"},
		{pct(50, 200, true), "25"},
		{pct(50, 0, true), "n/a"}, // no request/limit set
		{pct(50, 200, false), "-"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

func TestCapacityFormatting(t *testing.T) {
	if got := cores(7910); got != "7.9 cores" {
		t.Errorf("cores = %q", got)
	}
	if got := gibibytes(14974844 * 1024); got != "14.3Gi" {
		t.Errorf("gibibytes = %q", got)
	}
}
