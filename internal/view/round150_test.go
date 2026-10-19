package view

import "testing"

// Round 150: cores rounds half up from milli-cores, so 850m reads 0.9
// cores the same way 3850m reads 3.9 (a float 0.85 rounds down).
func TestCoresRoundHalfUp(t *testing.T) {
	for milli, want := range map[int64]string{
		850:  "0.9 cores",
		1850: "1.9 cores",
		3850: "3.9 cores",
		7910: "7.9 cores",
		849:  "0.8 cores",
		0:    "0.0 cores",
		1000: "1.0 cores",
	} {
		if got := cores(milli); got != want {
			t.Errorf("cores(%d) = %q, want %q", milli, got, want)
		}
	}
}
