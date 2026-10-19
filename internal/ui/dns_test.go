package ui

import (
	"testing"
	"time"
)

// A host that can't resolve is marked once the lookup is done; until then
// the address shows as it is.
func TestMarkPrivate(t *testing.T) {
	addr := "https://kubed-test.invalid"
	if got := markPrivate([]string{addr}); got[0] != addr {
		t.Fatalf("before the lookup: %q", got[0])
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, known := resolves(addr); known {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := markPrivate([]string{addr}); got[0] == addr {
		t.Errorf("an address that doesn't resolve should say so: %q", got[0])
	}
}
