package view

import (
	"strings"
	"testing"
)

// QA round 159: after u on a stuck stateful set, its failing pod is on
// the version gone back from, newer than the current one: "older" was
// false. It is on another version, whichever way.
func TestWaitsOnWordsSayAnotherVersion(t *testing.T) {
	if got := waitsOnWords("db-0"); strings.Contains(got, "older") || !strings.Contains(got, "another version") {
		t.Errorf("waitsOnWords = %q", got)
	}
}
