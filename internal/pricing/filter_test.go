package pricing

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// A tag key is quoted as JSON in the Cost Explorer filter: Go's %q quoting
// differs from JSON's, so a key with unusual characters broke the filter.
func TestClusterBillFilterIsValidJSON(t *testing.T) {
	var filter string
	runAWS = func(_ context.Context, _ AWS, args ...string) ([]byte, error) {
		for i, a := range args {
			if a == "--filter" && i+1 < len(args) {
				filter = args[i+1]
			}
		}
		return []byte(`{"ResultsByTime":[]}`), nil
	}
	defer func() {
		runAWS = func(ctx context.Context, a AWS, args ...string) ([]byte, error) { return a.exec(ctx, args...) }
	}()
	key := "team\"name\x7fé"
	_, _ = AWS{}.ClusterBill(context.Background(), "c", map[string][]string{key: {"v"}}, time.Now())
	var parsed struct {
		And []struct {
			Tags struct{ Key string }
		}
	}
	if err := json.Unmarshal([]byte(filter), &parsed); err != nil {
		t.Fatalf("filter isn't valid JSON: %v\n%s", err, filter)
	}
	if len(parsed.And) == 0 || parsed.And[0].Tags.Key != key {
		t.Errorf("tag key came out as %+v, want %q", parsed.And, key)
	}
}
