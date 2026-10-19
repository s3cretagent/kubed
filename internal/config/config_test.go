package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateRoundTripAndConfig(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	s := LoadState()
	s.History = []string{"pods", "nodes"}
	s.RecentNamespaces["ctx"] = []string{"auth", "payment"}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	got := LoadState()
	if len(got.History) != 2 || got.RecentNamespaces["ctx"][1] != "payment" {
		t.Fatalf("state = %+v", got)
	}

	if c, err := LoadConfig(); err != nil || c.ProdPattern != "" {
		t.Fatalf("missing config: %+v %v", c, err)
	}
	if err := os.WriteFile(filepath.Join(Dir(), "config.yaml"), []byte("prodPattern: 'prod|-p-'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if c, err := LoadConfig(); err != nil || c.ProdPattern != "prod|-p-" {
		t.Fatalf("config = %+v %v", c, err)
	}
}
