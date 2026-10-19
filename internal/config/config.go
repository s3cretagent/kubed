// Package config reads kubed's settings and remembers state between runs.
//
//	~/.config/kubed/config.yaml   settings you write (prodPattern, onDemandDiscount)
//	~/.config/kubed/state.json    what kubed remembers (history, namespaces)
//
// $XDG_CONFIG_HOME replaces ~/.config when set.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"
)

// Config is the user's settings file.
type Config struct {
	// ProdPattern is a regexp of contexts where writes need the resource
	// name typed to confirm. The --prod-pattern flag and $KUBED_PROD_PATTERN
	// override it.
	ProdPattern string `json:"prodPattern,omitempty"`
	// NonProdPattern matches contexts / clusters that are not production.
	NonProdPattern string `json:"nonProdPattern,omitempty"`
	// OnDemandDiscount is the percent your account pays below the AWS list
	// price for on-demand instances (Savings Plans, Reserved Instances, an
	// EDP). Spot prices are not discounted. OnDemandDiscounts sets it per
	// cluster (by context or cluster name) and wins over OnDemandDiscount.
	OnDemandDiscount  float64            `json:"onDemandDiscount,omitempty"`
	OnDemandDiscounts map[string]float64 `json:"onDemandDiscounts,omitempty"`
}

// Discount returns the on-demand discount (percent) for a cluster, trying
// each name it goes by (context, cluster name) before the default.
func (c Config) Discount(names ...string) float64 {
	for _, n := range names {
		if d, ok := c.OnDemandDiscounts[n]; ok {
			return d
		}
	}
	return c.OnDemandDiscount
}

// Validate rejects settings that can't be right.
func (c Config) Validate() error {
	bad := func(d float64) bool { return d < 0 || d >= 100 }
	if bad(c.OnDemandDiscount) {
		return fmt.Errorf("onDemandDiscount %v: want a percent from 0 to 99", c.OnDemandDiscount)
	}
	for name, d := range c.OnDemandDiscounts {
		if bad(d) {
			return fmt.Errorf("onDemandDiscounts[%s] %v: want a percent from 0 to 99", name, d)
		}
	}
	return nil
}

// State is remembered between runs.
type State struct {
	History          []string            `json:"history,omitempty"`
	RecentNamespaces map[string][]string `json:"recentNamespaces,omitempty"` // context → most recent first
	// Environments are your answers for clusters kubed couldn't identify,
	// keyed by API server URL: "production" or "non-production".
	Environments map[string]string `json:"environments,omitempty"`
}

// Dir is where both files live.
func Dir() string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "kubed")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "kubed")
	}
	return filepath.Join(home, ".config", "kubed")
}

// LoadConfig reads config.yaml; a missing file is not an error.
func LoadConfig() (Config, error) {
	var c Config
	data, err := os.ReadFile(filepath.Join(Dir(), "config.yaml"))
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// LoadState reads state.json; a missing or unreadable file gives an empty
// state (it is only a convenience).
func LoadState() *State {
	s := &State{RecentNamespaces: map[string][]string{}, Environments: map[string]string{}}
	data, err := os.ReadFile(filepath.Join(Dir(), "state.json"))
	if err != nil {
		return s
	}
	_ = json.Unmarshal(data, s)
	if s.RecentNamespaces == nil {
		s.RecentNamespaces = map[string][]string{}
	}
	if s.Environments == nil {
		s.Environments = map[string]string{}
	}
	return s
}

// Save writes state.json atomically.
func (s *State) Save() error {
	dir := Dir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "state.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "state.json"))
}
