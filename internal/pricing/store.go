package pricing

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// MaxAge is how old the cache may be before a background refresh runs.
const MaxAge = 24 * time.Hour

// Store picks the best available price table and refreshes the cache.
type Store struct {
	cachePath string
	client    *http.Client
}

// NewStore uses ~/.cache/kubed/prices.json (or the OS equivalent).
func NewStore() *Store {
	dir, err := os.UserCacheDir()
	if err != nil {
		dir = os.TempDir()
	}
	return &Store{
		cachePath: filepath.Join(dir, "kubed", "prices.json"),
		client:    &http.Client{Timeout: 5 * time.Minute},
	}
}

// Load returns the cached table when present, otherwise the builtin one.
// needsRefresh reports whether the returned table is older than MaxAge.
func (s *Store) Load() (t *Table, needsRefresh bool, err error) {
	if cached, err := s.readCache(); err == nil {
		return cached, time.Since(cached.FetchedAt) > MaxAge, nil
	}
	t, err = Builtin()
	return t, true, err
}

// Refresh downloads the feed and writes it to the cache.
func (s *Store) Refresh(ctx context.Context) (*Table, error) {
	t, err := Fetch(ctx, s.client)
	if err != nil {
		return nil, err
	}
	if err := Plausible(t); err != nil {
		return nil, err
	}
	return t, s.writeCache(t)
}

func (s *Store) readCache() (*Table, error) {
	f, err := os.Open(s.cachePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return decodeTable(f)
}

func (s *Store) writeCache(t *Table) error {
	if err := os.MkdirAll(filepath.Dir(s.cachePath), 0o755); err != nil {
		return err
	}
	tmp := s.cachePath + ".tmp"
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.cachePath)
}
