//go:build smoke

package ui

import (
	"os"
	"regexp"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/view"
)

// TestSmokeHomeTiming logs how long each step to a usable home screen takes.
func TestSmokeHomeTiming(t *testing.T) {
	ctx := os.Getenv("KUBED_SMOKE_CONTEXT")
	if ctx == "" {
		t.Skip("KUBED_SMOKE_CONTEXT not set")
	}
	start := time.Now()
	t.Cleanup(func() { view.ReadOnly = false }) // package state: later tests run read-write
	m, err := New(Options{Context: ctx, ReadOnly: true, ProdPattern: regexp.MustCompile("prod")})
	if err != nil {
		t.Fatal(err)
	}
	m.Update(tea.WindowSizeMsg{Width: 180, Height: 40})
	msg := m.connect(m.context)() // the full name, as New resolved it
	t.Logf("connect (auth + discovery): %v", time.Since(start))
	m.Update(msg)
	t.Logf("connected msg handled: %v", time.Since(start))
	if m.connErr != nil || m.client == nil {
		t.Fatalf("not connected: %v", m.connErr)
	}
	done := map[string]bool{}
	var served []schema.GroupVersionResource
	for _, g := range view.AppCore {
		if gvr, ok := m.client.Resources.Served(g); ok { // no ArgoCD: no applications
			served = append(served, gvr)
		}
	}
	for time.Since(start) < 60*time.Second && len(done) < len(served) {
		for _, g := range served {
			if !done[g.Resource] && m.client.Cache.Synced(g) {
				done[g.Resource] = true
				t.Logf("%s synced: %v", g.Resource, time.Since(start))
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
