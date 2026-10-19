//go:build smoke

// Smoke test against a real cluster, read-only. It renders a few views
// headlessly and prints them.
//
//	KUBED_SMOKE_CONTEXT=<context> go test -tags smoke -run TestSmoke -v ./internal/ui
package ui

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

func TestSmoke(t *testing.T) {
	ctx := os.Getenv("KUBED_SMOKE_CONTEXT")
	if ctx == "" {
		t.Skip("KUBED_SMOKE_CONTEXT not set")
	}
	t.Cleanup(func() { view.ReadOnly = false }) // package state: later tests run read-write
	m, err := New(Options{Context: ctx, ReadOnly: true, ProdPattern: regexp.MustCompile("prod")})
	if err != nil {
		t.Fatal(err)
	}
	m.Update(tea.WindowSizeMsg{Width: 180, Height: 40})
	m.Update(m.connect(m.context)()) // the full name, as New resolved it
	if m.connErr != nil {
		t.Fatal(m.connErr)
	}

	for _, res := range []string{"pods", "deploy", "nodes", "np", "es", "argo", "secrets", "sa", "events"} {
		if !m.openResource(res, false) {
			t.Logf("%s: not served on this cluster", res)
			continue
		}
		waitSynced(m, 45*time.Second)
		m.refreshRows()
		m.tbl.move(1) // select a row so the detail pane shows
		t.Logf("\n===== :%s =====\n%s", res, m.View())
	}
}

func waitSynced(m *Model, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := m.client.Cache.Synced(m.loc.res.GVR)
		for _, g := range []string{"pods", "nodes", "serviceaccounts", "applications", "externalsecrets", "deployments", "replicasets"} {
			if res, ok := m.client.Resources.Lookup(g); ok {
				ready = ready && m.client.Cache.Synced(res.GVR)
			}
		}
		if ready {
			time.Sleep(300 * time.Millisecond)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Every pod in the cluster must land in exactly one app on the home screen
// (a bug once showed 63 phantom apps and hid pods behind them).
func TestSmokeAppsAccountForEveryPod(t *testing.T) {
	ctx := os.Getenv("KUBED_SMOKE_CONTEXT")
	if ctx == "" {
		t.Skip("KUBED_SMOKE_CONTEXT not set")
	}
	t.Cleanup(func() { view.ReadOnly = false }) // package state: later tests run read-write
	m, err := New(Options{Context: ctx, ReadOnly: true, ProdPattern: regexp.MustCompile("prod")})
	if err != nil {
		t.Fatal(err)
	}
	m.Update(tea.WindowSizeMsg{Width: 160, Height: 40})
	m.Update(m.connect(m.context)()) // the full name, as New resolved it
	if m.connErr != nil {
		t.Fatal(m.connErr)
	}
	// Home loads its lists in stages; wait for every stage it started.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, stage := range view.AppStages {
			for _, g := range stage {
				if served, ok := m.client.Resources.Served(g); ok && !m.client.Cache.Synced(served) && m.client.Cache.Err(served) == nil {
					done = false
				}
			}
		}
		if done {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	m.refreshRows() // the first one fills caches
	start := time.Now()
	m.refreshRows() // home does this every second
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Errorf("refreshing home took %s; it runs every second, so it must stay well under that", took)
	}

	gvr, _ := m.client.Resources.Served(kube.Pods)
	// A live cluster moves on: a pod can start between home's count and
	// this one, so look again a few times before calling it a mismatch.
	var got, want int
	var names map[string]int
	for try := 0; try < 3; try++ {
		if try > 0 {
			time.Sleep(time.Second)
			m.refreshRows()
		}
		all := m.client.Cache.List(gvr, m.namespace) // home shows the current namespace ("" = all)
		want = len(all)
		got, names = 0, map[string]int{}
		matchers := make([]func(*unstructured.Unstructured) bool, 0, len(m.apps))
		for _, a := range m.apps {
			got += a.Pods + a.Leftovers
			names[a.Name+"/"+a.Namespace]++
			matchers = append(matchers, view.AppPods(m.enr, a))
		}
		// Finished pods whose owner is gone (a scan job that deleted
		// itself) belong to no app on purpose; a running one never may.
		for _, p := range all {
			if phase, _, _ := unstructured.NestedString(p.Object, "status", "phase"); phase != "Succeeded" && phase != "Failed" {
				continue
			}
			if !slices.ContainsFunc(matchers, func(f func(*unstructured.Unstructured) bool) bool { return f(p) }) {
				got++
			}
		}
		if got == want {
			break
		}
	}
	if got != want {
		t.Errorf("apps hold %d pods, the cluster has %d", got, want)
	}
	for n, c := range names {
		if c > 1 {
			t.Errorf("app %s listed %d times", n, c)
		}
	}
	// Opening an app must show exactly the pods home counted for it (as
	// above, a busy app can start a pod in between: look again first).
	var off []string
	for try := 0; try < 3; try++ {
		if try > 0 {
			time.Sleep(time.Second)
			m.refreshRows()
		}
		pods := m.client.Cache.List(gvr, m.namespace)
		off = off[:0]
		for _, a := range m.apps {
			match := view.AppPods(m.enr, a)
			n := 0
			for _, p := range pods {
				if match(p) {
					n++
				}
			}
			if n != a.Pods+a.Leftovers {
				off = append(off, fmt.Sprintf("app %s: home counts %d pods, opening it shows %d", a.Name, a.Pods+a.Leftovers, n))
			}
		}
		if len(off) == 0 {
			break
		}
	}
	for _, o := range off {
		t.Error(o)
	}
	t.Logf("%d apps, %d pods in %s, all accounted for", len(m.apps), got, nsLabel(m.namespace))
}
