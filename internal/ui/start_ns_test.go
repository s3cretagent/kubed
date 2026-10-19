package ui

import (
	"os"
	"path/filepath"
	"testing"
)

// Round 90: kubed -n all opened a namespace called "all" ("Nothing here in
// all"); it means every namespace, as :ns all does.
func TestStartNamespaceAll(t *testing.T) {
	kc := filepath.Join(t.TempDir(), "config")
	cfg := "apiVersion: v1\nkind: Config\ncurrent-context: x\nclusters:\n- name: c\n  cluster: {server: \"https://127.0.0.1:1\"}\ncontexts:\n- name: x\n  context: {cluster: c, user: u, namespace: home}\nusers:\n- name: u\n  user: {}\n"
	if err := os.WriteFile(kc, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"all", "-A"} {
		m, err := New(Options{Namespace: flag, Kubeconfig: kc})
		if err != nil {
			t.Fatal(err)
		}
		if m.namespace != "" {
			t.Errorf("-n %s: namespace %q", flag, m.namespace)
		}
	}
	if m, err := New(Options{Kubeconfig: kc}); err != nil || m.namespace != "home" {
		t.Errorf("default: %v %q", err, m.namespace)
	}
}
