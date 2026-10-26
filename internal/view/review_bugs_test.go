package view

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
)

// Review: a ConfigMap with only binaryData counted its keys in KEYS but
// its detail said "Keys none: it is empty". Its binary keys are listed,
// marked binary with their size, never their bytes.
func TestConfigMapDetailListsBinaryKeys(t *testing.T) {
	cm := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
		"metadata":   map[string]any{"name": "assets", "namespace": "shop"},
		"binaryData": map[string]any{"logo.png": "iVBORw0KGgo=", "big.bin": strings.Repeat("AAAA", 384)}}}
	v := For(kube.Resource{GVR: kube.ConfigMaps, Namespaced: true})
	e := enrich.New(mxStore{listStore{}}, nil, nil)
	if cells, _ := v.Row(e, cm); cells[1] != "2" {
		t.Errorf("KEYS %q", cells[1])
	}
	lines := v.Detail(e, cm)
	if !detailHas(lines, "Keys     big.bin (binary, 1.1 KB), logo.png (binary, 8 bytes)  (x shows them)") || detailHas(lines, "empty") || detailHas(lines, "iVBOR") {
		t.Errorf("detail: %q", lines)
	}
	cm.Object["data"] = map[string]any{"mode": "dark"}
	if lines := v.Detail(e, cm); !detailHas(lines, "Keys     mode, big.bin (binary, 1.1 KB), logo.png (binary, 8 bytes)") {
		t.Errorf("detail with data: %q", lines)
	}
}
