package ui

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// QA round 146: the logs of a pod deleted while it ignores SIGTERM ended,
// once its grace ran out and it was killed, "end of the run: it exited
// with an error (exit 137)": at the stream's end the pod still exists,
// being deleted. A pod being deleted counts as deleted.
func TestLogsOfPodBeingDeletedSayDeleted(t *testing.T) {
	for _, deleting := range []bool{true, false} {
		m := testModel()
		defer m.client.Cache.Stop()
		m.width, m.height = 200, 40
		pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": "victim", "namespace": "shop"},
			"status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "app",
				"state": map[string]any{"terminated": map[string]any{"exitCode": int64(137), "reason": "Error",
					"finishedAt": time.Now().UTC().Format(time.RFC3339)}}}}}}}
		if deleting {
			pod.Object["metadata"].(map[string]any)["deletionTimestamp"] = time.Now().Add(-time.Second).UTC().Format(time.RFC3339)
		}
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{kube.Pods: "PodList"})
		dyn.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) { return true, pod, nil })
		m.client.Dynamic = dyn
		p := newPager("logs of pod victim · app container · in shop", "", 100, 20)
		p.isLogs, p.lines, p.follow = true, nil, true
		p.source = logSource{ns: "shop", pod: "victim", container: "app"}
		p.logs = make(chan string)
		p.append([]string{"ignoring SIGTERM"})
		m.pager, m.mode = p, modePager
		_, cmd := m.Update(logLinesMsg{ch: p.logs, done: true})
		if cmd == nil {
			t.Fatal("no check at the stream's end")
		}
		m.Update(cmd())
		got := ansi.Strip(strings.Join(p.lines, "\n"))
		said := strings.HasSuffix(got, "── the pod was deleted; these are its last lines")
		if said != deleting || deleting && strings.Contains(got, "end of the run") {
			t.Errorf("deleting %v: end of its logs:\n%s", deleting, got)
		}
	}
}

// Review of round 146: the ingress list loaded every Secret to tell that
// a TLS secret isn't there. It reads only the names its ingresses use,
// metadata only, off the screen's loop; one it may not read stays unknown.
func TestIngressTLSSecretsReadByName(t *testing.T) {
	m := testModel()
	defer m.client.Cache.Stop()
	defer func(f func(ns, name string) (bool, bool)) { view.TLSSecret = f }(view.TLSSecret)
	meta := metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme())
	var asked []string
	meta.PrependReactor("get", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		name := a.(k8stesting.GetAction).GetName()
		asked = append(asked, a.GetNamespace()+"/"+name)
		switch name {
		case "kept-tls":
			return true, &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: a.GetNamespace()}}, nil
		case "hidden-tls":
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name, errors.New("no"))
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	})
	m.client.Metadata = meta
	ing := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
		"metadata": map[string]any{"name": "web", "namespace": "shop"},
		"spec": map[string]any{"ingressClassName": "alb", "tls": []any{map[string]any{"secretName": "gone-tls"},
			map[string]any{"secretName": "kept-tls"}, map[string]any{"secretName": "hidden-tls"}}}}}
	m.loc = location{kind: locResource, res: kube.Resource{GVR: kube.Ingresses, Kind: "Ingress", Namespaced: true}}
	m.tbl.setRows([]string{"NAME"}, []view.Row{{Cells: []string{"web"}, Obj: ing}}, nil)
	cmd := m.tlsSecretCmds()
	if cmd == nil {
		t.Fatal("no read of the TLS secrets")
	}
	m.Update(cmd())
	if m.tlsSecretCmds() != nil {
		t.Error("read again within the minute")
	}
	box := strings.Join(view.For(m.loc.res).Detail(m.enr, ing), "\n")
	if !strings.Contains(box, "TLS secret gone-tls isn't there") || strings.Contains(box, "kept-tls") || strings.Contains(box, "hidden-tls") || len(asked) != 3 {
		t.Errorf("box %s; asked %q", box, asked)
	}
}
