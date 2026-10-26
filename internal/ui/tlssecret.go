package ui

import (
	"context"
	"maps"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// The ingress list says when a TLS secret an ingress names isn't there.
// Loading every Secret for that (values and all) is heavy, so the names
// on the open list are read on their own, metadata only, once every
// tlsSecretTTL.
const tlsSecretTTL = time.Minute

// tlsSecretsAtOnce bounds the reads of one tick.
const tlsSecretsAtOnce = 50

type tlsSecret struct {
	exists, known bool
	at            time.Time // zero: being read
}

type tlsSecretMsg struct {
	context string
	read    map[string]tlsSecret // by ns/name
}

// tlsSecretLookup is view.TLSSecret over what has been read.
func (m *Model) tlsSecretLookup(ns, name string) (exists, known bool) {
	s, ok := m.tlsSecrets[ns+"/"+name]
	if !ok || s.at.IsZero() {
		return false, false
	}
	return s.exists, s.known
}

// tlsSecretCmds reads the TLS secrets the open ingress list names that
// aren't fresh, when the namespace's Secrets aren't loaded anyway.
func (m *Model) tlsSecretCmds() tea.Cmd {
	if m.loc.kind != locResource || m.loc.res.GVR.GroupResource() != kube.Ingresses.GroupResource() || m.client == nil || m.client.Metadata == nil {
		return nil
	}
	view.TLSSecret = m.tlsSecretLookup
	if m.tlsSecrets == nil {
		m.tlsSecrets = map[string]tlsSecret{}
	}
	var keys [][2]string
	for _, r := range m.tbl.rows {
		if r.Obj == nil || m.enr != nil && m.enr.SyncedIn(kube.Secrets, r.Obj.GetNamespace()) {
			continue
		}
		for _, name := range view.TLSSecretNames(r.Obj) {
			key := r.Obj.GetNamespace() + "/" + name
			if s, seen := m.tlsSecrets[key]; seen && (s.at.IsZero() || time.Since(s.at) < tlsSecretTTL) {
				continue // being read, or fresh
			}
			if len(keys) < tlsSecretsAtOnce {
				m.tlsSecrets[key] = tlsSecret{} // in flight
				keys = append(keys, [2]string{r.Obj.GetNamespace(), name})
			}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	meta, ctx := m.client.Metadata, m.context
	return func() tea.Msg {
		read := map[string]tlsSecret{}
		for _, k := range keys {
			cctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
			// Metadata only: kubed never needs the values to know it exists.
			_, err := meta.Resource(kube.Secrets).Namespace(k[0]).Get(cctx, k[1], metav1.GetOptions{})
			cancel()
			s := tlsSecret{at: time.Now()}
			switch {
			case err == nil:
				s.exists, s.known = true, true
			case apierrors.IsNotFound(err):
				s.known = true
			} // not allowed, or no answer: unknown, so nothing is said
			read[k[0]+"/"+k[1]] = s
		}
		return tlsSecretMsg{context: ctx, read: read}
	}
}

func (m *Model) onTLSSecret(msg tlsSecretMsg) {
	if msg.context != m.context || m.tlsSecrets == nil {
		return
	}
	maps.Copy(m.tlsSecrets, msg.read)
}
