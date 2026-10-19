package ui

import (
	"context"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// The error of a failing ExternalSecret isn't on the object (its condition
// only says "could not get secret data from provider"); it is in its
// events. Home doesn't load events (they are big), so the selected app's
// are read on their own, once every secretErrTTL.
const secretErrTTL = 5 * time.Minute

type secretErr struct {
	check view.SecretCheck
	at    time.Time
}

type secretErrMsg struct {
	context, key string
	check        view.SecretCheck
}

// secretError is a view.SecretLookup over what has been fetched.
func (m *Model) secretError(ns, name string) (view.SecretCheck, bool) {
	e, ok := m.secretErrs[ns+"/"+name]
	if !ok || e.at.IsZero() {
		return view.SecretCheck{}, false
	}
	return e.check, true
}

// secretErrCmds fetches the error of the selected app's failing secret.
func (m *Model) secretErrCmds() tea.Cmd {
	if m.loc.kind != locHome || m.client == nil {
		return nil
	}
	row, ok := m.tbl.selected()
	if !ok {
		return nil
	}
	a, ok := m.appFor(row)
	if !ok || len(a.Secrets) == 0 {
		return nil
	}
	f := a.Secrets[0]
	key := f.Namespace + "/" + f.Name
	if m.secretErrs == nil {
		m.secretErrs = map[string]secretErr{}
	}
	if e, seen := m.secretErrs[key]; seen && (e.at.IsZero() || time.Since(e.at) < secretErrTTL) {
		return nil // fetching, or fresh
	}
	m.secretErrs[key] = secretErr{} // in flight
	typed, meta, ctx := m.client.Typed, m.client.Metadata, m.context
	return func() tea.Msg {
		cctx, cancel := context.WithTimeout(context.Background(), apiTimeout)
		defer cancel()
		var check view.SecretCheck
		// Metadata only: kubed never needs the values to know it exists.
		_, err := meta.Resource(kube.Secrets).Namespace(f.Namespace).Get(cctx, f.Target, metav1.GetOptions{})
		check.TargetExists = err == nil || !apierrors.IsNotFound(err)
		list, err := typed.CoreV1().Events(f.Namespace).List(cctx, metav1.ListOptions{
			FieldSelector: "involvedObject.kind=ExternalSecret,involvedObject.name=" + f.Name,
		})
		if err != nil {
			check.Error = "can't read its events: " + err.Error()
			return secretErrMsg{context: ctx, key: key, check: check}
		}
		evs := list.Items
		sort.Slice(evs, func(i, j int) bool { return evs[i].LastTimestamp.After(evs[j].LastTimestamp.Time) })
		for _, ev := range evs {
			if ev.Type == "Warning" {
				check.Error = shortESError(ev.Message)
				break
			}
		}
		return secretErrMsg{context: ctx, key: key, check: check}
	}
}

func (m *Model) onSecretErr(msg secretErrMsg) {
	if msg.context == m.context {
		m.secretErrs[msg.key] = secretErr{check: msg.check, at: time.Now()}
	}
}

// shortESError keeps the cause of an ESO error: "error processing
// spec.data[0] (key: /a/b), err: key x does not exist in secret /a/b" →
// "key x does not exist in secret /a/b".
func shortESError(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if i := strings.LastIndex(s, "err: "); i >= 0 {
		return s[i+len("err: "):]
	}
	return s
}
