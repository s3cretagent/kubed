package ui

import (
	"context"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/s3cretagent/kubed/internal/view"
)

// Web addresses on a private network (or not in DNS yet) look like public
// ones. kubed looks each host up once, in the background, and marks the
// ones that don't resolve from here.
var dnsSeen sync.Map // host → dnsState

type dnsState int

const (
	dnsPending dnsState = iota
	dnsOK
	dnsNone
)

// resolves says whether a web address's host resolves from this machine;
// known is false until the lookup (started on the first call) is done.
func resolves(addr string) (ok, known bool) {
	u, err := url.Parse(addr)
	if err != nil || u.Hostname() == "" {
		return true, true
	}
	host := u.Hostname()
	if v, loaded := dnsSeen.LoadOrStore(host, dnsPending); loaded {
		s := v.(dnsState)
		return s == dnsOK, s != dnsPending
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		state := dnsOK
		if _, err := net.DefaultResolver.LookupHost(ctx, host); err != nil {
			state = dnsNone
		}
		dnsSeen.Store(host, state)
	}()
	return true, false
}

// markPrivate adds a note to web addresses that don't resolve from here.
func markPrivate(addrs []string) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a
		if ok, known := resolves(a); known && !ok {
			out[i] = a + " (not found from here: private network or VPN only?)"
		}
	}
	return out
}

// appWebNoted is appWeb for the boxes: addresses that don't resolve from
// here say so.
func (m *Model) appWebNoted(a *view.App) []string { return markPrivate(m.appWeb(a)) }
