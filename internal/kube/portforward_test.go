package kube

import (
	"errors"
	"testing"

	"github.com/go-logr/logr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Round 85: client-go says why a connection failed only to the runtime's
// error handlers; a refused one (nothing listening) must reach the forward.
func TestRefusedConnectionReachesTheForward(t *testing.T) {
	f := &Forward{LocalPort: 18530, PodPort: 80}
	newer := &Forward{LocalPort: 18530, PodPort: 80} // started on the same port after f stopped
	err := errors.New("error forwarding port 80 to pod web-1, uid : failed to execute portforward in network namespace: " +
		"failed to connect to localhost:80 inside namespace, IPv4: dial tcp4 127.0.0.1:80: connect: connection refused IPv6 dial tcp6: address localhost: no suitable address ")
	utilruntime.HandleErrorWithLogger(logr.New(forwardSink{f}), err, "An error occurred forwarding", "localPort", uint16(18530), "remotePort", uint16(80))
	if got := f.Failing(); got != "connection refused" {
		t.Errorf("Failing() = %q", got)
	}
	if got := newer.Failing(); got != "" {
		t.Errorf("a newer forward on the port got %q", got)
	}
}

// Review: the IPv4 half of the kubelet's message says why, not the IPv6 one.
func TestForwardErrorKeepsIPv4Half(t *testing.T) {
	f := &Forward{}
	_, _ = f.Write([]byte("error forwarding port 80 to pod x, uid : failed to connect to localhost:80 inside namespace, " +
		"IPv4: dial tcp4 127.0.0.1:80: connect: no route to host IPv6 dial tcp6 [::1]:80: connect: cannot assign requested address "))
	if got := f.Failing(); got != "no route to host" {
		t.Errorf("Failing() = %q", got)
	}
	_, _ = f.Write([]byte("socat: Connection refused"))
	if got := f.Failing(); got != "connection refused" {
		t.Errorf("Failing() = %q", got)
	}
}

// Review of round 87: a forward asked for on a cluster you have left (a
// reconnect still on its way when you switched) isn't started.
func TestNoForwardOnALeftCluster(t *testing.T) {
	c := &Client{Cache: NewCache(nil)}
	c.Close()
	if _, err := (&Forwards{}).Start(c, "ns", "db-0", 18986, 80); !errors.Is(err, ErrLeftCluster) {
		t.Errorf("err = %v", err)
	}
	if _, err := (&Forwards{}).Start(nil, "ns", "db-0", 18986, 80); !errors.Is(err, ErrLeftCluster) {
		t.Errorf("nil client: err = %v", err)
	}
}

// Review of round 87: a forward lost just before you switched cluster
// says so (OnClosed), so the new cluster's screens don't report it.
func TestForwardOnClosedClient(t *testing.T) {
	c := &Client{Cache: NewCache(nil)}
	f := &Forward{client: c}
	if f.OnClosed() {
		t.Error("open client counted as closed")
	}
	c.Close()
	if !f.OnClosed() {
		t.Error("closed client not noticed")
	}
}
