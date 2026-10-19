package kube

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// MetricsInterval is how often usage is fetched. metrics-server scrapes
// every 15s by default, so polling faster only redraws the same numbers.
const MetricsInterval = 10 * time.Second

// Usage is a CPU / memory reading.
type Usage struct {
	CPUMilli int64
	MemBytes int64
}

// Metrics polls metrics.k8s.io for pod and node usage. The API has no
// watch, so it is listed on a timer.
type Metrics struct {
	client metricsclient.Interface

	mu        sync.RWMutex
	pods      map[string]Usage // "ns/name" → sum of containers
	nodes     map[string]Usage
	available bool
	polled    bool // the first poll has finished, answered or not

	peaks map[string]Usage // highest pod usage seen since kubed started
	since time.Time        // first successful poll
}

func newMetrics(cfg *rest.Config) (*Metrics, error) {
	c, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Metrics{client: c}, nil
}

// run polls until stop closes.
func (m *Metrics) run(stop <-chan struct{}) {
	t := time.NewTicker(MetricsInterval)
	defer t.Stop()
	for {
		m.poll()
		select {
		case <-stop:
			return
		case <-t.C:
		}
	}
}

func (m *Metrics) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), MetricsInterval)
	defer cancel()
	api := m.client.MetricsV1beta1()

	nodes, nodeErr := api.NodeMetricses().List(ctx, metav1.ListOptions{})
	pods, podErr := api.PodMetricses("").List(ctx, metav1.ListOptions{})

	nodeUse := map[string]Usage{}
	if nodeErr == nil {
		for _, n := range nodes.Items {
			nodeUse[n.Name] = Usage{CPUMilli: n.Usage.Cpu().MilliValue(), MemBytes: n.Usage.Memory().Value()}
		}
	}
	podUse := map[string]Usage{}
	if podErr == nil {
		for _, p := range pods.Items {
			var u Usage
			for _, c := range p.Containers {
				u.CPUMilli += c.Usage.Cpu().MilliValue()
				u.MemBytes += c.Usage.Memory().Value()
			}
			podUse[p.Namespace+"/"+p.Name] = u
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.available, m.polled = nodeErr == nil || podErr == nil, true
	if m.available {
		m.nodes, m.pods = nodeUse, podUse
		if m.peaks == nil {
			m.peaks, m.since = map[string]Usage{}, time.Now()
		}
		for k, u := range podUse {
			p := m.peaks[k]
			m.peaks[k] = Usage{CPUMilli: max(p.CPUMilli, u.CPUMilli), MemBytes: max(p.MemBytes, u.MemBytes)}
		}
	}
}

// Available reports whether metrics-server answered. Before the first poll
// finishes it reports false.
func (m *Metrics) Available() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.available
}

// Polled reports whether the first poll has finished, so a missing reading
// means metrics-server has none rather than that kubed hasn't asked yet.
func (m *Metrics) Polled() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.polled
}

// Pod returns the summed container usage of a pod.
func (m *Metrics) Pod(ns, name string) (Usage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.pods[ns+"/"+name]
	return u, ok
}

// Node returns a node's usage.
func (m *Metrics) Node(name string) (Usage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.nodes[name]
	return u, ok
}

// Peak is the highest usage seen for a pod since kubed started, and since
// when kubed has been watching.
func (m *Metrics) Peak(ns, name string) (Usage, time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.peaks[ns+"/"+name]
	return u, m.since, ok
}
