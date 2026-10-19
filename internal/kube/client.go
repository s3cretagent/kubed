// Package kube talks to the Kubernetes API: kubeconfig loading, a lazily
// started informer cache, resource discovery and the actions kubed offers.
// It knows nothing about the UI.
package kube

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/disk"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client is a connection to one kubeconfig context.
type Client struct {
	closed    atomic.Bool // Close ran: you left this cluster
	Context   string
	Cluster   string // kubeconfig cluster name
	Server    string // API server URL: the same for everyone, so answers are remembered by it
	Version   string // API server version, e.g. v1.35.7-eks-cb19647
	Config    *rest.Config
	Dynamic   dynamic.Interface
	Metadata  metadata.Interface
	Typed     kubernetes.Interface
	Discovery discovery.DiscoveryInterface
	Cache     *Cache
	Resources *Resources
	Metrics   *Metrics

	stopMetrics chan struct{}
	discovery   discovery.DiscoveryInterface // cached; see Rediscover
}

// discoveryTTL is how long cached discovery is trusted. A resource typed
// that isn't in it asks the server again (Rediscover).
const discoveryTTL = 6 * time.Hour

// cachedDiscovery wraps discovery in a disk cache under the user cache
// dir, one folder per API server; on any problem it uses live discovery.
func cachedDiscovery(cfg *rest.Config, live discovery.DiscoveryInterface) discovery.DiscoveryInterface {
	dir, err := os.UserCacheDir()
	if err != nil {
		return live
	}
	base := filepath.Join(dir, "kubed", "discovery", hostDir.ReplaceAllString(cfg.Host, "_"))
	cached, err := disk.NewCachedDiscoveryClientForConfig(rest.CopyConfig(cfg), base, filepath.Join(base, "http"), discoveryTTL)
	if err != nil {
		return live
	}
	return cached
}

var hostDir = regexp.MustCompile(`[^A-Za-z0-9.-]+`)

// Rediscover drops cached discovery and asks the server again, for a
// resource (a new CRD) the cache doesn't know yet. It reports whether the
// list changed.
func (c *Client) Rediscover() bool {
	d, ok := c.discovery.(*disk.CachedDiscoveryClient)
	if !ok {
		return false
	}
	d.Invalidate()
	res, err := Discover(d)
	if err != nil || len(res.all) == len(c.Resources.all) {
		return false
	}
	*c.Resources = *res
	return true
}

// Kubeconfig wraps the merged kubeconfig ($KUBECONFIG or ~/.kube/config).
type Kubeconfig struct {
	loader clientcmd.ClientConfig
}

// LoadKubeconfig reads kubeconfig the same way kubectl does. path overrides
// the default lookup when non-empty.
func LoadKubeconfig(path string) *Kubeconfig {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	return &Kubeconfig{
		loader: clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}),
	}
}

// Contexts lists context names, sorted.
func (k *Kubeconfig) Contexts() ([]string, error) {
	raw, err := k.loader.RawConfig()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(raw.Contexts))
	for name := range raw.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// CurrentContext returns the kubeconfig's current-context.
func (k *Kubeconfig) CurrentContext() (string, error) {
	raw, err := k.loader.RawConfig()
	if err != nil {
		return "", err
	}
	return raw.CurrentContext, nil
}

// DefaultNamespace returns the namespace set on a context, or "".
func (k *Kubeconfig) DefaultNamespace(context string) string {
	raw, err := k.loader.RawConfig()
	if err != nil {
		return ""
	}
	if c, ok := raw.Contexts[context]; ok {
		return c.Namespace
	}
	return ""
}

// Every client kubed makes for a cluster (lists, watches, changes,
// metrics) shares one HTTP/2 connection to it. One that goes quiet
// without closing (a network or load balancer that dropped it) gets no
// error: watches see nothing new and a change sent on it waits.
// client-go notices only after 30s without a frame and then 15s waiting
// for an answer to a ping, so up to 45s; after that every watch and
// request starts again at once on a new connection. kubed checks sooner
// (up to 25s), but not so soon that a slow network drops a live one.
var (
	connQuiet    = 15 * time.Second // with nothing received for this long, send a ping
	connPingWait = 10 * time.Second // no answer in this long: the connection is dropped (a slow VPN or a busy API server can take seconds to answer)
)

// checkConnectionsSooner sets client-go's HTTP/2 health check to connQuiet
// and connPingWait. client-go reads them from the environment when it
// builds a connection, so this runs before any client is made. A value you
// set yourself is kept.
func checkConnectionsSooner() {
	for name, d := range map[string]time.Duration{
		"HTTP2_READ_IDLE_TIMEOUT_SECONDS": connQuiet,
		"HTTP2_PING_TIMEOUT_SECONDS":      connPingWait,
	} {
		if _, set := os.LookupEnv(name); !set {
			_ = os.Setenv(name, strconv.Itoa(max(int(d/time.Second), 1)))
		}
	}
}

// Connect builds clients for a context and runs discovery.
func (k *Kubeconfig) Connect(context string) (*Client, error) {
	checkConnectionsSooner()
	raw, err := k.loader.RawConfig()
	if err != nil {
		return nil, err
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(raw, context, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("context %q: %w", context, err)
	}
	cfg.QPS, cfg.Burst = 50, 100

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	meta, err := metadata.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	// Discovery comes from a disk cache (like kubectl's) so a start doesn't
	// wait ~1s for it; the server version is asked for at the same time.
	disc := cachedDiscovery(cfg, typed.Discovery())
	var version string
	versionDone := make(chan struct{})
	go func() {
		defer close(versionDone)
		if v, err := typed.Discovery().ServerVersion(); err == nil {
			version = v.GitVersion
		}
	}()
	res, err := Discover(disc)
	<-versionDone
	if err != nil {
		return nil, fmt.Errorf("context %q: discovery: %w", context, err)
	}
	metrics, err := newMetrics(cfg)
	if err != nil {
		return nil, err
	}
	c := &Client{
		Context:     context,
		Config:      cfg,
		Dynamic:     dyn,
		Metadata:    meta,
		Typed:       typed,
		Discovery:   typed.Discovery(),
		Cache:       NewCache(dyn),
		Resources:   res,
		Metrics:     metrics,
		stopMetrics: make(chan struct{}),
	}
	if kc, ok := raw.Contexts[context]; ok {
		c.Cluster = kc.Cluster
	}
	c.Server = cfg.Host
	c.Version = version
	c.discovery = disc
	c.Cache.Namespaced = func(gvr schema.GroupVersionResource) bool {
		r, ok := res.ByGVR(gvr)
		return !ok || r.Namespaced
	}
	go metrics.run(c.stopMetrics)
	return c, nil
}

// Close stops all informers and the metrics poller.
func (c *Client) Close() {
	c.closed.Store(true)
	c.Cache.Stop()
	if c.stopMetrics != nil {
		close(c.stopMetrics)
	}
}

// Closed reports that you left this cluster (switched to another).
func (c *Client) Closed() bool { return c.closed.Load() }

// AWSLogin is how the kubeconfig logs in to EKS: the environment and region
// it runs the aws CLI with. ok is false when it doesn't use the aws CLI.
func (c *Client) AWSLogin() (env []string, region string, ok bool) {
	if c.Config == nil || c.Config.ExecProvider == nil || filepath.Base(c.Config.ExecProvider.Command) != "aws" {
		return nil, "", false
	}
	x := c.Config.ExecProvider
	for _, e := range x.Env {
		env = append(env, e.Name+"="+e.Value)
		if e.Name == "AWS_REGION" || e.Name == "AWS_DEFAULT_REGION" {
			region = e.Value
		}
	}
	for i := 0; i+1 < len(x.Args); i++ {
		switch x.Args[i] {
		case "--region":
			region = x.Args[i+1]
		case "--profile":
			env = append(env, "AWS_PROFILE="+x.Args[i+1])
		}
	}
	return env, region, true
}

// eksHost matches an EKS API endpoint: <id>.<sk1|gr7…>.<region>.eks.amazonaws.com.
var eksHost = regexp.MustCompile(`\.([a-z]{2}(?:-gov)?-[a-z]+-\d)\.eks\.amazonaws\.com(?::\d+)?/?$`)

// EKSRegion is the region of an EKS cluster, from its API address; "" when
// the cluster isn't EKS.
func (c *Client) EKSRegion() string {
	if m := eksHost.FindStringSubmatch(c.Server); m != nil {
		return m[1]
	}
	return ""
}

// iamAccount matches the account in an IAM ARN.
var iamAccount = regexp.MustCompile(`arn:aws[a-z-]*:iam::(\d{12}):`)

// AWSAccount finds the AWS account an EKS cluster runs in when its
// kubeconfig doesn't say (no EKS ARN): the node role in aws-auth, else the
// account most service-account IAM roles (IRSA) belong to.
func (c *Client) AWSAccount(ctx context.Context) (string, error) {
	if cm, err := c.Typed.CoreV1().ConfigMaps("kube-system").Get(ctx, "aws-auth", metav1.GetOptions{}); err == nil {
		if m := iamAccount.FindStringSubmatch(cm.Data["mapRoles"]); m != nil {
			return m[1], nil
		}
	}
	sas, err := c.Typed.CoreV1().ServiceAccounts("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("can't tell which AWS account the cluster is in: %w", err)
	}
	count := map[string]int{}
	best := ""
	for _, sa := range sas.Items {
		if m := iamAccount.FindStringSubmatch(sa.Annotations["eks.amazonaws.com/role-arn"]); m != nil {
			count[m[1]]++
			if count[m[1]] > count[best] {
				best = m[1]
			}
		}
	}
	if best == "" {
		return "", errors.New("can't tell which AWS account the cluster is in")
	}
	return best, nil
}
