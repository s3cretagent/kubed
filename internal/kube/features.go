package kube

import (
	"os"

	clientfeatures "k8s.io/client-go/features"
)

// Streaming lists (client-go's WatchListClient) send every object as its
// own watch event. On EKS that made the first load of pods 10x slower than
// a plain paged list (22-30s vs 2s for ~500 pods), so kubed lists the plain
// way. KUBE_FEATURE_WatchListClient=true turns streaming back on.
func init() {
	clientfeatures.ReplaceFeatureGates(noWatchList{clientfeatures.FeatureGates()})
}

type noWatchList struct{ base clientfeatures.Gates }

func (g noWatchList) Enabled(f clientfeatures.Feature) bool {
	if f == clientfeatures.WatchListClient {
		return os.Getenv("KUBE_FEATURE_WatchListClient") == "true"
	}
	return g.base.Enabled(f)
}
