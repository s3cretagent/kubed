package kube

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Helm stores each release revision in a Secret named
// sh.helm.release.v1.<name>.v<revision>, labelled owner=helm.
const helmSelector = "owner=helm"

var secretsGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

// HelmRelease is one revision of a release, read from its Secret's labels.
type HelmRelease struct {
	Name, Namespace string
	Revision        int
	Status          string
	Updated         time.Time
	secret          string
}

// HelmReleases lists the latest revision of every release in ns ("" = all).
// Only metadata is fetched, so thousands of releases stay cheap.
func (c *Client) HelmReleases(ctx context.Context, ns string) ([]HelmRelease, error) {
	all, err := c.helmRevisions(ctx, ns, "")
	if err != nil {
		return nil, err
	}
	latest := map[string]HelmRelease{}
	for _, r := range all {
		key := r.Namespace + "/" + r.Name
		if cur, ok := latest[key]; !ok || r.Revision > cur.Revision {
			latest[key] = r
		}
	}
	out := make([]HelmRelease, 0, len(latest))
	for _, r := range latest {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// HelmHistory lists every stored revision of one release, newest first.
func (c *Client) HelmHistory(ctx context.Context, ns, name string) ([]HelmRelease, error) {
	revs, err := c.helmRevisions(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i].Revision > revs[j].Revision })
	return revs, nil
}

func (c *Client) helmRevisions(ctx context.Context, ns, name string) ([]HelmRelease, error) {
	selector := helmSelector
	if name != "" {
		selector += ",name=" + name
	}
	list, err := c.Metadata.Resource(secretsGVR).Namespace(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	out := make([]HelmRelease, 0, len(list.Items))
	for _, s := range list.Items {
		l := s.Labels
		rev, _ := strconv.Atoi(l["version"])
		// Each revision is a new Secret, made when it was deployed. The
		// modifiedAt label moves whenever the status changes (superseded), so
		// it says nothing about when the revision went out.
		updated := s.CreationTimestamp.Time
		out = append(out, HelmRelease{Name: l["name"], Namespace: s.Namespace, Revision: rev, Status: l["status"], Updated: updated, secret: s.Name})
	}
	return out, nil
}

// HelmDetail is the decoded content of one release revision.
type HelmDetail struct {
	Chart, ChartVersion, AppVersion string
	Status, Description             string
	Values                          map[string]any
	Manifest                        string
}

// HelmDetail decodes a release revision: Secret data → base64 → gzip → JSON.
func (c *Client) HelmDetail(ctx context.Context, r HelmRelease) (HelmDetail, error) {
	s, err := c.Typed.CoreV1().Secrets(r.Namespace).Get(ctx, r.secret, metav1.GetOptions{})
	if err != nil {
		return HelmDetail{}, err
	}
	return decodeHelmRelease(s.Data["release"])
}

func decodeHelmRelease(data []byte) (HelmDetail, error) {
	raw, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return HelmDetail{}, fmt.Errorf("helm release: %w", err)
	}
	if bytes.HasPrefix(raw, []byte{0x1f, 0x8b}) {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return HelmDetail{}, fmt.Errorf("helm release: %w", err)
		}
		defer zr.Close()
		if raw, err = io.ReadAll(zr); err != nil {
			return HelmDetail{}, fmt.Errorf("helm release: %w", err)
		}
	}
	var rel struct {
		Info struct {
			Status      string `json:"status"`
			Description string `json:"description"`
		} `json:"info"`
		Chart struct {
			Metadata struct {
				Name, Version string
				AppVersion    string `json:"appVersion"`
			} `json:"metadata"`
		} `json:"chart"`
		Config   map[string]any `json:"config"`
		Manifest string         `json:"manifest"`
	}
	if err := json.Unmarshal(raw, &rel); err != nil {
		return HelmDetail{}, fmt.Errorf("helm release: %w", err)
	}
	return HelmDetail{
		Chart: rel.Chart.Metadata.Name, ChartVersion: rel.Chart.Metadata.Version, AppVersion: rel.Chart.Metadata.AppVersion,
		Status: rel.Info.Status, Description: rel.Info.Description, Values: rel.Config, Manifest: rel.Manifest,
	}, nil
}
