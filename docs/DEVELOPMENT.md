# How kubed works

Notes for people who build kubed or want to know exactly what it does. To
learn how to use it, read the [guide](GUIDE.md).

## Building and testing

```bash
make install                    # build and install into $GOPATH/bin
make test                       # unit tests with the race detector
make lint                       # gofmt, go vet, golangci-lint
make smoke CONTEXT=<context>    # read-only checks against a real cluster
go test -bench . ./internal/view ./internal/ui   # rendering 5,000 pods
```

The smoke tests (build tag `smoke`) only read. They check, among other
things, that every pod in the cluster lands in exactly one app and how long
the first screen takes.

CI (`.github/workflows/ci.yml`) runs the race tests on Linux, lint, and
builds every release target on each pull request. Pushing a `v*` tag runs
GoReleaser (`release.yml`), which attaches macOS and Linux builds to a
GitHub release.

## Layout

```
cmd/kubed          flags, starts the UI
internal/ui        the screen: header, lists, pager, prompts, keys, actions
internal/view      one view per kind: columns, rows, detail lines, drill-down
internal/enrich    what kubed adds: apps, health, ArgoCD, Karpenter, prices, IAM roles, secrets
internal/kube      talking to Kubernetes: kubeconfig, cache, discovery, metrics, actions, exec, logs
internal/pricing   EC2 prices, your AWS bill, the cluster's bill
```

`kube` never imports `ui`. `view` turns objects into rows. `enrich` reads
only the local cache, so drawing the screen never waits on the API server.

## Loading data

- kubed keeps a watch-based cache per kind. A kind is only loaded when a
  screen needs it; reading the cache never starts a load by itself.
- The apps screen loads in two steps: pods, Deployments and ArgoCD apps
  first, then ReplicaSets, StatefulSets, DaemonSets, Jobs, nodes and
  ExternalSecrets. A pod finds its Deployment through its
  `pod-template-hash` label, so the first step doesn't need ReplicaSets
  (some clusters have thousands).
- client-go's streaming lists are turned off: on EKS they made the first
  pod list about ten times slower. `KUBE_FEATURE_WatchListClient=true` turns
  them back on.
- Discovery is cached on disk for six hours and refreshed when you type a
  kind kubed doesn't know.
- Secrets are cached with their keys only. Values, and the
  `last-applied-configuration` annotation that can hold them, are dropped.
  Showing a value (`x`, `y`, `e`) reads the Secret live.
- The secrets list asks the server to leave out Helm's release records
  (`type!=helm.sh/release.v1`). On some clusters they are most of the
  Secrets and each holds a whole compressed chart; the Helm screen reads
  them on its own.

## Apps

- With ArgoCD, an app is an ArgoCD Application. Pods are matched through
  the `argocd.argoproj.io/tracking-id` annotation (following pod →
  ReplicaSet → Deployment). A pod ArgoCD doesn't track joins the ArgoCD app
  in its namespace only when an operator made it (its top owner is a custom
  resource) or when it would form an app of the same name anyway. A plain
  Deployment made by hand next to an ArgoCD app stays its own app, so
  "Restart app" never reaches it.
- Without ArgoCD, pods are grouped by `app.kubernetes.io/instance`,
  `release`, `app.kubernetes.io/name` or `app`, then by their top owner,
  then by namespace.
- When ArgoCD calls an app degraded, newer ArgoCD versions don't keep the
  reason in the Application. kubed reads the resources the app manages
  (for the selected app only) and checks their status: not enough ready
  pods, and conditions such as `Ready=False`, `Fallback=True` or
  `ErrorOccurred=True`.
- A failing ExternalSecret shows its error from its events, read for the
  selected app only, and whether its Secret exists (a metadata-only read).
- An app's events and "what changed" keep only what belongs to it: its
  pods, their owners up the chain, their autoscaler, and the ConfigMaps and
  Secrets the pods use. Many apps share a namespace, so the namespace alone
  isn't enough.
- A pod's AWS role comes from its service account (IRSA), or else from the
  pod's `iam.amazonaws.com/role` annotation (kube2iam, kiam).

## Changes and safety

- Every change asks first and warns about what will undo it: ArgoCD
  self-heal, ExternalSecrets rewriting a Secret.
- Edits go through a server dry run first and carry the object's
  `resourceVersion`, so a change made by someone else in the meantime stops
  the edit.
- Production is decided by the `kube-public/kubed` ConfigMap, then your own
  patterns (`prodPattern` first, then `nonProdPattern`), then whole words in
  the context or cluster name (production words win over the others), then
  your answer (remembered per API server). Unknown clusters are treated like
  production for confirmations.
- `--readonly` turns off every change, including shells (commands there can
  change a pod).
- Error output from credential helpers goes to a log file of each kubed's
  own (`stderr-<pid>.log` in the cache folder), so two windows never mix
  their messages. A clean exit deletes it. If kubed crashes, the next start
  keeps the log as `crash-<time>.log` and says where it is.

## Costs

Costs show only when kubed finds the cluster's AWS account and can log in to
it. Otherwise nothing about money appears.

**Which account.** From the EKS ARN in the kubeconfig; otherwise the node
role in `kube-system/aws-auth`; otherwise the account most service-account
IAM roles belong to.

**How to log in.** The `aws` CLI with the profile and region the kubeconfig
uses for `aws eks get-token`. With a static token: the current AWS
environment if it is in that account, else a profile in `~/.aws/config` whose
`sso_account_id` or `role_arn` is that account. kubed then checks with
`sts get-caller-identity` that the login is in the cluster's own account,
so a role in account B never shows account B's bill for a cluster in A.

**Node prices.**

| Node | Price | From |
|---|---|---|
| spot | live price in the node's zone, matched by zone ID | `ec2 describe-spot-price-history`, every 15 minutes |
| on-demand | what the account paid per hour for that type last week (median of daily amortized cost ÷ hours) | Cost Explorer |
| on-demand type not on last week's bill | list price less the discount the bill shows for the other types | |
| no bill | list price (spot: recent average), or `onDemandDiscount` from config | instances.vantage.sh, refreshed daily, with a built-in copy |

**All-in.** Everything Cost Explorer bills under the cluster's tags over the
last seven days, limited to Kubernetes infrastructure (EC2 compute, EC2
other, load balancing, CloudWatch, VPC, EKS). The tags are the ones AWS and
its controllers set (`aws:eks:cluster-name`, `eks:cluster-name`,
`elbv2.k8s.aws/cluster`, `karpenter.sh/discovery`,
`kubernetes.io/cluster/<name>`) plus the EKS cluster's own tags whose value
is its name. If the tagged nodes come to less than half of what the nodes
cost, the tags aren't cost allocation tags yet and All-in stays hidden. The
tax rate comes from last month's invoice.

**Requests.** Cost Explorer: three requests per cluster per day, cached for
a day (AWS charges $0.01 each). EC2, EKS and STS reads are free.
Permissions: `ce:GetCostAndUsage`, `ec2:DescribeSpotPriceHistory`,
`eks:ListClusters`, `eks:DescribeCluster`, `sts:GetCallerIdentity`.

**:waste savings** are worked out per node: what the pods there could give
back (the larger of CPU and memory, as a share of the node), never more
than the node costs, shared between the workloads by how much each gives
back.

**An app's cost** is its pods' share of their nodes: the larger of CPU or
memory requested ÷ what the node has, times the node's price. Idle capacity
is in the node total and in no app.

The cache folder (`~/Library/Caches/kubed` on macOS, `~/.cache/kubed` on
Linux) holds the price list, the bills (one file per account and region)
and discovery. `make prices` refreshes the built-in price list.

## Wording

kubed talks like a person: short sentences, no jargon where a plain word
works, and no em-dashes. A test fails if one appears in the code or docs.
