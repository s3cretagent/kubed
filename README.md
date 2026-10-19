# kubed

**A simple terminal app for running Kubernetes.** Open it and you see your
apps, with the broken ones first. It tells you in plain words what is wrong
and how to fix it, and every screen shows the keys that work on it.

![kubed: apps with the broken ones first, the crash logs, what changed, and going back a version](docs/media/kubed-demo.gif)

On Amazon EKS it also shows what your cluster and each app cost, from your
own AWS bill and live spot prices.

## Install

With Homebrew (macOS and Linux):

```bash
brew install s3cretagent/tap/kubed
```

Or with Go: `go install github.com/s3cretagent/kubed/cmd/kubed@latest`. From a
copy of the code: `make install`. Builds for macOS and Linux are also on the
[releases page](https://github.com/s3cretagent/kubed/releases).

## Start

```bash
kubed                          # your current cluster (the same one kubectl uses)
kubed --readonly               # look around; nothing can be changed
kubed --context staging        # another cluster from your kubeconfig, by
                               # its name or part of it
```

Inside kubed, **:ctx** lists your clusters (production ones marked) and
**:ns** your namespaces. The header always says which cluster you are on.

The five keys to know:

| Key | What it does |
|---|---|
| enter | open a row, or see everything you can do with it |
| esc | go back |
| f | find anything by name |
| ? | help |
| q | quit |

## Learn more

- **[Using kubed](docs/GUIDE.md)**: everyday use, fixing problems, logs,
  making changes safely, production clusters, costs, and what to do when it
  can't connect.
- **[How kubed works](docs/DEVELOPMENT.md)**: building, testing and the
  details behind what it shows.
