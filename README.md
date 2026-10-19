# kubed

**A simple terminal app for running Kubernetes.** Open it and you see your
apps, with the broken ones first. It tells you in plain words what is wrong
and how to fix it, and every screen shows the keys that work on it.

```
╭──────── apps (81) · 1 failing · 3 to check · problems first · 10 idle ────╮
│ APP             NAMESPACE      STATUS        READY   RESTARTS  DEPLOYED   │
│ payments        payments       Rollout stuck 0/2     0         2h ago     │
│ cert-manager    cert-manager   Out of sync   3/3     0         61d ago    │
│ auth            auth           Running       2/2     5         9d ago     │
├──────────────────────────────── about payments ───────────────────────────┤
│ Problem  Rollout stuck: the new version can't start: can't pull image     │
│          registry/payments:v2: that image or tag doesn't exist            │
│ Fix      o to watch the rollout, or u to go back to the previous version  │
│ Pods     0 of 2 ready                                                     │
╰───────────────────────────────────────────────────────────────────────────╯
 enter open / actions · f find · / filter · w what changed · E events · ? help
```

On Amazon EKS it also shows what your cluster and each app cost, from your
own AWS bill and live spot prices.

## Install

```bash
gh api repos/s3cretagent/kubed/contents/scripts/install.sh -H 'Accept: application/vnd.github.raw' | sh
```

This needs the GitHub CLI, logged in (`gh auth login`) with access to this
repo. From a copy of the code: `make install`.

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
