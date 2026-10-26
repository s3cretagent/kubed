---
title: Install
description: Install kubed with Homebrew, Go, or a download.
sidebar: { order: 1 }
---

## Homebrew (macOS and Linux)

```bash
brew install s3cretagent/tap/kubed
```

Upgrade later with `brew upgrade kubed`.

## Go

```bash
go install github.com/s3cretagent/kubed/cmd/kubed@latest
```

## Download

Builds for macOS and Linux (Intel and ARM) are on the
[releases page](https://github.com/s3cretagent/kubed/releases). Unpack the
archive and put `kubed` somewhere on your `PATH`.

## Start

kubed uses the same kubeconfig as `kubectl`.

```bash
kubed                          # your current cluster
kubed --readonly               # look around; nothing can be changed
kubed --context staging        # another cluster from your kubeconfig
```

The five keys to know:

| Key | What it does |
|---|---|
| enter | open a row, or see everything you can do with it |
| esc | go back |
| f | find anything by name |
| ? | help |
| q | quit |

Next: [Using kubed](../guide/).
