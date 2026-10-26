---
title: kubed and k9s
description: How kubed and k9s differ, and when to use which.
---

[k9s](https://k9scli.io) is a great power tool for browsing every resource in a cluster, and many people use both. They answer different questions.

## In short

- **Use k9s** to explore any resource, use plugins, or the XRay and Pulses views.
- **Use kubed** when something is broken and you want the reason and the fix, fast.

## Side by side

| | k9s | kubed |
|---|---|---|
| Starts on | a list of one resource kind | your apps, broken ones first |
| Tells you | each resource's status | what is wrong, why, and what to do next |
| Crash reason | open the logs and look | the error line from the last run, in the app's box |
| History | not kept | what changed in the last 24 hours, per app |
| Production safety | read-only mode | red frame, typed names for every change, read-only mode |
| ArgoCD, ExternalSecrets, Karpenter | shown as resources | part of the app's health and its Fix line |
| Cost | not built in | per cluster, per node, per app on EKS |
