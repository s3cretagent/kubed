# Security

## Reporting a problem

Please don't open a public issue for a security problem. Report it privately
instead: on GitHub, open the **Security** tab of this repository and choose
**Report a vulnerability**. Only the maintainer can see the report.

Say what you found, how to reproduce it, and which version of kubed you use
(`kubed --version`). You'll get a reply within a few days, and a fix or a
plan within two weeks for anything serious.

## Supported versions

Fixes go into the latest release. Please upgrade before reporting
(`brew upgrade kubed`).

## What kubed does with your cluster

kubed uses your own kubeconfig and talks only to your cluster's API server,
plus AWS's own APIs for prices and costs on Amazon EKS. It sends nothing anywhere else.
`--readonly` turns off every action that changes the cluster.
