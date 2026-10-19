#!/bin/sh
# Install kubed. Needs the GitHub CLI (gh) logged in with access to the repo:
#
#   gh api repos/s3cretagent/kubed/contents/scripts/install.sh -H 'Accept: application/vnd.github.raw' | sh
#
# KUBED_INSTALL_DIR  where to put it (default ~/.local/bin)
# KUBED_VERSION      release tag or git ref (default: latest release, else main)
set -eu

REPO=s3cretagent/kubed
DEST=${KUBED_INSTALL_DIR:-$HOME/.local/bin}
VERSION=${KUBED_VERSION:-}

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case $(uname -m) in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "kubed: unsupported CPU $(uname -m)" >&2; exit 1 ;;
esac

command -v gh >/dev/null || { echo "kubed: install the GitHub CLI first (brew install gh), then: gh auth login" >&2; exit 1; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

if gh release view ${VERSION:+"$VERSION"} -R "$REPO" >/dev/null 2>&1; then
  echo "kubed: downloading ${VERSION:-latest release} for $os/$arch…"
  gh release download ${VERSION:+"$VERSION"} -R "$REPO" -p "kubed_*_${os}_${arch}.tar.gz" -D "$tmp"
  tar -xzf "$tmp"/kubed_*.tar.gz -C "$tmp" kubed
else
  command -v go >/dev/null || { echo "kubed: no release yet and Go is not installed (brew install go)" >&2; exit 1; }
  ref=${VERSION:-main}
  echo "kubed: no release found; building $ref from source…"
  GOPRIVATE=github.com/s3cretagent GOBIN="$tmp" GOFLAGS=-mod=mod \
    GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=credential.https://github.com.helper GIT_CONFIG_VALUE_0='!gh auth git-credential' \
    go install -ldflags "-X main.version=$ref" "github.com/$REPO/cmd/kubed@$ref"
fi

mkdir -p "$DEST"
install -m 0755 "$tmp/kubed" "$DEST/kubed"
echo "kubed: installed $DEST/kubed ($("$DEST/kubed" --version))"
case ":$PATH:" in
  *":$DEST:"*) ;;
  *) echo "kubed: add it to your PATH:  echo 'export PATH=\"$DEST:\$PATH\"' >> ~/.zshrc" ;;
esac
