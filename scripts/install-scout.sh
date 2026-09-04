#!/usr/bin/env bash
set -euo pipefail
test "$(uname -s)" = Linux
test "$(uname -m)" = x86_64
scout_tmp="$(mktemp -d)"
scout_plugins="${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins"
curl --fail --silent --show-error --location --retry 3 \
  https://github.com/docker/scout-cli/releases/download/v1.24.0/docker-scout_1.24.0_linux_amd64.tar.gz \
  --output "$scout_tmp/scout.tar.gz"
echo "f4e2814bd61040365153d5b964b144cb2dc6ee536a68b5bac4cadf00fc0ec34b  $scout_tmp/scout.tar.gz" | sha256sum --check --strict
tar -xzf "$scout_tmp/scout.tar.gz" -C "$scout_tmp" docker-scout
mkdir -p "$scout_plugins"
install -m 0755 "$scout_tmp/docker-scout" "$scout_plugins/docker-scout"
docker scout version
