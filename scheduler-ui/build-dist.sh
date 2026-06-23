#!/usr/bin/env bash
# Build the UI bundle inside Docker and write it to ./dist on the host. The Node
# toolchain lives only in the Dockerfile's build stage, so nothing needs to be
# installed locally. Point coordinator.uiDir at the resulting ./dist to serve it.
set -euo pipefail
cd "$(dirname "$0")"

DOCKER_BUILDKIT=1 docker build --target export --output type=local,dest=dist .

echo
echo "Built UI bundle → $(pwd)/dist"
echo "Set coordinator.uiDir to this path in control-plane.yaml to serve it."
