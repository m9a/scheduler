#!/usr/bin/env bash
# Starts/stops the control-plane docker stacks based on control_plane_config.yaml.
#
# The compose file is the template: which services run (compose profiles) and
# their ports/credentials (compose variables) are derived from the config here
# and fed to `docker compose` at invocation time — no generated compose file.
# On `up`, the fully resolved compose is logged to .control-plane-resolved.yml
# so failed startups can be debugged against exactly what was deployed.
#
# Usage: scripts/control-plane.sh up|down|status
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$REPO_ROOT/control_plane_config.yaml"
COMPOSE="$REPO_ROOT/docker-compose.test.yml"
RESOLVED="$REPO_ROOT/.control-plane-resolved.yml"

# Reads `<section>.<key>` from control_plane_config.yaml. Dependency-free on purpose
# (host pythons differ on pyyaml) — works because the config is constrained to
# flat two-level scalar keys; keep it that way.
cfg() {
    local value
    value=$(awk -v section="$1" -v key="$2" '
        /^[A-Za-z]/ { current=$1; sub(":", "", current) }
        current == section && $1 == key":" { print $2; exit }
    ' "$CONFIG")
    echo "${value:-$3}"
}

profiles=""
[ "$(cfg mlflow enabled false)" = "true" ] && profiles="mlflow"
[ "$(cfg metrics enabled false)" = "true" ] && profiles="${profiles:+$profiles,}metrics"

export COMPOSE_PROFILES="$profiles"
export MLFLOW_PORT="$(cfg mlflow port 5000)"
export PROMETHEUS_PORT="$(cfg metrics prometheusPort 9095)"
export GRAFANA_PORT="$(cfg metrics grafanaPort 3000)"
export MINIO_ACCESS_KEY="$(cfg minio accessKey minioadmin)"
export MINIO_SECRET_KEY="$(cfg minio secretKey minioadmin)"

case "${1:-}" in
    up)
        echo "Enabled stacks: ${COMPOSE_PROFILES:-(none — MinIO only)}"
        docker compose -f "$COMPOSE" config > "$RESOLVED"
        echo "Resolved compose written to $RESOLVED"
        docker compose -f "$COMPOSE" up -d
        ;;
    down)
        # --profile '*' tears down every profiled service regardless of what is
        # currently enabled, so a config edit between up and down leaves no orphans.
        docker compose -f "$COMPOSE" --profile '*' down -v
        ;;
    status)
        docker compose -f "$COMPOSE" ps
        ;;
    *)
        echo "Usage: $0 up|down|status" >&2
        exit 1
        ;;
esac
