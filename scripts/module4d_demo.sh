#!/usr/bin/env bash
# Start or stop one task-owned Module 4D synthetic demonstration.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  echo "Usage: $0 up | down <12-hex-run-id>" >&2
  exit 2
}

down_demo() {
  local run_id="$1"
  if [[ ! "$run_id" =~ ^[a-f0-9]{12}$ ]]; then
    echo "Run ID must be exactly 12 lowercase hexadecimal characters." >&2
    return 2
  fi
  local env_file="/tmp/module4d-demo-${run_id}.env"
  local project="module4d-demo-${run_id}"
  if [[ ! -f "$env_file" ]]; then
    echo "No private environment file exists for demo run ${run_id}." >&2
    return 1
  fi
  if ! docker compose --project-name "$project" --env-file "$env_file" \
    -f compose.module4d-demo.yaml down --remove-orphans; then
    echo "Could not stop demo ${run_id}; keeping its private environment file." >&2
    return 1
  fi
  local image_tag
  image_tag="$(sed -n 's/^MODULE4D_DEMO_TAG=//p' "$env_file")"
  if [[ ! "$image_tag" =~ ^[a-f0-9]{12}$ ]]; then
    echo "Private environment file has an invalid demo image tag; keeping it for inspection." >&2
    return 1
  fi
  local image
  for image in "instacart-module4d-demo-api:${image_tag}" "instacart-module4d-demo-ui:${image_tag}"; do
    if docker image inspect "$image" >/dev/null 2>&1; then
      if ! docker image rm --force "$image" >/dev/null 2>&1; then
        echo "Could not remove task-owned image ${image}; keeping the private environment file." >&2
        return 1
      fi
    fi
  done
  python3 -c 'from pathlib import Path; import sys; Path(sys.argv[1]).unlink(missing_ok=True)' "$env_file"
  echo "Stopped and removed task-owned demo ${run_id}; tmpfs data was discarded."
}

case "${1:-}" in
  down)
    [[ "$#" == 2 ]] || usage
    down_demo "$2"
    exit
    ;;
  up)
    [[ "$#" == 1 ]] || usage
    ;;
  *)
    usage
    ;;
esac

run_id="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
env_file="/tmp/module4d-demo-${run_id}.env"
project="module4d-demo-${run_id}"
unset MODULE4D_DEMO_TAG MODULE4D_DEMO_DATABASE MODULE4D_DEMO_ROOT_PASSWORD \
  MODULE4D_DEMO_READONLY_USERNAME MODULE4D_DEMO_READONLY_PASSWORD \
  MODULE4D_DEMO_TREND_NAMESPACE MODULE4D_DEMO_API_HOST_PORT MODULE4D_DEMO_UI_HOST_PORT
cleanup_failed_start() {
  local status=$?
  if (( status == 0 )); then
    status=130
  fi
  if (( status != 0 )); then
    trap - ERR INT TERM
    echo "Demo startup failed; cleaning only task-owned run ${run_id}." >&2
    down_demo "$run_id" >/dev/null 2>&1 || true
    exit "$status"
  fi
}
trap cleanup_failed_start ERR INT TERM

python3 scripts/module4d_demo_env.py --output "$env_file"
chmod 600 "$env_file"
compose=(docker compose --project-name "$project" --env-file "$env_file" -f compose.module4d-demo.yaml)
"${compose[@]}" config --quiet
"${compose[@]}" build module4d-api module4d-ui
"${compose[@]}" up -d module4d-mongo
"${compose[@]}" run --rm fixture
"${compose[@]}" up -d module4d-api module4d-ui
"${compose[@]}" run --rm validator
trap - ERR INT TERM

echo "Module 4D demo is available at http://127.0.0.1:18501"
echo "API health: http://127.0.0.1:18000/health"
echo "Run ID: ${run_id}"
echo "Stop and discard the demo with: bash scripts/module4d_demo.sh down ${run_id}"
