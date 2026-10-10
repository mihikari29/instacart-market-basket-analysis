#!/usr/bin/env bash
set -euo pipefail
cd /mnt/d/instacart-market-basket-analysis
export PATH=/opt/module4-runtime-20261008/docker:$PATH
export DOCKER_HOST=unix:///run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config
export COMPOSE_FILE=compose.yaml:docs/evidence/module4/acceptance-20261010/runtime.override.yml
receipt=results/module4/acceptance-20261010-fixed
mkdir -p /var/lib/module4-local-20261010 "$receipt"
chmod 1777 /var/lib/module4-local-20261010
run_step() {
  local name=$1
  shift
  printf 'Starting %s at %s\n' "$name" "$(date --iso-8601=seconds)"
  printf 'Command: ' > "$receipt/$name.log"
  printf '%q ' "$@" >> "$receipt/$name.log"
  printf '\n' >> "$receipt/$name.log"
  local code=0
  "$@" >> "$receipt/$name.log" 2>&1 || code=$?
  printf '\nExit code: %s\n' "$code" >> "$receipt/$name.log"
  printf 'Finished %s (exit %s) at %s\n' "$name" "$code" "$(date --iso-8601=seconds)"
  return "$code"
}
run_step services docker compose up -d --wait namenode datanode mongodb spark-master spark-worker
run_step runtime-smoke docker compose run --rm --use-aliases --no-deps --entrypoint python3 module4 scripts/module4_runtime_smoke.py
run_step regression-tests docker compose run --rm --use-aliases --no-deps --entrypoint python3 module4 -m pytest -q
run_step lint docker compose run --rm --no-deps --entrypoint python3 module4 -m ruff check .
for attempt in first replay; do
  run_id=full-ab-20261010-fixed-$attempt
  run_step "$attempt-pipeline" python3 scripts/module4.py all --run-id "$run_id" \
    --model-version als-full-20261010-fixed --graph-version graph-full-20261010-fixed \
    --shuffle-partitions 32 --executor-memory 2g --executor-cores 4 --total-cores 4 --als-blocks 8 \
    --pilot-users 5000 --max-trials 1 --graph-min-edge-weight 10 \
    --scope full_data --source-receipt data/raw/source-receipt.json
  comparison=()
  if [ "$attempt" = replay ]; then comparison=(--compare-to "$receipt/first-snapshot.json"); fi
  run_step "$attempt-readback" docker compose run --rm --no-deps --entrypoint python3 module4 \
    scripts/check_module4_run.py --run-dir "results/module4/$run_id" \
    --output "$receipt/$attempt-snapshot.json" "${comparison[@]}"
done
