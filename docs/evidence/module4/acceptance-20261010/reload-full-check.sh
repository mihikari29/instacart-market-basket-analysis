#!/usr/bin/env bash
set -euo pipefail
cd /mnt/d/instacart-market-basket-analysis
export PATH=/opt/module4-runtime-20261008/docker:$PATH
export DOCKER_HOST=unix:///run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config
export COMPOSE_FILE=compose.yaml:docs/evidence/module4/acceptance-20261010/runtime.override.yml
docker compose run --rm --use-aliases --no-deps --entrypoint python3 module4 \
  results/module4/acceptance-20261010/reload-full-check.py >results/module4/acceptance-20261010/reload-full-check.log 2>&1
