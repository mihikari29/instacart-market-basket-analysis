#!/usr/bin/env bash
set -euo pipefail
export PATH=/opt/module4-runtime-20261008/docker:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export DOCKER_HOST=unix:///var/run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config
storage_image=/mnt/d/instacart-market-basket-analysis/results/module4/acceptance-20261009/mongo-data.ext4
storage_mount=/mnt/module4-storage-20261009
old_data=/var/lib/module4-docker-20261008/volumes/instacart_mongodb_data/_data
test ! -e "$storage_image"
test -d "$old_data"
truncate -s 8G "$storage_image"
mkfs.ext4 -q -F "$storage_image"
mkdir -p "$storage_mount"
mount -o loop "$storage_image" "$storage_mount"
mkdir "$storage_mount/mongodb"
docker stop --timeout 30 instacart-mongodb-1
cp -a "$old_data/." "$storage_mount/mongodb/"
docker compose -f compose.yaml -f docs/evidence/module4/acceptance-20261009/runtime.override.yml \
  up -d --no-deps --wait mongodb
df -h "$storage_mount"
printf 'Original MongoDB volume retained: %s\n' "$old_data"
