#!/usr/bin/env bash
set -euo pipefail
export PATH=/opt/module4-runtime-20261008/docker:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export DOCKER_HOST=unix:///var/run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config
storage_mount=/mnt/module4-storage-20261009
mountpoint -q "$storage_mount"
test ! -e "$storage_mount/namenode"
test ! -e "$storage_mount/datanode"
docker stop --timeout 30 instacart-datanode-1 instacart-namenode-1
for service in namenode datanode; do
  mkdir "$storage_mount/$service"
  cp -a "/var/lib/module4-docker-20261008/volumes/instacart_${service}_data/_data/." "$storage_mount/$service/"
done
docker compose -f compose.yaml -f docs/evidence/module4/acceptance-20261009/runtime.override.yml \
  up -d --no-deps --wait namenode datanode
docker compose exec -T namenode hdfs dfsadmin -safemode wait
docker compose exec -T namenode hdfs dfs -ls /instacart/curated
df -h "$storage_mount"
