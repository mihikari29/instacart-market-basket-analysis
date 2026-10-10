#!/usr/bin/env bash
# Restore the local WSL MongoDB filesystem before using runtime.override.yml.
set -euo pipefail
storage_image=/mnt/d/instacart-market-basket-analysis/results/module4/acceptance-20261009/mongo-data.ext4
storage_mount=/mnt/module4-storage-20261009
test -f "$storage_image"
mkdir -p "$storage_mount"
if ! mountpoint -q "$storage_mount"; then
  mount -o loop "$storage_image" "$storage_mount"
fi
test "$(findmnt -n -o FSTYPE --target "$storage_mount")" = ext4
test -f "$storage_mount/mongodb/WiredTiger"
test -f "$storage_mount/namenode/current/VERSION"
test -f "$storage_mount/datanode/current/VERSION"
df -h "$storage_mount"
