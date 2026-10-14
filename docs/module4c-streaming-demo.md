# C8B controlled streaming demonstration

This experiment exercises the accepted streaming and serving path with 48
controlled synthetic events:

```text
Kafka 3.7.0 → Spark Structured Streaming 3.5.5 → Module 3 C4B foreachBatch
→ mutable realtime_trending rows + immutable realtime_trending_windows bundles
→ Module 4C C5 verification → real serve-user CLI → C6 versioned rows/pointer
```

The only fixture scope is `controlled_synthetic`. It uses a synthetic warm user
with 20 ALS recommendations and 20 matching graph/catalog entries. It does not
read official Instacart events, train data, or ground-truth labels.

## Event and window design

The fixed target windows preserve the production 120-minute window and
five-minute slide:

| Target | Start | End | Favored product |
| --- | --- | --- | --- |
| A | 2024-01-01 00:00 UTC | 2024-01-01 02:00 UTC | P1 |
| B | 2024-01-01 03:00 UTC | 2024-01-01 05:00 UTC | P2 |

Each target has one event per product and two extra events for its favored
product, all in the target's final ten minutes. Each group therefore has 22
events, and both 30-minute and 120-minute counts favor the intended product.
Two additional valid product-20 events at 02:20/02:25 and 05:20/05:25 UTC
advance Spark's ten-minute watermark. Their event times are outside both target
windows. The experiment waits for the observed Spark watermark to pass each
target end and for the actual C4B bundle to appear before requesting serving.
The five-minute slide can produce other finalized windows; the report records
all namespace bundle counts and identifies the two selected target windows.

An independent Python oracle checks counts, min-max normalization, deterministic
Module 3 order, and the lambda=0.5 hybrid scores. It predicts P1 as Window A's
Top-1 and P2 as Window B's Top-1. This is a contract demonstration, not an
accuracy evaluation or a production latency benchmark.

## Run in the pinned disposable environment

Run this from the repository root in Bash. It refuses to pull or build images,
checks all three required local images, creates one internal task-labeled
network, starts one KRaft broker and one tmpfs-only MongoDB, then runs the
streaming harness in the pinned Spark image. The Spark source tree is mounted
read-only; only the unique evidence run directory is writable. There are no
published ports, attached project volumes, or connections to project services.
The Kafka image declares three anonymous Docker volumes; the task-owned teardown
captures and removes those exact run-created volumes after inspection.

```bash
set -Eeuo pipefail

IMAGE_KAFKA=apache/kafka:3.7.0
IMAGE_MONGO=mongo:7.0.16
IMAGE_SPARK=instacart-spark:3.5.5
for image in "$IMAGE_KAFKA" "$IMAGE_MONGO" "$IMAGE_SPARK"; do
  docker image inspect "$image" >/dev/null || {
    printf 'Required pinned image is absent: %s\n' "$image" >&2
    exit 2
  }
done

RUN_ID="c8b-$(date -u +%Y%m%dt%H%M%Sz | tr '[:upper:]' '[:lower:]')-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
SAFE_RUN="${RUN_ID//-/_}"
NETWORK="i3-c8b-net-$RUN_ID"
KAFKA="i3-c8b-kafka-$RUN_ID"
MONGO="i3-c8b-mongo-$RUN_ID"
SPARK="i3-c8b-spark-$RUN_ID"
OWNER_HELPER="i3-c8b-owner-helper-$RUN_ID"
TOPIC="i3-c8b-$RUN_ID"
DATABASE="i3_c8b_$SAFE_RUN"
RUN_DIR="$PWD/results/module4c-streaming-demo/$RUN_ID"
CLUSTER_ID="$(docker run --pull=never --rm --network none --entrypoint /opt/kafka/bin/kafka-storage.sh "$IMAGE_KAFKA" random-uuid)"
mkdir -p results
docker run --pull=never --rm --network none \
  -v "$PWD/results:/results:rw" \
  --entrypoint sh "$IMAGE_SPARK" -c \
  "mkdir -p '/results/module4c-streaming-demo/$RUN_ID' && chown $(id -u):$(id -g) '/results/module4c-streaming-demo/$RUN_ID'"

GIT_COMMIT="$(git rev-parse HEAD)"
GIT_STATUS_JSON="$(git status --porcelain | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().splitlines()))')"
DEADLINE=$((SECONDS + 270))

cleanup() {
  local rc=$?
  local cleanup_rc=0
  local cleaned=no
  local finalize_rc=0
  trap - EXIT INT TERM
  set +e
  if [[ -d "$RUN_DIR" ]]; then
    docker run --pull=never --rm --name "$OWNER_HELPER" --network none \
      --label codex.task=c8b --label "codex.run=$RUN_ID" \
      -v "$RUN_DIR:/out:rw" --entrypoint sh "$IMAGE_SPARK" -c \
      "chown -R $(id -u):$(id -g) /out"
    helper_rc=$?
    if (( helper_rc != 0 )); then
      printf 'Evidence ownership helper failed with exit %s\n' "$helper_rc" >&2
      cleanup_rc=$helper_rc
    fi
  fi
  python3 scripts/module4/module4c_streaming_demo.py cleanup-docker \
    --run-id "$RUN_ID" --evidence-dir "$RUN_DIR"
  docker_cleanup_rc=$?
  if (( docker_cleanup_rc != 0 )); then
    cleanup_rc=1
  elif (( cleanup_rc == 0 )); then
    cleaned=yes
  fi
  if [[ -f "$RUN_DIR/report.json" ]]; then
    args=(python3 scripts/module4/module4c_streaming_demo.py finalize --run-id "$RUN_ID" \
      --report "$RUN_DIR/report.json" --docker-cleanup-report "$RUN_DIR/docker-cleanup.json" \
      --cleaned "$cleaned" --experiment-exit-code "$rc")
    "${args[@]}"
    finalize_rc=$?
    if (( finalize_rc != 0 )); then cleanup_rc=1; fi
  fi
  if (( rc == 0 && cleanup_rc != 0 )); then rc=1; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker network create --internal \
  --label codex.task=c8b --label "codex.run=$RUN_ID" "$NETWORK" >/dev/null

docker run --pull=never -d --name "$KAFKA" --network "$NETWORK" --network-alias c8b-kafka \
  --label codex.task=c8b --label "codex.run=$RUN_ID" --memory 1g --cpus 1 \
  -e CLUSTER_ID="$CLUSTER_ID" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://c8b-kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@c8b-kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  "$IMAGE_KAFKA" >/dev/null

docker run --pull=never -d --name "$MONGO" --network "$NETWORK" --network-alias c8b-mongo \
  --label codex.task=c8b --label "codex.run=$RUN_ID" --memory 1g --cpus 1 \
  --tmpfs /data/db:rw,nosuid,nodev,size=512m \
  --tmpfs /data/configdb:rw,nosuid,nodev,size=32m \
  "$IMAGE_MONGO" --bind_ip_all >/dev/null

ready=no
for _ in $(seq 1 75); do
  if docker exec "$KAFKA" /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server c8b-kafka:9092 >/dev/null 2>&1; then ready=yes; break; fi
  sleep 1
done
[[ "$ready" == yes ]] || { docker logs "$KAFKA" >&2; exit 3; }

ready=no
for _ in $(seq 1 45); do
  if [[ "$(docker exec "$MONGO" mongosh --quiet --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null || true)" == 1 ]]; then ready=yes; break; fi
  sleep 1
done
[[ "$ready" == yes ]] || { docker logs "$MONGO" >&2; exit 4; }

if [[ "${C8B_TEST_FAIL_BEFORE_SPARK:-0}" == 1 ]]; then
  printf 'Injected C9A-FIX failure before Spark start; run_id=%s\n' "$RUN_ID" >&2
  exit 42
fi

remaining_seconds=$((DEADLINE - SECONDS))
(( remaining_seconds > 0 )) || { printf 'C8B overall deadline expired before Spark start\n' >&2; exit 124; }
timeout --signal=TERM --kill-after=10s "${remaining_seconds}s" docker run --pull=never \
  --name "$SPARK" --network "$NETWORK" \
  --label codex.task=c8b --label "codex.run=$RUN_ID" --memory 3g --cpus 2 \
  -e PYTHONDONTWRITEBYTECODE=1 -e SPARK_LOCAL_IP=127.0.0.1 \
  -e C8B_GIT_COMMIT="$GIT_COMMIT" -e "C8B_GIT_STATUS_JSON=$GIT_STATUS_JSON" \
  -v "$PWD:/workspace:ro" -v "$RUN_DIR:/demo-output:rw" \
  -w /workspace --entrypoint python3 "$IMAGE_SPARK" \
  scripts/module4/module4c_streaming_demo.py live \
  --run-id "$RUN_ID" --output-dir /demo-output \
  --mongodb-uri mongodb://c8b-mongo:27017 --database "$DATABASE" --topic "$TOPIC" \
  >"$RUN_DIR/spark-console.log" 2>&1
```

The launcher has 75-second Kafka and 45-second Mongo readiness limits and a
270-second overall deadline. The in-container experiment bounds Kafka sends,
Spark offset/watermark/bundle polling, and each CLI call. The EXIT trap calls
the same host-side `cleanup-docker` subcommand used by the cleanup tests. It
inspects exact run-specific container names, verifies both C8B ownership labels,
and records container mount identities before removal. For the Kafka image's
image-declared anonymous volumes, Docker creates 64-character volume IDs even
when the launcher specifies no `-v` option. `docker rm -f` alone leaves these
anonymous volumes behind; the cleanup helper uses `docker rm -f -v` with the
verified container ID only after confirming each volume is image-declared, not an
explicit named mount, and not referenced outside the current run. It then
inspects each captured volume ID and the exact task containers/network, with
task/run label queries as an additional check. Named, external, or unexpected
volume mounts are preserved and make cleanup fail with their identifiers.

The temporary evidence-owner helper uses a task-specific name, the same labels,
and `--rm`; it runs before the cleanup verifier so a leaked helper container is
also detected. The finalizer requires a successful cleanup receipt with all
captured volumes absent before it can preserve `status: passed`. On cleanup
failure, inspect `docker-cleanup.json`, the saved sanitized container logs, and
the listed container, network, or volume IDs. Do not use a global volume prune.
The trap preserves the original experiment exit code; cleanup or finalization
failure changes an otherwise successful run to nonzero and prevents a PASS
report. EXIT, INT, and TERM paths converge on this trap.
Setting `C8B_TEST_FAIL_BEFORE_SPARK=1` injects exit code 42 after Kafka and
MongoDB are ready; it is reserved for checking the documented failure trap.

The C9A audit found that the original C8B cleanup verified containers and
networks but missed Kafka's anonymous volumes. Twenty-one exact anonymous
volume IDs recorded by seven historical Kafka container inspections were
subsequently removed manually after confirming that no container referenced
them. The original C8B reports and their checksums are preserved unchanged;
their cleanup fields do not establish anonymous-volume cleanup. The new
`docker-cleanup.json` receipt is separate evidence from a corrected teardown.
The launcher never runs `docker compose`, never removes named project volumes,
and selects containers only after checking their exact C8B task and run labels.

The C9A-FIX2 failure-path check used run
`c8b-20261013t163130z-5dbc0328`. The Kafka broker readiness probe and MongoDB
ping both succeeded before `C8B_TEST_FAIL_BEFORE_SPARK=1` injected exit 42.
The EXIT trap retained that original exit code, while the cleanup command
returned 0. It removed the exact run containers and network, captured and
removed all three image-declared Kafka anonymous volumes, and verified each
volume ID absent. A second cleanup invocation also passed with no resources to
remove. No experiment report was created, so the intentionally failed run was
not represented as a passing experiment. The targeted before/after inventory
showed the six pre-existing `instacart` Compose containers and their three
project volumes unchanged during this check. Sanitized receipts and Docker
events are recorded under
`results/module4c-streaming-demo/c8b-20261013t163130z-5dbc0328/`.

An earlier Docker event window showed the pre-existing `instacart` Compose
containers start and then stop with SIGTERM/SIGKILL around 16:03 UTC. The event
history does not identify the initiating process. The documented launcher and
cleanup commands use run-specific C8B names and labels and do not invoke
Compose; the FIX2 before/after inventory confirms those project resources were
not changed during the controlled failure test. The short-lived evidence-owner
helper in that test was auto-removed; Docker events also recorded injected
Compose metadata on it for project `c7b-final-20261013` and service
`spark-master`. The source of that metadata remains unknown.

## Validation

Use the established pinned Spark image; do not install host dependencies:

```bash
docker run --rm --network none \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$PWD:/workspace:ro" -w /workspace \
  --entrypoint python3 instacart-spark:3.5.5 \
  -m pytest -q -p no:cacheprovider \
  tests/test_module4c_streaming_demo.py \
  tests/test_module4c_responsiveness.py \
  tests/test_module4c_cli.py \
  tests/test_module4c_service.py

docker run --rm --network none \
  -v "$PWD:/workspace:ro" -w /workspace \
  --entrypoint ruff instacart-spark:3.5.5 \
  check --no-cache \
  scripts/module4/module4c_streaming_demo.py \
  tests/test_module4c_streaming_demo.py
```

The JSON report separates Kafka acknowledgement times, Spark micro-batch
durations, wall-clock delay from the final watermark-event acknowledgement to
atomic bundle observation, and Module 4C CLI wall time. Event-time distance is
never used as a latency measurement. The Spark progress and sink JSONL logs,
sanitized service logs, and report checksum are saved under
`results/module4c-streaming-demo/<run-id>/`.

The report documents that recommendation accuracy and production latency were
not measured and that a streaming checkpoint restart was not exercised. The
existing source-generation race coverage remains dependent on I2 C6; this demo
validates a seeded, ready synthetic ALS/graph snapshot and does not extend that
race coverage. C8B ends before full-data evaluation, API/dashboard work, or
deployment work. Its files and evidence are for review and are not committed by
the experiment runner.

The Spark image's default account has a valid passwd entry for Java and Hadoop.
Its repository bind mount remains read-only, and its only writable host mount is
the unique run evidence directory. After stopping and removing the run's
services, a short-lived `--network none` helper mounts only that evidence
directory and restores ownership to the invoking user. The stream sets
`spark.jars.ivy` to a dedicated writable path under `/tmp/c8b`; all required
Spark Kafka connector jars are already present in the pinned image.
