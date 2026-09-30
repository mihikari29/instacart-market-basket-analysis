# Module 3 Evidence (Speed Layer)

> Status: **Implementation complete; live-cluster validation pending in CI.**

This folder collects acceptance evidence for the Spark Structured Streaming
speed layer (Proposal §17–22, syllabus W10). Layout mirrors
[`docs/evidence/`](../README.md) used by Module 2.

## Expected deliverables (Definition of Done — Proposal §42)

| Gate | Artifact |
|---|---|
| Kafka connectivity + topic metadata | `results/module3/<run_id>/summary.json` → `kafka_validation` |
| Kafka → Spark Structured Streaming runs | `streaming_progress.jsonl` per `benchmark-throughput`/`run` |
| JSON parse + poison-pill drop | covered by `tests/test_module3.py::test_poison_pill_*` |
| Event-time window (30 / 120 minutes, 5-minute slide) | `tests/test_module3.py::test_window_short_aggregation_counts` |
| Watermark 10 minutes active | `streaming_progress.jsonl` records dropped windows |
| Late-event handling | `late-demo` subcommand + producer `--inject "late:0.05,dup:0.02"` |
| Trending score computed | `trend_score`/`trend_rank` fields in `realtime_trending` Mongo collection |
| Checkpoint recovery | checkpoint dir under `results/module3/<run_id>/checkpoint/` |
| Idempotent MongoDB upsert | `sink_mongo.write_mongo` uses `_id = "window_end__product_id"`, `upsert=True` |
| Throughput benchmark (Proposal §15.4) | `results/module3/<run_id>/summary.json` → `throughput_benchmark` |
| Kafka partition sweep (Proposal §15.5) | `results/module3/<run_id>/summary.json` → `partition_benchmark` |

## Live cluster run

A full run is triggered automatically on pushes to
`module3-speed-layer` via [module3-full.yml](../../.github/workflows/module3-full.yml)
and uploads `results/module3/*` as an artifact for 14 days. Once a successful
run is recorded, copy `summary.json` and `streaming_progress.jsonl` into
`full/` and link the GitHub Actions run URL here.

## Local / fixture evidence

The unit-level evidence for schema parsing, window alignment, normalization
bounds, trend-rank uniqueness and weight-sum invariant is captured by
`tests/test_module3.py`. Numbers will be filled in after the first
`docker compose run --rm --no-deps --entrypoint python3 module3 -m pytest -q
tests/test_module3.py` execution.

## Notes / known limitations

- The partition sweep requires recreating the Kafka topic between arms
  (`kafka-topics.sh --create --partitions n`); this is documented in the
  `partition_benchmark.note` field of `summary.json` and is driven from CLI.
- Single-broker KRaft (compose.yaml) cannot demonstrate broker-level HA; the
  benchmark instead exercises consumer-side parallelism (number of partitions
  pulling in parallel from a single broker).
- TTL on `realtime_trending.updated_at` is **disabled by default**
  (`ttl_seconds = 0`, see Q-B); pass `--ttl-seconds 86400` to demo the W4
  eventual-consistency / automatic window-cleanup behavior.
