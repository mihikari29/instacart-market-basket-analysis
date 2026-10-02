# Module 3 Evidence (Speed Layer)

> Status: **Implemented and validated locally with Docker Compose on 2026-10-02.**

Module 3 uses the pinned Spark 3.5.5 / Java 17 image. The host Python/Java
installations are not used for Spark execution. The checked-in CI workflow
repeats a bounded version of the live validations below. Its current execution
status is reported by the pull-request checks rather than this local snapshot.

## Local validation results

| Gate | Measured result | Local artifact |
|---|---|---|
| Module 3 tests | `10 passed in 11.91s` | Docker pytest output |
| Complete repository tests | `30 passed in 24.62s` | Docker pytest output |
| Kafka connectivity | host listener `localhost:9092`; Docker listener `kafka:29092`; canonical topic has 4 partitions | `results/module3/validate/` |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.5` loaded by a live query | `results/module3/smoke/` |
| Live end-to-end smoke | 50,000 produced and 50,000 processed; 1,111,592 finalized Mongo documents | `results/module3/smoke/smoke_evidence.json` |
| Watermark / late data | within-watermark event accepted; beyond-watermark event dropped; selected output count remained 2 | `results/module3/late/*/late_data_evidence.json` |
| Checkpoint recovery | first process read 3 rows; second process with the same checkpoint read only 2 new rows; query ID unchanged | `results/module3/recovery/recovery_evidence.json` |
| Duration zero | remained active until interactive Ctrl+C, then stopped with exit code 0 | `results/module3/duration-zero/` |
| Throughput | 100/s, 500/s, and bulk producer arms; 1 warm-up + 3 measured trials each | `results/module3/benchmark-throughput/*/summary.json` |
| Kafka partitions | broker and Spark both observed actual 1/2/4/8 partition topics; 1 warm-up + 3 runs each | `results/module3/benchmark-partitions/*/summary.json` |

Generated `results/module3/` data is intentionally gitignored because Spark
event logs and checkpoints are large. A compact checked-in measurement summary
is in [`local-validation-2026-10-02.json`](local-validation-2026-10-02.json).

## Architecture validated

The streaming plan contains one stateful 120-minute sliding aggregation with a
5-minute slide and 10-minute watermark. `purchase_count_30m` is calculated by
conditionally counting events in `[window_end - 30 minutes, window_end)`.
Append mode emits finalized windows. Min/max normalization and deterministic
`row_number` ranking run only on the static DataFrame provided by
`foreachBatch`.

Progress JSONL records each `batchId` once and includes input/processing rates,
`durationMs`, watermark, state totals/updates/removals/drops, source offsets,
and sink metadata.

## Measured summaries

The 50,000-event smoke producer achieved 13,021.60 events/s. Spark consumed all
records in one data batch at 4,351.61 processed rows/s with an 11,489 ms trigger.
The subsequent finalized-window sink batch took 82,434 ms; this large fan-out is
expected because each source row can contribute to 24 five-minute windows.

Throughput trial medians:

| Producer target | Measured producer rate | Spark processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 100/s | 100.74/s | 40.55 | 2,383 |
| 500/s | 497.97/s | 46.86 | 2,123 |
| unbounded bulk | 5,695.47/s | 47.48 | 2,106 |

Partition trial medians:

| Actual partitions | Spark-observed partitions | Processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 1 | 1 | 51.57 | 1,939 |
| 2 | 2 | 60.86 | 1,643 |
| 4 | 4 | 47.73 | 2,095 |
| 8 | 8 | 31.26 | 3,199 |

These are small development measurements, not production capacity claims.

## Limitations

- GitHub Actions results are intentionally not represented in this local
  evidence snapshot; use the pull-request checks for current CI status.
- The single Kafka broker validates consumer partition parallelism, not broker
  high availability.
- Kafka duplicate injection represents duplicate input and increments counts
  more than once. Mongo idempotent upsert prevents duplicate output documents;
  it does not deduplicate source events. The Proposal does not require source
  event deduplication.
- TTL remains disabled by default. Pass `--ttl-seconds 86400` only when the
  optional cleanup behavior is desired.
