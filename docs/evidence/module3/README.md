# Module 3 Validation Evidence

Module 3 is implemented and technically validated. The measurements below are
bounded correctness and development experiments, not production-capacity claims.

Machine-readable records:

- [final-release-validation-2026-10-03.json](final-release-validation-2026-10-03.json)
- [hardening-validation-2026-10-03.json](hardening-validation-2026-10-03.json)
- [local-validation-2026-10-02.json](local-validation-2026-10-02.json) — earlier
  development measurements, retained as historical evidence

## Validation environment

| Configuration | Value |
|---|---|
| Spark / Java / Python | 3.5.5 / 17.0.14 / 3.10.12 |
| Spark resources | Standalone, 2 cores, 8 shuffle partitions |
| Kafka | 1 broker, 4 partitions for the primary smoke |
| Windows | C30=30m, C120=120m, slide=5m |
| Event-time watermark | 10 minutes |
| Trend weights | 0.7 short-term / 0.3 long-term |
| Serving | Top-20 per finalized window |
| MongoDB retention | 604,800 seconds (7 days); explicit zero disables TTL |
| Source-code digest | `3fb1c0240e873cb0700320a236caef73761b9f9ccbfbf2713d8a1fae264da8b4` |

The Kafka source uses `failOnDataLoss=true` by default. Relaxed offset-loss
behavior requires an explicit environment or CLI opt-out.

## Streaming topology

```text
Kafka events
  -> schema/domain validation + record_quality metrics
  -> event-time watermark
  -> event_id deduplication within watermark
  -> one stateful 120-minute sliding aggregation (C30 + C120)
  -> append-mode finalized window
  -> full-population normalization + deterministic ranking
  -> configurable Top-K
  -> replace complete Mongo finalized-window snapshot + idempotent upserts
```

Append mode is intentional: ranking is performed only after a window is
finalized, when its complete product population is available. `product_id` is
the deterministic tie-break for equal scores. Top-K filtering occurs after
normalization and ranking.

## Correctness validation

| Requirement | Observed result |
|---|---|
| Automated suite | 51 tests passed; Ruff, Compose and whitespace checks passed |
| Schema/domain validation | Malformed and domain-invalid records counted and excluded |
| Poison record handling | Poison input counted as invalid without entering stateful aggregation |
| Duplicate handling | Repeated source `event_id` dropped within watermark state |
| Fault identity | Duplicate retains the source ID; late copy receives a distinct deterministic ID |
| Watermark behavior | Within-watermark event accepted; beyond-watermark event dropped |
| Finalized aggregation | Selected result `C30=2`, `C120=2` in the deterministic scenario |
| Full-population ranking | Normalization and deterministic tie handling asserted before Top-K |
| MongoDB snapshot | Replay converges without duplicate window/product keys or ranks |
| TTL | Seven-day default, index reconfiguration and explicit disable tested |
| Checkpoint recovery | Same query identity; batch sequence continues; no replay from earliest |

## End-to-end streaming validation

A clean isolated environment regenerated a bounded Module 1 feed, built the
pinned Spark runtime, started Spark, Kafka and MongoDB, validated connectivity,
executed the real Kafka → Spark → MongoDB path, verified checkpoint recovery and
late-data behavior, measured a short throughput workload, captured diagnostics
and stopped the temporary services.

In the final 5,000-event smoke, the producer attempted and acknowledged all
5,000 events with zero failures. Spark read 5,000 valid rows, wrote 79,970 Mongo
documents, produced no duplicate window/product or window/rank keys, reported no
invalid scores and kept every window within Top-20. Observed processing rate was
374.20 rows/s for this bounded workload.

## Bounded 50k measurement

Command shape:

```bash
python3 scripts/module3_smoke.py --events 50000 --duration-seconds 120 \
  --feed data/synthesized/hardening_500 --top-k 20
```

| Metric | Result |
|---|---:|
| Timestamp | 2026-10-03T06:37:19Z |
| Kafka attempted / acknowledged / failed | 50,000 / 50,000 / 0 |
| Producer elapsed / acknowledged rate | 4.621 s / 10,821.22 events/s |
| Reader strategy | external merge; 65,536-row chunks; 2 initial runs |
| Spark total input / valid / invalid | 50,000 / 50,000 / 0 |
| Processed rows/s | 3,203.90 |
| Input trigger / addBatch | 15,605 ms / 14,098 ms |
| Peak aggregate / dedup state rows | 1,174,010 / 50,000 |
| Watermark drops in this on-time workload | 0 |
| Finalized sink batch | 62,021.821 ms |
| Finalized windows / Mongo documents | 35,701 / 580,342 |
| Maximum documents per window | 20 |
| Duplicate window/product keys / ranks | 0 / 0 |
| Invalid scores / windows above Top-K | 0 / 0 |

Kafka was preloaded before the streaming query started, so the reported input
rows/s was 0.0 while the processed rate remained meaningful. Each source event
may contribute to as many as 24 overlapping five-minute windows; this state and
output fan-out, plus the single-machine sink, dominates the bounded result.

## Checkpoint recovery

The `recovery-smoke` experiment used two separate Spark processes sharing one
checkpoint. Process 1 consumed three records. After two new records were added,
process 2 consumed only those two records, retained the same query identity,
continued the batch sequence and did not restart from Kafka's earliest offsets.
The validated scenario produced 109 Mongo documents and zero duplicate groups.

## Late-data and watermark validation

The deterministic scenario submitted three input records: two valid and one
poison/invalid. One valid record duplicated an existing `event_id` and was
dropped. An event at 00:15 was accepted while the watermark was 00:10. After the
watermark advanced to 02:50, a subsequently submitted event at 00:18 was dropped
from watermark-managed state. The selected finalized result remained `C30=2`
and `C120=2`.

## Throughput measurements

Each workload used one warm-up and three measured trials.

| Producer target | Achieved producer rate | Spark processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 100/s | 100.74/s | 40.55 | 2,383 |
| 500/s | 497.97/s | 46.86 | 2,123 |
| Unbounded bulk | 5,695.47/s | 47.48 | 2,106 |

The partition experiment used a real temporary Kafka topic for each arm and
verified broker metadata before measurement.

| Kafka partitions | Spark observed | Processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 1 | 1 | 51.57 | 1,939 |
| 2 | 2 | 60.86 | 1,643 |
| 4 | 4 | 47.73 | 2,095 |
| 8 | 8 | 31.26 | 3,199 |

Two partitions performed best in this small workload. Scheduling and state
management overhead dominated at four and eight partitions, so these results do
not imply that increasing partitions always improves throughput.

## Limitations

- All live measurements are bounded checks on a single machine or isolated
  host, not production-scale benchmarks.
- One Kafka broker validates consumer partition parallelism, not broker high
  availability.
- Output is finalized-window trending, not an open-window live leaderboard.
- Sliding windows amplify state and output; the 50k experiment reached 1,174,010
  aggregate-state rows.
- Mongo writes use bounded driver-side `toLocalIterator` and bulk operations;
  Top-K bounds serving rows but this is not a distributed Mongo writer.
- Snapshot replacement is logically idempotent, but delete + upsert is not
  transactionally atomic for concurrent readers. A reader may briefly observe
  an empty or partial snapshot; immutable versioned snapshots or a transaction
  would be appropriate for stricter production serving.
