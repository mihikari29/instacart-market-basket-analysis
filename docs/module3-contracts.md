# Module 3 streaming contracts

This document describes the current Module 3 implementation. The [Module 3
evidence record](evidence/module3/README.md) preserves dated results; the
[Docker guide](docker.md) owns operating commands. The approved requirements
remain in [Proposal §§9, 17–22, 35–38](Proposal.md).

## Role and data flow

Module 3 is the speed layer. It consumes Module 1 Kafka purchase events,
validates and aggregates them by event time, then publishes finalized trending
windows to MongoDB. The legacy rows preserve the mutable compatibility output
contract. An optional immutable bundle collection supplies verified trend input
to Module 4C and finalized-trend reads to Module 4D.

```text
Module 1 synthetic feed -> Kafka -> Module 3
                                  ├─> `realtime_trending`
                                  │    mutable compatibility output
                                  └─> optional `realtime_trending_windows`
                                         ├─> Module 4C verified trend input
                                         │       + ready A/B snapshots
                                         │       -> Hybrid ranking/publication
                                         └─> Module 4D finalized-trend API view
```

The ordinary generated stream contains both prior and train orders. A stable
event time or a Module 3 cutoff alone does not make this mixed replay suitable
for official-train recommendation evaluation. Module 4C quality evaluation
needs a separately justified leakage-safe chronology and source provenance.

## Kafka event contract and provenance

The Kafka topic defaults to `instacart-purchase-events`; the producer key is
`user_id`. Its JSON fields are defined in
[`src/module3/schemas.py`](../src/module3/schemas.py): `event_id`, order/user/
product identifiers, cart position, reorder flag, aisle/department identifiers,
order weekday/hour, `event_time_epoch_ms`, `event_time_iso`, and
`ingestion_time_epoch_ms`.

`event_time_epoch_ms` is synthetic replay time derived from order sequence and
basket position. It is the Spark event-time input; it is not an original
Instacart purchase timestamp. The producer adds `ingestion_time_epoch_ms` at
send time as a wall-clock arrival timestamp. Module 1's local generated Parquet
also has `synthetic_date`; that field is not part of the Kafka schema.

Use the exact provenance phrase **“Synthetic event time for replay simulation”**
when presenting these timestamps as project-generated data. The source scope
stored in a bundle is descriptive metadata, not proof that the feed excludes
train events.

## Parsing, validation, and offset loss

[`parse.py`](../src/module3/parse.py) parses JSON against the typed schema,
labels invalid rows, and exposes input/valid/invalid counts as `record_quality`
progress metrics. It rejects a missing or blank `event_id`; missing or
nonpositive `order_id`, `user_id`, `product_id`, or `add_to_cart_order`; weekday
outside 0–6; hour outside 0–23; invalid/nonpositive event or ingestion epoch;
and an invalid ISO timestamp. Invalid rows are excluded before stateful
aggregation.

Kafka `failOnDataLoss` defaults to `true` in
[`config.py`](../src/module3/config.py). If a requested offset is unavailable,
the query fails instead of silently skipping it. Relaxed behavior requires an
explicit `--allow-data-loss` or `MODULE3_FAIL_ON_DATA_LOSS=false` opt-in. When a
checkpoint is reused, Spark restores offsets and state from that checkpoint;
changing `startingOffsets` does not reset it.

## Event-time windows and ranking

The default configuration in [`config.py`](../src/module3/config.py) is a
30-minute short count, 120-minute long window, five-minute slide, ten-minute
watermark, weights 0.7/0.3, Top-20, and ten-second processing trigger.

[`trending.py`](../src/module3/trending.py) applies the watermark and
deduplicates `event_id` within watermark-bounded state. This is bounded state,
not a permanent uniqueness registry. One stateful 120-minute sliding
aggregation computes `purchase_count_120m` and a conditional
`purchase_count_30m` for events in the final 30 minutes of each long window.

The query uses append mode, so `foreachBatch` receives finalized windows. For
each window, the static batch normalizes each count by its full represented
product population using min-max normalization; a degenerate equal-count range
maps to zero. The score is:

```text
trend_score = 0.7 × normalized_C30 + 0.3 × normalized_C120
```

Rows are ordered by descending score and ascending `product_id` for
deterministic ties. Configured Top-K filtering occurs after ranking. This is a
finalized-window result, not a continuously refreshed open-window leaderboard.

## Two MongoDB publication contracts

| Contract | Collection | Identity and behavior | Retention and guarantee |
|---|---|---|---|
| Legacy mutable rows | `realtime_trending` | Deterministic window/product document identity; unique window/product and window/rank indexes; complete window is deleted before Top-K upserts | Seven-day default TTL on `updated_at`; replay converges, but delete/upsert is not transactionally atomic for readers |
| Immutable bundles | `realtime_trending_windows` | One complete ranked document per finalized window and source namespace; insert-only upsert accepts identical retries and rejects conflicting content | Optional `expires_at` plus TTL index; a single document is atomic, while a multi-window publication is not one transaction |

The legacy writer provisions its own ranking and TTL indexes. When bundle
publication is enabled, [`trend_bundle.py`](../src/module3/trend_bundle.py)
maintains the absolute-expiration TTL index on `expires_at`; the default
retention is seven days, and `ttl_seconds=0` disables bundle expiry. The Module
4D API's selection query also needs a compatible
`{source_namespace: 1, window_end: -1}` index. The API checks and hints that
index but does not create it; an administrator provisions it using the [Docker
guide's index instructions](docker.md#required-dashboard-indexes). The raw
Module 4C CLI reader does not perform the API's index preflight.

A bundle records schema version 1, stable `source_namespace`, descriptive
`source_scope`, window bounds, Top-K rows, `config_hash`, `content_sha256`,
publication time, and optional expiry. Its identity combines a digest of the
namespace with the UTC window end. Hashes cover canonical logical content and
ranking configuration; they do not certify the source data's train/prior
eligibility.

MongoDB TTL is based on processing-time expiry (`expires_at`). Module 4C
selects the newest eligible bundle document whose event-time `window_end` is at
or before the explicit UTC cutoff, validates it, and fails if that newest
document is corrupt. An optional maximum event age is a separate freshness
policy. TTL, event-time eligibility, and optional event-age rejection answer
different questions.

## Checkpoints, retries, and failure boundaries

[`stream.py`](../src/module3/stream.py) stores the current state contract under
the configured output base's
`checkpoint/<checkpoint-id>/ingestion-latency-v1` directory. The default ID is
the run ID; `--checkpoint-id` allows a later process to reuse that versioned
checkpoint deliberately. The version marker prevents restoring an older
unversioned state schema after adding the ingestion timestamp aggregate; such a
checkpoint fails closed and remains untouched. Recovery requires the same
versioned directory, Kafka cluster/topic, and compatible query configuration.
Keep the checkpoint with its corresponding stream state. Use a new identity
for a genuinely new query; do not delete a checkpoint to repair a query without
first deciding that resetting offsets and aggregation state is intended.

The legacy Mongo replacement is retry-convergent but can be observed empty or
partial between delete and upsert. Each microbatch runs the legacy writer first;
if that write fails, bundle publication is not attempted. Bundle publication
validates windows and existing identities before writes, then uses bounded
insert-only upserts. Identical retries converge; different content for the same
namespace/window is a conflict. A completed bundle remains immutable. These
guarantees are per window, not a transaction over every window in a microbatch.

The existing Module 3 two-process recovery evidence validates checkpoint
continuation for the legacy stream. The bounded C8B receipt validates event
reconciliation and bundle/Hybrid publication, but does not demonstrate
restarting Spark from a checkpoint during bundle publication. See [Module 3
recovery evidence](evidence/module3/README.md#checkpoint-recovery) and the
[C8B archive](evidence/module4c/c8b-streaming-demo.md).

## Module 4C consumption boundary

Bundle publication is **opt-in**. `publish_trend_bundles` defaults to false;
the CLI flag is `--publish-trend-bundles`. `--trend-source-namespace` is
required when publishing bundles and must remain stable for the same logical
source/replay; do not use a run ID. `--trend-source-scope` records descriptive
provenance and defaults to `unknown_or_mixed`.

Module 4C consumes `realtime_trending_windows`, not the mutable legacy rows. It
requests an explicit namespace and timezone-aware, BSON-millisecond-aligned
`as_of_event_time`, validates the newest eligible bundle's schema, metadata,
bounds, and hash, and fails on a corrupt newest result rather than falling back
to an older one. Module 4A/4B readiness is also required for Hybrid serving.
Module 4D's API has its own exact namespace allowlist.

Each successful `sink_batches.jsonl` row also records
`ingestion_to_output_latency`: minimum, mean, and maximum milliseconds from the
producer's wall-clock `ingestion_time_epoch_ms` to output completion after the
enabled Mongo publishers finish. A sample is the latest contributing event
timestamp for one finalized product/window row, reduced with constant memory.
The producer timestamp is taken immediately before Kafka send, not at broker
append or acknowledgement. The producer and output host clocks are not
independently synchronized; a future timestamp is reported as possible clock
skew and can yield a negative measurement. This is distinct from Kafka
acknowledgement latency, Spark input/processed rates, trigger/addBatch duration,
and Mongo sink duration. It also includes the configured event-time watermark
wait for finalized windows. C8B's acknowledgement-to-bundle-visible interval
remains a separate bounded synthetic boundary measurement.

## Source, tests, and evidence

- Parsing and offset contract: [`src/module3/parse.py`](../src/module3/parse.py), [`src/module3/config.py`](../src/module3/config.py), [`tests/test_module3.py`](../tests/test_module3.py), [`tests/test_module3_config.py`](../tests/test_module3_config.py).
- Window and legacy output: [`src/module3/trending.py`](../src/module3/trending.py), [`src/module3/sink_mongo.py`](../src/module3/sink_mongo.py), and `tests/test_module3.py`.
- Immutable bundle contract: [`src/module3/trend_bundle.py`](../src/module3/trend_bundle.py), [`tests/test_module3_trend_bundle.py`](../tests/test_module3_trend_bundle.py), and [`tests/test_module3_trend_bundle_mongo.py`](../tests/test_module3_trend_bundle_mongo.py).
- Module 3 to Module 4C integration: [`src/module4/hybrid/trend_reader.py`](../src/module4/hybrid/trend_reader.py), [`tests/test_module4c_trend_reader.py`](../tests/test_module4c_trend_reader.py), and [`tests/test_module3_bundle_integration.py`](../tests/test_module3_bundle_integration.py).
- Static, bounded operating instructions: [Docker guide](docker.md).
- Dated Module 3 validation and measurements: [Module 3 evidence](evidence/module3/README.md).
- Bounded C8A/C8B observations: [Module 4C evidence index](evidence/module4c/README.md).
