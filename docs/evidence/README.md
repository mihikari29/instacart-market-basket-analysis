# Validation Evidence

This directory contains measured technical evidence for the ingestion, batch and
streaming layers. Each report records the data, runtime configuration, observed
results and limitations of its experiment.

## Full-data validation

The full public Instacart validation reconstructed **33,819,106 events** from
3,421,083 orders for 206,209 users. Source, local partitioned and HDFS event
counts matched exactly across **456 daily partitions**, with zero monotonicity
violations. Module 2 preserved the 1,384,617 train facts for later evaluation,
used 32,434,489 prior facts for analytics, verified equal pivot/unpivot totals,
published MongoDB aggregates and measured join, pruning and cache behavior.

See [full-data validation and batch benchmarks](full/README.md). Compact data is
available in [current-hardening-2026-10-03.json](full/current-hardening-2026-10-03.json),
with detailed plans and metrics in [full/summary.json](full/summary.json).

## Module 3 streaming validation

Streaming validation exercised the real Kafka → Spark Structured Streaming →
MongoDB path. It covered schema and domain validation, poison records,
`event_id` deduplication, watermark behavior, finalized-window C30/C120
aggregation, full-population ranking before Top-K, snapshot idempotency, TTL,
two-process checkpoint recovery and bounded throughput measurements.

See [Module 3 validation evidence](module3/README.md) and its machine-readable
[final validation record](module3/final-release-validation-2026-10-03.json).

## Fixture validation

The deterministic fixture contains 16 orders, 24 prior facts, 12 train facts and
36 events across four users and six products. Source, local and Spark counts are
equal; fact/event multiset and partition integrity checks pass. The fixture also
verifies Spark operators, output equivalence, Mongo replacement behavior and
repeatable join, pruning and cache measurements.

- [Fixture summary](fixture-summary.json)
- [Fixture analytics](fixture-analytics.json)
- Historical fixture environment: Windows local mode, Spark 3.5.5, Java 17.0.20.1,
  Python 3.13.2, eight shuffle partitions, one warm-up and three measured trials.

Fixture timings demonstrate correctness and instrumentation on a tiny synthetic
dataset; they are not full-scale performance claims.
