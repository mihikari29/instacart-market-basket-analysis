# Full-data Validation and Batch Benchmark Report

## Experiment summary

Full public-dataset validation was executed on 2026-10-03 in an isolated Ubuntu
24.04 environment. The experiment fetched and verified the source archive,
cleaned the data, regenerated the complete deterministic event feed, promoted it
atomically into HDFS, executed Module 2 analytics and benchmarks, published the
serving outputs to MongoDB, and then verified the resulting counts and contracts.

Classification: **FULL PUBLIC DATASET / ISOLATED RUNTIME VALIDATION**.

| Configuration | Value |
|---|---|
| Experiment ID | `20261003T080535_74b9d443` |
| Timestamp | `2026-10-03T08:05:35Z` |
| Platform | Ubuntu 24.04, Linux 6.17.0-1022-azure |
| Spark / Java / Python | 3.5.5 / 17.0.14 / 3.10.12 |
| Spark resources | Standalone, 2 cores, one 1 GiB executor |
| Shuffle partitions | 32 |
| Repetitions | 1 warm-up + 3 measured trials per arm |
| Generation | seed 42, 13-week scatter, 2,000 users per batch |
| Source-code digest | `88d94941bd3263ed49f238d023f38d87527ed1162a7dd50466857c3e078270a1` |
| Automated tests | 51 passed in 30.72 seconds |

## Dataset and reconciliation

| Dataset | Rows |
|---|---:|
| Orders | 3,421,083 |
| Prior facts | 32,434,489 |
| Train facts | 1,384,617 |
| Products | 49,688 |
| Users | 206,209 |
| Aisles / departments | 134 / 21 |
| Reconstructed events | 33,819,106 |

Source facts, the local partitioned feed and the Spark read from HDFS each
contained **33,819,106 events**. Exact fact/event multiset equality, key and
relationship checks, date/epoch consistency and partition integrity all passed.
The feed spans 2024-01-01 through 2025-03-31 in **456 daily partitions / 456
files**; entity-keyed generation is invariant to batch size and produced zero
per-user monotonicity violations.

## Module 2 technical acceptance

- Historical analytics use `order_products__prior` only; the train set remains
  untouched as Module 4 ground truth.
- `_sources.json` and `_handoff.json` checksums and counts match the staged data.
- User features were written to `/instacart/features/user_features`.
- Spark SQL analytics, rolling trends and explicit department/hour pivot and
  `unpivot` completed; both pivot totals equal **32,434,489**.
- MongoDB publication produced **49,677 product** and **21 department**
  documents. Eleven catalog products have no prior purchases.
- Sort-Merge Join and Broadcast Hash Join plans were asserted and returned equal
  outputs. Partition filters and cached/uncached aggregates also returned equal
  outputs.

Observed prior-only reorder rate was **58.9697%** and average basket size was
**10.0889**. The most-purchased products were Banana (472,565), Bag of Organic
Bananas (379,450) and Organic Strawberries (264,683). Calendar trends are based
on synthesized replay time and must not be interpreted as original purchase
dates.

## Benchmark methodology and results

AQE and automatic broadcast selection were disabled so each join arm exercised
the requested physical strategy. Each experiment used one warm-up and three
measured repetitions with alternating or rotating arm order. Equivalent arms
were accepted only after operator, row-count and checksum assertions. Spark task
metrics came from the event log.

| Experiment / arm | Median seconds |
|---|---:|
| Join: Sort-Merge | 17.581934 |
| Join: Broadcast Hash | 2.304046 |
| Scan: all dates | 3.497363 |
| Scan: date-column filter | 1.666808 |
| Scan: partition-column filter | 0.074906 |
| Aggregate: uncached | 2.110284 |
| Cache materialization | 2.175583 |
| Aggregate: cached reuse | 0.088085 |

The equivalent date and partition filters both returned **177,739 events**.
Partition pruning substantially reduced the scanned input and median latency for
this seven-day query. Broadcast Hash Join was faster than Sort-Merge Join in this
single-executor configuration. Cache materialization is reported separately:
reuse is fast, but the build cost must be included when assessing total benefit.

These timings compare controlled arms on one warm system. OS and HDFS page caches
were not flushed, the all-date scan has different scope from the filtered arms,
and the measurements do not establish cluster-scale performance.

## Evidence files

- [current-hardening-2026-10-03.json](current-hardening-2026-10-03.json): compact
  configuration, reconciliation and benchmark record.
- [summary.json](summary.json): detailed validation, trial timings, checksums,
  physical plans and Spark task metrics.
- [analytics.json](analytics.json): observed full-data aggregates.
- [manifest.json](manifest.json): generated feed identity and calendar statistics.
- [source-receipt.json](source-receipt.json): source archive/file SHA-256 values
  and row counts.
- [environment.txt](environment.txt), [compose-status.txt](compose-status.txt) and
  [execution-checks.txt](execution-checks.txt): selected runtime, health and
  acceptance output.

## Limitations

- The experiment used one isolated host, one two-core Spark executor and
  approximately 16 GiB RAM; it is not a multi-node scaling study.
- Raw source data and large transient logs are not stored with this compact
  evidence.
- Storage publication assumes a single writer during an execution.
- The complete experiment took approximately 19 minutes, including acquisition,
  generation, image build, tests and cleanup.
- Module 3 streaming validation is reported separately, and Module 4 has not yet
  been implemented.

See the [technical runbook](../../progress.md) for reproduction commands.
