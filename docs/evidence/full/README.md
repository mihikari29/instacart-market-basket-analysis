# Full-data execution: passed

[GitHub Actions run 36162730659](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/36162730659) completed successfully on
2026-09-25 at 17:04 UTC (2026-09-26 in Asia/Saigon). Executed source commit:
`bf8aeda076cda72cc6e59a8e79154ab9235c68a3`.
Production Python digest: `d3e76ca21b4bf76cbd9c20e0c8c75f7f431fbb95ce750eb7c7ac6fdf09e21e38`.
The digest was independently matched against committed Python source bytes.
Later documentation commits do not change this executed source.

## Acceptance evidence

- Public version-1 Kaggle mirror downloaded and all six source counts verified.
- Corrected cleaning and full feed generation: 206,209 users, 33,819,106 events,
  seed 42, 13-week scatter, 2,000 users/batch, no user limit; zero monotonic violations.
- Docker build and **15 tests passed in 16.94 seconds**.
- Source = partitioned local = Spark-read HDFS events = **33,819,106**;
  **456 daily partitions / 456 files**, exact fact/event multiset equality,
  keys, relationships and date/epoch consistency passed.
- Canonical Docker runner completed analytics, SQL, pivot, seven-day trends,
  HDFS user features, all three benchmark families and serving publication.
- MongoDB verification: **49,677 product** and **21 department** documents, matching
  aggregate Parquet and run ID. Eleven catalog products have no prior purchases.
- HDFS, Spark master/worker and MongoDB healthchecks passed. The final check printed
  `FULL DATA + STANDALONE SPARK + HDFS + MONGODB: PASS`.

| Source | Rows |
|---|---:|
| Orders | 3,421,083 |
| Prior facts | 32,434,489 |
| Train facts | 1,384,617 |
| Products | 49,688 |
| Aisles / departments | 134 / 21 |

Prior-only reorder rate: **58.9697%**; average basket size: **10.0889**.
Top purchased products: Banana (472,565), Bag of Organic Bananas (379,450),
Organic Strawberries (264,683). Synthetic calendar trends are replay simulations.

## Measured benchmarks

Spark 3.5.5, Java 17.0.14, Python 3.10.12; standalone master and HDFS containers.
One two-core, 1 GiB executor; 32 shuffle partitions; AQE and automatic broadcast
disabled. One warm-up and three measured repetitions per arm, alternating/rotating
order, asserted physical operators and equal output checksums for equivalent arms.

| Experiment / arm | Median seconds | Task input bytes | Shuffle read / write bytes |
|---|---:|---:|---:|
| Join: Sort-Merge | 14.602316 | 70,562,653 | 289,395,061 / 289,395,061 |
| Join: Broadcast Hash | 2.083508 | 70,562,653 | 2,946 / 2,946 |
| Scan: all dates | 2.511430 | 216,222,873 | 1,701 / 1,701 |
| Scan: date-column filter | 1.228454 | 52,153,677 | 1,416 / 1,416 |
| Scan: partition-column filter | 0.064017 | 1,181,369 | 140 / 140 |
| Aggregate: uncached | 1.858871 | 70,562,653 | 2,946 / 2,946 |
| Aggregate: cached reuse | 0.085279 | 10,316 | 0 / 0 |

Broadcast was **7.01 times faster** than Sort-Merge in this configuration.
Equivalent date filters both returned 177,912 events for January 1–7, 2024.
Partition pruning reduced task input bytes by **97.73%** and median runtime by a
factor of **19.19** versus the date-column filter. The all-date scan has different
scope and is not used as an equivalent-query speedup comparison.

Cache materialization costs an additional **1.833358 seconds** (median); reuse is
useful across repeated consumers. Cached task input describes cache batches, not
disk reads or semantic department-row counts. Build cost must be included when
assessing total benefit. OS/HDFS page caches were not flushed.

## Files and limits

- [summary.json](summary.json): configuration, validation, trial timings, checksums,
  physical plans and Spark task metrics.
- [analytics.json](analytics.json): observed full-data aggregates.
- [manifest.json](manifest.json): generated feed identity and calendar statistics.
- [source-receipt.json](source-receipt.json): archive/file SHA-256 and source counts.
- [environment.txt](environment.txt), [compose-status.txt](compose-status.txt):
  hosted runtime and healthy services; [execution-checks.txt](execution-checks.txt)
  retains selected test/staging/serving log lines.

Absolute checkout prefixes are normalized to `<repository>`; numeric evidence is
unchanged. The runner had approximately 16 GiB RAM; this is a single hosted Linux
machine, not a multi-node scaling result or a permanent deployment. Containers
were stopped after the run. Raw data and giant logs are not committed; GitHub's
artifact has 14-day retention, while these compact files remain in Git history.
The complete workflow took approximately 19 minutes, including data acquisition,
generation, Docker build, tests and cleanup. Module 3 streaming and Module 4 ML
remain separate work. See [reproduction instructions](../../full_execution.md).
