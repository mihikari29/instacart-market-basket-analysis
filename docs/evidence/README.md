# Observed fixture results

This is the historical local fixture report. The later [full-data report](full/README.md)
closes the Docker/HDFS and full-data gates below; code has also been pushed to GitHub.

Run: 20260925T154536_9ad62d33. Status: passed.

Windows local[2], Spark 3.5.5, Java 17.0.20.1, Python 3.13.2.
One warm-up and three measured trials per arm; eight shuffle partitions. Raw trials,
checksums, physical plans and task metrics: [fixture-summary.json](fixture-summary.json).

Input: 16 orders, 24 prior facts, 12 train facts, 36 events, four users, six products,
three aisles and two departments. Source/local/Spark events all equal 36. Exact
fact/event multiset equality and date/epoch partition integrity passed. This is
synthetic test data, not a sample downloaded from Instacart.

Prior analytics: reorder rate 0.5; average basket size 3; four user-feature rows.
MongoDB 7.0.16 contained six product and two department documents. A second run
removed an injected stale product ID while preserving an unrelated collection.
The seven-day window 2024-01-01 through 2024-01-07 contains 24 of 36 events.

| Experiment | Arm | Median seconds | Median task input bytes | Median shuffle read/write bytes |
|---|---|---:|---:|---:|
| join_benchmark | sort_merge | 0.991051 | 6,296 | 1,278 / 1,278 |
| join_benchmark | broadcast_hash | 0.421334 | 6,296 | 180 / 180 |
| partition_pruning | full_scan | 0.217934 | 21,321 | 130 / 130 |
| partition_pruning | date_filter | 0.278986 | 23,071 | 125 / 125 |
| partition_pruning | partition_filter | 0.224673 | 14,214 | 129 / 129 |
| cache_benchmark | uncached | 0.425186 | 6,296 | 180 / 180 |
| cache_benchmark | cached_reuse | 0.121881 | 1,604 | 0 / 0 |

Cache materialization median: 0.399689 s (additional to reuse).

Broadcast reduced join shuffle on this fixture. Date predicates on partition
columns reduced task input bytes by about 38.4% versus the equivalent date-column
filter. The full scan was slightly faster than pruning here despite reading more
bytes; scheduling overhead dominates such tiny inputs. Cache input bytes include
in-memory task input and must not be labelled disk bytes. None of these timings
establishes a full-scale speedup. Exact physical operators and output equivalence
were asserted before results were accepted.

## Execution gates at the time of this fixture run

- Full local CLI all: PASS, including analytics, Parquet features, MongoDB, joins, pruning, caching and event logs.
- Full automated test suite: PASS, 14 tests in 104.25 seconds (11 unit/regression + 3 Spark integration).
- Compose configuration: PASS with standalone Docker Compose v2.35.1.
- Spark image tag/digest: verified via authenticated public Docker Registry response (HTTP 200).
- Docker image build, container/HDFS smoke and full Instacart execution: NOT RUN; no Docker/WSL or dataset.
- GitHub remote delivery: blocked by unavailable authentication; see delivery report.

## Host-only runtime provenance

Portable Eclipse Temurin Java 17 downloaded from Adoptium and checked against its
published SHA-256. PySpark 3.5.5 and test helpers were isolated outside the checkout.
Windows Hadoop helpers came from cdarlint/winutils at commit
7386986d5d8a079b5cd4464f4599766dd27e7d13 (Hadoop 3.3.5). These are local test
prerequisites only; production uses the pinned Linux Spark image. No binaries
or event logs are committed.

## Self-review

Correctness, Spark operators, repeated experiments, task metrics and fixture
reproducibility have executable evidence. The Docker and full-data gates were still
open at this fixture run and are now closed by the linked full-data execution. Storage snapshots have documented publication windows and are intended for
single-writer course runs. No 10/10 or full-completion claim is made.
