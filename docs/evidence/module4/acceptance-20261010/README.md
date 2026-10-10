# Module 4A–4B acceptance — 10/10/2026 (Asia/Bangkok)

Initial audit: **20/21 checklist gates closed**. Full-data A/B publication and
read-back remained open. The 09/10 final attempt had no final summary and WSL
was stopped; its original artifacts remain under
`results/module4/full-ab-20261009-final/`.

Status: **complete and validated on the full dataset**, including same-version full-data replay.

- Initial repository regression before the OOM fix: **91 passed, zero skipped**, five PySpark
  warnings, 158.63 seconds. Ruff passed.
- Fresh standalone smoke passed: Spark 3.5.5, Scala 2.12.18, GraphFrames Python
  0.6/JAR 0.8.3, isolated-vertex PageRank, HDFS round-trip and MongoDB ping.
- `full-ab-20261010-first` passed input validation: 3,421,083 orders,
  32,434,489 prior facts, 1,384,617 train facts, 49,688 catalog products,
  206,209 prior users and 131,209 official-train users; hashes match staged receipts.
- Internal holdout: 30,294,701 training facts, 2,139,788 validation facts and
  206,209 held-out orders; order sets are disjoint, official-train overlap is zero.

The ALS search budget is one baseline configuration (`rank=32`, `regParam=0.1`,
`alpha=10`, `maxIter=10`, raw strength), with 5,000 deterministic pilot users,
full-size internal validation and final fitting on all prior interactions.
This measures the baseline configuration; it does not establish a tuned optimum.
Official train is reserved for the final K=5/10/20 evaluation against global
popularity and personal frequency, including warm/all and history cohorts.

Graph scope is the full catalog and all prior baskets, with no product, basket
or pair sampling. Thresholds 2/5/10 are compared. The final configured threshold
10 requires repeated co-purchase evidence and removes the low-frequency tail;
coverage, isolation and weight retention must be interpreted against all three
trial reports. PageRank uses symmetric topology through real GraphFrames.

[Runtime override](runtime.override.yml) uses four executor cores and a 2 GiB
executor. Shuffle and worker files use native Linux storage at
`/var/lib/module4-local-20261010`, avoiding Windows filesystem overhead. The
preserved HDFS/MongoDB ext4 image on D: is mounted using the
[existing mount script](../acceptance-20261009/mount-runtime-storage.sh).
At restoration, C: had approximately 47 GiB and D: 16 GiB free.

The initial [full-data attempt](../full-ab-20261010-first/README.md) failed after
model reload: auto-broadcast planned four recommendation tasks and the worker
exhausted its heap while merging spills. No new snapshot was published. The
[failure receipt](initial-oom-interruption.json) preserves the observed error,
original manifest and termination details. Runtime now disables auto-broadcast;
the regression suite uses the actual Module 4 session and checks that reloaded
factor recommendations retain a distributed Cartesian block join. This follows
the [pinned Spark ALS implementation](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala).

The read-only full saved-model regression passed in 251.136 seconds: 206,209
users, 4,330,389 candidate rows (21 per user), zero invalid scores and a
distributed Cartesian block join. This diagnostic does not count as A/B
publication acceptance. Candidate frames are now materialized to temporary
disk caches so the boundary-tie check and final ranking reuse one recommendation
calculation. Temporary caches are released on success/failure; callers own and
release the final cache. Fixture tests check tie membership and cache release;
the fresh repository suite after these changes passed **91 tests**, zero skips,
five warnings, in 165.09 seconds. Ruff and runtime smoke passed.

Corrected first/replay use model `als-full-20261010-fixed`, graph
`graph-full-20261010-fixed`, fixed
source/input/config, separate run IDs and sequential execution. The snapshot
checker requires a ready A/B pair, exact schema/index/count/version checks and
matching logical digests. Floating scores/PageRank are compared at six decimal
places; keys, membership, ranks, degrees, versions and thresholds are exact.

Historical GitHub runtime CI passed for commit `11ac21c` on 09/10; see the
[previous receipt](../acceptance-20261009/github-run.json). That result is not a
GitHub run of the current uncommitted workspace. The full-data manual workflow
has not been dispatched; current acceptance is measured locally.

## Completed acceptance

**21/21 checklist gates closed**. Both full-data runs published and validated
the A/B pair. The same-version replay loaded the saved model and matched the
first run’s logical snapshot digests at the declared six-decimal precision.

| Run | Result | UTC timestamp span (s) | Sum of measured stages (s) |
|---|---|---:|---:|
| [full-ab-20261010-fixed-first](../full-ab-20261010-fixed-first/README.md) | passed; serving validated | 2605.493 | 2705.912 |
| [full-ab-20261010-fixed-replay](../full-ab-20261010-fixed-replay/README.md) | passed; serving validated | 2497.451 | 2603.335 |

| Stage | First wall time (s) | Replay wall time (s) |
|---|---:|---:|
| input_validation | 200.725 | 214.436 |
| als_compute | 1726.502 | 1534.922 |
| graph_compute | 374.172 | 465.716 |
| recommendation_publication | 395.278 | 383.598 |
| graph_publication | 9.237 | 4.663 |

Stage wall times use `time.perf_counter()` on the recorded single-host
runtime; the overall UTC span uses the recorded start/finish timestamps.
The WSL run has a discrepancy between these time bases (the first stage
sum exceeds its UTC span); use the monotonic stage counters for stage
comparisons rather than treating the UTC span as their sum. The
replay reuses the final model but repeats input validation, bounded tuning,
evaluation, graph construction and publication. Event-log job-group
shuffle/task/resource measurements are in each run’s `performance.json`
and `spark_event_metrics.json`; they are not production-capacity estimates.

Publication: **4,124,180 recommendations**
for **206,209 prior-factor users**;
**49,688 graph documents**, including isolated catalog products.
Graph scope: `full_catalog_vertices_and_prior_baskets`; real GraphFrames; final threshold 10.
Canonical edges: 3,492,837; directed edges: 6,985,674.
Weighted-degree sum 324,442,508 = 2 × retained edge weight 162,221,254.
Isolated vertices: 21,164; PageRank is finite.

| Threshold | Edges | Active catalog coverage | Prior fact coverage | Retained pair weight | Isolates | Trial time (s) |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 18,804,842 | 96.56% | 99.98% | 90.80% | 1,708 | 103.203 |
| 5 | 7,243,387 | 74.81% | 99.59% | 78.19% | 12,515 | 36.851 |
| 10 | 3,492,837 | 57.41% | 98.79% | 68.04% | 21,164 | 23.580 |

Official-train comparison on the same all-user cohort at K=10:

| Method | Precision@10 | Recall@10 | MAP@10 | NDCG@10 |
|---|---:|---:|---:|---:|
| als | 0.095786 | 0.121472 | 0.069884 | 0.137102 |
| global_popularity | 0.072522 | 0.069843 | 0.044008 | 0.097881 |
| personal_frequency | 0.266928 | 0.319232 | 0.261650 | 0.386763 |

ALS does not exceed personal frequency on Recall@10; acceptance demonstrates correctness and reproducibility, not recommendation-quality improvement.
K=5/10/20, warm/all cohorts, reordered/new-to-user subgroups and exact deltas
are in [evaluation.json](../full-ab-20261010-fixed-first/evaluation.json).

The active readiness pointer describes the replay run:
`full-ab-20261010-fixed-replay`, model `als-full-20261010-fixed`, graph
`graph-full-20261010-fixed`, `ready=true`. Consumers check
`module4_snapshot_readiness` (`_id=active`) before reading both versioned collections.

Reproduction and audit:

- [Exact sequential commands](run-acceptance.sh); choose new run IDs for a new rehearsal.
- [Fresh repository tests](regression-tests.txt), [runtime smoke](runtime-smoke.txt), [lint](lint.txt).
- [Final static, documentation and integrity checks](final-static-checks.txt).
- [First snapshot read-back](first-snapshot.json), [same-version replay read-back](replay-snapshot.json).
- [Full saved-model regression](reload-full-check.json), [physical plan](reload-full-plan.txt).
- [Saved-model regression command](reload-full-check.sh), [actual log](reload-full-check.txt).
- [Source integrity](source-integrity.json), [runtime environment](runtime-environment.txt).
- [Initial OOM failure](initial-oom-interruption.json) and [09/10 interrupted run](previous-interruption.json).

Keep the D: ext4 image to retain the accepted HDFS models and MongoDB snapshots.
After WSL restart, mount it with the linked mount script before starting services.
This is a single-host validation with one baseline ALS configuration. Connected
Components and Module 4C–4D are outside the mandatory A/B checklist.
