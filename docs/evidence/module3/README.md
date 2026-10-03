# Module 3 Evidence (Speed Layer)

> Status: **implemented and validated locally with Docker Compose and on a
> GitHub-hosted runner. PR #2 was merged into `main` on 2026-10-03.**

Module 3 uses the pinned Spark 3.5.5 / Java 17 image. The results below are
bounded development/correctness evidence, not production capacity claims. The
compact machine-readable record is
[`hardening-validation-2026-10-03.json`](hardening-validation-2026-10-03.json).
The final release-gate record is
[`final-release-validation-2026-10-03.json`](final-release-validation-2026-10-03.json).

## Merged baseline

- PR #2: merged
- Merge revision: `0622b14309b3ca379752cc827cdc7a7a45919551`
- Final PR head: `c462618048b3c0e2462486a8f145858762878feb`
- Validated source revision: `a35170ff0f98729ad2b64b8df4c96c4bf6321201`
- Post-merge Fast validation: run
  [37119416919](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37119416919), SUCCESS
- Post-merge Module 3 streaming validation: run
  [37119416925](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37119416925), SUCCESS

The 33,819,106-event full-data workflow ran on the validated source revision
`a35170f`, not on the final PR head or merge revision.

## Correctness evidence

| Gate | Result | Evidence |
|---|---|---|
| Complete repository tests | Current final-hardening gate: `51 passed in 28.33s`; historical: `43` after HDFS rollback and `42` on `ff06239` | local Docker pytest; CI Fast validation |
| Ruff / Compose / whitespace | PASS / PASS / PASS | local gate and CI |
| Deterministic generation | Equal logical rows across fixture batch sizes; entity-keyed seed, no batch index | `test_generation_is_invariant_to_user_batch_size` |
| Bounded replay | Disk-backed external merge; chunk bound and early stop for declared-sorted input asserted | Module 1 hardening tests |
| Kafka delivery accounting | attempted/acknowledged/failed callback outcomes and non-zero normal exit asserted | Module 1 hardening tests |
| Fault injection identity | duplicates retain source `event_id`; late events use deterministic distinct IDs and older timestamps | Module 1 hardening tests |
| Kafka offset-loss policy | `failOnDataLoss=true` by default; environment and CLI relaxation are explicit | Module 3 configuration/source tests |
| HDFS failure behavior | Failed upload cleans staging; failed promotion restores prior target | staging tests |
| Pivot/unpivot | Spark pivot and explicit unpivot totals both `24` on fixture | `test_department_hour_pivot_unpivot_consistency` |
| Duplicate/poison/late scenario | input `3`, valid `2`, invalid `1`, duplicate dropped `1`; beyond-watermark event dropped; finalized `C30=C120=2` | exact-source CI artifact from run `37103621207` |
| Full-population ranking / Top-K | Top-K is asserted after normalization/ranking; deterministic product-ID tie-break | Module 3 tests |
| Mongo snapshot/idempotency | Complete finalized windows replace stale rows; replay converges without duplicate product/rank keys | Module 3 tests and 50k smoke |
| TTL | Seven-day default; index reconfiguration tested; `0` explicitly disables | Module 3 tests |
| Checkpoint recovery | process 1 read `3`; process 2 read only `2` new rows; same query ID; continued batches; no earliest replay | local and CI recovery evidence |

The production streaming topology is:

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

Append mode is intentional. Ranking needs the complete product population for a
finalized window; this is not an open-window ranking recomputed every trigger.

## Local bounded 50k measurement

Command:

```bash
python3 scripts/module3_smoke.py --events 50000 --duration-seconds 120 \
  --feed data/synthesized/hardening_500 \
  --output results/module3-hardening/smoke-50k-final-61ae930 --top-k 20
```

| Metadata | Value |
|---|---|
| Classification | LOCAL / BOUNDED / DEVELOPMENT |
| Revision | `61ae930c883293fd006eb6d27f7da70aab376d8b` |
| Source digest | `3fb1c0240e873cb0700320a236caef73761b9f9ccbfbf2713d8a1fae264da8b4` |
| Timestamp | `2026-10-03T06:37:19Z` |
| Platform | WSL2 Linux, Spark 3.5.5, Java 17.0.14, Python 3.10.12 |
| Resources | standalone Spark, 2 cores, 8 shuffle partitions, one Kafka broker, 4 topic partitions |
| Windows | C30=30m, C120=120m, slide=5m, watermark=10m |
| Serving | Top-20, TTL 604800 seconds |

| Metric | Result |
|---|---:|
| Kafka attempted / acknowledged / failed | 50,000 / 50,000 / 0 |
| Producer elapsed / acknowledged rate | 4.621 s / 10,821.22 events/s |
| Reader strategy | external merge; 65,536-row chunks; 2 initial runs |
| Spark total input / valid / invalid | 50,000 / 50,000 / 0 |
| Input rows/s | 0.0 (events existed before query start) |
| Processed rows/s | 3,203.90 |
| Input trigger / addBatch | 15,605 ms / 14,098 ms |
| Peak aggregate / dedup state rows | 1,174,010 / 50,000 |
| Watermark drops in this on-time workload | 0 |
| Finalized sink batch | 62,021.821 ms |
| Finalized windows / Mongo documents | 35,701 / 580,342 |
| Maximum documents per window | 20 |
| Replaced stale documents | 580,342 |
| Duplicate window/product keys / ranks | 0 / 0 |
| Invalid scores / windows above Top-K | 0 / 0 |

Each source event may contribute to as many as 24 overlapping five-minute
windows. That state/output fan-out and the single-machine sink dominate this
bounded run; the numbers must not be extrapolated to a production cluster.

## GitHub Actions validation

| Workflow | Revision | Run | Result |
|---|---|---|---|
| Fast validation (push) | `ff06239` | [37094705692](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37094705692) | SUCCESS; 42 tests, Ruff, Compose |
| Module 3 streaming validation (push) | `ff06239` | [37094705601](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37094705601) | SUCCESS |
| Fast validation (push) | `61ae930` | [37103618599](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37103618599) | SUCCESS; 43 tests |
| Module 3 streaming validation (PR) | `61ae930` | [37103621207](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37103621207) | SUCCESS |
| Fast validation (PR, final source) | `a35170f` | [37107841305](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37107841305) | SUCCESS; 51 tests, Ruff, Compose |
| Module 3 streaming validation (PR, final source) | `a35170f` | [37107841330](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37107841330) | SUCCESS |
| Fast validation (PR, final head) | `c462618` | [37109270743](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37109270743) | SUCCESS |
| Module 3 streaming validation (PR, final head) | `c462618` | [37109270688](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37109270688) | SUCCESS |
| Fast validation (post-merge push) | `0622b143` | [37119416919](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37119416919) | SUCCESS |
| Module 3 streaming validation (post-merge push) | `0622b143` | [37119416925](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37119416925) | SUCCESS |

Run `37094705601` independently built the pinned runtime on a clean hosted
runner, regenerated a bounded Module 1 feed, ran all 42 tests, validated service
connectivity, executed real Kafka → Spark → MongoDB, verified checkpoint recovery
and the duplicate/poison/late-data scenario, ran a short throughput smoke,
uploaded evidence, and cleaned up services. The later `61ae930` change is scoped
to HDFS target rollback plus its regression test; run `37103621207` repeated the
complete streaming workflow successfully on that exact revision.

## Historical evidence

[`local-validation-2026-10-02.json`](local-validation-2026-10-02.json) and older
`results/module3/` measurements predate the hardening pass. They remain historical
records and must not be presented as current Top-K/deduplication performance.

## Limitations

- All live runs are bounded development checks on a single machine or hosted
  runner, not production-scale benchmarks.
- One Kafka broker validates functional partition parallelism, not broker high
  availability.
- Output is intentionally finalized-window trending, not an open-window live
  leaderboard.
- Sliding windows amplify state and output; the 50k run reached 1,174,010
  aggregate-state rows.
- Mongo writes use bounded driver-side `toLocalIterator` and bulk operations;
  Top-K bounds serving rows but this is not a distributed Mongo writer.
- Finalized-window replacement is logically idempotent and converges under
  Spark retry, but delete + upsert is not transactionally atomic for concurrent
  readers. A dashboard may briefly observe an empty or partial snapshot. A
  production design could use immutable snapshot/version IDs, an active-version
  pointer, or a Mongo transaction where appropriate.
