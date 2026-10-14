# Module 4C release review

## Review status

The local release audit passed on branch feat/module4c-ranking at commit c475e46da6257b5ca6b9199a146ee5bc4d30606e. The feature branch adds hybrid recommendation serving over the accepted Module 4A–4B outputs and finalized Module 3 trend bundles. This review records implementation boundaries and evidence; it does not establish production capacity or recommendation quality.

## Architecture and implemented components

The request path in src/module4c combines:

1. C4A immutable, hashed finalized-window bundles written by Module 3.
2. C5 bounded trend reads that select the latest verified bundle at or before an explicit UTC as-of event time and validate its schema, source identity, content hash, timestamp precision, and expiry.
3. A bounded capture of the active Module 4A–4B readiness pointer, ALS rows, graph/catalog rows, and source identities. Readiness is checked before and after capture under the accepted publication lock.
4. Deterministic ranking over normalized ALS and trend scores, with stable product-ID tie breaking, explicit ALS-only, trend-only, and hybrid modes, plus documented cold-user fallback behavior.
5. C6 immutable, versioned per-user recommendation rows and a per-user active pointer. The publisher validates prepared inputs, protects rows with unique version/user/product and version/user/rank indexes, reads back complete publications, and prevents stale or incomparable source generations from replacing the active pointer.
6. A CLI entry point at python -m src.module4c.cli serve-user. It requires an explicit as-of event time and source namespace, validates the user and ranking inputs, and redacts MongoDB credentials from error output.
7. C7B invocation coverage, C8A controlled responsiveness measurement, and C8B bounded Kafka → Spark → Module 3 → Module 4C integration evidence.

The C1–C8B work covers configuration and contracts, readiness-aware source capture, deterministic ranking, atomic trend bundles, trend snapshot verification, immutable recommendation publication, the serving CLI, responsiveness measurement, and a bounded streaming demonstration. The established Module 1–3 contracts and the Module 4A–4B outputs remain the serving inputs.

## Concurrency and idempotency correction

The original writer preflighted a bundle identity and issued insert-only writes. When MongoDB returned duplicate-key E11000 after a concurrent writer had already persisted the expected bundle, the error path validated the stored digest and then unconditionally raised a content conflict. That incorrectly rejected an identical concurrent retry.

Commit c475e46da6257b5ca6b9199a146ee5bc4d30606e changes src/module3/trend_bundle.py to fetch and validate complete stored documents and classify only duplicate errors for the expected _id identities as candidates for recovery. It rejects write concern errors, unrelated unique-index duplicates, non-duplicate errors, malformed metadata, and incomplete read-back. After an accepted duplicate, it reads every expected publication document and verifies identity, source metadata, immutable content, and digest. A differing digest raises TrendBundleConflictError; any missing expected window or failed read raises TrendBundlePublicationError. Insert-only writes preserve the first stored document and its publication timestamps. Result counts distinguish actual inserts from identical retries.

Regression tests cover the matching and conflicting E11000 paths, direct DuplicateKeyError, unrelated unique-index errors, write concern and read failures, corrupted metadata, missing and partial multi-window state, exact digest and retry accounting, and preservation of existing timestamps and documents.

The correction changes only the approved trend-bundle writer and its unit/live MongoDB tests. It does not change the bundle schema, identity or content-hash algorithms, config hash, timestamp precision, namespace semantics, TTL policy, ranking formula, or legacy realtime_trending behavior.

The live concurrency suite used two independent Mongo clients and a MongoDB server failpoint to force the real server-side duplicate-key recovery path after both callers passed preflight. The identical race converged to one stored bundle with both calls successful and retry accounting correct. A different-content race produced one success and one explicit conflict while preserving the winner. A controlled partial multi-window failure rejected missing output; a later identical retry converged.

## C8A controlled responsiveness

C8A used a synthetic warm-user fixture with 20 ALS recommendations and fixed A/B readiness, graph, and catalog sources. Two immutable trend windows changed the trend distribution; five lambda values were compared using an independent score oracle.

| Lambda | Baseline Top-1 | Changed Top-1 | Top-1 changed | Top-20 overlap | Rank changes | Mean absolute rank displacement |
|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | P1 | P2 | Yes | 20/20 | 2 | 0.1 |
| 0.5 | P1 | P2 | Yes | 20/20 | 2 | 0.1 |
| 0.7 | P1 | P1 | No | 20/20 | 0 | 0.0 |
| 0.9 | P1 | P1 | No | 20/20 | 0 | 0.0 |
| 1.0 | P1 | P1 | No | 20/20 | 0 | 0.0 |

At the default lambda 0.7, the controlled trend shift changed scores but did not change the Top-1 or rank order in this fixture. For lambda 0.5, three measured first-publication CLI subprocesses had a baseline median of 790.660 ms (range 777.896–794.417 ms) and changed-window median of 803.200 ms (range 777.641–807.051 ms). Fixture and bundle setup were outside the timed interval; process startup and MongoDB serving were included. These measurements are not Kafka-to-serving latency.

## C8B bounded Kafka/Spark integration

C8B used synthetic controlled events and pinned Kafka 3.7.0, Spark 3.5.5, and MongoDB 7.0.16. It planned, acknowledged, and consumed 48 unique events, with zero invalid records and offsets 0–48. The configuration used a 10-minute watermark, 30-minute and 120-minute windows, a 5-minute slide, Top-20 output, and weights 0.7/0.3. The run observed 36 finalized bundle documents.

| Window | Event-time interval | Observed watermark | Batch | Rows | Top-1 | Last-watermark-ack to bundle visible |
|---|---|---|---:|---:|---:|---:|
| A | 00:00–02:00 UTC | 02:15 UTC | 3 | 20 | P1 | 12.291 s |
| B | 03:00–05:00 UTC | 05:15 UTC | 7 | 20 | P2 | 7.329 s |

Both serving versions contained 20 rows. Readiness remained valid, the per-user pointer advanced from A to B, and prior version rows remained immutable. Repeating B returned already_active with the same serving version and rows in 894.717 ms. Six recorded Spark trigger execution durations ranged from 14.723 s to 2.222 s across the run. These are observations from a small single-host demonstration, not capacity or production latency results. Checkpoint restart was not exercised.

The original C8B report records docker_resources_removed=true. A later C9A audit found that the original cleanup inspected containers and network but did not account for three anonymous volumes declared by the Kafka image. Commit ace465107f40c4ead89834c347351a21ca738354 added exact run-owned volume inspection and removal, including failure-path validation. The original C8B report remains unchanged; its historical cleanup field does not assert that those image-declared volumes were removed.

## Validation and evidence

The focused C4A/C4B/Module 4C suite passed 204 tests. The Module 3 integration suite passed 17 tests. The isolated MongoDB 7.0.16 concurrency suite passed 11 tests with no skips. Repository-wide pytest passed 542 tests, skipped 69 service-backed Mongo integration cases, and reported no failures. The skips were guarded by absent test URIs for the I1, I2, and I3 Mongo services and the Module 4 Mongo suite; the repository-wide container intentionally had no network. The dedicated C9A live suite ran separately with its isolated URI and passed.

Repository-wide Ruff, docker compose config --quiet, source compileability, and git diff --check passed. The pinned image was instacart-spark:3.5.5. The test container used a read-only source mount and no network.

The original report checksums match the required values:

- C8A report: cfb07b5d37fc244f9cbf9b5e7d22f4660ca3db47ec0f3f6352477718be095362
- C8B report: 9035deca000e138840286514543769cfb11fe8680a5cf7c4c3c9fffc23523ecd

Compact sanitized receipts and the detailed evidence links are in docs/evidence/module4c/README.md. Historical C8A and C8B reports are preserved under results/ and are not included in the source commit.

## Audit findings and remaining limits

- The Module 1 generator merges prior and train stream inputs, and its emitted event schema does not retain eval_set provenance. Existing event artifacts therefore do not establish a leakage-safe official-train chronology. No recommendation-quality comparison against ALS-only, personal frequency, or global popularity was run.
- C8A and C8B use controlled synthetic events and users. They validate contracts and bounded behavior, not recommendation accuracy.
- The local suite does not demonstrate cluster-scale capacity, multi-node failure recovery, or sustained production load. C8B did not exercise checkpoint restart.
- C8A and C8B held an accepted A/B source generation fixed and did not exercise concurrent A/B source-generation races. The full I2 live MongoDB suite was not run in this audit because its test URI was unavailable.
- The readiness pointer does not record recommendation K; the configured recommendation_k must agree with the accepted A/B publication.
- During the broader Docker audit, signals and Compose labels were observed on pre-existing project containers. The origin of that external Docker event could not be established. The task-owned test teardown was scoped to its own labeled container and network, and no global prune was used.

This review covers the local release gates. GitHub CI, independent review, branch protection, and merge status are tracked separately on the pull request.
