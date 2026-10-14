# Module 4D Release Review

**Review date:** 2026-10-14
**Local D3 gates:** passed
**GitHub PR:** [#7](https://github.com/mihikari29/instacart-market-basket-analysis/pull/7)
**Remote checks:** passed on implementation head `172f2f748a28bb9b23b98063173f8dfe7136d7f8`; the results are recorded below.

Current Docker commands use the root `Dockerfile` with named `spark`, `api`,
`ui`, and `browser` targets and one shared `.dockerignore`. The accepted D3
measurements below are historical release evidence. See [the Docker runbook](docker.md)
for the consolidated build, runtime boundaries, and subsequent validation.

## Scope and release boundary

This release integrates the existing Module 4D read API with the four-page
Streamlit dashboard. It verifies a complete read path against a disposable
MongoDB 7.0.16 instance:

```text
accepted synthetic publishers -> MongoDB snapshots -> read-only FastAPI -> HTTP -> Streamlit -> Chromium
```

The UI does not connect to MongoDB. The demo does not replay full data, run
full-data ALS training, or start Kafka/HDFS. It is an academic local demo, not
a production deployment.

## Architecture

Module 4A/4B retain their ALS and product graph responsibilities. Module 4C
verifies finalized Module 3 trend bundles and publishes immutable per-user
Hybrid snapshots. Module 4D exposes bounded read-only HTTP views. The frontend
and backend remain under `src/module4/dashboard/`; Hybrid serving remains under
`src/module4/hybrid/`.

The dashboard pages are:

1. **Overview** — API process health, independent source summaries, timestamps,
   and archived evidence availability.
2. **Realtime & Batch Analytics** — one selected finalized trend window,
   prior-only product/department purchases, and archived all-order distributions.
3. **Hybrid Recommendations** — the persisted per-user ranking, mode, scores,
   and source identities, including fallback and ready-empty states.
4. **Evaluation & Performance** — archived ALS/baseline evaluation, Spark
   benchmarks, and C8A/C8B reports with unavailable metrics called out.

FastAPI exposes nine route families:

| Route | Data returned |
|---|---|
| `GET /health` | API process liveness only |
| `GET /api/v1/recommendations/{user_id}` | Verified persisted Hybrid serving version and rows |
| `GET /api/v1/trends` | Newest eligible finalized bundle for an allowlisted namespace and event-time cutoff |
| `GET /api/v1/batch/products` | Bounded prior-only purchase or reorder-rate ranking |
| `GET /api/v1/batch/departments` | Prior-only department metrics |
| `GET /api/v1/evidence` | Fixed archived evidence registry |
| `GET /api/v1/evidence/{evidence_id}` | One registered report only |
| `GET /api/v1/analytics/orders-distribution` | Archived all-order day/hour distributions |
| `GET /api/v1/evaluation/als` | Archived official-train ALS and recorded baseline metrics |

The API reads MongoDB and the fixed packaged evidence registry. The Streamlit
client calls only these HTTP routes and does not recompute recommendation
scores.

## Isolated Docker demonstration

The reproducible operator entry point is:

```bash
bash scripts/module4d_demo.sh up
# Open http://127.0.0.1:18501; the command prints the run ID.
bash scripts/module4d_demo.sh down <printed-12-hex-run-id>
```

The script generates a mode-0600 environment file outside the repository. A
startup failure or interruption removes only that run's containers, networks,
and private environment file. Normal shutdown discards MongoDB tmpfs and removes
only that run's uniquely tagged API/UI images. It does not prune shared Docker
resources.

Startup builds the `api` and `ui` targets; API/fixture dependencies come from
the internal pinned `spark` stage without a separate image-build prerequisite.
The fixture uses one small `local[1]` Spark session for the synthetic Module 2
aggregation; no Spark cluster is started for dashboard use or page refresh.

The Compose project is uniquely named and owns two task-labeled bridge networks:

- MongoDB and FastAPI share the database network.
- FastAPI and Streamlit share the frontend network. Streamlit has no route to
  MongoDB and receives no database credentials.

MongoDB uses `mongo:7.0.16@sha256:c630c59342c1493d50345136df2af14a76b9e827dd5316bfabee07a0880a5f3a`,
768 MiB `/data/db` and 64 MiB
`/data/configdb` tmpfs mounts, and has no host-published port or persistent
volume. FastAPI publishes host port `18000` only on `127.0.0.1`; Streamlit
publishes `18501` only on `127.0.0.1`. Health checks gate service startup. The
API and UI targets do not mount the repository or `results/`.

The fixture administrator provisions source data and indexes, then creates a
database-scoped built-in MongoDB `read` user. FastAPI and the live API validator
use only this read identity. The fixture proves `find` and index metadata reads
succeed and that `insert` and `createIndexes` fail with MongoDB authorization
code 13. The API does not initialize or repair indexes.

## Synthetic fixture and publication contracts

Final browser run ID: `0d5e9ce999eb`. Its temporary database was
`module4d_demo_212d337539c9`; its namespace was `module4d-demo`. The database
was stored only in tmpfs and was discarded during final cleanup. The fixture
contains synthetic demonstration data, not observed customer purchases.

The fixture publishes through the accepted code paths:

- **Module 2:** ten prior-only item rows across five synthetic orders are
  aggregated by `src.module2.analytics.metrics` and persisted with
  `src.module2.analytics.publish_mongo`. Four product and three department
  documents use `run_id=module4d-demo-batch-001`.
- **Module 3:** `write_trend_bundles` publishes two distinct finalized,
  two-hour bundles for the same namespace, with canonical content digests and
  ranked items. As-of event time is `2026-10-14T14:17:00Z`; the windows end at
  `13:47:00Z` and `14:07:00Z`. The selected latest digest is
  `7e2210fdc1594e7766da63f65dd751b4957c22fcac8133571dcbf5e9f51ce86a` and its
  ordered products are `1021`, `1001`, `1002`. Its source scope is
  `synthetic-demonstration-window`.
- **Module 4A/4B:** `publish_recommendations`, `publish_graph_metrics`,
  `validate_snapshot`, and `update_pair_readiness` publish a ready generation
  with source run `module4d-demo-source-001`, model
  `module4d-demo-als-v1`, graph `module4d-demo-graph-v1`, 40 ALS rows, and 40
  graph catalog rows. No full-data model training is run.
- **Module 4C:** `serve_hybrid_user` publishes warm user 601 using the selected
  finalized trend, and the identical repeat returns the same serving version.
  User 602 receives `als_fallback_trend_unavailable`; user 603 has a valid
  ready-empty `no_candidates` snapshot. User 9999 has no pointer.

The warm persisted serving version is
`hybrid:6b2261f0cd02747bd6248f211278c9ca043801b0c7943f8e8e05258494e1be12`.
It contains 20 contiguous ranked rows. The first three are product 1001 at
`0.9562499999996963`, product 1002 at `0.8537828947366086`, and product 1003 at
`0.6263157894736513`. The route validator compared every returned ranking field
with the immutable Mongo rows and the selected readiness pointer.

## MongoDB indexes and operational setup

The fixture verifies the exact accepted definitions before serving requests:

| Collection | Index | Keys |
|---|---|---|
| `final_recommendations` | `uq_serving_user_rank` (unique) | `(serving_version ASC, user_id ASC, rank ASC)` |
| `final_recommendations` | `uq_serving_user_product` (unique) | `(serving_version ASC, user_id ASC, product_id ASC)` |
| `realtime_trending_windows` | `idx_source_namespace_window_end_desc` | `(source_namespace ASC, window_end DESC)` |
| `batch_product_metrics` | `idx_batch_product_purchase_count_desc_product_id_asc` | `(purchase_count DESC, product_id ASC)` |
| `batch_product_metrics` | `idx_batch_product_reorder_rate_desc_product_id_asc` | `(reorder_rate DESC, product_id ASC)` |

The ordinary `_id_` index is also checked. MongoDB explain plans use the
expected trend and batch ranking indexes. **Module 2 publication replaces the
product collection and its indexes; recreate both dashboard ranking indexes
after each Module 2 publication.**

## FastAPI read and error validation

The live validator uses only the database-scoped reader, checks the actual
Mongo rows and indexes, calls every route family and all five registered
evidence details, and compares collection names, index metadata, and readiness
pointers before and after GET requests. It checked 21 HTTP cases:

- Successful reads returned HTTP 200 for liveness, warm/fallback/ready-empty
  recommendations, selected trend, both product rankings, departments,
  evidence registry and details, order distributions, and ALS evaluation.
- Unknown user, missing readiness, and unknown evidence returned their accepted
  404 envelopes.
- Unallowlisted namespace and invalid event time returned HTTP 422.
- GET requests changed no collections, indexes, or readiness pointers; the
  response scan found no credential-bearing values.

The latest finalized trend was selected by `window_end <= as_of_event_time`;
the response digest, window timestamps, publication timestamp, schema/config
identity, source scope, row count, ordered product IDs, ranks, and scores match
the stored finalized bundle. Batch purchase and reorder-rate ordering and ties
match their persisted documents. The archived all-order response reconciles to
3,421,083 orders and remains explicitly distinct from prior-only item metrics.

The live API suite also covers pointer changes during reads, corrupt or
incomplete Hybrid rows, malformed/newest trend data with fail-closed behavior,
missing trend and batch indexes, temporary MongoDB outage, read-role write
denials, and no writes from GET requests. These are failure-path checks; they
are separate from the successful synthetic browser scenario.

## Archived evidence and metric interpretation

All five packaged report views were available and their response digests
matched the committed files:

| Evidence | SHA-256 |
|---|---|
| Module 2 full analytics | `fb2e1168ccace368ff9ad563420425fb418758125375f837cf3ac47731d7761b` |
| Module 2 full hardening benchmarks | `89273dabd43864d116f30c87d3f11b6050dc29f1ba23d1f2726d6db19df24320` |
| Module 4A full ALS evaluation | `a8e598c10a3b2f182069c0656a9aee3655019ea79e4b26b0011a32904f56751c` |
| Module 4C C8A responsiveness receipt | `b9c8e0cc5561cf18b9e5897a522e4155f5bd03affea2c9f827eecdf5e2b76207` |
| Module 4C C8B streaming receipt | `d5c5515695a91523c5ec75f547c996cbd4567d30198dba4c92d25ddb61b592fa` |

The evaluation page preserves the archived all-user official-train Recall@10
values: ALS `0.12147208248298487`, personal frequency
`0.31923221187877215`. The benchmark report explicitly marks structured
shuffle metrics unavailable. C8A/C8B remain bounded historical synthetic
experiments; they are not live Kafka/Spark service status or sustained capacity
measurements.

## Browser E2E

The pinned `mcr.microsoft.com/playwright/python:v1.63.0-noble` image
(`sha256:72bd171a9ffc2b4b59532aaa6210e21014d07093120dc25528870c0b840da1f0`,
Playwright Python 1.63.0, Chromium 153.0.8010.12) ran against the host-bound
URL `http://127.0.0.1:18501` using host networking.
This validates host browser access; a container-only HTTP check was not used as
a substitute. Chromium verified all four pages, navigation, controls, table and
chart content from the API responses, refresh, warm recommendation scores,
fallback mode, unknown-user error and recovery, the all-order count, the
archived Recall@10 values, and unavailable shuffle metrics. No JavaScript
exceptions, visible Python traceback, or credential-like value appeared.
The evaluation page rendered five charts, showed the join timing, partition scan,
cache, C8A publication, and C8B duration sections, and responded to cohort, K,
metric, and recorded-method selector changes.

The actual visible values included trend products `1021, 1001, 1002`, batch
products `1001–1004`, and warm-user top products `1001, 1002, 1003`. Streamlit's
health URL returned `ok`, and the API health URL returned `{"status":"ok"}`.

## Regression and packaging results

All commands ran on 2026-10-14. Test files remain owned by their appropriate
runtime; the UI suites are not skipped but run in the UI development image.

| Validation | Runtime | Result | Exit |
|---|---|---:|---:|
| Repository Spark suite, excluding D1 API-owned live files and UI-owned files | `instacart-spark:3.5.5`, Python 3.10, PySpark 3.5.5 | 690 passed, 69 service-backed skips, 5 Spark warnings; 154.27 s | 0 |
| D2A/D2B UI client, AppTest, and Evaluation tests | Python 3.14.8 UI development image; Streamlit 1.65.0 | 64 passed; 5.65 s | 0 |
| D1A/D1B and Module 4C API/Hybrid unit tests | Python 3.10.12 API image, FastAPI 0.143.0, PyMongo 4.11.3 | 497 passed; 3.07 s | 0 |
| D1A/D1B live Mongo API tests | MongoDB 7.0.16, tmpfs, separate database-scoped reader | 26 passed; 34.87 s | 0 |
| Module 3 bundle, Module 4, Module 4C Mongo regressions | MongoDB 7.0.16 with test failpoints enabled; `instacart-spark:3.5.5` | 93 passed; 46.82 s | 0 |
| D3 accepted-publisher fixture and 21-case live route validator | MongoDB 7.0.16 / FastAPI / Streamlit Compose project | all checks passed | 0 |
| Pinned Chromium E2E | Playwright Python 1.63.0 / Chromium 153.0.8010.12 | four pages, five evaluation charts, all evaluation selectors, source-derived content, recovery, refresh, no page exceptions | 0 |
| Repository Ruff | Python 3.10.12 Spark image, Ruff 0.11.13 | all checks passed | 0 |
| Docker Compose config, Python compileall, shell syntax, and `git diff --check` | local plus pinned Spark image | passed | 0 |
| API, UI, and browser image dependency checks | pinned runtime images | no broken requirements | 0 |

The 69 skips in the broad Spark run are service-backed tests whose Mongo/Kafka
URIs are intentionally unset in that suite; D1 API Mongo tests and the listed
Module 3/4/4C Mongo suites were run separately with isolated services. The
supplementary Module 3 live tests use `enableTestCommands=1` because two tests
exercise MongoDB's `configureFailPoint`; the correctly configured rerun passed
all 93 selected integration tests. Spark reported five Deprecation and
UserWarnings. Docker dependency installation emitted the standard pip root-user
warning during image builds; API, UI, and browser image `pip check` commands all
reported no broken requirements.

The existing GitHub `fixtures` job remains in place as the required status. A
new `module4d-demo-browser` job runs the UI test suites and the isolated fixture,
API validator, and pinned Chromium checks after it. On PR #7 implementation
head `172f2f748a28bb9b23b98063173f8dfe7136d7f8`, GitHub reported success for:

- Required `fixtures` check and dependent `module4d-demo-browser` job in [Fast validation](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38579894942).
- `streaming` in [Module 3 streaming validation](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38579894987).
- `spark-graphframes-mongo` in [Module 4A/4B pull-request validation](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38579894929) and the matching [branch-push validation](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38579857897).

The first pull-request Module 4A/4B attempt stopped during its pinned Spark
image build when the GraphFrames artifact host reset the TLS connection. The
failed-job-only retry on the same head passed the full runtime and staged-flow
checks. The repository's main branch rules require only `fixtures` and zero
approving reviews; at the recorded head GitHub reported the PR mergeable, with
no review requests or discussion comments. The PR remains a draft until the
final release action. The Module 3 and Module 4 repository-wide workflows
exclude UI-owned files from their Spark containers and run the UI suites in the
pinned UI development image; their core checks remain enabled.

## Claim boundaries and known limitations

| Claim | Evidence | Limit |
|---|---|---|
| API correctness | Unit and live Mongo tests; source-vs-response route validator; error and mutation checks | Does not establish deployment uptime or performance at scale |
| Isolated Mongo integration | Real MongoDB 7.0.16, accepted publishers, required indexes, database-scoped read role | Fixture data is synthetic and disposable |
| Browser functionality | Pinned Chromium through host loopback, four pages and source-derived visible values | Does not establish production browser/device coverage |
| Archived benchmark evidence | Fixed evidence registry and matching SHA-256 digests | Historical evidence is not a live benchmark run |
| Offline ALS quality | Archived all-user official-train Recall@10 and baselines | ALS `0.121472` is below personal frequency `0.319232`; this is one recorded configuration |
| Hybrid quality | No new quality result was produced | Hybrid remains unevaluated against leakage-safe ALS-only/popularity baselines |
| Production scalability | No production load, multi-node, or sustained-capacity study was run | Unknown |

The demo's event time, processing/publication time, API observation time, and
archived evidence time are distinct. The selected trends route uses explicit
UTC event-time eligibility; report digests identify immutable archived files;
`/health` describes API process liveness only. None of these claims should be
used as a proxy for another.
