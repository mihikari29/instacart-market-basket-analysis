# External acceptance handoff

**Implementation complete; external runtime acceptance is pending.** This handoff prepares an operator to review the feature branch, reproduce its offline checks in the pinned environment, and collect fresh runtime evidence. Historical records and offline source checks do not establish current runtime acceptance.

## Delivered components and source identity

The implementation source passed independent source-code review at `a5fe5003134efb26f2a8826ce51e98bc291a2ff9`. The later feature-branch cleanup revision `e4bd750b2e744c80025cc9deead4b3c2365c18f4` changed Markdown only; the reviewed source tree is unchanged by that cleanup. This restoration is also documentation-only. Verify the feature branch's final source tree and remote identity before using these records as evidence.

| Component | Delivered behavior | Main implementation and contract references |
|---|---|---|
| Module 1 | Six-file fail-closed validation, typed cleaning, deterministic synthetic event generation, source/handoff receipts, HDFS staging and Kafka producer. | `src/module1/`; [generation guide](generate.md); [Docker Module 1 procedure](docker.md#module-1-ingestion-generation-staging-and-kafka-replay). |
| Module 2 | Prior-history Spark analytics, descriptive user features, Mongo publication and local/standalone benchmark tooling. | `src/module2/`; [benchmark guide](module2-executor-benchmark.md); [Docker Module 2 procedure](docker.md#module-2-batch-processing-and-benchmarks). |
| Module 3 | Kafka validation, event-time windows, bounded replay/offset reconciliation, checkpoint contracts, optional finalized bundles and completed-batch telemetry. | `src/module3/`; [Module 3 contract](module3-contracts.md); [Docker streaming procedure](docker.md#module-3-streaming-operations). |
| Modules 4A/4B | Prior-only ALS, catalog-wide product graph, offline evaluation and versioned Mongo snapshots. | `src/module4/`; [Module 4 plan](module4-plan.md); [Docker full-data guide](docker.md#module-4a4b-operation-and-full-data-acceptance). |
| Module 4C | Readiness-aware Hybrid source reads, deterministic versioned ranking and prior-only evaluation tooling. | `src/module4/hybrid/`; [Module 4 §19](module4-plan.md#module4c-spec). |
| Module 4D | Bounded read-only API and Streamlit dashboard; UI data access goes through FastAPI. | `src/module4/dashboard/`; [Module 4 §20](module4-plan.md#module4d-spec); Docker API/UI procedure. |
| Kubernetes | Run-scoped B2/B3/B4 jobs, private receipts, identity locks, guarded acceptance and separate teardown. | `scripts/k8s/`, `deploy/k8s/`; [Kubernetes runbook](kubernetes-demo-runbook.md). |

## Important input and output contracts

The full-data source contract is exactly six Instacart CSV files: orders, prior interactions, train interactions, products, aisles and departments. Module 1 validates columns, raw domains, keys, foreign keys and contiguous cart positions before coercion and publication. Preserve the six original inputs and their source receipt. A fixture is not a substitute for those files in full-data acceptance.

The source dataset has no absolute purchase dates. Generated event time exists for replay and event-time aggregation. The ordinary Module 1 stream includes prior and train orders; its 13-field Kafka message does not carry `eval_set`. Do not interpret generated dates as customer chronology or use an event-time cutoff alone as proof of a leakage-safe train evaluation.

Module 2 batch analytics and Module 4A/4B model/graph inputs use prior interactions. Module 2's `user_features` output is descriptive, marked `prior_only` and is not an ALS fit input. Official train baskets are evaluation truth. Module 3 emits finalized windows only after its event-time watermark closes them; a finite replay may finish with an unfinalized tail in checkpoint state. Module 4C requires versioned ready batch sources and an immutable eligible trend bundle. Module 4D reads bounded versioned records and does not publish or start processing jobs.

HDFS, Kafka, MongoDB and checkpoints have separate receipt and ownership boundaries. A successful promotion of one Mongo collection is not a transaction across other outputs. The detailed schemas, counts, fields and recovery contracts are in the linked technical guides and [implementation ledger](implementation-ledger.md).

## Recorded development checks

Checks associated with the reviewed source identity include:

- Independent source-code review: **PASS**.
- Broad non-service pytest selection: **803 passed, 7 skipped**; the seven skips are Spark-dependent modules, not seven individual skipped test cases.
- Focused telemetry/BSON regressions: **59 passed**.
- Focused telemetry, Module 4D API and UI client/AppTest selection: **134 passed**.
- Repository-wide Ruff: **PASS**; Python compilation: **190 files PASS**.
- Offline Kubernetes fake-client/static, local rendering, runner and packaging/source checks: **PASS** within their recorded selection.

The checks ran with host Python 3.14 and temporary lightweight dependencies under `/tmp`. They did not establish the pinned Spark 3.5.5 / Java 17 container behavior or every transitive dependency. The broad suite used this selection:

```bash
python3 -B -m pytest -p no:cacheprovider -q -m 'not integration' \
  --ignore=tests/test_module3_trend_bundle_mongo.py \
  --ignore=tests/test_module4_mongo.py \
  --ignore=tests/test_module4c_cli_mongo.py \
  --ignore=tests/test_module4c_data_mongo.py \
  --ignore=tests/test_module4c_sink_mongo_mongo.py \
  --ignore=tests/test_module4c_trend_reader_mongo.py \
  --ignore=tests/test_module4d_api_mongo.py \
  --ignore=tests/test_module4d_batch_api_mongo.py \
  --ignore=tests/test_module4c_responsiveness.py \
  --ignore=tests/test_module4c_streaming_demo.py
```

Seven Spark-dependent modules were skipped by the selection: Hybrid evaluation parity, Module 3, bundle integration, trend bundle writer, Module 4 Spark fixtures, Spark batch fixtures, and the Module 2 producer-to-Parquet-to-Module 4 reader handoff. Eight live MongoDB modules were excluded to avoid service access. The responsiveness and streaming-demo suites were also excluded because their imports require unavailable PySpark. Those tests are **not run** in this host suite.

Ruff and compilation are source checks. Fake Kubernetes clients and offline Kustomize rendering are not a live cluster. Historical green GitHub workflows refer to their recorded source revisions; the finalization push used `[skip ci]`, and no CI result is claimed for that push. The exact per-check logs and historic result dates remain in the [implementation ledger](implementation-ledger.md) and evidence archive.

## Historical evidence and scope

The evidence archive preserves dated results that predate or otherwise have a different scope from the reviewed source revision:

| Record | Scope |
|---|---|
| [Full-data Module 1/2 report](evidence/full/README.md) | 3 October 2026 full public dataset, one isolated host, Spark 3.5.5/Java 17/Python 3.10, one two-core executor, 33,819,106 generated events and recorded batch benchmarks. |
| [Module 3 validation](evidence/module3/README.md) | Bounded Kafka/Spark/Mongo correctness, watermark, recovery, throughput and partition experiments on one broker and a small Spark runtime. |
| [Module 4 acceptance](evidence/module4/acceptance-20261010/README.md) | Historical 10 October 2026 run with 21/21 gates, its input/model/graph metrics, and its stated limitations. It is not a fresh run of the reviewed source. |
| [Module 4C records](evidence/module4c/README.md) | Bounded synthetic integration observations; no Hybrid quality or capacity claim. |
| [Module 4D records](evidence/module4d/README.md) | Dated API/UI design and release records; use each report's own revision and scope. |
| [Style comparison](style-regression-validation.md) | Separate historical comparison marked **NOT FULLY VERIFIED**; not current runtime acceptance. |

Historical measurements must retain their date, code/runtime identity, input scope, method and limitations. A recorded count or passing gate does not become a new result because it appears in a current status document.

## Validation not executed for the reviewed source

The following work remains with an external operator in a suitable isolated environment:

1. Run the fixture and integration suites inside the pinned Spark 3.5.5 / Java 17 environment, including PySpark-dependent cases, and execute isolated live MongoDB tests.
2. Complete browser checks against the isolated dashboard fixture and inspect package boundaries.
3. Build/import the `k8s-runner`, `ui` and `pipeline` packages using the manual Kubernetes packaging workflow or its source-verified equivalent. No workflow dispatch or image build is claimed here.
4. Run isolated Kubernetes B2/B3/B4; verify run ownership, storage, service readiness, Mongo roles/indexes, API/UI behavior, checkpoint/replay evidence, network policy allow/deny probes and retained receipts.
5. Exercise checkpoint recovery with the chosen runtime and verify query identity/offset continuity.
6. Run full-data processing with exactly the six reviewed CSV inputs, verify their receipts/hashes, observe the 4 GiB raw-input cap and retain complete output/model/replay evidence.
7. Run the standalone executor-layout experiment and capture actual executor allocation. Measure streaming latency using the defined ingestion-to-finalized-output boundary. Do not infer an improvement before observing the result.
8. Record fresh results with exact source revision, input receipts, environment identities, commands, output, skips, failures and limitations. Keep fixture and full-data status separate.

Historical Module 1–3 runs and the 10 October Module 4 acceptance are not substitutes for these checks.

## Environment and reproducibility prerequisites

For Docker work, review the persistent Compose volume boundary and the separate disposable dashboard setup before running services. Keep existing datasets, checkpoints, credentials and evidence outside cleanup actions. Use the [Docker guide](docker.md) for current commands, required indexes, ports and teardown scope.

For Kubernetes, use a dedicated run ID, profile, context, namespace, storage and private output directory. The runbook requires at least six physical CPU cores, 12 GiB available RAM and 40 GiB free disk; full-data mode caps raw input at 4 GiB and reserves explicit pipeline storage. Measure WSL backing-disk capacity independently when applicable. The committed runtime lock is a template and must be finalized from measured host, cluster, image, toolchain and storage identities. Do not fill missing identities with estimates.

Use the exact source files and preserve their receipts. Record the source tree/commit and working-tree state, toolchain and image digests, configuration and scenario, input counts/hashes, output receipts, service identities, all executed commands and exit statuses. Keep secret values out of logs and evidence. Synthetic timestamps are simulation metadata only.

## Packaging, acceptance and teardown boundaries

The manual Kubernetes packaging workflow is service-free and network-disabled; a workflow definition is not a passing workflow result. Runtime acceptance requires successful live execution of the declared scope with evidence bound to the run identity. A `runtime_pass` receipt records B4 completion for fixture or full-data scope; it does not show that resources were deleted. The offline `verify` operation contacts no cluster and reports teardown as not performed.

Resources are retained after success or failure. Collect and inspect sanitized failure evidence before any cleanup. Teardown is a separate run-scoped operation requiring explicit execution and live ownership checks; it is not a prerequisite for reporting B4 runtime acceptance. Never use global prune, shared Compose cleanup or name-only deletion for a Kubernetes run. Follow the [Kubernetes runbook](kubernetes-demo-runbook.md) for the reviewed plan and exact procedure.

## Acceptance record ownership

The external operator owns the fresh environment and runtime evidence. Preserve source receipts, lock files, B2/B3/B4 receipts, logs, command output and failure evidence with run identity and digests. Mark each test as executed, skipped, failed or blocked with a reason. Identify fixture versus full data, historical versus fresh, offline versus runtime, and source revision. Do not report runtime acceptance until the applicable live checks pass; do not infer production scalability from bounded measurements.
