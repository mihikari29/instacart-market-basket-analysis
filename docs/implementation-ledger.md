# Offline implementation campaign ledger

## Independent review remediation — 18 October 2026

Baseline: `c169b76574f9abf8a8763c3c4cb742ec3ea12685` on
`feat/offline-project-finalization`. The original dirty checkout, main,
Proposal, and historical evidence are preserved. Runtime acceptance remains
assigned to the external operator.

- M1: added fresh run-only HDFS ownership bootstrap and consistent
  `k8spipeline` staging/readback identity; required Immediate PVC binding.
  Offline HDFS, pipeline, and runtime tests passed (see final validation record
  below for the consolidated results). No runtime command was executed.
- M2: finite replay now waits for producer success and committed terminal Kafka
  offsets. Full mode retains every generated event, with an 18-hour replay
  ceiling and matching Job/host bounds. Delivery/consumption evidence is checked
  again by B4; event-time tail state remains in the checkpoint. Fake-process,
  pacing, pipeline and acceptance-task tests were run without services.
- M3: raw baskets require contiguous cart positions 1..N. Reconstructed gap
  range and weekday checks now fail before publication. Regression cases cover
  both interaction tables, existing-output preservation, and capped recovery.
- M4: offline Hybrid endpoint candidate sets now follow serving: trend-only at
  lambda zero, ALS-only at lambda one for warm users, trend fallback for cold
  users. Spark parity cases were added; they require the external Spark test
  environment and are not represented as executed here.
- M5: added an explicit standalone Spark benchmark mode with distinct executor
  core/memory layouts, isolated worker processes and bounded REST evidence of
  active executor allocation. Existing local[1]/local[2] mode is preserved.
  Fake-worker/configuration checks passed; no executor experiment was run.

## Baseline and safety

- Requested source baseline: `origin/main` at `1565b65f44a4b4c03f7df761e8e47fe3ba09ce3d`.
- Implementation branch: `feat/offline-project-finalization`.
- Worktree: `/home/minh/projects/instacart-market-basket-analysis-finalize`.
- Initial worktree status: clean and exactly at the requested baseline.
- The original checkout and other registered worktrees were inspected read-only and remain untouched.
- `docs/Proposal.md` is the requirements source and is not to be edited.

## Gap-to-file implementation map

| Milestone | Verified gap or work boundary | Planned implementation files |
|---|---|---|
| 1A: raw preflight | `src/module1/clean.py` currently skips missing CSVs, asks pandas to coerce narrow dtypes during read, and publishes Parquet files one at a time. The existing hardening tests cover event generation and delivery, not raw table contracts. | `src/module1/clean.py`, `tests/test_module1_hardening.py` |
| 1B: ALS view | Module 4D exposes verified Hybrid rows at `/api/v1/recommendations/{user_id}`. It has no standalone reader for versioned `user_recommendations`; the Streamlit recommendation view explicitly says direct ALS comparison is unavailable. | `src/module4/dashboard/readers.py`, `schemas.py`, `api.py`, `ui/api_client.py`, `ui/pages/recommendations.py`, corresponding `tests/test_module4d_*` |
| 1C: Module 2 feature consumer | Module 2 writes prior-only `user_features` to the configured HDFS feature path. No Module 4 descriptive/cohort reader is declared. ALS inputs are separately defined and must remain unchanged. | Inspect `src/module2/config.py`, `src/module4/cli.py`, `src/module4/data.py`; add a bounded prior-only descriptive path and contract tests, or record a precise unresolved choice. |
| 2: B2 serving | B1 provides the private renderer, structural ownership validator, readiness probe and serving manifests. It does not discover a live target, collect complete live receipts, finalize the lock, build/load images, create accounts/indexes or execute/verify a deployment. Runtime commands are expressly out of scope for this campaign. | Extend `scripts/k8s/ownership.py`; add staged lock/preflight/provenance helpers under `scripts/k8s/`; extend `deploy/k8s/` and `tests/test_k8s_*`; update `docs/kubernetes.md`. |
| 3: B3 Lambda | The current Kubernetes base has serving resources only. Compose contains the working Kafka/HDFS/Spark contracts, but no run-scoped Kubernetes data services, jobs, checkpoints or end-to-end job plan. The committed code will keep the Proposal's isolated topology and local-mode Spark execution explicit. | Add `deploy/k8s/base/` resources and deterministic workload planning/validation under `scripts/k8s/`; add offline manifest and ordering tests. Do not reuse Compose volumes. |
| 4: B4 acceptance | B1 has static validation and API readiness, but no run-specific runtime acceptance collector, scenario matrix, verified teardown gate or failure evidence workflow. Mocks must only emit offline-scope results. | Add future-ready acceptance/evidence/teardown validators under `scripts/k8s/`; add fake-client tests and acceptance documentation. |
| 5A–5E: experiments | Module 3 lacked an output boundary latency measure, Module 2 lacked a reproducible execution configuration comparison, and Hybrid evaluation needed a trend source independent of the mixed Module 1 feed. Browser CI already asserts five evaluation charts. | Added `src/module3/latency.py`, versioned checkpoint state handling, `src/module2/executor_benchmark.py`, prior-only official-train evaluation and gates in `src/module4/evaluation.py`, runtime smoke gates, browser synchronization, and focused tests/docs. |
| 6: final runner | The B2/B3/B4 APIs had no single command to stage profile creation, runtime lock evidence, deployment, acceptance, and evidence. | Added `scripts/k8s/demo.py` with local preflight/plan, explicit run gate, automated runtime lock/image preparation, B2→B3→B4 orchestration, receipt verification/collection, and separate teardown review/execution. Added source-backed local[1]/local[2] benchmarking to the B3 Module 2 Job and B4 gate. |
| 7: documentation | The previous Kubernetes and progress pages described only the pre-finalization audit state. Proposal sections 39, 42 and 45 are the demo/acceptance sources. | Updated current status, detailed runbook, demo script, acceptance matrix, capacity limits, CLI reference, recovery and teardown instructions. `docs/Proposal.md` remains unchanged. |

## Milestone tracking

| Milestone | Status | Files changed | Tests added / run | Checkpoint |
|---|---|---|---|---|
| 1 Application completeness | Implemented; automated application suite blocked by missing runtime dependencies | Module 1 preflight/atomic publisher; ALS reader/API/schema/client/UI; Module 2 provenance; Module 4 feature reader/CLI; focused tests | Added tests in `test_module1_hardening.py`, `test_module4d_readers.py`, `test_module4d_api.py`, `test_module4d_ui_client.py`, `test_module4d_ui_app.py`, `test_module4_feature_reader.py`, and `test_k8s_static.py`. Executed AST parse for 18 changed Python files, `git diff --check`, and a five-case direct feature-reader smoke. `pytest` suite NOT RUN: pytest and application dependencies are absent. | `05ddd1f` |
| 2 Kubernetes B2 | Code implemented; runtime evidence and execution pending | Staged strict lock lifecycle; host/toolchain/profile/storage discovery; run-scoped image build/load provenance; Python package inventory; MongoDB bootstrap/indexes and in-memory Secrets; serving deployment/readiness; exact NetworkPolicy inventory; complete live ownership/storage/image receipt and comparison | Added fake/static tests under `test_k8s_runtime.py`, `test_k8s_lock_lifecycle.py`, `test_k8s_image_provenance.py`, `test_k8s_mongo_secrets.py`, `test_k8s_network_policy.py`, `test_k8s_deployment.py`; extended ownership/static suites. Executed 47 focused K8s `unittest` cases (PASS), AST parse for 21 Python files (PASS), YAML parse for 8 lock/manifests (PASS), offline Kustomize rendering as part of the suite (PASS), and `git diff --check` (PASS). Docker/Minikube/Kubernetes/MongoDB were not contacted. | `c728ded` |
| 3 Kubernetes B3 | Code implemented; runtime evidence and execution pending | Run-scoped HDFS/Kafka platform, six-step input/Module 1/2/4/replay/Hybrid pipeline, deterministic bounded fixture, full-data input receipt/transfer, pipeline image target, DataNode/KRaft service and NetworkPolicy contracts, durable task and artifact receipts, strict pipeline ownership/storage/image receipt | Added `test_k8s_pipeline.py`; executed 13 B3 cases and 19 related B2 ownership/network-policy/runtime/lock cases (32 total, PASS), Python bytecode compilation, all `deploy/k8s/**/*.yaml` YAML parsing, and `git diff --check` (PASS). No Docker, services, cluster, or full-data run. | `c13e979` |
| 4 Kubernetes B4 | Code implemented; runtime evidence and execution pending | Strict B2/B3 live receipt comparison; dedicated acceptance Job; fixture/full-data status scopes; HDFS/Kafka/batch/stream/watermark/checkpoint/Mongo/ALS/graph/Hybrid/API/UI/NetworkPolicy evidence; sanitized failure capture; separate namespace/profile teardown gate | Added `test_k8s_acceptance.py`; executed 41 focused B2/B3/B4 ownership, pipeline, policy, runtime, lock, and acceptance cases (PASS), Python compilation, all Kubernetes YAML parsing, and `git diff --check` (PASS). No Docker, services, cluster, acceptance Job, or teardown ran. | `5ce484f` |
| 5 Experiments/evaluation | Implemented; runtime measurements and full-data acceptance pending | Streaming ingestion-to-output latency, checkpoint schema guard, local Spark configuration comparison, prior-only Hybrid evaluation/report, Module 2/3/4 gates, five-chart browser synchronization | Added focused standard-library unit tests: 14 cases passed (4 latency, 3 checkpoint, 3 Spark comparison, 4 provenance/acceptance). AST parsed 19 changed Python files, offline executor plan rendered, and `git diff --check` passed. `pytest` is unavailable; Spark, browser, Docker, and full-data tests were not run. | `d284f11` |
| 6 Final runner | Implemented; runtime deployment and acceptance pending | `scripts/k8s/demo.py` stages local checks, fresh profile/image provenance/lock finalization, B2/B3/B4 execution, private receipts, local verify/collect, and separately gated teardown. The B3 Module 2 Job includes source-backed local[1]/local[2] trials; B4 validates their matching inputs and shuffle evidence. | Added seven demo runner tests and two acceptance-task benchmark tests. Executed 79 focused K8s tests and 4 Module 2 executor benchmark tests, Python compilation, local-only preflight/plan commands, and `git diff --check`. No Docker/Minikube/Kubernetes command or image build/pull ran. | pending |
| 7 Final documentation | Implemented; runtime acceptance pending | Updated current campaign status and Kubernetes guide; added full CLI/runbook, Proposal §39 demo script, §42/§45 acceptance matrix, recovery, security, storage, capacity, limitations, and teardown procedure. | Documentation consistency review passed; local plan/preflight, 79 focused K8s tests, 4 benchmark tests, Python compilation, Proposal immutability check, and `git diff --check` passed. | pending |

## Decisions and limitations

- B3 will target a single isolated Minikube profile and Kubernetes-owned HDFS, Kafka, MongoDB and pipeline storage. Spark will use `local[2]` inside run-scoped Kubernetes Jobs; this is not native Spark-on-Kubernetes executor scaling.
- No image digest, toolchain identity, cluster UID, runtime metric or successful runtime receipt will be invented. Values that require a later authorized image build or cluster are runtime-discovered lock fields.
- The complete lock remains a shared B2/B3 identity source. B3 finalization adds exact Kafka/HDFS/pipeline image provenance to the already complete serving and host lock; every selected image identity must be concrete before the common lock can pass complete-lock validation.
- Runtime deployment, Docker, package installation, full-data execution, existing service access, and teardown are prohibited for this campaign.
- B2's 47 focused offline K8s tests have run and passed. Milestone 1 application suites remain NOT RUN because the available Python runtime lacks pytest, pandas, PyArrow, FastAPI, PyMongo, Streamlit, and PySpark; no packages were installed.

### Milestone 5 implementation record

- Module 3 now retains the latest producer `ingestion_time_epoch_ms` for each finalized product/window output row, removes the private field before Mongo publication, and records a constant-memory min/mean/max delay to output completion after enabled publishers. It records sink duration separately and documents that producer/output host clocks are not independently synchronized. The bounded Module 3 smoke verifier now requires a numeric latency observation. Stateful checkpoint paths use `ingestion-latency-v1`; an old unversioned checkpoint is rejected without mutation.
- Module 2 now has an offline `plan` and a separate `run` command comparing the actual `local[1]` and `local[2]` execution models in isolated Spark processes. It records source receipt and output checksum, code/runtime identities, actual master/configuration, input rows/bytes, wall times, physical plans, and event-log task/shuffle metrics. The harness explicitly does not claim native executor scaling.
- Module 4A/4B now writes a separate `hybrid_evaluation.json`: prior-only global Top-20 frequency candidates, a preregistered lambda sweep, ALS and existing baseline comparisons, all official-train and warm cohorts, source hashes, model/run/code identities, and cold-start summary. It does not consume the mixed Module 1 event feed or official-train labels when fitting, tuning, trends, or selecting candidates. The report disclaims equivalence to Module 3 temporal trends and makes no superiority claim. The full-data checker requires matching source receipts, complete K=5/10/20 metrics and all fixed methods.
- The Module 3 smoke acceptance gate checks the new runtime latency observation; the Module 4 full-data gate checks the prior-only evaluation receipt. The browser E2E waits for the fifth chart and retains the `>= 5` assertion with rendered-heading diagnostics. No browser CI job or runtime workload ran in this campaign.
- Static/offline checks completed: 14 new standard-library unit tests passed, 19 changed Python files parsed, offline Spark benchmark plan rendered, and `git diff --check` passed. `pytest` and `ruff` were unavailable; PySpark, browser, Docker, and full-data validation remain NOT RUN. No packages or services were installed or contacted.

### Milestone 1 implementation record

- Module 1 now requires all six tables, checks exact headers and raw domains/keys/relationships before numeric coercion, rejects non-canonical integer spellings that could alias after conversion, and stages all Parquet outputs before directory publication. Failure diagnostics omit source values; valid reordered headers and first-order nullable gaps remain supported.
- The ALS API reads only the active paired Module 4A/4B snapshot, validates user/model/rank/score provenance, applies indexed Top-20 queries, enriches from the matching graph version when metadata exists, and rechecks the readiness identity. Empty user results and unavailable model versions have distinct contracts. Mongo write methods are not used.
- The dashboard requests ALS and Hybrid snapshots independently, shows source versions, aligns by product ID, and leaves unavailable scores blank. An empty ALS result is explicitly not proof that a user is absent from source data.
- Module 2 now stamps `user_features` rows with `module2_run_id`, `feature_scope=prior_only`, and UTC generation time. The new Module 4 `user-features --user-id` command reads one bounded descriptive row; the exact schema rejects unexpected fields and the consumer is explicitly excluded from ALS model inputs.
- Test execution blocker: `/usr/bin/python3` is Python 3.14.4 and has no pytest, pandas, PyArrow, FastAPI, PyMongo, Streamlit, or PySpark. No installation was attempted. The five-case feature-reader smoke ran with the Python standard library and passed; it does not replace the test suite.

### Milestone 2 implementation record

- The lock remains incomplete in `deploy/k8s/versions.lock.yaml`. Static validation accepts only exact selected releases and image sources; finalization requires measured host capacity, the exact dedicated cluster marker/profile/context, control-plane and Calico digests, toolchain and StorageClass identities, clean-source BuildKit provenance, image manifest/config digests, exact Python inventory hashes, and no unresolved fields. A finalized copy must be a new output file; the committed template cannot be overwritten by the writer.
- Host preflight measures guest CPU/memory/disk and fails closed below B2 (4 CPU, 6 GiB RAM, 20 GiB disk). WSL backing-disk capacity must be provided as measured evidence. The existing prior preflight reported 5.41 GiB available RAM and unknown Windows backing-disk capacity; that blocks runtime creation until refreshed, but does not block implementation.
- Dedicated profile/context discovery and every future `kubectl` execution use the generated run identity. Creation, image build/load, Docker image metadata extraction, Secret apply, cluster identity queries, deployment, and live receipt collection require explicit `execute=True`; no such external operation was invoked.
- The renderer can still produce a private offline base render without a lock. A render intended for deployment must validate a complete lock, matching profile, all serving image digests, namespace labels, and the Mongo PVC claim-template ownership labels. Image build plans use run-specific tags and an explicit platform.
- MongoDB now bootstraps a separate admin credential, a database-scoped read account, and a separate database-scoped publisher account. Four opaque Secret manifests exist only in memory and are applied through stdin. Bootstrap code creates the B1/B2 read indexes; API readiness remains read-only. The publisher is currently scoped to the per-run database with MongoDB's `readWrite` built-in role; B3 must narrow this if its actual write contracts permit a custom role.
- The B2 receipt model validates namespace and cluster marker, exact direct inventory, controller children and owner-reference chains, PVC/PV claim identity, StorageClass UID/provisioner/local hostPath/node, serving Pod image IDs, and all Kubernetes/Calico component image digests. A separate live comparator rejects any drift. No teardown or automatic cleanup is implemented.
- Validation scope is offline only: 47 focused K8s unit/static tests passed; AST and YAML parsing and `git diff --check` passed. There is no live deployment receipt or runtime acceptance result.

## Remaining acceptance work

- Authorized host/toolchain/image discovery and a fresh isolated Minikube run.
- Real HDFS/Kafka/Spark/Mongo/API/UI readiness and acceptance evidence.
- Full-data acceptance under separately verified capacity/storage guards.
- Review of the completed code and ledger by the independent Sol 6.1 reviewer.

### Milestone 3 implementation record

- Added dedicated, run-labeled HDFS NameNode/DataNode and Kafka KRaft StatefulSets, isolated PVCs, and eight narrow NetworkPolicies. The manifest validator checks exact service flows, and the Kubernetes receipt binds direct objects, owner chains, PVC/PV identities, and running image/config digests to the lock.
- Added a private six-step Job plan: input, Module 1 staging, Module 2, Module 4, bounded Kafka replay/Structured Streaming, and a Hybrid read/publish. Fixture inputs are deterministic and capped at 50 users / 5,000 events. Full-data mode accepts exactly six reviewed CSV paths, verifies a SHA-256 input receipt, copies only those files to the run PVC, and checks the uploaded manifest inside the input Job before work proceeds.
- Module 1 staging now supports an isolated HDFS root and the in-cluster WebHDFS redirect without the legacy Compose hostname rewrite. Jobs are sequential, run-scoped, non-retrying at the Kubernetes layer, and retain their work, checkpoint, logs, and per-step JSON receipts for explicit failure inspection.
- Kafka uses the selected 3.7.0 KRaft release; the controller port is explicitly exposed and allowed only for Kafka self-traffic. HDFS DataNode WebHDFS and transfer ports are 9864 and 9866, with hostname redirects configured to use pod IPs. These are static image/config assumptions pending an isolated runtime check.
- Runtime build, component image discovery, profile creation, job execution, service readiness, and final B3 receipt collection were not invoked. Full-data execution was not attempted.

### Milestone 4 implementation record

- B4 requires both saved B2 and B3 ownership receipts to validate against fresh live collectors before applying its separate, run-labeled acceptance Job. The Job image is digest-pinned; its Job/Pod owner chain, labels, exit status, runtime image ID, lock digest, profile, cluster UID, and namespace UID are recorded.
- The in-cluster matrix verifies the six-file input receipt and mode/bounds, WebHDFS readback, Kafka produce/consume, Module 2/4 artifact receipts, Module 3 event-time/watermark/late-event behavior, checkpoint restart continuity, repeated Mongo publication, ALS/graph readback, a trend-sensitive Hybrid ranking check using live run-scoped source snapshots, and a denied pipeline-to-API path. The outer collector separately probes API readiness, the recommendation endpoint, UI HTTP, and the UI-to-API path.
- Offline, CI, fixture-runtime, and full-data-runtime scopes remain distinct. Only complete checks with bound live identities can report runtime success; fixture success is explicitly not full-data acceptance. Sanitized task output and Job logs are retained with SHA-256 identities.
- Failure evidence collection captures namespaced Events, Jobs, Pods, StatefulSets, PVCs, Pod descriptions, current/previous logs, strips Mongo URIs and secret assignments, and writes private output. Teardown has its own `execute=True` gate, verifies B2 and B3 again, validates B4/failure evidence and live acceptance Job UIDs, then targets only the exact run namespace and dedicated profile. No B4 Job or teardown action ran during this campaign.

### Milestone 6 implementation record

- `scripts.k8s.demo` provides local-only `preflight`, `plan`, `verify`, and inventory `collect`; `run` requires `--execute`. Without a supplied complete lock, it creates a fresh run-scoped profile, collects runtime/image identities, builds API/UI/pipeline images, inspects selected Kafka/HDFS images, finalizes private B2/B3 locks, then runs B2 serving, B3 jobs, and B4 acceptance in order. Supplied complete locks are bound to their run profile/context. Preparation and run receipts are private, and partial failures retain resources and evidence.
- Fixture mode uses the bounded deterministic image fixture. Full-data mode requires the six-file input receipt, an operator-provided WSL backing-disk measurement when applicable, the B3 capacity reserve, and the 12 GiB input ceiling. Automatic resume remains disabled because ownership and checkpoint continuity are not verified; teardown is a separate, failure-evidence-gated command.
- The Module 2 B3 step now executes repeated `local[1]`/`local[2]` comparisons against the same source after batch publication. Its B4 validator requires matched source/output evidence, actual masters, runtime and input-size identity, at least three trials per arm, and task/shuffle evidence. Job subprocess timeouts now support the bounded benchmark/pipeline window.
- Mongo admin, API-read, and publisher identities remain separate. The publisher's built-in `readWrite` role is scoped to the run database, not admin or other databases. B3's writers create run-owned staging collections, indexes, and replacement snapshots, so the built-in role remains broad within that isolated database; collection-specific privileges would require changing those writer contracts and a separate runtime authorization review.
- Offline checks: 79 focused K8s tests and 4 Module 2 benchmark tests passed; seven demo-runner tests and two synthetic benchmark-receipt tests are included in those suites. Python compilation, local-only preflight/plan, and `git diff --check` passed. No runtime command, image build/pull, cluster, service, full-data workload, or teardown ran.

### Milestone 7 implementation record

- Updated `docs/progress.md` and `docs/kubernetes.md` to distinguish implementation completion from runtime acceptance. Added `docs/kubernetes-demo-runbook.md` with prerequisites and observed host limits, run/verify/collect/recovery/teardown commands, topology and security boundaries, a Proposal §39 demo sequence, and §42/§45 acceptance matrix. `docs/module2-executor-benchmark.md` documents its B3 integration in the M6 checkpoint.
- The final status is **CODE COMPLETE — RUNTIME ACCEPTANCE PENDING**. Documentation does not claim runtime, full-data, CI, or production acceptance. `docs/Proposal.md` remains unchanged; no historical result was relabeled or regenerated.
- The closing local plan and preflight commands had no external effects. At preflight, the source tree was clean and the host measured 5.285 GiB available RAM, below the 12 GiB B3 reserve; Minikube was absent and WSL backing-disk capacity unknown. No runtime command was invoked.

## CODE COMPLETE — RUNTIME ACCEPTANCE PENDING

Milestones 1–7 are implemented in the isolated branch and have local checkpoint
commits. Offline checks do not demonstrate image-build, cluster, service,
full-data, interactive-demo, or teardown behavior. Runtime acceptance remains
pending, and the current measured host RAM is below the documented B3 reserve.
