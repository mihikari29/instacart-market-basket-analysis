# Offline implementation campaign ledger

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
| 5A–5E: experiments | Module 3 reports throughput, trigger, add-batch and sink durations, but not ingestion-to-output latency. Module 2 has no executor comparison. Hybrid evaluation documentation identifies leakage because its event feed mixes prior/train. Browser CI already has a five-chart assertion; preserve it and only improve synchronization/diagnostics if source analysis supports it. | Add timestamp provenance and latency harness in `src/module3/`; reproducible executor experiment in `src/module2/`; prior-only evaluation-compatible path in `src/module4/hybrid/`; regression-only browser edits if justified; add scoped tests. |
| 6: final runner | There is no one-session Kubernetes CLI. Existing `scripts/k8s/render.py` is offline-only and `ownership.py` is pure validation. | Add `scripts/k8s/demo.py` (or equivalent) with dry-run planning, explicit `--execute`, injected command executor, receipts and tests. Runtime operations stay unexecuted. |
| 7: documentation | Current Kubernetes docs describe B1 and planned B2–B4; progress records the same outstanding gaps. Proposal sections 39, 42 and 45 are the demo/acceptance sources. | Update `docs/progress.md`, `docs/kubernetes.md`, and add a final runbook after code exists. |

## Milestone tracking

| Milestone | Status | Files changed | Tests added / run | Checkpoint |
|---|---|---|---|---|
| 1 Application completeness | Implemented; automated application suite blocked by missing runtime dependencies | Module 1 preflight/atomic publisher; ALS reader/API/schema/client/UI; Module 2 provenance; Module 4 feature reader/CLI; focused tests | Added tests in `test_module1_hardening.py`, `test_module4d_readers.py`, `test_module4d_api.py`, `test_module4d_ui_client.py`, `test_module4d_ui_app.py`, `test_module4_feature_reader.py`, and `test_k8s_static.py`. Executed AST parse for 18 changed Python files, `git diff --check`, and a five-case direct feature-reader smoke. `pytest` suite NOT RUN: pytest and application dependencies are absent. | `05ddd1f` |
| 2 Kubernetes B2 | Code implemented; runtime evidence and execution pending | Staged strict lock lifecycle; host/toolchain/profile/storage discovery; run-scoped image build/load provenance; Python package inventory; MongoDB bootstrap/indexes and in-memory Secrets; serving deployment/readiness; exact NetworkPolicy inventory; complete live ownership/storage/image receipt and comparison | Added fake/static tests under `test_k8s_runtime.py`, `test_k8s_lock_lifecycle.py`, `test_k8s_image_provenance.py`, `test_k8s_mongo_secrets.py`, `test_k8s_network_policy.py`, `test_k8s_deployment.py`; extended ownership/static suites. Executed 47 focused K8s `unittest` cases (PASS), AST parse for 21 Python files (PASS), YAML parse for 8 lock/manifests (PASS), offline Kustomize rendering as part of the suite (PASS), and `git diff --check` (PASS). Docker/Minikube/Kubernetes/MongoDB were not contacted. | `c728ded` |
| 3 Kubernetes B3 | Code implemented; runtime evidence and execution pending | Run-scoped HDFS/Kafka platform, six-step input/Module 1/2/4/replay/Hybrid pipeline, deterministic bounded fixture, full-data input receipt/transfer, pipeline image target, DataNode/KRaft service and NetworkPolicy contracts, durable task and artifact receipts, strict pipeline ownership/storage/image receipt | Added `test_k8s_pipeline.py`; executed 13 B3 cases and 19 related B2 ownership/network-policy/runtime/lock cases (32 total, PASS), Python bytecode compilation, all `deploy/k8s/**/*.yaml` YAML parsing, and `git diff --check` (PASS). No Docker, services, cluster, or full-data run. | pending |
| 4 Kubernetes B4 | Code implemented; runtime evidence and execution pending | Strict B2/B3 live receipt comparison; dedicated acceptance Job; fixture/full-data status scopes; HDFS/Kafka/batch/stream/watermark/checkpoint/Mongo/ALS/graph/Hybrid/API/UI/NetworkPolicy evidence; sanitized failure capture; separate namespace/profile teardown gate | Added `test_k8s_acceptance.py`; executed 41 focused B2/B3/B4 ownership, pipeline, policy, runtime, lock, and acceptance cases (PASS), Python compilation, all Kubernetes YAML parsing, and `git diff --check` (PASS). No Docker, services, cluster, acceptance Job, or teardown ran. | pending |
| 5 Experiments/evaluation | Not started | — | — | — |
| 6 Final runner | Not started | — | — | — |
| 7 Final documentation | Not started | — | — | — |

## Decisions and limitations

- B3 will target a single isolated Minikube profile and Kubernetes-owned HDFS, Kafka, MongoDB and pipeline storage. Spark will use `local[2]` inside run-scoped Kubernetes Jobs; this is not native Spark-on-Kubernetes executor scaling.
- No image digest, toolchain identity, cluster UID, runtime metric or successful runtime receipt will be invented. Values that require a later authorized image build or cluster are runtime-discovered lock fields.
- The complete lock remains a shared B2/B3 identity source. B3 finalization adds exact Kafka/HDFS/pipeline image provenance to the already complete serving and host lock; every selected image identity must be concrete before the common lock can pass complete-lock validation.
- Runtime deployment, Docker, package installation, full-data execution, existing service access, and teardown are prohibited for this campaign.
- B2's 47 focused offline K8s tests have run and passed. Milestone 1 application suites remain NOT RUN because the available Python runtime lacks pytest, pandas, PyArrow, FastAPI, PyMongo, Streamlit, and PySpark; no packages were installed.

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
