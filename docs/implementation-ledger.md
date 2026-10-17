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
| 1 Application completeness | Implemented; automated suite blocked by missing runtime dependencies | Module 1 preflight/atomic publisher; ALS reader/API/schema/client/UI; Module 2 provenance; Module 4 feature reader/CLI; focused tests | Added tests in `test_module1_hardening.py`, `test_module4d_readers.py`, `test_module4d_api.py`, `test_module4d_ui_client.py`, `test_module4d_ui_app.py`, `test_module4_feature_reader.py`, and `test_k8s_static.py`. Executed AST parse for 18 changed Python files, `git diff --check`, and a five-case direct feature-reader smoke. `pytest` suite NOT RUN: pytest and application dependencies are absent. | Pending |
| 2 Kubernetes B2 | Not started | — | — | — |
| 3 Kubernetes B3 | Not started | — | — | — |
| 4 Kubernetes B4 | Not started | — | — | — |
| 5 Experiments/evaluation | Not started | — | — | — |
| 6 Final runner | Not started | — | — | — |
| 7 Final documentation | Not started | — | — | — |

## Decisions and limitations

- B3 will target a single isolated Minikube profile and Kubernetes-owned HDFS, Kafka, MongoDB and pipeline storage. Spark will use `local[2]` inside run-scoped Kubernetes Jobs; this is not native Spark-on-Kubernetes executor scaling.
- No image digest, toolchain identity, cluster UID, runtime metric or successful runtime receipt will be invented. Values that require a later authorized image build or cluster are runtime-discovered lock fields.
- Runtime deployment, Docker, package installation, full-data execution, existing service access, and teardown are prohibited for this campaign.
- No tests have been run yet. Test safety and available dependencies will be checked before each targeted invocation.

### Milestone 1 implementation record

- Module 1 now requires all six tables, checks exact headers and raw domains/keys/relationships before numeric coercion, rejects non-canonical integer spellings that could alias after conversion, and stages all Parquet outputs before directory publication. Failure diagnostics omit source values; valid reordered headers and first-order nullable gaps remain supported.
- The ALS API reads only the active paired Module 4A/4B snapshot, validates user/model/rank/score provenance, applies indexed Top-20 queries, enriches from the matching graph version when metadata exists, and rechecks the readiness identity. Empty user results and unavailable model versions have distinct contracts. Mongo write methods are not used.
- The dashboard requests ALS and Hybrid snapshots independently, shows source versions, aligns by product ID, and leaves unavailable scores blank. An empty ALS result is explicitly not proof that a user is absent from source data.
- Module 2 now stamps `user_features` rows with `module2_run_id`, `feature_scope=prior_only`, and UTC generation time. The new Module 4 `user-features --user-id` command reads one bounded descriptive row; the exact schema rejects unexpected fields and the consumer is explicitly excluded from ALS model inputs.
- Test execution blocker: `/usr/bin/python3` is Python 3.14.4 and has no pytest, pandas, PyArrow, FastAPI, PyMongo, Streamlit, or PySpark. No installation was attempted. The five-case feature-reader smoke ran with the Python standard library and passed; it does not replace the test suite.

## Remaining acceptance work

- Authorized host/toolchain/image discovery and a fresh isolated Minikube run.
- Real HDFS/Kafka/Spark/Mongo/API/UI readiness and acceptance evidence.
- Full-data acceptance under separately verified capacity/storage guards.
- Review of the completed code and ledger by the independent Sol 6.1 reviewer.
