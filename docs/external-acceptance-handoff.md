# External acceptance handoff — 18 October 2026

**IMPLEMENTATION COMPLETE — AWAITING INDEPENDENT REVIEW.** This source is ready
for an independent review before handoff to the external acceptance operator.
Runtime, full-data and production acceptance are not claimed.

At the earlier review baseline `b4a9bbe19e5949a0b181151ea605b54ed670b6a9`,
Sol found two P1 defects. The `1a25d10` correction uses Spark 3.5.5's
`durationMs.addBatch` marker, recorded inside `MicroBatchExecution.runBatch`,
to retain zero-input state-cleanup batches while ignoring idle records. It
pairs each executed batch with its same-ID sink receipt, processes every new
record, applies bounded backpressure, and reports sanitized failures.

The subsequent review of `1a25d108f13baa1db5c41e0d222e74e07a708ebf` found two
P2 defects. `run_stream` now calls synchronous `query.stop()` even after Spark
reports the query inactive, then signals the progress logger. The logger treats
that owner signal as the only shutdown condition and performs its final drain
after `stop()` returns, when Spark's execution thread has finished reporting
progress. Deterministic tests drive the actual `run_stream` coordination and
logger through delayed final progress, zero-input cleanup, multiple final
batches, missing receipts, publication errors, idle ID reuse, and empty
shutdown.

Unavailable latency now requires the exact three-field schema and a strict
bounded integer zero sample count. BSON `Int64(0)` remains valid, while booleans,
floats and out-of-contract values fail. The reader converts valid BSON integer
subclasses to ordinary Python integers, including the unavailable sample count,
before returning JSON. Real BSON round trips exercise reader rejection,
HTTP 502 `invalid_snapshot`, and UI-client rejection for malformed documents;
valid Python and BSON integer zeros reach HTTP 200 and pass the UI validator.

Use `feat/offline-project-finalization` in
`/home/minh/projects/instacart-market-basket-analysis-finalize`. The remediation
starts from reviewed HEAD `1a25d108f13baa1db5c41e0d222e74e07a708ebf`; verify
the final source identity with `git rev-parse HEAD` against the remote branch
before acceptance. Main and PR #16 are outside this handoff; the PR remains
closed and unmerged.

## Completed milestones and source map

| Milestone | Files and resulting behavior |
|---|---|
| 1. HDFS and storage | `scripts/k8s/{hdfs_lifecycle,pipeline,pipeline_task,acceptance_task,runtime}.py`: checked fresh run-only HDFS ownership, consistent `k8spipeline` identity, Immediate binding admission. |
| 2. Finite replay | `src/module3/{replay,stream}.py` and K8s pipeline/acceptance tasks: producer success, terminal offset drainage, count reconciliation, compatible full-feed deadlines. |
| 3. Cleaning | `src/module1/clean.py`: contiguous 1..N baskets and reconstructed gap/weekday gates before any publication; failure preserves previous outputs. |
| 4. Hybrid parity | `src/module4/evaluation.py`: endpoint candidates follow serving, including cold-user fallback and deterministic zero-score ties. |
| 5. Executor layouts | `src/module2/{config,spark,executor_benchmark}.py`: explicit existing-standalone-master mode, distinct layouts, actual executor evidence; existing local-thread comparison retained. |
| 6. Streaming telemetry | `src/module3/{metrics,progress_log,stream,trend_bundle}.py`, dashboard reader/schema/API/client/Overview and `Dockerfile`: bounded same-batch rates/latency, executed-batch progress pairing including zero-input cleanup, BSON integer handling, indexed read-only lookup, source/capture identity and explicit unavailable/skew states. |
| 7. Cleanup and handoff | Current README/runbooks/contracts, offline tests, and `.github/workflows/kubernetes-packaging.yml`: corrected status, source-package checks, producer/reader regression and a manual service-free image import job. Removed unused destructive `reset_checkpoint` helper and obsolete PySpark test stubs; no runtime files were cleaned. |

Focused tests accompany each milestone. `docs/Proposal.md`, `docs/evidence/`,
raw data, runtime outputs, volumes, credentials and the original dirty checkout
were preserved. The runtime version-lock template remains incomplete: actual
host/cluster/image identities must be collected rather than invented.

## Executed development checks

Checks ran on host Python 3.14 using temporary package directories under `/tmp`,
outside the repository. Lightweight Python dependencies and the UI test runtime
were installed there; no infrastructure was installed. This is not validation
of the pinned Spark/Java container environment or every transitive dependency.

| Check | Final result |
|---|---|
| Focused telemetry/BSON regression tests | **59 passed**; adds production `run_stream` shutdown orderings A–H, including a query already inactive before cleanup, and actual BSON validation at validator, reader, HTTP and UI boundaries. |
| Focused telemetry, Module 4D API and UI client/AppTest selection | **134 passed**; includes HTTP 200 for current-epoch measured `Int64` values and valid unavailable Python/BSON integer zero, plus HTTP 502 for malformed unavailable counts. |
| Broad non-service pytest suite on this correction | **803 passed, 7 skipped**; zero failures. Includes fake K8s clients, local Kustomize rendering, cleaning of the bounded synthetic fixture, ranking/publication/readers, HTTP/client contracts, Streamlit AppTest and source-package imports. |
| Repository-wide Ruff | **PASS**, zero findings. |
| Python source compilation | **PASS**, 190 Python files compiled without writing bytecode. |
| YAML parsing | **NOT RUN**; no YAML manifests or workflow files changed in this correction. |
| `git diff --check` and protected-path comparison | **PASS**. |

These checks ran with Python 3.14 and temporary lightweight packages under
`/tmp`, outside the repository. No full Spark process or service was started.
The follow-up correction push uses `[skip ci]`; no GitHub Actions result is
claimed for it. Historical green workflow runs attached to `c169b76` do not
validate either later correction.

The broad suite used this selection (with pytest/application packages available):

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

Seven skipped modules require PySpark: Hybrid evaluation parity, Module 3,
bundle integration, trend bundle writer, Module 4 Spark fixtures, Spark batch
fixtures, and the new actual Module 2 producer → Parquet → Module 4 reader
handoff. Eight live MongoDB modules were excluded to avoid service access.
C8A responsiveness and C8B streaming-demo test modules were excluded because
their imports require unavailable PySpark. They are **NOT RUN** here.

Initial regression failures were resolved before final checks: delayed-binding
fixtures, obsolete API/UI route expectations, the Spark-dependent API import
smoke, a leaked legacy PySpark stub during collection, and a new packaging-test
parser error. Final results above supersede those development attempts.

## External operator tasks

1. Review the final feature revision and rerun the complete fixture suite in
   the pinned Spark 3.5.5 / Java 17 runtime, including the skipped parity and
   producer/reader cases. Run isolated MongoDB tests and browser checks.
2. Run the manual **Kubernetes packaging imports** workflow, or its build/import
   commands, for `k8s-runner`, `ui`, and `pipeline`. No image was built locally.
   The final push uses `[skip ci]` because automatic Module 4 CI builds images
   and starts runtime services. GitHub documents this marker in
   [Skipping workflow runs](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/skip-workflow-runs).
   No workflow was dispatched during this task.
3. Follow the [Kubernetes runbook](kubernetes-demo-runbook.md) on a suitable
   machine. Refresh capacity evidence (6 physical cores, 12 GiB available RAM,
   40 GiB free disk; independently measured WSL backing disk when applicable).
   Validate selected releases, profile-local Immediate storage and image/tool
   identities. Finalize a private lock copy; keep the committed template intact.
4. Perform an explicitly authorized fresh isolated fixture B2 → B3 → B4 run.
   Verify HDFS ownership and Spark writes, producer/offset reconciliation,
   checkpoints and final-window tail disclosure, Mongo permissions/indexes,
   actual metrics through API/UI, startup/readiness, NetworkPolicy and retained
   evidence. Offline mocks do not establish these behaviors.
5. Perform separately authorized full-data acceptance with exactly six reviewed
   CSV inputs and the 4 GiB raw-input cap. Allow the full replay deadline and
   collect all source, delivery, consumption, output, model and quality receipts.
   The full feed is not shortened. Do not infer full-data success from a fixture.
6. Run the [standalone executor experiment](module2-executor-benchmark.md) for
   Proposal §15.6. The automated B3 local[1]/local[2] comparison measures task
   threads; it does not establish executor-layout performance. Record measured
   results and limitations without asserting an improvement in advance.
7. Review the interactive Proposal demo and the acceptance matrix. Collect
   success/failure artifacts. Review teardown separately before executing it.

No full-data, Docker, Kubernetes, Kafka, HDFS, Spark, MongoDB-server, browser
runtime or production acceptance ran here. Measurements, image/cluster
identity, service behavior and acceptance of this revision remain unverified
until these operator tasks run. Historical results retain their original
dates/revisions. Code completion awaits the independent Sol 6.1 review of this
correction.
