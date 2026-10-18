# External acceptance handoff

**CODE COMPLETE — READY FOR EXTERNAL ACCEPTANCE. Runtime acceptance is pending.**
The final independent source-code review passed at the implementation revision
identified in the [implementation summary](implementation-ledger.md#reviewed-source-revision).
This handoff distinguishes recorded development checks from the work that still
requires a runtime environment.

## Implemented scope

| Area | Delivered behavior | Main reference |
|---|---|---|
| Modules 1–2 | Validated ingestion, deterministic replay event generation, HDFS staging, batch analytics, prior-history features, and execution comparison tools. | [Repository layout](repository-layout.md), [Docker guide](docker.md) |
| Module 3 | Kafka event-time aggregation, bounded replay reconciliation, checkpoint handling, finalized trend publication, and completed-batch telemetry. | [Module 3 contract](module3-contracts.md) |
| Modules 4A–4C | Prior-only ALS and product graph outputs, Hybrid ranking/evaluation, and versioned publication. | [Module 4 plan](module4-plan.md) |
| Module 4D | Read-only API and dashboard for recommendations, batch metrics, trends, and evidence. | [Module 4 plan](module4-plan.md), [Docker guide](docker.md) |
| Kubernetes | Isolated B2 serving, B3 processing, B4 acceptance, evidence capture, and separately gated teardown. | [Kubernetes runbook](kubernetes-demo-runbook.md) |

## Recorded development checks

Checks associated with the reviewed source revision:

- Final independent source-code review: **PASS**.
- Broad non-service suite: **803 passed, 7 skipped**.
- Focused telemetry/BSON suite: **59 passed**; focused telemetry, API, and UI
  selection: **134 passed**.
- Repository-wide Ruff: **PASS**; Python compilation: **190 files PASS**.
- Offline Kubernetes fake-client/static checks, local rendering, and runner
  checks: **PASS** as summarized in the [implementation ledger](implementation-ledger.md).

The tests ran with host Python 3.14 and temporary lightweight dependencies.
They did not validate the pinned Spark 3.5.5 / Java 17 image environment. The
seven skipped cases require PySpark; service-backed MongoDB checks and selected
PySpark-importing integration modules were excluded. These results do not
establish live service behavior.

The prior implementation push used `[skip ci]`; no GitHub Actions result is
claimed for that source revision. This documentation-only update also does not
claim a new CI result. Historical workflow and full-data results keep their
original source revision and scope in the [evidence archive](evidence/README.md).

## External validation still required

1. Run the applicable fixture and integration suites in the pinned Spark and
   MongoDB environment, including the skipped Spark-dependent cases.
2. Complete browser checks against the isolated dashboard fixture.
3. Build and inspect the `k8s-runner`, `ui`, and `pipeline` image packages.
4. Run Kubernetes B2/B3/B4 on a suitable isolated host; verify ownership,
   storage, service readiness, MongoDB permissions, API/UI behavior, network
   policy, and retained evidence.
5. Exercise streaming checkpoint/recovery behavior with the selected runtime.
6. Run full-data processing with exactly the six reviewed CSV inputs and retain
   their source receipts. A bounded fixture is not full-data acceptance.
7. Measure actual performance, including streaming latency and the
   standalone executor-layout comparison.
8. Record fresh acceptance evidence with the exact source revision, inputs,
   environment, commands, results, skips, and limitations.

Use the [Docker validation and operating guide](docker.md#validation-commands)
for test ownership and service procedures, the
[Kubernetes demonstration runbook](kubernetes-demo-runbook.md) for capacity,
execution, recovery, evidence, and teardown, and the
[executor benchmark guide](module2-executor-benchmark.md) for the separate
standalone Spark experiment. This handoff is an overview; those guides own the
operational details.

## Operator preparation and safeguards

- Prepare the six reviewed source CSVs for a full-data run and verify their
  recorded hashes before execution. The Kubernetes runbook caps raw input at
  4 GiB.
- Use a suitable dedicated runtime environment. The Kubernetes runbook
  specifies a minimum of 6 physical CPU cores, 12 GiB available RAM, and 40 GiB
  free disk; WSL operators must separately measure backing-disk capacity.
- Keep each Kubernetes run bound to a fresh dedicated profile, context,
  namespace, storage, image identities, and run ID. The committed version lock
  is a template; do not invent values for measured runtime identities.
- Keep persistent Compose data, existing checkpoints, credentials, datasets,
  and historical evidence outside cleanup actions. Review the run-specific
  teardown plan and receipts before using its explicit execution flag.
- Treat fixture, full-data, CI, offline static checks, and runtime acceptance
  as separate evidence scopes. Synthetic timestamps describe replay, not actual
  purchase times.

The detailed design and verification boundaries are in the
[implementation summary](implementation-ledger.md). The approved Proposal and
dated historical evidence remain the requirements and provenance records.
