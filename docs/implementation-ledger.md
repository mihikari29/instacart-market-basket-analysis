# Implementation summary

This document summarizes the implemented system, its key contracts, and the
limits of the recorded verification. It is a technical reference rather than a
development timeline. The approved [Proposal](Proposal.md) remains unchanged;
dated measurements and acceptance records remain in the
[evidence archive](evidence/README.md).

## Implemented components

| Component | Implementation |
|---|---|
| **Module 1 — ingestion** | Validates the six source tables before publication, cleans and stages curated data, generates deterministic replay events, and supports Kafka delivery. |
| **Module 2 — batch** | Computes Spark analytics and prior-history user features, publishes results, and provides local-thread and existing-standalone-master comparison modes. |
| **Module 3 — streaming** | Consumes Kafka events, performs event-time aggregation, finalizes trend bundles, tracks checkpoints, reconciles bounded replay, and publishes completed-batch telemetry. |
| **Modules 4A–4B — recommendation and graph** | Builds prior-history ALS recommendations and product graph outputs, with versioned publication and evaluation contracts. |
| **Module 4C — Hybrid** | Reads eligible batch snapshots and finalized trend bundles, applies deterministic ranking, evaluates prior-only inputs, and publishes versioned results. |
| **Module 4D — read interface** | Provides read-only FastAPI routes and a Streamlit dashboard for recommendations, metrics, trends, and registered evidence. |
| **Kubernetes demonstration** | Provides isolated B2 serving, B3 data processing, B4 runtime acceptance, a one-session runner, private receipts, and separately gated teardown. |

Source ownership and package paths are mapped in the
[repository layout](repository-layout.md). Runtime commands and prerequisites
are in the [Docker guide](docker.md) and [Kubernetes runbook](kubernetes-demo-runbook.md).

## Design decisions and contracts

### Data and evaluation

- The source dataset has no absolute purchase timestamps. Generated event times
  are replay timestamps and must not be described as actual purchase times.
- ALS training and product graph inputs use prior-history data. Official train
  baskets are evaluation truth and are not ALS training data.
- Module 1 gates raw-table structure, domains, keys, relationships, and basket
  positions before replacing published output. A failed validation preserves
  the prior publication.
- Module 2 features describe prior history. They do not silently change the
  ALS fitting contract.

### Streaming and publication

- Kafka delivery, event-time progress, and ingestion time are distinct. A
  bounded replay is complete only after producer success and terminal offset
  reconciliation.
- `realtime_trending` retains the mutable legacy output contract. Optional
  `realtime_trending_windows` bundles provide finalized, immutable input for
  Module 4C and the Module 4D trends view.
- Completed-batch telemetry pairs a real executed Spark batch with that batch's
  sink receipt. It accounts for zero-input cleanup batches, bounded receipt
  backpressure, shutdown progress, and explicit unavailable measurements.
- Telemetry schemas validate BSON integer values at the API boundary and return
  ordinary JSON-compatible integers. Invalid documents fail closed.
- Checkpoint ownership and continuity are explicit. A failed or interrupted
  run is not automatically resumed without matching runtime ownership and
  offset evidence.

The detailed field, ranking, retry, and storage contracts are in the
[Module 3 guide](module3-contracts.md) and [Module 4 plan](module4-plan.md).

### Runtime isolation and security

- Kubernetes runs use a dedicated profile, context, namespace, run ID, storage,
  and image identities. The committed version-lock template stays incomplete
  until real environment and image identities are measured.
- Kubernetes Spark execution uses local mode inside run-owned Jobs; it is not
  native Spark-on-Kubernetes executor scaling.
- Kubernetes MongoDB admin, publisher, and API-read identities are distinct.
  Generated credentials are sent through the selected context and are not
  written into receipts. Network policy claims require live allowed/denied
  probes.
- Kubernetes runs retain resources and evidence after success or failure.
  Teardown is a separate operation that revalidates ownership and requires an
  explicit execution flag.
- The isolated synthetic dashboard demo uses disposable storage and loopback
  ports. The root Compose project uses persistent development data and has
  separate cleanup boundaries.

See the [Kubernetes runbook](kubernetes-demo-runbook.md) and
[Docker guide](docker.md) for exact configuration values, commands, and safety
constraints.

## Verification boundary

The independent source-code review passed at the reviewed revision. Recorded
offline development checks for that revision include:

- Broad non-service suite: **803 passed, 7 skipped**.
- Focused telemetry/BSON regression suite: **59 passed**; focused telemetry,
  API, and UI selection: **134 passed**.
- Repository Ruff: **PASS**; Python compilation: **190 files PASS**.
- Offline Kubernetes fake-client and static checks, local plan/preflight, and
  packaging/source checks are summarized in the
  [acceptance handoff](external-acceptance-handoff.md).

These results do not establish pinned Spark or MongoDB integration, browser
behavior, container image build/package correctness, Kubernetes B2/B3/B4
behavior, full-data processing, runtime checkpoint recovery, measured
performance, or external runtime acceptance. No fresh CI or runtime result is
claimed for a documentation-only change.

Historical Module 4A/4B acceptance and Module 1–3 measurements retain their
original scope and dates in the [evidence archive](evidence/README.md). The
[style regression report](style-regression-validation.md) remains a separate
historical comparison record; it is not current runtime acceptance evidence.

## Review provenance

<a id="reviewed-source-revision"></a>

The final independent source-code review passed at:

`a5fe5003134efb26f2a8826ce51e98bc291a2ff9`

That revision is the source identity associated with the offline results above.
The exact revision for any later runtime acceptance must be recorded with its
inputs, environment, command results, and evidence.
