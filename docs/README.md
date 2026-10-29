# Documentation index

Start with the [project overview](../README.md) for architecture and a first
synthetic dashboard run. Use [project status](progress.md) to distinguish
implemented features, historical observations, and acceptance still required.

## Project and requirements

| Document | Purpose |
|---|---|
| [Proposal](Proposal.md) | Repository requirements baseline. Its relationship to the latest revised proposal and course identifier needs confirmation; implementation status is separate. |
| [Project status](progress.md) | Capabilities, configuration boundaries, unfinished work, and validation limits. |

## Architecture and technical contracts

| Document | Purpose |
|---|---|
| [Implementation and design ledger](implementation-ledger.md) | Cross-module data, publication, recovery and isolation decisions. It contains active contracts despite its historical title. |
| [Module 3 contracts](module3-contracts.md) | Kafka schema, synthetic event time, windows, finalized bundles, checkpoints, and telemetry. |
| [Module 4 specification](module4-plan.md) | ALS, graph, evaluation, Mongo publication, Hybrid, API/UI and acceptance contracts. |

The Module 4 specification retains its established filename for references in
the requirements baseline and generated reports. Completed schedules and work
breakdowns have been removed; the acceptance checklist remains prospective.

## Development and operations

| Document | Purpose |
|---|---|
| [Coding conventions](coding-conventions.md) | Human-facing Python/PySpark, testing, and reproducibility guidance. |
| [Docker guide](docker.md) | Image targets, persistent versus disposable environments, Module 1–4 commands, indexes, experiments, and scoped cleanup. |
| [Kubernetes guide](kubernetes.md) | Run identity, prerequisites, lock preparation, rendering, execution, evidence collection, and separate teardown. Runtime acceptance remains pending. |

## Testing and acceptance

| Document | Purpose |
|---|---|
| [Docker validation commands](docker.md#validation-commands) | Repository fixtures, UI tests, API validation, and packaged runtime checks. |
| [Acceptance guide](acceptance.md) | Offline selection, service prerequisites, fixture/full-data acceptance procedures, and evidence requirements. Its dated source-review appendix is historical. |

A configured workflow, offline rendering pass, or historical acceptance result
is not a successful hosted or live run of the current source.

## Historical evidence available here

The full `docs/evidence/` archive is absent from this checkout. The runbooks identify missing historical records explicitly; no archive has
been recreated. The application retains five bounded artifacts with their original
provenance in [`src/module4/dashboard/assets/`](../src/module4/dashboard/assets/):

| Artifact | Recorded scope |
|---|---|
| [Batch analytics](../src/module4/dashboard/assets/analytics.json) | Historical product, department and order aggregates. |
| [Batch benchmark receipt](../src/module4/dashboard/assets/current-hardening-2026-10-03.json) | Historical single-host data, generation and performance scope. |
| [ALS evaluation](../src/module4/dashboard/assets/evaluation.json) | Official-train cohorts and baseline comparisons. |
| [Hybrid responsiveness receipt](../src/module4/dashboard/assets/c8a-receipt.json) | Controlled synthetic ranking response; no accuracy result. |
| [Streaming demonstration receipt](../src/module4/dashboard/assets/c8b-receipt.json) | Bounded event/bundle reconciliation, with recovery and cleanup limitations. |

The [historical style regression comparison](style-regression-validation.md)
preserves a **NOT FULLY VERIFIED** assessment, unexplained browser assertions,
and uncertainty about earlier persistent-volume effects. Its raw local
artifacts are not included here. Neither that narrative nor the five curated
artifacts replaces fresh end-to-end acceptance evidence.
