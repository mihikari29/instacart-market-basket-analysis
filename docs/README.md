# Documentation index

The repository root [README](../README.md) is the front door: it explains what
the project is, how the modules fit together and where every artifact lives.
This page lists the documents that own a specific subject.

## Project and status

| Document | Owns |
|---|---|
| [Proposal](Proposal.md) | The approved assignment requirements and architecture baseline. Immutable. |
| [Project progress](progress.md) | The team record: what is implemented, what was measured, what is still open, and the dated evidence for each claim. |
| [Implementation ledger](implementation-ledger.md) | Design decisions, safety measures and the limits of each implementation choice. |
| [External acceptance](acceptance.md) | The operator path from preparation to an evidence-backed acceptance report, plus the delivered scope and the boundaries of what has not been verified. |

## Operating guides

| Document | Owns |
|---|---|
| [Docker guide](docker.md) | Image targets, service prerequisites, every module's operating commands, synthetic event generation, the Spark executor layout experiment, validation commands and cleanup boundaries. |
| [Kubernetes guide](kubernetes.md) | The Kubernetes demonstration: design facts (identity, credentials, storage, network policy, version lock) and the operator procedure (preflight, run, verify, collect, teardown). |
| [Module 3 contracts](module3-contracts.md) | Kafka event provenance, event-time aggregation, publication, bundles, checkpoints and the Module 4C consumption boundary. |
| [Module 4 plan](module4-plan.md) | The Module 4A-4D algorithms, data contracts, ranking, evaluation, API routes and dashboard contracts. |
| [Coding conventions](coding-conventions.md) | The project's Python, PySpark, error-handling and testing rules. |

## Historical evidence

| Document | Owns |
|---|---|
| [Evidence index](evidence/README.md) | Dated validation reports, receipts and archived design documents, with the five files that live code registers. |
| [Module 4 evidence index](evidence/module4/README.md) | The eleven Module 4 run directories, their status, and which artifacts are unique to each run. |
| [Style regression validation](style-regression-validation.md) | The 15 October 2026 post-refactor comparison and its **NOT FULLY VERIFIED** assessment. |

Historical records keep their original dates, code and environment identities
and experiment boundaries. Updating this index does not turn a historical
result into validation of the current commit; see
[project progress](progress.md) for the present status.
