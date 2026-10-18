# Documentation index

Start with the root [README](../README.md) for the project overview.

## Project and status

- [Proposal](Proposal.md) — approved goals, requirements, and architecture.
- [Project status](progress.md) — current implementation, recorded checks,
  outstanding runtime validation, and links to historical results.
- [External acceptance handoff](external-acceptance-handoff.md) — delivered
  scope, operator preparation, and acceptance boundaries.
- [Implementation summary](implementation-ledger.md) — design decisions,
  important contracts, safety measures, and verification limits.
- [Repository layout](repository-layout.md) — code ownership and stable paths.

## Technical and operating guides

- [Docker guide](docker.md) — image builds, setup, module commands, validation,
  and service safety.
- [Kubernetes demonstration runbook](kubernetes-demo-runbook.md) — capacity,
  isolated execution, evidence collection, recovery, and teardown.
- [Module 3 contracts](module3-contracts.md) — Kafka event provenance,
  event-time processing, publication, and checkpoint contracts.
- [Module 4 plan](module4-plan.md) — recommendation, graph, Hybrid, API, and
  dashboard contracts.
- [Module 2 executor benchmark](module2-executor-benchmark.md) — local and
  standalone Spark comparison procedures.
- [Module 4 package migration](module4-package-migration.md) — compatibility
  links to the current package, CLI, API, and validation guides.
- [Module 4C evaluation plan](module4c-evaluation-plan.md) — compatibility
  entry point for the preserved evaluation design.
- [Coding conventions](coding-conventions.md) — project code style and
  behavior-preservation principles.

## Historical evidence

The [evidence index](evidence/README.md) links dated measurements, acceptance
records, and archived design documents. Historical records retain their
original scope and are not current runtime acceptance claims. The separate
[style regression report](style-regression-validation.md) documents an earlier
comparison and its limits.
