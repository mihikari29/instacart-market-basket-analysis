# Coding conventions

These conventions help developers change and review Python and PySpark code.
Use the existing module contracts and tests to assess correctness. Automated
agent instructions live in [`AGENTS.md`](../AGENTS.md); this page does not
replace that rulebook.

## Python quality and module boundaries

- Use the existing [Ruff configuration](../pyproject.toml): Python 3.10 target,
  110-character line length, and rules `E4`, `E7`, `E9`, `F`. This selection does
  not enforce every PEP 8 guideline. Avoid unrelated formatting churn.
- Use `snake_case` for functions and locals, `PascalCase` for classes, and
  `UPPER_CASE` for constants. Familiar analytical names such as `df`, `rng` and
  PySpark `F` are useful when their roles are clear.
- Group standard-library, third-party and project imports. Prefer explicit
  imports; keep lazy/optional imports where they protect CLI help or package
  startup from heavy runtime dependencies.
- Add type hints to stable interfaces when they clarify inputs and outputs.
  Keep core code compatible with Python 3.10. Use concise English docstrings
  for non-obvious assumptions, side effects and invariants.
- Keep data preparation, batch computation, streaming, publication and read
  interfaces within their owning modules. Shared helpers belong in
  `src/common/` when they express a real shared contract. Canonical serving
  packages are `src.module4.hybrid` and `src.module4.dashboard`.
- Make functions understandable by responsibility. Extract repeated logic when
  ownership, resource lifetime and exceptions stay clear; file length alone is
  not a reason to split orchestration.

## Spark transformations and data contracts

Use `from pyspark.sql import functions as F`, explicit joins and aggregations,
and parenthesized transformation chains. Name analytical windows and
intermediate frames by their role. During a refactor, account for join type,
broadcasting, null behavior, casts, column aliases, partitioning, sort order,
cache lifetime and checkpoint behavior.

Keep full-data operations distributed. Bounded report collection does not
justify collecting an unbounded DataFrame in the driver. Retain deterministic
ranking ties and validate equivalent schemas and row multisets when changing a
transformation; aggregate pass counts alone do not prove equivalent output.

Preserve prior/train separation: prior interactions feed analytics, ALS and the
co-purchase graph; official train baskets are evaluation truth. Generated
purchase time is **Synthetic event time for replay simulation**. A mixed
prior/train Kafka feed does not become leakage-safe merely by applying an
event-time cutoff.

Module 3 watermarking and deduplication precede stateful aggregation. Normalize
and rank finalized batches before Top-K selection. The
[streaming contracts](module3-contracts.md) and
[Module 4 specification](module4-plan.md#module4c-spec) own these details.

## Errors, tests and reproducibility

Validate inputs before side effects and keep failures visible. Preserve useful
exception types, sanitized diagnostics, exit codes and scoped cleanup. Do not
log credentials or raw connection strings, turn missing values into successful
zero results, or catch an error merely to continue publication.

Use behavior-focused tests for success, failure, recovery, security and boundary
conditions. A shared function can need several distinct tests. Service-backed
tests must declare their prerequisites and preserve their intended fail/skip
contract. Keep the repository fixture, UI, isolated MongoDB and Kubernetes
selections independently runnable; see [validation commands](docker.md#validation-commands)
and the [offline selection](acceptance.md#phase-1).

Run Ruff, relevant pytest selections, Python compilation and `git diff --check`
for code changes; validate Compose when its configuration changes. Use the
[documented runtime](docker.md), including Spark 3.5.5/Java 17 for Spark checks
and the separate API, UI and browser environments. Report skipped and unrun
checks explicitly. Fake clients, offline rendering and cached imports do not
establish live deployment acceptance.

Keep seeds, entity-keyed randomness, timestamps, schemas, evaluation splits,
scoring arithmetic, source identities and publication ordering reproducible.
Record input receipts, effective configuration, image/runtime identities and
output provenance in run artifacts. Do not edit historical measurements to
make a changed implementation appear equivalent. Explain measured performance
within its actual data, cache, hardware and execution scope.

## Historical lab references

The [historical instructor-materials snapshot][instructor-snapshot] preserves
the exact material used to establish this guidance. Its pinned link is retained
for provenance; it does not establish the current course identifier or approval
of the project's revised proposal. Paths below are relative to that snapshot.

| Material | Useful guidance for this project |
|---|---|
| `README.md`, “Code Practices” | Version control, readable comments, PEP 8, testing, and documenting findings. |
| `NoSQL_lab/notebooks/02_mongodb_exercises.ipynb`, “Submission Guidelines” | Meaningful names and type hints; tests for inputs, edge cases and errors; documented usage and assumptions. |
| `Spark_lab/README.md`, “Best Practices” | Scripts for operational work, validation, logging, error handling, and suitable storage/transformation choices. |
| `Kafka_lab/README.md` and `Kafka_lab/notebooks/04_offset_management.ipynb` | Offset handling, failure recovery, lag monitoring, and idempotent processing. |

`Spark_lab/notebooks/01_spark_batch_processing.ipynb`,
`Spark_lab/notebooks/02_spark_streaming.ipynb`, and
`Spark_lab/test_spark_connectivity.py` provide examples of named DataFrames,
windows and small documented functions.
They do not prescribe a formatter, function-length limit, annotation quota or
particular project architecture. Embedded demo credentials, wildcard imports
and broad notebook exception handlers are not patterns to copy into runtime
code.

[instructor-snapshot]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/tree/5f8ed408691485f954a3a07fe3d23be789a39770
