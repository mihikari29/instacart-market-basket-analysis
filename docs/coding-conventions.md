# Coding conventions

Correctness and the project's technical contracts take priority over lab style,
then cosmetic consistency. Apply these conventions to code being changed; avoid
repository-wide formatting or annotation churn.

## Instructor guidance and project choices

The materials below were inspected at instructor commit
[`5f8ed408691485f954a3a07fe3d23be789a39770`][reference]. Links pin that revision
so documented requirements can be distinguished from examples.

| Instructor material | Documented principle | Adoption in this project |
|---|---|---|
| [Root README, Code Practices][practices] | Use Git, clear comments, PEP 8, testing before submission, and documented findings/issues. | Focused branches and diffs, readable Python, relevant regression checks, and runbooks. |
| [NoSQL MongoDB exercises, Submission Guidelines][nosql] | Meaningful names, appropriate type hints, readable code and comments; test inputs, edge cases, errors and expected outputs; document assumptions, usage and performance. | Clarify stable interfaces and non-obvious invariants; retain acceptance tests and recorded evidence. |
| [Spark README, Best Practices][spark] | Use scripts for production, proper error handling, logging, validation, Parquet, and appropriate distributed operations. | Keep the modular Python pipeline, explicit DataFrame transformations, existing error paths and runtime boundaries. |
| [Kafka README][kafka] and [offset-management notebook][offsets] | Cover offsets and recovery; the notebook recommends handling failures, monitoring lag and idempotent processing. | Preserve the existing acknowledgement, deduplication and recovery contracts. |

The [batch][batch] and [streaming][streaming] notebooks illustrate named
DataFrames, joins, aggregations, window specifications and event-time reasoning.
The [Spark connectivity script][script] also illustrates small functions with
docstrings. These examples inform readability; they do not define a mandatory
formatter, alias, function length, docstring template or annotation quota.
Wildcard imports, embedded demo credentials, notebook globals and broad demo
exception handlers are teaching details, not conventions to copy.

## Python style and interfaces

- Follow PEP 8 with the project's existing Ruff settings in
  [pyproject.toml](../pyproject.toml): Python 3.10 target, 110-character line
  length, and lint rules `E4`, `E7`, `E9`, `F`. Keep dependencies and policy stable.
  Ruff does not enforce every PEP 8 guideline under this selection.
- Use `snake_case` for functions and local variables, `PascalCase` for classes,
  and `UPPER_CASE` for constants. Prefer descriptive locals; keep familiar names
  such as `df`, `rng`, and PySpark `F` when their role is clear.
- Group standard-library, third-party and project imports. Prefer explicit
  imports; preserve optional/lazy imports that protect runtime boundaries.
- Preserve exported names, import paths, parameter names, CLI flags, environment
  variables, schemas and externally consumed identifiers during style changes.
- Add simple type hints to stable interfaces when they clarify inputs or outputs.
  Keep core code compatible with Python 3.10; annotations must not add validation
  or require a new runtime dependency. Mixed report dictionaries need no elaborate
  type hierarchy solely for style.
- Use concise English docstrings for unclear purpose, return values, side effects
  or assumptions. Explain why a transformation or guard exists, especially
  leakage prevention, ordering and recovery invariants. Avoid narrating obvious
  statements or adding docstrings to every trivial helper.
- Prefer functions with one understandable responsibility. Extract helpers only
  when ordering, exceptions, resource lifetime and atomicity remain demonstrably
  equivalent; file length alone is not a reason to split orchestration.

## PySpark transformations

- Keep `from pyspark.sql import functions as F`. Use explicit joins, aggregations
  and parenthesized chains; wrap dense expressions without changing their order.
- Give analytical windows and intermediate frames meaningful names. Preserve
  join types, broadcast hints, grouping keys, casts, null behavior, aliases,
  sort direction, tie-breaks, partitioning, persistence and checkpoint behavior.
- Keep distributed work distributed. Existing collection for bounded report
  views does not justify replacing a distributed transformation with `collect()`.
- Preserve prior/train separation and calendar-range definitions. Keep Module 3
  watermarking and deduplication before its stateful aggregation; normalize and
  rank finalized static batches before selecting Top-K.

## Errors, testing and reproducibility

Keep failures explicit and preserve their type, message and cleanup behavior.
Do not hide errors, embed credentials, broaden access, change retries, or alter
security validation as a style fix. Existing logging and observable diagnostics
remain part of the contract.

Use the established runtime from the [Docker runbook](docker.md): core Spark
checks use `instacart-spark:3.5.5` (Python 3.10, Spark 3.5.5, Java 17); API, UI and
browser checks retain their separate dependencies. Before editing, inspect the
Git diff and establish relevant lint/test results. After editing, run `ruff check .`,
relevant pytest suites, Python compile checks and `git diff --check`. Validate
Compose if configuration changes; run relevant Spark/integration checks if
executable logic is reorganized. Report failures, skips and unrun checks honestly.
Use existing behavior-focused tests for style-only changes.

Preserve seeds, entity-keyed randomness, timestamp calculations, schemas,
evaluation splits, scoring arithmetic, ranking ties, version identities and
publication ordering. Keep Module 1–4 ownership and Lambda Architecture intact.
Never rewrite `docs/evidence/`, historical measurements, datasets or generated
results to accommodate a style change. Defer any refactor whose equivalence
requires substantial new behavioral or integration reasoning.

[reference]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/tree/5f8ed408691485f954a3a07fe3d23be789a39770
[practices]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/README.md#code-practices
[nosql]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/NoSQL_lab/notebooks/02_mongodb_exercises.ipynb
[spark]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Spark_lab/README.md#-best-practices
[kafka]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Kafka_lab/README.md
[offsets]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Kafka_lab/notebooks/04_offset_management.ipynb
[batch]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Spark_lab/notebooks/01_spark_batch_processing.ipynb
[streaming]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Spark_lab/notebooks/02_spark_streaming.ipynb
[script]: https://github.com/trungtv/IT4931_data_management_and_processing_lab_materials/blob/5f8ed408691485f954a3a07fe3d23be789a39770/Spark_lab/test_spark_connectivity.py
