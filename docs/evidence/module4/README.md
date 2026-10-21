# Module 4 evidence index

The Module 4 archive holds eleven dated run directories. Each one keeps the
exact artifacts its run produced, including the failed attempts, because a
failure record is the part of the audit trail that is hardest to reconstruct
later.

Two facts matter when reading these directories:

- The five files registered in `src/module4/dashboard/evidence.py` are a
  **runtime contract**. The dashboard serves them by path, and the API
  enforces a 256 KiB per-artifact limit. They must never move or be trimmed.
- The 10 October "fixed" pair is one run executed twice. The replay reuses the
  saved model, so several artifacts are byte-identical and the rest differ
  only in run identity, timings and last-digit PageRank floats. The unique
  value of the replay directory is that timing and reuse delta.

| Directory | Date | Scope | Status | What it records |
|---|---|---|---|---|
| [validation-20261008](validation-20261008/README.md) | 8 Oct | Host validation | stopped at operator request | Static checks, source preparation, replay failure, cross-module smoke. |
| [acceptance-20261009](acceptance-20261009/README.md) | 9 Oct | Full-data acceptance attempt | incomplete | Host contract tests, CI receipt, storage preparation, interrupted runs. |
| [full-input-20261009](full-input-20261009/README.md) | 9 Oct | Input validation only | passed | Full-input row and hash validation against the staged receipts. |
| [full-ab-20261009-optimized](full-ab-20261009-optimized/README.md) | 9 Oct | Full A/B attempt | failed | Interrupted during holdout preparation; the manifest holds the HDFS Safe Mode stack trace. |
| [full-ab-20261010-first](full-ab-20261010-first/README.md) | 10 Oct | Full A/B attempt | failed | The out-of-memory failure during model reload. No snapshot was published. |
| [flow-923e255d…-first](flow-923e255d911244e2b368be3809cc2bd7-first/README.md) | 8 Oct | Staged fixture flow | passed | First staged publication and read-back. |
| [flow-923e255d…-replay](flow-923e255d911244e2b368be3809cc2bd7-replay/README.md) | 8 Oct | Staged fixture replay | failed | Configuration drift; it stopped before producing ALS and graph artifacts. |
| [cross-932c44d…](cross-932c44d6fcc4439ba10d5f8e4979a272/README.md) | 8 Oct | Cross-module smoke | passed | Module 2 to Module 4 flow over a staged fixture. |
| **[full-ab-20261010-fixed-first](full-ab-20261010-fixed-first/README.md)** | 10 Oct | Full A/B acceptance | **passed** | The accepted first run: 21/21 gates, 4,124,180 recommendations, 49,688 graph documents. |
| **[full-ab-20261010-fixed-replay](full-ab-20261010-fixed-replay/README.md)** | 10 Oct | Full A/B replay | **passed** | The same-version replay that reuses the saved model and reproduces the logical digests. |
| [acceptance-20261010](acceptance-20261010/README.md) | 10 Oct | Acceptance procedure | complete | The acceptance checklist, the exact commands, the failure receipts and the read-back checks. |

## Which files are byte-identical

`full-ab-20261010-fixed-replay` is a re-execution of
`full-ab-20261010-fixed-first`. These artifacts are byte-for-byte identical
between the two directories:

- `evaluation.json`
- `graph_validation.json`
- `recommendation_validation.json`
- `split_summary.json`
- `test-results.txt`

The remaining artifacts differ only in `run_id`, the start and finish
timestamps, the recorded command line, the `reused_existing_model` flag, the
temporary staging collection names, and floating-point PageRank values that
agree to six decimal places.

`docs/evidence/SHA256SUMS` records the digest of every archived file, so
these relationships can be re-checked rather than rediscovered.
