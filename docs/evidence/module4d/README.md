# Module 4D Documentation Archive

This directory preserves the eight retired top-level Module 4C/4D documents.
Their detailed design notes, historical observations, and original release
records remain available here; the current sources of truth are consolidated in
the canonical guides below.

| Retired document | Current canonical location | Preserved archive |
|---|---|---|
| `module4c-evaluation-plan.md` | [Module 4 plan §19.4](../../module4-plan.md#module4c-spec) | [evaluation-plan.md](../module4c/evaluation-plan.md) |
| `module4c-release-review.md` | [Project progress §7](../../progress.md#progress-module4c) | [release-review.md](../module4c/release-review.md) |
| `module4c-streaming-demo.md` | [Docker guide](../../docker.md#module-4c-controlled-streaming-demo) | [c8b-streaming-demo.md](../module4c/c8b-streaming-demo.md) |
| `module4d-plan.md` | [Module 4 plan §20](../../module4-plan.md#module4d-spec) | [architecture-plan.md](architecture-plan.md) |
| `module4d-api.md` | [Module 4 plan §20.1–20.2](../../module4-plan.md#module4d-spec) and [Docker guide](../../docker.md) | [api-contract.md](api-contract.md) |
| `module4d-data-contracts.md` | [Module 4 plan §20.2 and §20.4](../../module4-plan.md#module4d-spec) | [data-contracts.md](data-contracts.md) |
| `module4d-release-review.md` | [Project progress §8](../../progress.md#progress-module4d) | [release-review.md](release-review.md) |
| `module4d-streamlit.md` | [Module 4 plan §20.3](../../module4-plan.md#module4d-spec) and [Docker guide](../../docker.md) | [streamlit-implementation.md](streamlit-implementation.md) |

The original standalone documents are retained as historical appendices. The
canonical plan consolidates current design and interface contracts; progress
records completed, tested, full-data-validated, and unevaluated claims; and the
Docker guide owns startup, demo, validation, and cleanup procedures. Historical
measurements and failures in release-review documents and C8 receipts remain
attributed to their recorded dates, runs, and source revisions. Relocation only
required updating relative Markdown link targets; recorded measurements,
conclusions, and revision identities were left intact.

Module 4D's local D3 release review and its reported GitHub checks refer to
implementation head `172f2f748a28bb9b23b98063173f8dfe7136d7f8` and PR #7. They
are not validation of later commits or this documentation-only branch. Read the
release review for its exact environment, test split, browser results, report
digests, and limitations.
