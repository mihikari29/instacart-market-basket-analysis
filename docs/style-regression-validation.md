# Historical style regression comparison

**Recorded assessment: NOT FULLY VERIFIED.** This summarizes the comparison
performed on 15 October 2026. No refactor-induced logical fixture difference was
reported, but two original browser assertion failures remained unexplained,
possible earlier persistent-volume effects were indeterminate, and full-data
workflows were not run. It is not acceptance of the current cleanup worktree.

## Objective, source scope and evidence availability

The experiment compared a clean historical baseline with uncommitted
readability/configuration edits at the same HEAD. The changed historical paths
were `src/module1/config.py`, `src/module2/analytics.py`, `src/module2/config.py`,
`src/module3/config.py`, `src/module3/trending.py`, and coding conventions.
The edits concerned docstrings, annotations, imports, formatting and local
names. Review and executable-AST comparison reported no
algorithm change; Module 4 source was outside that edit scope.

The raw JUnit XML, command logs, fixture JSON, Parquet captures, footer diffs,
browser traces and Docker snapshots are **not included in this checkout**.
Some original browser traces were never retained. The observations below are
preserved historical results, not independently reverified measurements; the
available source and narrative alone cannot reconstruct the complete experiment.
No experiments were rerun for this documentation edit.

Essential recorded identities are kept together because the corresponding
technical artifacts are unavailable:

| Record | Historical identity |
|---|---|
| Baseline Git revision | `b89e2787a4ea0faa5bbcfa36b997ec511d292d19` |
| Shared Spark image | `sha256:7944963206c8f4d1bf29db42c9c4900ba8ceb17a1dfd1f0afc577b4878075636` |
| Isolated MongoDB image | `sha256:c630c59342c1493d50345136df2af14a76b9e827dd5316bfabee07a0880a5f3a` |
| Equal deterministic fixture JSON | `sha256:c48e8cd8411094572213fda1166a0aba36afe7377932cae67933bfc103686e12` |

## Environment and method

Both source variants used Python 3.10.12, Java 17.0.14, Spark 3.5.5, Scala
2.12.18, Ruff 0.11.13, pytest 8.4.2, NumPy 2.2.6, pandas 2.3.3, PyArrow 25.0.1,
PyMongo 4.11.3, GraphFrames Python 0.6 and kafka-python 3.0.11. The recorded host
used Python 3.14.4, Java 21.0.12.1, Docker 29.8.1 and Compose 5.5.1. Browser
checks used Playwright 1.63.0/Chromium 153.0.8010.12; the dashboard test image
used Python 3.14. These describe the historical environment, not today's tools.

Live MongoDB used private networks, tmpfs databases, no published host port and
isolated `i1-mongo`, `i2-mongo`, `i3-mongo` aliases. Stream, HDFS and dashboard
fixtures used unique projects/resource identities with scoped cleanup. Matching
source variants received identical fixtures and settings. Current operator
commands live in the [Docker runbook](docker.md#validation-commands); they are
not substitutes for the missing historical command logs.

Comparison covered seeded Module 1 samples; Module 2 schemas/counts/metrics,
ordered outputs and features; Module 3 schemas/window ranks/Top-K; Hybrid
recommendations; and representative error type/message contracts. Deterministic
fixture JSON was reported byte-identical without removing business fields.
Late-data comparison excluded only generated query IDs and execution times.
Test selections below overlap and must not be summed.

## Recorded outcomes

| Scope | Baseline and refactored observations | Interpretation |
|---|---|---|
| Static checks | Ruff, compilation, Compose configuration and diff checks passed on both. | Source checks only. |
| CI selection without Mongo configuration | Both: 690 passed, 69 skipped, zero failures. | Conditional live-service skips, not completed integration checks. |
| Same selection with isolated live Mongo | Both: 759 passed, zero skipped/failed; identical test identities/outcomes; five Spark warnings. | Reported fixture/service equivalence within that selection. |
| API/isolated Mongo harness | Both: 497 unit and 26 live passes, startup/OpenAPI/Ruff checks, 21 expected HTTP statuses and read-only/index/credential assertions. | API package/fixture scope. |
| Streamlit UI | Both: 64 passed. | Separate UI selection. |
| Module 3 smokes | Each 500-event smoke acknowledged all events, failed none and wrote 1,860 rows, with no invalid scores, duplicate keys/ranks, missing fields or windows over Top-K. Two-process recovery kept query/checkpoint identity and ended at 96 documents without duplicate groups. | Bounded legacy-stream recovery; not all bundle/full-data recovery paths. |
| Late-data fixture | Both accepted the in-watermark record, dropped the too-late record and duplicate ID, counted the malformed record as invalid, and matched selected window counts/ranks. | Controlled event-time behavior. |
| Module 4 runtime and A/B flow | GraphFrames/standalone Spark, HDFS round trip and Mongo ping passed. First/replay output had nine recommendation and five graph documents. Both staged 8 orders, 10 prior, 4 train and 5 products with equal normalized business summaries. | Four raw Parquet hashes differed; strict byte equality failed. See below. |
| Module 1→2→4 handoff | Equal source receipts/counts (16/24/12/6), four user features, matching downstream counts and ready A/B pair. | Small cross-module fixture only. |
| Chromium | Baseline failed two assertions, then passed; refactored passed initially. One later matched rerun on each source also passed. | Original assertion failures remain unexplained. |
| Full-data Module 2/3/4 workflows | NOT RUN for either source. | No full-data equivalence or fresh hosted Actions result established. |

## Parquet differences and logical equivalence

The flow fixture wrote one unpartitioned Parquet file per table. Its source
receipt hashed file bytes, including data pages and footer metadata, rather
than filenames, directory layout, HDFS timestamps, `_SUCCESS` or the separate
receipt file. Current ownership is visible in the
[fixture flow runner](../scripts/module4_smoke.py) and
[source reader](../src/module4/data.py); their present bytes are not the
historical artifact capture.

The following recorded identifiers are retained for interpretation. File bytes
are unavailable here; hashes have not been recomputed in this phase.

| Table | Schema (all fields nullable; no field metadata) | Key | Rows | Baseline hash | Refactored hash | File bytes |
|---|---|---|---:|---|---|---:|
| `orders` | `order_id: long, user_id: integer, eval_set: string, order_number: integer` | `order_id` | 8 | `b06f16ea9a7f15f63fc18c93ca240e5621e586f3e03074a27df88877116e929a` | `209008f2aa291067370bd55762701afb77747bc1f582808647264553f31cf963` | 1404 |
| `order_products_prior` | `order_id: long, product_id: integer, add_to_cart_order: integer, reordered: boolean` | `(order_id, product_id)` | 10 | `4013ed63f4cb70a9453904ac15257ac3c7c69aef04f99b66ba5bdc5e47123d5b` | `9b8d6268750c6529cf2cb8dcb34f0e984101d1ab9664c6d6047129de104dd6e2` | 1370 |
| `order_products_train` | `order_id: long, product_id: integer, add_to_cart_order: integer, reordered: boolean` | `(order_id, product_id)` | 4 | `859fbda042b3ee4dc1ff0f82039bd8ed8517ad19d5c4fa766d825489f57faba2` | `2027b584e54ec3fc51d9ffbbf2df7ebe2ac0da72cdbd611c107c43c749d2d92d` | 1316 |
| `products` | `product_id: integer, product_name: string, aisle_id: integer, department_id: integer` | `product_id` | 5 | `5b0c6da362d776d118573b9b1aded5a2e6494ca51915f56565127c5f2b8863fb` | `d5e749d27725a36ba1e0c04b8d3d64d89f0590c6d63303c1a00e87f3507616e1` | 1361 |

Follow-up captures reportedly reproduced both hash sets under both code
versions. Every pair had equal length, one row group, equal complete schemas
including nullability/metadata, exact counts, bidirectional full-row
`exceptAll`, and grouped-key multiset comparisons preserving duplicates. Null
counts matched at zero. No row ordering contract was declared; a separate
single-file diagnostic also observed the same sequence.

The report attributes each difference to serialized ordering of identical
encoding enum values in the footer's per-column `encodings` arrays. Data-page
prefixes, decoded metadata, schema, Snappy compression, encoding sets,
`parquet-mr 1.13.1` writer identity and Spark 3.5.5 metadata matched. The recorded
classification is **serialization-only difference in these bounded fixtures**.
It explains the reported hash difference without establishing byte equality,
deterministic Parquet output across applications, or full-data equivalence.

## Browser failures and persistent-state uncertainty

| Original baseline assertion | What is known |
|---|---|
| Only four evaluation charts instead of the required five | The runner reached the evaluation page and counted charts immediately after the heading. |
| Missing “Recorded Spark batch durations (milliseconds)” | The runner reached evidence-content assertions; no original timestamped UI/API snapshot or raw traceback survived. |

These were application E2E assertions, not browser-startup failures. API fixture
validation had passed, but the UI had no Compose healthcheck. A rerender timing
issue is plausible, not established. The later matched runs used the same
fixture, validator, pinned browser stack and loopback ports in separate projects
and generated databases. Both passed four pages, five charts, selectors,
fallback/recovery, refresh and JavaScript checks after API/Mongo readiness;
passing reruns do not explain the original failures. A separate follow-up
harness setup lacked Python Playwright despite having Chromium; correcting
that setup did not establish a cause for the original assertions.

During the historical investigation, Module 2 wrapper help started the default
Compose project before forwarding help. This predated the style edits. The
current shared [Compose wrapper](../scripts/compose_runner.py) handles help
before service startup; that later correction does not resolve earlier volume
uncertainty.

The recorded volumes were `instacart_namenode_data`, `instacart_datanode_data`
and `instacart_mongodb_data`. Later metadata showed Spark containers Created
with zero start times and HDFS/Mongo stopped, but no pre-incident data snapshot
existed. No volume was deliberately reset, removed or pruned; transient writes
cannot be ruled out. Later isolated cleanup does not prove the earlier
persistent data was unchanged.

## Performance and remaining verification

| Single historical comparison | Baseline | Refactored |
|---|---:|---:|
| Live-Mongo pytest elapsed | 192.92 s | 191.44 s |
| 500-event stream processed rate | about 663/s | about 570/s |
| ALS fixture evaluation | about 102.7 s | about 102.5 s |
| Graph fixture work | about 11.1 s | about 11.7 s |

These were short single-run sanity checks, not repeated performance experiments;
they establish neither speedup nor throughput regression. Full ingestion/HDFS
staging and rollback, full batch publication, the 5,000-event Module 3 workflow,
full-data ALS/graph/Hybrid acceptance and replay remained NOT RUN.

Remaining work is to run isolated full-data comparisons with matching source
and output evidence, preserve browser/service traces per attempt, and decide
whether deterministic Parquet bytes are actually required before considering a
separate writer change. Keep logical and byte comparisons distinct. Treat
persistent-volume state as historically indeterminate without independent
pre-incident evidence. The recorded assessment remains NOT FULLY VERIFIED.
