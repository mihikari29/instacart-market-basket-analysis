# Module 4 validation checkpoint — 08/10/2026

Work stopped at the user's request. Status: **fixture/integration validated;
full-data acceptance pending**. The plan records the remaining gates and how to
resume: [module4-plan.md](../../../module4-plan.md).

- [Regression suite](regression-tests.txt): 78 passed, no skips.
- [Later Module 4 suite](module4-tests.txt): 33 passed, no skips; includes real ALS,
  GraphFrames, HDFS receipt checks and live Mongo fault/replay tests.
- [Runtime smoke](runtime-smoke.txt): Spark 3.5.5 / Scala 2.12.18, GraphFrames
  Python 0.6 and checksum-verified 0.8.3 JAR; standalone PageRank, isolated vertex,
  HDFS round-trip and Mongo ping passed.
- [Cross-module flow](cross-module-smoke.txt): Module 1 partition receipts,
  Module 2 validation/4 user features, Module 4 A/B publication passed. The run
  published and checked 24 recommendation and 6 graph documents, with pair ready.
- [First staged flow](../flow-923e255d911244e2b368be3809cc2bd7-first/README.md): passed,
  9 recommendation documents and 5 graph products, including an official-test user.
- [Model replay](../flow-923e255d911244e2b368be3809cc2bd7-replay/README.md): failed
  because source/config metadata changed while the first attempt was running.
  [Actual failure log](replay-failure.txt) is retained; stable-source model replay
  remains pending. Independent live Mongo same-version snapshot replay passed.
- [Source receipt](download-source-receipt.json), [download](source-download.txt),
  [cleaning](source-clean.txt) and [staging](source-staging.txt) confirm preparation
  of all six full source tables, including 32,434,489 prior and 1,384,617 train rows.
- [Interrupted full-input job](full-input-interruption.json): validate-only,
  waiting for worker resources when stopped. No full-data ALS/graph fit or serving
  publication was started. The original manifest is embedded without claiming pass.

Compact run artifacts retain their original run IDs and source digests. Tests
were executed separately; 78 and 33 are overlapping suites, not 111 unique tests.
Later CLI/runner edits are covered by static checks and recorded as requiring
runtime follow-up where appropriate. GitHub Actions for this snapshot has not
been observed. Large event logs and source data remain local; temporary fixture
HDFS roots/databases were cleaned after completed smoke runs.
