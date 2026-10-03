# Current delivery gates

Full runtime evidence: [successful hosted run](full/README.md).
Historical fixture evidence remains in [README.md](README.md).

| Gate | Result |
|---|---|
| Module boundaries and source/history review | PASS; streaming M3 and ML M4 remain separate |
| M1 overwrite reproduction / lossless writer | PASS; deterministic regression and 33,819,106-row full staging |
| Replacement, stale partitions and upload failure behavior | PASS; fixture regressions |
| Full source/local/HDFS counts and exact fact equality | PASS; 33,819,106 each, 456 files / daily partitions |
| Docker build / standalone Spark / HDFS / canonical runner | PASS on hosted Linux |
| Analytics, SQL, pivot, rolling trends and HDFS user features | PASS on full data |
| MongoDB serving | PASS; 49,677 products / 21 departments; fixture also checks rerun cleanup |
| SMJ/BHJ operators, AQE, repeated output equality | PASS; one warm-up, three trials, plans and task metrics retained |
| Equivalent partition filters | PASS; 177,912 events; 97.73% lower task input bytes |
| Caching | PASS; materialization reported separately; InMemoryTableScan verified |
| Automated suite | PASS; 15 tests in 16.94 seconds in Docker (Module 3 streaming tests gated by `integration` marker; Kafka/Mongo live integration requires services) |
| Module 3 streaming — validation/dedup/window/watermark/late/trending | PASS; PR #2 branch `fix/module3-hardening`, live CI run 37094705601 on `ff06239` |
| Module 3 streaming — finalized Top-K snapshot / TTL / recovery | PASS; tests, local 50k smoke and full CI run 37103621207 on `61ae930` |
| Module 3 streaming — throughput + Kafka partition benchmark | PASS as bounded development evidence; producer/consumer pair measured via `streaming_progress.jsonl` |
| Compile / Ruff / Compose / whitespace | PASS; 43-test local gate after HDFS rollback regression |
| Credentials and datasets excluded from Git | PASS; compact measured evidence only |
| Commits and remote push | PASS; `fix/module3-hardening` published; hardening `ff06239`, rollback fix `61ae930` |
| Merge | PR #2 is open against `main`; not merged as of 2026-10-03 |

This records executed acceptance gates, not a subjective 10/10 grade. Timings are
specific to the recorded single-host resources and warm-system methodology.
