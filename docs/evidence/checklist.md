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
| Module 3 streaming — Kafka connectivity + window/watermark/late/trending | Implementation complete; live-cluster CI on `module3-speed-layer` branch |
| Module 3 streaming — throughput + Kafka partition benchmark | Same as above; producer/consumer pair measured via `streaming_progress.jsonl` |
| Compile / Ruff / Compose / whitespace | PASS; final documentation update rechecked |
| Credentials and datasets excluded from Git | PASS; compact measured evidence only |
| Commits and remote push | PASS; feature branch published; full-run source bf8aeda |
| Merge | Not automatic; review feature branch through a pull request |

This records executed acceptance gates, not a subjective 10/10 grade. Timings are
specific to the recorded single-host resources and warm-system methodology.
