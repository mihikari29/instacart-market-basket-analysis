# Delivery gate evaluation

| Gate | Result / evidence |
|---|---|
| Repository, source, proposal, history, branch and status inspected | PASS; started from clean main at bb6cbdd; separate feature branch |
| Module boundaries confirmed | PASS; Proposal assigns analytics/features/MongoDB and three batch experiments to M2 |
| M1 overwrite hypothesis | CONFIRMED; old writer retains 2/6 events; corrected writer retains 6/6 |
| Lossless bounded staging and idempotence | PASS on deterministic overlapping dates and replacement-feed tests |
| Source/local/Spark counts and date integrity | PASS locally: 36/36/36; exact fact multiset equality |
| Full source/local/HDFS equality | NOT RUN; full source and Docker/HDFS unavailable |
| Runtime and dependencies | Implemented; Spark image digest exists; Compose configuration valid; container build/run unverified |
| Analytics, SQL, pivot, rolling window and features | PASS in local Spark; 24 prior facts, reorder rate 0.5, basket mean 3 |
| MongoDB outputs and rerun cleanup | PASS on MongoDB 7.0.16; 6 products, 2 departments; stale key removed, unrelated collection retained |
| SMJ/BHJ operators, AQE and output equivalence | PASS; two asserted operators per arm; AQE off; equal SHA-256s |
| Warm-ups, repetitions, medians and trial retention | PASS; one warm-up and three measured repetitions per arm |
| Input/shuffle/task/stage evidence | PASS; completed event logs parsed; skipped cached dependencies distinguished |
| Pruning plans and logical equivalence | PASS; partition filters asserted; equivalent windows both return 24 events |
| Scan reduction | Observed fixture input 23,071 vs 14,214 bytes for equivalent window predicates |
| Third experiment: aggregate caching | PASS; build cost separate, InMemoryTableScan verified, results equal |
| Automated suite | PASS: python -m pytest -q, 14 tests in 104.25 s |
| Local canonical CLI | PASS: python -m src.module2.cli all with fixture URI and local MongoDB |
| Docker canonical runner | NOT RUN: docker executable/daemon and WSL absent |
| Compile/import | PASS: python -m compileall -q src scripts tests |
| Lint | PASS: python -m ruff check . |
| Compose schema | PASS: standalone docker-compose.exe config --quiet (v2.35.1) |
| Git whitespace check | PASS: git diff --check |
| Documentation and truthful status | PASS: module2.md, README, progress correction and fixture evidence |
| Source diff review / no dataset or credentials staged | PASS; compact evidence only; local paths normalized in committed evidence |
| Full-data benchmark | NOT RUN: no available full data or Kaggle authentication |
| Commits / push | Local commits prepared; GitHub authentication unavailable; final SHA and actual push error in delivery report |
| PR | Not created; no successful push/authenticated GitHub CLI |

This evaluates the requested checklist without equating fixture success with full
HDFS delivery. No grade is assigned. Source SHA-256 in fixture-summary.json matches
the final production Python source at verification time. Published timings are
real observed measurements; no full-data values are supplied.
