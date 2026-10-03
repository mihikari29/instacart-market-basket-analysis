# Technical Acceptance Checklist

| Requirement | Result |
|---|---|
| Source, local and HDFS event count equality | PASS; 33,819,106 events in each representation |
| Deterministic generation | PASS; entity-keyed output is invariant to `--batch-users`; zero monotonicity violations |
| HDFS staging safety | PASS; stale partitions are replaced and upload/promotion failures restore the prior target |
| Kafka delivery accounting | PASS; attempted, acknowledged and failed outcomes are explicit; failures return non-zero |
| Fault injection identity | PASS; duplicates retain `event_id`; late events receive distinct deterministic IDs |
| Prior-only historical analytics | PASS; 32,434,489 prior facts used for Module 2 analytics |
| Train-set preservation | PASS; 1,384,617 train facts retained as Module 4 ground truth |
| Spark SQL and user features | PASS; analytics complete and user features written to HDFS |
| Pivot / unpivot | PASS; both full-data totals equal 32,434,489 |
| Join strategies | PASS; Broadcast Hash Join and Sort-Merge Join operators asserted with equal outputs |
| Partition pruning | PASS; equivalent filters return 177,739 events; partition predicate reduces task input |
| Cache behavior | PASS; materialization measured separately and `InMemoryTableScan` verified |
| MongoDB batch publication | PASS; 49,677 product and 21 department documents |
| Streaming schema/domain validation | PASS; invalid and poison rows are observable and excluded from aggregation |
| Kafka offset-loss policy | PASS; `failOnDataLoss=true` by default with explicit opt-out |
| `event_id` deduplication | PASS; duplicates are removed within watermark state |
| Event-time watermark and late data | PASS; within-watermark event accepted and beyond-watermark event dropped |
| Finalized-window aggregation | PASS; C30/C120 result verified with 30m/120m windows, 5m slide and 10m watermark |
| Ranking and Top-K | PASS; full population normalized and deterministically ranked before Top-K |
| MongoDB streaming idempotency | PASS; complete finalized snapshots replace stale rows without duplicate keys or ranks |
| TTL behavior | PASS; seven-day default, reconfiguration tested, explicit zero disables TTL |
| Checkpoint recovery | PASS; second process reads only new records with the same query identity |
| Automated test suite | PASS; 51 tests |
| Runtime sanity checks | PASS; Ruff, Docker Compose configuration and whitespace checks |

Performance values are bounded experimental measurements on the recorded
single-host configurations, not production-capacity guarantees.
