# Synthetic event generation

The Instacart source records the order sequence, order weekday and hour, and an inter-order interval. It does not include absolute purchase timestamps. Module 1 assigns deterministic synthetic times so product events can be replayed through Kafka and aggregated by event time. The generated calendar is a simulation input; it must not be described as the date on which a customer actually purchased an item.

## Inputs and outputs

The generator reads cleaned `orders`, prior and train interaction tables, and product dimension fields needed for each event. It excludes the official `test` order set. The default output is event Parquet; JSONL is also supported. Parquet includes `order_number` and `synthetic_date` for analysis and partitioning. The producer's Kafka record omits those fields and has its own 13-field contract, including ingestion time assigned during send. See the [Module 3 event provenance contract](module3-contracts.md#kafka-event-contract-and-provenance).

The `event_id` combines order ID and cart position. One row represents one product in one basket, not one whole order. The generated feed normally includes both prior and train purchases, and the Kafka payload does not retain `eval_set`. Therefore a cutoff on generated event time is not a valid way to isolate official-train evaluation data.

## Deterministic time construction

The implementation uses 2024-01-01, a Monday, as the synthetic calendar base. For each user it derives a stable anchor week from the configured seed and user ID. The recorded first order weekday offsets that anchor; subsequent orders accumulate cleaned `days_since_prior_order` values. Recorded order hour is preserved, while minute and second are derived by stable order-keyed values in the default uniform mode. The deterministic mode derives its order minute from order identity and uses zero seconds.

Within each order, events are sorted by `add_to_cart_order`. The first item's gap is zero, and each later item's event time advances by the cumulative configured interval. The default interval sampler is exponential with a 30-second mean clipped to 5–120 seconds. A stable hash namespace and SplitMix64 transform derive each value from the seed, order ID and cart position; generation does not depend on Python's process-randomized hash or row iteration order.

A final per-user pass checks events in `order_number` and cart order. If the next order would not start at least one second after the prior order's final event, it shifts that order forward. This guarantees strictly increasing event time along the user's business sequence. The generator records the resulting span and monotonicity count in its manifest.

Because source intervals at 30 days are capped, Module 1 first reconstructs a minimum weekday-consistent gap in the range 30–36 days. The generator uses that cleaned synthetic gap; it does not recover the original hidden interval.

## `SyntheticConfig` and scenarios

[`SyntheticConfig`](../src/module1/config.py) records the seed, scatter window, time mode, item-gap sampler and optional user limit. Supported scatter windows are 1, 4, 13, 26 and 52 weeks. The default seed is 42 and default scatter is 13 weeks.

| Scenario | Item-gap distribution | Order-time mode | Purpose |
|---|---|---|---|
| `default` | Exponential mean 30 s, clipped to [5, 120] s | `uniform` | General replay feed. |
| `mobile-fast` | Exponential mean 20 s, clipped to [3, 90] s | `uniform` | Faster within-cart activity. |
| `desktop-browse` | Exponential mean 40 s, clipped to [10, 240] s | `uniform` | Slower within-cart activity. |
| `uniform` | Uniform [15, 50] s | `uniform` | Fixed bounded interval family. |
| `deterministic` | Exponential mean 30 s, clipped to [5, 120] s | `hash` | Hash-derived order time for repeatable runs. |

`--limit-users` selects the lowest sorted user IDs for a bounded fixture. `--batch-users` is a processing-memory control (default 20,000), not an event-scope control. For the same source and seed, changing this batch size preserves generated values. These are different controls: a user limit changes the generated population, while batch size changes how that population is processed.

## Feed manifest and reproducibility

The feed manifest records the complete config, batch-size setting, deterministic namespaces/keys, event/user/order counts, event-time span, daily event counts, peak/mean daily rates, and monotonicity violations. Retain the manifest and source receipt with any measurement. Two runs are comparable only when source identity, seed, scenario, scatter window, time mode, user scope, and output format are understood.

The named `scatter_1w`, `scatter_1m` and `scatter_3m` measurements in [project progress](progress.md#23-synthetic-feed-configurations) are historical observations. The separate 3 October 2026 full-data manifest used seed 42, 13-week scatter and 2,000 users per processing batch. Those measurements must not be conflated with the generator's current default batch size.

## HDFS staging and Kafka publication

Generation writes a local event artifact and manifest. HDFS staging uploads the raw CSVs and curated tables, writes `_sources.json` for curated Parquet, and promotes partitioned interactions separately with `_handoff.json`. The six-file input receipt remains separate run evidence. Kafka publication is another step that sends event rows keyed by `user_id`; producer receipt counts and consumed terminal offsets are needed to establish bounded replay completion. Current commands, paths and safety constraints are maintained in the [Docker Module 1 runbook](docker.md#module-1-ingestion-generation-staging-and-kafka-replay). The generation algorithm and scenario parameters are source-owned in [`src/module1/generate.py`](../src/module1/generate.py) and [`src/module1/config.py`](../src/module1/config.py).
