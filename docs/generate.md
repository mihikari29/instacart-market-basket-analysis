# Synthetic event generation

Module 1 assigns deterministic synthetic event times so purchase events can be
replayed through Kafka and aggregated by event time. These generated times are
for simulation; the source dataset does not contain absolute purchase
timestamps.

## `SyntheticConfig` and scenarios

[`SyntheticConfig`](../src/module1/config.py) describes the generation seed,
time mode, and scenario parameters. The [project status page](progress.md#23-synthetic-feed-configurations)
records the named historical feed measurements and their scope. Current
commands for generation, staging, and replay are in the
[Docker guide](docker.md#module-1-ingestion-generation-staging-and-kafka-replay).
