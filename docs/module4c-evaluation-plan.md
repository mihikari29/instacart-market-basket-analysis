# Module 4C evaluation plan

This plan separates responsiveness of the serving path from recommendation quality. The C8A experiment is Track A only. It uses a small synthetic fixture and makes no claim about Instacart recommendation accuracy or production streaming performance.

## Track A — responsiveness

### Controlled before and after windows

Use one warm user with exactly 20 valid ALS rows, one accepted active A/B readiness pointer, and graph/catalog rows for the bounded 20 product fixture. Keep the source run, model, graph, ALS scores, user, trend namespace, ranking settings, and container resources fixed within each lambda arm. Publish two immutable finalized C4A bundles with the real static ranking and bundle writer:

| Window | Event-time interval | Controlled counts | Independently specified trend scores |
|---|---|---|---|
| Baseline | 2024-01-01 00:00–02:00 UTC | P1: 99; P2–P20: 0 | P1: 1; P2/P3: 0 |
| Changed trend | 2024-01-01 01:00–03:00 UTC | P1: 10; P2: 99; P3–P20: 0 | P1: 10/99; P2: 1; P3: 0 |

For both count columns the same counts are used, so Module 3's configured 0.7/0.3 blend yields the shown normalized scores. Window ends are strictly increasing, UTC, BSON-millisecond aligned, and used as explicit serving `as_of_event_time` values. The accepted CLI selects the latest verified bundle at or before that event time. No event-time freshness threshold is supplied implicitly.

Evaluate lambda values 0.0, 0.5, 0.7, 0.9, and 1.0. Use a separate disposable database per lambda because C6 correctly rejects conflicting logical snapshots at the same event time. Lambda 0 uses the trend-only candidate endpoint and lambda 1 uses the ALS-only endpoint. Use an independent three-product score oracle with full-precision comparisons and an absolute tolerance, never rounded score equality.

For each fixed Top-K comparison, report baseline and changed Top-1, whether Top-1 changed, overlap count, Jaccard similarity, changed product-rank count, mean absolute rank displacement, and the P1/P2/P3 score deltas. The candidate universe is the union of the two returned Top-K lists. A product absent from one list receives sentinel rank `K+1`; rank-change count and mean displacement are calculated over that union. This is a ranking-responsiveness description, not a relevance or accuracy metric.

Check that C6 advances the per-user pointer to the later event time and changed serving version, that baseline rows remain immutable, and that the accepted A/B readiness document remains unchanged. The experiment measures the actual serving CLI subprocess wall time for lambda 0.5: one warm-up in a separate database, then three independent first publications for each selected window. Fixture creation and trend-bundle publication are outside the timed interval. Report median, minimum, maximum, and all observations separately for baseline and changed windows. Do not mix idempotent retries with first-publication observations.

The fixture's synthetic trend contents do not represent observed user activity or real-world trend shifts. CLI wall time includes process startup and MongoDB serving work, but does not measure Kafka event-to-serving latency, Spark streaming latency, or recommendation accuracy.

### Future Kafka-to-serving demonstration prerequisites

A future demonstration should use a disposable, isolated Kafka/Spark/MongoDB stack and an explicitly identified event source. It should record event IDs, source provenance, event-time range, watermark and finalized-window status, processing delay, bundle identity, serving `as_of_event_time`, serving-version identity, and pointer-promotion result. It must keep source-generation identity fixed or explicitly test the generation change. The current race-coverage dependency on I2 C6 should be satisfied before claiming concurrent A/B generation-race coverage. Measure event-to-serving latency with synchronized, documented clocks and repeated runs; keep it distinct from CLI process timing. Do not replay the full dataset as a substitute for a leakage-safe quality evaluation.

## Track B — leakage-safe recommendation quality feasibility

This is a methodology plan only. No official-train evaluation, retraining, reprocessing, or quality claim is part of C8A.

### Provenance and historical cutoff

The existing Module 1 synthetic event feed includes both `prior` and `train` interactions. The ordinary feed does not retain `eval_set` provenance on final event records, so an ordinary Kafka replay cannot directly supply trend input for official-train recommendation evaluation. Before such an evaluation, inspect the source order sequence and the actual synthetic timestamp reconstruction. Do not assume that a calendar timestamp or order number establishes a valid historical cutoff.

Define and justify an explicit cutoff per user or cohort. Restrict the trend windows and their watermarks to events known to be available before that cutoff. The cutoff must be earlier than every evaluation truth interaction, and finalized-window availability must be based on the event-time watermark and processing history that would have been available then. Reprocessing will be needed if current event artifacts cannot prove prior-only provenance. Record excluded/late events and prove that no official-train truth event contributes to trend counts.

### Sources and cohorts

Train or reconstruct ALS using only interactions allowable before the cutoff. Build the graph and catalog snapshot from information available at the same cutoff; record source run, model, graph, catalog, trend namespace, and window identities. A graph or popularity source built using post-cutoff information is leakage even if train labels are not passed directly to the ranker.

Freeze one evaluation cohort and use it for every method in a comparison. Define a warm cohort using source availability and item-factor coverage without filtering based on the evaluation truth itself. Also report all nonempty official-train truth users, including cold-start users, plus explicit cold-user and cold-item cohorts. Document any user/product exclusions and coverage denominators. Do not silently change cohorts between ALS-only, Trend-only, Hybrid, PersonalFrequency, and GlobalPopularity.

### Methods and metrics

Compare ALS-only, Trend-only, Hybrid at a preregistered set of lambda values, PersonalFrequency, and GlobalPopularity. Keep the eligible catalog and cutoff-specific history identical where method semantics allow; define deterministic tie breaking and candidate eligibility. For each K in `{5, 10, 20}`, report Precision@K, Recall@K, MAP@K, NDCG@K, and coverage using the definitions and aggregation conventions in `src/module4/evaluation.py`. Report warm and all-nonempty-truth cohorts separately and stratify cold-start coverage. Record lambda, source composition, trend-window availability, and the count of evaluation users/products whose candidates are missing factors or catalog entries.

The official train split may be used as ground truth only after a defensible chronology and cutoff have been established from the actual order sequence. It must not influence model fitting, tuning, threshold selection, trend construction, graph construction, candidate filtering, or baseline computation. If no leakage-safe temporal partition can be demonstrated, report the evaluation as infeasible rather than presenting a potentially contaminated score.

## Current C8A boundary

C8A writes evidence under `results/module4c-responsiveness/<run-id>/`, uses disposable MongoDB databases only, and stops after the controlled synthetic responsiveness experiment and this plan. It does not run official-train evaluation, a full Kafka replay, dashboard work, or a production-data experiment. Concurrent A/B source-generation race coverage remains dependent on I2 C6; C8A holds one stable A/B generation fixed.
