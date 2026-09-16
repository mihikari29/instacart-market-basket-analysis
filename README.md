# IT4931 — Big Data Project · Lambda Architecture

Replay of the **Instacart Market Basket** dataset as a live real-time feed: cleaned
parquet facts + synthesized absolute timestamps → Kafka → Spark Streaming, with a
batch layer for ALS recommendation and trending. The spec is `project data require.md`.

## Repo layout

```
src/clean.py      raw CSVs      → data/clean/*.parquet   (cleaned facts + dims)
src/config.py     SyntheticConfig + scenario presets
src/generate.py   clean parquet → data/synthesized/<scenario>/events.parquet + manifest.json
data/*.csv                        original Kaggle files (archive — never read by modules)
data/clean/*.parquet              batch source of truth (facts + dims)
data/synthesized/scatter_{1w,1m,3m}   three demo feeds, pick ONE
```

| file | used by |
|---|---|
| `data/clean/orders.parquet`, `order_products__*.parquet`, `products/aisles/departments.parquet` | Module 2 (batch/ML) |
| `data/synthesized/scatter_*/events.parquet` | Module 1 producer → Kafka (Module 3) |

## Run

```bash
python src/clean.py --validate
python src/generate.py --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
python src/generate.py --scenario default --scatter-weeks 1  --out-dir data/synthesized/scatter_1w
python src/generate.py --scenario default --scatter-weeks 4  --out-dir data/synthesized/scatter_1m
python src/generate.py --list-scenarios
```

## SyntheticConfig

- **Absolute-time axis**: `scatter_window_weeks` (1/4/13/26/52) spreads users' first
  orders; span and crowding: 1w→372 d @ 4.72× · 1m→393 d @ 2.80× · **3m (default)→456 d @ 2.81×**.
- **Session axis**: `delta` seconds between item-adds — `exponential` (Poisson arrivals,
  mean 30 s, clip [5,120]) or `uniform [15,50]`; `minute_mode` `uniform` (U(0,59)) or `hash` (RNG-free).
- **Presets**: `default`, `mobile-fast` (20 s), `desktop-browse` (40 s), `uniform`, `deterministic`.
- **Invariant**: per-user monotonicity always enforced → **0 violations** across all 33.8 M events.

## Event schema (one row = one product event)

`event_id` `<order_id>_<add_to_cart_order>`, `order_id`, `user_id`, `product_id`,
`add_to_cart_order`, `reordered`, `aisle_id`, `department_id`, `order_hour_of_day`,
`order_dow`, `event_time_epoch_ms`, `event_time_iso`, `synthetic_date`.
The per-item gaps (exp/uniform deltas) are folded into `event_time_epoch_ms` —
recover by `diff()` within `order_id`.

## Cleaning / notes

- Cleaning: 206,209 first-order NaN gaps → 0 · 369,323 cap-30 gaps recovered to
  [30,36] (Strategy B) → 0 day-of-week mismatches · 16 names `\xa0` normalized ·
  75,000 `test` orders excluded from the stream.
- Dataset: 206,209 users · 3,346,083 stream orders · 33,819,106 events
  (prior 32,434,489 + train 1,384,617) · 49,688 products · 134 aisles · 21 departments.
- Next: Kafka producer (replay pacing + `--inject late|burst|dup|skew|poison`), HDFS layout.