# Module 4 package migration guide

Module 4A, 4B, 4C, and 4D keep distinct responsibilities in one Python source
package. The root retains the accepted Module 4A/4B implementation; Hybrid
serving and the read-only dashboard API live in dedicated subpackages.

## Source paths and imports

| Former path | Canonical path |
|---|---|
| `src/module4/` | `src/module4/` (Module 4A/4B; unchanged) |
| `src/module4c/` | `src/module4/hybrid/` (Module 4C) |
| `src/module4d/` | `src/module4/dashboard/` (Module 4D D1A/D1B API) |

Use the consolidated imports:

```python
from src.module4.hybrid.service import serve_hybrid_user
from src.module4.dashboard.api import create_app
```

The dashboard module exports a FastAPI `app` for ASGI servers. There are no
compatibility source trees at `src.module4c` or `src.module4d`.

## Hybrid CLI

The canonical command is:

```bash
python3 -m src.module4.hybrid.cli serve-user \
  --user-id USER_ID \
  --as-of-event-time 2024-01-01T00:00:00Z \
  --source-namespace NAMESPACE \
  --mongodb-uri "$MONGODB_URI" \
  --mongo-database "$MONGO_DATABASE"
```

Keep the existing explicit cutoff, namespace, readiness, and ranking arguments
required by the Hybrid CLI contract. Set the URI and database variables from
the deployment's protected environment.

## FastAPI startup

Install the API requirements in the active Python environment, configure the
backend settings below, and start Uvicorn from the repository root:

```bash
python3 -m pip install -r deploy/module4/requirements/api.txt
python3 -m uvicorn src.module4.dashboard.api:app --host 127.0.0.1 --port 8000
```

The module-level ASGI `app` is the inspected D1B entry point. Application
startup requires explicit configuration; `/health` reports process liveness
and does not query MongoDB.

## Docker build and run

Build the `api` target of the canonical root `Dockerfile`. It inherits the
internal pinned `spark` stage without requiring a separately tagged Spark
image. The shared `.dockerignore` excludes raw `results/`, `.git/`, datasets,
private environment/key files, and caches. Each stage copies its own explicit
resources; API packages the consolidated Python source tree, tests, selected
validation scripts, and only the five registered evidence files. See the
[Docker runbook](docker.md) for all four targets and the context safety audit.

```bash
docker build --pull=false -f Dockerfile --target api \
  -t instacart-module4d-api:local .
docker run --rm --env-file /secure/path/module4d.env \
  -p 127.0.0.1:8000:8000 instacart-module4d-api:local \
  python3 -m uvicorn src.module4.dashboard.api:app --host 0.0.0.0 --port 8000
```

Keep the environment file outside the repository with restrictive file
permissions. Set `MODULE4D_APP_ROOT=/workspace` in the container environment so
the fixed evidence allowlist resolves to the packaged artifacts.

## MongoDB indexes and read-only account

Provision these indexes administratively in the configured database before
serving requests:

- `final_recommendations`: the C6 `uq_serving_user_rank` index, with keys
  `{serving_version: 1, user_id: 1, rank: 1}`.
- `realtime_trending_windows`: `{source_namespace: 1, window_end: -1}` named
  `idx_source_namespace_window_end_desc`.
- `batch_product_metrics`: `{purchase_count: -1, product_id: 1}` named
  `idx_batch_product_purchase_count_desc_product_id_asc`.
- `batch_product_metrics`: `{reorder_rate: -1, product_id: 1}` named
  `idx_batch_product_reorder_rate_desc_product_id_asc`.

MongoDB's ordinary `_id_` indexes must also remain available. Module 2's
publication replaces its product collection and indexes, so administrators
must recreate both D1B product indexes after every publication. The API never
creates or repairs indexes. See [the full API contract](module4d-api.md) for
index compatibility and failure behavior.

Create a database-scoped MongoDB user with only the built-in `read` role on the
API database. Configure the server-side environment with:

```text
MODULE4D_MONGODB_URI=<URI for the database-scoped read-only user>
MODULE4D_MONGO_DATABASE=<explicit database name>
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated exact namespace allowlist>
MODULE4D_APP_ROOT=/workspace
```

The URI database, when present, must match `MODULE4D_MONGO_DATABASE`. Keep
credentials in the backend environment; never expose them to a browser or
commit them. The API requires read access to its five MongoDB collections and
permission to inspect index metadata. It must not have write, index-management,
rename, or drop privileges. D1B's product indexes are externally provisioned
administrative dependencies.

## Tests and validation

Run the complete applicable D1A/D1B and Hybrid regression suite in the pinned
API image, including isolated live MongoDB tests, with:

```bash
python3 scripts/module4/module4d_api_validation.py --mode all
```

The script builds a narrow temporary context, runs the complete API unit test
set and the MongoDB 7.0.16 live tests, verifies Uvicorn/OpenAPI startup, and
removes its temporary image and containers. For the repository-wide suite,
build the Spark test runtime (which includes the API test dependencies) and run:

```bash
docker compose build spark-master
docker run --rm -v "$PWD:/workspace" -w /workspace \
  instacart-spark:3.5.5 python3 -m pytest -q \
  --ignore=tests/test_module4d_api_mongo.py \
  --ignore=tests/test_module4d_batch_api_mongo.py \
  --ignore=tests/test_module4d_ui_app.py \
  --ignore=tests/test_module4d_ui_client.py \
  --ignore=tests/test_module4d_ui_evaluation.py
```

The two D1A/D1B live MongoDB test files are owned by
`scripts/module4/module4d_api_validation.py --mode all`; the Spark command excludes
those files because the API harness supplies their isolated MongoDB 7.0.16
service and read-only account. The three UI test files run with dependencies
from a development image built using:

```bash
docker build --build-arg MODULE4D_UI_DEV=1 -f Dockerfile --target ui \
  -t instacart-module4d-ui-dev:local .
```

The D3 browser and publisher checks use the pinned `deploy/module4/compose.demo.yaml`
fixture and Playwright image; see the [Streamlit runbook](module4d-streamlit.md).

Repository Ruff and Compose checks:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  instacart-spark:3.5.5 python3 -m ruff check .
docker compose config --quiet
python3 -m compileall -q src scripts
git diff --check
```

## Delivery boundary

Module 4D D0 architecture, D1A read-only Hybrid/trend API, the D1A-FIX C5
selection parity correction, D1B batch/evidence API, and D2 four-page Streamlit
dashboard are implemented. D3 packages and validates a synthetic isolated
MongoDB demonstration. The API and browser evidence are distinct from offline
ALS quality, unevaluated Hybrid quality, and production scalability; see the
[release review](module4d-release-review.md).
