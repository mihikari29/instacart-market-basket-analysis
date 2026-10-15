# Module 4 package migration guide

This page preserves the established Module 4 migration links. Current package,
CLI, API, Docker, index, and validation instructions live in the canonical
guides below.

## Source paths and imports

See the [canonical Module 4 package paths and imports](repository-layout.md#module4-package-paths),
including the ASGI entry point and retired compatibility package names.

## Hybrid CLI

Use the [Hybrid CLI operating instructions](docker.md#hybrid-cli) and the
[Module 4C serving contract](module4-plan.md#module4c-spec) for required
arguments, publication behavior, and provenance limits.

## FastAPI startup

Follow the [host API runbook](docker.md#run-the-api-and-dashboard-against-existing-data)
for dependencies, explicit configuration, and local startup.

## Docker build and run

The [image build guide](docker.md#build-each-image) describes the targets; the
[standalone API container runbook](docker.md#standalone-api-container) covers
protected configuration and container networking.

## MongoDB indexes and read-only account

Use the [required dashboard index list](docker.md#required-dashboard-indexes)
and [Module 4D API contract](module4-plan.md#module4d-spec) for administrator
provisioning and read-only access requirements.

## Tests and validation

See the [validation command ownership and runbook](docker.md#validation-commands)
for repository, API, UI, browser, live MongoDB, and full-data boundaries.

## Delivery boundary

The [Module 4C progress record](progress.md#progress-module4c),
[Module 4D progress record](progress.md#progress-module4d), and
[Module 4D evidence archive](evidence/module4d/README.md) record delivered
scope and its validation evidence.
