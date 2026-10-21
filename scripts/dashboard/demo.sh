#!/usr/bin/env bash
# Start or stop one task-owned Module 4D synthetic demonstration.
#
# The steps live in `scripts/dashboard/demo.py` so that the Linux procedure,
# the Windows procedure and the continuous integration job all run exactly the
# same code. This wrapper only exists so the documented shell command keeps
# working.

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

exec python3 scripts/dashboard/demo.py "$@"
