# Initial full-data attempt — 10/10/2026

Status: **failed** during full-catalog recommendation generation after model reload.

Input validation, prior-only tuning, final fitting and save/load passed. No new MongoDB snapshot was published. The original manifest and artifacts are retained unchanged; the worker exhausted its heap, then retries were deliberately stopped (wrapper exit 137).

See [the interruption receipt](../acceptance-20261010/initial-oom-interruption.json) and [actual command log](pipeline-command.txt). The corrected attempt uses separate `*-fixed-*` run IDs and versions.
