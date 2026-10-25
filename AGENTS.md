# Engineering & Architectural Guidelines

## 1. Zero Legacy & Backward Compatibility
- **Never maintain backward-compatibility shims**, deprecated aliases, dual-write sinks, or fallback paths.
- When an interface, schema, or configuration format changes, refactor all call sites, configs, and test fixtures directly.
- Fail immediately on invalid or outdated inputs rather than silently falling back.

## 2. No Phantom Flexibility or Dead Switches
- **No dead flags**: Do not introduce parameters, CLI flags, or boolean switches that are hardcoded or always evaluated to a single constant value (e.g. `require_backing_disk = True`).
- **No unused alternate branches**: If a feature or alternate branch has no realistic secondary use case (e.g., a dummy backend, unused feature modes, or dead execution toggles), eliminate the branch entirely and enforce the primary implementation.
- Keep signatures lean; do not propagate unused or unvaried configuration knobs through call chains.

## 3. Clean Code & Control Flow
- **Avoid redundant defensive nesting**: Eliminate deep nested `try...except` blocks and repetitive `if-else` cascades that re-validate invariant assumptions.
- **Avoid introspection acrobatics**: Do not use `getattr()`, `hasattr()`, or `setattr()` to paper over weak typing or schema uncertainty. Use explicit dataclasses, Pydantic schemas, or well-typed objects.
- **Balanced function sizing**:
  - Avoid monster functions doing multiple unrelated steps.
  - Avoid "fractured" one-line micro-functions that are only called once and add cognitive overhead without real reuse.
  - Promote heavily reused inline logic into well-named helper functions.
- **Consolidate fragmented parameters**: Group tightly coupled parameters into logical models or configs rather than passing 8+ individual primitive arguments.

## 4. Clean Design & Optimal Project Structure
- **Cohesive module boundaries**: Keep data ingestion, processing, modeling, serving, and orchestration cleanly separated with well-defined contracts.
- **Keep documentation lean and accurate**: Avoid bloat in markdown files, notes, or outdated docs. Eliminate speculative or obsolete documentation.
- **Minimize test bloat**: Test real behaviors, interfaces, and boundary contracts. Prune redundant, trivial, or brittle tests that merely test tautologies or obsolete compatibility code.

## 5. Environment & Hardware Generalization (Anti-Overfitting)
- **Target standard Linux containers and Kubernetes**: Do not overfit code, scripts, or validation logic to a specific developer laptop setup (e.g., WSL2 vs. Windows host disk branching).
- Rely on standard OS/runtime telemetry (e.g., `shutil.disk_usage`) rather than platform-specific workarounds.
- Avoid hardcoded machine paths, local username assumptions, or host-specific process wrappers.

## 6. Single Source of Truth for Configuration
- All pipeline hyperparameters, stage limits, resource reservations, and model weights must reside in `config/pipeline.yaml`.
- Do not create secondary configuration files for settings that belong in the main pipeline configuration.
- Avoid adding CLI argument overrides for values that are already defined in `config/pipeline.yaml` unless dynamic runtime overriding is explicitly required.

## 7. Canonical Role and Stage Naming
- Maintain consistent, canonical naming across manifests, code, task wrappers, and logs.
- Disallow arbitrary synonyms (e.g., use `step_input`, `step_module1`, `step_module2`, `step_module3`, `step_module4`, `step_replay`, `step_hybrid` consistently; do not mix in `input_stage` or unprefixed variants).

## 8. Development Workflow: Batch Refactoring & Test Efficiency
- Implement refactoring, cleanups, and edits across all affected files in cohesive batches.
- **Do not spam the test suite**: Never run test runs after every minor file edit. Verify the full batched change set once editing across modules is complete.

## 9. Communication & Pre-Push Verification Invariants
- **Answer First on Diagnostic Inquiries**: When the user asks a question about errors, failures, or status (e.g., "what's the problem?"), immediately explain what is known. Do not silently chain background tasks, tool installations, or multi-step fixes across multiple turns without directly updating the user.
- **Mandatory Pre-Push Lint Verification**: Never push commits to remote branches without running both the linter (`ruff check .`) and the relevant test suite locally. Zero tolerance for unverified syntax, unused imports, or undefined variable names reaching remote CI.
