# tools/ — Harness Engineering Tooling

## Purpose
Mechanical enforcement of golden principles and taste invariants.
These tools are the "harness" that enables agents to work reliably.

## Tools
- `lint_check.py` — Custom linter enforcing naming, file size, architecture, logging rules.
  Every error message includes a `FIX:` directive for agent remediation.
- `garbage_collect.py` — Entropy management. Scans for stale docs, tech debt markers,
  oversized files, orphaned tests. Run on a regular cadence.

## CI Integration
- `.pre-commit-config.yaml` at project root runs both tools on every commit
- `test/structural/test_architecture.py` runs the same checks as pytest tests

## Adding New Lint Rules
1. Add a `check_*` function in `lint_check.py`
2. Return `LintViolation` objects with actionable `remediation` messages
3. Add a corresponding test in `test/structural/test_architecture.py`
4. The error message IS the documentation — make it tell agents exactly what to do
