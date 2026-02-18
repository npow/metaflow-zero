# Metaflow-Zero — Agent Guide

## What is this?
Clean-room reimplementation of [Metaflow](https://metaflow.org). The codebase is 100% agent-generated.

## Architecture (Layered)
```
Foundation  →  Core  →  Storage  →  FlowSpec  →  Client API  →  Runtime  →  CLI  →  Runner
```
Dependency direction is strictly **left-to-right**. Never import from a higher layer into a lower one.

## Where to look
- `metaflow/` — All source code. Each subdirectory has its own `AGENTS.md`.
- `test/` — All tests. See `test/AGENTS.md`.
- `tools/` — Linters, garbage collection, CI tooling.

## Running tests
```bash
# Core integration tests (471 tests)
cd test/core && PYTHONPATH=/root/code/metaflow_cc python run_tests.py --debug

# Unit tests (60 tests) — run separately due to conftest conflicts
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/unit/test_*.py test/data/s3/test_s3op.py -k "not test_long_filename_download" -v

# Spin/configs/inheritance tests (58 tests) — each suite separately
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/unit/spin/ -v
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/unit/configs/ -v
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/unit/inheritance/ -v

# Cmd tests (37 tests)
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/cmd/ -v

# Structural tests (architecture enforcement)
PYTHONPATH=/root/code/metaflow_cc python -m pytest test/structural/ -v

# Lint checks
python tools/lint_check.py
```

## Golden Principles
1. **Parse at boundaries, trust internally** — Validate data at system edges (user input, S3, REST APIs). Internal code uses typed interfaces.
2. **Shared utilities over hand-rolled helpers** — Use `metaflow/util.py`, never re-implement `to_bytes`, `to_unicode`, etc.
3. **Structured logging only** — No bare `print()` in library code. Use the logging patterns in `metaflow/util.py`.
4. **Enforce architecture with tests, not comments** — See `test/structural/`.
5. **One module, one responsibility** — Files should be under 500 lines. Split if larger.
6. **Errors carry remediation** — Exception messages must tell the agent/user what to do to fix the problem.
7. **No YOLO data probing** — Never guess data shapes. Use typed SDKs, parse at boundaries.
8. **Provider pattern for cross-cutting concerns** — Auth, connectors, telemetry, feature flags enter through `get_datastore()`, `get_metadata_provider()`, `get_secrets_backend_provider()`.
9. **Convention over configuration** — Environment variables follow `METAFLOW_*` prefix convention.
10. **Tests are the spec** — Every module's behavior is defined by its test file first.

## Known Technical Debt
- S3 integration tests require MinIO/DevStack (not available in CI yet)
- Kubernetes/Argo tests require cluster infrastructure
- No behavioral tests for deployer.py yet
