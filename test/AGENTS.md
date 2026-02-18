# test/ — Test Suite

## Test Organization
```
test/
├── core/           — Integration tests (471 tests, fork-based execution)
│   └── run_tests.py — Entry point: python run_tests.py --debug
├── unit/           — Unit tests (55 tests)
│   ├── configs/    — Config parameter tests (8)
│   ├── inheritance/ — Flow inheritance tests (38)
│   └── spin/       — Spin (single-step) tests (12)
├── cmd/            — CLI command tests (37)
│   ├── develop/    — Stub generator tests (31)
│   └── diff/       — Code diff tests (6)
├── data/s3/        — S3 client tests (5 unit + integration)
└── structural/     — Architecture enforcement tests
```

## Running Tests
- **Never run all test dirs together** — `spin/`, `configs/`, and `inheritance/` have conftest conflicts with `--use-latest` option
- Core tests take ~5 minutes (fork-based, sequential)
- Unit tests run in < 1 second
- S3 integration tests need `METAFLOW_S3_TEST_ROOT` and MinIO

## Writing New Tests
1. Tests are the specification — write them BEFORE the implementation
2. Use `pytest.mark.parametrize` for data-driven tests
3. Use `unittest.mock.patch` for external dependencies
4. Test file naming: `test_<module_name>.py`
