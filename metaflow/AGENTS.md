# metaflow/ — Source Code

## Layer Architecture (enforced by `test/structural/test_architecture.py`)
```
Layer 0: Foundation  — exception.py, util.py, metaflow_config.py, _extension_loader.py
Layer 1: Core        — graph.py, parameters.py, includefile.py, user_configs/, namespace.py
Layer 2: Storage     — datastore/
Layer 3: FlowSpec    — flowspec.py, decorators.py, metaflow_current.py
Layer 4: Client API  — client/
Layer 5: Runtime     — runtime.py
Layer 6: CLI         — cli.py, cli_components/, cmd/
Layer 7: Runner      — runner/
Cross-cutting        — plugins/ (enters via Provider factories only)
```

## Dependency Rules
- Lower layers MUST NOT import from higher layers
- `plugins/` modules access core through `metaflow.metaflow_config`, `metaflow.exception`, `metaflow.util`
- Cross-cutting concerns (datastores, metadata, secrets) enter through factory functions in their `__init__.py`

## Naming Conventions
- Module files: snake_case (e.g., `my_module.py`)
- Classes: `PascalCase`
- Exceptions: end with `Exception` or `Error` (e.g., `MetaflowS3NotFound`)
- Config variables: `UPPER_SNAKE_CASE`, prefixed with `METAFLOW_` in env vars
- Factory functions: `get_*()` pattern

## File Size Limit
Target: **< 500 lines per file**. Split into submodules if larger.
