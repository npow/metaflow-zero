# plugins/ — Extension Modules

## Structure
Each plugin lives in its own subdirectory with an `__init__.py` that exports the public API.

```
plugins/
├── argo/           — Argo Workflows orchestration
├── aws/            — AWS utilities (tag validation, resource computation)
├── cards/          — Card rendering and components
├── datatools/s3/   — S3 client (get/put/list/info with ranges, retries)
├── kubernetes/     — K8s execution (decorator, job CRUD, executor)
├── metadata_providers/ — Local filesystem + REST service metadata
├── pypi/           — Conda/PyPI environment management + parsers
├── secrets/        — Secret specification and validation
└── timeout_decorator/ — Step timeout handling
```

## Provider Pattern
Cross-cutting concerns are accessed through factory functions, never by direct import of implementation:
- `metaflow.datastore.get_datastore(type)` → returns `LocalDatastore` or `S3Datastore`
- `metaflow.plugins.metadata_providers.get_metadata_provider(type)` → returns `Local` or `Service`
- `metaflow.plugins.secrets.secrets_decorator.get_secrets_backend_provider(type)` → returns registered backend

## Adding a New Plugin
1. Create `metaflow/plugins/<name>/` with `__init__.py`
2. Add to the relevant factory if it's a provider
3. Add structural test in `test/structural/test_architecture.py`
4. Write tests first in `test/unit/test_<name>.py`
