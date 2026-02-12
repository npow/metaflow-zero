# metaflow-zero

A clean-room reimplementation of [Metaflow](https://metaflow.org) — built from scratch, zero lines copied.

**37 files. ~7,500 lines. 471 tests passing.**

## Highlights

- **Full Metaflow API** — `FlowSpec`, `@step`, `self.next()`, branching, foreach, switches, joins
- **All the decorators** — `@retry`, `@catch`, `@timeout`, `@card`, `@resources`, `@environment`, `@secrets`, `@parallel`
- **Parameters & Config** — `Parameter`, `Config`, `ConfigValue`, `IncludeFile`, `JSONType`
- **Client API** — `Flow`, `Run`, `Step`, `Task`, `DataArtifact` for inspecting past runs
- **Resume** — re-execute from a specific step, reusing completed work
- **Cards** — pluggable card system with rendering, runtime updates, and CLI access
- **Runner** — programmatic execution via `Runner.run()`, `Runner.resume()`, `Runner.spin()`
- **CLI** — `run`, `resume`, `dump`, `logs`, `tag`, `card`, `show`
- **Fork-based isolation** — each step runs in a forked child process; SIGKILL/SIGSEGV can't kill the orchestrator

## Getting started

Create a flow:

```python
# hello.py
from metaflow import FlowSpec, step

class HelloFlow(FlowSpec):
    @step
    def start(self):
        self.message = "hello world"
        self.next(self.end)

    @step
    def end(self):
        print(self.message)

if __name__ == "__main__":
    HelloFlow()
```

Run it:

```console
$ python hello.py run
```

Inspect the results:

```python
from metaflow import Flow

run = list(Flow("HelloFlow"))[0]
print(run["end"].task["message"].data)  # "hello world"
```

## Features

### Branching and joins

```python
@step
def start(self):
    self.next(self.train, self.validate)

@step
def train(self):
    self.model = fit()
    self.next(self.join)

@step
def validate(self):
    self.score = evaluate()
    self.next(self.join)

@step
def join(self, inputs):
    self.merge_artifacts(inputs, include=["model", "score"])
    self.next(self.end)
```

### Foreach

```python
@step
def start(self):
    self.datasets = ["a.csv", "b.csv", "c.csv"]
    self.next(self.process, foreach="datasets")

@step
def process(self):
    print("Processing", self.input)
    self.next(self.join)
```

### Decorators

```python
@retry(times=3)
@catch(var="error")
@timeout(minutes=10)
@card(type="default")
@step
def train(self):
    ...
```

### Parameters and config

```python
from metaflow import FlowSpec, step, Parameter, Config

class TrainFlow(FlowSpec):
    lr = Parameter("lr", default=0.01)
    settings = Config("settings", default="config.json")

    @step
    def start(self):
        print(self.lr, self.settings)
        self.next(self.end)
```

```console
$ python train.py run --lr 0.001
```

### Runner API

```python
from metaflow import Runner

with Runner("hello.py").run() as result:
    assert result.status == "successful"
    print(result.run["end"].task["message"].data)
```

### Resume

```console
$ python hello.py resume --origin-run-id 12345 start
```

Re-executes from `start`, reusing all steps before it from the original run.

## Architecture

```
Layer 5  CLI + Runner          User-facing entry points
Layer 4  Runtime Engine        Fork-based DAG execution with retry/catch/timeout
Layer 3  Client API            Read-only access to completed runs
Layer 2  Storage               Local datastore + metadata provider
Layer 1  Core                  FlowSpec, Graph, Parameters, Decorators, Current
Layer 0  Foundation            Exceptions, utilities, config, namespace
```

Each layer depends only on layers below it. No circular imports.

### Interface contracts

Components communicate through explicit public interfaces and frozen dataclasses:

| Interface | Location | Purpose |
|---|---|---|
| `Transition` | flowspec.py | Immutable output of `self.next()` — targets, foreach/condition vars, switch resolution |
| `TaskResult` | runtime.py | Child-to-parent IPC — success/failure, taken branch, exception |
| `TaskContext` | metaflow_current.py | All task-level context, passed to `Current.bind()` in one call |
| `FlowSpec` methods | flowspec.py | `load_parent_state()`, `get_artifacts()`, `set_artifact()`, `get_transition()`, etc. |

### Fork-based execution

Every step attempt runs in a **forked child process**:

- SIGKILL/SIGSEGV in step code doesn't crash the orchestrator
- Stdout/stderr captured per-attempt via temp files
- Artifacts saved by child, loaded back by parent
- `TaskResult` dataclass pickled to disk for IPC

## Project structure

```
metaflow/
  flowspec.py               FlowSpec, Transition, self.next(), merge_artifacts
  graph.py                  DAG construction from @step methods
  runtime.py                Fork-based execution engine, TaskResult
  decorators.py             Step/flow decorators (@retry, @catch, @card, ...)
  parameters.py             Parameter descriptor
  includefile.py            IncludeFile descriptor
  metaflow_current.py       current singleton, TaskContext
  exception.py              Exception hierarchy
  namespace.py              Namespace management
  cli.py                    CLI entry point
  cli_components/
    run_cmds.py             run, resume commands
  client/
    __init__.py             Flow, Run, Step, Task, DataArtifact
  datastore/
    local.py                Pickle-based local artifact storage
  plugins/
    metadata_providers/
      local.py              JSON-based local metadata
    cards/                  Card system
    catch_decorator.py      FailureHandledByCatch
    timeout_decorator/      TimeoutException
  runner/
    __init__.py             Runner, spin
    click_api.py            CLI introspection utilities
  user_configs/
    config_parameters.py    Config, ConfigValue
  util.py                   Shared utilities
```

## Testing

```console
$ # Core integration tests — 471 cases across 20+ graph topologies
$ cd test/core && PYTHONPATH=/path/to/metaflow-zero python run_tests.py --debug

$ # Unit tests
$ PYTHONPATH=/path/to/metaflow-zero python -m pytest test/unit/test_config_value.py test/unit/test_local_metadata_provider.py

$ # Run a specific test
$ cd test/core && PYTHONPATH=/path/to/metaflow-zero python run_tests.py --tests=BasicArtifactTest --debug
```

## Design decisions

| Decision | Rationale |
|---|---|
| **Clean-room** | Zero lines copied from original Metaflow. Derived entirely from public docs and test suite. |
| **Fork, don't thread** | `os.fork()` per step attempt. IPC overhead is worth true crash isolation. |
| **Pickle for IPC** | `TaskResult` dataclasses pickled to disk. Exceptions survive the boundary via custom `__reduce__`. |
| **Explicit interfaces** | Frozen dataclasses + public methods replace ~80 private attribute accesses across components. |
| **Local only** | No cloud backends = simpler architecture without plugin/provider abstractions. |

## Limitations

- Local execution only (no AWS/Azure/GCP backends)
- No conda/pypi environment isolation
- No Argo/Step Functions/Kubernetes schedulers
- Cards render simple HTML (no rich visualizations)

## Acknowledgments

This is an independent reimplementation. It is not affiliated with or endorsed by [Outerbounds](https://outerbounds.com) or Netflix.

Built using only the public [Metaflow documentation](https://docs.metaflow.org) and test specifications as reference.
