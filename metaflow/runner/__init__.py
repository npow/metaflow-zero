"""Runner API for programmatic flow execution."""

import json
import os
import subprocess
import sys
import tempfile


class _CommandObj:
    """Stores command execution details."""
    def __init__(self, command, process, log_files):
        self.command = command
        self.process = process
        self.log_files = log_files


class _ExecutingRun:
    """Result of a Runner.run() or Runner.resume()."""
    def __init__(self, command_obj, flow_name, run_id):
        self.command_obj = command_obj
        self._flow_name = flow_name
        self._run_id = run_id
        self._run = None

    @property
    def run(self):
        if self._run is None and self._run_id:
            from ..client import Flow
            from ..namespace import namespace, get_namespace
            old_ns = get_namespace()
            namespace(None)
            try:
                self._run = Flow(self._flow_name)[self._run_id]
            finally:
                namespace(old_ns)
        return self._run

    @property
    def status(self):
        if self.command_obj.process.returncode is None:
            return "running"
        elif self.command_obj.process.returncode == 0:
            return "successful"
        else:
            return "failed"


class Runner:
    """Run a flow from Python."""

    def __init__(self, flow_path, *, show_output=True, env=None, cwd=None, **top_level_kwargs):
        self.flow_path = flow_path
        self.show_output = show_output
        self.env = env
        self.cwd = cwd
        self.top_level_kwargs = top_level_kwargs

    def _build_top_opts(self):
        """Build top-level CLI options from kwargs."""
        opts = []
        for key, val in self.top_level_kwargs.items():
            cli_key = "--%s" % key.replace("_", "-")
            if isinstance(val, bool):
                if val:
                    opts.append(cli_key)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        opts.extend([cli_key, str(item[0]), str(item[1])])
                    else:
                        opts.extend([cli_key, str(item)])
            else:
                opts.extend([cli_key, str(val)])
        return opts

    def _extract_flow_name(self):
        """Extract flow class name from the flow file.

        Strategy:
        1. Look for a class instantiated in __main__ block (e.g., MyFlow())
        2. Fall back to the last class definition in the file
        3. Fall back to any class inheriting from FlowSpec
        """
        import ast
        flow_path = os.path.abspath(self.flow_path)
        with open(flow_path) as f:
            source = f.read()
        tree = ast.parse(source)

        # Strategy 1: Find class instantiated in if __name__ == '__main__' block
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                # Check for if __name__ == '__main__'
                test = node.test
                if (isinstance(test, ast.Compare) and
                    isinstance(test.left, ast.Name) and
                    test.left.id == "__name__"):
                    for stmt in node.body:
                        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                            func = stmt.value.func
                            if isinstance(func, ast.Name):
                                return func.id

        # Strategy 2: Last class definition in the top-level
        last_class = None
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.ClassDef):
                last_class = node.name

        # Strategy 3: Any class inheriting from FlowSpec
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    base_name = ""
                    if isinstance(base, ast.Name):
                        base_name = base.id
                    elif isinstance(base, ast.Attribute):
                        base_name = base.attr
                    if base_name == "FlowSpec":
                        return node.name

        return last_class or "Flow"

    def run(self, **kwargs):
        """Execute the flow. Returns a context manager yielding ExecutingRun."""
        return _RunContextManager(self, "run", kwargs)

    def resume(self, **kwargs):
        """Resume the flow. Returns a context manager yielding ExecutingRun."""
        return _RunContextManager(self, "resume", kwargs)

    def _execute(self, mode, **kwargs):
        """Execute a flow command."""
        flow_name = self._extract_flow_name()

        # Create temp files for logs
        stdout_file = tempfile.NamedTemporaryFile(
            mode="w", suffix="_stdout.log", delete=False
        )
        stderr_file = tempfile.NamedTemporaryFile(
            mode="w", suffix="_stderr.log", delete=False
        )

        # Build command - use absolute path for flow file
        flow_path = os.path.abspath(self.flow_path)
        cmd = [sys.executable, flow_path]
        cmd.extend(self._build_top_opts())
        cmd.append(mode)

        # Handle run_id_file
        run_id_file = kwargs.pop("run_id_file", None)
        if not run_id_file:
            run_id_file = tempfile.mktemp(suffix="_runid")
        cmd.extend(["--run-id-file", run_id_file])

        # Handle step_to_rerun for resume
        step_to_rerun = kwargs.pop("step_to_rerun", None)
        if step_to_rerun and mode == "resume":
            cmd.append(step_to_rerun)

        # Add remaining kwargs as options
        for key, val in kwargs.items():
            cli_key = "--%s" % key.replace("_", "-")
            if isinstance(val, bool):
                if val:
                    cmd.append(cli_key)
            elif isinstance(val, list):
                for item in val:
                    cmd.extend([cli_key, str(item)])
            elif val is not None:
                cmd.extend([cli_key, str(val)])

        # Set up environment
        run_env = dict(os.environ)
        if self.env:
            run_env.update({k: str(v) for k, v in self.env.items()})

        # Ensure datastore root is absolute so client API can find artifacts
        # Only set when cwd is explicitly provided (so client API in the
        # same process can find the data written by the subprocess)
        if self.cwd and "METAFLOW_DATASTORE_SYSROOT_LOCAL" not in run_env:
            ds_root = os.path.join(os.path.abspath(self.cwd), ".metaflow")
            run_env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = ds_root
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = ds_root

        # Execute
        with open(stdout_file.name, "w") as stdout_f, \
             open(stderr_file.name, "w") as stderr_f:
            process = subprocess.run(
                cmd,
                stdout=stdout_f,
                stderr=stderr_f,
                env=run_env,
                cwd=self.cwd,
            )

        # Read run ID
        run_id = None
        try:
            with open(run_id_file) as f:
                run_id = f.read().strip()
        except FileNotFoundError:
            pass

        log_files = {
            "stdout": stdout_file.name,
            "stderr": stderr_file.name,
        }

        command_obj = _CommandObj(cmd, process, log_files)
        return _ExecutingRun(command_obj, flow_name, run_id)

    def spin(self, pathspec, artifacts_module=None, persist=False,
             skip_decorators=False, **kwargs):
        """Re-execute a single step."""
        return _SpinContextManager(
            self, pathspec, artifacts_module, persist, skip_decorators, kwargs
        )


class _RunContextManager:
    """Context manager for Runner.run() / Runner.resume().

    Supports both usage patterns:
      - Direct: result = runner.run(); result.command_obj  (lazy execution)
      - Context manager: with runner.run() as result: result.run
    """
    def __init__(self, runner, mode, kwargs):
        self._runner = runner
        self._mode = mode
        self._kwargs = kwargs
        self._executing_run = None

    def _ensure_executed(self):
        if self._executing_run is None:
            self._executing_run = self._runner._execute(self._mode, **self._kwargs)
        return self._executing_run

    def __enter__(self):
        return self._ensure_executed()

    def __exit__(self, *args):
        pass

    # Proxy attributes to the underlying _ExecutingRun for direct usage
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._ensure_executed(), name)


class _SpinContextManager:
    """Context manager for Runner.spin()."""
    def __init__(self, runner, pathspec, artifacts_module, persist,
                 skip_decorators, kwargs):
        self.runner = runner
        self.pathspec = pathspec
        self.artifacts_module = artifacts_module
        self.persist = persist
        self.skip_decorators = skip_decorators
        self.kwargs = kwargs
        self._context = None

    def __enter__(self):
        if self.kwargs:
            # Check for flow parameters
            raise Exception("Unknown argument")
        self._context = _SpinContext(
            self.runner, self.pathspec, self.artifacts_module,
            self.persist, self.skip_decorators
        )
        self._context.execute()
        return self._context

    def __exit__(self, *args):
        pass


class _SpinTask:
    """In-memory task-like object for spin results."""
    def __init__(self, pathspec, artifacts, parent_pathspecs=None):
        self._pathspec = pathspec
        self._artifacts = artifacts  # dict of name -> value
        self._parent_pathspecs = parent_pathspecs or []

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def finished(self):
        return True

    @property
    def successful(self):
        return True

    @property
    def parent_task_pathspecs(self):
        return iter(self._parent_pathspecs)

    @property
    def artifacts(self):
        from ..client import DataArtifact, _ArtifactCollection
        result = []
        for name, val in self._artifacts.items():
            result.append(DataArtifact(self._pathspec, name=name, value=val))
        return _ArtifactCollection(result)

    def __getitem__(self, name):
        from ..client import DataArtifact
        if name in self._artifacts:
            return DataArtifact(self._pathspec, name=name,
                                value=self._artifacts[name])
        raise KeyError("No artifact '%s'" % name)

    def __contains__(self, name):
        return name in self._artifacts

    def __iter__(self):
        from ..client import DataArtifact
        for name, val in self._artifacts.items():
            yield DataArtifact(self._pathspec, name=name, value=val)


class _InputView:
    """Provides attribute access to a set of artifacts for join step inputs."""
    def __init__(self, artifacts_dict, step_name=None):
        object.__setattr__(self, '_artifacts', artifacts_dict)
        object.__setattr__(self, '_step_name', step_name)

    def get_artifacts(self) -> dict:
        """Return artifact dict. Used by merge_artifacts."""
        return self._artifacts

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._artifacts:
            return self._artifacts[name]
        raise AttributeError("No artifact '%s'" % name)


class _InputsView:
    """Provides iteration and step-name attribute access over join inputs."""
    def __init__(self, inputs_list):
        self._inputs = inputs_list
        self._by_name = {}
        for inp in inputs_list:
            if inp._step_name:
                self._by_name[inp._step_name] = inp

    def __iter__(self):
        return iter(self._inputs)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._by_name:
            return self._by_name[name]
        raise AttributeError("No input from step '%s'" % name)


class _SpinContext:
    """Context for spin execution."""
    def __init__(self, runner, pathspec, artifacts_module, persist, skip_decorators):
        self.runner = runner
        self.pathspec = pathspec
        self.artifacts_module = artifacts_module
        self.persist = persist
        self.skip_decorators = skip_decorators
        self._task = None
        self._spin_pathspec = None

    def execute(self):
        """Execute the spin — re-run the step with optional artifact overrides."""
        import importlib.util
        import time
        import pickle

        from ..datastore.local import LocalDatastore
        from ..plugins.metadata_providers.local import LocalMetadataProvider
        from ..graph import FlowGraph

        # Parse pathspec: FlowName/RunId/StepName/TaskId
        parts = self.pathspec.split("/")
        if len(parts) != 4:
            raise ValueError("Pathspec must be FlowName/RunId/StepName/TaskId")
        flow_name, run_id, step_name, task_id = parts

        # Determine datastore root
        ds_root = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        if not ds_root and self.runner.cwd:
            ds_root = os.path.join(os.path.abspath(self.runner.cwd), ".metaflow")
        datastore = LocalDatastore(root=ds_root)
        metadata = LocalMetadataProvider(datastore_root=ds_root)

        # Load original task artifacts (the step's OUTPUT) for reference
        orig_artifacts = datastore.load_artifacts(flow_name, run_id, step_name, task_id)

        # Load the flow module
        flow_path = os.path.abspath(self.runner.flow_path)
        spec = importlib.util.spec_from_file_location("__spin_flow__", flow_path,
                                                       submodule_search_locations=[os.path.dirname(flow_path)])
        flow_mod = importlib.util.module_from_spec(spec)

        # Add flow directory to path for local imports
        flow_dir = os.path.dirname(flow_path)
        old_path = list(sys.path)
        if flow_dir not in sys.path:
            sys.path.insert(0, flow_dir)
        try:
            spec.loader.exec_module(flow_mod)
        finally:
            sys.path = old_path

        # Find the flow class
        flow_cls = None
        target_name = self.runner._extract_flow_name()
        for attr in dir(flow_mod):
            obj = getattr(flow_mod, attr)
            if isinstance(obj, type) and attr == target_name:
                flow_cls = obj
                break

        if flow_cls is None:
            raise RuntimeError("Could not find flow class '%s'" % target_name)

        # Create a flow instance without CLI
        flow = flow_cls(use_cli=False)
        graph = flow._graph

        # Check if this is a join step
        node = graph[step_name]
        is_join = node.type == "join" or len(node.in_funcs) > 1

        # Get parent pathspecs from metadata
        parent_pathspecs = []
        try:
            task_meta = metadata.get_task_metadata(flow_name, run_id, step_name, task_id)
            for entry in task_meta:
                if entry.get("type") == "parent-task-ids":
                    import json as _json
                    parent_pathspecs = _json.loads(entry.get("value", "[]"))
        except Exception:
            pass

        # Load PARENT artifacts (the INPUT to this step)
        parent_artifacts = {}
        if not is_join and parent_pathspecs:
            # For non-join steps, load from the single parent
            ps = parent_pathspecs[0]
            ps_parts = ps.split("/")
            if len(ps_parts) == 4:
                p_flow, p_run, p_step, p_task = ps_parts
                parent_artifacts = datastore.load_artifacts(p_flow, p_run, p_step, p_task)
            elif len(ps_parts) == 3:
                p_step, p_task = ps_parts[1], ps_parts[2]
                parent_artifacts = datastore.load_artifacts(flow_name, run_id, p_step, p_task)
        elif not is_join:
            # Start step — no parent, use orig_artifacts for params/configs only
            parent_artifacts = orig_artifacts

        # Set up artifacts on the flow from parent (non-private)
        flow.load_parent_state(parent_artifacts)

        # Also copy parameters and configs from orig_artifacts (they persist)
        for param_name, param_desc in flow._params.items():
            attr_name = getattr(param_desc, '_attr_name', param_name)
            if attr_name in orig_artifacts:
                flow.set_artifact(attr_name, orig_artifacts[attr_name])
            elif param_name in orig_artifacts:
                flow.set_artifact(param_name, orig_artifacts[param_name])

        for config_name, config_desc in flow._configs.items():
            attr_name = config_desc._attr_name
            if attr_name in orig_artifacts:
                flow.set_artifact(attr_name, orig_artifacts[attr_name])
            elif config_name in orig_artifacts:
                flow.set_artifact(config_name, orig_artifacts[config_name])

        # Load artifacts override from artifacts_module
        override_artifacts = {}
        if self.artifacts_module:
            override_artifacts = self._load_artifacts_module(self.artifacts_module)

        # Apply artifact overrides
        if override_artifacts:
            if is_join and any(isinstance(v, dict) for v in override_artifacts.values()):
                pass  # Handled in the join execution
            else:
                for name, val in override_artifacts.items():
                    flow.set_artifact(name, val)

        # Resolve configs from runner top_level_kwargs or environment
        # This must happen AFTER loading parent artifacts so spin-provided
        # config values override the original run's config artifacts
        self._resolve_configs(flow)

        # Get the step function
        step_func = getattr(flow_cls, step_name, None)
        if step_func is None:
            raise RuntimeError("Step '%s' not found" % step_name)

        # Get decorators and resolve deferred config attributes
        decorators = []
        if not self.skip_decorators:
            decorators = list(getattr(step_func, "_decorators", []))
            self._resolve_deferred_deco_attrs(decorators, flow)

        # Generate spin run_id upfront so cards and persist use the same pathspec
        spin_run_id = str(int(time.time() * 1000000))

        # Execute the step
        from ..metaflow_current import current, TaskContext
        current.bind(TaskContext(
            flow_name=flow_name,
            run_id=spin_run_id,
            step_name=step_name,
            task_id="1",
            retry_count=0,
        ))

        # Set foreach context from _foreach_stack in the ORIGINAL task
        from ..runtime import _ensure_foreach_frames
        if "_foreach_stack" in orig_artifacts:
            fs = _ensure_foreach_frames(orig_artifacts["_foreach_stack"])
            flow._foreach_stack = fs
            if fs:
                last_frame = fs[-1]
                flow.set_input_context(last_frame.value, last_frame.index)

        flow._current_step = step_name

        try:
            for deco in decorators:
                deco.task_pre_step(step_name, None, None,
                                   current.run_id, current.task_id,
                                   flow, graph, 0, 0)

            if is_join:
                inputs = self._build_join_inputs(
                    datastore, flow_name, run_id, step_name, task_id,
                    node, metadata, override_artifacts
                )
                wrapped = lambda fl: step_func(fl, inputs)
            else:
                wrapped = lambda fl: step_func(fl)

            for deco in decorators:
                wrapped = deco.task_decorate(wrapped, flow, graph, 0, 0)

            wrapped(flow)

            for deco in reversed(decorators):
                deco.task_post_step(step_name, flow, graph, 0, 0)

        except Exception as e:
            suppressed = False
            for deco in reversed(decorators):
                if deco.task_exception(e, step_name, flow, graph, 0, 0):
                    suppressed = True
                    break
            if not suppressed:
                raise

        # Collect resulting artifacts (including internal state)
        result_artifacts = flow.get_persistable_state(task_ok=True)

        # Build graph info
        graph_info = {}
        for n in graph:
            graph_info[n.name] = {
                "type": n.type,
                "in_funcs": n.in_funcs,
                "out_funcs": n.out_funcs,
            }
        result_artifacts["_graph_info"] = graph_info

        if self.persist:
            # Create a new spin run/task in the datastore
            spin_task_id = "1"
            self._spin_pathspec = "%s/%s/%s/%s" % (
                flow_name, spin_run_id, step_name, spin_task_id
            )

            metadata.new_run(flow_name, spin_run_id, [], [])
            metadata.new_step(flow_name, spin_run_id, step_name)
            metadata.new_task(flow_name, spin_run_id, step_name, spin_task_id)

            # Save parent-task-ids metadata
            if parent_pathspecs:
                import json as _json
                metadata.register_metadata(flow_name, spin_run_id, step_name,
                                           spin_task_id,
                                           [{"type": "parent-task-ids",
                                             "value": _json.dumps(parent_pathspecs)}])

            metadata.register_metadata(flow_name, spin_run_id, step_name,
                                       spin_task_id,
                                       [{"type": "attempt", "value": "0"}])
            metadata.done_task(flow_name, spin_run_id, step_name, spin_task_id)
            metadata.done_run(flow_name, spin_run_id)

            datastore.save_artifacts(flow_name, spin_run_id, step_name,
                                     spin_task_id, result_artifacts)

            self._task = None  # Will be loaded from client
        else:
            self._spin_pathspec = self.pathspec
            self._task = _SpinTask(
                self.pathspec, result_artifacts, parent_pathspecs
            )

    def _resolve_deferred_deco_attrs(self, decorators, flow):
        """Resolve _DeferredConfigAttr values in decorator attributes."""
        from ..user_configs.config_parameters import _DeferredConfigAttr
        for deco in decorators:
            for key, val in list(deco.attributes.items()):
                if isinstance(val, _DeferredConfigAttr):
                    # Resolve by walking config value attributes
                    config_name = val._config_name
                    attr_chain = val._attr_chain
                    arts = flow.get_artifacts()
                    resolved = arts.get(config_name)
                    if resolved is None:
                        resolved = arts.get(config_name.replace("-", "_"))
                    try:
                        for attr in attr_chain:
                            resolved = resolved[attr] if isinstance(resolved, dict) else getattr(resolved, attr)
                        deco.attributes[key] = resolved
                    except (KeyError, AttributeError, TypeError):
                        pass

    def _resolve_configs(self, flow):
        """Resolve config values from runner kwargs or environment.

        Also updates flow._artifacts with resolved config values so that
        spin-provided configs override values loaded from parent artifacts.
        """
        config_values = {}
        for key, val in self.runner.top_level_kwargs.items():
            if key == "config_value":
                for cfg_name, cfg_val in val:
                    config_values[cfg_name] = cfg_val

        for config_name, config_desc in flow._configs.items():
            if config_name in config_values:
                config_desc.resolve(config_values[config_name])
                # Update flow artifacts with spin-provided config value
                attr_name = getattr(config_desc, '_attr_name', config_name)
                flow.set_artifact(attr_name, config_desc.value)
            else:
                config_desc.resolve()

    def _load_artifacts_module(self, path):
        """Load ARTIFACTS dict from a Python file."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("__artifacts__", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, "ARTIFACTS", {})

    def _build_join_inputs(self, datastore, flow_name, run_id, step_name,
                           task_id, node, metadata, override_artifacts):
        """Build InputsView for a join step."""
        # Find parent tasks from metadata
        task_meta = metadata.get_task_metadata(flow_name, run_id, step_name, task_id)
        parent_pathspecs = []
        for entry in task_meta:
            if entry.get("type") == "parent-task-ids":
                import json as _json
                parent_pathspecs = _json.loads(entry.get("value", "[]"))

        inputs = []
        for ps in parent_pathspecs:
            ps_parts = ps.split("/")
            if len(ps_parts) == 4:
                _, p_run, p_step, p_task = ps_parts
            elif len(ps_parts) == 3:
                p_run, p_step, p_task = run_id, ps_parts[0], ps_parts[1]
            else:
                continue

            arts = datastore.load_artifacts(flow_name, p_run, p_step, p_task)

            # Check for overrides keyed by pathspec
            override_key = "%s/%s/%s" % (p_run, p_step, p_task)
            if override_key in override_artifacts:
                arts.update(override_artifacts[override_key])

            inputs.append(_InputView(arts, step_name=p_step))

        return _InputsView(inputs)

    @property
    def task(self):
        if self._task is None:
            from ..client import Task
            ps = self._spin_pathspec or self.pathspec
            self._task = Task(ps, _namespace_check=False)
        return self._task


def inspect_spin(flows_dir):
    """Set metadata provider for spin tasks.

    This sets METAFLOW_DATASTORE_SYSROOT_LOCAL so the Client API
    can find artifacts created by spin tasks run from flows_dir.
    """
    ds_root = os.path.join(os.path.abspath(flows_dir), ".metaflow")
    os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = ds_root
