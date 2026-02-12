"""Runtime engine — orchestrates flow execution."""

import copy
import io
import json
import os
import sys
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Optional

ForeachFrame = namedtuple("ForeachFrame", ["step", "var", "index", "value", "num_splits"])


@dataclass(frozen=True)
class TaskResult:
    """Child→parent IPC result for a forked task execution."""
    success: bool
    taken_branch: str = None       # switch routing
    exception: Exception = None    # if failed
    exception_message: str = None  # fallback when exception not picklable
    exception_type: str = None     # fallback


def _ensure_foreach_frames(stack):
    """Convert raw tuples in a foreach stack to ForeachFrame namedtuples."""
    result = []
    for item in stack:
        if isinstance(item, ForeachFrame):
            result.append(item)
        elif isinstance(item, (tuple, list)) and len(item) >= 5:
            result.append(ForeachFrame(item[0], item[1], item[2], item[3], item[4]))
        elif isinstance(item, (tuple, list)) and len(item) >= 4:
            result.append(ForeachFrame(item[0], item[1], item[2], item[3], None))
        else:
            result.append(item)
    return result

from .metaflow_current import current
from .decorators import (
    RetryDecorator, CatchDecorator, TimeoutDecorator, EnvironmentDecorator,
    SecretsDecorator, CardDecorator, ParallelDecorator,
    MetaflowExceptionWrapper, _ConfigExpr,
)
from .user_configs.config_parameters import ConfigValue, _DeferredConfigAttr
from .parameters import Parameter
from .includefile import IncludeFile
from .user_configs.config_parameters import Config


class _InputProxy:
    """Represents a single input to a join step."""
    def __init__(self, artifacts, step_name=None):
        self._artifacts = artifacts
        self._step_name = step_name

    def get_artifacts(self) -> dict:
        """Return artifact dict. Used by merge_artifacts."""
        return self._artifacts

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._artifacts:
            return self._artifacts[name]
        raise AttributeError("Input has no artifact '%s'" % name)


class _InputsProxy:
    """Represents all inputs to a join step."""
    def __init__(self, inputs_list):
        self._inputs = inputs_list
        self._by_name = {}
        for inp in inputs_list:
            if inp._step_name:
                self._by_name[inp._step_name] = inp

    def __iter__(self):
        return iter(self._inputs)

    def __getitem__(self, idx):
        if isinstance(idx, int):
            return self._inputs[idx]
        raise KeyError(idx)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._by_name:
            return self._by_name[name]
        raise AttributeError("No branch named '%s'" % name)

    def __len__(self):
        return len(self._inputs)


class Runtime:
    """Execute a flow DAG."""

    def __init__(self, flow_cls, graph, datastore, metadata,
                 run_id, tags=None, sys_tags=None,
                 max_workers=16, max_num_splits=100,
                 origin_run_id=None):
        self.flow_cls = flow_cls
        self.graph = graph
        self.datastore = datastore
        self.metadata = metadata
        self.run_id = str(run_id)
        self.tags = list(tags or [])
        self.sys_tags = list(sys_tags or [])
        self.max_workers = max_workers
        self.max_num_splits = max_num_splits
        self.origin_run_id = origin_run_id

        self._task_counter = 0
        # step_name -> list of (task_id, artifacts_dict)
        self._step_results = {}
        # Steps already executed (for foreach handling)
        self._executed_steps = set()
        # Cached parameter values (computed once)
        self._resolved_params = None

    def _next_task_id(self):
        self._task_counter += 1
        return str(self._task_counter)

    def execute(self, resume_step=None):
        """Execute the flow."""
        flow_name = self.flow_cls.__name__

        # Set project info on current early (before param resolution)
        from .metaflow_current import TaskContext
        username = os.environ.get("METAFLOW_USER", os.environ.get("USER", "unknown"))
        current.bind(TaskContext(
            flow_name=flow_name,
            run_id=self.run_id,
            step_name="",
            task_id="",
            namespace="user:%s" % username,
            username=username,
            origin_run_id=self.origin_run_id,
            project_name=getattr(self.flow_cls, "_project_name", None),
            branch_name=getattr(self.flow_cls, "_branch_name", None),
            project_flow_name=getattr(self.flow_cls, "_project_flow_name", None),
            is_production=getattr(self.flow_cls, "_is_production", False),
        ))

        self.metadata.new_run(flow_name, self.run_id,
                              tags=self.tags, sys_tags=self.sys_tags)

        configs = self._resolve_configs()

        # Initialize all flow decorators
        flow_decos = getattr(self.flow_cls, "_flow_decorators", [])
        from .decorators import ProjectDecorator
        for deco in flow_decos:
            if isinstance(deco, ProjectDecorator):
                name = deco.project_name
                if isinstance(name, _ConfigExpr):
                    name = self._eval_config_expr(name.expr, configs)
                elif isinstance(name, _DeferredConfigAttr):
                    name = self._eval_config_expr(name._expr, configs)
                if name != deco.project_name:
                    deco.project_name = name
                    deco.flow_init(self.flow_cls, self.graph, None, None, None)
                    # Update sys_tags with project info
                    if hasattr(self.flow_cls, "_project_name"):
                        proj_tags = [
                            "project:%s" % self.flow_cls._project_name,
                            "project_branch:%s" % self.flow_cls._branch_name,
                        ]
                        if self.flow_cls._is_production:
                            proj_tags.append("production:True")
                        for pt in proj_tags:
                            if pt not in self.sys_tags:
                                self.sys_tags.append(pt)
                        # Update current with new project info
                        current._update(
                            project_name=self.flow_cls._project_name,
                            branch_name=self.flow_cls._branch_name,
                            project_flow_name=self.flow_cls._project_flow_name,
                            is_production=self.flow_cls._is_production,
                        )
            else:
                # Other flow decorators (extensions): call flow_init with options
                options = {}
                deco_options = getattr(deco, "options", {})
                for opt_name, opt_spec in deco_options.items():
                    env_key = "METAFLOW_%s" % opt_name.upper()
                    env_val = os.environ.get(env_key)
                    if env_val is not None:
                        options[opt_name] = env_val
                    else:
                        default = opt_spec.get("default") if isinstance(opt_spec, dict) else None
                        options[opt_name] = default
                if hasattr(deco, "flow_init"):
                    try:
                        import inspect
                        sig = inspect.signature(deco.flow_init)
                        if "options" in sig.parameters:
                            deco.flow_init(
                                self.flow_cls, self.graph, None, None,
                                self.metadata, None, None, options
                            )
                        else:
                            deco.flow_init(self.flow_cls, self.graph, None, None, None)
                    except TypeError:
                        pass

        topo_order = list(self.graph)

        # Resume: find reusable steps
        reused_steps = set()
        if self.origin_run_id and resume_step:
            reused_steps = self._find_reusable_steps(topo_order, resume_step)

        for node in topo_order:
            step_name = node.name

            # Skip if already executed (e.g., as part of foreach chain)
            if step_name in self._executed_steps:
                continue

            # Register step
            self.metadata.new_step(flow_name, self.run_id, step_name)

            # Resume: reuse completed tasks
            if step_name in reused_steps:
                # For foreach splits: check if the entire foreach block is reusable
                if node.type == "foreach" and node.foreach_param and node.matching_join:
                    # Collect all steps in this foreach block
                    inner_steps = self._collect_inner_steps(
                        node.out_funcs[0], node.matching_join
                    )
                    all_block_steps = [step_name] + inner_steps + [node.matching_join]
                    all_reusable = all(s in reused_steps for s in all_block_steps)

                    if all_reusable:
                        # Reuse entire foreach block
                        for bs in all_block_steps:
                            if bs not in self._executed_steps:
                                if bs not in self._step_results:
                                    self.metadata.new_step(flow_name, self.run_id, bs)
                                self._reuse_step(flow_name, bs)
                                self._executed_steps.add(bs)
                        continue
                    else:
                        # Some inner steps need re-execution
                        # Reuse the split step from origin run, then re-execute
                        # inner chains (step code uses is_resumed() to handle)
                        self._reuse_step(flow_name, step_name)
                        self._execute_foreach_split_and_chain(
                            flow_name, step_name, node, configs,
                            reuse_split=True,
                        )
                        continue

                self._reuse_step(flow_name, step_name)
                self._executed_steps.add(step_name)
                continue

            if node.type == "foreach" and node.foreach_param:
                self._execute_foreach_split_and_chain(flow_name, step_name, node, configs)
            elif node.type == "foreach" and not node.foreach_param:
                # Parallel split: execute as linear step; parallel tasks are
                # created when the runtime processes the parallel_step node
                self._execute_linear_step(flow_name, step_name, node, configs)
            elif node.type == "split-or":
                # Must check split-or BEFORE join because recursive switches
                # have multiple in_funcs (from self + parent)
                self._execute_switch_step(flow_name, step_name, node, configs)
            elif node.type == "join" or len(node.in_funcs) > 1:
                # Check if this is a true join or a merge after switch
                # For switch merges, only one parent was executed
                executed_parents = [p for p in node.in_funcs if p in self._step_results]
                import inspect
                func = getattr(self.flow_cls, step_name, None)
                sig = inspect.signature(func) if func else None
                param_count = len(sig.parameters) if sig else 1

                if len(executed_parents) == 1 and param_count <= 1:
                    # Single parent executed and function doesn't take inputs
                    # → treat as linear step (post-switch merge)
                    self._execute_linear_step(flow_name, step_name, node, configs)
                else:
                    self._execute_join_step(flow_name, step_name, node, configs)
            elif node.parallel_step and node.num_parallel > 0:
                self._execute_parallel_step(flow_name, step_name, node, configs)
            else:
                self._execute_linear_step(flow_name, step_name, node, configs)

            self._executed_steps.add(step_name)

        self.metadata.done_run(flow_name, self.run_id)

    def _resolve_configs(self):
        """Resolve all Config descriptors on the flow class."""
        configs = {}
        config_values_env = {}
        config_files_env = {}

        env_config_value = os.environ.get("METAFLOW_FLOW_CONFIG_VALUE")
        if env_config_value:
            try:
                config_values_env = json.loads(env_config_value)
            except json.JSONDecodeError:
                pass

        env_config = os.environ.get("METAFLOW_FLOW_CONFIG")
        if env_config:
            try:
                config_files_env = json.loads(env_config)
            except json.JSONDecodeError:
                pass

        cli_config_values = {}
        cli_config_files = {}
        env_cli_vals = os.environ.get("_METAFLOW_CLI_CONFIG_VALUE")
        if env_cli_vals:
            try:
                cli_config_values = json.loads(env_cli_vals)
            except json.JSONDecodeError:
                pass
        env_cli_files = os.environ.get("_METAFLOW_CLI_CONFIG")
        if env_cli_files:
            try:
                cli_config_files = json.loads(env_cli_files)
            except json.JSONDecodeError:
                pass

        for attr_name in dir(self.flow_cls):
            obj = getattr(self.flow_cls, attr_name, None)
            if isinstance(obj, Config):
                config_name = obj.name
                value = None

                if config_name in cli_config_values:
                    value = cli_config_values[config_name]
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass
                elif config_name in cli_config_files:
                    with open(cli_config_files[config_name]) as f:
                        value = f.read()
                elif config_name in config_values_env:
                    value = config_values_env[config_name]
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass
                elif config_name in config_files_env:
                    with open(config_files_env[config_name]) as f:
                        value = f.read()

                if value is None and callable(obj.default_value):
                    ctx = _ConfigContext(configs)
                    value = obj.default_value(ctx)

                if value is not None:
                    obj.resolve(value)
                else:
                    obj.resolve()

                configs[config_name] = obj.value
                configs[obj._attr_name] = obj.value

        return configs

    def _resolve_decorator_args(self, deco, configs):
        """Resolve config expressions in decorator arguments."""
        for key, val in list(deco.attributes.items()):
            if isinstance(val, _ConfigExpr):
                resolved = self._eval_config_expr(val.expr, configs)
                deco.attributes[key] = resolved
            elif isinstance(val, _DeferredConfigAttr):
                resolved = self._eval_config_expr(val._expr, configs)
                deco.attributes[key] = resolved
            elif isinstance(val, dict):
                # Resolve nested config expressions in dict values
                new_dict = {}
                for dk, dv in val.items():
                    if isinstance(dv, _ConfigExpr):
                        new_dict[dk] = self._eval_config_expr(dv.expr, configs)
                    elif isinstance(dv, _DeferredConfigAttr):
                        new_dict[dk] = self._eval_config_expr(dv._expr, configs)
                    else:
                        new_dict[dk] = dv
                deco.attributes[key] = new_dict

    def _eval_config_expr(self, expr, configs):
        """Evaluate a config expression."""
        try:
            safe_builtins = {"str": str, "int": int, "float": float, "bool": bool,
                             "list": list, "dict": dict, "len": len, "repr": repr,
                             "True": True, "False": False, "None": None}
            namespace = dict(safe_builtins)
            namespace.update(configs)
            return eval(expr, {"__builtins__": safe_builtins}, namespace)
        except Exception:
            return expr

    def _get_parent_artifacts(self, step_name, node):
        """Load artifacts from parent step(s)."""
        if step_name == "start" or not node.in_funcs:
            return {}
        # Try each in_func to find one with results (for switch merges,
        # only the taken branch has results)
        for parent_name in node.in_funcs:
            if parent_name in self._step_results:
                results = self._step_results[parent_name]
                if results:
                    # Use last result: for recursive switches, the last task
                    # has the final state; for linear steps there's only one
                    return dict(results[-1][1])
        return {}

    def _create_flow_instance(self, configs):
        """Create a fresh flow instance with params and configs set."""
        flow = self.flow_cls._create_instance(self.graph)
        return flow

    def _get_parent_task_pathspecs(self, step_name, node):
        """Get pathspecs of parent tasks.
        For linear/switch steps: returns pathspec of last task from parent.
        For join steps: _execute_join_step builds its own pathspec list."""
        if step_name == "start" or not node.in_funcs:
            return []
        # Try each in_func to find one with results
        for parent_name in node.in_funcs:
            if parent_name in self._step_results:
                results = self._step_results[parent_name]
                if results:
                    flow_name = self.flow_cls.__name__
                    # Use last result (important for recursive switches)
                    tid, _ = results[-1]
                    return [
                        "%s/%s/%s/%s" % (flow_name, self.run_id, parent_name, tid)
                    ]
        return []

    def _execute_linear_step(self, flow_name, step_name, node, configs):
        """Execute a linear step (single task)."""
        task_id = self._next_task_id()
        parent_artifacts = self._get_parent_artifacts(step_name, node)
        parent_pathspecs = self._get_parent_task_pathspecs(step_name, node)
        self._execute_task(
            flow_name, step_name, task_id, node, configs,
            parent_artifacts=parent_artifacts,
            parent_task_pathspecs=parent_pathspecs,
        )

    def _execute_foreach_split_and_chain(self, flow_name, step_name, node, configs,
                                         override_parent_arts=None,
                                         override_parent_pathspecs=None,
                                         reuse_split=False):
        """Execute a foreach split, inner steps (possibly nested), and join."""
        # 1. Execute (or reuse) the split step
        if reuse_split and step_name in self._step_results and self._step_results[step_name]:
            # Split already reused from origin run; skip re-execution
            pass
        else:
            task_id = self._next_task_id()
            parent_artifacts = override_parent_arts if override_parent_arts is not None \
                else self._get_parent_artifacts(step_name, node)
            parent_pathspecs = override_parent_pathspecs if override_parent_pathspecs is not None \
                else self._get_parent_task_pathspecs(step_name, node)

            if step_name not in self._step_results:
                self._step_results[step_name] = []

            self._execute_task(
                flow_name, step_name, task_id, node, configs,
                parent_artifacts=parent_artifacts,
                parent_task_pathspecs=parent_pathspecs,
            )
        self._executed_steps.add(step_name)

        # 2. Get the foreach var and items
        split_task_id, split_artifacts = self._step_results[step_name][-1]
        foreach_var = node.foreach_param
        foreach_items = split_artifacts.get(foreach_var, [])

        if not foreach_items or not node.out_funcs:
            return

        # 3. Find the matching join for this foreach level
        join_step_name = node.matching_join

        inner_steps = self._collect_inner_steps(node.out_funcs[0], join_step_name)

        # Register inner steps
        for inner_step_name in inner_steps:
            if inner_step_name not in self._step_results:
                self.metadata.new_step(flow_name, self.run_id, inner_step_name)
                self._step_results[inner_step_name] = []

        from .plugins import InternalTestUnboundedForeachInput
        if isinstance(foreach_items, InternalTestUnboundedForeachInput):
            num_splits = None  # Unbounded: num_splits unknown
        else:
            num_splits = len(foreach_items)
        split_task_pathspec = "%s/%s/%s/%s" % (flow_name, self.run_id, step_name, split_task_id)

        # Save current step_results state for inner steps (to scope join correctly)
        saved_inner_results = {}
        for inner_step_name in inner_steps:
            saved_inner_results[inner_step_name] = list(
                self._step_results.get(inner_step_name, [])
            )
            self._step_results[inner_step_name] = []

        # 4. Execute inner steps for each foreach item
        for idx, item in enumerate(foreach_items):
            parent_arts = dict(split_artifacts)
            parent_arts["_foreach_stack"] = list(split_artifacts.get("_foreach_stack", []))
            parent_arts["_foreach_stack"].append(
                ForeachFrame(step_name, foreach_var, idx, item, num_splits)
            )

            self._execute_inner_chain(
                flow_name, inner_steps, join_step_name, configs,
                parent_arts, item, idx,
                parent_arts.get("_foreach_stack", []),
                split_task_pathspec,
            )

        # Mark inner steps as executed
        for inner_step_name in inner_steps:
            self._executed_steps.add(inner_step_name)

        # 5. Execute the join step (uses current scoped results for inner steps)
        if join_step_name:
            join_node = self.graph[join_step_name]
            if join_step_name not in self._step_results:
                self.metadata.new_step(flow_name, self.run_id, join_step_name)
                self._step_results[join_step_name] = []
            self._execute_join_step(flow_name, join_step_name, join_node, configs)
            self._executed_steps.add(join_step_name)

        # 5b. Create control task for unbounded foreach (InternalTestUnboundedForeachInput)
        from .plugins import InternalTestUnboundedForeachInput
        if isinstance(foreach_items, InternalTestUnboundedForeachInput):
            for inner_step_name in inner_steps:
                self._create_control_task(
                    flow_name, inner_step_name, split_artifacts,
                    split_task_pathspec, num_splits,
                )

        # 6. Restore inner step results (combine saved + this scope's results)
        for inner_step_name in inner_steps:
            current_results = self._step_results.get(inner_step_name, [])
            self._step_results[inner_step_name] = (
                saved_inner_results.get(inner_step_name, []) + current_results
            )

    def _collect_inner_steps(self, start_name, join_name):
        """Collect all step names between start and join (exclusive), handling nesting.
        Uses BFS to discover all reachable nodes including switch branches."""
        result = []
        visited = set()
        queue = [start_name]
        while queue:
            name = queue.pop(0)
            if name in visited or name == join_name:
                continue
            visited.add(name)
            result.append(name)
            cn = self.graph[name]
            for out in cn.out_funcs:
                if out not in visited and out != join_name:
                    # Allow self-references (recursive switch) - they won't
                    # be added again since name is already in visited
                    queue.append(out)
        return result

    def _execute_inner_chain(self, flow_name, inner_steps, join_name, configs,
                              parent_arts, foreach_input, foreach_index,
                              foreach_stack, prev_task_pathspec):
        """Execute a chain of inner steps for one foreach iteration.
        Handles nested foreach, branches, and switches by recursing/dispatching."""
        inner_set = set(inner_steps)
        executed_in_chain = set()
        i = 0
        while i < len(inner_steps):
            chain_step_name = inner_steps[i]

            if chain_step_name in executed_in_chain:
                i += 1
                continue

            chain_node = self.graph[chain_step_name]

            if chain_node.type == "foreach":
                # Nested foreach: recurse
                self._execute_foreach_split_and_chain(
                    flow_name, chain_step_name, chain_node, configs,
                    override_parent_arts=parent_arts,
                    override_parent_pathspecs=[prev_task_pathspec],
                )
                executed_in_chain.add(chain_step_name)
                # Skip past the nested foreach's inner steps + join
                nested_join = chain_node.matching_join
                for s in inner_steps[i:]:
                    if s == nested_join:
                        break
                    executed_in_chain.add(s)
                if nested_join:
                    executed_in_chain.add(nested_join)
                    # Update parent_arts from the join results
                    join_results = self._step_results.get(nested_join, [])
                    if join_results:
                        _, parent_arts = join_results[-1]
                        parent_arts = dict(parent_arts)
                    prev_task_pathspec = "%s/%s/%s/%s" % (
                        flow_name, self.run_id, nested_join, join_results[-1][0]
                    ) if join_results else prev_task_pathspec
                i += 1
                continue

            elif chain_node.type == "split-or":
                # Switch inside foreach: execute switch step and route
                parent_arts, prev_task_pathspec = self._execute_inner_switch(
                    flow_name, chain_step_name, chain_node, configs,
                    inner_steps, inner_set, executed_in_chain,
                    parent_arts, foreach_input, foreach_index, foreach_stack,
                    prev_task_pathspec,
                )
                i += 1
                continue

            elif chain_node.type == "split-and":
                # Branch inside foreach: execute split, branches, and join
                parent_arts, prev_task_pathspec = self._execute_inner_branch(
                    flow_name, chain_step_name, chain_node, configs,
                    inner_steps, inner_set, executed_in_chain,
                    parent_arts, foreach_input, foreach_index, foreach_stack,
                    prev_task_pathspec,
                )
                i += 1
                continue

            elif chain_node.type == "join":
                # Inner join (from nested split-and or similar)
                self._execute_join_step(flow_name, chain_step_name, chain_node, configs)
                executed_in_chain.add(chain_step_name)
                join_results = self._step_results.get(chain_step_name, [])
                if join_results:
                    _, parent_arts = join_results[-1]
                    parent_arts = dict(parent_arts)
                prev_task_pathspec = "%s/%s/%s/%s" % (
                    flow_name, self.run_id, chain_step_name,
                    join_results[-1][0] if join_results else "0"
                )
                i += 1
                continue

            # Linear step
            task_id = self._next_task_id()
            self._execute_task(
                flow_name, chain_step_name, task_id, chain_node, configs,
                parent_artifacts=parent_arts,
                foreach_input=foreach_input,
                foreach_index=foreach_index,
                foreach_stack=foreach_stack,
                parent_task_pathspecs=[prev_task_pathspec],
            )
            executed_in_chain.add(chain_step_name)

            prev_task_pathspec = "%s/%s/%s/%s" % (flow_name, self.run_id, chain_step_name, task_id)

            # Update parent_arts for next step
            for tid, arts in self._step_results[chain_step_name]:
                if tid == task_id:
                    parent_arts = dict(arts)
                    break

            i += 1

    def _execute_inner_switch(self, flow_name, step_name, node, configs,
                               inner_steps, inner_set, executed_in_chain,
                               parent_arts, foreach_input, foreach_index,
                               foreach_stack, prev_task_pathspec):
        """Execute a switch step inside a foreach iteration.
        Returns (updated_parent_arts, updated_prev_pathspec)."""
        # Handle recursive switch (self-loop) inside foreach
        while True:
            task_id = self._next_task_id()
            switch_info = self._execute_task(
                flow_name, step_name, task_id, node, configs,
                parent_artifacts=parent_arts,
                foreach_input=foreach_input,
                foreach_index=foreach_index,
                foreach_stack=foreach_stack,
                parent_task_pathspecs=[prev_task_pathspec],
            )

            taken_branch = None
            if switch_info and "taken_branch" in switch_info:
                taken_branch = switch_info["taken_branch"]

            if taken_branch == step_name:
                # Recursive: update parent_arts and loop
                results = self._step_results.get(step_name, [])
                if results:
                    _, last_arts = results[-1]
                    parent_arts = dict(last_arts)
                prev_task_pathspec = "%s/%s/%s/%s" % (
                    flow_name, self.run_id, step_name, task_id
                )
                continue
            else:
                break

        executed_in_chain.add(step_name)
        prev_task_pathspec = "%s/%s/%s/%s" % (
            flow_name, self.run_id, step_name, task_id
        )

        # Update parent_arts from the switch step
        results = self._step_results.get(step_name, [])
        if results:
            _, parent_arts = results[-1]
            parent_arts = dict(parent_arts)

        # Mark non-taken branches within inner_steps as executed
        if taken_branch:
            for branch_name in node.out_funcs:
                if branch_name != taken_branch and branch_name != step_name:
                    # Mark this branch and descendants (within inner_set) as executed
                    self._mark_inner_descendants(branch_name, inner_set, executed_in_chain)

        return parent_arts, prev_task_pathspec

    def _mark_inner_descendants(self, start_name, inner_set, executed_in_chain):
        """Mark start_name and all its descendants that are within inner_set as executed."""
        queue = [start_name]
        while queue:
            name = queue.pop(0)
            if name in executed_in_chain:
                continue
            executed_in_chain.add(name)
            if name in self.graph:
                cn = self.graph[name]
                for out in cn.out_funcs:
                    if out in inner_set and out not in executed_in_chain:
                        queue.append(out)

    def _execute_inner_branch(self, flow_name, step_name, node, configs,
                               inner_steps, inner_set, executed_in_chain,
                               parent_arts, foreach_input, foreach_index,
                               foreach_stack, prev_task_pathspec):
        """Execute a branch split inside a foreach iteration.
        Returns (updated_parent_arts, updated_prev_pathspec)."""
        # Execute the split step itself
        task_id = self._next_task_id()
        self._execute_task(
            flow_name, step_name, task_id, node, configs,
            parent_artifacts=parent_arts,
            foreach_input=foreach_input,
            foreach_index=foreach_index,
            foreach_stack=foreach_stack,
            parent_task_pathspecs=[prev_task_pathspec],
        )
        executed_in_chain.add(step_name)

        split_pathspec = "%s/%s/%s/%s" % (flow_name, self.run_id, step_name, task_id)

        # Update parent_arts from split step
        results = self._step_results.get(step_name, [])
        split_arts = parent_arts
        if results:
            _, split_arts = results[-1]
            split_arts = dict(split_arts)

        # Execute each branch
        for branch_name in node.out_funcs:
            if branch_name in executed_in_chain:
                continue
            branch_node = self.graph[branch_name]
            # Execute this branch step
            btask_id = self._next_task_id()
            self._execute_task(
                flow_name, branch_name, btask_id, branch_node, configs,
                parent_artifacts=split_arts,
                foreach_input=foreach_input,
                foreach_index=foreach_index,
                foreach_stack=foreach_stack,
                parent_task_pathspecs=[split_pathspec],
            )
            executed_in_chain.add(branch_name)

        # The join will be handled when topo order reaches it in inner_steps
        return split_arts, split_pathspec

    def _execute_parallel_step(self, flow_name, step_name, node, configs):
        """Execute a parallel step with num_parallel tasks."""
        num_parallel = node.num_parallel
        parent_artifacts = self._get_parent_artifacts(step_name, node)
        parent_pathspecs = self._get_parent_task_pathspecs(step_name, node)

        self._step_results[step_name] = []

        for idx in range(num_parallel):
            task_id = self._next_task_id()
            self._execute_task(
                flow_name, step_name, task_id, node, configs,
                parent_artifacts=parent_artifacts,
                parallel_index=idx,
                parallel_total=num_parallel,
                parent_task_pathspecs=parent_pathspecs,
            )

    def _execute_switch_step(self, flow_name, step_name, node, configs):
        """Execute a switch/conditional step and only the taken branch.
        Handles recursive switches where taken_branch == step_name (self-loop)."""
        parent_artifacts = self._get_parent_artifacts(step_name, node)
        parent_pathspecs = self._get_parent_task_pathspecs(step_name, node)

        # Resume: for recursive switches, clone completed tasks from origin run
        is_recursive = step_name in node.out_funcs
        if is_recursive and self.origin_run_id:
            parent_artifacts, parent_pathspecs = self._clone_recursive_switch_tasks(
                flow_name, step_name, parent_artifacts, parent_pathspecs,
            )

        while True:
            task_id = self._next_task_id()
            switch_info = self._execute_task(
                flow_name, step_name, task_id, node, configs,
                parent_artifacts=parent_artifacts,
                parent_task_pathspecs=parent_pathspecs,
            )

            taken_branch = None
            if switch_info and "taken_branch" in switch_info:
                taken_branch = switch_info["taken_branch"]

            if taken_branch is None:
                return

            if taken_branch == step_name:
                # Recursive switch: update parent artifacts from just-executed task
                # and loop back to re-execute
                results = self._step_results.get(step_name, [])
                if results:
                    _, last_arts = results[-1]
                    parent_artifacts = dict(last_arts)
                parent_pathspecs = [
                    "%s/%s/%s/%s" % (flow_name, self.run_id, step_name, task_id)
                ]
                continue
            else:
                break

        # Mark non-taken branches as executed (skip entire sub-DAGs)
        self._mark_skipped_branches(flow_name, step_name, node, taken_branch)

    def _mark_skipped_branches(self, flow_name, switch_step, node, taken_branch):
        """Mark non-taken switch branches (and their descendants up to the merge) as executed."""
        for branch_name in node.out_funcs:
            if branch_name != taken_branch and branch_name != switch_step:
                # Mark this branch and all its unique descendants as executed
                self._mark_descendant_steps(flow_name, branch_name, taken_branch)

    def _mark_descendant_steps(self, flow_name, start_name, stop_at=None):
        """Mark start_name and all unique descendants as executed (skipped).
        Stops at steps already executed or at stop_at."""
        queue = [start_name]
        visited = set()
        while queue:
            name = queue.pop(0)
            if name in visited or name in self._executed_steps:
                continue
            if stop_at and name == stop_at:
                continue
            visited.add(name)
            self._executed_steps.add(name)
            self.metadata.new_step(flow_name, self.run_id, name)
            # Follow descendants
            if name in self.graph:
                cn = self.graph[name]
                for out in cn.out_funcs:
                    if out not in visited:
                        queue.append(out)

    def _clone_recursive_switch_tasks(self, flow_name, step_name,
                                       parent_artifacts, parent_pathspecs):
        """Clone completed tasks from a recursive switch step in the origin run.
        Returns updated (parent_artifacts, parent_pathspecs) for continuing execution."""
        origin_tasks = self.metadata.get_task_ids(
            flow_name, self.origin_run_id, step_name
        )
        if not origin_tasks:
            return parent_artifacts, parent_pathspecs

        if step_name not in self._step_results:
            self._step_results[step_name] = []

        for orig_task_id in origin_tasks:
            # Check if this task completed successfully by loading artifacts
            artifacts = self.datastore.load_artifacts(
                flow_name, self.origin_run_id, step_name, orig_task_id
            )
            if not artifacts.get("_task_ok", False):
                # This task failed — stop cloning here, re-execute from this point
                break

            task_id = self._next_task_id()
            self.metadata.new_task(flow_name, self.run_id, step_name, task_id)

            # Copy original metadata
            orig_metadata = self.metadata.get_task_metadata(
                flow_name, self.origin_run_id, step_name, orig_task_id
            )
            if orig_metadata:
                self.metadata.register_metadata(
                    flow_name, self.run_id, step_name, task_id, orig_metadata
                )

            # Add resume metadata
            import platform
            self.metadata.register_metadata(
                flow_name, self.run_id, step_name, task_id,
                [
                    {"type": "origin-task-id", "value": str(orig_task_id)},
                    {"type": "origin-run-id", "value": self.origin_run_id},
                    {"type": "python_version", "value": platform.python_version()},
                ]
            )

            self.datastore.save_artifacts(
                flow_name, self.run_id, step_name, task_id, artifacts
            )
            for stream in ("stdout", "stderr"):
                log = self.datastore.load_log(
                    flow_name, self.origin_run_id, step_name, orig_task_id, stream
                )
                self.datastore.save_log(
                    flow_name, self.run_id, step_name, task_id, stream, log
                )

            self.metadata.done_task(flow_name, self.run_id, step_name, task_id)
            self._step_results[step_name].append((task_id, artifacts))

            # Update parent artifacts to the last cloned task's state
            parent_artifacts = dict(artifacts)
            parent_pathspecs = [
                "%s/%s/%s/%s" % (flow_name, self.run_id, step_name, task_id)
            ]

        return parent_artifacts, parent_pathspecs

    def _execute_join_step(self, flow_name, step_name, node, configs):
        """Execute a join step collecting inputs from parent steps."""
        task_id = self._next_task_id()

        inputs_list = []
        parent_pathspecs = []
        for parent_name in node.in_funcs:
            if parent_name in self._step_results:
                for tid, arts in self._step_results[parent_name]:
                    inputs_list.append(
                        _InputProxy(arts, step_name=parent_name)
                    )
                    parent_pathspecs.append(
                        "%s/%s/%s/%s" % (flow_name, self.run_id, parent_name, tid)
                    )

        inputs = _InputsProxy(inputs_list)

        self._execute_task(
            flow_name, step_name, task_id, node, configs,
            inputs=inputs,
            parent_task_pathspecs=parent_pathspecs,
        )

    def _execute_task(self, flow_name, step_name, task_id, node, configs,
                      parent_artifacts=None, inputs=None,
                      foreach_input=None, foreach_index=None,
                      foreach_stack=None,
                      parallel_index=None, parallel_total=None,
                      parent_task_pathspecs=None):
        """Execute a single task."""
        self.metadata.new_task(flow_name, self.run_id, step_name, task_id)

        flow = self._create_flow_instance(configs)

        # Load parent artifacts
        if parent_artifacts:
            flow.load_parent_state(parent_artifacts)

        # Set foreach context
        if foreach_input is not None:
            flow.set_input_context(foreach_input, foreach_index)
        elif flow._foreach_stack:
            # Reconstruct input/index from the foreach stack (for nested foreach)
            last_frame = flow._foreach_stack[-1]
            flow.set_input_context(last_frame.value, last_frame.index)
        # Set parallel context (input = node_index for parallel steps)
        if parallel_index is not None and foreach_input is None:
            flow.set_input_context(parallel_index, parallel_index)
        if foreach_stack:
            flow.set_foreach_context(flow._input, flow._index, foreach_stack)

        # Set current step on flow
        flow._current_step = step_name

        # Set parameters, configs, and class vars on the flow
        flow.bind_params(self._resolve_params_once(configs))
        flow.bind_configs(configs, self.flow_cls)
        flow.bind_class_vars()

        # Update current singleton via TaskContext
        from .metaflow_current import TaskContext
        username = os.environ.get("METAFLOW_USER", os.environ.get("USER", "unknown"))
        param_names = frozenset(
            obj.name for attr_name in dir(self.flow_cls)
            for obj in [getattr(self.flow_cls, attr_name, None)]
            if isinstance(obj, Parameter)
        )
        current.bind(TaskContext(
            flow_name=flow_name,
            run_id=self.run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=0,
            origin_run_id=self.origin_run_id,
            namespace="user:%s" % username,
            username=username,
            parameter_names=param_names,
            user_tags=tuple(self.tags),
            sys_tags=tuple(self.sys_tags),
            parallel_num_nodes=parallel_total if parallel_index is not None else 1,
            parallel_node_index=parallel_index if parallel_index is not None else 0,
            project_name=getattr(self.flow_cls, "_project_name", None),
            branch_name=getattr(self.flow_cls, "_branch_name", None),
            project_flow_name=getattr(self.flow_cls, "_project_flow_name", None),
            is_production=getattr(self.flow_cls, "_is_production", False),
            graph=self.graph,
        ))

        # Get decorators
        func = getattr(self.flow_cls, step_name, None)
        decorators = list(getattr(func, "_decorators", []))

        for deco in decorators:
            self._resolve_decorator_args(deco, configs)

        # Find retry count
        max_retries = 0
        for deco in decorators:
            if isinstance(deco, RetryDecorator):
                max_retries = deco.attributes.get("times", 3)

        # Metadata (non-attempt metadata registered once)
        metadata_list = []
        if self.origin_run_id:
            metadata_list.append({"type": "origin-run-id", "value": self.origin_run_id})
        if foreach_stack:
            metadata_list.append({
                "type": "foreach-indices",
                "value": json.dumps([(f.step, f.var, f.index) for f in foreach_stack]),
            })
        if parent_task_pathspecs:
            metadata_list.append({
                "type": "parent-task-ids",
                "value": json.dumps(parent_task_pathspecs),
            })
        if parallel_index is not None:
            metadata_list.append({
                "type": "parallel-node-index",
                "value": str(parallel_index),
            })
            metadata_list.append({
                "type": "parallel-num-nodes",
                "value": str(parallel_total),
            })
            if parallel_index == 0:
                metadata_list.append({
                    "type": "internal_task_type",
                    "value": "control",
                })
        if metadata_list:
            self.metadata.register_metadata(
                flow_name, self.run_id, step_name, task_id, metadata_list
            )

        # Fork-based execution: each attempt runs in a child process so that
        # SIGKILL / SIGSEGV in step code doesn't kill the orchestrator.
        import pickle as _pickle
        import signal as _signal
        import tempfile as _tempfile

        success = False
        is_join = (node.type == "join" or len(node.in_funcs) > 1) and inputs is not None

        # Save base artifacts for retry cleanup
        base_artifacts = dict(flow._artifacts)
        last_stdout = ""
        last_stderr = ""
        killed_by_signal = None
        taken_branch = None

        for attempt in range(max_retries + 1):
            current.bind_retry(attempt)

            # Register attempt metadata
            self.metadata.register_metadata(
                flow_name, self.run_id, step_name, task_id,
                [{"type": "attempt", "value": str(attempt)}]
            )

            if attempt > 0:
                flow.reset_for_retry(base_artifacts)
            current.card._reset()

            # Create temp files for child→parent IPC
            _res_fd, _res_path = _tempfile.mkstemp(prefix='mf_res_')
            _out_fd, _out_path = _tempfile.mkstemp(prefix='mf_out_')
            _err_fd, _err_path = _tempfile.mkstemp(prefix='mf_err_')
            os.close(_res_fd)
            os.close(_out_fd)
            os.close(_err_fd)

            pid = os.fork()

            if pid == 0:
                # ── CHILD PROCESS ──
                _co = None
                _ce = None
                try:
                    _co = open(_out_path, 'w', buffering=1)
                    _ce = open(_err_path, 'w', buffering=1)
                    sys.stdout = _co
                    sys.stderr = _ce

                    # Pre-step hooks
                    for deco in decorators:
                        deco.task_pre_step(
                            step_name, None, self.metadata,
                            self.run_id, task_id, flow, self.graph,
                            attempt, max_retries,
                        )

                    # Build callable
                    raw_func = getattr(self.flow_cls, step_name)
                    if is_join:
                        def _mkjc(fn, fl, inp):
                            def c():
                                return fn(fl, inp)
                            return c
                        step_callable = _mkjc(raw_func, flow, inputs)
                    else:
                        def _mkc(fn, fl):
                            def c():
                                return fn(fl)
                            return c
                        step_callable = _mkc(raw_func, flow)

                    # Apply decorator wrapping (timeout, etc.)
                    wrapped = step_callable
                    for deco in decorators:
                        nf = deco.task_decorate(
                            wrapped, flow, self.graph, attempt, max_retries
                        )
                        if nf is not None:
                            wrapped = nf

                    wrapped()

                    # Post-step hooks
                    for deco in reversed(decorators):
                        deco.task_post_step(
                            step_name, flow, self.graph, attempt, max_retries
                        )

                    # Save artifacts to datastore (child writes directly).
                    # Clear stale artifacts from previous attempts first.
                    self.datastore.clear_task_artifacts(
                        flow_name, self.run_id, step_name, task_id
                    )
                    _arts = flow.get_persistable_state(task_ok=True)
                    self.datastore.save_artifacts(
                        flow_name, self.run_id, step_name, task_id, _arts
                    )

                    # Compute switch routing in child via Transition
                    _tb = None
                    _transition = flow.get_transition()
                    if _transition and _transition.condition_var:
                        _cv = flow._artifacts.get(_transition.condition_var)
                        if _cv is not None:
                            _tb = _transition.resolve_switch_target(_cv)

                    with open(_res_path, 'wb') as _rf:
                        _pickle.dump(
                            TaskResult(success=True, taken_branch=_tb),
                            _rf,
                        )

                    _co.flush()
                    _ce.flush()
                    os._exit(0)

                except Exception as _e:
                    try:
                        self.datastore.clear_task_artifacts(
                            flow_name, self.run_id, step_name, task_id
                        )
                        _ea = flow.get_persistable_state(task_ok=False)
                        self.datastore.save_artifacts(
                            flow_name, self.run_id, step_name, task_id, _ea
                        )
                    except Exception:
                        pass
                    try:
                        _e.__traceback__ = None
                        with open(_res_path, 'wb') as _rf:
                            _pickle.dump(
                                TaskResult(success=False, exception=_e),
                                _rf,
                            )
                    except Exception:
                        try:
                            with open(_res_path, 'wb') as _rf:
                                _pickle.dump(
                                    TaskResult(
                                        success=False,
                                        exception_message=str(_e),
                                        exception_type=(
                                            type(_e).__module__ + '.'
                                            + type(_e).__qualname__
                                        ),
                                    ),
                                    _rf,
                                )
                        except Exception:
                            pass
                    try:
                        if _co:
                            _co.flush()
                        if _ce:
                            _ce.flush()
                    except Exception:
                        pass
                    os._exit(1)
                except BaseException:
                    os._exit(2)
            else:
                # ── PARENT PROCESS ──
                _, status = os.waitpid(pid, 0)

                # Read stdout / stderr from temp files
                try:
                    with open(_out_path, 'r') as _f:
                        last_stdout = _f.read()
                except Exception:
                    last_stdout = ""
                try:
                    with open(_err_path, 'r') as _f:
                        last_stderr = _f.read()
                except Exception:
                    last_stderr = ""
                for _p in (_out_path, _err_path):
                    try:
                        os.unlink(_p)
                    except OSError:
                        pass

                # --- Child killed by signal ---
                if os.WIFSIGNALED(status):
                    sig = os.WTERMSIG(status)
                    killed_by_signal = sig
                    if sig == _signal.SIGSEGV:
                        last_stderr += (
                            "\nStep failure could be a segmentation fault.\n"
                        )
                    try:
                        os.unlink(_res_path)
                    except OSError:
                        pass
                    if attempt < max_retries:
                        continue
                    break  # all retries exhausted

                # --- Normal exit ---
                exit_code = (
                    os.WEXITSTATUS(status) if os.WIFEXITED(status) else -1
                )
                result = None
                try:
                    with open(_res_path, 'rb') as _f:
                        result = _pickle.load(_f)
                except Exception:
                    pass
                try:
                    os.unlink(_res_path)
                except OSError:
                    pass

                _task_result = result if isinstance(result, TaskResult) else None

                if exit_code == 0 and _task_result and _task_result.success:
                    # ── SUCCESS ──
                    saved = self.datastore.load_artifacts(
                        flow_name, self.run_id, step_name, task_id
                    )
                    flow._artifacts = {}
                    flow.load_parent_state(saved)
                    taken_branch = _task_result.taken_branch
                    killed_by_signal = None
                    success = True
                    break

                elif exit_code == 1:
                    # ── EXCEPTION in child ──
                    exc = None
                    if _task_result:
                        exc = _task_result.exception
                        if exc is None and _task_result.exception_message:
                            exc = RuntimeError(_task_result.exception_message)
                    if exc is None:
                        exc = RuntimeError(
                            "Unknown error in step '%s'" % step_name
                        )

                    if attempt < max_retries:
                        flow.reset_for_retry(base_artifacts)
                        continue

                    # Final attempt — load artifacts the child saved
                    try:
                        saved = self.datastore.load_artifacts(
                            flow_name, self.run_id, step_name, task_id
                        )
                        flow._artifacts = {}
                        flow.load_parent_state(saved)
                    except Exception:
                        pass

                    # Call exception handlers
                    suppressed = False
                    for deco in reversed(decorators):
                        if deco.task_exception(
                            exc, step_name, flow, self.graph,
                            attempt, max_retries,
                        ):
                            suppressed = True
                            break

                    if suppressed:
                        success = True
                        break
                    else:
                        flow.set_exception(exc)
                        self._save_task_results(
                            flow_name, step_name, task_id, flow,
                            last_stdout, last_stderr, success=False,
                        )
                        raise exc

                else:
                    # Unknown exit code — treat like signal kill
                    killed_by_signal = killed_by_signal or -1
                    if attempt < max_retries:
                        continue
                    break

        # ── Signal-kill fallback after all retries ──
        if not success and killed_by_signal is not None:
            has_catch = any(
                isinstance(d, CatchDecorator) for d in decorators
            )
            is_parallel_non_control = (
                parallel_index is not None and parallel_index != 0
            )

            if has_catch and not is_parallel_non_control:
                # Control task (or non-parallel): create fallback attempt
                fallback = max_retries + 1
                current.bind_retry(fallback)
                self.metadata.register_metadata(
                    flow_name, self.run_id, step_name, task_id,
                    [{"type": "attempt", "value": str(fallback)}],
                )

                from .plugins.catch_decorator import FailureHandledByCatch

                exc = FailureHandledByCatch(
                    "Step '%s' failed due to signal %d."
                    % (step_name, killed_by_signal)
                )
                suppressed = False
                for deco in reversed(decorators):
                    if deco.task_exception(
                        exc, step_name, flow, self.graph,
                        fallback, max_retries,
                    ):
                        suppressed = True
                        break

                if suppressed:
                    success = True
                else:
                    flow.set_exception(exc)
                    self._save_task_results(
                        flow_name, step_name, task_id, flow,
                        last_stdout, last_stderr, success=False,
                    )
                    raise exc

            elif is_parallel_non_control:
                # Non-control parallel task: save as failed, don't raise
                # (the control task handles the failure via @catch)
                self._save_task_results(
                    flow_name, step_name, task_id, flow,
                    last_stdout, last_stderr, success=False,
                )
                return None

            else:
                # No @catch — hard failure
                from .exception import MetaflowException as _MFE

                if killed_by_signal == _signal.SIGSEGV:
                    msg = ("Step '%s' failed with a segmentation fault."
                           % step_name)
                else:
                    msg = ("Step '%s' was killed by signal %d."
                           % (step_name, killed_by_signal))
                exc = _MFE(msg)
                flow.set_exception(exc)
                self._save_task_results(
                    flow_name, step_name, task_id, flow,
                    last_stdout, last_stderr, success=False,
                )
                raise exc

        # Save final results
        self._save_task_results(
            flow_name, step_name, task_id, flow,
            last_stdout, last_stderr, success=success,
        )

        # Return switch routing info
        if taken_branch:
            return {"taken_branch": taken_branch}
        return None

    def _resolve_params_once(self, configs):
        """Resolve parameter values once and cache them."""
        if self._resolved_params is not None:
            return self._resolved_params

        self._resolved_params = {}
        for attr_name in dir(self.flow_cls):
            obj = getattr(self.flow_cls, attr_name, None)
            if isinstance(obj, Parameter):
                env_val = obj._load_from_env()
                if env_val is not None:
                    val = obj._coerce_value(env_val)
                else:
                    default = obj.default
                    if isinstance(default, _ConfigExpr):
                        val = self._eval_config_expr(default.expr, configs)
                    elif isinstance(default, _DeferredConfigAttr):
                        val = self._eval_config_expr(default._expr, configs)
                    elif callable(default):
                        ctx = _ParameterContext(
                            parameter_name=obj.name,
                            flow_name=self.flow_cls.__name__,
                            user_name=os.environ.get("METAFLOW_USER",
                                                     os.environ.get("USER", "unknown")),
                            configs=_ConfigNamespace(configs),
                        )
                        val = default(ctx)
                    else:
                        val = default
                    val = obj._coerce_value(val)

                self._resolved_params[attr_name] = val

            elif isinstance(obj, IncludeFile):
                env_val = obj._load_from_env()
                if env_val:
                    content = obj._load_file(env_val)
                elif obj.default:
                    content = obj._load_file(obj.default)
                else:
                    content = None
                self._resolved_params[attr_name] = content

        return self._resolved_params

    def _set_params_on_flow(self, flow, configs):
        """Set parameter values on the flow instance."""
        resolved = self._resolve_params_once(configs)
        for attr_name, val in resolved.items():
            flow._artifacts[attr_name] = val
            flow._immutable_attrs.add(attr_name)

    def _create_control_task(self, flow_name, step_name, parent_artifacts,
                              parent_pathspec, num_splits):
        """Create a synthetic control task for unbounded foreach.
        The control task has parallel-node-index=0 metadata."""
        task_id = self._next_task_id()
        self.metadata.new_task(flow_name, self.run_id, step_name, task_id)

        # Register control task metadata
        self.metadata.register_metadata(
            flow_name, self.run_id, step_name, task_id,
            [
                {"type": "attempt", "value": "0"},
                {"type": "parallel-node-index", "value": "0"},
                {"type": "parallel-num-nodes", "value": str((num_splits or 0) + 1)},
            ]
        )

        # Save minimal artifacts (parent artifacts + task_ok)
        artifacts = dict(parent_artifacts)
        artifacts["_task_ok"] = True
        self.datastore.save_artifacts(
            flow_name, self.run_id, step_name, task_id, artifacts
        )
        self.datastore.save_log(
            flow_name, self.run_id, step_name, task_id, "stdout", ""
        )
        self.datastore.save_log(
            flow_name, self.run_id, step_name, task_id, "stderr", ""
        )
        self.metadata.done_task(flow_name, self.run_id, step_name, task_id)

        # Add to step results
        if step_name not in self._step_results:
            self._step_results[step_name] = []
        self._step_results[step_name].append((task_id, artifacts))

    def _save_task_results(self, flow_name, step_name, task_id, flow,
                           stdout, stderr, success=True):
        """Save artifacts and logs for a completed task."""
        artifacts = flow.get_persistable_state(task_ok=success)

        self.datastore.save_artifacts(
            flow_name, self.run_id, step_name, task_id, artifacts
        )
        self.datastore.save_log(
            flow_name, self.run_id, step_name, task_id, "stdout", stdout
        )
        self.datastore.save_log(
            flow_name, self.run_id, step_name, task_id, "stderr", stderr
        )

        self.metadata.done_task(flow_name, self.run_id, step_name, task_id)

        if step_name not in self._step_results:
            self._step_results[step_name] = []
        self._step_results[step_name].append((task_id, artifacts))

    def _find_reusable_steps(self, topo_order, resume_step):
        """Find steps that can be reused from the origin run."""
        reusable = set()
        found_resume = False
        for node in topo_order:
            if node.name == resume_step:
                found_resume = True
            if not found_resume:
                reusable.add(node.name)
        return reusable

    def _reuse_step(self, flow_name, step_name):
        """Reuse tasks from origin run for a step."""
        origin_tasks = self.metadata.get_task_ids(
            flow_name, self.origin_run_id, step_name
        )

        self._step_results[step_name] = []

        for orig_task_id in origin_tasks:
            task_id = self._next_task_id()

            artifacts = self.datastore.load_artifacts(
                flow_name, self.origin_run_id, step_name, orig_task_id
            )

            self.metadata.new_task(flow_name, self.run_id, step_name, task_id)

            # Copy original metadata
            orig_metadata = self.metadata.get_task_metadata(
                flow_name, self.origin_run_id, step_name, orig_task_id
            )
            if orig_metadata:
                self.metadata.register_metadata(
                    flow_name, self.run_id, step_name, task_id, orig_metadata
                )

            # Register resume-specific metadata
            import platform
            self.metadata.register_metadata(
                flow_name, self.run_id, step_name, task_id,
                [
                    {"type": "origin-task-id", "value": str(orig_task_id)},
                    {"type": "origin-run-id", "value": self.origin_run_id},
                    {"type": "python_version", "value": platform.python_version()},
                ]
            )

            self.datastore.save_artifacts(
                flow_name, self.run_id, step_name, task_id, artifacts
            )

            for stream in ("stdout", "stderr"):
                log = self.datastore.load_log(
                    flow_name, self.origin_run_id, step_name, orig_task_id, stream
                )
                self.datastore.save_log(
                    flow_name, self.run_id, step_name, task_id, stream, log
                )

            self.metadata.done_task(flow_name, self.run_id, step_name, task_id)
            self._step_results[step_name].append((task_id, artifacts))


class _ParameterContext:
    def __init__(self, parameter_name=None, flow_name=None,
                 user_name=None, configs=None):
        self.parameter_name = parameter_name
        self.flow_name = flow_name
        self.user_name = user_name
        self.configs = configs


class _ConfigContext:
    def __init__(self, resolved_configs):
        self._configs = resolved_configs

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._configs:
            return self._configs[name]
        raise AttributeError("No config '%s'" % name)


class _ConfigNamespace:
    def __init__(self, configs):
        self._configs = configs

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._configs:
            return self._configs[name]
        raise AttributeError("No config '%s'" % name)
