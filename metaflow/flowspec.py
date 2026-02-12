"""FlowSpec — base class for all Metaflow flows."""

from dataclasses import dataclass
from typing import Optional

from .graph import FlowGraph
from .parameters import Parameter
from .user_configs.config_parameters import Config, ConfigValue
from .includefile import IncludeFile
from .decorators import FlowMutator, StepMutator


@dataclass(frozen=True)
class Transition:
    """What self.next() produces — the step transition specification."""
    targets: tuple                # step method refs or (dict,) for dict-style switch
    foreach_var: str = None       # if foreach split
    condition_var: str = None     # if switch/conditional

    def resolve_switch_target(self, condition_value) -> Optional[str]:
        """Resolve which branch a switch takes.

        Handles direct (value is method ref) and dict-style (targets[0] is dict).
        Returns the step name string.
        """
        cv = condition_value
        if self.targets and len(self.targets) == 1 and isinstance(self.targets[0], dict):
            # Dict-style switch: condition value is a key into a mapping
            target_func = self.targets[0].get(str(cv))
            if target_func is not None:
                if hasattr(target_func, '__func__'):
                    return target_func.__func__.__name__
                elif hasattr(target_func, '__name__'):
                    return target_func.__name__
                else:
                    return str(target_func)
            else:
                return str(cv)
        elif hasattr(cv, '__func__'):
            return cv.__func__.__name__
        elif hasattr(cv, '__name__'):
            return cv.__name__
        elif isinstance(cv, str):
            return cv
        return None


class _FlowSpecMeta(type):
    """Metaclass for FlowSpec that handles class construction."""
    pass


class FlowSpec(metaclass=_FlowSpecMeta):
    """Base class for Metaflow flows."""

    _flow_decorators = []

    def __init__(self, use_cli=True):
        self._artifacts = {}
        self._private_artifacts = set()
        self._params = {}
        self._configs = {}
        self._class_vars = {}
        self._immutable_attrs = set()
        self._input = None
        self._index = None
        self._foreach_stack = []
        self._next_targets = None
        self._foreach_var = None
        self._condition_var = None

        # Apply deferred FlowMutators before building graph/collecting params
        FlowMutator.apply_all_mutators(type(self))

        self._graph = FlowGraph(type(self))

        # Collect parameters, configs, class vars
        for attr_name in dir(type(self)):
            obj = getattr(type(self), attr_name, None)
            if isinstance(obj, Parameter):
                self._params[obj.name] = obj
                self._immutable_attrs.add(obj._attr_name)
            elif isinstance(obj, Config):
                self._configs[obj.name] = obj
                self._immutable_attrs.add(attr_name)
            elif isinstance(obj, IncludeFile):
                self._params[obj.name] = obj
                self._immutable_attrs.add(obj._attr_name)
            elif not callable(obj) and not attr_name.startswith("_") and \
                 attr_name not in ("name",) and \
                 not isinstance(obj, (property, classmethod, staticmethod)):
                # Class-level constants
                if not isinstance(obj, (Parameter, Config, IncludeFile)):
                    self._class_vars[attr_name] = obj

        if use_cli:
            from .cli import create_cli
            create_cli(type(self))

    @property
    def name(self):
        return type(self).__name__

    def __iter__(self):
        """Yield graph nodes."""
        return iter(self._graph)

    @classmethod
    def _create_instance(cls, graph):
        """Create a fresh runtime instance with graph but no params/configs yet."""
        flow = cls.__new__(cls)
        flow._artifacts = {}
        flow._private_artifacts = set()
        flow._params = {}
        flow._configs = {}
        flow._class_vars = {}
        flow._immutable_attrs = set()
        flow._input = None
        flow._index = None
        flow._foreach_stack = []
        flow._next_targets = None
        flow._foreach_var = None
        flow._condition_var = None
        flow._graph = graph

        for attr_name in dir(cls):
            obj = getattr(cls, attr_name, None)
            if isinstance(obj, Parameter):
                flow._params[obj.name] = obj
                flow._immutable_attrs.add(attr_name)
            elif isinstance(obj, Config):
                flow._configs[obj.name] = obj
                flow._immutable_attrs.add(attr_name)
            elif isinstance(obj, IncludeFile):
                flow._params[obj.name] = obj
                flow._immutable_attrs.add(attr_name)

        for attr_name in dir(cls):
            obj = getattr(cls, attr_name, None)
            if (obj is not None and not callable(obj) and
                    not attr_name.startswith("_") and
                    attr_name not in ("name",) and
                    not isinstance(obj, (property, classmethod, staticmethod,
                                        Parameter, Config, IncludeFile))):
                flow._class_vars[attr_name] = obj
                flow._immutable_attrs.add(attr_name)

        return flow

    def load_parent_state(self, parent_artifacts: dict):
        """Load artifacts from parent task. Filters _ prefix, extracts _foreach_stack."""
        from .runtime import _ensure_foreach_frames
        for k, v in parent_artifacts.items():
            if not k.startswith("_"):
                self._artifacts[k] = v
            elif k == "_foreach_stack":
                self._foreach_stack = _ensure_foreach_frames(v)

    def get_persistable_state(self, task_ok: bool) -> dict:
        """Return dict for datastore: user artifacts + _task_ok + _foreach_stack."""
        arts = dict(self._artifacts)
        arts["_task_ok"] = task_ok
        arts["_foreach_stack"] = list(self._foreach_stack)
        return arts

    def get_artifacts(self) -> dict:
        """Return copy of user-visible artifacts (no _ prefix)."""
        return {k: v for k, v in self._artifacts.items() if not k.startswith("_")}

    def set_artifact(self, name, value):
        """Set a single artifact (bypasses immutability — for Runtime/decorator use)."""
        self._artifacts[name] = value

    def set_foreach_context(self, input_value, input_index, foreach_stack):
        """Set foreach execution context."""
        from .runtime import _ensure_foreach_frames
        self._input = input_value
        self._index = input_index
        self._foreach_stack = _ensure_foreach_frames(foreach_stack)

    def set_input_context(self, value, index):
        """Set input/index for parallel steps (no foreach stack modification)."""
        self._input = value
        self._index = index

    def get_transition(self):
        """Return Transition from last self.next() call, or None if end step."""
        if self._next_targets is None:
            return None
        return Transition(
            targets=self._next_targets,
            foreach_var=self._foreach_var,
            condition_var=self._condition_var,
        )

    def reset_for_retry(self, base_artifacts: dict):
        """Reset artifacts to pre-execution state for retry."""
        self._artifacts = dict(base_artifacts)

    def bind_params(self, resolved_params: dict):
        """Set parameter values as immutable artifacts."""
        for attr_name, val in resolved_params.items():
            self._artifacts[attr_name] = val
            self._immutable_attrs.add(attr_name)

    def bind_configs(self, configs: dict, flow_cls):
        """Set config values as immutable artifacts."""
        for attr_name in dir(flow_cls):
            obj = getattr(flow_cls, attr_name, None)
            if isinstance(obj, Config) and obj._is_resolved:
                self._artifacts[attr_name] = obj.value

    def bind_class_vars(self):
        """Set class-level constants as immutable artifacts."""
        for name, val in self._class_vars.items():
            if name not in self._artifacts:
                self._artifacts[name] = val

    def set_exception(self, exc):
        """Store MetaflowExceptionWrapper on the flow."""
        from .decorators import MetaflowExceptionWrapper
        self._artifacts["_exception"] = MetaflowExceptionWrapper(exc)

    def next(self, *targets, foreach=None, condition=None, num_parallel=None):
        """Record transition to next step(s)."""
        # This is used at runtime by the runtime engine
        if foreach:
            self._next_targets = targets
            self._foreach_var = foreach
        elif condition:
            self._next_targets = targets
            self._condition_var = condition
        elif num_parallel:
            self._next_targets = targets
        else:
            self._next_targets = targets

    def merge_artifacts(self, inputs, exclude=None, include=None):
        """Merge artifacts from branch/foreach inputs."""
        from .exception import (
            MetaflowException,
            UnhandledInMergeArtifactsException,
            MissingInMergeArtifactsException,
        )

        # Validate: merge_artifacts only valid in join steps
        step_name = getattr(self, "_current_step", None)
        if step_name and hasattr(self, "_graph") and self._graph:
            node = self._graph[step_name]
            if node.type != "join" and len(node.in_funcs) <= 1:
                raise MetaflowException(
                    "merge_artifacts can only be called in join steps"
                )

        if exclude is not None and include is not None:
            raise MetaflowException(
                "Cannot specify both 'exclude' and 'include' in merge_artifacts"
            )

        exclude = set(exclude) if exclude else set()
        include_set = set(include) if include else None

        # Collect all artifacts from all inputs
        all_artifacts = {}
        for inp in inputs:
            inp_arts = inp.get_artifacts() if hasattr(inp, 'get_artifacts') else inp._artifacts
            for name, val in inp_arts.items():
                if name.startswith("_"):
                    continue
                if name in self._immutable_attrs:
                    continue
                if name in exclude:
                    continue
                if include_set is not None and name not in include_set:
                    continue
                if name not in all_artifacts:
                    all_artifacts[name] = []
                all_artifacts[name].append(val)

        # Check for include referencing non-existent artifacts
        if include_set:
            all_available = set()
            for inp in inputs:
                inp_arts = inp.get_artifacts() if hasattr(inp, 'get_artifacts') else inp._artifacts
                for name in inp_arts:
                    if not name.startswith("_"):
                        all_available.add(name)
            missing = include_set - all_available - set(self._artifacts.keys()) - self._immutable_attrs
            if missing:
                raise MissingInMergeArtifactsException(
                    "The following artifacts were specified in 'include' "
                    "but do not exist: %s" % ", ".join(sorted(missing))
                )

        # Check for conflicts — collect to-set artifacts first, only apply if no conflicts
        unhandled = []
        to_set = {}
        for name, values in all_artifacts.items():
            # Skip if already set in current step
            if name in self._artifacts:
                continue
            unique_vals = []
            for v in values:
                is_dup = False
                for uv in unique_vals:
                    try:
                        if v == uv:
                            is_dup = True
                            break
                    except Exception:
                        if v is uv:
                            is_dup = True
                            break
                if not is_dup:
                    unique_vals.append(v)

            if len(unique_vals) > 1:
                unhandled.append(name)
            elif len(unique_vals) == 1:
                to_set[name] = unique_vals[0]

        if unhandled:
            raise UnhandledInMergeArtifactsException(
                "Unhandled artifacts in merge: %s. Use 'include' or 'exclude', "
                "or set them explicitly before calling merge_artifacts."
                % ", ".join(sorted(unhandled)),
                unhandled=sorted(unhandled),
            )

        # No conflicts — apply all merged artifacts
        for name, val in to_set.items():
            self._artifacts[name] = val

    @property
    def input(self):
        return self._input

    @property
    def index(self):
        return self._index

    def foreach_stack(self):
        """Return foreach nesting hierarchy as (index, num_splits, value) tuples."""
        return [(frame.index, frame.num_splits, frame.value) for frame in self._foreach_stack]

    def __getattribute__(self, name):
        # For private/internal attributes, use normal lookup
        if name.startswith("_") or name in (
            "name", "input", "index", "next", "merge_artifacts",
            "foreach_stack",
            # Public interface methods
            "load_parent_state", "get_persistable_state", "get_artifacts",
            "set_artifact", "set_foreach_context", "set_input_context",
            "get_transition", "reset_for_retry", "bind_params",
            "bind_configs", "bind_class_vars", "set_exception",
        ):
            return object.__getattribute__(self, name)

        # Check _artifacts first for user-defined artifacts, params, configs
        try:
            artifacts = object.__getattribute__(self, "_artifacts")
            if name in artifacts:
                return artifacts[name]
        except AttributeError:
            pass

        # Fall back to normal lookup (for methods like step functions)
        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return

        # Check immutability for params and class vars
        if hasattr(self, "_immutable_attrs") and name in self._immutable_attrs:
            if name in self._artifacts:
                raise AttributeError(
                    "Cannot modify parameter/config '%s'" % name
                )

        # Check class var immutability
        if hasattr(self, "_class_vars") and name in self._class_vars:
            if name in self._artifacts:
                raise AttributeError(
                    "Cannot modify class variable '%s'" % name
                )

        if hasattr(self, "_artifacts"):
            self._artifacts[name] = value
        else:
            object.__setattr__(self, name, value)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        # Check artifacts first
        if "_artifacts" in self.__dict__ and name in self.__dict__["_artifacts"]:
            return self.__dict__["_artifacts"][name]

        raise AttributeError(
            "'%s' object has no attribute '%s'" % (type(self).__name__, name)
        )

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        if hasattr(self, "_artifacts") and name in self._artifacts:
            del self._artifacts[name]
        else:
            object.__delattr__(self, name)
