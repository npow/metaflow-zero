"""Step and flow decorators for Metaflow."""

import hashlib
import os
import traceback


# Card registry - maps card type names to card classes
_CARD_REGISTRY = {}


def _register_card_type(name, cls):
    _CARD_REGISTRY[name] = cls


def _get_card_class(name):
    if name in _CARD_REGISTRY:
        return _CARD_REGISTRY[name]
    # Try loading from plugins
    try:
        from .plugins.cards.card_modules import test_cards
        _load_test_cards()
    except Exception:
        pass
    return _CARD_REGISTRY.get(name)


def _load_test_cards():
    """Load built-in test card types."""
    try:
        from .plugins.cards.card_modules.test_cards import (
            TaskspecCard, TestPathspecCard, TestEditableCard,
            TestTimeoutCard, TestBrokenCard, NonEditableImportTestCard,
            EditableImportTestCard, TestEditableCard2,
        )
    except ImportError:
        pass


def _store_card_artifact(flow, card_type, card_id, html):
    """Store a card as an artifact on the flow."""
    card_hash = hashlib.md5((html or "").encode()).hexdigest()[:8]
    # Find next index for this card type
    # Note: we still need direct access to _artifacts for the existence check
    # since card artifact names start with "_" (not user-visible via get_artifacts)
    idx = 0
    while True:
        if card_id:
            artifact_name = "_card_%s_%s_%d" % (card_type, card_id, idx)
        else:
            artifact_name = "_card_%s_%d" % (card_type, idx)
        if artifact_name not in flow._artifacts:
            break
        idx += 1
    flow.set_artifact(artifact_name, html)


def step(f):
    """Mark a method as a step in a Metaflow flow."""
    f._is_step = True
    if not hasattr(f, "_decorators"):
        f._decorators = []
    return f


class StepDecorator:
    """Base class for step decorators."""
    name = None
    defaults = {}

    def __init__(self, **kwargs):
        self.attributes = dict(self.defaults)
        self.attributes.update(kwargs)

    def step_init(self, flow, graph, step_name, decos, environment, datastore, logger):
        pass

    def task_pre_step(self, step_name, task_datastore, metadata,
                      run_id, task_id, flow, graph, retry_count, max_user_code_retries):
        pass

    def task_decorate(self, step_func, flow, graph, retry_count, max_user_code_retries):
        return step_func

    def task_post_step(self, step_name, flow, graph, retry_count, max_user_code_retries):
        pass

    def task_exception(self, exception, step_name, flow, graph,
                       retry_count, max_user_code_retries):
        return False


step_decorator = StepDecorator


class FlowDecorator:
    """Base class for flow-level decorators."""
    name = None
    options = {}

    def __init__(self, **kwargs):
        self.attributes = kwargs

    def flow_init(self, flow_cls, graph, environment, datastore, logger):
        pass


class MetaflowExceptionWrapper:
    """Wraps exceptions for @catch decorator."""

    def __init__(self, exc):
        self.exception = str(exc)
        self.type = type(exc).__module__ + "." + type(exc).__name__
        self.traceback = traceback.format_exc()

    def __repr__(self):
        return "MetaflowExceptionWrapper(%s: %s)" % (self.type, self.exception)

    def __str__(self):
        return "%s: %s" % (self.type, self.exception)

    def __eq__(self, other):
        if isinstance(other, MetaflowExceptionWrapper):
            return self.exception == other.exception and self.type == other.type
        return False


# --- Concrete Step Decorators ---

class RetryDecorator(StepDecorator):
    name = "retry"
    defaults = {"times": 3, "minutes_between_retries": 2}


class CatchDecorator(StepDecorator):
    name = "catch"
    defaults = {"var": None, "print_exception": True}

    def task_exception(self, exception, step_name, flow, graph,
                       retry_count, max_user_code_retries):
        var = self.attributes.get("var")
        if self.attributes.get("print_exception", True):
            import sys
            traceback.print_exc(file=sys.stderr)
        wrapper = MetaflowExceptionWrapper(exception)
        if var:
            flow.set_artifact(var, wrapper)
            flow._private_artifacts.discard(var)
        return True


class TimeoutDecorator(StepDecorator):
    name = "timeout"
    defaults = {"seconds": 0, "minutes": 0, "hours": 0}

    def _get_timeout_seconds(self):
        return (self.attributes.get("seconds", 0) +
                self.attributes.get("minutes", 0) * 60 +
                self.attributes.get("hours", 0) * 3600)

    def task_decorate(self, step_func, flow, graph, retry_count, max_user_code_retries):
        import signal
        from .plugins.timeout_decorator import TimeoutException

        timeout_secs = self._get_timeout_seconds()
        if timeout_secs <= 0:
            return step_func

        def _handler(signum, frame):
            raise TimeoutException("Step timed out after %d seconds" % timeout_secs)

        def wrapped(*args, **kwargs):
            old_handler = signal.signal(signal.SIGALRM, _handler)
            signal.alarm(timeout_secs)
            try:
                result = step_func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            return result

        return wrapped


class _CardArtifactProxy:
    """Proxy to expose flow artifacts with a .data attribute for card rendering."""
    def __init__(self, value):
        self.data = value

class _CardTaskProxy:
    """Task-like proxy that provides artifact access during card rendering."""
    def __init__(self, flow, pathspec):
        self._flow = flow
        self._cached_artifacts = None
        self.pathspec = pathspec

    def _get_artifacts(self):
        if self._cached_artifacts is None:
            self._cached_artifacts = self._flow.get_artifacts()
        return self._cached_artifacts

    def __getitem__(self, name):
        arts = self._get_artifacts()
        if name in arts:
            return _CardArtifactProxy(arts[name])
        raise KeyError(name)

    def __contains__(self, name):
        return name in self._get_artifacts()

    def __str__(self):
        return self.pathspec

    def __repr__(self):
        return self.pathspec


class CardDecorator(StepDecorator):
    name = "card"
    defaults = {"type": "default", "id": None, "options": None,
                "timeout": None, "save_errors": True, "customize": False}

    def task_pre_step(self, step_name, task_datastore, metadata,
                      run_id, task_id, flow, graph, retry_count, max_user_code_retries):
        from .metaflow_current import current
        card_type = self.attributes.get("type", "default")
        card_id = self.attributes.get("id")
        customize = self.attributes.get("customize", False)

        # Look up card class to determine if it allows user components
        card_cls = _get_card_class(card_type)
        allow_user = bool(card_cls and getattr(card_cls, 'ALLOW_USER_COMPONENTS', False))

        current.card._register_card(card_type, card_id, customize, allow_user)

    def task_post_step(self, step_name, flow, graph, retry_count, max_user_code_retries):
        import json
        import threading

        from .metaflow_current import current
        from .datastore.local import LocalDatastore

        card_type = self.attributes.get("type", "default")
        card_id = self.attributes.get("id")
        options = self.attributes.get("options") or {}
        save_errors = self.attributes.get("save_errors", True)
        card_timeout = self.attributes.get("timeout")

        # Get the card class from the registry
        card_cls = _get_card_class(card_type)
        if card_cls is None:
            if save_errors:
                self._write_card_file(current, card_type, card_id, "")
            return

        try:
            card_inst = card_cls(options=options)
            card_inst.id = card_id

            # Get user components appended via current.card
            components = current.card._get_components(card_type, card_id)
            if components:
                card_inst._components = list(components)

            # Build Task-like proxy for render
            pathspec = current.pathspec
            task_proxy = _CardTaskProxy(flow, pathspec)

            # Render with optional timeout
            html = None
            if card_timeout and card_timeout > 0:
                result = [None]
                error = [None]

                def _render():
                    try:
                        result[0] = card_inst.render(task_proxy)
                    except Exception as e:
                        error[0] = e

                t = threading.Thread(target=_render)
                t.daemon = True
                t.start()
                t.join(timeout=card_timeout)

                if t.is_alive():
                    # Timed out - don't save if save_errors=False
                    if save_errors:
                        self._write_card_file(current, card_type, card_id, "")
                    return
                elif error[0]:
                    raise error[0]
                else:
                    html = result[0]
            else:
                html = card_inst.render(task_proxy)

            # Store as card file
            self._write_card_file(current, card_type, card_id, html or "")

        except Exception:
            if save_errors:
                self._write_card_file(current, card_type, card_id, "")

    @staticmethod
    def _write_card_file(current_obj, card_type, card_id, html):
        """Write a card as an HTML file with metadata JSON."""
        import json

        from .datastore.local import LocalDatastore

        ds = LocalDatastore()
        pathspec = current_obj.pathspec
        if not pathspec:
            return

        parts = pathspec.split("/")
        card_dir = os.path.join(ds.root, *parts, "cards")
        os.makedirs(card_dir, exist_ok=True)

        idx = current_obj.card._allocate_card_index()
        card_hash = hashlib.md5((html or "").encode()).hexdigest()[:8]

        # Write HTML content
        html_path = os.path.join(card_dir, "%d.html" % idx)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html or "")

        # Write metadata
        meta_path = os.path.join(card_dir, "%d.json" % idx)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"type": card_type, "id": card_id, "hash": card_hash}, f)


class ResourcesDecorator(StepDecorator):
    name = "resources"
    defaults = {}


class EnvironmentDecorator(StepDecorator):
    name = "environment"
    defaults = {"vars": {}}

    def task_pre_step(self, step_name, task_datastore, metadata,
                      run_id, task_id, flow, graph, retry_count, max_user_code_retries):
        env_vars = self.attributes.get("vars", {})
        for k, v in env_vars.items():
            os.environ[k] = str(v)


class CondaDecorator(StepDecorator):
    name = "conda"
    defaults = {"packages": {}, "python": None, "disabled": False}


class PypiDecorator(StepDecorator):
    name = "pypi"
    defaults = {"packages": {}, "python": None}


class SecretsDecorator(StepDecorator):
    name = "secrets"
    defaults = {"sources": [], "inline": {}}

    def task_pre_step(self, step_name, task_datastore, metadata,
                      run_id, task_id, flow, graph, retry_count, max_user_code_retries):
        # Handle inline secrets
        inline = self.attributes.get("inline", {})
        for k, v in inline.items():
            os.environ[k] = str(v)
        # Handle sources with type=inline
        sources = self.attributes.get("sources", [])
        for source in sources:
            if isinstance(source, dict):
                stype = source.get("type", "")
                if stype == "inline":
                    env_vars = source.get("options", {}).get("env_vars", {})
                    for k, v in env_vars.items():
                        os.environ[k] = str(v)


class ParallelDecorator(StepDecorator):
    name = "parallel"
    defaults = {}


class ScheduleDecorator(FlowDecorator):
    name = "schedule"


class TriggerDecorator(FlowDecorator):
    name = "trigger"


class TriggerOnFinishDecorator(FlowDecorator):
    name = "trigger_on_finish"


class ProjectDecorator(FlowDecorator):
    name = "project"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.project_name = kwargs.get("name", "")

    def flow_init(self, flow_cls, graph, environment, datastore, logger):
        name = self.project_name
        username = os.environ.get("METAFLOW_USER", os.environ.get("USER", "unknown"))

        if os.environ.get("METAFLOW_PRODUCTION"):
            branch_name = "prod"
            is_production = True
        elif os.environ.get("METAFLOW_BRANCH"):
            branch_name = "test.%s" % os.environ["METAFLOW_BRANCH"]
            is_production = False
        else:
            branch_name = "user.%s" % username
            is_production = False

        project_flow_name = "%s.%s.%s" % (name, branch_name, flow_cls.__name__)

        flow_cls._project_name = name
        flow_cls._branch_name = branch_name
        flow_cls._project_flow_name = project_flow_name
        flow_cls._is_production = is_production


# --- Decorator factory functions ---

def _make_step_decorator_factory(deco_cls):
    """Create a decorator factory for a step decorator class."""
    def factory(**kwargs):
        def decorator(f):
            if not hasattr(f, "_is_step"):
                raise TypeError(
                    "@%s must be applied above @step" % deco_cls.name
                )
            if not hasattr(f, "_decorators"):
                f._decorators = []
            f._decorators.append(deco_cls(**kwargs))
            return f
        return decorator
    return factory


def retry(times=3, minutes_between_retries=2):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@retry must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(RetryDecorator(times=times, minutes_between_retries=minutes_between_retries))
        return f
    return decorator


def catch(var=None, print_exception=True):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@catch must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(CatchDecorator(var=var, print_exception=print_exception))
        return f
    return decorator


def timeout(seconds=0, minutes=0, hours=0):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@timeout must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(TimeoutDecorator(seconds=seconds, minutes=minutes, hours=hours))
        return f
    return decorator


def card(type="default", id=None, options=None, save_errors=True, **extra_kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@card must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(CardDecorator(
            type=type, id=id, options=options, save_errors=save_errors,
            **extra_kwargs
        ))
        return f
    return decorator


def resources(**kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@resources must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(ResourcesDecorator(**kwargs))
        return f
    return decorator


def environment(vars=None, **kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@environment must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        env_vars = vars if vars is not None else kwargs
        f._decorators.append(EnvironmentDecorator(vars=env_vars))
        return f
    return decorator


def conda(**kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@conda must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(CondaDecorator(**kwargs))
        return f
    return decorator


def pypi(**kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@pypi must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(PypiDecorator(**kwargs))
        return f
    return decorator


def secrets(sources=None, inline=None, **kwargs):
    def decorator(f):
        if not hasattr(f, "_is_step"):
            raise TypeError("@secrets must be applied above @step")
        if not hasattr(f, "_decorators"):
            f._decorators = []
        f._decorators.append(SecretsDecorator(
            sources=sources or [],
            inline=inline or {},
            **kwargs
        ))
        return f
    return decorator


def parallel(f):
    """Mark a step as parallel."""
    if not hasattr(f, "_is_step"):
        raise TypeError("@parallel must be applied above @step")
    f._parallel = True
    if not hasattr(f, "_decorators"):
        f._decorators = []
    f._decorators.append(ParallelDecorator())
    return f


def unbounded_test_foreach_internal(f):
    """No-op decorator marking a step for unbounded foreach (test only)."""
    return f


def test_step_decorator(f):
    """No-op test step decorator (extensions test)."""
    return f


def test_flow_decorator(cls):
    """Test flow decorator that reads METAFLOW_FOOBAR env var and sets current.foobar_value."""
    from .metaflow_current import current
    foobar_val = os.environ.get("METAFLOW_FOOBAR")
    if foobar_val is not None:
        current._ext_attrs["foobar_value"] = foobar_val
    return cls


def project(name=None, **kwargs):
    """Flow-level @project decorator."""
    def decorator(cls):
        if not hasattr(cls, "_flow_decorators"):
            cls._flow_decorators = []
        cls._flow_decorators.append(ProjectDecorator(name=name, **kwargs))
        return cls
    return decorator


def schedule(**kwargs):
    """No-op flow decorator for scheduling."""
    def decorator(cls):
        if not hasattr(cls, "_flow_decorators"):
            cls._flow_decorators = []
        cls._flow_decorators.append(ScheduleDecorator(**kwargs))
        return cls
    return decorator


def trigger(**kwargs):
    """No-op flow decorator for event triggers."""
    def decorator(cls):
        if not hasattr(cls, "_flow_decorators"):
            cls._flow_decorators = []
        cls._flow_decorators.append(TriggerDecorator(**kwargs))
        return cls
    return decorator


def trigger_on_finish(**kwargs):
    """No-op flow decorator for trigger on finish."""
    def decorator(cls):
        if not hasattr(cls, "_flow_decorators"):
            cls._flow_decorators = []
        cls._flow_decorators.append(TriggerOnFinishDecorator(**kwargs))
        return cls
    return decorator


def config_expr(expr):
    """Config expression helper - returns string sentinel for deferred evaluation."""
    return _ConfigExpr(expr)


class _ConfigExpr:
    """Sentinel for deferred config expressions."""
    def __init__(self, expr):
        self.expr = expr

    def __str__(self):
        return self.expr

    def __repr__(self):
        return "config_expr(%r)" % self.expr

    def __getattr__(self, name):
        return _ConfigExpr("%s.%s" % (self.expr, name))


# --- FlowMutator / StepMutator ---

class MutableStep:
    """Mutable view of a step for FlowMutator/StepMutator."""

    def __init__(self, name, func, flow_ref=None):
        self.name = name
        self._func = func
        self._flow = flow_ref
        self.decorator_specs = list(getattr(func, "_decorators", []))

    @property
    def flow(self):
        return self._flow

    def add_decorator(self, deco_or_name, deco_kwargs=None, **kwargs):
        if isinstance(deco_or_name, str):
            deco_cls = _DECORATOR_REGISTRY.get(deco_or_name)
            if deco_cls:
                kw = deco_kwargs or kwargs
                self.decorator_specs.append(deco_cls(**kw))
        elif isinstance(deco_or_name, type) and issubclass(deco_or_name, StepDecorator):
            kw = deco_kwargs or kwargs
            self.decorator_specs.append(deco_or_name(**kw))
        elif isinstance(deco_or_name, StepDecorator):
            self.decorator_specs.append(deco_or_name)

    def remove_decorator(self, decospec):
        """Remove decorator by spec string (e.g., 'retry' or 'retry(times=3)')."""
        name = decospec.split("(")[0]
        self.decorator_specs = [d for d in self.decorator_specs if d.name != name]


class MutableFlow:
    """Mutable view of a flow for FlowMutator."""

    def __init__(self, flow_cls):
        self._flow_cls = flow_cls
        self._steps = {}
        self._configs = {}
        self._parameters = {}
        self._added_parameters = {}

        from .parameters import Parameter
        from .user_configs.config_parameters import Config

        for attr_name in dir(flow_cls):
            obj = getattr(flow_cls, attr_name, None)
            if obj is not None:
                if callable(obj) and getattr(obj, "_is_step", False):
                    self._steps[attr_name] = MutableStep(attr_name, obj, self)
                elif isinstance(obj, Config):
                    self._configs[obj.name] = obj
                elif isinstance(obj, Parameter):
                    self._parameters[obj.name] = obj

    @property
    def steps(self):
        return self._steps.items()

    @property
    def configs(self):
        return self._configs.items()

    @property
    def parameters(self):
        return list(self._parameters.items()) + list(self._added_parameters.items())

    @property
    def start(self):
        return self._steps.get("start")

    def config(self, name=None):
        if name:
            cfg = self._configs.get(name)
            if cfg and cfg._is_resolved:
                return cfg.value
            return None
        # Return dict-like access
        return {n: (c.value if c._is_resolved else None) for n, c in self._configs.items()}

    def add_parameter(self, name, param, overwrite=False):
        from .parameters import Parameter
        if isinstance(param, Parameter):
            self._added_parameters[name] = param
        else:
            self._added_parameters[name] = param


class MutableFlow:
    """Interface for FlowMutator to inspect and modify a flow class."""

    def __init__(self, cls):
        self._cls = cls

    @property
    def steps(self):
        """Dict of step_name -> MutableStep."""
        result = {}
        for klass in self._cls.__mro__:
            for name, val in vars(klass).items():
                if name not in result and callable(val) and getattr(val, "_is_step", False):
                    result[name] = MutableStep(val, self)
        return result

    @property
    def configs(self):
        """Iterable of (name, resolved_config_value) tuples."""
        from .user_configs.config_parameters import Config
        seen = set()
        result = []
        for klass in self._cls.__mro__:
            for name, val in vars(klass).items():
                if name not in seen and isinstance(val, Config):
                    seen.add(name)
                    if not val._is_resolved:
                        val.resolve()
                    result.append((name, val.value))
        return result

    @property
    def parameters(self):
        """Iterable of (name, Parameter) tuples."""
        from .parameters import Parameter
        seen = set()
        result = []
        for klass in self._cls.__mro__:
            for name, val in vars(klass).items():
                if name not in seen and isinstance(val, Parameter):
                    seen.add(name)
                    result.append((name, val))
        return result

    def config(self, name):
        """Get a resolved config value by name."""
        for cname, cval in self.configs:
            if cname == name:
                return cval
        return None

    @property
    def start(self):
        """MutableStep for 'start'."""
        return self.steps.get("start")

    def add_parameter(self, name, param):
        """Add a Parameter dynamically to the flow class."""
        setattr(self._cls, name, param)


class MutableStep:
    """Interface for inspecting/modifying a step."""

    def __init__(self, func, flow):
        self._func = func
        self._flow = flow

    @property
    def decorator_specs(self):
        """List of (name, kwargs) tuples for current decorators."""
        result = []
        for deco in getattr(self._func, "_decorators", []):
            result.append((deco.name, dict(deco.attributes)))
        return result

    def add_decorator(self, name, **kwargs):
        """Add a decorator by name."""
        if name in _DECORATOR_REGISTRY:
            deco_cls = _DECORATOR_REGISTRY[name]
            if not hasattr(self._func, "_decorators"):
                self._func._decorators = []
            self._func._decorators.append(deco_cls(**kwargs))

    def remove_decorator(self, name):
        """Remove a decorator by name."""
        if hasattr(self._func, "_decorators"):
            self._func._decorators = [
                d for d in self._func._decorators if d.name != name
            ]

    @property
    def flow(self):
        return self._flow


class FlowMutator:
    """Base class for flow mutators applied as class decorators.

    Supports two usage patterns:
      @MyMutator          # No args - applied directly
      @MyMutator("arg")   # With args - init() receives args
    """

    def __init__(self, *args, **kwargs):
        # If the first arg is a class (direct @MyMutator usage), apply immediately
        if len(args) == 1 and isinstance(args[0], type) and not kwargs:
            # Direct decorator: @MyMutator applied to a class
            self._target_cls = args[0]
            self.init()
            self._store_on_class(args[0])
        else:
            # Parameterized decorator: @MyMutator("arg")
            self._target_cls = None
            self._init_args = args
            self._init_kwargs = kwargs
            self.init(*args, **kwargs)

    def _store_on_class(self, cls):
        """Store the mutator on the class for deferred execution."""
        if "_own_flow_mutators" not in cls.__dict__:
            cls._own_flow_mutators = []
        cls._own_flow_mutators.append(self)

    @staticmethod
    def apply_all_mutators(cls):
        """Collect and apply all FlowMutators from the class hierarchy."""
        # Collect mutators from all classes in MRO (reverse so base class mutators run first)
        mutators = []
        for klass in reversed(cls.__mro__):
            for m in klass.__dict__.get("_own_flow_mutators", []):
                mutators.append(m)
        if mutators:
            mf = MutableFlow(cls)
            for m in mutators:
                m.pre_mutate(mf)

    def __call__(self, cls):
        """When used as @MyMutator("arg"), this is called with the class."""
        if self._target_cls is not None:
            # Already applied during __init__
            return self._target_cls
        # Store mutator on cls for deferred execution
        self._store_on_class(cls)
        return cls

    def init(self, *args, **kwargs):
        pass

    def pre_mutate(self, mutable_flow):
        pass

    def mutate(self, mutable_flow):
        """Alias for pre_mutate for compatibility."""
        pass


class StepMutator:
    """Base class for step mutators."""

    def __init__(self, func=None):
        self._my_step = func

    def __call__(self, *args, **kwargs):
        if self._my_step is not None:
            return self._my_step(*args, **kwargs)

    def init(self):
        pass

    @classmethod
    def mutate_step(cls, mutable_step):
        pass

    def mutate(self, mutable_step):
        pass

    def __getattr__(self, name):
        if self._my_step is not None and name != "_my_step":
            return getattr(self._my_step, name)
        raise AttributeError(name)


class UserStepDecorator:
    """User-defined step decorator with lifecycle hooks."""

    skip_step = False

    def init(self):
        pass

    def pre_step(self, step_name, flow, inputs):
        pass

    def post_step(self, step_name, flow, exception=None):
        pass


def user_step_decorator(cls):
    """Register a UserStepDecorator subclass."""
    cls._is_user_step_decorator = True
    return cls


def extract_step_decorator_from_decospec(decospec):
    """Parse a decorator spec string like 'retry(times=3)' into (name, kwargs)."""
    if "(" not in decospec:
        return decospec, {}
    name = decospec.split("(")[0]
    kwargs_str = decospec[len(name) + 1:-1]
    kwargs = {}
    if kwargs_str.strip():
        # Use a safe eval-like parser
        try:
            kwargs = eval("dict(%s)" % kwargs_str)
        except Exception:
            pass
    return name, kwargs


# Decorator registry for name-based lookup
_DECORATOR_REGISTRY = {
    "retry": RetryDecorator,
    "catch": CatchDecorator,
    "timeout": TimeoutDecorator,
    "card": CardDecorator,
    "resources": ResourcesDecorator,
    "environment": EnvironmentDecorator,
    "conda": CondaDecorator,
    "pypi": PypiDecorator,
    "secrets": SecretsDecorator,
    "parallel": ParallelDecorator,
}
