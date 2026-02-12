"""Metaflow — a framework for real-life data science."""

# Core
from .flowspec import FlowSpec
from .decorators import (
    step,
    retry,
    catch,
    timeout,
    card,
    resources,
    environment,
    conda,
    pypi,
    secrets,
    parallel,
    project,
    schedule,
    trigger,
    trigger_on_finish,
    StepDecorator,
    FlowDecorator,
    FlowMutator,
    StepMutator,
    UserStepDecorator,
    user_step_decorator,
    step_decorator,
    MetaflowExceptionWrapper,
    config_expr,
    unbounded_test_foreach_internal,
    test_step_decorator,
    test_flow_decorator,
)
from .parameters import Parameter, JSONType
from .includefile import IncludeFile
from .metaflow_current import current
from .user_configs.config_parameters import Config, ConfigValue
from .namespace import namespace, get_namespace, default_namespace
from .exception import (
    MetaflowException,
    MetaflowNotFound,
    MetaflowNamespaceMismatch,
    UnhandledInMergeArtifactsException,
    MissingInMergeArtifactsException,
    ExternalCommandFailed,
    InvalidDecoratorAttribute,
    MetaflowInternalError,
    ParameterFieldFailed,
    MetaflowDataMissing,
    InvalidNextException,
)
from .client import Flow, Run, Step, Task, DataArtifact, Metaflow
from .runner import Runner, inspect_spin
from .cards import get_cards


# --- Extension loading ---
try:
    from ._extension_loader import (
        load_toplevel_extensions,
        load_plugin_extensions,
        load_card_extensions,
        resolve_decorator_class,
        promote_submodules,
    )

    # Load toplevel extension values (e.g., tl_value=42) into this module
    _tl_values = load_toplevel_extensions()
    globals().update(_tl_values)

    # Load plugin extensions (decorators, promoted submodules)
    _step_decos, _flow_decos, _promoted, _org_plugin_mods = load_plugin_extensions()

    # Register step decorators from extensions
    _registered_step_decos = {}
    for _name, _import_path in _step_decos:
        for _base_mod, _ in _org_plugin_mods:
            _cls = resolve_decorator_class(_base_mod, _import_path)
            if _cls is not None:
                _registered_step_decos[_name] = _cls
                break

    # Register flow decorators from extensions
    _registered_flow_decos = {}
    for _name, _import_path in _flow_decos:
        for _base_mod, _ in _org_plugin_mods:
            _cls = resolve_decorator_class(_base_mod, _import_path)
            if _cls is not None:
                _registered_flow_decos[_name] = _cls
                break

    # Override no-op decorators with extension implementations
    from . import decorators as _decos_mod
    for _name, _cls in _registered_step_decos.items():
        if hasattr(_decos_mod, _name):
            # Create a decorator function that wraps the step decorator class
            def _make_step_deco_factory(deco_cls):
                def _factory(**kwargs):
                    def _wrapper(f):
                        if not hasattr(f, "_decorators"):
                            f._decorators = []
                        f._decorators.append(deco_cls(**kwargs))
                        return f
                    return _wrapper
                # Also support @test_step_decorator without parens
                def _smart_factory(f=None, **kwargs):
                    if f is not None and callable(f):
                        # Used as @decorator without parens
                        if not hasattr(f, "_decorators"):
                            f._decorators = []
                        f._decorators.append(deco_cls())
                        return f
                    return _factory(**kwargs)
                return _smart_factory
            _new_deco = _make_step_deco_factory(_cls)
            setattr(_decos_mod, _name, _new_deco)
            globals()[_name] = _new_deco

    for _name, _cls in _registered_flow_decos.items():
        if hasattr(_decos_mod, _name):
            # Create a flow decorator factory
            def _make_flow_deco_factory(deco_cls):
                def _factory(cls_or_none=None, **kwargs):
                    if cls_or_none is not None and isinstance(cls_or_none, type):
                        # Used as @decorator without parens on a class
                        inst = deco_cls()
                        if not hasattr(cls_or_none, "_flow_decorators"):
                            cls_or_none._flow_decorators = []
                        cls_or_none._flow_decorators.append(inst)
                        return cls_or_none
                    return lambda cls: _factory(cls, **kwargs)
                return _factory
            _new_deco = _make_flow_deco_factory(_cls)
            setattr(_decos_mod, _name, _new_deco)
            globals()[_name] = _new_deco

    # Promote extension submodules (e.g., nondecoplugin, frameworks)
    promote_submodules(_org_plugin_mods, _promoted)

    # Load card type extensions and register them
    _card_classes = load_card_extensions()
    from .decorators import _register_card_type
    for _card_cls in _card_classes:
        _register_card_type(_card_cls.type, _card_cls)

except Exception:
    pass
