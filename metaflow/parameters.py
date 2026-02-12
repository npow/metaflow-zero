"""Parameter descriptor for FlowSpec classes."""

import json
import os


class _JSONTypeSentinel:
    pass


JSONType = _JSONTypeSentinel


class Parameter:
    """A flow parameter descriptor."""

    def __init__(self, name, default=None, required=False, help=None,
                 type=None, separator=None, show_default=True, **kwargs):
        self.name = name
        self._attr_name = name.replace("-", "_")
        self.default = default
        self.required = required
        self.help = help
        self.type = type
        self.separator = separator
        self.show_default = show_default
        self.kwargs = kwargs
        self._value_set = False

    def _resolve_default(self, ctx=None):
        """Resolve the default value, handling callables and config expressions."""
        val = self.default
        if callable(val) and not isinstance(val, _JSONTypeSentinel):
            if ctx is not None:
                val = val(ctx)
            else:
                val = None
        return val

    def _load_from_env(self):
        """Try to load value from env var METAFLOW_RUN_<UPPER_NAME>."""
        env_key = "METAFLOW_RUN_%s" % self._attr_name.upper()
        return os.environ.get(env_key)

    def _coerce_value(self, val):
        """Coerce string value to the parameter type."""
        if val is None:
            return val

        # Determine effective type
        param_type = self.type
        if param_type is None and self.default is not None:
            # Infer type from default for basic types
            default_type = type(self.default)
            if default_type in (bool, int, float):
                param_type = default_type

        if param_type is JSONType or isinstance(param_type, _JSONTypeSentinel):
            if isinstance(val, str):
                return json.loads(val)
            return val

        if self.separator and isinstance(val, str):
            val = val.split(self.separator)
            return val

        if param_type is not None and param_type is not JSONType:
            # Bool must come before int since bool is a subclass of int
            if param_type == bool:
                if isinstance(val, str):
                    return val.lower() not in ("false", "0", "no", "")
                return bool(val)
            if isinstance(val, str):
                return param_type(val)
            return val

        return val

    def click_option(self):
        """Return kwargs for @click.option."""
        import click

        opts = {}
        opts["default"] = None  # we handle defaults ourselves
        opts["help"] = self.help or ""
        opts["show_default"] = self.show_default
        opts["required"] = False  # we handle required ourselves

        if self.type is JSONType or isinstance(self.type, _JSONTypeSentinel):
            opts["type"] = click.STRING
        elif self.type == bool:
            opts["is_flag"] = True
            opts["default"] = False
        elif self.type == int:
            opts["type"] = click.INT
        elif self.type == float:
            opts["type"] = click.FLOAT
        elif self.type is not None:
            opts["type"] = click.STRING
        else:
            opts["type"] = click.STRING

        return opts
