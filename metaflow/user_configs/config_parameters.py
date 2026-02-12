"""Config and ConfigValue for Metaflow."""

import json


class ConfigValue(dict):
    """Immutable dict subclass with attribute access. Wraps nested dicts."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                "'ConfigValue' object has no attribute '%s'" % name
            )

    def __setattr__(self, name, value):
        raise TypeError("ConfigValue is immutable")

    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return _wrap(val)

    def __setitem__(self, key, value):
        raise TypeError("ConfigValue is immutable")

    def __delitem__(self, key):
        raise TypeError("ConfigValue is immutable")

    def __reduce__(self):
        """Support pickling by using raw dict data."""
        # Use dict.items to avoid __getitem__ wrapping
        return (ConfigValue, ({k: v for k, v in dict.items(self)},))

    def pop(self, *args):
        raise TypeError("ConfigValue is immutable")

    def popitem(self):
        raise TypeError("ConfigValue is immutable")

    def clear(self):
        raise TypeError("ConfigValue is immutable")

    def update(self, *args, **kwargs):
        raise TypeError("ConfigValue is immutable")

    def setdefault(self, *args):
        raise TypeError("ConfigValue is immutable")

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def values(self):
        return [_wrap(v) for v in dict.values(self)]

    def items(self):
        return [(k, _wrap(v)) for k, v in dict.items(self)]

    def to_dict(self):
        """Recursively unwrap to plain dicts."""
        result = {}
        for k, v in dict.items(self):
            result[k] = _unwrap(v)
        return result

    def __iter__(self):
        return dict.__iter__(self)

    def __len__(self):
        return dict.__len__(self)

    def __contains__(self, key):
        return dict.__contains__(self, key)

    def keys(self):
        return dict.keys(self)

    def __repr__(self):
        return "ConfigValue(%s)" % dict.__repr__(self)


def _wrap(val):
    """Wrap dicts in ConfigValue, and dicts inside lists/tuples."""
    if isinstance(val, ConfigValue):
        return val
    if isinstance(val, dict):
        return ConfigValue(val)
    if isinstance(val, list):
        return [_wrap(v) for v in val]
    if isinstance(val, tuple):
        return tuple(_wrap(v) for v in val)
    return val


def _unwrap(val):
    """Recursively unwrap ConfigValue to plain dict."""
    if isinstance(val, ConfigValue):
        return val.to_dict()
    if isinstance(val, dict):
        return {k: _unwrap(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_unwrap(v) for v in val]
    if isinstance(val, tuple):
        return tuple(_unwrap(v) for v in val)
    return val


class _DeferredConfigAttr:
    """Deferred config attribute access for use at class definition time."""
    def __init__(self, config_name, attr_chain):
        self._config_name = config_name
        self._attr_chain = attr_chain

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _DeferredConfigAttr(self._config_name, self._attr_chain + [name])

    @property
    def _expr(self):
        return "%s.%s" % (self._config_name, ".".join(self._attr_chain))

    def __str__(self):
        return self._expr

    def __repr__(self):
        return "_DeferredConfigAttr(%s)" % self._expr


class Config:
    """Config descriptor for FlowSpec classes."""

    def __init__(self, name, default=None, default_value=None,
                 required=False, parser=None, help=None, plain=False):
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, '_attr_name', name.replace("-", "_"))
        object.__setattr__(self, 'default', default)
        object.__setattr__(self, 'default_value', default_value)
        object.__setattr__(self, 'required', required)
        object.__setattr__(self, 'parser', parser)
        object.__setattr__(self, 'help', help)
        object.__setattr__(self, 'plain', plain)
        object.__setattr__(self, '_resolved_value', None)
        object.__setattr__(self, '_is_resolved', False)

    def __getattr__(self, name):
        """Support deferred attribute access at class definition time."""
        if name.startswith("_"):
            raise AttributeError(name)
        return _DeferredConfigAttr(self.name, [name])

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)

    def resolve(self, value=None):
        """Resolve config to final value."""
        if value is not None:
            result = value
        elif self.default_value is not None:
            if callable(self.default_value):
                result = self.default_value
            else:
                result = self.default_value
        elif self.default is not None:
            # default is a file path
            try:
                with open(self.default, "r") as f:
                    result = f.read()
                if self.parser:
                    result = self.parser(result)
                else:
                    result = json.loads(result)
            except Exception:
                result = self.default
        else:
            if self.required:
                from ..exception import MetaflowException
                raise MetaflowException(
                    "Config '%s' is required but not provided" % self.name
                )
            result = None

        if isinstance(result, str) and self.parser is None and not self.plain:
            try:
                result = json.loads(result)
            except (json.JSONDecodeError, TypeError):
                pass

        if self.parser and not callable(self.default_value):
            if isinstance(result, str):
                result = self.parser(result)

        if not self.plain and isinstance(result, dict):
            result = ConfigValue(result)

        self._resolved_value = result
        self._is_resolved = True
        return result

    @property
    def value(self):
        if not self._is_resolved:
            raise RuntimeError("Config '%s' not yet resolved" % self.name)
        return self._resolved_value
