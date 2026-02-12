"""Click API introspection utilities."""

import click


class MetaflowAPI:
    """Introspection class for Metaflow CLI."""
    pass


# Map click type CLASSES to Python types
# click.STRING is an instance of click.types.StringParamType, etc.
click_to_python_types = {
    type(click.STRING): str,
    type(click.INT): int,
    type(click.FLOAT): float,
    type(click.BOOL): bool,
    click.types.StringParamType: str,
    click.types.IntParamType: int,
    click.types.FloatParamType: float,
    click.types.BoolParamType: bool,
}

# Also add the instance types if they're separate classes
try:
    click_to_python_types[click.types.IntRange] = int
    click_to_python_types[click.types.FloatRange] = float
except AttributeError:
    pass

try:
    click_to_python_types[click.types.Path] = str
    click_to_python_types[click.types.Choice] = str
except AttributeError:
    pass

try:
    click_to_python_types[click.types.Tuple] = tuple
except AttributeError:
    pass

try:
    click_to_python_types[click.types.FuncParamType] = str
except AttributeError:
    pass

try:
    click_to_python_types[click.types.UNPROCESSED.__class__] = str
except AttributeError:
    pass


def extract_all_params(command):
    """Extract parameters from a click command.

    Returns (names, types, param_opts, required, defaults)
    """
    names = []
    types = []
    param_opts = {}
    required = []
    defaults = {}

    if hasattr(command, "params"):
        params = command.params
    elif hasattr(command, "callback") and hasattr(command.callback, "__click_params__"):
        params = command.callback.__click_params__
    else:
        params = []

    for param in params:
        name = param.name
        if name is None:
            continue
        names.append(name)
        types.append(type(param.type))
        param_opts[name] = param
        required.append(param.required)
        defaults[name] = param.default

    return names, types, param_opts, required, defaults
