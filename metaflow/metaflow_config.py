"""Metaflow configuration module.

Loads configuration values from metaflow_extensions packages."""

from ._extension_loader import load_config_extensions

# Load extension config values into this module's namespace
_ext_config = load_config_extensions()
globals().update(_ext_config)
