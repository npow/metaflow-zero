"""Metaflow configuration module.

Loads configuration values from metaflow_extensions packages."""

import os

from ._extension_loader import load_config_extensions

# Default configuration values
DEFAULT_METADATA = os.environ.get("METAFLOW_DEFAULT_METADATA", "local")
DEFAULT_DATASTORE = os.environ.get("METAFLOW_DEFAULT_DATASTORE", "local")
DATASTORE_SYSROOT_S3 = os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3")
DEFAULT_SECRETS_BACKEND_TYPE = os.environ.get("METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE")
SERVICE_URL = os.environ.get("METAFLOW_SERVICE_URL")
S3_ENDPOINT_URL = os.environ.get("METAFLOW_S3_ENDPOINT_URL")

# Load extension config values into this module's namespace
_ext_config = load_config_extensions()
globals().update(_ext_config)
