"""Secrets decorator for Metaflow."""

import re

from metaflow.exception import MetaflowException
import metaflow.metaflow_config


# Valid env var name: starts with letter or underscore, then alphanumeric/underscore.
# Must not start with METAFLOW_.
_ENV_VAR_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Secret backend provider registry
_SECRETS_PROVIDERS = {}


class SecretSpec:
    """Specification for a secret to be fetched."""

    def __init__(self, secrets_backend_type, secret_id, options=None, role=None):
        self.secrets_backend_type = secrets_backend_type
        self.secret_id = secret_id
        self.options = options or {}
        self.role = role

    @staticmethod
    def secret_spec_from_str(secret_str, role):
        """Create SecretSpec from a string like 'type.id' or just 'id'.

        If no '.' is found, uses DEFAULT_SECRETS_BACKEND_TYPE.
        """
        if "." in secret_str:
            backend_type, secret_id = secret_str.split(".", 1)
        else:
            backend_type = getattr(
                metaflow.metaflow_config, "DEFAULT_SECRETS_BACKEND_TYPE", None
            )
            if backend_type is None:
                raise MetaflowException(
                    "No default secrets backend type configured. "
                    "Set DEFAULT_SECRETS_BACKEND_TYPE or use 'type.id' format."
                )
            secret_id = secret_str

        return SecretSpec(
            secrets_backend_type=backend_type,
            secret_id=secret_id,
            role=role,
        )

    @staticmethod
    def secret_spec_from_dict(d, role=None):
        """Create SecretSpec from a dict with keys: type, id, options, role."""
        # Validate 'id'
        secret_id = d.get("id")
        if not isinstance(secret_id, str):
            raise MetaflowException(
                "Secret spec 'id' must be a string, got %s" % type(secret_id).__name__
            )

        # Validate 'type'
        backend_type = d.get("type")
        if backend_type is not None and not isinstance(backend_type, str):
            raise MetaflowException(
                "Secret spec 'type' must be a string, got %s"
                % type(backend_type).__name__
            )
        if backend_type is None:
            backend_type = getattr(
                metaflow.metaflow_config, "DEFAULT_SECRETS_BACKEND_TYPE", None
            )
            if backend_type is None:
                raise MetaflowException(
                    "No default secrets backend type configured."
                )

        # Validate 'options'
        options = d.get("options", {})
        if not isinstance(options, dict):
            raise MetaflowException(
                "Secret spec 'options' must be a dict, got %s"
                % type(options).__name__
            )

        # Validate 'role' - source level wins over decorator level
        source_role = d.get("role")
        if source_role is not None and not isinstance(source_role, str):
            raise MetaflowException(
                "Secret spec 'role' must be a string, got %s"
                % type(source_role).__name__
            )
        effective_role = source_role if source_role is not None else role

        return SecretSpec(
            secrets_backend_type=backend_type,
            secret_id=secret_id,
            options=options,
            role=effective_role,
        )

    def to_json(self):
        return {
            "secrets_backend_type": self.secrets_backend_type,
            "secret_id": self.secret_id,
            "options": self.options,
            "role": self.role,
        }


def validate_env_vars(env_vars):
    """Validate that env_vars is a dict with valid env var names and string values."""
    for k, v in env_vars.items():
        if not isinstance(k, str):
            raise MetaflowException(
                "Environment variable key must be a string, got %s"
                % type(k).__name__
            )
        if not isinstance(v, str):
            raise MetaflowException(
                "Environment variable value for '%s' must be a string, got %s"
                % (k, type(v).__name__)
            )
        if not _ENV_VAR_RE.match(k):
            raise MetaflowException(
                "Invalid environment variable name: '%s'. "
                "Must start with a letter or underscore and contain only "
                "alphanumeric characters and underscores." % k
            )
        if k.startswith("METAFLOW_"):
            raise MetaflowException(
                "Environment variable name '%s' must not start with 'METAFLOW_'." % k
            )


def validate_env_vars_across_secrets(all_secrets_env_vars):
    """Check that no two secrets define the same env var.

    Parameters
    ----------
    all_secrets_env_vars : list of (SecretSpec, dict) tuples
    """
    seen = {}
    for spec, env_vars in all_secrets_env_vars:
        for k in env_vars:
            if k in seen:
                raise MetaflowException(
                    "Environment variable '%s' is defined by multiple secrets: "
                    "'%s' and '%s'"
                    % (k, seen[k].secret_id, spec.secret_id)
                )
            seen[k] = spec


def validate_env_vars_vs_existing_env(all_secrets_env_vars):
    """Check that secrets don't overwrite existing environment variables.

    Parameters
    ----------
    all_secrets_env_vars : list of (SecretSpec, dict) tuples
    """
    import os

    for spec, env_vars in all_secrets_env_vars:
        for k in env_vars:
            if k in os.environ:
                raise MetaflowException(
                    "Secret '%s' defines environment variable '%s' which "
                    "already exists in the environment." % (spec.secret_id, k)
                )


def get_secrets_backend_provider(backend_type):
    """Look up a secrets backend provider by type name.

    Raises MetaflowException if not found.
    """
    if backend_type in _SECRETS_PROVIDERS:
        return _SECRETS_PROVIDERS[backend_type]
    raise MetaflowException(
        "Unknown secrets backend type: '%s'. Available types: %s"
        % (backend_type, list(_SECRETS_PROVIDERS.keys()))
    )
