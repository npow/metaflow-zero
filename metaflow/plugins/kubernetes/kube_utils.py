"""Kubernetes utility functions."""

import re

from .kubernetes import KubernetesException

# Kubernetes label value: up to 63 chars, alphanumeric + '-' + '_' + '.',
# must start and end with alphanumeric (or be empty).
_LABEL_VALUE_RE = re.compile(r"^([a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?)?$")


def validate_kube_labels(labels):
    """Validate Kubernetes labels dict. Returns True or raises KubernetesException."""
    if labels is None:
        return True
    for key, value in labels.items():
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            raise KubernetesException(
                "Label value for key '%s' must be a string, got %s" % (key, type(value))
            )
        if not _LABEL_VALUE_RE.match(value):
            raise KubernetesException(
                "Invalid Kubernetes label value for key '%s': '%s'. "
                "Must be <= 63 characters, start and end with alphanumeric, "
                "and contain only alphanumeric, '-', '_', or '.'." % (key, value)
            )
    return True


def parse_kube_keyvalue_list(items, requires_both=True):
    """Parse a list of 'key=value' strings into a dict.

    If requires_both is True, every item must have '='. If False, items without
    '=' get value None. Duplicate keys always raise.
    """
    result = {}
    for item in items:
        if "=" in item:
            key, value = item.split("=", 1)
        else:
            if requires_both:
                raise KubernetesException(
                    "Expected 'key=value' format, got '%s'" % item
                )
            key, value = item, None

        if key in result:
            raise KubernetesException("Duplicate key '%s'" % key)
        result[key] = value
    return result
