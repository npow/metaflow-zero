"""AWS utility functions for Metaflow."""

from metaflow.exception import MetaflowException


class AWSException(MetaflowException):
    headline = "AWS Error"


def validate_aws_tag(key, value):
    """Validate an AWS tag key/value pair.

    Rules:
    - Key max 128 chars, value max 256 chars
    - Neither key nor value may start with 'aws:' (case-insensitive)
    """
    if len(key) > 128:
        raise AWSException(
            "AWS tag key exceeds 128 characters: '%s'" % key[:50]
        )
    if len(value) > 256:
        raise AWSException(
            "AWS tag value exceeds 256 characters for key '%s'" % key
        )
    if key.lower().startswith("aws:"):
        raise AWSException(
            "AWS tag key must not start with 'aws:': '%s'" % key
        )
    if value.lower().startswith("aws:"):
        raise AWSException(
            "AWS tag value must not start with 'aws:' for key '%s'" % key
        )


def compute_resource_attributes(decos, current, defaults):
    """Compute effective resource attributes from decorators and defaults.

    Parameters
    ----------
    decos : list
        Other decorators (e.g., @resources) that may set resource attributes.
    current : namedtuple/object
        The current decorator (e.g., @batch/@kubernetes) with .name and .attributes.
    defaults : dict
        Default resource values. Keys with None values are optional string attributes.

    Returns
    -------
    dict
        Merged resource attributes.
    """
    result = {}

    # Find any @resources decorator in the list
    resources_deco = None
    for d in decos:
        if d.name == "resources":
            resources_deco = d
            break

    for attr_name, default_val in defaults.items():
        current_val = current.attributes.get(attr_name)
        resources_val = resources_deco.attributes.get(attr_name) if resources_deco else None

        # Determine if value is numeric (can be compared numerically)
        is_numeric = default_val is not None and _is_numeric(default_val)

        if is_numeric:
            # Numeric attribute: take largest of resources vs current, fall back to default
            if resources_val is not None and current_val is not None:
                # Both set: take the larger
                r_float = float(resources_val)
                c_float = float(current_val)
                result[attr_name] = str(max(r_float, c_float))
            elif current_val is not None:
                result[attr_name] = str(int(current_val) if isinstance(current_val, int) else current_val)
            elif resources_val is not None:
                result[attr_name] = str(resources_val)
            else:
                result[attr_name] = str(default_val)
        else:
            # String attribute: current wins over default, None means "not set"
            if current_val is not None:
                result[attr_name] = current_val
            elif default_val is not None:
                result[attr_name] = default_val
            # If both are None, skip this attribute entirely

    return result


def _is_numeric(val):
    """Check if a value can be treated as numeric."""
    try:
        float(val)
        return True
    except (TypeError, ValueError):
        return False
