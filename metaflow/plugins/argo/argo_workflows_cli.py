"""Argo Workflows CLI utilities."""

import re


def sanitize_for_argo(name):
    """Sanitize a name for use in Argo Workflows (RFC 1123 subdomain).

    Rules applied per DNS label (split on '.'):
    - Remove characters that are not alphanumeric or '-'
    - Strip leading '-' from each label
    - Strip trailing '-' from non-last labels only (last part keeps trailing
      hyphens since a hash will be appended)
    - Collapse consecutive '.' into single '.'
    - Remove empty labels
    """
    parts = name.split(".")
    sanitized_parts = []
    for part in parts:
        # Remove characters that are not alphanumeric or hyphen
        part = re.sub(r"[^a-zA-Z0-9-]", "", part)
        # Strip leading hyphens
        part = part.lstrip("-")
        if part:
            sanitized_parts.append(part)

    # Strip trailing hyphens from all parts except the last
    for i in range(len(sanitized_parts) - 1):
        sanitized_parts[i] = sanitized_parts[i].rstrip("-")

    # Filter out any parts that became empty after rstrip
    sanitized_parts = [p for p in sanitized_parts if p]

    return ".".join(sanitized_parts)
