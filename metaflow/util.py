"""Metaflow utility functions."""

import os


def is_stringish(x):
    """Return True if x is str or bytes."""
    return isinstance(x, (str, bytes))


def get_username():
    """Get username from METAFLOW_USER or USER env var."""
    return os.environ.get("METAFLOW_USER", os.environ.get("USER", "unknown"))


def to_unicode(x):
    """Convert bytes to str."""
    if isinstance(x, bytes):
        return x.decode("utf-8")
    return x


def to_bytes(x):
    """Convert str to bytes."""
    if isinstance(x, str):
        return x.encode("utf-8")
    return x


unicode_type = str
