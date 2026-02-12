"""Namespace management for Metaflow."""

from .util import get_username

_current_namespace = None
_default_initialized = False


def _ensure_default():
    global _current_namespace, _default_initialized
    if not _default_initialized:
        _current_namespace = "user:%s" % get_username()
        _default_initialized = True


def namespace(ns):
    """Set the current namespace. None means global namespace."""
    global _current_namespace, _default_initialized
    _default_initialized = True
    _current_namespace = ns
    return ns


def get_namespace():
    """Get the current namespace string or None."""
    _ensure_default()
    return _current_namespace


def default_namespace():
    """Reset namespace to user:<username>."""
    global _current_namespace, _default_initialized
    _default_initialized = True
    _current_namespace = "user:%s" % get_username()
    return _current_namespace
