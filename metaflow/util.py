"""Metaflow utility functions."""

import io
import os
from urllib.parse import quote as _url_quote_str


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


def url_quote(url):
    """URL-quote a string or bytes, returning bytes."""
    if isinstance(url, bytes):
        return _url_quote_str(url.decode("utf-8", errors="replace"), safe="/:@").encode(
            "utf-8"
        )
    return _url_quote_str(url, safe="/:@").encode("utf-8")


def to_fileobj(x):
    """Convert str/bytes to a file-like object."""
    if isinstance(x, str):
        return io.BytesIO(x.encode("utf-8"))
    if isinstance(x, bytes):
        return io.BytesIO(x)
    return x


unicode_type = str
