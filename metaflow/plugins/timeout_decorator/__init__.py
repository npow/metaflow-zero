"""Timeout decorator exceptions."""
from metaflow.exception import MetaflowException


class TimeoutException(MetaflowException):
    headline = "Step timed out"
