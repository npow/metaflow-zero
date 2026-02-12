"""Catch decorator support — FailureHandledByCatch exception."""

from ..exception import MetaflowException


class FailureHandledByCatch(MetaflowException):
    """Exception used when a step was killed by a signal (SIGKILL, SIGSEGV, etc.)
    and the @catch decorator handles the failure."""

    headline = "Failed task handled by catch decorator"

    def __init__(self, msg=""):
        super().__init__(msg)
