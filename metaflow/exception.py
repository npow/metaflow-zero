"""Metaflow exception classes."""


def _reconstruct_metaflow_exception(cls, args, state):
    """Reconstruct a MetaflowException subclass without calling __init__.

    This avoids pickling failures when subclasses override __init__ with
    different signatures (e.g., TestRetry.__init__(self) with no args).
    """
    obj = Exception.__new__(cls)
    obj.args = args
    obj.__dict__.update(state)
    return obj


class MetaflowException(Exception):
    headline = "Flow Exception"

    def __init__(self, msg="", lineno=None):
        self.message = msg
        self.lineno = lineno
        super().__init__(msg)

    def __reduce__(self):
        return (_reconstruct_metaflow_exception,
                (type(self), self.args, self.__dict__.copy()))


class MetaflowNotFound(MetaflowException):
    headline = "Not Found"


class MetaflowNamespaceMismatch(MetaflowException):
    headline = "Namespace Mismatch"


class UnhandledInMergeArtifactsException(MetaflowException):
    headline = "Unhandled Artifacts in Merge"

    def __init__(self, msg="", unhandled=None):
        self.artifact_names = unhandled or []
        super().__init__(msg)


class MissingInMergeArtifactsException(MetaflowException):
    headline = "Missing Artifacts in Merge"


class ExternalCommandFailed(MetaflowException):
    headline = "External Command Failed"


class InvalidDecoratorAttribute(MetaflowException):
    headline = "Invalid Decorator Attribute"


class MetaflowInternalError(MetaflowException):
    headline = "Internal Error"


class ParameterFieldFailed(MetaflowException):
    headline = "Parameter Field Failed"


class MetaflowDataMissing(MetaflowException):
    headline = "Data Missing"


class InvalidNextException(MetaflowException):
    headline = "Invalid self.next() Transition"


# Load exception extensions into this module's namespace
try:
    from ._extension_loader import load_exception_extensions
    _ext_exceptions = load_exception_extensions()
    globals().update(_ext_exceptions)
except Exception:
    pass
