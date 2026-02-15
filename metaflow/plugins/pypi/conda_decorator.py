"""Conda step decorator for Metaflow."""


class CondaStepDecorator:
    """Tracks conda environment attributes for a step.

    Supports 'libraries' as a backward-compatible alias for 'packages'.
    """

    name = "conda"
    defaults = {
        "packages": {},
        "python": None,
        "libraries": {},
    }

    def __init__(self, attributes=None):
        self._user_defined = set()
        self.attributes = dict(self.defaults)
        if attributes:
            for k, v in attributes.items():
                self.attributes[k] = v
                self._user_defined.add(k)

    def init(self):
        # Backward compat: if 'libraries' was user-defined, treat 'packages' as user-defined too
        if "libraries" in self._user_defined:
            self._user_defined.add("packages")

    def is_attribute_user_defined(self, attr_name):
        return attr_name in self._user_defined
