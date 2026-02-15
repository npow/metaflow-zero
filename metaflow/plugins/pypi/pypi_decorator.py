"""PyPI step decorator for Metaflow."""


class PyPIStepDecorator:
    """Tracks PyPI environment attributes for a step."""

    name = "pypi"
    defaults = {
        "packages": {},
        "python": None,
    }

    def __init__(self, attributes=None):
        self._user_defined = set()
        self.attributes = dict(self.defaults)
        if attributes:
            for k, v in attributes.items():
                self.attributes[k] = v
                self._user_defined.add(k)

    def init(self):
        pass

    def is_attribute_user_defined(self, attr_name):
        return attr_name in self._user_defined
