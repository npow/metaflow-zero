"""IncludeFile descriptor for FlowSpec classes."""

import os


class IncludedFile:
    """Wrapper for loaded file contents."""

    def __init__(self, content, descriptor=None):
        self._content = content
        self.descriptor = descriptor

    def __str__(self):
        return self._content if isinstance(self._content, str) else str(self._content)

    def __eq__(self, other):
        if isinstance(other, IncludedFile):
            return self._content == other._content
        return self._content == other

    def __hash__(self):
        return hash(self._content)


class IncludeFile:
    """A file include descriptor for FlowSpec."""

    def __init__(self, name, default=None, required=True, help=None,
                 is_text=True, encoding=None):
        self.name = name
        self._attr_name = name.replace("-", "_")
        self.default = default
        self.required = required
        self.help = help
        self.is_text = is_text
        self.encoding = encoding or "utf-8"
        self._value_set = False

    def _load_from_env(self):
        env_key = "METAFLOW_RUN_%s" % self._attr_name.upper()
        return os.environ.get(env_key)

    def _load_file(self, path):
        if path is None:
            return None
        mode = "r" if self.is_text else "rb"
        kwargs = {"encoding": self.encoding} if self.is_text else {}
        with open(path, mode, **kwargs) as f:
            return f.read()

    def click_option(self):
        import click
        return {
            "default": None,
            "help": self.help or "",
            "type": click.STRING,
            "required": False,
        }
