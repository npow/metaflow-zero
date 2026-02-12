"""Metaflow plugins."""


class InternalTestUnboundedForeachInput:
    """Test stub for unbounded foreach input.

    Wraps an iterable and marks it as an unbounded foreach source.
    Behaves like a list for all practical purposes.
    """

    def __init__(self, data):
        self._data = list(data)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        return self._data[index]

    def __repr__(self):
        return "InternalTestUnboundedForeachInput(%r)" % self._data
