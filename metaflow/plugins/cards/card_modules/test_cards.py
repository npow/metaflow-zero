"""Built-in test card types for Metaflow."""

from ....cards import MetaflowCard
from ....decorators import _register_card_type


class TestStringComponent:
    """A test component that stores a string."""
    def __init__(self, text=""):
        self.text = str(text)

    def render(self):
        return self.text


class TaskspecCard(MetaflowCard):
    type = "taskspec_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        return str(task) if task else ""


class TestPathspecCard(MetaflowCard):
    type = "test_pathspec_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        return str(task) if task else ""


class TestEditableCard(MetaflowCard):
    type = "test_editable_card"
    ALLOW_USER_COMPONENTS = True

    def render(self, task):
        components = getattr(self, '_components', [])
        if components:
            # Only render components that have a render() method
            parts = []
            for comp in components:
                if hasattr(comp, 'render'):
                    parts.append(comp.render())
            return "\n".join(parts)
        return str(task) if task else ""


class TestEditableCard2(MetaflowCard):
    type = "test_editable_card_2"
    ALLOW_USER_COMPONENTS = True

    def render(self, task):
        components = getattr(self, '_components', [])
        if components:
            parts = []
            for comp in components:
                if hasattr(comp, 'render'):
                    parts.append(comp.render())
            return "\n".join(parts)
        return str(task) if task else ""


class TestTimeoutCard(MetaflowCard):
    type = "test_timeout_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        import time
        timeout = self.options.get("timeout", 0)
        if timeout:
            time.sleep(timeout + 5)
        return str(task) if task else ""


class TestBrokenCard(MetaflowCard):
    type = "test_broken_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        raise ImportError("This card is intentionally broken")


class EditableImportTestCard(MetaflowCard):
    type = "editable_import_test_card"
    ALLOW_USER_COMPONENTS = True

    def render(self, task):
        components = getattr(self, '_components', [])
        if components:
            parts = []
            for comp in components:
                if hasattr(comp, 'render'):
                    parts.append(comp.render())
            return "\n".join(parts)
        return str(task) if task else ""


class NonEditableImportTestCard(MetaflowCard):
    type = "non_editable_import_test_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        return str(task) if task else ""


class TestNonEditableCard(MetaflowCard):
    type = "test_non_editable_card"
    ALLOW_USER_COMPONENTS = False

    def render(self, task):
        components = getattr(self, '_components', [])
        if components:
            # Even non-editable cards can have components if accessed via id
            parts = []
            for comp in components:
                if hasattr(comp, 'render'):
                    parts.append(comp.render())
            return "\n".join(parts)
        return str(task) if task else ""


# Register all card types
for cls in [TaskspecCard, TestPathspecCard, TestEditableCard, TestEditableCard2,
            TestTimeoutCard, TestBrokenCard, EditableImportTestCard,
            NonEditableImportTestCard, TestNonEditableCard]:
    _register_card_type(cls.type, cls)
