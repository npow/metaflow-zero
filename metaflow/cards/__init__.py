"""Cards system for Metaflow."""

import hashlib
import json
import os

from ..plugins.cards.exception import CardNotPresentException


class _Card:
    """A single card."""
    def __init__(self, html="", card_type="default", card_id=None, card_hash=None, path=None):
        self._html = html
        self.type = card_type
        self.id = card_id
        self.hash = card_hash or hashlib.md5((html or "").encode()).hexdigest()[:8]
        self.path = path or ""

    def get(self):
        return self._html

    def get_data(self):
        return None


class CardContainer:
    """List-like container of cards."""
    def __init__(self, cards=None):
        self._cards = cards or []

    def __len__(self):
        return len(self._cards)

    def __getitem__(self, idx):
        return self._cards[idx]

    def __iter__(self):
        return iter(self._cards)

    def __bool__(self):
        return len(self._cards) > 0

    def get(self, index=0):
        if index < len(self._cards):
            return self._cards[index].get()
        return None

    def get_data(self, index=0):
        if index < len(self._cards):
            return self._cards[index].get_data()
        return None


def _load_cards_from_dir(card_dir, type=None, id=None):
    """Load cards from a card directory."""
    cards = []
    if not os.path.exists(card_dir):
        return cards

    # Find all metadata JSON files
    meta_files = sorted([f for f in os.listdir(card_dir) if f.endswith(".json")])
    for meta_file in meta_files:
        meta_path = os.path.join(card_dir, meta_file)
        html_file = meta_file.replace(".json", ".html")
        html_path = os.path.join(card_dir, html_file)

        if not os.path.exists(html_path):
            continue

        try:
            with open(meta_path, "r") as f:
                meta = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        card_type = meta.get("type", "default")
        card_id = meta.get("id")
        card_hash = meta.get("hash", "")

        # Apply filters
        if type is not None and card_type != type:
            continue
        if id is not None and card_id != id:
            continue

        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        cards.append(_Card(
            html=html,
            card_type=card_type,
            card_id=card_id,
            card_hash=card_hash,
            path=html_path,
        ))

    return cards


def get_cards(pathspec_or_task, type=None, id=None, follow_resumed=True):
    """Get cards for a task.

    Args:
        pathspec_or_task: Task pathspec string or Task object
        type: Card type filter
        id: Card ID filter
        follow_resumed: Follow resumed tasks to find origin cards

    Returns:
        CardContainer with matching cards
    """
    from ..datastore.local import LocalDatastore
    from ..plugins.metadata_providers.local import LocalMetadataProvider

    # Resolve pathspec
    task_obj = None
    if hasattr(pathspec_or_task, "pathspec"):
        task_obj = pathspec_or_task
        pathspec = pathspec_or_task.pathspec
    else:
        pathspec = str(pathspec_or_task)

    parts = pathspec.split("/")
    if len(parts) != 4:
        raise CardNotPresentException("Invalid pathspec for cards: %s" % pathspec)

    flow_name, run_id, step_name, task_id = parts

    ds = LocalDatastore()

    # Try to find cards in current task's directory
    card_dir = os.path.join(ds.root, flow_name, run_id, step_name, task_id, "cards")
    cards = _load_cards_from_dir(card_dir, type=type, id=id)

    # If no cards and follow_resumed, look for origin task cards
    if not cards and follow_resumed:
        # Method 1: Use task object's origin_pathspec
        if task_obj and hasattr(task_obj, "origin_pathspec") and task_obj.origin_pathspec:
            origin = task_obj.origin_pathspec
            orig_parts = origin.split("/")
            if len(orig_parts) == 4:
                orig_card_dir = os.path.join(ds.root, *orig_parts, "cards")
                cards = _load_cards_from_dir(orig_card_dir, type=type, id=id)

        # Method 2: Check metadata for origin-run-id and origin-task-id
        if not cards:
            try:
                meta = LocalMetadataProvider()
                task_meta = meta.get_task_metadata(flow_name, run_id, step_name, task_id)
                origin_run = None
                origin_task = None
                for m in (task_meta or []):
                    mtype = m.get("type") if isinstance(m, dict) else getattr(m, "type", None)
                    mvalue = m.get("value") if isinstance(m, dict) else getattr(m, "value", None)
                    if mtype == "origin-run-id":
                        origin_run = mvalue
                    elif mtype == "origin-task-id":
                        origin_task = mvalue

                if origin_run and origin_task:
                    orig_card_dir = os.path.join(
                        ds.root, flow_name, origin_run, step_name, origin_task, "cards"
                    )
                    cards = _load_cards_from_dir(orig_card_dir, type=type, id=id)
            except Exception:
                pass

    if not cards:
        raise CardNotPresentException("No cards found for %s" % pathspec)

    return CardContainer(cards)


# Card component stubs
class Markdown:
    def __init__(self, text=""):
        self.text = text

    def render(self):
        return self.text

    def update(self, text):
        self.text = text


class Image:
    def __init__(self, src=None, label=None, disable_updates=False):
        self.src = src
        self.label = label

    def render(self):
        return "<img/>"

    @classmethod
    def from_matplotlib(cls, fig):
        return cls()

    @classmethod
    def from_pil_image(cls, img):
        return cls()


class Table:
    def __init__(self, data=None, headers=None, disable_updates=False):
        self.data = data
        self.headers = headers

    def render(self):
        return "<table/>"

    @classmethod
    def from_dataframe(cls, df):
        return cls()


class Artifact:
    def __init__(self, artifact=None, name=None, compressed=False):
        self.artifact = artifact
        self.name = name

    def render(self):
        return str(self.artifact)


class VegaChart:
    def __init__(self, spec=None):
        self.spec = spec

    def render(self):
        return "<div/>"

    def update(self, spec):
        self.spec = spec

    @classmethod
    def from_altair_chart(cls, chart):
        return cls()


class ProgressBar:
    def __init__(self, max=100, label="", value=0, unit=None, metadata=None):
        self.max = max
        self.label = label
        self.value = value

    def render(self):
        return "<progress/>"

    def update(self, value, metadata=None):
        self.value = value


class MetaflowCard:
    """Base class for custom card types."""
    type = "default"
    ALLOW_USER_COMPONENTS = False

    def __init__(self, options=None):
        self.options = options or {}
        self.id = None
        self.hash = ""

    def render(self, task):
        return ""

    def render_runtime(self, task, data):
        return ""

    def refresh(self, task, data):
        return {}
