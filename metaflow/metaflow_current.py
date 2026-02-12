"""Current singleton — runtime context for Metaflow."""

import os
import tempfile
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TaskContext:
    """Immutable snapshot of all task-level context, passed to Current.bind()."""
    flow_name: str
    run_id: str
    step_name: str
    task_id: str
    retry_count: int = 0
    origin_run_id: str = None
    namespace: str = None
    username: str = "unknown"
    parameter_names: frozenset = frozenset()
    user_tags: tuple = ()
    sys_tags: tuple = ()
    parallel_num_nodes: int = 1
    parallel_node_index: int = 0
    project_name: str = None
    branch_name: str = None
    project_flow_name: str = None
    is_production: bool = False
    graph: object = None


class _ParallelInfo:
    def __init__(self):
        self.num_nodes = 1
        self.node_index = 0


class _CardComponentList:
    """Component list for a specific card instance."""
    def __init__(self):
        self._components = []

    def append(self, component):
        self._components.append(component)

    def clear(self):
        self._components.clear()

    def extend(self, components):
        self._components.extend(components)

    def refresh(self, data=None):
        pass

    @property
    def components(self):
        return {}


class _CardContext:
    """Card context managing all registered cards for the current step."""

    def __init__(self):
        self._reset()

    def _reset(self):
        """Reset card context for a new step."""
        self._registered_cards = []  # list of (type, id, customize, allow_user_components)
        self._card_components = {}   # (type, id) -> _CardComponentList
        self._id_components = {}     # card_id -> _CardComponentList
        self._default_editable_key = None
        self._next_card_index = 0

    def _register_card(self, card_type, card_id, customize, allow_user_components):
        """Register a card from CardDecorator.task_pre_step."""
        key = (card_type, card_id)
        self._registered_cards.append((card_type, card_id, customize, allow_user_components))
        if key not in self._card_components:
            self._card_components[key] = _CardComponentList()
        if card_id is not None and card_id not in self._id_components:
            self._id_components[card_id] = self._card_components[key]
        self._update_default_editable()

    def _update_default_editable(self):
        """Determine the default editable card."""
        # Priority 1: Card with customize=True
        for ctype, cid, customize, allow in self._registered_cards:
            if customize:
                self._default_editable_key = (ctype, cid)
                return

        # Priority 2: Single editable card without id
        editable_no_id = [
            (ctype, cid) for ctype, cid, _, allow in self._registered_cards
            if allow and cid is None
        ]
        if len(editable_no_id) == 1:
            self._default_editable_key = editable_no_id[0]
            return

        # Priority 3: Single editable card (even with id)
        editable_all = [
            (ctype, cid) for ctype, cid, _, allow in self._registered_cards
            if allow
        ]
        if len(editable_all) == 1:
            self._default_editable_key = editable_all[0]
            return

        self._default_editable_key = None

    def _allocate_card_index(self):
        """Allocate the next card file index."""
        idx = self._next_card_index
        self._next_card_index += 1
        return idx

    def _get_components(self, card_type, card_id):
        """Get components for a specific card."""
        key = (card_type, card_id)
        if key in self._card_components:
            return self._card_components[key]._components
        return []

    def append(self, component):
        """Append component to default editable card."""
        if self._default_editable_key and self._default_editable_key in self._card_components:
            self._card_components[self._default_editable_key].append(component)

    def clear(self):
        """Clear components from default editable card."""
        if self._default_editable_key and self._default_editable_key in self._card_components:
            self._card_components[self._default_editable_key].clear()

    def extend(self, components):
        """Extend default editable card components."""
        if self._default_editable_key and self._default_editable_key in self._card_components:
            self._card_components[self._default_editable_key].extend(components)

    def get(self, type=None):
        """Get components by type. Returns empty list for nonexistent type."""
        if type:
            for key, comp_list in self._card_components.items():
                if key[0] == type:
                    return comp_list._components
        return []

    def refresh(self, data=None):
        """Refresh card data (no-op for basic implementation)."""
        pass

    def __getitem__(self, card_id):
        """Access card component list by id."""
        if card_id in self._id_components:
            return self._id_components[card_id]
        # Return a dummy that silently accepts appends but doesn't store
        return _CardComponentList()

    @property
    def components(self):
        """Dict-like access to named components."""
        return {}


class Current:
    """Runtime context singleton for Metaflow steps."""

    def __init__(self):
        self._flow_name = None
        self._run_id = None
        self._step_name = None
        self._task_id = None
        self._retry_count = 0
        self._origin_run_id = None
        self._namespace = None
        self._username = None
        self._tags = frozenset()
        self._sys_tags = frozenset()
        self._user_tags = frozenset()
        self._parameter_names = set()
        self._tempdir = None
        self._is_production = False
        self._project_name = None
        self._branch_name = None
        self._project_flow_name = None
        self._parallel = _ParallelInfo()
        self._card = _CardContext()
        self._graph = None
        self._ext_attrs = {}

    def bind(self, ctx: TaskContext):
        """Bind all task context at once from a TaskContext dataclass."""
        self._flow_name = ctx.flow_name
        self._run_id = ctx.run_id
        self._step_name = ctx.step_name
        self._task_id = ctx.task_id
        self._retry_count = ctx.retry_count
        self._origin_run_id = ctx.origin_run_id
        self._namespace = ctx.namespace
        self._username = ctx.username
        self._parameter_names = set(ctx.parameter_names)
        self._user_tags = frozenset(ctx.user_tags)
        self._sys_tags = frozenset(ctx.sys_tags)
        self._tags = self._user_tags
        self._parallel.num_nodes = ctx.parallel_num_nodes
        self._parallel.node_index = ctx.parallel_node_index
        self._project_name = ctx.project_name
        self._branch_name = ctx.branch_name
        self._project_flow_name = ctx.project_flow_name
        self._is_production = ctx.is_production
        self._graph = ctx.graph

    def bind_retry(self, retry_count: int):
        """Update retry count within a task. Called between attempts."""
        self._retry_count = retry_count

    def _update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, "_%s" % k, v)

    def _update_env(self, env_dict):
        """Update extension attributes on current from a dict."""
        for k, v in env_dict.items():
            self._ext_attrs[k] = v

    def _set_tags(self, user_tags=None, sys_tags=None):
        if user_tags is not None:
            self._user_tags = frozenset(user_tags)
        if sys_tags is not None:
            self._sys_tags = frozenset(sys_tags)
        self._tags = self._user_tags

    @property
    def flow_name(self):
        return self._flow_name

    @property
    def run_id(self):
        return self._run_id

    @property
    def step_name(self):
        return self._step_name

    @property
    def task_id(self):
        return self._task_id

    @property
    def retry_count(self):
        return self._retry_count

    @property
    def origin_run_id(self):
        return self._origin_run_id

    @property
    def namespace(self):
        return self._namespace

    @property
    def username(self):
        return self._username

    @property
    def pathspec(self):
        if self._flow_name and self._run_id and self._step_name and self._task_id:
            return "%s/%s/%s/%s" % (self._flow_name, self._run_id,
                                    self._step_name, self._task_id)
        return None

    @property
    def tags(self):
        return set(self._user_tags)

    @property
    def parameter_names(self):
        return sorted(self._parameter_names)

    @property
    def tempdir(self):
        if self._tempdir is None:
            self._tempdir = tempfile.mkdtemp(prefix="metaflow_")
        return self._tempdir

    @property
    def is_production(self):
        return self._is_production

    @property
    def project_name(self):
        return self._project_name

    @property
    def branch_name(self):
        return self._branch_name

    @property
    def project_flow_name(self):
        return self._project_flow_name

    @property
    def parallel(self):
        return self._parallel

    @property
    def card(self):
        return self._card

    @property
    def task(self):
        """Lazy-loaded Task from client API."""
        if self.pathspec:
            from .client import Task
            return Task(self.pathspec)
        return None

    @property
    def run(self):
        """Lazy-loaded Run from client API."""
        if self._flow_name and self._run_id:
            from .client import Run
            return Run("%s/%s" % (self._flow_name, self._run_id))
        return None

    @property
    def is_running_flow(self):
        return self._flow_name is not None

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if "_ext_attrs" in self.__dict__ and name in self.__dict__["_ext_attrs"]:
            return self.__dict__["_ext_attrs"][name]
        raise AttributeError("Current has no attribute '%s'" % name)


# Module-level singleton
current = Current()
