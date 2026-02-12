"""Metaflow Client API — read-only access to past runs."""

import os
from ..namespace import namespace, get_namespace, default_namespace
from ..exception import MetaflowNotFound, MetaflowNamespaceMismatch


def _get_datastore():
    from ..datastore.local import LocalDatastore
    return LocalDatastore()


def _get_metadata():
    from ..plugins.metadata_providers.local import LocalMetadataProvider
    return LocalMetadataProvider()


class _MetaflowCode:
    """Stub for code package."""
    def __init__(self):
        self.tarball = None


class MetaflowData:
    """Dot-access namespace for task artifacts."""

    def __init__(self, artifacts_dict):
        self._artifacts = artifacts_dict

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._artifacts:
            return self._artifacts[name]
        raise AttributeError("No artifact '%s'" % name)

    def __contains__(self, name):
        return name in self._artifacts

    def __repr__(self):
        return "MetaflowData(%s)" % list(self._artifacts.keys())


class _ArtifactCollection:
    """Collection of DataArtifact objects supporting iteration and attribute access."""

    def __init__(self, artifacts_list):
        self._list = artifacts_list
        self._by_name = {a.id: a for a in artifacts_list}

    def __iter__(self):
        return iter(self._list)

    def __len__(self):
        return len(self._list)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._by_name:
            return self._by_name[name]
        raise AttributeError("No artifact '%s'" % name)

    def __repr__(self):
        return "_ArtifactCollection(%s)" % [a.id for a in self._list]


class _MetadataEntry:
    """A metadata entry with .type and .value."""
    def __init__(self, entry_dict):
        self.type = entry_dict.get("type", "")
        self.value = entry_dict.get("value", "")

    def __repr__(self):
        return "Metadata(type=%s, value=%s)" % (self.type, self.value)


class DataArtifact:
    """A single artifact."""

    def __init__(self, pathspec, name=None, value=None, _namespace_check=True):
        self._pathspec = pathspec
        self._name = name
        self._value = value
        self._loaded = value is not None
        # Extract flow_name and run_id from pathspec
        parts = pathspec.split("/")
        self._flow_name = parts[0] if len(parts) > 0 else None
        self._run_id = parts[1] if len(parts) > 1 else None

    @property
    def data(self):
        if not self._loaded:
            parts = self._pathspec.split("/")
            ds = _get_datastore()
            self._value = ds.load_artifact(parts[0], parts[1], parts[2], parts[3], self._name)
            self._loaded = True
        return self._value

    @property
    def id(self):
        return self._name

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def tags(self):
        if self._flow_name and self._run_id:
            meta = _get_metadata()
            run_meta = meta.get_run_meta(self._flow_name, self._run_id)
            if run_meta:
                return frozenset(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        return frozenset()


class Task:
    """A task in a step."""

    def __init__(self, pathspec, _namespace_check=True):
        parts = pathspec.split("/")
        if len(parts) != 4:
            raise MetaflowNotFound("Invalid task pathspec: %s" % pathspec)
        self._flow_name = parts[0]
        self._run_id = parts[1]
        self._step_name = parts[2]
        self._task_id = parts[3]
        self._pathspec = pathspec
        self._artifacts_cache = None
        self._metadata_cache = None

        if _namespace_check:
            self._check_namespace()

    def _check_namespace(self):
        ns = get_namespace()
        if ns is None:
            return
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta is None:
            return
        all_tags = set(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        if ns not in all_tags and not ns.startswith("user:"):
            raise MetaflowNamespaceMismatch(
                "Task %s not in namespace %s" % (self._pathspec, ns)
            )
        if ns.startswith("user:"):
            if ns not in all_tags:
                raise MetaflowNamespaceMismatch(
                    "Task %s not in namespace %s" % (self._pathspec, ns)
                )

    def _load_artifacts(self):
        if self._artifacts_cache is None:
            ds = _get_datastore()
            self._artifacts_cache = ds.load_artifacts(
                self._flow_name, self._run_id, self._step_name, self._task_id
            )
        return self._artifacts_cache

    @property
    def id(self):
        return self._task_id

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def parent(self):
        return Step("%s/%s/%s" % (self._flow_name, self._run_id, self._step_name),
                    _namespace_check=False)

    @property
    def data(self):
        arts = self._load_artifacts()
        # Filter out private artifacts
        public = {k: v for k, v in arts.items() if not k.startswith("_")}
        return MetaflowData(public)

    @property
    def artifacts(self):
        arts = self._load_artifacts()
        result = []
        for name, val in arts.items():
            result.append(DataArtifact(self._pathspec, name=name, value=val))
        return _ArtifactCollection(result)

    def __getitem__(self, name):
        arts = self._load_artifacts()
        if name in arts:
            return DataArtifact(self._pathspec, name=name, value=arts[name])
        raise KeyError("No artifact '%s' in task %s" % (name, self._pathspec))

    def __contains__(self, name):
        arts = self._load_artifacts()
        return name in arts

    def __iter__(self):
        return iter(self.artifacts)

    @property
    def stdout(self):
        ds = _get_datastore()
        return ds.load_log(self._flow_name, self._run_id,
                           self._step_name, self._task_id, "stdout")

    @property
    def stderr(self):
        ds = _get_datastore()
        return ds.load_log(self._flow_name, self._run_id,
                           self._step_name, self._task_id, "stderr")

    def loglines(self, stream):
        """Yield (timestamp, message) tuples from logs."""
        from datetime import datetime
        ds = _get_datastore()
        log_text = ds.load_log(self._flow_name, self._run_id,
                               self._step_name, self._task_id, stream)
        for line in log_text.splitlines():
            # Return with a None timestamp for local logs
            yield datetime.utcnow(), line

    @property
    def exception(self):
        arts = self._load_artifacts()
        return arts.get("_exception")

    @property
    def finished(self):
        meta = _get_metadata()
        return meta.is_task_done(self._flow_name, self._run_id,
                                 self._step_name, self._task_id)

    @property
    def successful(self):
        arts = self._load_artifacts()
        return arts.get("_task_ok", False)

    @property
    def metadata(self):
        if self._metadata_cache is None:
            meta = _get_metadata()
            raw = meta.get_task_metadata(
                self._flow_name, self._run_id, self._step_name, self._task_id
            )
            self._metadata_cache = [_MetadataEntry(e) for e in raw]
        return self._metadata_cache

    @property
    def origin_pathspec(self):
        origin_task_id = None
        origin_run_id = None
        for m in self.metadata:
            if m.type == "origin-task-id":
                origin_task_id = m.value
            if m.type == "origin-run-id":
                origin_run_id = m.value
        if origin_task_id and origin_run_id:
            return "%s/%s/%s/%s" % (
                self._flow_name, origin_run_id, self._step_name, origin_task_id
            )
        return None

    @property
    def code(self):
        return _MetaflowCode()

    @property
    def parent_task_pathspecs(self):
        """Return parent task pathspecs as an iterator."""
        result = []
        for m in self.metadata:
            if m.type == "parent-task-ids":
                import json
                try:
                    result = json.loads(m.value)
                except (json.JSONDecodeError, TypeError):
                    pass
        return iter(result)

    @property
    def metadata_dict(self):
        """Return metadata as a dict keyed by type."""
        result = {}
        for m in self.metadata:
            result[m.type] = m.value
        return result

    @property
    def parent_tasks(self):
        """Return parent Task objects from metadata."""
        return [Task(ps, _namespace_check=False) for ps in self.parent_task_pathspecs]

    @property
    def child_task_pathspecs(self):
        """Return pathspecs of child tasks."""
        return [t.pathspec for t in self.child_tasks]

    @property
    def child_tasks(self):
        """Return child Task objects (tasks in subsequent steps that have this task as parent)."""
        meta = _get_metadata()
        # Look at all steps after this one to find tasks that list us as parent
        step_names = meta.get_step_names(self._flow_name, self._run_id)
        result = []
        for sname in step_names:
            task_ids = meta.get_task_ids(self._flow_name, self._run_id, sname)
            for tid in task_ids:
                task_meta = meta.get_task_metadata(
                    self._flow_name, self._run_id, sname, tid
                )
                for entry in task_meta:
                    if entry.get("type") == "parent-task-ids":
                        import json
                        try:
                            parents = json.loads(entry.get("value", "[]"))
                        except (json.JSONDecodeError, TypeError):
                            parents = []
                        if self._pathspec in parents:
                            result.append(
                                Task("%s/%s/%s/%s" % (self._flow_name, self._run_id, sname, tid),
                                     _namespace_check=False)
                            )
        return result

    @property
    def tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        return frozenset()

    @property
    def user_tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("tags", []))
        return frozenset()

    @property
    def system_tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("sys_tags", []))
        return frozenset()

    def __eq__(self, other):
        if isinstance(other, Task):
            return self._pathspec == other._pathspec
        return NotImplemented

    def __hash__(self):
        return hash(self._pathspec)

    def __repr__(self):
        return "Task('%s')" % self._pathspec


class Step:
    """A step in a run."""

    def __init__(self, pathspec, _namespace_check=True):
        parts = pathspec.split("/")
        if len(parts) != 3:
            raise MetaflowNotFound("Invalid step pathspec: %s" % pathspec)
        self._flow_name = parts[0]
        self._run_id = parts[1]
        self._step_name = parts[2]
        self._pathspec = pathspec

        if _namespace_check:
            self._check_namespace()

    def _check_namespace(self):
        ns = get_namespace()
        if ns is None:
            return
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta is None:
            return
        all_tags = set(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        if ns.startswith("user:") and ns not in all_tags:
            raise MetaflowNamespaceMismatch(
                "Step %s not in namespace %s" % (self._pathspec, ns)
            )

    @property
    def id(self):
        return self._step_name

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def task(self):
        """Return the latest task (highest task_id)."""
        tasks = list(self.tasks())
        if tasks:
            return tasks[-1]
        raise MetaflowNotFound("No tasks in step %s" % self._pathspec)

    def tasks(self, *tags):
        meta = _get_metadata()
        task_ids = meta.get_task_ids(self._flow_name, self._run_id, self._step_name)
        for tid in task_ids:
            t = Task("%s/%s/%s/%s" % (self._flow_name, self._run_id,
                                       self._step_name, tid),
                     _namespace_check=False)
            if tags:
                if all(tag in t.tags for tag in tags):
                    yield t
            else:
                yield t

    def control_tasks(self):
        """Return tasks where parallel node_index=0.
        Only returns tasks that explicitly have parallel-node-index metadata."""
        for task in self.tasks():
            meta = task.metadata
            for entry in meta:
                if entry.type == "parallel-node-index":
                    if entry.value == "0":
                        yield task
                    break

    @property
    def child_steps(self):
        """Return successor steps in the DAG by looking at child tasks of tasks in this step."""
        child_step_names = set()
        meta = _get_metadata()
        all_step_names = meta.get_step_names(self._flow_name, self._run_id)
        for task in self.tasks():
            ct = task.child_tasks
            for child_task in ct:
                child_step_names.add(child_task._step_name)
        return [
            Step("%s/%s/%s" % (self._flow_name, self._run_id, sn),
                 _namespace_check=False)
            for sn in child_step_names
        ]

    def __getitem__(self, task_id):
        return Task("%s/%s/%s/%s" % (self._flow_name, self._run_id,
                                      self._step_name, str(task_id)),
                    _namespace_check=False)

    def __iter__(self):
        return self.tasks()

    @property
    def tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        return frozenset()

    @property
    def parent(self):
        return Run("%s/%s" % (self._flow_name, self._run_id),
                   _namespace_check=False)

    @property
    def path_components(self):
        return [self._flow_name, self._run_id, self._step_name]

    @property
    def origin_pathspec(self):
        """Check if any task in this step was resumed from another run."""
        meta = _get_metadata()
        task_ids = meta.get_task_ids(self._flow_name, self._run_id, self._step_name)
        for tid in task_ids:
            task_meta = meta.get_task_metadata(
                self._flow_name, self._run_id, self._step_name, tid
            )
            origin_run_id = None
            for entry in task_meta:
                if entry.get("type") == "origin-run-id":
                    origin_run_id = entry.get("value")
            if origin_run_id:
                return "%s/%s/%s" % (self._flow_name, origin_run_id, self._step_name)
        return None

    def __eq__(self, other):
        if isinstance(other, Step):
            return self._pathspec == other._pathspec
        return NotImplemented

    def __hash__(self):
        return hash(self._pathspec)

    def __repr__(self):
        return "Step('%s')" % self._pathspec


class Run:
    """A run of a flow."""

    def __init__(self, pathspec, _namespace_check=True):
        if "/" in pathspec:
            parts = pathspec.split("/")
            self._flow_name = parts[0]
            self._run_id = parts[1]
        else:
            self._flow_name = None
            self._run_id = pathspec
        self._pathspec = pathspec

        if _namespace_check and self._flow_name:
            self._check_namespace()

    def _check_namespace(self):
        ns = get_namespace()
        if ns is None:
            return
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta is None:
            raise MetaflowNamespaceMismatch(
                "Run %s not found or not in namespace %s" % (self._pathspec, ns)
            )
        all_tags = set(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        if ns not in all_tags:
            raise MetaflowNamespaceMismatch(
                "Run %s not in namespace %s" % (self._pathspec, ns)
            )

    @property
    def id(self):
        return self._run_id

    @property
    def pathspec(self):
        return self._pathspec

    @property
    def parent(self):
        if self._flow_name:
            return Flow(self._flow_name)
        return None

    @property
    def data(self):
        """Shortcut to end task data."""
        try:
            end_step = self["end"]
            return end_step.task.data
        except Exception:
            return None

    @property
    def end_task(self):
        try:
            return self["end"].task
        except Exception:
            return None

    @property
    def successful(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta and run_meta.get("status") == "done":
            # Check if end step completed successfully
            try:
                end_step = self["end"]
                for task in end_step.tasks():
                    if task.successful:
                        return True
            except Exception:
                pass
        return False

    @property
    def finished(self):
        meta = _get_metadata()
        return meta.is_run_done(self._flow_name, self._run_id)

    @property
    def code(self):
        return _MetaflowCode()

    @property
    def origin_pathspec(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            # Check for origin run ID in metadata
            for step_name in meta.get_step_names(self._flow_name, self._run_id):
                for task_id in meta.get_task_ids(self._flow_name, self._run_id, step_name):
                    task_meta = meta.get_task_metadata(
                        self._flow_name, self._run_id, step_name, task_id
                    )
                    for entry in task_meta:
                        if entry.get("type") == "origin-run-id":
                            origin_id = entry.get("value")
                            return "%s/%s" % (self._flow_name, origin_id)
        return None

    def steps(self, *tags):
        meta = _get_metadata()
        step_names = meta.get_step_names(self._flow_name, self._run_id)
        for sname in step_names:
            s = Step("%s/%s/%s" % (self._flow_name, self._run_id, sname),
                     _namespace_check=False)
            if tags:
                if all(tag in s.tags for tag in tags):
                    yield s
            else:
                yield s

    def __getitem__(self, step_name):
        return Step("%s/%s/%s" % (self._flow_name, self._run_id, step_name),
                    _namespace_check=False)

    def __iter__(self):
        return self.steps()

    @property
    def tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
        return frozenset()

    @property
    def user_tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("tags", []))
        return frozenset()

    @property
    def system_tags(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return frozenset(run_meta.get("sys_tags", []))
        return frozenset()

    @staticmethod
    def _validate_tag(tag):
        """Validate a single tag."""
        if not isinstance(tag, str):
            raise Exception("Tag must be a string, got %s" % type(tag).__name__)
        if len(tag) == 0:
            raise Exception("Tag must not be empty")
        if len(tag) > 512:
            raise Exception("Tag must not exceed 512 characters")
        # Verify UTF-8 encodability
        try:
            tag.encode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            raise Exception("Tag must be valid UTF-8")

    def _get_sys_tags_set(self):
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            return set(run_meta.get("sys_tags", []))
        return set()

    def add_tag(self, tag):
        # Deprecated: accepts string or list
        if isinstance(tag, list):
            self.add_tags(tag)
            return
        self._validate_tag(tag)
        sys_tags = self._get_sys_tags_set()
        if tag in sys_tags:
            return  # silently skip system tags
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            tags = set(run_meta.get("tags", []))
            tags.add(tag)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(tags))

    def add_tags(self, tags):
        sys_tags = self._get_sys_tags_set()
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            existing = set(run_meta.get("tags", []))
            for t in tags:
                self._validate_tag(t)
                if t not in sys_tags:
                    existing.add(t)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(existing))

    def remove_tag(self, tag):
        # Deprecated: accepts string or list
        if isinstance(tag, list):
            self.remove_tags(tag)
            return
        if not isinstance(tag, str):
            raise Exception("Tag must be a string, got %s" % type(tag).__name__)
        sys_tags = self._get_sys_tags_set()
        if tag in sys_tags:
            raise Exception("Cannot remove system tag '%s'" % tag)
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            tags = set(run_meta.get("tags", []))
            tags.discard(tag)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(tags))

    def remove_tags(self, tags):
        sys_tags = self._get_sys_tags_set()
        for t in tags:
            if not isinstance(t, str):
                raise Exception("Tag must be a string, got %s" % type(t).__name__)
            if t in sys_tags:
                raise Exception("Cannot remove system tag '%s'" % t)
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            existing = set(run_meta.get("tags", []))
            existing -= set(tags)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(existing))

    def replace_tag(self, old_tag, new_tag):
        # Deprecated: accepts (list, list) pair
        if isinstance(old_tag, list):
            self.replace_tags(old_tag, new_tag)
            return
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            tags = set(run_meta.get("tags", []))
            tags.discard(old_tag)
            tags.add(new_tag)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(tags))

    def replace_tags(self, tags_to_remove, tags_to_add):
        if not tags_to_remove and not tags_to_add:
            raise Exception("Must provide tags to remove or add")
        sys_tags = self._get_sys_tags_set()
        for t in tags_to_remove:
            if not isinstance(t, str):
                raise Exception("Tag must be a string, got %s" % type(t).__name__)
            if t in sys_tags:
                raise Exception("Cannot remove system tag '%s'" % t)
        meta = _get_metadata()
        run_meta = meta.get_run_meta(self._flow_name, self._run_id)
        if run_meta:
            tags = set(run_meta.get("tags", []))
            tags -= set(tags_to_remove)
            tags.update(tags_to_add)
            meta.update_run_tags(self._flow_name, self._run_id, tags=list(tags))

    def __eq__(self, other):
        if isinstance(other, Run):
            return self._pathspec == other._pathspec
        return NotImplemented

    def __hash__(self):
        return hash(self._pathspec)

    def __repr__(self):
        return "Run('%s')" % self._pathspec


class Flow:
    """A named flow."""

    def __init__(self, name, _namespace_check=True):
        self._name = name
        self._namespace_check = _namespace_check

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._name

    @property
    def latest_run(self):
        meta = _get_metadata()
        run_ids = meta.get_run_ids(self._name)
        if run_ids:
            return Run("%s/%s" % (self._name, run_ids[0]), _namespace_check=False)
        return None

    @property
    def latest_successful_run(self):
        meta = _get_metadata()
        run_ids = meta.get_run_ids(self._name)
        for rid in run_ids:
            r = Run("%s/%s" % (self._name, rid), _namespace_check=False)
            if r.successful:
                return r
        return None

    def runs(self, *tags):
        meta = _get_metadata()
        ns = get_namespace()
        run_ids = meta.get_run_ids(self._name)
        for rid in run_ids:
            run_meta = meta.get_run_meta(self._name, rid)
            if run_meta is None:
                continue
            all_tags = set(run_meta.get("tags", []) + run_meta.get("sys_tags", []))

            # Namespace filtering
            if ns is not None and ns not in all_tags:
                continue

            # Tag filtering
            if tags:
                if not all(t in all_tags for t in tags):
                    continue

            yield Run("%s/%s" % (self._name, rid), _namespace_check=False)

    def __getitem__(self, run_id):
        # Check namespace
        ns = get_namespace()
        if ns is not None:
            meta = _get_metadata()
            run_meta = meta.get_run_meta(self._name, str(run_id))
            if run_meta is None:
                raise MetaflowNamespaceMismatch(
                    "Run '%s/%s' not found" % (self._name, run_id)
                )
            all_tags = set(run_meta.get("tags", []) + run_meta.get("sys_tags", []))
            if ns not in all_tags:
                raise MetaflowNamespaceMismatch(
                    "Run '%s/%s' not in namespace '%s'" % (self._name, run_id, ns)
                )
        return Run("%s/%s" % (self._name, str(run_id)), _namespace_check=False)

    def __iter__(self):
        return self.runs()

    @property
    def tags(self):
        return frozenset()

    def __repr__(self):
        return "Flow('%s')" % self._name


class Metaflow:
    """Top-level Metaflow object."""

    def __init__(self, _current_metadata=None):
        pass

    @property
    def flows(self):
        return []
