"""Local metadata provider for Metaflow."""

import json
import os
import time


class LocalMetadataProvider:
    """Store metadata as JSON files in _meta/ directories."""

    def __init__(self, datastore_root=None):
        self.root = datastore_root or os.environ.get(
            "METAFLOW_DATASTORE_SYSROOT_LOCAL", ".metaflow"
        )

    @staticmethod
    def _deduce_run_id_from_meta_dir(meta_path, sub_type):
        """Extract run_id from a metadata path."""
        parts = meta_path.replace("\\", "/").split("/")
        # Remove trailing _meta
        if parts[-1] == "_meta":
            parts = parts[:-1]
        # Structure: root/flow_name/run_id/step_name/task_id
        # For flow: root/flow_name/_meta -> no run_id
        # For run: root/flow_name/run_id/_meta -> run_id at index -1
        # For step: root/flow_name/run_id/step_name/_meta -> run_id at index -2
        # For task: root/flow_name/run_id/step_name/task_id/_meta -> run_id at index -3
        depth_map = {"flow": None, "run": -1, "step": -2, "task": -3}
        idx = depth_map.get(sub_type)
        if idx is None:
            return None
        try:
            return parts[idx]
        except IndexError:
            return None

    def _meta_dir(self, flow_name, run_id=None, step_name=None, task_id=None):
        parts = [self.root, flow_name]
        if run_id is not None:
            parts.append(str(run_id))
        if step_name is not None:
            parts.append(step_name)
        if task_id is not None:
            parts.append(str(task_id))
        parts.append("_meta")
        return os.path.join(*parts)

    def _ensure_meta_dir(self, *args):
        d = self._meta_dir(*args)
        os.makedirs(d, exist_ok=True)
        return d

    def new_run(self, flow_name, run_id, tags=None, sys_tags=None):
        meta_dir = self._ensure_meta_dir(flow_name, run_id)
        info = {
            "run_id": str(run_id),
            "flow_name": flow_name,
            "tags": list(tags or []),
            "sys_tags": list(sys_tags or []),
            "created_at": time.time(),
            "status": "running",
        }
        with open(os.path.join(meta_dir, "run_info.json"), "w") as f:
            json.dump(info, f)
        # Also ensure flow-level meta exists
        self._ensure_meta_dir(flow_name)

    def new_step(self, flow_name, run_id, step_name):
        meta_dir = self._ensure_meta_dir(flow_name, run_id, step_name)
        info = {
            "step_name": step_name,
            "created_at": time.time(),
        }
        with open(os.path.join(meta_dir, "step_info.json"), "w") as f:
            json.dump(info, f)

    def new_task(self, flow_name, run_id, step_name, task_id):
        meta_dir = self._ensure_meta_dir(flow_name, run_id, step_name, task_id)
        info = {
            "task_id": str(task_id),
            "created_at": time.time(),
            "status": "running",
        }
        with open(os.path.join(meta_dir, "task_info.json"), "w") as f:
            json.dump(info, f)

    def register_metadata(self, flow_name, run_id, step_name, task_id, metadata_list):
        meta_dir = self._ensure_meta_dir(flow_name, run_id, step_name, task_id)
        existing = self._load_metadata_list(meta_dir)
        existing.extend(metadata_list)
        with open(os.path.join(meta_dir, "metadata.json"), "w") as f:
            json.dump(existing, f)

    def _load_metadata_list(self, meta_dir):
        path = os.path.join(meta_dir, "metadata.json")
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        return []

    def done_task(self, flow_name, run_id, step_name, task_id):
        meta_dir = self._meta_dir(flow_name, run_id, step_name, task_id)
        info_path = os.path.join(meta_dir, "task_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            info["status"] = "done"
            info["finished_at"] = time.time()
            with open(info_path, "w") as f:
                json.dump(info, f)

    def done_run(self, flow_name, run_id):
        meta_dir = self._meta_dir(flow_name, run_id)
        info_path = os.path.join(meta_dir, "run_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            info["status"] = "done"
            info["finished_at"] = time.time()
            with open(info_path, "w") as f:
                json.dump(info, f)

    def is_task_done(self, flow_name, run_id, step_name, task_id):
        meta_dir = self._meta_dir(flow_name, run_id, step_name, task_id)
        info_path = os.path.join(meta_dir, "task_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            return info.get("status") == "done"
        return False

    def is_run_done(self, flow_name, run_id):
        meta_dir = self._meta_dir(flow_name, run_id)
        info_path = os.path.join(meta_dir, "run_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            return info.get("status") == "done"
        return False

    def get_run_ids(self, flow_name):
        flow_dir = os.path.join(self.root, flow_name)
        if not os.path.exists(flow_dir):
            return []
        runs = []
        for entry in os.listdir(flow_dir):
            if entry.startswith("_") or entry.startswith("."):
                continue
            meta_dir = os.path.join(flow_dir, entry, "_meta")
            if os.path.exists(meta_dir):
                runs.append(entry)
        return sorted(runs, reverse=True)

    def get_step_names(self, flow_name, run_id):
        run_dir = os.path.join(self.root, flow_name, str(run_id))
        if not os.path.exists(run_dir):
            return []
        steps = []
        for entry in os.listdir(run_dir):
            if entry.startswith("_") or entry.startswith("."):
                continue
            meta_dir = os.path.join(run_dir, entry, "_meta")
            if os.path.exists(meta_dir):
                steps.append(entry)
        return steps

    def get_task_ids(self, flow_name, run_id, step_name):
        step_dir = os.path.join(self.root, flow_name, str(run_id), step_name)
        if not os.path.exists(step_dir):
            return []
        tasks = []
        for entry in os.listdir(step_dir):
            if entry.startswith("_") or entry.startswith("."):
                continue
            meta_dir = os.path.join(step_dir, entry, "_meta")
            if os.path.exists(meta_dir):
                tasks.append(entry)
        return sorted(tasks)

    def update_run_tags(self, flow_name, run_id, tags=None, sys_tags=None):
        meta_dir = self._meta_dir(flow_name, run_id)
        info_path = os.path.join(meta_dir, "run_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                info = json.load(f)
            if tags is not None:
                info["tags"] = list(tags)
            if sys_tags is not None:
                info["sys_tags"] = list(sys_tags)
            with open(info_path, "w") as f:
                json.dump(info, f)

    def get_run_meta(self, flow_name, run_id):
        meta_dir = self._meta_dir(flow_name, run_id)
        info_path = os.path.join(meta_dir, "run_info.json")
        if os.path.exists(info_path):
            with open(info_path) as f:
                return json.load(f)
        return None

    def get_task_metadata(self, flow_name, run_id, step_name, task_id):
        meta_dir = self._meta_dir(flow_name, run_id, step_name, task_id)
        return self._load_metadata_list(meta_dir)
