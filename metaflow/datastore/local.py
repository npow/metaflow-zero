"""Local filesystem datastore for Metaflow."""

import os
import pickle


class LocalDatastore:
    """Store artifacts and logs on local filesystem."""

    def __init__(self, root=None):
        self.root = root or os.environ.get(
            "METAFLOW_DATASTORE_SYSROOT_LOCAL", ".metaflow"
        )

    def _artifact_dir(self, flow_name, run_id, step_name, task_id):
        return os.path.join(self.root, flow_name, str(run_id),
                            step_name, str(task_id), "artifacts")

    def _artifact_path(self, flow_name, run_id, step_name, task_id, name):
        return os.path.join(
            self._artifact_dir(flow_name, run_id, step_name, task_id),
            name + ".pkl"
        )

    def _log_path(self, flow_name, run_id, step_name, task_id, stream):
        return os.path.join(self.root, flow_name, str(run_id),
                            step_name, str(task_id), "logs", stream + ".txt")

    def clear_task_artifacts(self, flow_name, run_id, step_name, task_id):
        """Remove all .pkl files for a task. Used between retry attempts."""
        art_dir = self._artifact_dir(flow_name, run_id, step_name, task_id)
        if os.path.isdir(art_dir):
            for fn in os.listdir(art_dir):
                if fn.endswith('.pkl'):
                    try:
                        os.unlink(os.path.join(art_dir, fn))
                    except OSError:
                        pass

    def save_artifacts(self, flow_name, run_id, step_name, task_id, artifacts_dict):
        art_dir = self._artifact_dir(flow_name, run_id, step_name, task_id)
        os.makedirs(art_dir, exist_ok=True)
        for name, value in artifacts_dict.items():
            path = os.path.join(art_dir, name + ".pkl")
            with open(path, "wb") as f:
                pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_artifacts(self, flow_name, run_id, step_name, task_id):
        art_dir = self._artifact_dir(flow_name, run_id, step_name, task_id)
        result = {}
        if not os.path.exists(art_dir):
            return result
        for fname in os.listdir(art_dir):
            if fname.endswith(".pkl"):
                name = fname[:-4]
                with open(os.path.join(art_dir, fname), "rb") as f:
                    result[name] = pickle.load(f)
        return result

    def load_artifact(self, flow_name, run_id, step_name, task_id, name):
        path = self._artifact_path(flow_name, run_id, step_name, task_id, name)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return pickle.load(f)

    def has_artifact(self, flow_name, run_id, step_name, task_id, name):
        path = self._artifact_path(flow_name, run_id, step_name, task_id, name)
        return os.path.exists(path)

    def artifact_names(self, flow_name, run_id, step_name, task_id):
        art_dir = self._artifact_dir(flow_name, run_id, step_name, task_id)
        if not os.path.exists(art_dir):
            return []
        return [f[:-4] for f in os.listdir(art_dir) if f.endswith(".pkl")]

    def save_log(self, flow_name, run_id, step_name, task_id, stream, content):
        path = self._log_path(flow_name, run_id, step_name, task_id, stream)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def load_log(self, flow_name, run_id, step_name, task_id, stream):
        path = self._log_path(flow_name, run_id, step_name, task_id, stream)
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
