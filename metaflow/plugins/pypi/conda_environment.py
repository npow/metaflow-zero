"""Conda environment creation and management for Metaflow."""

import hashlib
import json
import os
import subprocess
import tempfile


class CondaEnvironment:
    """Create and manage conda environments for Metaflow steps."""

    def __init__(self, packages=None, python=None, channels=None):
        self.packages = packages or {}
        self.python = python
        self.channels = channels or ["defaults", "conda-forge"]
        self._env_path = None

    @property
    def env_id(self):
        """Unique ID for this environment based on package specs."""
        spec = json.dumps(
            {"packages": self.packages, "python": self.python, "channels": self.channels},
            sort_keys=True,
        )
        return hashlib.md5(spec.encode()).hexdigest()[:12]

    def create(self, base_dir=None):
        """Create the conda environment.

        Returns the path to the environment.
        """
        if base_dir is None:
            base_dir = os.path.join(tempfile.gettempdir(), "metaflow_conda_envs")
        os.makedirs(base_dir, exist_ok=True)

        env_path = os.path.join(base_dir, "env_%s" % self.env_id)

        if os.path.exists(env_path):
            self._env_path = env_path
            return env_path

        # Build conda create command
        cmd = ["conda", "create", "--prefix", env_path, "--yes", "--quiet"]

        for channel in self.channels:
            cmd.extend(["-c", channel])

        if self.python:
            cmd.append("python=%s" % self.python)
        else:
            cmd.append("python")

        for pkg, version in self.packages.items():
            if version:
                cmd.append("%s=%s" % (pkg, version))
            else:
                cmd.append(pkg)

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Failed to create conda environment: %s" % e.stderr)

        self._env_path = env_path
        return env_path

    def resolve(self):
        """Resolve the environment without creating it.

        Returns a dict of resolved package versions.
        """
        cmd = ["conda", "create", "--dry-run", "--json", "--yes", "--quiet"]

        for channel in self.channels:
            cmd.extend(["-c", channel])

        if self.python:
            cmd.append("python=%s" % self.python)

        for pkg, version in self.packages.items():
            if version:
                cmd.append("%s=%s" % (pkg, version))
            else:
                cmd.append(pkg)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            resolved = {}
            for action in data.get("actions", {}).get("LINK", []):
                resolved[action["name"]] = action["version"]
            return resolved
        except Exception:
            return {}

    @property
    def python_path(self):
        """Path to the Python executable in this environment."""
        if self._env_path:
            return os.path.join(self._env_path, "bin", "python")
        return None
