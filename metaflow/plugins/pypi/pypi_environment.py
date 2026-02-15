"""PyPI (venv + pip) environment creation and management for Metaflow."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import venv


class PyPIEnvironment:
    """Create and manage virtual environments with pip packages for Metaflow steps."""

    def __init__(self, packages=None, python=None):
        self.packages = packages or {}
        self.python = python
        self._env_path = None

    @property
    def env_id(self):
        """Unique ID for this environment based on package specs."""
        spec = json.dumps(
            {"packages": self.packages, "python": self.python},
            sort_keys=True,
        )
        return hashlib.md5(spec.encode()).hexdigest()[:12]

    def create(self, base_dir=None):
        """Create the virtual environment and install packages.

        Returns the path to the environment.
        """
        if base_dir is None:
            base_dir = os.path.join(tempfile.gettempdir(), "metaflow_pypi_envs")
        os.makedirs(base_dir, exist_ok=True)

        env_path = os.path.join(base_dir, "env_%s" % self.env_id)

        if os.path.exists(env_path):
            self._env_path = env_path
            return env_path

        # Create venv
        builder = venv.EnvBuilder(with_pip=True, clear=True)
        builder.create(env_path)

        # Install packages
        pip_path = os.path.join(env_path, "bin", "pip")
        install_args = []
        for pkg, version in self.packages.items():
            if version:
                install_args.append("%s==%s" % (pkg, version))
            else:
                install_args.append(pkg)

        if install_args:
            cmd = [pip_path, "install", "--quiet"] + install_args
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                raise RuntimeError("Failed to install packages: %s" % e.stderr)

        self._env_path = env_path
        return env_path

    def resolve(self):
        """Resolve packages without creating the environment.

        Returns a dict of resolved package versions using pip's resolver.
        """
        # Use pip to dry-run resolve
        install_args = []
        for pkg, version in self.packages.items():
            if version:
                install_args.append("%s==%s" % (pkg, version))
            else:
                install_args.append(pkg)

        if not install_args:
            return {}

        cmd = [sys.executable, "-m", "pip", "install", "--dry-run",
               "--report", "-", "--quiet"] + install_args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            resolved = {}
            for item in data.get("install", []):
                meta = item.get("metadata", {})
                resolved[meta.get("name", "")] = meta.get("version", "")
            return resolved
        except Exception:
            return {}

    @property
    def python_path(self):
        """Path to the Python executable in this environment."""
        if self._env_path:
            return os.path.join(self._env_path, "bin", "python")
        return None
