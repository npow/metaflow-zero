"""Structural tests enforcing architectural invariants.

These tests validate the golden principles mechanically — they are the
automated equivalent of code review for architecture and taste.

Inspired by OpenAI's harness engineering approach: "constraints are an
early prerequisite that allow speed without decay or architectural drift."
"""

import ast
import os
import re

import pytest

METAFLOW_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "metaflow")
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")

# Layer definitions — dependency direction must be left-to-right only
LAYERS = {
    0: {"exception.py", "util.py", "metaflow_config.py", "_extension_loader.py"},
    1: {"graph.py", "parameters.py", "includefile.py", "namespace.py"},
    2: set(),  # datastore/ directory
    3: {"flowspec.py", "decorators.py", "metaflow_current.py"},
    4: set(),  # client/ directory
    5: {"runtime.py"},
    6: {"cli.py"},
    7: set(),  # runner/ directory
}

FILE_TO_LAYER = {}
for layer, files in LAYERS.items():
    for f in files:
        FILE_TO_LAYER[f] = layer
# Directory-based layers
DIR_TO_LAYER = {
    "datastore": 2,
    "client": 4,
    "cli_components": 6,
    "cmd": 6,
    "runner": 7,
}


def _get_py_files(directory):
    """Yield all .py files under directory."""
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d != "__pycache__" and d != "_vendor"]
        for f in files:
            if f.endswith(".py"):
                yield os.path.join(root, f)


def _get_layer(filepath):
    """Determine the architectural layer of a file. Returns None for plugins."""
    rel = os.path.relpath(filepath, METAFLOW_ROOT)
    parts = rel.replace("\\", "/").split("/")

    if parts[0] == "plugins" or parts[0] == "user_configs":
        return None  # cross-cutting, exempt from layer checks

    if parts[0] in DIR_TO_LAYER:
        return DIR_TO_LAYER[parts[0]]

    basename = parts[0]
    return FILE_TO_LAYER.get(basename)


# ============================================================
# Test 1: File size limits
# ============================================================
class TestFileSizeLimits:
    """Every source file should be under 500 lines.
    FIX: Split large files into focused submodules."""

    MAX_LINES = 500

    def _collect_files(self):
        return list(_get_py_files(METAFLOW_ROOT))

    @pytest.mark.parametrize("filepath", list(_get_py_files(METAFLOW_ROOT)),
                             ids=lambda p: os.path.relpath(p, METAFLOW_ROOT))
    def test_file_under_limit(self, filepath):
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            line_count = sum(1 for _ in f)
        assert line_count <= self.MAX_LINES, (
            "%s has %d lines (limit: %d). "
            "FIX: Split into smaller focused modules in the same directory."
            % (os.path.relpath(filepath, METAFLOW_ROOT), line_count, self.MAX_LINES)
        )


# ============================================================
# Test 2: Naming conventions
# ============================================================
class TestNamingConventions:
    """Module files must be snake_case.
    FIX: Rename the file to use snake_case (lowercase with underscores)."""

    @pytest.mark.parametrize("filepath", list(_get_py_files(METAFLOW_ROOT)),
                             ids=lambda p: os.path.relpath(p, METAFLOW_ROOT))
    def test_snake_case_filename(self, filepath):
        basename = os.path.basename(filepath)
        if basename.startswith("_"):
            return  # private/dunder files ok
        name = basename.replace(".py", "")
        assert name == name.lower(), (
            "File '%s' is not snake_case. FIX: Rename to '%s.py'"
            % (basename, name.lower())
        )


# ============================================================
# Test 3: Exception classes end with Exception/Error
# ============================================================
class TestExceptionNaming:
    """Custom exception classes must end with Exception or Error.
    FIX: Rename the class to end with 'Exception' or 'Error'."""

    @pytest.mark.parametrize("filepath", list(_get_py_files(METAFLOW_ROOT)),
                             ids=lambda p: os.path.relpath(p, METAFLOW_ROOT))
    def test_exception_class_names(self, filepath):
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                tree = ast.parse(f.read(), filename=filepath)
        except SyntaxError:
            return

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    base_name = ""
                    if isinstance(base, ast.Name):
                        base_name = base.id
                    elif isinstance(base, ast.Attribute):
                        base_name = base.attr

                    if "Exception" in base_name or "Error" in base_name:
                        # Allow Metaflow convention: MetaflowS3NotFound, etc.
                        # The class name must contain Exception/Error OR
                        # inherit from a Metaflow*Exception class (transitive)
                        has_exception_marker = (
                            "Exception" in node.name
                            or "Error" in node.name
                            or "Metaflow" in base_name  # Metaflow exception hierarchy
                        )
                        assert has_exception_marker, (
                            "%s:%d — Class '%s' inherits from '%s' but doesn't "
                            "contain 'Exception' or 'Error' in its name. "
                            "FIX: Rename to '%sException' or '%sError'."
                            % (os.path.relpath(filepath, METAFLOW_ROOT),
                               node.lineno, node.name, base_name, node.name, node.name)
                        )


# ============================================================
# Test 4: Provider pattern — cross-cutting through factories
# ============================================================
class TestProviderPattern:
    """Datastore and metadata implementations should be accessed through factories,
    not imported directly outside of the factory module itself.
    FIX: Import from metaflow.datastore (get_datastore) or
    metaflow.plugins.metadata_providers (get_metadata_provider) instead."""

    FORBIDDEN_DIRECT_IMPORTS = {
        "metaflow.datastore.local": "metaflow.datastore.get_datastore",
        "metaflow.datastore.s3": "metaflow.datastore.get_datastore",
    }

    # Files that are allowed to do direct imports (the factories themselves, tests)
    EXEMPT_PATTERNS = [
        "datastore/__init__.py",
        "datastore/local.py",
        "datastore/s3.py",
        "metadata_providers/__init__.py",
        "metadata_providers/local.py",
        "metadata_providers/service.py",
        "client/__init__.py",  # client needs direct access for bootstrapping
    ]

    @pytest.mark.parametrize("filepath", list(_get_py_files(METAFLOW_ROOT)),
                             ids=lambda p: os.path.relpath(p, METAFLOW_ROOT))
    def test_no_direct_implementation_import(self, filepath):
        rel = os.path.relpath(filepath, METAFLOW_ROOT)
        for pattern in self.EXEMPT_PATTERNS:
            if rel.endswith(pattern):
                return

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                tree = ast.parse(f.read(), filename=filepath)
        except SyntaxError:
            return

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                for forbidden, use_instead in self.FORBIDDEN_DIRECT_IMPORTS.items():
                    if node.module == forbidden or node.module.startswith(forbidden + "."):
                        assert False, (
                            "%s:%d — Direct import of '%s' bypasses the provider pattern. "
                            "FIX: Use '%s()' instead."
                            % (rel, node.lineno, node.module, use_instead)
                        )


# ============================================================
# Test 5: AGENTS.md progressive disclosure
# ============================================================
class TestAgentLegibility:
    """Key directories must have AGENTS.md for agent progressive disclosure.
    FIX: Create AGENTS.md with directory purpose, contents, and dependency rules."""

    REQUIRED_DIRS = [
        os.path.join(PROJECT_ROOT, "metaflow"),
        os.path.join(PROJECT_ROOT, "metaflow", "plugins"),
        os.path.join(PROJECT_ROOT, "test"),
    ]

    @pytest.mark.parametrize("directory", REQUIRED_DIRS,
                             ids=lambda p: os.path.relpath(p, PROJECT_ROOT))
    def test_agents_md_exists(self, directory):
        agents_path = os.path.join(directory, "AGENTS.md")
        assert os.path.exists(agents_path), (
            "Missing AGENTS.md in %s/. "
            "FIX: Create %s with directory purpose, module listing, "
            "and dependency rules." % (
                os.path.relpath(directory, PROJECT_ROOT),
                os.path.relpath(agents_path, PROJECT_ROOT),
            )
        )


# ============================================================
# Test 6: Golden principle — CLAUDE.md exists at project root
# ============================================================
def test_claude_md_exists():
    """Project root must have CLAUDE.md with golden principles.
    FIX: Create CLAUDE.md at project root. See existing for format."""
    assert os.path.exists(os.path.join(PROJECT_ROOT, "CLAUDE.md")), (
        "Missing CLAUDE.md at project root. "
        "FIX: Create CLAUDE.md with architecture overview, golden principles, "
        "and test commands."
    )


# ============================================================
# Test 7: Config variables follow METAFLOW_ convention
# ============================================================
def test_config_env_vars_prefixed():
    """All environment variable lookups in metaflow_config.py must use METAFLOW_ prefix.
    FIX: Rename the environment variable to start with METAFLOW_."""
    config_path = os.path.join(METAFLOW_ROOT, "metaflow_config.py")
    with open(config_path, "r") as f:
        source = f.read()

    # Find all os.environ.get() calls
    for match in re.finditer(r'os\.environ\.get\(["\']([^"\']+)', source):
        var_name = match.group(1)
        if not var_name.startswith("METAFLOW_"):
            # AWS_* and standard env vars are ok for credential passthrough
            if not var_name.startswith(("AWS_", "HOME", "USER", "PATH")):
                assert False, (
                    "metaflow_config.py reads env var '%s' without METAFLOW_ prefix. "
                    "FIX: Rename to 'METAFLOW_%s' or document why this is an exception."
                    % (var_name, var_name)
                )
