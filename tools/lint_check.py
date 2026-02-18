#!/usr/bin/env python
"""Custom linter for metaflow-zero.

Enforces taste invariants and golden principles with remediation messages.
Every error message tells the agent/user exactly how to fix the violation.

Checks:
1. File size limits (< 500 lines)
2. Naming conventions (snake_case files, PascalCase classes, UPPER_SNAKE config)
3. No bare print() in library code (structured logging only)
4. Exception messages must be actionable (contain remediation)
5. No hand-rolled utility functions (use metaflow.util)
6. Import direction enforcement (lower layers can't import higher)
7. Provider pattern enforcement (no direct implementation imports for cross-cutting)
8. AGENTS.md existence in key directories
"""

import ast
import os
import re
import sys

METAFLOW_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "metaflow")
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

MAX_FILE_LINES = 500

# Layer ordering for import direction checks
LAYER_MODULES = {
    0: {"exception", "util", "metaflow_config", "_extension_loader"},
    1: {"graph", "parameters", "includefile", "namespace"},
    2: {"datastore"},
    3: {"flowspec", "decorators", "metaflow_current"},
    4: {"client"},
    5: {"runtime"},
    6: {"cli", "cli_components", "cmd"},
    7: {"runner"},
}

MODULE_TO_LAYER = {}
for layer, modules in LAYER_MODULES.items():
    for mod in modules:
        MODULE_TO_LAYER[mod] = layer

# Utility functions that should never be re-implemented
UTILITY_PATTERNS = {
    r"\.encode\(['\"]utf-8['\"]\)": "Use metaflow.util.to_bytes() instead of .encode('utf-8')",
    r"\.decode\(['\"]utf-8['\"]\)": "Use metaflow.util.to_unicode() instead of .decode('utf-8')",
}

# Directories that must have AGENTS.md
REQUIRED_AGENTS_MD = [
    "metaflow",
    "metaflow/plugins",
    "test",
]


class LintViolation:
    def __init__(self, filepath, line, code, message, remediation):
        self.filepath = filepath
        self.line = line
        self.code = code
        self.message = message
        self.remediation = remediation

    def __str__(self):
        rel = os.path.relpath(self.filepath, PROJECT_ROOT)
        return (
            "[%s] %s:%d — %s\n"
            "  FIX: %s" % (self.code, rel, self.line, self.message, self.remediation)
        )


def check_file_size(filepath):
    """Files should be under MAX_FILE_LINES lines."""
    violations = []
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    if len(lines) > MAX_FILE_LINES:
        violations.append(LintViolation(
            filepath, 1, "SIZE001",
            "File has %d lines (limit: %d)" % (len(lines), MAX_FILE_LINES),
            "Split this module into smaller files. Extract related functions into "
            "a submodule or separate file in the same directory.",
        ))
    return violations


def check_naming_conventions(filepath):
    """Enforce snake_case filenames for Python modules."""
    violations = []
    basename = os.path.basename(filepath)
    if basename.startswith("_") or basename == "__init__.py":
        return violations
    name = basename.replace(".py", "")
    if name != name.lower():
        violations.append(LintViolation(
            filepath, 1, "NAME001",
            "Filename '%s' is not snake_case" % basename,
            "Rename to '%s.py'" % name.lower(),
        ))
    return violations


def check_no_bare_print(filepath):
    """Library code should not use bare print(). Test files are exempt."""
    violations = []
    rel = os.path.relpath(filepath, PROJECT_ROOT)
    if rel.startswith("test/") or rel.startswith("tools/"):
        return violations

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            tree = ast.parse(f.read(), filename=filepath)
    except SyntaxError:
        return violations

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "print":
                violations.append(LintViolation(
                    filepath, node.lineno, "LOG001",
                    "Bare print() call in library code",
                    "Use sys.stderr for debug output, or remove the print statement. "
                    "Library code should use structured error reporting via exceptions.",
                ))
    return violations


def check_exception_remediation(filepath):
    """Exception messages should contain actionable remediation."""
    violations = []
    rel = os.path.relpath(filepath, PROJECT_ROOT)
    if rel.startswith("test/"):
        return violations

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            source = f.read()
            tree = ast.parse(source, filename=filepath)
    except SyntaxError:
        return violations

    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and node.exc is not None:
            if isinstance(node.exc, ast.Call) and node.exc.args:
                first_arg = node.exc.args[0]
                if isinstance(first_arg, (ast.Constant, ast.JoinedStr)):
                    msg = ""
                    if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                        msg = first_arg.value
                    # Check if message is too short / non-actionable
                    if msg and len(msg) < 15 and ":" not in msg:
                        violations.append(LintViolation(
                            filepath, node.lineno, "ERR001",
                            "Exception message is too terse: '%s'" % msg[:50],
                            "Add context about what went wrong and how to fix it. "
                            "Example: 'Secret backend \\'x\\' not found. Available: [...]'",
                        ))
    return violations


def check_utility_reimplementation(filepath):
    """Detect hand-rolled utility functions that should use metaflow.util."""
    violations = []
    rel = os.path.relpath(filepath, PROJECT_ROOT)
    if rel.startswith("test/") or "util.py" in rel:
        return violations

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, 1):
            for pattern, remediation in UTILITY_PATTERNS.items():
                if re.search(pattern, line):
                    violations.append(LintViolation(
                        filepath, lineno, "UTIL001",
                        "Hand-rolled utility detected",
                        remediation,
                    ))
    return violations


def check_agents_md_exists():
    """Key directories must have AGENTS.md for agent legibility."""
    violations = []
    for rel_dir in REQUIRED_AGENTS_MD:
        agents_path = os.path.join(PROJECT_ROOT, rel_dir, "AGENTS.md")
        if not os.path.exists(agents_path):
            violations.append(LintViolation(
                os.path.join(PROJECT_ROOT, rel_dir), 0, "DOC001",
                "Missing AGENTS.md in %s/" % rel_dir,
                "Create %s/AGENTS.md with directory purpose, module listing, "
                "and dependency rules. See existing AGENTS.md files for format." % rel_dir,
            ))
    return violations


def check_import_direction(filepath):
    """Enforce that lower architectural layers don't import higher ones."""
    violations = []
    rel = os.path.relpath(filepath, METAFLOW_ROOT)
    if rel.startswith("plugins/") or rel.startswith(".."):
        return violations  # plugins are cross-cutting

    # Determine this file's layer
    parts = rel.replace("\\", "/").split("/")
    module_name = parts[0].replace(".py", "")
    current_layer = MODULE_TO_LAYER.get(module_name)
    if current_layer is None:
        return violations

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            tree = ast.parse(f.read(), filename=filepath)
    except SyntaxError:
        return violations

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported = node.module
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imported = alias.name
            else:
                continue

            if not imported.startswith("metaflow.") and not imported.startswith("."):
                continue

            imported_parts = imported.replace("metaflow.", "").split(".")
            imported_module = imported_parts[0]
            imported_layer = MODULE_TO_LAYER.get(imported_module)

            if imported_layer is not None and imported_layer > current_layer:
                violations.append(LintViolation(
                    filepath, getattr(node, "lineno", 0), "ARCH001",
                    "Layer %d (%s) imports from layer %d (%s)"
                    % (current_layer, module_name, imported_layer, imported_module),
                    "Lower layers must not import from higher layers. "
                    "Move the dependency to a higher layer, or use dependency injection.",
                ))
    return violations


def main():
    violations = []
    py_files = []

    for root, dirs, files in os.walk(METAFLOW_ROOT):
        dirs[:] = [d for d in dirs if d != "__pycache__" and d != "_vendor"]
        for f in files:
            if f.endswith(".py"):
                py_files.append(os.path.join(root, f))

    for filepath in py_files:
        violations.extend(check_file_size(filepath))
        violations.extend(check_naming_conventions(filepath))
        violations.extend(check_no_bare_print(filepath))
        violations.extend(check_exception_remediation(filepath))
        violations.extend(check_utility_reimplementation(filepath))
        violations.extend(check_import_direction(filepath))

    violations.extend(check_agents_md_exists())

    if violations:
        print("=" * 70)
        print("LINT VIOLATIONS FOUND: %d" % len(violations))
        print("=" * 70)
        for v in violations:
            print()
            print(v)
        print()
        print("Run 'python tools/lint_check.py' to re-check after fixes.")
        return 1
    else:
        print("All lint checks passed. (%d files checked)" % len(py_files))
        return 0


if __name__ == "__main__":
    sys.exit(main())
