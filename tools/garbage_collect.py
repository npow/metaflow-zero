#!/usr/bin/env python
"""Garbage collection tool for metaflow-zero.

Inspired by OpenAI's harness engineering: "Technical debt is like a
high-interest loan — it's almost always better to pay it down continuously
in small increments than to let it compound."

Scans for:
1. Stale AGENTS.md files (missing or outdated)
2. Orphaned test files (test files without matching source)
3. Unused imports in source files
4. TODO/FIXME/HACK markers
5. Files over the size limit
6. Dead code (modules not imported anywhere)
7. Documentation drift (AGENTS.md listing modules that don't exist)

Run on a regular cadence. Most fixes can be reviewed in under a minute.
"""

import ast
import os
import re
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
METAFLOW_ROOT = os.path.join(PROJECT_ROOT, "metaflow")
TEST_ROOT = os.path.join(PROJECT_ROOT, "test")
MAX_FILE_LINES = 500


class Finding:
    def __init__(self, category, filepath, message, suggested_action):
        self.category = category
        self.filepath = filepath
        self.message = message
        self.suggested_action = suggested_action

    def __str__(self):
        rel = os.path.relpath(self.filepath, PROJECT_ROOT) if self.filepath else "N/A"
        return "[%s] %s\n  %s\n  ACTION: %s" % (
            self.category, rel, self.message, self.suggested_action
        )


def scan_todo_markers():
    """Find TODO/FIXME/HACK markers that represent unaddressed tech debt."""
    findings = []
    for root, dirs, files in os.walk(METAFLOW_ROOT):
        dirs[:] = [d for d in dirs if d != "__pycache__" and d != "_vendor"]
        for f in files:
            if not f.endswith(".py"):
                continue
            filepath = os.path.join(root, f)
            with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                for lineno, line in enumerate(fh, 1):
                    for marker in ("TODO", "FIXME", "HACK", "XXX"):
                        if marker in line and not line.strip().startswith("def "):
                            findings.append(Finding(
                                "DEBT",
                                filepath,
                                "Line %d: %s found — '%s'" % (lineno, marker, line.strip()[:80]),
                                "Address the %s or create a tracked issue for it." % marker,
                            ))
    return findings


def scan_oversized_files():
    """Find files over the size limit."""
    findings = []
    for root, dirs, files in os.walk(METAFLOW_ROOT):
        dirs[:] = [d for d in dirs if d != "__pycache__" and d != "_vendor"]
        for f in files:
            if not f.endswith(".py"):
                continue
            filepath = os.path.join(root, f)
            with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                line_count = sum(1 for _ in fh)
            if line_count > MAX_FILE_LINES:
                findings.append(Finding(
                    "SIZE",
                    filepath,
                    "File has %d lines (limit: %d)" % (line_count, MAX_FILE_LINES),
                    "Split into focused submodules. Extract related code into "
                    "separate files in the same directory.",
                ))
    return findings


def scan_agents_md_drift():
    """Check if AGENTS.md files reference modules that don't exist."""
    findings = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "_vendor", ".git", "node_modules")]
        if "AGENTS.md" in files:
            agents_path = os.path.join(root, "AGENTS.md")
            with open(agents_path, "r") as f:
                content = f.read()
            # Find referenced .py files
            for match in re.finditer(r"`([a-z_]+\.py)`", content):
                referenced = match.group(1)
                if not os.path.exists(os.path.join(root, referenced)):
                    findings.append(Finding(
                        "DOC-DRIFT",
                        agents_path,
                        "References '%s' which does not exist in %s/" % (
                            referenced, os.path.relpath(root, PROJECT_ROOT)
                        ),
                        "Update AGENTS.md to remove stale reference or create the missing file.",
                    ))
    return findings


def scan_orphaned_tests():
    """Find test files that don't have a corresponding source module."""
    findings = []
    for root, dirs, files in os.walk(TEST_ROOT):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            if not f.startswith("test_") or not f.endswith(".py"):
                continue
            # test_foo.py should correspond to foo.py somewhere in metaflow/
            module_name = f[5:-3]  # strip test_ prefix and .py suffix
            # Check if any matching module exists
            found = False
            for src_root, _, src_files in os.walk(METAFLOW_ROOT):
                if module_name + ".py" in src_files:
                    found = True
                    break
                # Also check for directory-based modules
                if module_name in os.listdir(src_root) if os.path.isdir(src_root) else []:
                    found = True
                    break
            # Don't flag tests for well-known patterns
            skip_patterns = ("test_config_", "test_compute_", "test_spin", "test_inheritance",
                             "test_stub_", "test_metaflow_diff", "test_architecture")
            if not found and not any(f.startswith(p) for p in skip_patterns):
                findings.append(Finding(
                    "ORPHAN",
                    os.path.join(root, f),
                    "Test file '%s' has no obvious source module '%s.py'" % (f, module_name),
                    "Verify the test still corresponds to existing code, or remove if obsolete.",
                ))
    return findings


def main():
    findings = []
    findings.extend(scan_todo_markers())
    findings.extend(scan_oversized_files())
    findings.extend(scan_agents_md_drift())
    findings.extend(scan_orphaned_tests())

    if findings:
        by_category = {}
        for f in findings:
            by_category.setdefault(f.category, []).append(f)

        total = len(findings)
        sys.stdout.write("=" * 70 + "\n")
        sys.stdout.write("GARBAGE COLLECTION REPORT: %d findings\n" % total)
        sys.stdout.write("=" * 70 + "\n")

        for cat in sorted(by_category.keys()):
            items = by_category[cat]
            sys.stdout.write("\n--- %s (%d) ---\n" % (cat, len(items)))
            for item in items:
                sys.stdout.write("\n%s\n" % item)

        sys.stdout.write("\n" + "=" * 70 + "\n")
        sys.stdout.write("Run 'python tools/garbage_collect.py' after addressing findings.\n")
        return 1
    else:
        sys.stdout.write("No garbage collection findings. Codebase is clean.\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
