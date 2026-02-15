"""Metaflow code diff/patch/pull commands.

Provides utilities to compare the current code with the code stored
in a previous run's code package.
"""

import os
import sys
import shutil
import tempfile
from subprocess import run, PIPE

from metaflow.client import Run


def extract_code_package(runspec):
    """Extract a run's code package into a temporary directory.

    Returns a TemporaryDirectory whose .name is the extraction path.
    """
    r = Run(runspec, _namespace_check=False)
    tmp = tempfile.TemporaryDirectory()
    members = r.code.tarball.getmembers()
    r.code.tarball.extractall(tmp.name, members=members)
    return tmp


def perform_diff(source_dir, target_dir=None, output=False):
    """Run git diff between source_dir and target_dir (defaults to cwd).

    Parameters
    ----------
    source_dir : str
        Directory containing the code from a run's code package.
    target_dir : str, optional
        Directory to diff against. Defaults to current working directory.
    output : bool
        If True, capture and return the diff output (no pager).
        If False, pipe through less -R for interactive viewing.

    Returns
    -------
    list of str or None
        Diff output lines if output=True, else None.
    """
    if target_dir is None:
        target_dir = os.getcwd()

    # Build file list from target
    files = []
    for root, dirs, filenames in os.walk(target_dir):
        for fname in filenames:
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, target_dir)
            files.append(rel)

    results = []
    for rel_path in sorted(files):
        source_file = os.path.join(source_dir, rel_path)

        is_tty = sys.stdout.isatty() if not output else False
        color_flag = "--color" if is_tty else "--no-color"

        cmd = [
            "git", "diff", "--no-index", "--exit-code",
            color_flag,
            "./%s" % rel_path,
            source_file,
        ]

        result = run(cmd, text=True, stdout=PIPE, cwd=target_dir)

        if result.returncode != 0 and result.stdout:
            if output:
                results.append(result.stdout)
            else:
                # Pipe through less for interactive viewing
                run(["less", "-R"], input=result.stdout, text=True)

    if output:
        return results
    return None


def run_op(runspec, op_func, *args, **kwargs):
    """Extract a code package and run an operation on it."""
    tmp = extract_code_package(runspec)
    try:
        op_func(tmp.name, *args, **kwargs)
    finally:
        shutil.rmtree(tmp.name)


def op_diff(source_dir, target_dir=None):
    """Show diff between extracted code and current directory."""
    perform_diff(source_dir, target_dir)


def op_patch(source_dir, patch_file):
    """Generate a patch file from diff output."""
    diff_output = perform_diff(source_dir, output=True)
    with open(patch_file, "w") as f:
        for chunk in diff_output:
            f.write(chunk)


def op_pull(source_dir, target_dir=None):
    """Copy extracted code to target directory, overwriting existing files."""
    if target_dir is None:
        target_dir = os.getcwd()

    for root, dirs, filenames in os.walk(source_dir):
        for fname in filenames:
            src = os.path.join(root, fname)
            rel = os.path.relpath(src, source_dir)
            dst = os.path.join(target_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
