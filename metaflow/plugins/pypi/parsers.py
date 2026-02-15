"""Parsers for requirements.txt, conda environment.yml, and pyproject.toml."""

import re


class ParserValueError(ValueError):
    """Raised when a parser encounters invalid content."""
    pass


def requirements_txt_parser(content):
    """Parse a requirements.txt file content into {python, packages}.

    Supports standard requirements.txt and Rye lockfiles.
    Raises ParserValueError on unsupported options like --no-index or -e (non-Rye).
    """
    packages = {}
    lines = content.strip().splitlines()

    for line in lines:
        line = line.strip()
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue
        # Skip indented comment lines (Rye lockfile annotations like "    # via ...")
        if line.lstrip().startswith("#"):
            continue

        # Rye lockfiles contain "-e file:." which should be skipped
        if line.startswith("-e file:"):
            continue

        # Reject unsupported pip options
        if line.startswith("-") or line.startswith("--"):
            raise ParserValueError(
                "Unsupported option in requirements.txt: '%s'" % line
            )

        # Parse package==version
        match = re.match(r"^([a-zA-Z0-9_.-]+)==([^\s;]+)", line)
        if match:
            packages[match.group(1)] = match.group(2)

    return {"python": None, "packages": packages}


def conda_environment_yml_parser(content):
    """Parse a conda environment.yml file content into {python, packages}.

    Handles loosely formatted YAML with '=' or '==' separators.
    """
    packages = {}
    python_version = None
    in_dependencies = False

    for line in content.strip().splitlines():
        stripped = line.strip()

        if stripped.startswith("dependencies:"):
            in_dependencies = True
            continue

        if in_dependencies:
            # End of dependencies section
            if stripped and not stripped.startswith("-") and ":" in stripped:
                in_dependencies = False
                continue

            if stripped.startswith("-"):
                # Remove the leading "- "
                dep = stripped.lstrip("- ").strip()

                # Parse "name = version" or "name=version" or "name==version"
                # Handle whitespace around separators
                match = re.match(
                    r"^([a-zA-Z0-9_.-]+)\s*={1,2}\s*(.+)$", dep
                )
                if match:
                    name = match.group(1).strip()
                    version = match.group(2).strip()
                    if name == "python":
                        python_version = version
                    else:
                        packages[name] = version

    result = {"packages": packages}
    if python_version:
        result["python"] = python_version
    else:
        result["python"] = None
    return result


def pyproject_toml_parser(content):
    """Parse a pyproject.toml file content into {python, packages}.

    Extracts dependencies from [project] section and requires-python.
    """
    packages = {}
    python_version = None

    # Parse requires-python
    match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
    if match:
        python_version = match.group(1)

    # Parse dependencies list
    dep_match = re.search(
        r"dependencies\s*=\s*\[(.*?)\]", content, re.DOTALL
    )
    if dep_match:
        deps_block = dep_match.group(1)
        for dep_line in deps_block.splitlines():
            dep_line = dep_line.strip().strip(",").strip('"').strip("'")
            if not dep_line or dep_line.startswith("#"):
                continue
            pkg_match = re.match(r"^([a-zA-Z0-9_.-]+)==([^\s;,\"']+)", dep_line)
            if pkg_match:
                packages[pkg_match.group(1)] = pkg_match.group(2)

    return {"python": python_version, "packages": packages}
