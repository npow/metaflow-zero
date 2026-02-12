"""Minimal extension loading system for metaflow_extensions packages."""

import importlib
import importlib.util
import os
import pkgutil
import sys


def _discover_extensions():
    """Discover all metaflow_extensions packages on the Python path.
    Handles namespace packages (no __init__.py)."""
    orgs = []
    # Search sys.path for metaflow_extensions directories
    ext_paths = []
    for p in sys.path:
        ext_dir = os.path.join(p, "metaflow_extensions")
        if os.path.isdir(ext_dir):
            ext_paths.append(ext_dir)

    for ext_dir in ext_paths:
        for entry in os.listdir(ext_dir):
            if entry.startswith("_") or entry.startswith("."):
                continue
            full = os.path.join(ext_dir, entry)
            if os.path.isdir(full) and entry not in orgs:
                orgs.append(entry)
    return orgs


def _load_mfextinit(org, category):
    """Load the mfextinit_<org>.py file for a given category.
    Handles namespace packages by finding the file on disk.
    Returns the module or None."""
    mod_name = "metaflow_extensions.%s.%s.mfextinit_%s" % (org, category, org)
    try:
        return importlib.import_module(mod_name)
    except (ImportError, ModuleNotFoundError):
        pass
    # Fallback: find the file on sys.path and load it directly
    for p in sys.path:
        filepath = os.path.join(p, "metaflow_extensions", org, category,
                                "mfextinit_%s.py" % org)
        if os.path.isfile(filepath):
            spec = importlib.util.spec_from_file_location(mod_name, filepath,
                                                           submodule_search_locations=[
                                                               os.path.dirname(filepath)])
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                sys.modules[mod_name] = mod
                # Also register parent namespace packages
                _ensure_namespace_package("metaflow_extensions")
                _ensure_namespace_package("metaflow_extensions.%s" % org,
                                          os.path.join(p, "metaflow_extensions", org))
                _ensure_namespace_package("metaflow_extensions.%s.%s" % (org, category),
                                          os.path.join(p, "metaflow_extensions", org, category))
                try:
                    spec.loader.exec_module(mod)
                    return mod
                except Exception:
                    del sys.modules[mod_name]
    return None


def _ensure_namespace_package(name, path=None):
    """Register a namespace package if it doesn't exist."""
    if name not in sys.modules:
        import types
        mod = types.ModuleType(name)
        mod.__path__ = [path] if path else []
        mod.__package__ = name
        sys.modules[name] = mod
    elif path and hasattr(sys.modules[name], "__path__"):
        if path not in sys.modules[name].__path__:
            sys.modules[name].__path__.append(path)


def load_config_extensions():
    """Load config values from all extensions. Returns dict of values."""
    result = {}
    for org in _discover_extensions():
        mod = _load_mfextinit(org, "config")
        if mod:
            for attr in dir(mod):
                if not attr.startswith("_"):
                    result[attr] = getattr(mod, attr)
    return result


def load_exception_extensions():
    """Load exception classes from all extensions. Returns dict of name->class."""
    result = {}
    for org in _discover_extensions():
        mod = _load_mfextinit(org, "exceptions")
        if mod:
            for attr in dir(mod):
                if not attr.startswith("_"):
                    obj = getattr(mod, attr)
                    if isinstance(obj, type) and issubclass(obj, Exception):
                        result[attr] = obj
    return result


def load_plugin_extensions():
    """Load plugin registration info from all extensions.
    Returns (step_decos, flow_decos, promoted_submodules, all_org_plugin_modules)."""
    step_decos = []
    flow_decos = []
    promoted = []
    plugin_modules = []
    for org in _discover_extensions():
        mod = _load_mfextinit(org, "plugins")
        if mod:
            step_decos.extend(getattr(mod, "STEP_DECORATORS_DESC", []))
            flow_decos.extend(getattr(mod, "FLOW_DECORATORS_DESC", []))
            promoted.extend(getattr(mod, "__mf_promote_submodules__", []))
            plugin_modules.append(("metaflow_extensions.%s.plugins" % org, mod))
    return step_decos, flow_decos, promoted, plugin_modules


def load_toplevel_extensions():
    """Load toplevel extension values. Returns dict of name->value."""
    result = {}
    for org in _discover_extensions():
        mod = _load_mfextinit(org, "toplevel")
        if mod:
            toplevel_name = getattr(mod, "toplevel", None)
            if toplevel_name:
                # Import the actual toplevel module
                tl_mod_name = "metaflow_extensions.%s.toplevel.%s" % (org, toplevel_name)
                try:
                    tl_mod = importlib.import_module(tl_mod_name)
                    for attr in dir(tl_mod):
                        if not attr.startswith("_"):
                            result[attr] = getattr(tl_mod, attr)
                except ImportError:
                    pass
    return result


def load_card_extensions():
    """Discover and load card types from metaflow_extensions.*/plugins/cards/.
    Returns list of card classes."""
    card_classes = []
    for org in _discover_extensions():
        # Try multiple discovery patterns for cards
        cards_pkg = "metaflow_extensions.%s.plugins.cards" % org

        # Pattern 1: __init__.py has CARDS list
        try:
            mod = importlib.import_module(cards_pkg)
            if hasattr(mod, "CARDS"):
                card_classes.extend(mod.CARDS)
                continue
        except (ImportError, ModuleNotFoundError):
            pass

        # Pattern 2: mfextinit_*.py in cards directory
        for p in sys.path:
            cards_dir = os.path.join(p, "metaflow_extensions", org, "plugins", "cards")
            if not os.path.isdir(cards_dir):
                continue

            for fname in os.listdir(cards_dir):
                if fname.startswith("mfextinit_") and fname.endswith(".py"):
                    mod_name = cards_pkg + "." + fname[:-3]
                    # Ensure parent namespace packages are registered
                    _ensure_namespace_package("metaflow_extensions")
                    _ensure_namespace_package("metaflow_extensions.%s" % org,
                                              os.path.join(p, "metaflow_extensions", org))
                    _ensure_namespace_package("metaflow_extensions.%s.plugins" % org,
                                              os.path.join(p, "metaflow_extensions", org, "plugins"))
                    _ensure_namespace_package(cards_pkg, cards_dir)
                    # Also register subpackages used by mfextinit
                    for subdir in os.listdir(cards_dir):
                        subdir_path = os.path.join(cards_dir, subdir)
                        if os.path.isdir(subdir_path) and not subdir.startswith("_"):
                            sub_pkg = cards_pkg + "." + subdir
                            _ensure_namespace_package(sub_pkg, subdir_path)
                    try:
                        fpath = os.path.join(cards_dir, fname)
                        spec = importlib.util.spec_from_file_location(
                            mod_name, fpath,
                            submodule_search_locations=[cards_dir])
                        if spec and spec.loader:
                            mod = importlib.util.module_from_spec(spec)
                            sys.modules[mod_name] = mod
                            spec.loader.exec_module(mod)
                            if hasattr(mod, "CARDS"):
                                card_classes.extend(mod.CARDS)
                    except Exception:
                        pass

            # Pattern 3: subdirectories with __init__.py
            for subdir in os.listdir(cards_dir):
                subdir_path = os.path.join(cards_dir, subdir)
                if not os.path.isdir(subdir_path) or subdir.startswith("_"):
                    continue
                init_file = os.path.join(subdir_path, "__init__.py")
                if not os.path.isfile(init_file):
                    continue
                mod_name = cards_pkg + "." + subdir
                _ensure_namespace_package("metaflow_extensions")
                _ensure_namespace_package("metaflow_extensions.%s" % org,
                                          os.path.join(p, "metaflow_extensions", org))
                _ensure_namespace_package("metaflow_extensions.%s.plugins" % org,
                                          os.path.join(p, "metaflow_extensions", org, "plugins"))
                _ensure_namespace_package(cards_pkg, cards_dir)
                try:
                    spec = importlib.util.spec_from_file_location(
                        mod_name, init_file,
                        submodule_search_locations=[subdir_path])
                    if spec and spec.loader:
                        mod = importlib.util.module_from_spec(spec)
                        sys.modules[mod_name] = mod
                        spec.loader.exec_module(mod)
                        if hasattr(mod, "CARDS"):
                            card_classes.extend(mod.CARDS)
                except Exception:
                    pass
            break  # found cards dir for this org

    return card_classes


def resolve_decorator_class(base_module, import_path):
    """Resolve a decorator class from a relative import path like '.test_step_decorator.TestStepDecorator'."""
    parts = import_path.lstrip(".").split(".")
    class_name = parts[-1]
    module_path = ".".join(parts[:-1])
    full_path = base_module + "." + module_path
    try:
        mod = importlib.import_module(full_path)
        return getattr(mod, class_name, None)
    except ImportError:
        return None


def promote_submodules(org_plugin_modules, promoted_names):
    """Make promoted submodules accessible as metaflow.plugins.<name>."""
    import metaflow.plugins as plugins_pkg
    for base_module, mod in org_plugin_modules:
        for name in promoted_names:
            sub_mod_name = "%s.%s" % (base_module, name)
            try:
                sub_mod = importlib.import_module(sub_mod_name)
                # Register under metaflow.plugins.<name>
                sys.modules["metaflow.plugins.%s" % name] = sub_mod
                setattr(plugins_pkg, name, sub_mod)
            except ImportError:
                pass
        # Also promote frameworks
        fw_mod_name = "%s.frameworks" % base_module
        try:
            fw_mod = importlib.import_module(fw_mod_name)
            # Merge the original frameworks dir so _orig subpackage stays accessible
            orig_fw_dir = os.path.join(os.path.dirname(plugins_pkg.__file__), "frameworks")
            if os.path.isdir(orig_fw_dir) and hasattr(fw_mod, "__path__"):
                if orig_fw_dir not in fw_mod.__path__:
                    fw_mod.__path__.append(orig_fw_dir)
            sys.modules["metaflow.plugins.frameworks"] = fw_mod
            setattr(plugins_pkg, "frameworks", fw_mod)
            # Also register sub-packages of frameworks
            if hasattr(fw_mod, "__path__"):
                for _, subname, _ in pkgutil.iter_modules(fw_mod.__path__):
                    sub = importlib.import_module("%s.%s" % (fw_mod_name, subname))
                    sys.modules["metaflow.plugins.frameworks.%s" % subname] = sub
        except ImportError:
            pass
