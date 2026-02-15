"""Python type stub (.pyi) generator for Metaflow.

Generates type stubs by inspecting modules and their type annotations,
handling complex generic types, TypeVars, ForwardRefs, and more.
"""

import inspect
import os
import typing


class StubGenerator:
    """Generates Python .pyi stub files from live module inspection."""

    def __init__(self, output_dir, include_generated_for=True):
        self._output_dir = output_dir
        self._include_generated_for = include_generated_for
        self._reset()

    def _reset(self):
        """Reset internal state for a new module."""
        self._typing_imports = set()
        self._typevars = set()
        self._imports = set()
        self._current_module_name = None
        self._current_name = None

    def _get_module_name_alias(self, module_name):
        """Alias metaflow_extensions.* to metaflow.mf_extensions.* for stubs."""
        if module_name and module_name.startswith("metaflow_extensions."):
            return "metaflow.mf_extensions." + module_name[len("metaflow_extensions."):]
        return module_name

    def _get_element_name_with_module(self, element):
        """Convert a type annotation element to its string representation.

        Handles: builtin types, classes, TypeVars, ForwardRefs, string annotations,
        generic aliases (List[X], Dict[K,V], etc.), Union, Optional, Callable,
        Tuple, ClassVar, Final, Literal, NewType, and deeply nested generics.
        """
        # None / NoneType
        if element is type(None):
            return "None"

        # String annotations → quoted
        if isinstance(element, str):
            return '"%s"' % element

        # ForwardRef
        if isinstance(element, typing.ForwardRef):
            arg = element.__forward_arg__
            return '"%s"' % arg

        # TypeVar
        if isinstance(element, typing.TypeVar):
            self._typevars.add(element.__name__)
            return element.__name__

        # NewType (in Python 3.10+, NewType creates a callable)
        if callable(element) and hasattr(element, "__supertype__"):
            name = getattr(element, "__name__", None) or str(element)
            self._typevars.add(name)
            return name

        # Ellipsis
        if element is Ellipsis:
            return "..."

        # Generic aliases (List[int], Dict[str, int], Optional[X], Union[X, Y], etc.)
        origin = getattr(element, "__origin__", None)
        args = getattr(element, "__args__", None)

        if origin is not None:
            # Get the name of the generic type
            origin_name = self._get_generic_origin_name(origin, element)

            if args is not None:
                # Special handling for Callable
                if origin is typing.Callable or (hasattr(origin, "__name__") and origin.__name__ == "Callable"):
                    return self._format_callable(origin_name, args)

                # Process type arguments recursively
                arg_strs = []
                for arg in args:
                    arg_strs.append(self._get_element_name_with_module(arg))

                return "%s[%s]" % (origin_name, ", ".join(arg_strs))
            else:
                return origin_name

        # Regular class/type
        if isinstance(element, type):
            return self._get_class_name(element)

        # Fallback for anything else
        return str(element)

    def _get_generic_origin_name(self, origin, element):
        """Get the proper typing.X name for a generic origin."""
        # Check for _name attribute (typing generics have this)
        name = getattr(element, "_name", None)
        if name is not None:
            return "typing.%s" % name

        # For typing special forms
        if origin is typing.Union:
            return "typing.Union"
        if origin is typing.Callable:
            return "typing.Callable"

        # ClassVar, Final have __class__._name or similar
        # Check typing special forms
        special_names = {
            typing.ClassVar: "typing.ClassVar",
            typing.Final: "typing.Final",
        }
        if origin in special_names:
            return special_names[origin]

        # For built-in origins like list, dict, set, tuple, frozenset
        origin_to_typing = {
            list: "typing.List",
            dict: "typing.Dict",
            set: "typing.Set",
            tuple: "typing.Tuple",
            frozenset: "typing.FrozenSet",
            type: "typing.Type",
        }
        if origin in origin_to_typing:
            return origin_to_typing[origin]

        # Fallback: try to get from the element itself
        if hasattr(origin, "__name__"):
            module = inspect.getmodule(origin)
            if module and module.__name__ != "builtins":
                mod_name = self._get_module_name_alias(module.__name__)
                self._typing_imports.add(mod_name)
                return "%s.%s" % (mod_name, origin.__name__)
            return origin.__name__

        return str(origin)

    def _format_callable(self, origin_name, args):
        """Format a Callable type annotation."""
        if len(args) == 2 and args[0] is Ellipsis:
            # Callable[..., ReturnType]
            ret = self._get_element_name_with_module(args[1])
            return "%s[..., %s]" % (origin_name, ret)

        # Callable[[Arg1, Arg2, ...], ReturnType]
        # In typing, args is (Arg1, Arg2, ..., ReturnType)
        # The last element is the return type
        param_args = args[:-1]
        ret_type = args[-1]

        param_strs = [self._get_element_name_with_module(a) for a in param_args]
        ret_str = self._get_element_name_with_module(ret_type)

        return "%s[[%s], %s]" % (origin_name, ", ".join(param_strs), ret_str)

    def _get_class_name(self, cls):
        """Get the fully qualified name for a class."""
        if cls.__module__ == "builtins":
            return cls.__name__

        module = inspect.getmodule(cls)
        if module is None:
            return cls.__name__

        mod_name = self._get_module_name_alias(module.__name__)
        if mod_name == self._current_module_name:
            return cls.__name__

        self._typing_imports.add(mod_name)
        return "%s.%s" % (mod_name, cls.__name__)

    def _exploit_annotation(self, annotation):
        """Convert an annotation to ': type' string, or '' if no annotation."""
        if annotation is None or annotation is inspect.Parameter.empty:
            return ""
        type_str = self._get_element_name_with_module(annotation)
        return ": %s" % type_str

    def _generate_function_stub(self, name, func):
        """Generate a stub for a function."""
        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError):
            return "def %s(*args, **kwargs): ...\n" % name

        params = []
        for param_name, param in sig.parameters.items():
            annotation = self._exploit_annotation(param.annotation)
            if param.default is not inspect.Parameter.empty:
                default_repr = repr(param.default)
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    params.append("*%s%s" % (param_name, annotation))
                elif param.kind == inspect.Parameter.VAR_KEYWORD:
                    params.append("**%s%s" % (param_name, annotation))
                else:
                    params.append("%s%s = %s" % (param_name, annotation, default_repr))
            else:
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    params.append("*%s%s" % (param_name, annotation))
                elif param.kind == inspect.Parameter.VAR_KEYWORD:
                    params.append("**%s%s" % (param_name, annotation))
                else:
                    params.append("%s%s" % (param_name, annotation))

        ret_annotation = ""
        if sig.return_annotation is not inspect.Signature.empty:
            ret_annotation = " -> %s" % self._get_element_name_with_module(
                sig.return_annotation
            )

        return "def %s(%s)%s: ...\n" % (name, ", ".join(params), ret_annotation)

    def _generate_class_stub(self, name, cls):
        """Generate a stub for a class."""
        # Determine bases
        bases = []
        for base in cls.__bases__:
            if base is object:
                continue
            base_name = self._get_class_name(base)
            bases.append(base_name)

        if bases:
            header = "class %s(%s):\n" % (name, ", ".join(bases))
        else:
            header = "class %s:\n" % name

        body_lines = []

        # Generate stubs for methods
        for attr_name in sorted(dir(cls)):
            if attr_name.startswith("__") and attr_name.endswith("__"):
                # Skip most dunder methods except __init__
                if attr_name not in ("__init__",):
                    continue

            try:
                attr = getattr(cls, attr_name)
            except AttributeError:
                continue

            if callable(attr) and not isinstance(attr, type):
                try:
                    # Check if this method is defined in this class (not inherited)
                    if attr_name in cls.__dict__:
                        stub = self._generate_function_stub(attr_name, attr)
                        body_lines.append("    %s" % stub)
                except Exception:
                    pass

        if not body_lines:
            body_lines.append("    ...\n")

        return header + "".join(body_lines)
