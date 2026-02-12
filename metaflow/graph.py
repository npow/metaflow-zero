"""Flow graph analysis: DAGNode and FlowGraph."""

import ast
import inspect
import textwrap


class DAGNode:
    """Represents a node in the flow DAG."""

    def __init__(self, name, func=None):
        self.name = name
        self.func = func
        self.type = "linear"  # linear, split-and, foreach, join, start, end, split-or
        self.in_funcs = []
        self.out_funcs = []
        self.parallel_step = False
        self.num_parallel = 0
        self.foreach_param = None
        self.condition = None
        self.matching_join = None
        self._decorators = []

    def __repr__(self):
        return "DAGNode(%s, type=%s)" % (self.name, self.type)


class FlowGraph:
    """Builds a DAG from step methods in a FlowSpec class."""

    def __init__(self, flow_cls):
        self.flow_cls = flow_cls
        self.name = flow_cls.__name__
        self._nodes = {}
        self._steps = []
        self._build_graph()

    def _build_graph(self):
        """Build the DAG by parsing step methods."""
        # Collect all step methods
        for attr_name in dir(self.flow_cls):
            obj = getattr(self.flow_cls, attr_name, None)
            if obj is not None and callable(obj) and getattr(obj, "_is_step", False):
                node = DAGNode(attr_name, obj)
                node._decorators = list(getattr(obj, "_decorators", []))
                node.parallel_step = getattr(obj, "_parallel", False)
                self._nodes[attr_name] = node
                self._steps.append(attr_name)

        if "start" not in self._nodes:
            from .exception import MetaflowException
            raise MetaflowException("Flow must have a 'start' step")
        if "end" not in self._nodes:
            from .exception import MetaflowException
            raise MetaflowException("Flow must have an 'end' step")

        # Set start/end types
        self._nodes["start"].type = "start"
        self._nodes["end"].type = "end"

        # Parse transitions from source code
        self._parse_transitions()

    def _parse_transitions(self):
        """Parse self.next() calls from source to determine transitions."""
        for name, node in self._nodes.items():
            if name == "end":
                continue
            func = node.func
            try:
                source = inspect.getsource(func)
                source = textwrap.dedent(source)
                tree = ast.parse(source)
            except (OSError, TypeError, IndentationError):
                continue

            self._extract_next_calls(tree, node)

    def _extract_next_calls(self, tree, node):
        """Extract self.next() calls from the AST."""
        for ast_node in ast.walk(tree):
            if not isinstance(ast_node, ast.Call):
                continue
            func = ast_node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "next"
                    and isinstance(func.value, ast.Name) and func.value.id == "self"):
                continue

            targets = []
            foreach_var = None
            condition = None
            num_parallel = None
            has_dict_arg = False

            for arg in ast_node.args:
                if isinstance(arg, ast.Attribute) and isinstance(arg.value, ast.Name) and arg.value.id == "self":
                    targets.append(arg.attr)
                elif isinstance(arg, ast.Dict):
                    has_dict_arg = True
                    for val in arg.values:
                        if isinstance(val, ast.Attribute) and isinstance(val.value, ast.Name) and val.value.id == "self":
                            targets.append(val.attr)

            for kw in ast_node.keywords:
                if kw.arg == "foreach":
                    if isinstance(kw.value, ast.Constant):
                        foreach_var = kw.value.value
                elif kw.arg == "condition":
                    if isinstance(kw.value, ast.Constant):
                        condition = kw.value.value
                elif kw.arg == "num_parallel":
                    if isinstance(kw.value, ast.Constant):
                        num_parallel = kw.value.value

            node.out_funcs = targets

            # Set up incoming edges
            for t in targets:
                if t in self._nodes:
                    if node.name not in self._nodes[t].in_funcs:
                        self._nodes[t].in_funcs.append(node.name)

            # Determine node type
            if foreach_var:
                node.type = "foreach"
                node.foreach_param = foreach_var
            elif num_parallel:
                node.type = "foreach"  # Parallel splits are foreach-like for graph API
                node.num_parallel = num_parallel
                # The target step is a parallel step
                if targets and targets[0] in self._nodes:
                    self._nodes[targets[0]].parallel_step = True
                    self._nodes[targets[0]].num_parallel = num_parallel
            elif condition:
                node.type = "split-or"
                node.condition = condition
            elif len(targets) > 1 and not has_dict_arg:
                node.type = "split-and"

        # Detect joins: any node with multiple in_funcs
        for name, n in self._nodes.items():
            if len(n.in_funcs) > 1 and n.type == "linear" and name not in ("start", "end"):
                n.type = "join"

        # Detect joins by function signature: (self, inputs) means join
        for name, n in self._nodes.items():
            if n.type not in ("linear",):
                continue
            if name in ("start", "end"):
                continue
            func = n.func
            if func is not None:
                try:
                    sig = inspect.signature(func)
                    params = list(sig.parameters.keys())
                    if len(params) >= 2:  # self + inputs
                        n.type = "join"
                except (ValueError, TypeError):
                    pass

        # Set matching_join for foreach and split-and nodes
        # Must handle nesting: count depth of splits vs joins
        for name, n in self._nodes.items():
            if n.type in ("foreach", "split-and", "split-or"):
                current_name = n.out_funcs[0] if n.out_funcs else None
                # Guard against self-referencing nodes (recursive switch)
                if current_name == name:
                    current_name = n.out_funcs[1] if len(n.out_funcs) > 1 else None
                depth = 1
                visited = {name}
                while current_name and current_name not in visited:
                    visited.add(current_name)
                    cn = self._nodes.get(current_name)
                    if cn is None:
                        break
                    if cn.type in ("foreach", "split-and", "split-or"):
                        # Self-referencing split-or (recursive switch) is a loop,
                        # not a true split that needs a matching join
                        if not (cn.type == "split-or" and cn.name in cn.out_funcs):
                            depth += 1
                    elif cn.type == "join":
                        depth -= 1
                        if depth == 0:
                            n.matching_join = current_name
                            break
                    current_name = cn.out_funcs[0] if cn.out_funcs else None
                    # Skip self-references in traversal
                    if current_name and current_name in visited:
                        cn2 = self._nodes.get(current_name)
                        if cn2 and len(cn2.out_funcs) > 1:
                            for alt in cn2.out_funcs[1:]:
                                if alt not in visited:
                                    current_name = alt
                                    break

    def __iter__(self):
        """Yield DAGNode objects in topological order."""
        visited = set()
        order = []

        def visit(name):
            if name in visited:
                return
            visited.add(name)
            node = self._nodes[name]
            for out in node.out_funcs:
                if out in self._nodes:
                    visit(out)
            order.append(node)

        visit("start")
        # Add any remaining unvisited nodes
        for name in self._nodes:
            if name not in visited:
                visit(name)

        for node in reversed(order):
            yield node

    def __getitem__(self, name):
        return self._nodes[name]

    def __contains__(self, name):
        return name in self._nodes

    @property
    def nodes(self):
        return self._nodes
