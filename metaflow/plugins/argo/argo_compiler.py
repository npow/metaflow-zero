"""Argo Workflows compiler for Metaflow.

Walks the FlowGraph and emits Argo Workflow YAML (DAG template).
Foreach maps to withItems, branches map to parallel tasks.
"""

import json
import os
from collections import OrderedDict

from .argo_workflows_cli import sanitize_for_argo


def compile_flow_to_argo(flow_cls, flow_graph, image, namespace="default",
                         service_account=None, s3_root=None, metadata_url=None):
    """Compile a Metaflow flow into an Argo Workflow spec.

    Parameters
    ----------
    flow_cls : type
        The FlowSpec subclass.
    flow_graph : dict
        The flow's DAG graph.
    image : str
        Docker image for running steps.
    namespace : str
        Kubernetes namespace.
    service_account : str, optional
        K8s service account.
    s3_root : str, optional
        S3 datastore root.
    metadata_url : str, optional
        Metadata service URL.

    Returns
    -------
    dict
        Argo Workflow spec as a Python dict (ready for yaml.dump).
    """
    flow_name = flow_cls.__name__
    sanitized_name = sanitize_for_argo(flow_name.lower())

    # Build DAG tasks from flow graph
    dag_tasks = []
    templates = []

    for step_name, step_info in flow_graph.items():
        task_name = sanitize_for_argo(step_name)

        # Build environment variables
        env = _build_env_vars(flow_name, step_name, s3_root, metadata_url)

        # Build step command
        command = [
            "python", "-m", "metaflow.cli", "step", step_name,
            "--run-id", "{{workflow.name}}",
            "--task-id", "{{pod.name}}",
        ]

        template = {
            "name": task_name,
            "container": {
                "image": image,
                "command": command,
                "env": env,
            },
        }

        # Add resource requirements from decorators
        decos = step_info.get("decorators", [])
        for deco in decos:
            if deco.get("name") in ("kubernetes", "resources"):
                attrs = deco.get("attributes", {})
                template["container"]["resources"] = {
                    "requests": {
                        "cpu": str(attrs.get("cpu", 1)),
                        "memory": "%sMi" % attrs.get("memory", 4096),
                    },
                    "limits": {
                        "cpu": str(attrs.get("cpu", 1)),
                        "memory": "%sMi" % attrs.get("memory", 4096),
                    },
                }
                break

        if service_account:
            template["serviceAccountName"] = service_account

        templates.append(template)

        # Build DAG task entry
        dag_task = {
            "name": task_name,
            "template": task_name,
        }

        # Add dependencies (edges in the graph)
        deps = step_info.get("in_edges", [])
        if deps:
            dag_task["dependencies"] = [sanitize_for_argo(d) for d in deps]

        # Handle foreach with withItems
        if step_info.get("type") == "foreach":
            dag_task["withItems"] = "{{tasks.%s.outputs.parameters.foreach_items}}" % (
                sanitize_for_argo(deps[0]) if deps else "start"
            )

        dag_tasks.append(dag_task)

    # Build the main DAG template
    dag_template = {
        "name": "main",
        "dag": {
            "tasks": dag_tasks,
        },
    }
    templates.insert(0, dag_template)

    # Build the full Argo Workflow
    workflow = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "generateName": "%s-" % sanitized_name,
            "namespace": namespace,
            "labels": {
                "metaflow/flow_name": flow_name,
            },
        },
        "spec": {
            "entrypoint": "main",
            "templates": templates,
        },
    }

    if service_account:
        workflow["spec"]["serviceAccountName"] = service_account

    return workflow


def _build_env_vars(flow_name, step_name, s3_root=None, metadata_url=None):
    """Build Argo container environment variable list."""
    env = [
        {"name": "METAFLOW_FLOW_NAME", "value": flow_name},
        {"name": "METAFLOW_STEP_NAME", "value": step_name},
    ]

    if s3_root:
        env.append({"name": "METAFLOW_DATASTORE_SYSROOT_S3", "value": s3_root})
        env.append({"name": "METAFLOW_DEFAULT_DATASTORE", "value": "s3"})

    if metadata_url:
        env.append({"name": "METAFLOW_SERVICE_URL", "value": metadata_url})
        env.append({"name": "METAFLOW_DEFAULT_METADATA", "value": "service"})

    # Forward AWS credentials from environment
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
                "METAFLOW_S3_ENDPOINT_URL"):
        val = os.environ.get(key)
        if val:
            env.append({"name": key, "value": val})

    return env
