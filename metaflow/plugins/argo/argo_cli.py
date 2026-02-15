"""Argo Workflows CLI commands for Metaflow.

Provides CLI commands for compiling and submitting flows to Argo Workflows.
"""

import json
import os
import sys

from .argo_client import ArgoClient
from .argo_compiler import compile_flow_to_argo


def cmd_argo_workflows_create(flow_cls, flow_graph, **kwargs):
    """Compile a flow into an Argo Workflow and optionally write to file.

    Usage: python flow.py argo-workflows create [--image IMAGE] [--output FILE]
    """
    image = kwargs.get("image") or os.environ.get("METAFLOW_KUBERNETES_IMAGE", "python:3.10")
    namespace = kwargs.get("namespace", "default")
    service_account = kwargs.get("service_account")
    s3_root = kwargs.get("s3_root") or os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3")
    metadata_url = kwargs.get("metadata_url") or os.environ.get("METAFLOW_SERVICE_URL")
    output_file = kwargs.get("output")

    workflow = compile_flow_to_argo(
        flow_cls=flow_cls,
        flow_graph=flow_graph,
        image=image,
        namespace=namespace,
        service_account=service_account,
        s3_root=s3_root,
        metadata_url=metadata_url,
    )

    if output_file:
        try:
            import yaml
            with open(output_file, "w") as f:
                yaml.dump(workflow, f, default_flow_style=False)
        except ImportError:
            with open(output_file, "w") as f:
                json.dump(workflow, f, indent=2)
        print("Argo Workflow written to %s" % output_file)
    else:
        try:
            import yaml
            print(yaml.dump(workflow, default_flow_style=False))
        except ImportError:
            print(json.dumps(workflow, indent=2))

    return workflow


def cmd_argo_workflows_trigger(flow_cls, flow_graph, **kwargs):
    """Compile and submit a flow to Argo Workflows.

    Usage: python flow.py argo-workflows trigger [--image IMAGE]
    """
    workflow = cmd_argo_workflows_create(flow_cls, flow_graph, **kwargs)

    server_url = kwargs.get("server_url") or os.environ.get("ARGO_SERVER_URL")
    token = kwargs.get("token") or os.environ.get("ARGO_TOKEN")
    namespace = kwargs.get("namespace", "default")

    client = ArgoClient(server_url=server_url, token=token, namespace=namespace)

    try:
        result = client.submit_workflow(workflow)
        wf_name = result.get("metadata", {}).get("name", "unknown")
        print("Workflow submitted: %s" % wf_name)
        return result
    except Exception as e:
        print("Failed to submit workflow: %s" % str(e), file=sys.stderr)
        raise
