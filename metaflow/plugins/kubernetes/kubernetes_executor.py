"""Kubernetes step executor for Metaflow.

Executes a step in a Kubernetes pod by:
1. Packaging the flow code into a tarball and uploading to S3
2. Creating a K8s Job that downloads and runs the step
3. Waiting for completion and streaming logs
"""

import json
import os
import sys
import tarfile
import tempfile

from .kubernetes import KubernetesException
from .kubernetes_job import create_k8s_job, wait_for_k8s_job


def execute_step_in_kubernetes(
    flow_name,
    run_id,
    step_name,
    task_id,
    attempt,
    flow_file,
    image,
    namespace="default",
    cpu="1",
    memory="4096",
    disk="10240",
    gpu="0",
    gpu_vendor="nvidia.com",
    service_account=None,
    node_selector=None,
    tolerations=None,
    secrets=None,
    environment_variables=None,
    shared_memory=None,
    code_package_url=None,
):
    """Execute a Metaflow step in a Kubernetes pod.

    Parameters
    ----------
    flow_name : str
    run_id : str
    step_name : str
    task_id : str
    attempt : int
    flow_file : str
        Path to the flow Python file.
    image : str
        Docker image to use.
    code_package_url : str, optional
        S3 URL of the code package. If not provided, code is packaged and uploaded.
    """
    if code_package_url is None:
        code_package_url = _package_and_upload_code(flow_name, run_id, flow_file)

    # Build the command to run inside the container
    command = _build_step_command(
        flow_file=os.path.basename(flow_file),
        step_name=step_name,
        run_id=run_id,
        task_id=task_id,
        attempt=attempt,
        code_package_url=code_package_url,
    )

    # Build environment variables
    env = dict(environment_variables or {})
    env.update({
        "METAFLOW_FLOW_NAME": flow_name,
        "METAFLOW_RUN_ID": str(run_id),
        "METAFLOW_STEP_NAME": step_name,
        "METAFLOW_TASK_ID": str(task_id),
        "METAFLOW_RETRY_COUNT": str(attempt),
    })

    # Forward S3/metadata config
    for key in (
        "METAFLOW_DEFAULT_DATASTORE",
        "METAFLOW_DATASTORE_SYSROOT_S3",
        "METAFLOW_DEFAULT_METADATA",
        "METAFLOW_SERVICE_URL",
        "METAFLOW_S3_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
    ):
        val = os.environ.get(key)
        if val:
            env[key] = val

    job_name = create_k8s_job(
        flow_name=flow_name,
        run_id=run_id,
        step_name=step_name,
        task_id=task_id,
        attempt=attempt,
        image=image,
        command=command,
        namespace=namespace,
        cpu=cpu,
        memory=memory,
        disk=disk,
        gpu=gpu,
        gpu_vendor=gpu_vendor,
        environment_variables=env,
        service_account=service_account,
        node_selector=node_selector,
        tolerations=tolerations,
        secrets=secrets,
        shared_memory=shared_memory,
    )

    succeeded, logs = wait_for_k8s_job(job_name, namespace)

    if logs:
        sys.stderr.write(logs)
        sys.stderr.write("\n")

    if not succeeded:
        raise KubernetesException(
            "Step %s/%s/%s/%s failed in Kubernetes" % (flow_name, run_id, step_name, task_id)
        )


def _package_and_upload_code(flow_name, run_id, flow_file):
    """Create a tarball of the flow code and upload to S3."""
    from metaflow.plugins.datatools.s3 import S3
    from metaflow.metaflow_config import DATASTORE_SYSROOT_S3

    if not DATASTORE_SYSROOT_S3:
        raise KubernetesException("DATASTORE_SYSROOT_S3 must be configured for Kubernetes execution")

    code_root = "s3://%s/_code_packages" % DATASTORE_SYSROOT_S3.replace("s3://", "")

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        flow_dir = os.path.dirname(os.path.abspath(flow_file))
        with tarfile.open(tmp_path, "w:gz") as tar:
            for root, dirs, files in os.walk(flow_dir):
                for fname in files:
                    if fname.endswith(".py") or fname.endswith(".json"):
                        full = os.path.join(root, fname)
                        arcname = os.path.relpath(full, flow_dir)
                        tar.add(full, arcname=arcname)

        code_key = "%s/%s/code.tar.gz" % (flow_name, run_id)
        with S3(s3root=code_root) as s3:
            with open(tmp_path, "rb") as f:
                s3.put(code_key, f.read())

        return "%s/%s" % (code_root, code_key)
    finally:
        os.unlink(tmp_path)


def _build_step_command(flow_file, step_name, run_id, task_id, attempt, code_package_url):
    """Build the shell command to run inside the Kubernetes container."""
    return (
        "set -e && "
        "mkdir -p /metaflow_code && cd /metaflow_code && "
        "python -c \""
        "import boto3, os, tarfile, io; "
        "from urllib.parse import urlparse; "
        "url = urlparse('{code_url}'); "
        "s3 = boto3.client('s3', "
        "endpoint_url=os.environ.get('METAFLOW_S3_ENDPOINT_URL'), "
        "region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')); "
        "resp = s3.get_object(Bucket=url.netloc, Key=url.path.lstrip('/')); "
        "tar = tarfile.open(fileobj=io.BytesIO(resp['Body'].read())); "
        "tar.extractall('.'); tar.close()"
        "\" && "
        "python {flow_file} step {step_name} "
        "--run-id {run_id} --task-id {task_id} --retry-count {attempt}"
    ).format(
        code_url=code_package_url,
        flow_file=flow_file,
        step_name=step_name,
        run_id=run_id,
        task_id=task_id,
        attempt=attempt,
    )
