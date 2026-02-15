"""Low-level Kubernetes Job/Pod CRUD operations.

Uses the kubernetes Python client for direct API interaction.
"""

import os
import json
import time
import uuid

from .kubernetes import KubernetesException


def create_k8s_job(
    flow_name,
    run_id,
    step_name,
    task_id,
    attempt,
    image,
    command,
    namespace="default",
    cpu="1",
    memory="4096",
    disk="10240",
    gpu="0",
    gpu_vendor="nvidia.com",
    environment_variables=None,
    labels=None,
    annotations=None,
    service_account=None,
    node_selector=None,
    tolerations=None,
    secrets=None,
    shared_memory=None,
):
    """Create and submit a Kubernetes Job.

    Returns the job name.
    """
    try:
        from kubernetes import client as k8s_client, config as k8s_config
    except ImportError:
        raise KubernetesException(
            "kubernetes Python package is required. Install with: pip install kubernetes"
        )

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    job_name = "metaflow-%s-%s-%s-%s" % (
        flow_name.lower().replace("_", "-")[:20],
        step_name.lower().replace("_", "-")[:20],
        task_id,
        uuid.uuid4().hex[:8],
    )

    # Build environment
    env_vars = []
    for k, v in (environment_variables or {}).items():
        env_vars.append(k8s_client.V1EnvVar(name=k, value=str(v)))

    # Resource requirements
    resources = k8s_client.V1ResourceRequirements(
        requests={
            "cpu": str(cpu),
            "memory": "%sMi" % memory,
            "ephemeral-storage": "%sMi" % disk,
        },
        limits={
            "cpu": str(cpu),
            "memory": "%sMi" % memory,
        },
    )

    if int(gpu) > 0:
        gpu_resource = "%s/gpu" % gpu_vendor
        resources.limits[gpu_resource] = str(gpu)
        resources.requests[gpu_resource] = str(gpu)

    # Volume mounts for shared memory
    volume_mounts = []
    volumes = []
    if shared_memory:
        volume_mounts.append(
            k8s_client.V1VolumeMount(name="dshm", mount_path="/dev/shm")
        )
        volumes.append(
            k8s_client.V1Volume(
                name="dshm",
                empty_dir=k8s_client.V1EmptyDirVolumeSource(
                    medium="Memory",
                    size_limit="%sMi" % shared_memory,
                ),
            )
        )

    container = k8s_client.V1Container(
        name="main",
        image=image,
        command=["/bin/bash", "-c"],
        args=[command],
        env=env_vars,
        resources=resources,
        volume_mounts=volume_mounts or None,
    )

    # Pod spec
    pod_spec = k8s_client.V1PodSpec(
        containers=[container],
        restart_policy="Never",
        service_account_name=service_account,
        volumes=volumes or None,
    )

    if node_selector:
        pod_spec.node_selector = node_selector

    if tolerations:
        pod_spec.tolerations = [
            k8s_client.V1Toleration(**t) if isinstance(t, dict) else t
            for t in tolerations
        ]

    # Job metadata
    job_labels = {
        "app": "metaflow",
        "metaflow/flow_name": flow_name,
        "metaflow/run_id": str(run_id),
        "metaflow/step_name": step_name,
        "metaflow/task_id": str(task_id),
    }
    if labels:
        job_labels.update(labels)

    job_annotations = annotations or {}

    template = k8s_client.V1PodTemplateSpec(
        metadata=k8s_client.V1ObjectMeta(labels=job_labels, annotations=job_annotations),
        spec=pod_spec,
    )

    job_spec = k8s_client.V1JobSpec(
        template=template,
        backoff_limit=0,
        ttl_seconds_after_finished=600,
    )

    job = k8s_client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client.V1ObjectMeta(
            name=job_name,
            namespace=namespace,
            labels=job_labels,
        ),
        spec=job_spec,
    )

    batch_api = k8s_client.BatchV1Api()
    batch_api.create_namespaced_job(namespace=namespace, body=job)
    return job_name


def wait_for_k8s_job(job_name, namespace="default", timeout=86400, poll_interval=5):
    """Wait for a Kubernetes job to complete.

    Returns (succeeded: bool, logs: str).
    """
    try:
        from kubernetes import client as k8s_client, config as k8s_config
    except ImportError:
        raise KubernetesException("kubernetes Python package required")

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    batch_api = k8s_client.BatchV1Api()
    core_api = k8s_client.CoreV1Api()

    start_time = time.time()
    while time.time() - start_time < timeout:
        job = batch_api.read_namespaced_job(job_name, namespace)
        status = job.status

        if status.succeeded and status.succeeded > 0:
            logs = _get_pod_logs(core_api, job_name, namespace)
            return True, logs

        if status.failed and status.failed > 0:
            logs = _get_pod_logs(core_api, job_name, namespace)
            return False, logs

        time.sleep(poll_interval)

    raise KubernetesException(
        "Job %s timed out after %d seconds" % (job_name, timeout)
    )


def delete_k8s_job(job_name, namespace="default"):
    """Delete a Kubernetes job."""
    try:
        from kubernetes import client as k8s_client, config as k8s_config
    except ImportError:
        raise KubernetesException("kubernetes Python package required")

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    batch_api = k8s_client.BatchV1Api()
    batch_api.delete_namespaced_job(
        job_name, namespace,
        body=k8s_client.V1DeleteOptions(propagation_policy="Background"),
    )


def _get_pod_logs(core_api, job_name, namespace):
    """Get logs from pods associated with a job."""
    try:
        pods = core_api.list_namespaced_pod(
            namespace,
            label_selector="job-name=%s" % job_name,
        )
        logs = []
        for pod in pods.items:
            try:
                log = core_api.read_namespaced_pod_log(pod.metadata.name, namespace)
                logs.append(log)
            except Exception:
                pass
        return "\n".join(logs)
    except Exception:
        return ""
