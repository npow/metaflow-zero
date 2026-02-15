"""Kubernetes step decorator for Metaflow.

Marks a step for remote execution on a Kubernetes cluster.
"""

from metaflow.decorators import StepDecorator


class KubernetesDecorator(StepDecorator):
    """Execute a step on Kubernetes."""

    name = "kubernetes"
    defaults = {
        "cpu": 1,
        "memory": 4096,
        "disk": 10240,
        "image": None,
        "namespace": None,
        "service_account": None,
        "secrets": None,
        "node_selector": None,
        "tolerations": None,
        "gpu": 0,
        "gpu_vendor": "nvidia.com",
        "shared_memory": None,
        "port": None,
        "compute_pool": None,
        "hostname": None,
    }

    def step_init(self, flow, graph, step_name, decos, environment, datastore, logger):
        from metaflow.plugins.aws.aws_utils import compute_resource_attributes

        resource_defaults = {
            "cpu": str(self.defaults["cpu"]),
            "memory": str(self.defaults["memory"]),
            "disk": str(self.defaults["disk"]),
            "gpu": str(self.defaults["gpu"]),
        }

        resources_decos = [d for d in decos if d.name == "resources"]
        from collections import namedtuple
        MockDeco = namedtuple("MockDeco", ["name", "attributes"])
        current = MockDeco(self.name, self.attributes)

        computed = compute_resource_attributes(resources_decos, current, resource_defaults)
        self.attributes.update(computed)

    def task_pre_step(self, step_name, task_datastore, metadata,
                      run_id, task_id, flow, graph, retry_count, max_user_code_retries):
        pass
