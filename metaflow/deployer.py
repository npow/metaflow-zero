"""Metaflow Deployer API.

Provides programmatic deployment of Metaflow flows to various execution backends.
"""

import os
import subprocess
import sys


class Deployer:
    """Deploy Metaflow flows to production backends.

    Usage:
        deployer = Deployer(flow_file='my_flow.py')
        deployment = deployer.argo_workflows()
        deployment.create()
        deployment.trigger()
    """

    def __init__(self, flow_file, **kwargs):
        self.flow_file = os.path.abspath(flow_file)
        self.kwargs = kwargs

    def argo_workflows(self, **kwargs):
        """Get an ArgoWorkflowsDeployment handle."""
        merged = {**self.kwargs, **kwargs}
        return ArgoWorkflowsDeployment(self.flow_file, **merged)

    def kubernetes(self, **kwargs):
        """Get a KubernetesDeployment handle."""
        merged = {**self.kwargs, **kwargs}
        return KubernetesDeployment(self.flow_file, **merged)


class ArgoWorkflowsDeployment:
    """Deployment handle for Argo Workflows."""

    def __init__(self, flow_file, **kwargs):
        self.flow_file = flow_file
        self.kwargs = kwargs
        self._workflow = None

    def create(self, **kwargs):
        """Compile and register the flow as an Argo Workflow."""
        merged = {**self.kwargs, **kwargs}
        result = self._run_command("argo-workflows", "create", **merged)
        return result

    def trigger(self, **kwargs):
        """Trigger execution of the deployed workflow."""
        merged = {**self.kwargs, **kwargs}
        result = self._run_command("argo-workflows", "trigger", **merged)
        return result

    def delete(self, **kwargs):
        """Delete the deployed workflow."""
        merged = {**self.kwargs, **kwargs}
        return self._run_command("argo-workflows", "delete", **merged)

    def status(self, **kwargs):
        """Get the status of the deployed workflow."""
        merged = {**self.kwargs, **kwargs}
        return self._run_command("argo-workflows", "status", **merged)

    def _run_command(self, *args, **kwargs):
        cmd = [sys.executable, self.flow_file] + list(args)
        for k, v in kwargs.items():
            if v is not None:
                cmd.extend(["--%s" % k.replace("_", "-"), str(v)])
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            env={**os.environ, **{k: str(v) for k, v in kwargs.items() if v is not None}},
        )
        if result.returncode != 0:
            raise RuntimeError("Deployment command failed: %s\n%s" % (
                " ".join(cmd), result.stderr
            ))
        return result.stdout


class KubernetesDeployment:
    """Deployment handle for direct Kubernetes execution."""

    def __init__(self, flow_file, **kwargs):
        self.flow_file = flow_file
        self.kwargs = kwargs

    def run(self, **kwargs):
        """Run the flow on Kubernetes."""
        merged = {**self.kwargs, **kwargs}
        cmd = [sys.executable, self.flow_file, "run"]
        for k, v in merged.items():
            if v is not None:
                cmd.extend(["--%s" % k.replace("_", "-"), str(v)])
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("Kubernetes run failed: %s" % result.stderr)
        return result.stdout
