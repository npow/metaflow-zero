"""Argo Workflows REST client.

Communicates with the Argo server API for workflow submission, status checks, etc.
"""

import json
import os
import urllib.request
import urllib.error

from metaflow.util import to_bytes, to_unicode


class ArgoClient:
    """REST client for the Argo Workflows server."""

    def __init__(self, server_url=None, token=None, namespace="default"):
        self.server_url = (
            server_url or os.environ.get("ARGO_SERVER_URL", "http://localhost:2746")
        ).rstrip("/")
        self.token = token or os.environ.get("ARGO_TOKEN")
        self.namespace = namespace

    def _request(self, method, path, data=None):
        url = "%s%s" % (self.server_url, path)
        body = to_bytes(json.dumps(data)) if data else None
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer %s" % self.token

        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(to_unicode(resp.read()))
        except urllib.error.HTTPError as e:
            error_body = to_unicode(e.read())
            raise RuntimeError(
                "Argo API error %d: %s\n%s" % (e.code, e.reason, error_body)
            )

    def submit_workflow(self, workflow_spec):
        """Submit a workflow to Argo.

        Parameters
        ----------
        workflow_spec : dict
            The full Argo Workflow spec.

        Returns
        -------
        dict
            The created workflow response.
        """
        ns = workflow_spec.get("metadata", {}).get("namespace", self.namespace)
        return self._request(
            "POST",
            "/api/v1/workflows/%s" % ns,
            {"workflow": workflow_spec},
        )

    def get_workflow(self, name, namespace=None):
        """Get workflow status by name."""
        ns = namespace or self.namespace
        return self._request("GET", "/api/v1/workflows/%s/%s" % (ns, name))

    def list_workflows(self, namespace=None, label_selector=None):
        """List workflows."""
        ns = namespace or self.namespace
        path = "/api/v1/workflows/%s" % ns
        if label_selector:
            path += "?listOptions.labelSelector=%s" % label_selector
        return self._request("GET", path)

    def delete_workflow(self, name, namespace=None):
        """Delete a workflow."""
        ns = namespace or self.namespace
        return self._request("DELETE", "/api/v1/workflows/%s/%s" % (ns, name))

    def get_workflow_logs(self, name, namespace=None, container="main"):
        """Get logs for a workflow's pods."""
        ns = namespace or self.namespace
        return self._request(
            "GET",
            "/api/v1/workflows/%s/%s/log?logOptions.container=%s" % (ns, name, container),
        )

    def resubmit_workflow(self, name, namespace=None):
        """Resubmit a workflow."""
        ns = namespace or self.namespace
        return self._request("PUT", "/api/v1/workflows/%s/%s/resubmit" % (ns, name))

    def stop_workflow(self, name, namespace=None):
        """Stop a running workflow."""
        ns = namespace or self.namespace
        return self._request("PUT", "/api/v1/workflows/%s/%s/stop" % (ns, name))
