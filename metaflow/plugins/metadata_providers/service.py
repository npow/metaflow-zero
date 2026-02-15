"""REST metadata service client for Metaflow.

Same interface as LocalMetadataProvider but uses the metadata service REST API.
"""

import json
import os
import time
import urllib.request
import urllib.error


class ServiceMetadataProvider:
    """Metadata provider that communicates with the Metaflow metadata service."""

    def __init__(self, service_url=None):
        self.service_url = (
            service_url
            or os.environ.get("METAFLOW_SERVICE_URL", "http://localhost:8080")
        ).rstrip("/")

    def _request(self, method, path, data=None):
        url = "%s%s" % (self.service_url, path)
        body = json.dumps(data).encode("utf-8") if data else None
        req = urllib.request.Request(
            url, data=body, method=method,
            headers={"Content-Type": "application/json"} if body else {},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
        except urllib.error.URLError:
            return None

    def new_run(self, flow_name, run_id, tags=None, sys_tags=None):
        self._request("POST", "/flows/%s/runs" % flow_name, {
            "run_number": str(run_id),
            "tags": list(tags or []),
            "system_tags": list(sys_tags or []),
        })

    def new_step(self, flow_name, run_id, step_name):
        self._request(
            "POST",
            "/flows/%s/runs/%s/steps" % (flow_name, run_id),
            {"step_name": step_name},
        )

    def new_task(self, flow_name, run_id, step_name, task_id):
        self._request(
            "POST",
            "/flows/%s/runs/%s/steps/%s/tasks" % (flow_name, run_id, step_name),
            {"task_id": str(task_id)},
        )

    def register_metadata(self, flow_name, run_id, step_name, task_id, metadata_list):
        self._request(
            "POST",
            "/flows/%s/runs/%s/steps/%s/tasks/%s/metadata"
            % (flow_name, run_id, step_name, task_id),
            metadata_list,
        )

    def done_task(self, flow_name, run_id, step_name, task_id):
        self._request(
            "POST",
            "/flows/%s/runs/%s/steps/%s/tasks/%s/heartbeat"
            % (flow_name, run_id, step_name, task_id),
            {"status": "done"},
        )

    def done_run(self, flow_name, run_id):
        self._request(
            "POST",
            "/flows/%s/runs/%s/heartbeat" % (flow_name, run_id),
            {"status": "done"},
        )

    def is_task_done(self, flow_name, run_id, step_name, task_id):
        resp = self._request(
            "GET",
            "/flows/%s/runs/%s/steps/%s/tasks/%s"
            % (flow_name, run_id, step_name, task_id),
        )
        if resp:
            return resp.get("status") == "done"
        return False

    def is_run_done(self, flow_name, run_id):
        resp = self._request("GET", "/flows/%s/runs/%s" % (flow_name, run_id))
        if resp:
            return resp.get("status") == "done"
        return False

    def get_run_ids(self, flow_name):
        resp = self._request("GET", "/flows/%s/runs" % flow_name)
        if resp and isinstance(resp, list):
            return [str(r.get("run_number", r.get("run_id", ""))) for r in resp]
        return []

    def get_step_names(self, flow_name, run_id):
        resp = self._request("GET", "/flows/%s/runs/%s/steps" % (flow_name, run_id))
        if resp and isinstance(resp, list):
            return [s.get("step_name", "") for s in resp]
        return []

    def get_task_ids(self, flow_name, run_id, step_name):
        resp = self._request(
            "GET",
            "/flows/%s/runs/%s/steps/%s/tasks" % (flow_name, run_id, step_name),
        )
        if resp and isinstance(resp, list):
            return [str(t.get("task_id", "")) for t in resp]
        return []

    def update_run_tags(self, flow_name, run_id, tags=None, sys_tags=None):
        data = {}
        if tags is not None:
            data["tags"] = list(tags)
        if sys_tags is not None:
            data["system_tags"] = list(sys_tags)
        self._request("PATCH", "/flows/%s/runs/%s" % (flow_name, run_id), data)

    def get_run_meta(self, flow_name, run_id):
        return self._request("GET", "/flows/%s/runs/%s" % (flow_name, run_id))

    def get_task_metadata(self, flow_name, run_id, step_name, task_id):
        resp = self._request(
            "GET",
            "/flows/%s/runs/%s/steps/%s/tasks/%s/metadata"
            % (flow_name, run_id, step_name, task_id),
        )
        return resp if isinstance(resp, list) else []

    @staticmethod
    def _deduce_run_id_from_meta_dir(meta_path, sub_type):
        """Not applicable for service provider, but provided for interface compat."""
        return None
