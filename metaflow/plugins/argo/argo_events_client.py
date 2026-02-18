"""Argo Events client for Metaflow.

Manages cron sensors, webhook sensors, and event triggers for
scheduled and event-driven flow execution.
"""

import json
import os
import urllib.request
import urllib.error

from metaflow.util import to_bytes, to_unicode


class ArgoEventsClient:
    """Client for managing Argo Events resources (sensors, event sources)."""

    def __init__(self, server_url=None, namespace="default"):
        self.server_url = (
            server_url or os.environ.get("ARGO_EVENTS_URL", "http://localhost:7777")
        ).rstrip("/")
        self.namespace = namespace

    def _request(self, method, path, data=None):
        url = "%s%s" % (self.server_url, path)
        body = to_bytes(json.dumps(data)) if data else None
        headers = {"Content-Type": "application/json"} if body else {}
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(to_unicode(resp.read()))
        except urllib.error.HTTPError as e:
            error_body = to_unicode(e.read())
            raise RuntimeError(
                "Argo Events API error %d: %s" % (e.code, error_body)
            )

    def create_cron_sensor(self, flow_name, cron_schedule, workflow_template_name):
        """Create a cron event source and sensor for scheduled flow execution.

        Parameters
        ----------
        flow_name : str
            Name of the flow.
        cron_schedule : str
            Cron expression (e.g., '0 9 * * *').
        workflow_template_name : str
            Name of the Argo WorkflowTemplate to trigger.
        """
        sensor_name = "metaflow-%s-cron" % flow_name.lower().replace("_", "-")

        event_source = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "EventSource",
            "metadata": {
                "name": sensor_name,
                "namespace": self.namespace,
            },
            "spec": {
                "calendar": {
                    flow_name: {
                        "schedule": cron_schedule,
                    },
                },
            },
        }

        sensor = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Sensor",
            "metadata": {
                "name": sensor_name,
                "namespace": self.namespace,
            },
            "spec": {
                "dependencies": [
                    {
                        "name": "cron-dep",
                        "eventSourceName": sensor_name,
                        "eventName": flow_name,
                    },
                ],
                "triggers": [
                    {
                        "template": {
                            "name": "trigger-%s" % flow_name,
                            "argoWorkflow": {
                                "operation": "submit",
                                "source": {
                                    "resource": {
                                        "apiVersion": "argoproj.io/v1alpha1",
                                        "kind": "Workflow",
                                        "metadata": {
                                            "generateName": "%s-" % flow_name.lower(),
                                        },
                                        "spec": {
                                            "workflowTemplateRef": {
                                                "name": workflow_template_name,
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                ],
            },
        }

        return {"event_source": event_source, "sensor": sensor}

    def create_webhook_sensor(self, flow_name, endpoint, workflow_template_name):
        """Create a webhook event source and sensor for event-driven flow execution.

        Parameters
        ----------
        flow_name : str
            Name of the flow.
        endpoint : str
            Webhook endpoint path (e.g., '/metaflow/myflow').
        workflow_template_name : str
            Name of the Argo WorkflowTemplate to trigger.
        """
        sensor_name = "metaflow-%s-webhook" % flow_name.lower().replace("_", "-")

        event_source = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "EventSource",
            "metadata": {
                "name": sensor_name,
                "namespace": self.namespace,
            },
            "spec": {
                "webhook": {
                    flow_name: {
                        "port": "12000",
                        "endpoint": endpoint,
                        "method": "POST",
                    },
                },
            },
        }

        sensor = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Sensor",
            "metadata": {
                "name": sensor_name,
                "namespace": self.namespace,
            },
            "spec": {
                "dependencies": [
                    {
                        "name": "webhook-dep",
                        "eventSourceName": sensor_name,
                        "eventName": flow_name,
                    },
                ],
                "triggers": [
                    {
                        "template": {
                            "name": "trigger-%s" % flow_name,
                            "argoWorkflow": {
                                "operation": "submit",
                                "source": {
                                    "resource": {
                                        "apiVersion": "argoproj.io/v1alpha1",
                                        "kind": "Workflow",
                                        "metadata": {
                                            "generateName": "%s-" % flow_name.lower(),
                                        },
                                        "spec": {
                                            "workflowTemplateRef": {
                                                "name": workflow_template_name,
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                ],
            },
        }

        return {"event_source": event_source, "sensor": sensor}
