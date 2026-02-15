"""S3-backed datastore for Metaflow.

Same interface as LocalDatastore but stores artifacts and logs in S3.
"""

import os
import pickle
import tempfile

from metaflow.plugins.datatools.s3 import S3, MetaflowS3NotFound


class S3Datastore:
    """Store artifacts and logs in S3."""

    def __init__(self, root=None):
        self.root = root or os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3")
        if not self.root:
            raise ValueError("S3 datastore root not configured. Set METAFLOW_DATASTORE_SYSROOT_S3.")

    def _artifact_prefix(self, flow_name, run_id, step_name, task_id):
        return "%s/%s/%s/%s/artifacts" % (flow_name, str(run_id), step_name, str(task_id))

    def _artifact_key(self, flow_name, run_id, step_name, task_id, name):
        return "%s/%s.pkl" % (
            self._artifact_prefix(flow_name, run_id, step_name, task_id),
            name,
        )

    def _log_key(self, flow_name, run_id, step_name, task_id, stream):
        return "%s/%s/%s/%s/logs/%s.txt" % (
            flow_name, str(run_id), step_name, str(task_id), stream,
        )

    def clear_task_artifacts(self, flow_name, run_id, step_name, task_id):
        prefix = self._artifact_prefix(flow_name, run_id, step_name, task_id)
        with S3(s3root=self.root) as s3:
            objs = s3.list_recursive([prefix])
            for obj in objs:
                if obj.key.endswith(".pkl"):
                    # Delete by putting empty (S3 doesn't have direct delete in our API,
                    # but we can use boto3 directly)
                    pass
            # Use boto3 directly for delete
            from urllib.parse import urlparse
            parsed = urlparse(self.root)
            bucket = parsed.netloc
            root_prefix = parsed.path.lstrip("/")
            full_prefix = "%s/%s" % (root_prefix, prefix) if root_prefix else prefix

            import boto3
            endpoint_url = os.environ.get("METAFLOW_S3_ENDPOINT_URL")
            kwargs = {"region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}
            if endpoint_url:
                kwargs["endpoint_url"] = endpoint_url
            client = boto3.client("s3", **kwargs)

            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
                for content in page.get("Contents", []):
                    if content["Key"].endswith(".pkl"):
                        client.delete_object(Bucket=bucket, Key=content["Key"])

    def save_artifacts(self, flow_name, run_id, step_name, task_id, artifacts_dict):
        with S3(s3root=self.root) as s3:
            items = []
            for name, value in artifacts_dict.items():
                key = self._artifact_key(flow_name, run_id, step_name, task_id, name)
                data = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
                items.append((key, data))
            if items:
                s3.put_many(items)

    def load_artifacts(self, flow_name, run_id, step_name, task_id):
        prefix = self._artifact_prefix(flow_name, run_id, step_name, task_id)
        result = {}
        with S3(s3root=self.root) as s3:
            try:
                objs = s3.get_recursive([prefix])
            except Exception:
                return result
            for obj in objs:
                if obj.key.endswith(".pkl"):
                    name = obj.key.rsplit("/", 1)[-1][:-4]
                    result[name] = pickle.loads(obj.blob)
        return result

    def load_artifact(self, flow_name, run_id, step_name, task_id, name):
        key = self._artifact_key(flow_name, run_id, step_name, task_id, name)
        with S3(s3root=self.root) as s3:
            try:
                obj = s3.get(key)
                return pickle.loads(obj.blob)
            except MetaflowS3NotFound:
                return None

    def has_artifact(self, flow_name, run_id, step_name, task_id, name):
        key = self._artifact_key(flow_name, run_id, step_name, task_id, name)
        with S3(s3root=self.root) as s3:
            try:
                s3.info(key)
                return True
            except MetaflowS3NotFound:
                return False

    def artifact_names(self, flow_name, run_id, step_name, task_id):
        prefix = self._artifact_prefix(flow_name, run_id, step_name, task_id)
        with S3(s3root=self.root) as s3:
            try:
                objs = s3.list_recursive([prefix])
            except Exception:
                return []
            return [
                obj.key.rsplit("/", 1)[-1][:-4]
                for obj in objs
                if obj.key.endswith(".pkl")
            ]

    def save_log(self, flow_name, run_id, step_name, task_id, stream, content):
        key = self._log_key(flow_name, run_id, step_name, task_id, stream)
        with S3(s3root=self.root) as s3:
            s3.put(key, content)

    def load_log(self, flow_name, run_id, step_name, task_id, stream):
        key = self._log_key(flow_name, run_id, step_name, task_id, stream)
        with S3(s3root=self.root) as s3:
            try:
                obj = s3.get(key)
                return obj.blob.decode("utf-8")
            except MetaflowS3NotFound:
                return ""
