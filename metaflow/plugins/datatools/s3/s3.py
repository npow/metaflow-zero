"""Metaflow S3 client.

Provides a high-level interface for interacting with S3-compatible storage.
Thread-safe — each S3 instance creates its own boto3 client.
"""

import json
import os
import random
import shutil
import tempfile
from collections import namedtuple
from hashlib import sha1
from urllib.parse import urlparse

from metaflow.exception import MetaflowException
from metaflow.util import to_bytes, to_unicode, url_quote

from .s3op import generate_local_path

# Retry configuration for transient S3 errors
S3_TRANSIENT_RETRY_COUNT = 7
S3_TRANSIENT_RETRY_CODES = frozenset(
    ["SlowDown", "RequestTimeout", "ServiceUnavailable", "InternalError"]
)

S3RangeInfo = namedtuple("S3RangeInfo", "total_size request_offset request_length")

# Metadata key for user-defined attributes
METADATA_USER_KEY = "metaflow-user-attributes"


class MetaflowS3Exception(MetaflowException):
    headline = "S3 Error"


class MetaflowS3NotFound(MetaflowS3Exception):
    headline = "S3 Object Not Found"


class MetaflowS3AccessDenied(MetaflowS3Exception):
    headline = "S3 Access Denied"


class MetaflowS3URLException(MetaflowS3Exception):
    headline = "S3 URL Error"


class MetaflowS3InvalidObject(MetaflowS3Exception):
    headline = "S3 Invalid Object"


class S3GetObject:
    """Represents an S3 GET request (input) or result (output).

    As input: url, req_offset, req_size define what to fetch.
    As output: holds downloaded data and metadata.
    """

    def __init__(self, url=None, req_offset=None, req_size=None):
        # Request fields
        self._url = url
        self._req_offset = req_offset
        self._req_size = req_size

        # Result fields
        self._key = None
        self._prefix = None
        self._path = None
        self._size = None
        self._exists = None
        self._downloaded = False
        self._has_info = False
        self._content_type = None
        self._range_info = None
        self._metadata = None
        self._encryption = None

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, val):
        self._url = val

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, val):
        self._key = val

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, val):
        self._prefix = val

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, val):
        self._path = val

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, val):
        self._size = val

    @property
    def exists(self):
        return self._exists

    @exists.setter
    def exists(self, val):
        self._exists = val

    @property
    def downloaded(self):
        return self._downloaded

    @downloaded.setter
    def downloaded(self, val):
        self._downloaded = val

    @property
    def has_info(self):
        return self._has_info

    @has_info.setter
    def has_info(self, val):
        self._has_info = val

    @property
    def content_type(self):
        return self._content_type

    @content_type.setter
    def content_type(self, val):
        self._content_type = val

    @property
    def range_info(self):
        return self._range_info

    @range_info.setter
    def range_info(self, val):
        self._range_info = val

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, val):
        self._metadata = val

    @property
    def encryption(self):
        return self._encryption

    @encryption.setter
    def encryption(self, val):
        self._encryption = val

    @property
    def blob(self):
        if self._path and os.path.exists(self._path):
            with open(self._path, "rb") as f:
                return f.read()
        return None


class S3PutObject:
    """Represents an object to upload to S3.

    Can be initialized with positional args (key, value) for tuple-like usage,
    or with keyword args for full control.
    """

    def __init__(self, key=None, value=None, path=None, content_type=None,
                 metadata=None, encryption=None):
        self.key = key
        self.value = value
        self.path = path
        self.content_type = content_type
        self.metadata = metadata
        self.encryption = encryption

    def __iter__(self):
        """Support tuple unpacking: key, value = S3PutObject(...)"""
        yield self.key
        yield self.value if self.value is not None else self.path


class S3:
    """High-level S3 client with context manager support.

    Usage:
        with S3(s3root='s3://bucket/prefix') as s3:
            obj = s3.get('key')
            s3.put('key', 'value')
    """

    def __init__(self, s3root=None, bucket=None, prefix=None, run=None,
                 inject_failure_rate=0, encryption=None, **kwargs):
        self._s3root = None
        self._bucket = bucket
        self._prefix = prefix or ""
        self._inject_failure_rate = inject_failure_rate
        self._encryption = encryption
        self._tmpdir = None
        self._client = None

        if s3root is not None:
            self._s3root = s3root.rstrip("/") if s3root else s3root
            parsed = urlparse(s3root, allow_fragments=False)
            self._bucket = parsed.netloc
            self._prefix = parsed.path.lstrip("/")
            # Remove trailing slash from prefix
            if self._prefix.endswith("/"):
                self._prefix = self._prefix[:-1]
        elif bucket is not None:
            self._bucket = bucket
            raw_prefix = prefix.lstrip("/") if prefix else ""
            if raw_prefix.endswith("/"):
                raw_prefix = raw_prefix[:-1]
            self._prefix = raw_prefix
            self._s3root = "s3://%s/%s" % (bucket, self._prefix) if self._prefix else "s3://%s" % bucket

            if run is not None:
                from metaflow.metaflow_current import current
                try:
                    flow_name = current.flow_name
                    run_id = current.run_id
                except AttributeError:
                    flow_name = getattr(run, "name", None) or getattr(run, "flow_name", None)
                    run_id = getattr(run, "id", None) or getattr(run, "run_id", None)
                if flow_name and run_id:
                    parts = [self._prefix, flow_name, run_id] if self._prefix else [flow_name, run_id]
                    self._prefix = "/".join(parts)
                    self._s3root = "s3://%s/%s" % (self._bucket, self._prefix)
        elif run is not None:
            from metaflow.metaflow_current import current
            try:
                flow_name = current.flow_name
                run_id = current.run_id
            except AttributeError:
                raise MetaflowS3URLException(
                    "Cannot determine S3 root from run object without current context."
                )
            # Need datastore root from config
            from metaflow import metaflow_config
            ds_root = getattr(metaflow_config, "DATASTORE_SYSROOT_S3", None)
            if not ds_root:
                raise MetaflowS3URLException(
                    "DATASTORE_SYSROOT_S3 not configured."
                )
            self._s3root = "%s/%s/%s" % (ds_root.rstrip("/"), flow_name, run_id)
            parsed = urlparse(self._s3root, allow_fragments=False)
            self._bucket = parsed.netloc
            self._prefix = parsed.path.lstrip("/").rstrip("/")

    def __enter__(self):
        self._tmpdir = tempfile.mkdtemp(prefix="metaflow.s3.")
        self._client = self._create_client()
        return self

    def __exit__(self, *args):
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir = None
        self._client = None

    def _create_client(self):
        import boto3
        endpoint_url = os.environ.get("METAFLOW_S3_ENDPOINT_URL")
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        kwargs = {"region_name": region}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        return boto3.client("s3", **kwargs)

    def _should_inject_failure(self):
        if self._inject_failure_rate > 0:
            return random.randint(1, 100) <= self._inject_failure_rate
        return False

    def _parse_url(self, url_or_key):
        """Parse a URL or key into (bucket, key, full_url)."""
        if isinstance(url_or_key, S3GetObject):
            url_str = url_or_key.url
            if url_str is None:
                url_str = url_or_key._url
        else:
            url_str = str(url_or_key)

        if url_str.startswith("s3://"):
            parsed = urlparse(url_str, allow_fragments=False)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            if not key:
                raise MetaflowS3URLException(
                    "S3 URL must include a path: '%s'" % url_str
                )
            return bucket, key, url_str

        if self._s3root is None:
            raise MetaflowS3URLException(
                "Cannot use relative key '%s' without s3root" % url_str
            )

        # It's a relative key
        if self._prefix:
            full_key = "%s/%s" % (self._prefix, url_str.lstrip("/"))
        else:
            full_key = url_str.lstrip("/")
        full_url = "s3://%s/%s" % (self._bucket, full_key)
        return self._bucket, full_key, full_url

    def _head_object(self, bucket, key):
        """HEAD an object, returning metadata dict or None if not found."""
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
            return resp
        except self._client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if error_code == "404" or error_code == "NoSuchKey":
                return None
            if error_code == "403" or error_code == "AccessDenied":
                raise MetaflowS3AccessDenied("Access denied for s3://%s/%s" % (bucket, key))
            raise

    def _download_object(self, bucket, key, local_path, req_offset=None, req_size=None):
        """Download an S3 object to a local file, optionally with range."""
        kwargs = {"Bucket": bucket, "Key": key}
        if req_offset is not None or req_size is not None:
            range_str = _build_range_header(req_offset, req_size)
            if range_str:
                kwargs["Range"] = range_str

        resp = self._client.get_object(**kwargs)
        with open(local_path, "wb") as f:
            for chunk in resp["Body"].iter_chunks():
                f.write(chunk)
        return resp

    def _fill_result(self, result, bucket, key, full_url, download=True,
                     return_info=True, req_offset=None, req_size=None):
        """Fill an S3GetObject with data from S3."""
        result.url = full_url
        if result.key is None:
            if self._s3root and full_url.startswith(self._s3root):
                result.key = full_url[len(self._s3root):].lstrip("/")
                result.prefix = self._s3root
            else:
                result.key = full_url
                result.prefix = None

        if not download:
            # Info only
            head = self._head_object(bucket, key)
            if head is None:
                result.exists = False
                result.downloaded = False
                result.size = None
                return result

            result.exists = True
            result.downloaded = False
            result.size = head.get("ContentLength", 0)
            if return_info:
                result.has_info = True
                ct = head.get("ContentType", "binary/octet-stream")
                result.content_type = ct
                raw_meta = head.get("Metadata", {})
                user_attrs = raw_meta.get(METADATA_USER_KEY)
                if user_attrs:
                    result.metadata = json.loads(user_attrs)
                else:
                    result.metadata = None
                enc = head.get("ServerSideEncryption")
                if enc:
                    result.encryption = enc
            return result

        # Download
        local_fname = generate_local_path(
            full_url,
            range="bytes=%s-%s" % (req_offset or 0, req_size or "")
            if (req_offset is not None or req_size is not None)
            else "whole",
        )
        local_path = os.path.join(self._tmpdir, local_fname)

        try:
            resp = self._download_object(bucket, key, local_path, req_offset, req_size)
        except Exception as e:
            error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                result.exists = False
                result.downloaded = False
                result.size = None
                return result
            if error_code in ("403", "AccessDenied"):
                raise MetaflowS3AccessDenied("Access denied: s3://%s/%s" % (bucket, key))
            raise MetaflowS3NotFound("Failed to get s3://%s/%s: %s" % (bucket, key, str(e)))

        file_size = os.path.getsize(local_path)
        result.exists = True
        result.downloaded = True
        result.path = local_path
        result.size = file_size

        # Build range info
        if req_offset is not None or req_size is not None:
            content_range = resp.get("ContentRange", "")
            total_size = _parse_content_range_total(content_range, file_size)
            real_offset = req_offset if req_offset is not None else 0
            if req_size is not None and req_size < 0:
                # Suffix range
                real_offset = total_size + req_size
                if real_offset < 0:
                    real_offset = 0
            result.range_info = S3RangeInfo(
                total_size=total_size,
                request_offset=real_offset,
                request_length=file_size,
            )
        else:
            result.range_info = S3RangeInfo(
                total_size=file_size,
                request_offset=0,
                request_length=file_size,
            )

        if return_info:
            result.has_info = True
            ct = resp.get("ContentType", "binary/octet-stream")
            result.content_type = ct
            raw_meta = resp.get("Metadata", {})
            user_attrs = raw_meta.get(METADATA_USER_KEY)
            if user_attrs:
                result.metadata = json.loads(user_attrs)
            else:
                result.metadata = None
            enc = resp.get("ServerSideEncryption")
            if enc:
                result.encryption = enc

        return result

    # --- Public API ---

    def info(self, url=None, return_missing=False):
        """Get metadata for a single S3 object without downloading."""
        if url is None:
            if self._s3root:
                url = self._s3root
            else:
                raise MetaflowS3URLException("No URL specified and no s3root set.")

        bucket, key, full_url = self._parse_url(url)
        result = S3GetObject(full_url)
        result.key = full_url[len(self._s3root):].lstrip("/") if (
            self._s3root and full_url.startswith(self._s3root)
        ) else full_url
        result.prefix = self._s3root if self._s3root else None

        self._fill_result(result, bucket, key, full_url, download=False, return_info=True)
        if not result.exists and not return_missing:
            raise MetaflowS3NotFound("Object not found: %s" % full_url)
        return result

    def info_many(self, urls, return_missing=False):
        """Get metadata for multiple S3 objects without downloading."""
        results = []
        for url in urls:
            bucket, key, full_url = self._parse_url(url)
            result = S3GetObject(full_url)
            result.key = full_url[len(self._s3root):].lstrip("/") if (
                self._s3root and full_url.startswith(self._s3root)
            ) else full_url
            result.prefix = self._s3root if self._s3root else None
            self._fill_result(result, bucket, key, full_url, download=False, return_info=True)
            if not result.exists and not return_missing:
                raise MetaflowS3NotFound("Object not found: %s" % full_url)
            results.append(result)
        return results

    def get(self, url_or_key=None, return_missing=False, return_info=False):
        """Download a single S3 object."""
        if url_or_key is None:
            if self._s3root:
                url_or_key = self._s3root
            else:
                raise MetaflowS3URLException("No URL specified and no s3root set.")

        req_offset = None
        req_size = None
        if isinstance(url_or_key, S3GetObject):
            req_offset = url_or_key._req_offset
            req_size = url_or_key._req_size

        if self._should_inject_failure():
            raise MetaflowS3Exception("Injected failure")

        bucket, key, full_url = self._parse_url(url_or_key)
        result = S3GetObject(full_url)
        result.key = full_url[len(self._s3root):].lstrip("/") if (
            self._s3root and full_url.startswith(self._s3root)
        ) else full_url
        result.prefix = self._s3root if self._s3root else None

        self._fill_result(
            result, bucket, key, full_url,
            download=True, return_info=return_info,
            req_offset=req_offset, req_size=req_size,
        )
        if not result.exists and not return_missing:
            raise MetaflowS3NotFound("Object not found: %s" % full_url)
        return result

    def get_many(self, urls_or_keys, return_missing=False, return_info=False):
        """Download multiple S3 objects."""
        results = []
        missing = []
        for url_or_key in urls_or_keys:
            req_offset = None
            req_size = None
            if isinstance(url_or_key, S3GetObject):
                req_offset = url_or_key._req_offset
                req_size = url_or_key._req_size

            bucket, key, full_url = self._parse_url(url_or_key)
            result = S3GetObject(full_url)
            result.key = full_url[len(self._s3root):].lstrip("/") if (
                self._s3root and full_url.startswith(self._s3root)
            ) else full_url
            result.prefix = self._s3root if self._s3root else None

            retries = S3_TRANSIENT_RETRY_COUNT
            while True:
                if self._should_inject_failure():
                    if retries > 0:
                        retries -= 1
                        continue
                    raise MetaflowS3Exception("Injected failure after retries exhausted")
                break

            try:
                self._fill_result(
                    result, bucket, key, full_url,
                    download=True, return_info=return_info,
                    req_offset=req_offset, req_size=req_size,
                )
            except MetaflowS3AccessDenied:
                raise
            except MetaflowS3NotFound:
                result.exists = False
                result.downloaded = False
                result.size = None

            if not result.exists:
                missing.append(full_url)
            results.append(result)

        if missing and not return_missing:
            raise MetaflowS3NotFound("Objects not found: %s" % ", ".join(missing[:5]))
        return results

    def get_all(self, return_info=False):
        """Download all objects under the current s3root prefix."""
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot get_all without s3root")

        keys = self._list_objects(self._bucket, self._prefix)
        results = []
        for key in sorted(keys):
            full_url = "s3://%s/%s" % (self._bucket, key)
            result = S3GetObject(full_url)
            result.key = key[len(self._prefix):].lstrip("/") if self._prefix else key
            result.prefix = self._s3root

            retries = S3_TRANSIENT_RETRY_COUNT
            while True:
                if self._should_inject_failure():
                    if retries > 0:
                        retries -= 1
                        continue
                    break
                break

            self._fill_result(
                result, self._bucket, key, full_url,
                download=True, return_info=return_info,
            )
            results.append(result)
        return results

    def get_recursive(self, prefixes=None):
        """Download all objects recursively under given prefixes."""
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot get_recursive without s3root")

        if prefixes is None:
            prefixes = [""]

        results = []
        for prefix in prefixes:
            if prefix:
                full_prefix = "%s/%s" % (self._prefix, prefix) if self._prefix else prefix
            else:
                full_prefix = self._prefix

            # Ensure prefix ends with '/' for proper listing
            list_prefix = full_prefix
            if list_prefix and not list_prefix.endswith("/"):
                list_prefix += "/"

            keys = self._list_objects(self._bucket, list_prefix)
            prefix_results = []
            for key in sorted(keys):
                full_url = "s3://%s/%s" % (self._bucket, key)
                result = S3GetObject(full_url)
                result.key = key[len(self._prefix):].lstrip("/") if self._prefix else key
                if prefix:
                    result.prefix = "%s/%s" % (self._s3root, prefix) if self._s3root else prefix
                else:
                    result.prefix = self._s3root

                retries = S3_TRANSIENT_RETRY_COUNT
                while True:
                    if self._should_inject_failure():
                        if retries > 0:
                            retries -= 1
                            continue
                        break
                    break

                self._fill_result(
                    result, self._bucket, key, full_url,
                    download=True, return_info=False,
                )
                prefix_results.append(result)
            results.extend(prefix_results)
        return results

    def put(self, key, value=None, overwrite=True, content_type=None,
            metadata=None, encryption=None):
        """Upload a single object to S3.

        Returns the full S3 URL of the uploaded object.
        """
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot put without s3root")

        if value is not None and not isinstance(value, (str, bytes)):
            raise MetaflowS3InvalidObject(
                "Value must be str or bytes, got %s" % type(value).__name__
            )

        full_key = "%s/%s" % (self._prefix, key) if self._prefix else key
        full_url = "s3://%s/%s" % (self._bucket, full_key)

        if not overwrite:
            head = self._head_object(self._bucket, full_key)
            if head is not None:
                return full_url

        body = to_bytes(value) if isinstance(value, (str, bytes)) else value
        if body is None:
            # value is a local file path
            if isinstance(key, str) and os.path.exists(key):
                with open(key, "rb") as f:
                    body = f.read()
            else:
                raise MetaflowS3InvalidObject("No value or valid file path provided")

        extra = {}
        enc = encryption or self._encryption
        if enc:
            extra["ServerSideEncryption"] = enc
        if content_type:
            extra["ContentType"] = content_type
        if metadata:
            extra["Metadata"] = {METADATA_USER_KEY: json.dumps(metadata)}

        self._client.put_object(
            Bucket=self._bucket, Key=full_key, Body=body, **extra
        )
        return full_url

    def put_many(self, key_value_pairs, overwrite=True):
        """Upload multiple objects from (key, value) pairs.

        Returns list of (key, url) tuples for successfully uploaded objects.
        """
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot put_many without s3root")

        results = []
        failed = []
        for item in key_value_pairs:
            if isinstance(item, S3PutObject):
                key = item.key
                value = item.value
                content_type = item.content_type
                meta = item.metadata
                enc = item.encryption
            elif isinstance(item, tuple) and len(item) == 2:
                key, value = item
                content_type = None
                meta = None
                enc = None
            else:
                raise MetaflowS3InvalidObject(
                    "Expected (key, value) tuple or S3PutObject, got %s" % type(item).__name__
                )

            if not isinstance(value, (str, bytes)):
                raise MetaflowS3InvalidObject(
                    "Value must be str or bytes, got %s" % type(value).__name__
                )

            full_key = "%s/%s" % (self._prefix, key) if self._prefix else key

            if not overwrite:
                head = self._head_object(self._bucket, full_key)
                if head is not None:
                    continue

            body = to_bytes(value)
            extra = {}
            effective_enc = enc or self._encryption
            if effective_enc:
                extra["ServerSideEncryption"] = effective_enc
            if content_type:
                extra["ContentType"] = content_type
            if meta:
                extra["Metadata"] = {METADATA_USER_KEY: json.dumps(meta)}

            retries = S3_TRANSIENT_RETRY_COUNT
            uploaded = False
            while retries >= 0:
                if self._should_inject_failure():
                    retries -= 1
                    if retries < 0:
                        failed.append(key)
                        break
                    continue
                self._client.put_object(
                    Bucket=self._bucket, Key=full_key, Body=body, **extra
                )
                uploaded = True
                break

            if uploaded:
                full_url = "s3://%s/%s" % (self._bucket, full_key)
                results.append((key, full_url))

        if failed:
            raise MetaflowS3Exception(
                "%d upload(s) failed after retries exhausted" % len(failed)
            )
        return results

    def put_files(self, put_objects, overwrite=True):
        """Upload multiple files to S3.

        Parameters
        ----------
        put_objects : iterable of S3PutObject
            Objects to upload. Each must have .key and .path.

        Returns list of (key, url) tuples.
        """
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot put_files without s3root")

        results = []
        for obj in put_objects:
            if isinstance(obj, S3PutObject):
                key = obj.key
                path = obj.path
                content_type = obj.content_type
                meta = obj.metadata
                enc = obj.encryption
            elif isinstance(obj, tuple):
                key, path = obj[:2]
                content_type = None
                meta = None
                enc = None
            else:
                raise MetaflowS3InvalidObject(
                    "Expected S3PutObject or tuple, got %s" % type(obj).__name__
                )

            if not os.path.exists(path):
                raise MetaflowS3NotFound("Local file not found: %s" % path)

            full_key = "%s/%s" % (self._prefix, key) if self._prefix else key

            if not overwrite:
                head = self._head_object(self._bucket, full_key)
                if head is not None:
                    continue

            extra = {}
            effective_enc = enc or self._encryption
            if effective_enc:
                extra["ServerSideEncryption"] = effective_enc
            if content_type:
                extra["ContentType"] = content_type
            if meta:
                extra["Metadata"] = {METADATA_USER_KEY: json.dumps(meta)}

            retries = S3_TRANSIENT_RETRY_COUNT
            while True:
                if self._should_inject_failure():
                    if retries > 0:
                        retries -= 1
                        continue
                    break
                with open(path, "rb") as f:
                    self._client.put_object(
                        Bucket=self._bucket, Key=full_key, Body=f, **extra
                    )
                break

            full_url = "s3://%s/%s" % (self._bucket, full_key)
            results.append((key, full_url))
        return results

    def list_paths(self, prefixes=None):
        """List immediate children (objects and prefixes) under s3root.

        Returns a list of S3GetObject with exists=True for leaf objects
        and exists=False for prefix (directory) entries.
        """
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot list_paths without s3root")

        if prefixes is None:
            # List immediate children of s3root
            list_prefix = self._prefix
            if list_prefix and not list_prefix.endswith("/"):
                list_prefix += "/"

            results = []
            paginator = self._client.get_paginator("list_objects_v2")

            kwargs = {"Bucket": self._bucket, "Delimiter": "/"}
            if list_prefix:
                kwargs["Prefix"] = list_prefix

            for page in paginator.paginate(**kwargs):
                # Common prefixes (directories)
                for cp in page.get("CommonPrefixes", []):
                    cp_prefix = cp["Prefix"]
                    key = cp_prefix[len(list_prefix):].rstrip("/") if list_prefix else cp_prefix.rstrip("/")
                    full_url = "s3://%s/%s" % (self._bucket, cp_prefix.rstrip("/"))
                    obj = S3GetObject(full_url)
                    obj.key = key
                    obj.prefix = self._s3root
                    obj.exists = False
                    obj.downloaded = False
                    results.append(obj)

                # Objects (leaves)
                for content in page.get("Contents", []):
                    obj_key = content["Key"]
                    key = obj_key[len(list_prefix):] if list_prefix else obj_key
                    if not key:
                        continue
                    full_url = "s3://%s/%s" % (self._bucket, obj_key)
                    obj = S3GetObject(full_url)
                    obj.key = key
                    obj.prefix = self._s3root
                    obj.exists = True
                    obj.downloaded = False
                    obj.size = content.get("Size", 0)
                    results.append(obj)
            return results
        else:
            # List under specific prefixes
            results = []
            for prefix in prefixes:
                if prefix.startswith("s3://"):
                    parsed = urlparse(prefix, allow_fragments=False)
                    bucket = parsed.netloc
                    list_prefix = parsed.path.lstrip("/")
                else:
                    bucket = self._bucket
                    list_prefix = "%s/%s" % (self._prefix, prefix) if self._prefix else prefix

                if list_prefix and not list_prefix.endswith("/"):
                    list_prefix += "/"

                paginator = self._client.get_paginator("list_objects_v2")
                kwargs = {"Bucket": bucket, "Delimiter": "/"}
                if list_prefix:
                    kwargs["Prefix"] = list_prefix

                prefix_url = "s3://%s/%s" % (bucket, list_prefix.rstrip("/")) if list_prefix else "s3://%s" % bucket

                for page in paginator.paginate(**kwargs):
                    for cp in page.get("CommonPrefixes", []):
                        cp_key = cp["Prefix"]
                        key = cp_key[len(list_prefix):].rstrip("/") if list_prefix else cp_key.rstrip("/")
                        full_url = "s3://%s/%s" % (bucket, cp_key.rstrip("/"))
                        obj = S3GetObject(full_url)
                        obj.key = key
                        obj.prefix = prefix
                        obj.exists = False
                        obj.downloaded = False
                        results.append(obj)

                    for content in page.get("Contents", []):
                        obj_key = content["Key"]
                        key = obj_key[len(list_prefix):] if list_prefix else obj_key
                        if not key:
                            continue
                        full_url = "s3://%s/%s" % (bucket, obj_key)
                        obj = S3GetObject(full_url)
                        obj.key = key
                        obj.prefix = prefix
                        obj.exists = True
                        obj.downloaded = False
                        obj.size = content.get("Size", 0)
                        results.append(obj)
            return results

    def list_recursive(self, prefixes=None):
        """List all leaf objects recursively under given prefixes."""
        if self._s3root is None:
            raise MetaflowS3URLException("Cannot list_recursive without s3root")

        if prefixes is None:
            prefixes = [""]

        results = []
        for prefix in prefixes:
            if prefix:
                full_prefix = "%s/%s" % (self._prefix, prefix) if self._prefix else prefix
            else:
                full_prefix = self._prefix

            # Ensure prefix ends with '/' for proper listing
            list_prefix = full_prefix
            if list_prefix and not list_prefix.endswith("/"):
                list_prefix += "/"

            keys = self._list_objects(self._bucket, list_prefix)
            for key in sorted(keys):
                full_url = "s3://%s/%s" % (self._bucket, key)
                obj = S3GetObject(full_url)
                obj.key = key[len(self._prefix):].lstrip("/") if self._prefix else key
                if prefix:
                    obj.prefix = "%s/%s" % (self._s3root, prefix) if self._s3root else prefix
                else:
                    obj.prefix = self._s3root
                obj.exists = True
                obj.downloaded = False
                results.append(obj)
        return results

    def _list_objects(self, bucket, prefix):
        """List all object keys under a prefix."""
        keys = []
        paginator = self._client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        for page in paginator.paginate(**kwargs):
            for content in page.get("Contents", []):
                keys.append(content["Key"])
        return keys


def _build_range_header(offset, size):
    """Build an HTTP Range header value."""
    if offset is None and size is None:
        return None
    if size is not None and size < 0:
        # Suffix range: last N bytes
        return "bytes=%d" % size
    start = offset if offset is not None else 0
    if size is not None:
        end = start + size - 1
        return "bytes=%d-%d" % (start, end)
    return "bytes=%d-" % start


def _parse_content_range_total(content_range, fallback_size):
    """Parse total size from Content-Range header."""
    if content_range:
        # Format: bytes 0-999/8000
        parts = content_range.split("/")
        if len(parts) == 2 and parts[1] != "*":
            try:
                return int(parts[1])
            except ValueError:
                pass
    return fallback_size
