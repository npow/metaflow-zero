"""S3 operation utilities.

Provides helper functions for S3 operations including error conversion
and local path generation for downloaded files.
"""

import re
from hashlib import sha1

from metaflow.util import url_quote


# Maximum filename length on most filesystems
MAX_FILENAME_LENGTH = 255


class _FakeClientError(Exception):
    """Mimics botocore.exceptions.ClientError structure."""

    def __init__(self, response, operation_name):
        self.response = response
        self.operation_name = operation_name
        super().__init__(str(response))


def convert_to_client_error(error_string):
    """Convert an S3 error string to a ClientError-like object.

    Parses strings like:
    'boto3.exceptions.S3UploadFailedError: Failed to upload ... An error occurred
    (SlowDown) when calling the CompleteMultipartUpload operation (reached max
    retries: 4): Please reduce your request rate.'

    Returns an object with .response["Error"]["Code"], .response["Error"]["Message"],
    and .operation_name attributes.
    """
    # Extract error code from (ErrorCode)
    code_match = re.search(r"An error occurred \(([^)]+)\)", error_string)
    error_code = code_match.group(1) if code_match else "Unknown"

    # Extract operation name from "calling the X operation"
    op_match = re.search(r"when calling the (\S+) operation", error_string)
    operation_name = op_match.group(1) if op_match else "Unknown"

    # Extract message after the last ": "
    msg_match = re.search(r"\): (.+)$", error_string)
    message = msg_match.group(1) if msg_match else error_string

    try:
        from botocore.exceptions import ClientError

        response = {
            "Error": {
                "Code": error_code,
                "Message": message,
            }
        }
        return ClientError(response, operation_name)
    except ImportError:
        response = {
            "Error": {
                "Code": error_code,
                "Message": message,
            }
        }
        return _FakeClientError(response, operation_name)


def generate_local_path(url, range=None, suffix=None):
    """Generate a local filesystem path for a downloaded S3 object.

    The path is constructed as: {sha1_hash}-{encoded_filename}-{range}[-{suffix}]

    The total path is truncated to MAX_FILENAME_LENGTH (255) characters.
    Truncated filenames include a '...' indicator.

    Parameters
    ----------
    url : str
        The S3 URL.
    range : str, optional
        Range descriptor (e.g., 'bytes=0-1000' or 'whole').
    suffix : str, optional
        Additional suffix (e.g., 'info', 'meta').

    Returns
    -------
    str
        A filesystem-safe filename of at most 255 characters.
    """
    quoted = url_quote(url)
    # Extract just the filename portion from the URL
    fname = quoted.split(b"/")[-1].replace(b".", b"_").replace(b"-", b"_")
    sha = sha1(quoted).hexdigest()

    try:
        fname_decoded = fname.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        fname_decoded = str(fname)

    range_str = range if range else "whole"

    parts = [sha, fname_decoded, range_str]
    if suffix:
        parts.append(suffix)

    result = "-".join(parts)

    if len(result) > MAX_FILENAME_LENGTH:
        # Truncate the filename portion to fit, keeping hash and range
        overhead = len(sha) + 1 + len(range_str) + 1 + 3  # 3 for '...'
        if suffix:
            overhead += len(suffix) + 1
        max_fname = MAX_FILENAME_LENGTH - overhead
        if max_fname < 1:
            max_fname = 1
        truncated_fname = fname_decoded[:max_fname] + "..."
        parts_trunc = [sha, truncated_fname, range_str]
        if suffix:
            parts_trunc.append(suffix)
        result = "-".join(parts_trunc)

    return result[:MAX_FILENAME_LENGTH]
