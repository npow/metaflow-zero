"""S3 utility functions for creating boto3 clients."""

import os


def get_s3_client():
    """Create and return an S3 client using boto3.

    Respects environment variables:
    - METAFLOW_S3_ENDPOINT_URL: Custom S3 endpoint (e.g., MinIO)
    - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY: Credentials
    - AWS_DEFAULT_REGION: Region (defaults to 'us-east-1')

    Returns
    -------
    tuple of (s3_client, s3_resource)
    """
    import boto3

    endpoint_url = os.environ.get("METAFLOW_S3_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    kwargs = {"region_name": region}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    session = boto3.Session()
    client = session.client("s3", **kwargs)
    resource = session.resource("s3", **kwargs)

    return client, resource
