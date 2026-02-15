"""Datastore factory for Metaflow."""


def get_datastore(ds_type=None):
    """Get a datastore instance by type.

    Parameters
    ----------
    ds_type : str, optional
        'local' or 's3'. Defaults to METAFLOW_DEFAULT_DATASTORE or 'local'.
    """
    if ds_type is None:
        from metaflow.metaflow_config import DEFAULT_DATASTORE
        ds_type = DEFAULT_DATASTORE or "local"

    if ds_type == "local":
        from .local import LocalDatastore
        return LocalDatastore()
    elif ds_type == "s3":
        from .s3 import S3Datastore
        return S3Datastore()
    else:
        raise ValueError("Unknown datastore type: %s" % ds_type)
