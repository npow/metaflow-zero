"""Metadata provider factory for Metaflow."""


def get_metadata_provider(provider_type=None):
    """Get a metadata provider instance by type.

    Parameters
    ----------
    provider_type : str, optional
        'local' or 'service'. Defaults to METAFLOW_DEFAULT_METADATA or 'local'.
    """
    if provider_type is None:
        from metaflow.metaflow_config import DEFAULT_METADATA
        provider_type = DEFAULT_METADATA or "local"

    if provider_type == "local":
        from .local import LocalMetadataProvider
        return LocalMetadataProvider()
    elif provider_type == "service":
        from .service import ServiceMetadataProvider
        return ServiceMetadataProvider()
    else:
        raise ValueError("Unknown metadata provider type: %s" % provider_type)
