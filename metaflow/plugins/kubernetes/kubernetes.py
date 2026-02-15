"""Kubernetes plugin core."""

from metaflow.exception import MetaflowException


class KubernetesException(MetaflowException):
    headline = "Kubernetes Error"
