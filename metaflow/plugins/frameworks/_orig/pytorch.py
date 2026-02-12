"""Stub PyTorch parallel decorator for framework extensions."""

from metaflow.decorators import StepDecorator


class PytorchParallelDecorator(StepDecorator):
    name = "pytorch_parallel"


def setup_torch_distributed():
    pass
