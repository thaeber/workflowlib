from typing import Dict

from .process import ProcessBase

_registry: Dict[str, ProcessBase] = {}


def register(runner: ProcessBase):
    # check if a runner with that name is already in the registry
    if runner.fullname in _registry:
        raise ValueError(f'A runner with the name {runner.fullname} already exists')

    # add runner to registry
    _registry[runner.fullname] = runner


def get_runner(name: str, **config):
    # `updated` makes sure, that we return a new instance as to avoid
    # changing properties of registered runners
    return _registry[name].updated(**config)
