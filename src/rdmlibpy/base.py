from __future__ import annotations

import abc
from typing import Any, Dict, Optional

import pydantic


class ProcessBase(pydantic.BaseModel, abc.ABC):
    name: str
    version: str

    def updated(self, **config):
        _config = self.model_dump(exclude_defaults=True)
        _config.update(config)
        return self.model_validate(_config)

    @abc.abstractmethod
    def run(self, source, **kwargs) -> Any:
        pass

    def _run(self, node: ProcessNode):
        # resolve parameters;
        # each parameter, which itself represents an executable node, is
        # evaluated before the process of the current node instance is executed.
        params = node.get_params()

        if node.parent is not None:
            # run parent process
            source = node.parent.run()

            # run process & return result
            return self.run(source, **params)
        else:
            return self.run(**params)

    def preprocess(self):
        return None

    @property
    def fullname(self):
        return f'{self.name}@v{self.version}'


class ProcessNode:
    def __init__(
        self,
        parent: Optional[ProcessNode],
        runner: ProcessBase,
        params: Dict[str, ProcessParam],
    ):
        self.parent = parent
        self.runner = runner
        self.params = params

    def run(self):
        return self.runner._run(self)

    def get_param(self, key: str, default=None):
        if default is None:
            return self.params[key].get_value()
        else:
            if key in self.params:
                return self.params[key].get_value()
            else:
                return default

    def get_params(self):
        # resolve parameters;
        # Each parameter, which itself represents an executable node, is
        # evaluated before the process of the current node instance is executed.
        return {key: item.get_value() for key, item in self.params.items()}


class ProcessParam:
    def get_value(self):
        raise NotImplementedError()


class PlainProcessParam(ProcessParam):
    def __init__(self, value: Any):
        self.value = value

    def get_value(self):
        return self.value


class RunnableProcessParam:
    def __init__(self, node: ProcessNode):
        self.node = node

    def get_value(self):
        return self.node.run()
