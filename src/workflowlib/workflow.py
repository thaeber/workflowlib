from __future__ import annotations

from collections import deque
from typing import Any, Dict, Mapping, Optional, Sequence

from .base import ProcessStepBase
from .registry import get_runner

ProcessDescriptorType = Mapping[str, Any]
WorkflowDescriptorType = ProcessDescriptorType | Sequence[ProcessDescriptorType]


def run(workflow: WorkflowDescriptorType):
    return Workflow.create(workflow).run()


class Workflow:
    def __init__(self, process: ProcessNode):
        self.process = process

    def run(self):
        return self.process.run()

    @staticmethod
    def create(descriptor: WorkflowDescriptorType):
        match descriptor:
            case {**mapping}:
                return Workflow(Workflow._create_process(None, mapping))
            case [*sequence]:
                return Workflow(Workflow._create_sequence(sequence))
            case _:
                raise ValueError(
                    'The workflow descriptor must be either a mapping or a sequence of mappings.'
                )

    @staticmethod
    def _create_process(parent: Optional[ProcessNode], process: ProcessDescriptorType):
        # create a single process node from the descriptor (mapping)

        # check if the process contains a `run` element
        if 'run' not in process:
            raise ValueError(
                'The process parameters do not contain a `run` element.\n'
                + '\n'.join([f'{key}: {value}' for key, value in process.items()])
            )

        # get runner
        runner = get_runner(process['run'], **process.get('config', dict()))

        # get keyword arguments (anything except reserved keys)
        params: Dict[str, Any] = {}
        if 'params' in process:
            for key, value in process['params'].items():
                if key.startswith('$'):
                    # value itself is a process
                    params[key[1:]] = RunnableProcessParam(
                        Workflow.create(value).process
                    )
                else:
                    params[key] = PlainProcessParam(value)

        # invoke process
        return ProcessNode(parent, runner, params)

    @staticmethod
    def _create_sequence(sequence: Sequence[ProcessDescriptorType]):
        # create the process sequence and return the last process in the chain
        # (each process holds a reference to the previous/parent process in the chain)
        if not sequence:
            raise ValueError('The process sequence contains no elements.')

        seq = deque(sequence)
        process = Workflow._create_process(None, seq.popleft())
        while seq:
            process = Workflow._create_process(process, seq.popleft())
        return process


class ProcessNode:
    def __init__(
        self,
        parent: Optional[ProcessNode],
        runner: ProcessStepBase,
        params: Dict[str, ProcessParam],
    ):
        self.parent = parent
        self.runner = runner
        self.params = params

    def run(self):
        # resolve pre-process hooks (e.g. caching)
        result = self.runner.pre_process_hook()
        if result is not None:
            return result

        # resolve parameters;
        # Each parameter, which itself represents an executable node, is
        # evaluated before the process of the current node instance is executed.
        params = {key: item.get_value() for key, item in self.params.items()}

        if self.parent is not None:
            source = self.parent.run()
            return self.runner.process(source, **params)
        else:
            return self.runner.process(**params)


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
