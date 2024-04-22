from __future__ import annotations

from collections import deque
from typing import Any, Dict, Mapping, Optional, Sequence

from . import base
from .registry import get_runner

ProcessDescriptorType = Mapping[str, Any]
WorkflowDescriptorType = ProcessDescriptorType | Sequence[ProcessDescriptorType]


def run(workflow: WorkflowDescriptorType):
    return Workflow.create(workflow).run()


class Workflow:
    def __init__(self, process: base.ProcessNode):
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
    def _create_process(
        parent: Optional[base.ProcessNode], process: ProcessDescriptorType
    ):
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
                    params[key[1:]] = base.RunnableProcessParam(
                        Workflow.create(value).process
                    )
                else:
                    params[key] = base.PlainProcessParam(value)

        # invoke process
        return base.ProcessNode(parent, runner, params)

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
