from typing import Any, Dict, Mapping, Optional, Sequence, Type, TypeVar

from .base import ProcessStepBase
from .registry import get_runner


def _run_process(process: Mapping[str, Any], *args):
    # check if the process contains a `$run` element
    if 'run' not in process:
        raise ValueError(
            'The process parameters do not contain a `run` element.\n'
            + '\n'.join([f'{key}: {value}' for key, value in process.items()])
        )

    # get process instance
    runner = get_runner(process['run'], **process.get('config', dict()))

    # get keyword arguments (anything except reserved keys)
    kws = {}
    if 'params' in process:
        for key, value in process['params'].items():
            if key.startswith('$'):
                # value itself is a process, so run it
                kws[key[1:]] = run(value)
            else:
                kws[key] = value

    # invoke process
    return runner.process(*args, **kws)


def _run_process_pipe(sequence: Sequence, *args):
    args = run(sequence[0], *args)
    for step in sequence[1:]:
        if not isinstance(args, tuple):
            args = (args,)
        args = run(step, *args)
    return args


def run(workflow, *args):
    if isinstance(workflow, Mapping):
        # workflow is a single step
        return _run_process(workflow, *args)
    elif isinstance(workflow, Sequence):
        # run workflow pipe
        return _run_process_pipe(workflow, *args)
    else:
        raise ValueError('Invalid workflow specification')


class ProcessParam:
    def get_value(self):
        raise NotImplementedError()


class ProcessNode:
    def __init__(
        self,
        parent: Optional['ProcessNode'],
        runner: ProcessStepBase,
        params: Dict[str, ProcessParam],
    ):
        self.parent = parent
        self.runner = runner
        self.params = params

    @staticmethod
    def from_mapping(parent: Optional['ProcessNode'], process: Mapping[str, Any]):
        # check if the process contains a `$run` element
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
                if key.startswith('$') and isinstance(value, dict):
                    # value itself is a process, so run it
                    params[key[1:]] = None
                else:
                    params[key] = ProcessParamPlain(value)

        # invoke process
        return ProcessNode(parent, runner, params)

    def run(self):
        # resolve pre-process hooks (e.g. caching)
        result = self.runner.pre_process_hook()
        if result is not None:
            return result

        # resolve parameters
        params = {key: item.get_value() for key, item in self.params.items()}

        if self.parent is not None:
            source = self.parent.run()
            return self.runner.process(source, **params)
        else:
            return self.runner.process(**params)


class ProcessParamPlain:
    def __init__(self, value: Any):
        self.value = value

    def get_value(self):
        return self.value


class ProcessParamNode:
    def __init__(self, node: ProcessNode):
        self.node = node

    def get_value(self):
        return self.node.run()
