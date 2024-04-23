import abc
import os
from pathlib import Path
from typing import Any, Callable

from rdmlibpy.base import ProcessBase, ProcessNode


class DelegatedSource(ProcessBase):
    name: str = 'delegated.source'
    version: str = '1'
    delegate: Callable[[], Any]

    def run(self):
        return self.delegate()


class Loader(ProcessBase):
    @staticmethod
    def glob(source: str | os.PathLike):
        path = Path(source)

        path = path.absolute()
        try:
            parts = list(path.parts)
            idx = parts.index('**')
            root = path.parents[len(parts) - 1 - idx]
            pattern = str(path.relative_to(root))
        except ValueError:
            root = path.parent
            pattern = path.name

        sources = list(root.glob(pattern))

        if not sources:
            raise FileNotFoundError(f'No sources found matching expression: {source}')
        for src in sources:
            yield src


class Writer(ProcessBase):
    @classmethod
    def ensure_path(cls, filepath: str | os.PathLike):
        """Ensures that the parent path of the given file exists.
        Creates the path if it does not exists.

        Args:
            filepath (str | os.PathLike): The path and filename to the file.

        Returns:
            Path: The original filepath wrapped in a `Path`instance.
        """
        path = Path(filepath)

        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        return path


class Serializer(ProcessBase):
    def ensure_parent_path_exists(self, uri: Path):
        if not uri.parent.exists():
            uri.parent.mkdir(parents=True)
        return uri

    def load(self, uri: Path):
        pass

    def write(self, source, uri: Path):
        pass


class Transform(ProcessBase):
    pass


class Cache(ProcessBase):

    def run(self, source, **kwargs):
        # write source to cache
        self.write(source, **kwargs)

        # return source unaltered
        return source

    def _run(self, node: ProcessNode):
        # get parameters
        params = node.get_params()

        # check if cache is valid
        if self.cache_is_valid(**params):
            # return cached value
            return self.read(**params)
        else:
            # run process normally (and save value to cache)
            return super()._run(node)

    @abc.abstractmethod
    def read(self, **kwargs):
        pass

    @abc.abstractmethod
    def write(self, source, **kwargs):
        pass

    @abc.abstractmethod
    def cache_is_valid(self, **kwargs):
        pass
