import abc
import os
from pathlib import Path
from typing import Any

import pydantic


class ProcessBase(pydantic.BaseModel, abc.ABC):
    name: str
    version: str

    def updated(self, **config):
        _config = self.model_dump(exclude_defaults=True)
        _config.update(config)
        return self.model_validate(_config)

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> Any:
        pass

    def preprocess(self):
        return None

    @property
    def fullname(self):
        return f'{self.name}@v{self.version}'


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
    @staticmethod
    def ensure_path(filepath: str | os.PathLike):
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


# %%
