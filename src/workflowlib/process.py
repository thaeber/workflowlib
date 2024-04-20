import io
import os
from pathlib import Path
from typing import Any, Optional

import pydantic


class ProcessBase(pydantic.BaseModel):
    name: str
    version: str

    def updated(self, **config):
        _config = self.model_dump(exclude_defaults=True)
        _config.update(config)
        return self.model_validate(_config)

    def run(self, *args, **kwargs) -> Any:
        if len(args) == 1:
            return args[0]
        else:
            return (*args,)

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
