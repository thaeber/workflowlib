import io
import os
from pathlib import Path
from typing import Any, Optional

import pydantic


class ProcessStepBase(pydantic.BaseModel):
    name: str
    version: str

    def updated(self, **config):
        _config = self.model_dump(exclude_defaults=True)
        _config.update(config)
        return self.model_validate(_config)

    def process(self, source) -> Any:
        pass

    def pre_process_hook(self) -> Optional[Any]:
        return None

    @property
    def fullname(self):
        return f'{self.name}@v{self.version}'


class Loader(ProcessStepBase):
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


class Serializer(ProcessStepBase):
    def ensure_parent_path_exists(self, uri: Path):
        if not uri.parent.exists():
            uri.parent.mkdir(parents=True)
        return uri

    def load(self, uri: Path):
        pass

    def write(self, source, uri: Path):
        pass


class Transform(ProcessStepBase):
    pass


# %%
