from pathlib import Path
import pandas as pd

import pydantic

from ..process import ProcessBase, Serializer
from .pandas_dataframe import PandasDataFrameSerializer
from ..registry import get_runner


class FileCache(ProcessBase):
    name: str = 'file.cache'
    version: str = '1'
    serializer: str = 'auto'

    def get_serializer(self, source):
        if self.serializer == 'auto':
            if isinstance(source, pd.DataFrame):
                return PandasDataFrameSerializer()
            else:
                raise ValueError(f'No matching serializer for type {type(source)}')
        else:
            ser = get_runner(self.serializer)
            assert isinstance(ser, Serializer)
            return ser

    def run(self, source, target: Path):
        ser = self.get_serializer(source)

        if not target.exists():
            ser.write(source, target)

        return ser.load(target)
