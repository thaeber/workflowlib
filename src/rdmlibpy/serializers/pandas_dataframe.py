from pathlib import Path
from typing import Any, Dict, Literal

import pandas as pd
import pydantic

from ..process import Serializer

DataFrameFormat = Literal['csv', 'HDF5']


class PandasDataFrameSerializer(Serializer):
    name: str = 'pandas.dataframe'
    version: str = '1'
    format: DataFrameFormat = 'csv'
    options: Dict[str, Any] = pydantic.Field(default_factory=dict)  # type: ignore

    def load(self, uri: Path):
        match self.format:
            case 'csv':
                return pd.read_csv(uri, **self.options)
            case 'HDF5':
                return pd.read_hdf(uri, self.options.get('key', 'data'))
            case _:
                raise ValueError(f'Unsupported format: {self.format}')

    def write(self, source: pd.DataFrame, uri: Path):
        match self.format:
            case 'csv':
                source.to_csv(self.ensure_parent_path_exists(uri), **self.options)
            case 'HDF5':
                options = self.options.copy()
                if not 'key' in options:
                    options.update(key='data')
                source.to_hdf(self.ensure_parent_path_exists(uri), **options)
            case _:
                raise ValueError(f'Unsupported format: {self.format}')

    def run(self, *args, **kwargs):
        return args[0]
