import logging
from typing import Any, Dict

import pandas as pd
import pydantic

from .._typing import FilePath, WriteBuffer
from ..process import Writer

logger = logging.getLogger(__name__)


class DataFrameWriteCSV(Writer):
    name: str = 'dataframe.write_csv'
    version: str = '1'

    decimal: str = '.'
    separator: str = ','
    options: Dict[str, Any] = pydantic.Field(default_factory=dict)  # type: ignore
    date_format: str = 'ISO8601'

    def run(self, source: pd.DataFrame, target: FilePath | WriteBuffer, **kwargs):
        # merge process configuration with runtime keyword arguments
        options = dict(
            sep=self.separator, decimal=self.decimal, date_format=self.date_format
        )
        options |= self.options
        options |= kwargs

        # create paths if necessary
        target = Writer.ensure_path(target)

        # write data to csv
        source.to_csv(target, **options)
