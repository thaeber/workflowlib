import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pydantic

from .._typing import FilePath, ReadCsvBuffer
from ..process import Loader

logger = logging.getLogger(__name__)

ParseDatesType = None | List[str] | Dict[str, List[str]]


class DataFrameReadCSVBase(Loader):
    decimal: str = '.'
    separator: str = ','
    options: Dict[str, Any] = pydantic.Field(default_factory=dict)  # type: ignore
    concatenate: bool = True
    date_format: str = 'ISO8601'
    parse_dates: ParseDatesType = None

    def run(self, source: FilePath | ReadCsvBuffer, **kwargs):
        if isinstance(source, FilePath):
            # load using filename (possible a glob pattern)
            data = [self._read_csv(path, **kwargs) for path in Loader.glob(source)]
            if self.concatenate:
                data = pd.concat(data)
            return data
        else:
            # load from text buffer (e.g. file buffer or StringIO)
            return self._read_csv(source, **kwargs)

    def _read_csv(self, source: FilePath | ReadCsvBuffer, **kwargs):
        df = self._load(source, **kwargs)
        df = self._parse_dates(df)
        return df

    def _load(self, source: FilePath | ReadCsvBuffer, **kwargs) -> pd.DataFrame:
        if isinstance(source, Path):
            logger.info(f'Loading CSV data from: {source.name} ({source.parent})')
            if not source.exists():
                logger.error('File does not exists')
                raise FileNotFoundError(source)
        else:
            logger.info('Reading CSV data from text buffer')

        # merge process configuration with runtime keyword arguments
        options = dict(sep=self.separator, decimal=self.decimal, **self.options)
        options |= kwargs

        # load csv data & return
        return pd.read_csv(source, **options)

    def _parse_dates(self, df: pd.DataFrame):
        if self.parse_dates is None:
            return df

        match self.parse_dates:
            case [*column_names]:
                for col in column_names:
                    df = self._parse_dates_replacing_single_column(df, col)
            case {**nested}:
                # loop over mappings
                for key, column_names in nested.items():
                    if len(column_names) == 1:
                        name = column_names[0]
                        df = self._parse_dates_replacing_single_column(df, name)
                        if key != name:
                            df.rename(columns={name: key}, inplace=True, errors='raise')
                    else:
                        df = self._parse_dates_joining_columns(df, key, column_names)

        return df

    def _parse_dates_joining_columns(
        self, df: pd.DataFrame, target_name: str, column_names: List[str]
    ):

        def join_columns(column_names):
            return df[column_names].astype(str).agg(' '.join, axis=1)

        # generate datetime series from columns
        dt = pd.to_datetime(join_columns(column_names), format=self.date_format)

        # drop source columns
        df = df.drop(columns=column_names)

        # insert new column at the front (in place operation)
        df.insert(0, target_name, dt)

        return df

    def _parse_dates_replacing_single_column(self, df: pd.DataFrame, column: str):
        # get (index) location of original column
        index = df.columns.get_loc(column)
        if not isinstance(index, int):
            raise ValueError(
                f'Column label must be unique. Found multiple indices ({index}) for label ({column}).'
            )

        # "pop" column & generate datetime series
        dt = pd.to_datetime(df.pop(column), format=self.date_format)

        # insert new column at original index
        df.insert(index, column, dt)

        return df


class DataFrameReadCSV(DataFrameReadCSVBase):
    version: str = '1'
    name: str = 'dataframe.readcsv'
