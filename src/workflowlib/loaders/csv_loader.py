import io
import logging
import os
import tokenize
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pydantic

from ..base import Loader
from .._typing import FilePath, ReadCsvBuffer

logger = logging.getLogger(__name__)

ParseDatesType = None | List[str] | Dict[str, List[str]]


class CSVLoaderBase(Loader):
    decimal: str = '.'
    separator: str = ','
    options: Dict[str, Any] = pydantic.Field(default_factory=dict)  # type: ignore
    concatenate: bool = True
    date_format: str = 'ISO8601'
    parse_dates: ParseDatesType = None

    def process(self, source: FilePath | ReadCsvBuffer):
        if isinstance(source, FilePath):
            # load using filename (possible a glob pattern)
            data = [self._process_single(source) for source in Loader.glob(source)]
            if self.concatenate:
                data = pd.concat(data)
            return data
        else:
            # load from text buffer (e.g. file buffer or StringIO)
            return self._process_single(source)

    def _process_single(self, source):
        df = self._load(source)
        df = self._parse_dates(df)
        return df

    def _load(self, source: Path | ReadCsvBuffer) -> pd.DataFrame:
        if isinstance(source, Path):
            logger.info(f'Loading CSV data from: {source.name} ({source.parent})')
            if not source.exists():
                logger.error('File does not exists')
                raise FileNotFoundError(source)
        else:
            logger.info('Reading CSV data from text buffer')
        df = pd.read_csv(
            source, sep=self.separator, decimal=self.decimal, **self.options
        )
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


class CSVLoader(CSVLoaderBase):
    version: str = '1'
    name: str = 'load.csv'


class ChannelTCLoggerLoader(CSVLoaderBase):
    name: str = 'channel.tclogger'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    parse_dates: ParseDatesType = ['timestamp']
    date_format: str = 'ISO8601'
    options: Dict[str, Any] = dict(
        header='infer',
    )


class ChannelEurothermLoggerLoader(CSVLoaderBase):
    name: str = 'channel.eurotherm'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    parse_dates: ParseDatesType = ['timestamp']
    date_format: str = 'ISO8601'
    options: Dict[str, Any] = dict(
        names=['timestamp', 'temperature'],
    )


class MksFTIRLoader(CSVLoaderBase):
    name: str = 'mks.ftir'
    version: str = '1'

    decimal: str = ','
    separator: str = '\t'
    parse_dates: ParseDatesType = {'timestamp': ['Date', 'Time']}
    date_format: str = '%d.%m.%Y %H:%M:%S,%f'
    options: Dict[str, Any] = dict(
        header='infer',
    )


class HidenRGALoader(CSVLoaderBase):
    name: str = 'hiden.rga'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'

    def parse_header(self, file):
        header = {}
        tokens = tokenize.generate_tokens(file.readline)

        def _is_separator(token):
            return (token.type == tokenize.NEWLINE) or (
                token.type == tokenize.OP and token.string == self.separator
            )

        def split_by_tokens():
            stack = []
            for token in tokens:
                if _is_separator(token):
                    if stack:
                        joined = ''.join(t.string for t in stack)
                        yield joined
                        stack.clear()
                else:
                    stack.append(token)

        def parse_date(date_str: str):
            try:
                return datetime.strptime(date_str, r'%d.%m.%Y')
            except ValueError:
                return datetime.strptime(date_str, r'%Y-%m-%d')

        items = split_by_tokens()
        header['scans'] = int(next(items))
        assert next(items) == 'scans'
        header['data_length'] = int(next(items))
        assert next(items) == 'DataLength'
        assert next(items) == '"header"'
        header['header_lines'] = int(next(items))
        assert next(items) == '"lines"'
        assert next(items) == '"Date"'
        header['date'] = parse_date(next(items)).date()
        assert next(items) == '"Time"'
        header['time'] = datetime.strptime(next(items), r'%H:%M:%S').time()

        return header

    def create_timestamp(self, df: pd.DataFrame, t0: datetime):
        # create timestamp column
        df['timestamp'] = t0 + df['ms'].astype('<m8[ms]')  # type: ignore

        # move timestamp to front
        cols = list(df.columns)
        cols.insert(0, cols.pop(cols.index('timestamp')))
        return df[cols]

    def process(self, source):
        with open(source, 'r') as file:
            header = self.parse_header(file)
        t0 = datetime.combine(header['date'], header['time'])

        # skip header lines
        if 'skiprows' not in self.options:
            self.options.update(skiprows=header['header_lines'] + 1)

        data = super().process(source)

        if isinstance(data, list):
            return [self.create_timestamp(df, t0) for df in data]
        else:
            return self.create_timestamp(data, t0)
