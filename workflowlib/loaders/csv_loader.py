import logging
import tokenize
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pydantic

from ..base import Loader

logger = logging.getLogger(__name__)


class CSVLoaderBase(Loader):
    decimal: str = '.'
    separator: str = ';'
    options: Dict[str, Any] = pydantic.Field(default_factory=dict)  # type: ignore
    concatenate: bool = True

    def process(self, source):
        data = [self._load(source) for source in Loader.get_sources(source)]
        if self.concatenate:
            data = pd.concat(data)

        return data

    def _load(self, filename: Path) -> pd.DataFrame:
        logger.info(f'Loading: {filename.name} ({filename.parent})')
        if not filename.exists():
            logger.error('File does not exists')
            raise FileNotFoundError(filename)
        df = pd.read_csv(
            filename, sep=self.separator, decimal=self.decimal, **self.options
        )
        return df


class CSVLoader(CSVLoaderBase):
    version: str = '1'
    name: str = 'load.csv'


class ChannelTCLoggerLoader(CSVLoaderBase):
    name: str = 'channel.tclogger'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    options: Dict[str, Any] = dict(
        parse_dates=[0],
        date_format='ISO8601',
        header='infer',
    )


class ChannelEurothermLoggerLoader(CSVLoaderBase):
    name: str = 'channel.eurotherm'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    options: Dict[str, Any] = dict(
        parse_dates=[0],
        date_format='ISO8601',
        names=['timestamp', 'temperature'],
    )


class MksFTIRLoader(CSVLoaderBase):
    name: str = 'mks.ftir'
    version: str = '1'

    decimal: str = ','
    separator: str = '\t'
    options: Dict[str, Any] = dict(
        parse_dates={'timestamp': [1, 2]},
        date_format='%d.%m.%Y %H:%M:%S,%f',
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
