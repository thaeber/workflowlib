import tokenize
from datetime import datetime

import pandas as pd

from ..dataframes.loaders import DataFrameReadCSVBase


class HidenRGALoader(DataFrameReadCSVBase):
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

    def run(self, source):
        with open(source, 'r') as file:
            header = self.parse_header(file)
        t0 = datetime.combine(header['date'], header['time'])

        # skip header lines
        if 'skiprows' not in self.options:
            self.options.update(skiprows=header['header_lines'] + 1)

        data = super().run(source)

        if isinstance(data, list):
            return [self.create_timestamp(df, t0) for df in data]
        else:
            return self.create_timestamp(data, t0)
