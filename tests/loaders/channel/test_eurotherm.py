from pathlib import Path

import numpy as np
import pandas as pd

from rdmlibpy.loaders import (
    ChannelEurothermLoggerLoader,
    ChannelEurothermLoggerLoaderV1_1,
)


class TestChannelEurothermLoggerLoader:
    def test_create_loader(self):
        loader = ChannelEurothermLoggerLoader()

        assert loader.name == 'channel.eurotherm'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'
        assert loader.parse_dates == ['timestamp']
        assert loader.date_format == 'ISO8601'
        assert loader.options == dict(
            names=['timestamp', 'temperature'],
        )

    def test_load_single(self, data_path: Path):
        loader = ChannelEurothermLoggerLoader()
        df = loader.run(
            source=data_path / 'eurotherm/*.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8276  # type: ignore
        assert list(df.columns) == ['timestamp', 'temperature']
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-18T08:49:01.551')
        assert df['timestamp'].iloc[-1] == np.datetime64('2024-01-18T09:28:01.359')


class TestChannelEurothermLoggerLoaderV1_1:
    def test_create_loader(self):
        loader = ChannelEurothermLoggerLoaderV1_1()

        assert loader.name == 'channel.eurotherm'
        assert loader.version == '1.1'
        assert loader.decimal == '.'
        assert loader.separator == ';'
        assert loader.parse_dates == ['timestamp']
        assert loader.date_format == 'ISO8601'
        assert loader.options == dict(
            header='infer',
            # names=['timestamp', 'temperature', 'power'],
        )

    def test_load_single(self, data_path: Path):
        loader = ChannelEurothermLoggerLoaderV1_1()
        df = loader.run(
            source=data_path / 'eurotherm/v1.1/20240621T112547.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10  # type: ignore
        assert list(df.columns) == ['timestamp', 'temperature', 'power']
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-06-21T11:25:47.231')
        assert df['timestamp'].iloc[-1] == np.datetime64('2024-06-21T11:25:49.599')
