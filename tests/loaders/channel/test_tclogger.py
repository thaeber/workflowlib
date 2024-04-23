from pathlib import Path

import numpy as np
import pandas as pd

from rdmlibpy.loaders import ChannelTCLoggerLoader


class TestChannelTCLoggerLoader:
    def test_create_loader(self):
        loader = ChannelTCLoggerLoader()

        assert loader.name == 'channel.tclogger'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'
        assert loader.parse_dates == ['timestamp']
        assert loader.date_format == 'ISO8601'
        assert loader.options == dict(
            header='infer',
        )

    def test_load_single(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert 'timestamp' in df.columns
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-16T11:26:54.535')
