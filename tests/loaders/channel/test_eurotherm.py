from pathlib import Path

import numpy as np
import pandas as pd

from rdmlibpy.loaders import ChannelEurothermLoggerLoader


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
