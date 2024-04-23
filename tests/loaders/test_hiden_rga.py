from pathlib import Path

import numpy as np
import pandas as pd

from rdmlibpy.loaders import HidenRGALoader


class TestHidenRGALoader:
    def test_create_loader(self):
        loader = HidenRGALoader()

        assert loader.name == 'hiden.rga'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'

    def test_load_single(self, data_path: Path):
        loader = HidenRGALoader()
        df = loader.run(
            source=data_path / 'hiden/ae03_20240123_nh3lo_n2_2nlpm_f06_test1.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10  # type: ignore
        assert (
            len(df.columns) == 29
        )  # 25 Scans + Time + ms + (empty column) + timestamp
        assert df.columns[0] == 'timestamp'  # type: ignore
        assert df['timestamp'].dtype == np.dtype('<M8[us]')  # type: ignore

    def test_different_separator(self, data_path: Path):
        loader = HidenRGALoader(separator=',')
        df = loader.run(
            source=data_path / 'hiden/ae11_nh3x_n2_2nlpm_allars7-9_f02.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 13  # type: ignore
        assert (
            len(df.columns) == 19
        )  # 15 Scans + Time + ms + (empty column) + timestamp
        assert df.columns[0] == 'timestamp'  # type: ignore
        assert df['timestamp'].dtype == np.dtype('<M8[us]')  # type: ignore

    def test_date_parsing1(self, data_path: Path):
        loader = HidenRGALoader()
        df = loader.run(
            source=data_path / 'hiden/ae03_20240123_nh3lo_n2_2nlpm_f06_test1.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-23T13:33:24')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-01-23T13:33:29.771')

    def test_date_parsing2(self, data_path: Path):
        loader = HidenRGALoader(separator=',')
        df = loader.run(
            source=data_path / 'hiden/ae11_nh3x_n2_2nlpm_allars7-9_f02.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-02-05T16:04:55')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-02-05T16:04:56.022')

    def test_date_parsing3(self, data_path: Path):
        loader = HidenRGALoader(separator=';')
        df = loader.run(
            source=data_path / 'hiden/ae11_nh3x_n2_2nlpm_allars7-9_f06.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-02-07T06:43:27')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-02-07T06:43:28.017')
