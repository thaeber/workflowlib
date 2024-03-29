from pathlib import Path

import numpy as np
import pandas as pd

from workflowlib.loaders import (
    ChannelEurothermLoggerLoader,
    ChannelTCLoggerLoader,
    CSVLoader,
    HidenRGALoader,
    MksFTIRLoader,
)


class TestCSVLoader:
    def test_create_loader(self):
        loader = CSVLoader()

        assert loader.name == 'load.csv'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'

    def test_update_config(self):
        loader = CSVLoader()
        dummy = loader.updated(decimal=',')
        assert dummy.decimal == ','
        assert loader.decimal == '.'

    def test_load_single(self, data_path: Path):
        loader = CSVLoader(
            options=dict(names=['time', 'temperature']),
        )
        df = loader.process(
            source=data_path / 'eurotherm/20240118T084901.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1913  # type: ignore
        assert list(df.columns) == ['time', 'temperature']

    def test_load_single_as_arg(self, data_path: Path):
        loader = CSVLoader(
            options=dict(names=['time', 'temperature']),
        )
        df = loader.process(
            data_path / 'eurotherm/20240118T084901.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1913  # type: ignore
        assert list(df.columns) == ['time', 'temperature']


class TestChannelTCLoggerLoader:
    def test_create_loader(self):
        loader = ChannelTCLoggerLoader()

        assert loader.name == 'channel.tclogger'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'

    def test_load_single(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.process(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10  # type: ignore
        assert df.columns[0] == 'timestamp'  # type: ignore
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-16T11:26:54.535')


class TestChannelEurothermLoggerLoader:
    def test_create_loader(self):
        loader = ChannelEurothermLoggerLoader()

        assert loader.name == 'channel.eurotherm'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'
        assert loader.options == dict(
            parse_dates=[0],
            date_format='ISO8601',
            names=['timestamp', 'temperature'],
        )

    def test_load_single(self, data_path: Path):
        loader = ChannelEurothermLoggerLoader()
        df = loader.process(
            source=data_path / 'eurotherm/*.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8276  # type: ignore
        assert list(df.columns) == ['timestamp', 'temperature']
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-18T08:49:01.551')
        assert df['timestamp'].iloc[-1] == np.datetime64('2024-01-18T09:28:01.359')


class TestMksFTIRLoader:
    def test_create_loader(self):
        loader = MksFTIRLoader()

        assert loader.name == 'mks.ftir'
        assert loader.version == '1'
        assert loader.decimal == ','
        assert loader.separator == '\t'

    def test_load_single(self, data_path: Path):
        loader = MksFTIRLoader()
        df = loader.process(
            source=data_path / 'mks_ftir/2024-01-16-conc.prn',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 16  # type: ignore
        assert df.columns[0] == 'timestamp'  # type: ignore
        assert df.columns[1] == 'Spectrum'
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-16T10:05:21')


class TestHidenRGALoader:
    def test_create_loader(self):
        loader = HidenRGALoader()

        assert loader.name == 'hiden.rga'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ';'

    def test_load_single(self, data_path: Path):
        loader = HidenRGALoader()
        df = loader.process(
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
        df = loader.process(
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
        df = loader.process(
            source=data_path / 'hiden/ae03_20240123_nh3lo_n2_2nlpm_f06_test1.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-23T13:33:24')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-01-23T13:33:29.771')

    def test_date_parsing2(self, data_path: Path):
        loader = HidenRGALoader(separator=',')
        df = loader.process(
            source=data_path / 'hiden/ae11_nh3x_n2_2nlpm_allars7-9_f02.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-02-05T16:04:55')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-02-05T16:04:56.022')

    def test_date_parsing3(self, data_path: Path):
        loader = HidenRGALoader(separator=';')
        df = loader.process(
            source=data_path / 'hiden/ae11_nh3x_n2_2nlpm_allars7-9_f06.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert df['timestamp'].iloc[0] == np.datetime64('2024-02-07T06:43:27')
        assert df['timestamp'].iloc[1] == np.datetime64('2024-02-07T06:43:28.017')
