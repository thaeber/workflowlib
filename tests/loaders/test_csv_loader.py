import io
from pathlib import Path
from textwrap import dedent

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
        assert loader.separator == ','

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

    def test_load_from_text_buffer(self):
        data = """
            idx,timestamp,A,B,C
            1,2024-04-18T12:00:01,a,b,c
            2,2024-04-18T12:20:01,d,e,f
            3,2024-04-18T12:20:01,g,h,i
        """
        stream = io.StringIO(dedent(data))
        loader = CSVLoader(decimal='.', separator=',')
        df = loader.process(stream)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['idx', 'timestamp', 'A', 'B', 'C']

    def test_parse_dates_list_of_single_column(self):
        data = """
            idx,timestamp,A,B,C
            1,2024-04-18T12:00:01,a,b,c
            2,2024-04-19T12:20:01,d,e,f
            3,2024-04-20T12:21:01,g,h,i
        """
        stream = io.StringIO(dedent(data))
        loader = CSVLoader(decimal='.', separator=',', parse_dates=['timestamp'])
        df = loader.process(stream)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['idx', 'timestamp', 'A', 'B', 'C']
        assert df.loc[0, 'timestamp'] == np.datetime64('2024-04-18T12:00:01')
        assert df.loc[1, 'timestamp'] == np.datetime64('2024-04-19T12:20:01')
        assert df.loc[2, 'timestamp'] == np.datetime64('2024-04-20T12:21:01')

    def test_parse_dates_map_of_single_column(self):
        data = """
            idx,timestamp,A,B,C
            1,2024-04-18T12:00:01,a,b,c
            2,2024-04-19T12:20:01,d,e,f
            3,2024-04-20T12:21:01,g,h,i
        """
        stream = io.StringIO(dedent(data))
        loader = CSVLoader(
            decimal='.', separator=',', parse_dates={'time': ['timestamp']}
        )
        df = loader.process(stream)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['idx', 'time', 'A', 'B', 'C']
        assert df.loc[0, 'time'] == np.datetime64('2024-04-18T12:00:01')
        assert df.loc[1, 'time'] == np.datetime64('2024-04-19T12:20:01')
        assert df.loc[2, 'time'] == np.datetime64('2024-04-20T12:21:01')

    def test_parse_dates_map_of_joined_column(self):
        data = """
            idx,date,time,A,B,C
            1,18.04.2024,12:00:01,a,b,c
            2,19.04.2024,12:20:01,d,e,f
            3,20.04.2024,12:21:01,g,h,i
        """
        stream = io.StringIO(dedent(data))
        loader = CSVLoader(
            decimal='.',
            separator=',',
            parse_dates={'timestamp': ['date', 'time']},
            date_format='%d.%m.%Y %H:%M:%S',
        )
        df = loader.process(stream)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['timestamp', 'idx', 'A', 'B', 'C']
        assert df.loc[0, 'timestamp'] == np.datetime64('2024-04-18T12:00:01')
        assert df.loc[1, 'timestamp'] == np.datetime64('2024-04-19T12:20:01')
        assert df.loc[2, 'timestamp'] == np.datetime64('2024-04-20T12:21:01')


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
        df = loader.process(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert 'timestamp' in df.columns
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-16T11:26:54.535')


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
        assert loader.parse_dates == {'timestamp': ['Date', 'Time']}
        assert loader.date_format == '%d.%m.%Y %H:%M:%S,%f'
        assert loader.options == dict(
            header='infer',
        )

    def test_load_single(self, data_path: Path):
        loader = MksFTIRLoader()
        df = loader.process(
            source=data_path / 'mks_ftir/2024-01-16-conc.prn',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 16  # type: ignore
        # assert df.columns[0] == 'timestamp'  # type: ignore
        # assert df.columns[1] == 'Spectrum'
        assert 'timestamp' in df.columns
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
