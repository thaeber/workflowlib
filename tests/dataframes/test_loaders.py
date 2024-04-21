import io
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd
from workflowlib.dataframes import DataFrameReadCSV


class TestDataFrameReadCSV:
    def test_create_loader(self):
        loader = DataFrameReadCSV()

        assert loader.name == 'dataframe.readcsv'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ','

    def test_update_config(self):
        loader = DataFrameReadCSV()
        dummy = loader.updated(decimal=',')
        assert dummy.decimal == ','
        assert loader.decimal == '.'

    def test_load_single(self, data_path: Path):
        loader = DataFrameReadCSV(
            options=dict(names=['time', 'temperature']),
        )
        df = loader.run(
            source=data_path / 'eurotherm/20240118T084901.txt',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1913  # type: ignore
        assert list(df.columns) == ['time', 'temperature']

    def test_load_single_as_arg(self, data_path: Path):
        loader = DataFrameReadCSV(
            options=dict(names=['time', 'temperature']),
        )
        df = loader.run(
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
        loader = DataFrameReadCSV(decimal='.', separator=',')
        df = loader.run(stream)

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
        loader = DataFrameReadCSV(decimal='.', separator=',', parse_dates=['timestamp'])
        df = loader.run(stream)

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
        loader = DataFrameReadCSV(
            decimal='.', separator=',', parse_dates={'time': ['timestamp']}
        )
        df = loader.run(stream)

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
        loader = DataFrameReadCSV(
            decimal='.',
            separator=',',
            parse_dates={'timestamp': ['date', 'time']},
            date_format='%d.%m.%Y %H:%M:%S',
        )
        df = loader.run(stream)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['timestamp', 'idx', 'A', 'B', 'C']
        assert df.loc[0, 'timestamp'] == np.datetime64('2024-04-18T12:00:01')
        assert df.loc[1, 'timestamp'] == np.datetime64('2024-04-19T12:20:01')
        assert df.loc[2, 'timestamp'] == np.datetime64('2024-04-20T12:21:01')

    def test_runtime_arguments(self):
        data = """
            1,2024-04-18T12:00:01,3.0,b,c
            2,2024-04-18T12:20:01,2.0,e,f
            3,2024-04-18T12:20:01,1.0,h,i
        """
        stream = io.StringIO(dedent(data))
        loader = DataFrameReadCSV(decimal='.', separator=',')
        df = loader.run(stream, names=['idx', 'timestamp', 'A', 'B', 'C'])

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['idx', 'timestamp', 'A', 'B', 'C']
