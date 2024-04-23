import io
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd
import pint_pandas

from workflowlib.base import PlainProcessParam, ProcessNode
from workflowlib.dataframes import (
    DataFrameFileCache,
    DataFrameReadCSV,
    DataFrameWriteCSV,
)
from workflowlib.process import DelegatedSource


class TestDataFrameReadCSV:
    def test_create_loader(self):
        loader = DataFrameReadCSV()

        assert loader.name == 'dataframe.read_csv'
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


class TestDataFrameWriteCSV:
    def test_create_loader(self):
        writer = DataFrameWriteCSV()

        assert writer.name == 'dataframe.write_csv'
        assert writer.version == '1'
        assert writer.decimal == '.'
        assert writer.separator == ','
        assert writer.index == False

    def test_write_csv(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';')
        serializer.run(df, path)

        assert path.exists()
        df = pd.read_csv(path, sep=';')
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_write_csv_with_runtime_arguments(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';')
        serializer.run(df, path, sep=',')

        assert path.exists()
        df = pd.read_csv(path, sep=',')
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_datetime_formatting(self, tmp_path: Path):
        # check a successful write/read with default settings
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    np.datetime64('2024-01-16T10:05:28.537'),
                    np.datetime64('2024-01-16T10:05:29.735'),
                    np.datetime64('2024-01-16T10:05:30.935'),
                ],
            )
        )
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        df2 = pd.read_csv(path, sep=',', decimal='.', parse_dates=False)

        assert all(df.columns == df2.columns)

        # datetime should have been saved in ISO8601 format
        assert list(df2.C) == [
            '2024-01-16T10:05:28.537000',
            '2024-01-16T10:05:29.735000',
            '2024-01-16T10:05:30.935000',
        ]
        assert (df == df2).values.all()

    def test_write_with_units(self, tmp_path: Path):
        # check a successful write/read with default settings
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    np.datetime64('2024-01-16T10:05:28.537'),
                    np.datetime64('2024-01-16T10:05:29.735'),
                    np.datetime64('2024-01-16T10:05:30.935'),
                ],
            )
        )
        df['E'] = pint_pandas.PintArray([1.0, 2.0, 3.0], dtype='pint[m]')
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path, dequantify=True)
        assert path.exists()

        # load data from written file
        df2 = pd.read_csv(
            path,
            sep=',',
            decimal='.',
            parse_dates=[2],
            date_format='ISO8601',
            header=[0, 1],
        )
        df2 = df2.pint.quantify(level=-1)
        for col in df2.columns:
            try:
                if df2[col].dtype == 'object':
                    df2[col] = pd.to_numeric(df2[col])
            except ValueError:
                pass

        assert list(df.columns) == list(df2.columns)
        assert (df == df2).values.all()
        assert list(df.dtypes) == list(df2.dtypes)


class TestDataFrameCSVCache:

    def test_create(self):
        cache = DataFrameFileCache()

        assert cache.name == 'dataframe.cache.csv'
        assert cache.version == '1'

    def test_write_to_cache(self, tmp_path):
        path = tmp_path / 'cache.hd5'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    '2024-01-16T10:05:28.537',
                    '2024-01-16T10:05:29.735',
                    '2024-01-16T10:05:30.935',
                ],
            )
        )

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {'filename': PlainProcessParam(str(path))},
        )

        # verify cache does not exists yet
        assert not path.exists()

        # write to cache
        df2 = workflow.run()

        # check that workflow returned original data
        assert df is df2

        # check cache exists
        assert path.exists()

        # check written data
        df3 = pd.read_hdf(path).pint.quantify(level=-1)
        assert (df == df3).values.all()

    def test_read_from_cache(self, tmp_path):
        path = tmp_path / 'cache.hd5'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    '2024-01-16T10:05:28.537',
                    '2024-01-16T10:05:29.735',
                    '2024-01-16T10:05:30.935',
                ],
            )
        )
        df.pint.dequantify().to_hdf(path, key='data')

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {'filename': PlainProcessParam(str(path))},
        )

        cached = workflow.run()

        # check that returned data is a different object instance
        assert df is not cached

        # check data integrity
        assert (df == cached).values.all()

    def test_rebuild_cache(self, tmp_path):
        path = tmp_path / 'cache.hd5'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    '2024-01-16T10:05:28.537',
                    '2024-01-16T10:05:29.735',
                    '2024-01-16T10:05:30.935',
                ],
            )
        )
        df.to_hdf(path, key='data')

        df['D'] = ['R1', 'R2', 'R3']
        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {
                'filename': PlainProcessParam(str(path)),
                'rebuild': PlainProcessParam(True),
            },
        )

        cached = workflow.run()

        # check that returned data is the original object instance
        assert df is cached

        # check data integrity
        assert (df == cached).values.all()

    def test_dataframe_with_index(self, tmp_path):
        path = tmp_path / 'cache.csv'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    np.datetime64('2024-01-16T10:05:28.537'),
                    np.datetime64('2024-01-16T10:05:29.735'),
                    np.datetime64('2024-01-16T10:05:30.935'),
                ],
                D=['R1', 'R2', 'R3'],
            ),
        )
        df.set_index('D', inplace=True)

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {
                'filename': PlainProcessParam(str(path)),
            },
        )

        # create cache
        assert not path.exists()
        cached = workflow.run()

        # modify dataframe and load from cache
        df2 = df.copy(deep=True)
        df['E'] = [1, 2, 3]
        cached = workflow.run()

        # check returned data is a different object instance
        assert df is not cached

        # check data integrity
        assert (df2 == cached).values.all()

    def test_dataframe_with_datetime_index(self, tmp_path):
        path = tmp_path / 'cache.csv'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    np.datetime64('2024-01-16T10:05:28.537'),
                    np.datetime64('2024-01-16T10:05:29.735'),
                    np.datetime64('2024-01-16T10:05:30.935'),
                ],
                D=['R1', 'R2', 'R3'],
            ),
        )
        df.set_index('C', inplace=True)

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {
                'filename': PlainProcessParam(str(path)),
            },
        )

        # create cache
        assert not path.exists()
        cached = workflow.run()

        # modify dataframe and load from cache
        df2 = df.copy(deep=True)
        df['E'] = [1, 2, 3]
        cached = workflow.run()

        # check returned data is a different object instance
        assert df is not cached

        # check data integrity
        assert (df2 == cached).values.all()

    def test_dataframe_with_units(self, tmp_path):
        path = tmp_path / 'cache.csv'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
                C=[
                    np.datetime64('2024-01-16T10:05:28.537'),
                    np.datetime64('2024-01-16T10:05:29.735'),
                    np.datetime64('2024-01-16T10:05:30.935'),
                ],
                D=['R1', 'R2', 'R3'],
            ),
        )
        df['E'] = pint_pandas.PintArray([1.0, 2.0, 3.0], dtype='pint[m]')
        original = df.copy(deep=True)

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {
                'filename': PlainProcessParam(str(path)),
            },
        )

        # create cache
        assert not path.exists()
        cached = workflow.run()

        # modify dataframe and load from cache
        df['E'] = [1, 2, 3]
        cached = workflow.run()

        # check returned data is a different object instance
        assert df is not cached

        # check data integrity
        assert list(original.columns) == list(cached.columns)
        assert list(original.dtypes) == list(cached.dtypes)
        assert (original == cached).values.all()