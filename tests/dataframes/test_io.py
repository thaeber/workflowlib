import io
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd
import pandas._testing as tm
import pint_pandas

from rdmlibpy.base import PlainProcessParam, ProcessNode
from rdmlibpy.dataframes import DataFrameFileCache, DataFrameReadCSV, DataFrameWriteCSV
from rdmlibpy.dataframes.io import quantify
from rdmlibpy.process import DelegatedSource
from omegaconf import OmegaConf


class TestDataFrameReadCSV:
    def test_create_loader(self):
        loader = DataFrameReadCSV()

        assert loader.name == 'dataframe.read.csv'
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

        assert writer.name == 'dataframe.write.csv'
        assert writer.version == '1'
        assert writer.decimal == '.'
        assert writer.separator == ','
        assert writer.index == 'reset-named'
        assert writer.units == 'auto'
        assert writer.attributes == 'auto'

    def test_returns_data(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';')
        df2 = serializer.run(df, path)

        # make sure input data is returned unaltered
        assert df2 is df

    def test_write_csv(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';')
        serializer.run(df, path)

        assert path.exists()
        df = pd.read_csv(path, sep=';')
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_index_false(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';', index=False)
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path, sep=';')
        tm.assert_frame_equal(df, actual)

    def test_index_true(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(separator=';', index=True)
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path, sep=';', index_col=0)
        tm.assert_frame_equal(df, actual, check_column_type=False)

    def test_index_true_with_named_index(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']),
            index=pd.Index(['i', 'ii', 'iii'], name='C'),
        )
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(index=True)
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path, index_col=0)
        tm.assert_frame_equal(df, actual, check_column_type=False)

    def test_index_reset(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(index='reset')
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path)
        tm.assert_frame_equal(df.reset_index(), actual, check_column_type=False)

    def test_index_reset_with_named_index(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']),
            index=pd.Index(['i', 'ii', 'iii'], name='C'),
        )
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(index='reset')
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path)
        tm.assert_frame_equal(df.reset_index(), actual, check_column_type=False)

    def test_index_reset_named(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']),
            index=pd.Index(['i', 'ii', 'iii'], name='C'),
        )
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(index='reset-named')
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path)
        tm.assert_frame_equal(df.reset_index(), actual, check_column_type=False)

    def test_index_reset_named_with_unnamed_index(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = DataFrameWriteCSV(index='reset-named')
        serializer.run(df, path)

        assert path.exists()
        actual = pd.read_csv(path)
        tm.assert_frame_equal(df, actual, check_column_type=False)

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

    def test_dequantify_units(self, tmp_path: Path):
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
        writer.run(df, path, units='dequantify')
        assert path.exists()

        # load data from written file
        actual = pd.read_csv(
            path,
            sep=',',
            decimal='.',
            parse_dates=[2],
            date_format='ISO8601',
            header=[0, 1],
        )
        actual = quantify(actual, level=-1)

        tm.assert_frame_equal(actual, df)

    def test_keep_units(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            )
        )
        df['E'] = pint_pandas.PintArray([1.0, 2.0, 3.0], dtype='pint[m]')
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path, units='keep-units')
        assert path.exists()

        # load data from written file
        actual = pd.read_csv(path)

        df['E'] = df['E'].astype(
            str
        )  # that's what the column looks like in the csv file
        tm.assert_frame_equal(df, actual)

    def test_auto_units(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            )
        )
        df['E'] = pint_pandas.PintArray([1.0, 2.0, 3.0], dtype='pint[m]')
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        # load data from written file
        actual = pd.read_csv(path, header=[0, 1])
        actual = quantify(actual, level=-1)

        tm.assert_frame_equal(df, actual)

    def test_auto_units_without_units_present(self, tmp_path: Path):
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            )
        )
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        # load data from written file
        actual = pd.read_csv(path, header=[0])

        tm.assert_frame_equal(df, actual)

    def test_write_attributes_as_comment(self, tmp_path):
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            )
        )
        df.attrs.update(
            dict(
                date='2024-04-27',
                trial=3,
                inlet=dict(flow_rate='1.0L/min', T='20°C'),
            )
        )
        path = tmp_path / 'data.csv'

        # write csv data (will save attrs by default)
        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        # load csv file (header & data)
        header = []
        with open(path, 'r', encoding='utf-8') as file:
            while (line := file.readline().strip()).startswith('#'):
                header.append(line[1:])
        header = OmegaConf.create('\n'.join(header))
        actual_df = pd.read_csv(path, comment='#', encoding='utf-8')

        # compare saved comment header
        # (attrs is saved in a yaml compliant format, so we just check the structure)
        assert header == df.attrs

        # compare data frames
        tm.assert_frame_equal(actual_df, df)

    def test_write_attributes_as_comment_with_pint_columns(self, tmp_path):
        df = pd.DataFrame(
            data=dict(
                A=pd.Series([1.1, 2.2, 3.3]),
                B=pd.Series(['aa', 'bb', 'cc']),
                E=pd.Series([1.0, 2.0, 3.0], dtype='pint[m]'),
            )
        )
        df.attrs.update(
            dict(
                date='2024-04-27',
                trial=3,
                inlet=dict(flow_rate='1.0L/min', T='20°C'),
            )
        )
        path = tmp_path / 'data.csv'

        # write csv data (will save attrs by default)
        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        # load csv file (header & data)
        header = []
        with open(path, 'r', encoding='utf-8') as file:
            while (line := file.readline().strip()).startswith('#'):
                header.append(line[1:])
        header = OmegaConf.create('\n'.join(header))
        actual_df = pd.read_csv(path, comment='#', encoding='utf-8', header=[0, 1])
        actual_df = quantify(actual_df)

        # compare saved comment header
        # (attrs is saved in a yaml compliant format, so we just check the structure)
        assert header == df.attrs

        # compare data frames
        tm.assert_frame_equal(actual_df, df)

    def test_discard_attributes(self, tmp_path):
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            )
        )
        df.attrs.update(
            dict(
                date='2024-04-27',
                trial=3,
                inlet=dict(flow_rate='1.0L/min', T='20^C'),
            )
        )
        path = tmp_path / 'data.csv'

        # write csv data (will save attrs by default)
        writer = DataFrameWriteCSV(attributes='discard')
        writer.run(df, path)
        assert path.exists()

        # load csv file (header & data)
        header = []
        with open(path, 'r') as file:
            while (line := file.readline().strip()).startswith('#'):
                header.append(line[1:])
        header = OmegaConf.create('\n'.join(header))
        actual_df = pd.read_csv(path, comment='#')

        # compare saved comment header
        # (attrs is saved in a yaml compliant format, so we just check the structure)
        assert header == {}

        # compare data frames
        tm.assert_frame_equal(actual_df, df)


class TestDataFrameCSVCache:
    def test_create(self):
        cache = DataFrameFileCache()

        assert cache.name == 'dataframe.cache'
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
        path = tmp_path / 'cache.hd5'
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
        path = tmp_path / 'cache.hd5'
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
        path = tmp_path / 'cache.hd5'
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

    def test_preserve_attrs(self, tmp_path):
        path = tmp_path / 'cache.hd5'
        df = pd.DataFrame(
            data=dict(
                A=[1.1, 2.2, 3.3],
                B=['aa', 'bb', 'cc'],
            ),
        )
        df.attrs.update(
            dict(date='2024-04-26', inlet=dict(flow_rate='1.0L/min', scale=2.0))
        )

        workflow = ProcessNode(
            ProcessNode(None, DelegatedSource(delegate=lambda: df), {}),
            DataFrameFileCache(),
            {
                'filename': PlainProcessParam(str(path)),
            },
        )

        # create cache
        assert not path.exists()
        workflow.run()

        # load cached version (by running process again)
        cached = workflow.run()
        assert cached is not df

        # assert content
        tm.assert_frame_equal(df, cached)
        assert df.attrs == cached.attrs
