from pathlib import Path

import numpy as np
import pandas as pd
import pint_pandas

from rdmlibpy.loaders import ChannelTCLoggerLoader
from rdmlibpy.dataframes import DataFrameJoin, DataFrameSetIndex, DataFrameUnits


class TestDataFrameJoin:
    def test_create_loader(self):
        transform = DataFrameJoin()

        assert transform.name == 'dataframe.join'
        assert transform.version == '1'

    def _get_test_data(self):
        left = pd.DataFrame(
            data=dict(
                A=[0, 2, 4, 6, 8, 10, 12],
                B=[0, 1, 2, 3, 4, 5, 6],
            )
        ).set_index('A')
        right = pd.DataFrame(
            data=dict(
                A=[0, 1.5, 3, 4.5, 6, 7.5, 9, 10.5, 12],
                C=[0, 1, 2, 3, 4, 5, 6, 7, 8],
            )
        ).set_index('A')
        return left, right

    def test_join_outer_with_interpolation(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right, interpolate=True)

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 1.5, 2, 3, 4, 4.5, 6, 7.5, 8, 9, 10, 10.5, 12]
        assert np.allclose(
            df.B,
            [0, 0.75, 1, 1.5, 2, 2.25, 3, 3.75, 4, 4.5, 5, 5.25, 6],
            equal_nan=True,
        )
        assert np.allclose(
            df.C,
            [0, 1, 1 + 1 / 3, 2, 2 + 2 / 3, 3, 4, 5, 5 + 1 / 3, 6, 6 + 2 / 3, 7, 8],
            equal_nan=True,
        )

    def test_join_outer(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right)

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 1.5, 2, 3, 4, 4.5, 6, 7.5, 8, 9, 10, 10.5, 12]
        assert np.allclose(
            df.B,
            [0, np.nan, 1, np.nan, 2, np.nan, 3, np.nan, 4, np.nan, 5, np.nan, 6],
            equal_nan=True,
        )
        assert np.allclose(
            df.C,
            [0, 1, np.nan, 2, np.nan, 3, 4, 5, np.nan, 6, np.nan, 7, 8],
            equal_nan=True,
        )

    def test_join_right_with_interpolation(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right, how='right', interpolate=True)

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 1.5, 3, 4.5, 6, 7.5, 9, 10.5, 12]
        assert np.allclose(
            df.B,
            [0, 0.75, 1.5, 2.25, 3, 3.75, 4.5, 5.25, 6],
            equal_nan=True,
        )
        assert list(df.C) == [0, 1, 2, 3, 4, 5, 6, 7, 8]

    def test_join_right(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right, how='right')

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 1.5, 3, 4.5, 6, 7.5, 9, 10.5, 12]
        assert np.allclose(
            df.B,
            [0, np.nan, np.nan, np.nan, 3, np.nan, np.nan, np.nan, 6],
            equal_nan=True,
        )
        assert list(df.C) == [0, 1, 2, 3, 4, 5, 6, 7, 8]

    def test_join_left_with_interpolation(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right, how='left', interpolate=True)

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 2, 4, 6, 8, 10, 12]
        assert list(df.B) == [0, 1, 2, 3, 4, 5, 6]
        assert np.allclose(
            df.C, [0, 1 + 1 / 3, 2 + 2 / 3, 4, 5 + 1 / 3, 6 + 2 / 3, 8], equal_nan=True
        )

    def test_join_left(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()

        df = transform.run(left, right, how='left')

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 2, 4, 6, 8, 10, 12]
        assert list(df.B) == [0, 1, 2, 3, 4, 5, 6]
        assert np.allclose(
            df.C, [0, np.nan, np.nan, 4, np.nan, np.nan, 8], equal_nan=True
        )

    def test_join_pintarray_with_interpolation(self):
        transform = DataFrameJoin()
        left, right = self._get_test_data()
        right['C'] = pint_pandas.PintArray(right['C'], dtype='pint[m]')

        df = transform.run(left, right, how='left', interpolate=True)

        assert isinstance(df, pd.DataFrame)
        assert list(df.index) == [0, 2, 4, 6, 8, 10, 12]
        assert list(df.B) == [0, 1, 2, 3, 4, 5, 6]
        assert df.C.dtype == 'pint[m]'
        assert np.allclose(
            df.C.pint.m,
            [0, 1 + 1 / 3, 2 + 2 / 3, 4, 5 + 1 / 3, 6 + 2 / 3, 8],
            equal_nan=True,
        )


class TestDataFrameSetIndex:
    def test_create_loader(self):
        transform = DataFrameSetIndex()

        assert transform.name == 'dataframe.setindex'
        assert transform.version == '1'

    def test_process(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)

        transform = DataFrameSetIndex()
        df = transform.run(
            df,
            index_var=['timestamp'],
        )

        assert len(df) == 10  # type: ignore
        assert df.index.name == 'timestamp'


class TestDataFrameUnits:
    def test_create_loader(self):
        transform = DataFrameUnits()

        assert transform.name == 'dataframe.units'
        assert transform.version == '1'

    def test_process(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)
        df = df.set_index('timestamp')
        df = df[['sample-downstream', 'inlet', 'outlet']]

        transform = DataFrameUnits()
        df = transform.run(
            df,
            units={'sample-downstream': 'K'},
        )
        assert df['sample-downstream'].dtype == 'pint[K]'
        assert df['inlet'].dtype == 'float64'
        assert df['outlet'].dtype == 'float64'

    def test_process_with_default(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)
        df = df.set_index('timestamp')
        df = df[['sample-downstream', 'inlet', 'outlet']]

        transform = DataFrameUnits()
        df = transform.run(
            df,
            units={'sample-downstream': 'K'},
            default_unit='degC',
        )
        assert df['sample-downstream'].dtype == 'pint[K]'
        assert df['inlet'].dtype == 'pint[degC]'
        assert df['outlet'].dtype == 'pint[degC]'
