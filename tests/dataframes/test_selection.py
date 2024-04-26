from pathlib import Path

import pandas as pd

from rdmlibpy.loaders import ChannelTCLoggerLoader
from rdmlibpy.dataframes import SelectColumns, SelectTimespan


class TestSelectColumns:
    def test_create_loader(self):
        transform = SelectColumns()

        assert transform.name == 'dataframe.select.columns'
        assert transform.version == '1'

    def test_select_single(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)

        transform = SelectColumns()
        df = transform.run(
            df,
            select='sample-downstream',
        )

        assert len(df.columns) == 1
        assert list(df.columns) == ['sample-downstream']

    def test_select_multiple(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)

        transform = SelectColumns()
        df = transform.run(
            df,
            select=['sample-downstream', 'timestamp'],
        )

        assert len(df.columns) == 2
        assert list(df.columns) == ['sample-downstream', 'timestamp']

    def test_select_and_rename(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)

        transform = SelectColumns()
        df = transform.run(
            df,
            select={
                'sample-downstream': 'sample',
                'timestamp': 'timestamp',
            },
        )

        assert len(df.columns) == 2
        assert list(df.columns) == ['sample', 'timestamp']


class TestSelectTimespan:
    def test_create_loader(self):
        loader = SelectTimespan()

        assert loader.name == 'dataframe.select.timespan'
        assert loader.version == '1'

    def test_process(self, data_path: Path):
        loader = ChannelTCLoggerLoader()
        df = loader.run(
            source=data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        )
        assert isinstance(df, pd.DataFrame)

        transform = SelectTimespan()
        df = transform.run(
            df,
            'timestamp',
            start='2024-01-16T11:26:54.9',
            stop='2024-01-16T11:26:55.6',
        )

        assert len(df) == 4  # type: ignore
