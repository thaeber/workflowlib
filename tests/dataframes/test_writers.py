from pathlib import Path

import pandas as pd

from workflowlib.dataframes import DataFrameReadCSV, DataFrameWriteCSV


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

    def test_ensure_csv_roundtrip(self, tmp_path: Path):
        # check a successful write/read with default settings
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
        path = tmp_path / 'data.csv'

        writer = DataFrameWriteCSV()
        writer.run(df, path)
        assert path.exists()

        reader = DataFrameReadCSV()
        df2 = reader.run(path)

        assert all(df.columns == df2.columns)
        assert all(df == df2)
