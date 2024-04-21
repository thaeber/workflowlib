from pathlib import Path

import pandas as pd
from workflowlib.dataframes import DataFrameWriteCSV


class TestDataFrameWriteCSV:
    def test_create_loader(self):
        loader = DataFrameWriteCSV()

        assert loader.name == 'dataframe.write_csv'
        assert loader.version == '1'
        assert loader.decimal == '.'
        assert loader.separator == ','

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
