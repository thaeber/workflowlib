from pathlib import Path

import pandas as pd

from rdmlibpy.serializers import PandasDataFrameSerializer


class TestPandasDataFrameSerializer:
    def test_create(self):
        serializer = PandasDataFrameSerializer()

        assert serializer.name == 'pandas.dataframe'
        assert serializer.version == '1'
        assert serializer.format == 'csv'

    def test_load_csv(self, data_path: Path):
        serializer = PandasDataFrameSerializer(options=dict(sep=';'))
        df = serializer.load(data_path / 'dataframe/test_data.csv')

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['A', 'B', 'C']
        assert list(df.A) == [0, 1, 2]
        assert list(df.B) == ['a', 'b', 'c']
        assert list(df.C) == [100, 200, 300]

    def test_load_hdf5(self, data_path: Path):
        serializer = PandasDataFrameSerializer(format='HDF5')
        df = serializer.load(data_path / 'dataframe/test_data.hd5')

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['A', 'B', 'C']
        assert list(df.A) == [0, 1, 2]
        assert list(df.B) == ['a', 'b', 'c']
        assert list(df.C) == [100, 200, 300]

    def test_write_csv(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = PandasDataFrameSerializer(format='csv', options=dict(sep=';'))
        serializer.write(df, path)

        assert path.exists()
        df = pd.read_csv(path, sep=';', encoding='utf-8')
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_write_hdf5(self, tmp_path: Path):
        df = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.hd5'

        serializer = PandasDataFrameSerializer(format='HDF5', options=dict(key='dummy'))
        serializer.write(df, path)

        assert path.exists()
        df = pd.read_hdf(path, key='dummy')
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_roundtrip_csv(self, tmp_path: Path):
        source = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = PandasDataFrameSerializer(format='csv')
        serializer.write(source, path)
        df = serializer.load(path)

        assert path.exists()
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']

    def test_roundtrip_hdf5(self, tmp_path: Path):
        source = pd.DataFrame(data=dict(A=[1.1, 2.2, 3.3], B=['aa', 'bb', 'cc']))
        path = tmp_path / 'data.csv'

        serializer = PandasDataFrameSerializer(format='HDF5')
        serializer.write(source, path)
        df = serializer.load(path)

        assert path.exists()
        assert list(df.A) == [1.1, 2.2, 3.3]
        assert list(df.B) == ['aa', 'bb', 'cc']
