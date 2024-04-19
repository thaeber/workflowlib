from pathlib import Path

from workflowlib.base import Loader


class TestLoader:
    def test_single_source(self, data_path: Path):
        loader = Loader(
            name='loader',
            version='v1.0',
        )

        sources = list(
            loader.glob(
                source=data_path / 'eurotherm/20240118T084901.txt',
            )
        )
        assert sources == [data_path / 'eurotherm/20240118T084901.txt']

    def test_source_with_wildcard(self, data_path: Path):
        loader = Loader(
            name='loader',
            version='v1.0',
        )

        sources = list(
            loader.glob(
                source=data_path / 'eurotherm/*.txt',
            )
        )
        assert len(sources) == 4
        assert sorted(sources) == sorted(
            [
                data_path / 'eurotherm/20240118T084901.txt',
                data_path / 'eurotherm/20240118T085901.txt',
                data_path / 'eurotherm/20240118T090901.txt',
                data_path / 'eurotherm/20240118T091901.txt',
            ]
        )

    def test_source_with_recursion(self, data_path: Path):
        loader = Loader(
            name='loader',
            version='v1.0',
        )

        sources = list(
            loader.glob(
                source=data_path / 'eurotherm/subfolder/**/*.txt',
            )
        )
        assert len(sources) == 4
        assert sorted(sources) == sorted(
            [
                data_path / 'eurotherm/subfolder/a/20240118T084901.txt',
                data_path / 'eurotherm/subfolder/b/20240118T085901.txt',
                data_path / 'eurotherm/subfolder/c/20240118T090901.txt',
                data_path / 'eurotherm/subfolder/d/20240118T091901.txt',
            ]
        )
