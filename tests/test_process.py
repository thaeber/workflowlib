from pathlib import Path

from workflowlib.process import Loader, Writer


class TestLoader:
    def test_single_source(self, data_path: Path):
        sources = list(
            Loader.glob(
                source=data_path / 'eurotherm/20240118T084901.txt',
            )
        )
        assert sources == [data_path / 'eurotherm/20240118T084901.txt']

    def test_source_with_wildcard(self, data_path: Path):
        sources = list(
            Loader.glob(
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
        sources = list(
            Loader.glob(
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


class TestWriter:
    def test_create_path_via_ensure_path(self, tmp_path: Path):
        filepath = tmp_path / 'dummy/ascii.csv'

        # check path is not present
        assert not filepath.parent.exists()

        # create path
        result = Writer.ensure_path(filepath)

        assert result == filepath
        assert filepath.parent.exists()

    def test_calling_ensure_path_with_existing_path(self, tmp_path: Path):
        filepath = tmp_path / 'existing/ascii.csv'
        filepath.parent.mkdir(parents=True)

        # check path is not present
        assert filepath.parent.exists()

        # calling ensure_path with existing path
        result = Writer.ensure_path(filepath)

        assert result == filepath
        assert filepath.parent.exists()
