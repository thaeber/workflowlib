from pathlib import Path
from typing import Any, ClassVar

from workflowlib.base import ProcessBase, ProcessNode
from workflowlib.process import Cache, Loader, Writer, DelegatedSource


class TestDelegatedSource:
    def test_create(self):
        source = DelegatedSource(delegate=lambda: 'test')

        assert source.name == 'delegated.source'
        assert source.version == '1'
        assert source.delegate is not None

    def test_evaluate_delegate(self):
        counter = 1
        source = DelegatedSource(delegate=lambda: counter)

        assert source.run() == 1

        counter = 2
        assert source.run() == 2


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


class TestCache:

    def test_logic_cache_is_valid(self):

        class MySource(ProcessBase):
            name: str = 'my_source'
            version: str = '1'
            counter: ClassVar[int] = 1

            def run(self, **kwargs) -> Any:
                return MySource.counter

        class MyCache(Cache):
            name: str = 'my_cache'
            version: str = '1'
            cached: ClassVar[None | int] = None

            def cache_is_valid(self):
                return MyCache.cached is not None

            def write(self, source: int):
                MyCache.cached = source

            def read(self):
                return MyCache.cached

        workflow = ProcessNode(
            ProcessNode(None, MySource(), {}),
            MyCache(),
            {},
        )

        # 1st run
        assert MyCache.cached == None
        assert workflow.run() == 1

        # 2nd run
        MySource.counter = 2  # force update of source
        assert workflow.run() == 1
        assert MySource.counter == 2
        assert MyCache.cached == 1

        # 3rd run
        MySource.counter = 3  # force update of source
        assert workflow.run() == 1
        assert MySource.counter == 3
        assert MyCache.cached == 1

    def test_logic_cache_is_not_valid(self):

        class MySource(ProcessBase):
            name: str = 'my_source'
            version: str = '1'
            counter: ClassVar[int] = 0

            def run(self, **kwargs) -> Any:
                MySource.counter += 1
                return MySource.counter

        class MyCache(Cache):
            name: str = 'my_cache'
            version: str = '1'
            cached: ClassVar[None | int] = None

            def cache_is_valid(self):
                return False

            def write(self, source: int):
                MyCache.cached = source

            def read(self):
                return MyCache.cached

        workflow = ProcessNode(
            ProcessNode(None, MySource(), {}),
            MyCache(),
            {},
        )

        # 1st run
        assert MyCache.cached == None
        assert workflow.run() == 1

        # 2nd run
        assert workflow.run() == 2
        assert MySource.counter == 2
        assert MyCache.cached == 2

        # 3rd run
        assert workflow.run() == 3
        assert MySource.counter == 3
        assert MyCache.cached == 3
