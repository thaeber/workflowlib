import pytest
from rdmlibpy.metadata import query
from rdmlibpy.metadata import MetadataDict, MetadataList
from itertools import count


class TestMetadataQuery:
    def test_load_sample_data(self, sample_data):
        assert sample_data is not None

    def test_defines(self, sample_data):
        # test if a key is defined on the selected node
        # (inherited keys are ignored)
        assert query(sample_data).defines('date')
        assert query(sample_data.inlet.composition).defines(['CH4', 'O2', 'N2'])

        # defines will fail on inherited keys
        assert query(sample_data.inlet).defines('date') == False

    def test_defines_throws_on_sequence(self, sample_data):
        # 'defines' is only defined on mapping nodes
        with pytest.raises(Exception):
            assert sample_data.data.defines('date')

    def test_keys(self, sample_data):
        # mapping keys
        assert query(sample_data.inlet.composition).keys() == ['CH4', 'O2', 'N2']

        # sequence indices
        assert query(sample_data.tags).keys() == [0, 1, 2]

    def test_values(self, sample_data):
        # mapping keys
        assert query(sample_data.inlet.composition).values() == ['3200ppm', '10%', '*']

        # sequence indices
        assert query(sample_data.tags).values() == [
            'CH4Oxidation',
            'Channel',
            'LightOff',
        ]

    def test_children(self, sample_data):
        # the `children` function only returns nodes that are key/value
        # mappings or sequences themselves

        # children contains one mapping
        items = query(sample_data.inlet).children()
        assert len(items) == 1
        assert type(items[0]) == MetadataDict

        # node is a sequence of mappings
        items = query(sample_data.data).children()
        assert len(items) == 6
        assert all([type(v) == MetadataDict for v in items])

        # node contains only leaf nodes (actual metadata values)
        items = query(sample_data.inlet.composition).children()
        assert len(items) == 0

    def test_children_with_recursion(self, sample_data):
        items = query(sample_data.data).children(recursive=True)
        assert len(items) == 6 * 6

    def test_find(self, sample_data):
        # by default `find` will recurse over the children of its children
        items = query(sample_data).find(lambda node: query(node).defines('tag'))
        assert (
            sum(1 for _ in items) == 7
        )  # combination of sum and generator avoids materializing a list

        # exclude parent node
        items = query(sample_data).find(
            lambda node: query(node).defines('tag'), include_self=False
        )
        assert sum(1 for _ in items) == 6

        # no recursion
        items = query(sample_data).find(
            lambda node: query(node).defines('tag'), recursive=False
        )
        assert sum(1 for _ in items) == 1

        # excluding parent node and disabling recursion should return an empty iterator
        # no recursion
        items = query(sample_data).find(
            lambda node: query(node).defines('tag'),
            include_self=False,
            recursive=False,
        )
        assert sum(1 for _ in items) == 0
