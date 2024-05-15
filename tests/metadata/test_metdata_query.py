from itertools import count

import pytest

from rdmlibpy.metadata import Metadata, MetadataDict, MetadataList, query


class TestMetadataQuery:
    def test_defines(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # test if a key is defined on the selected node
        # (inherited keys are ignored)
        assert query(sample_data).defines('date')
        assert query(sample_data.inlet.composition).defines(['CH4', 'O2', 'N2'])

        # defines will fail on inherited keys
        assert query(sample_data.inlet).defines('date') == False

    def test_defines_throws_on_sequence(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # 'defines' is only defined on mapping nodes
        with pytest.raises(Exception):
            assert sample_data.data.defines('date')

    def test_keys(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # mapping keys
        assert query(sample_data.inlet.composition).keys() == ['CH4', 'O2', 'N2']

        # sequence indices
        assert query(sample_data.tags).keys() == [0, 1, 2]

    def test_values(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # mapping keys
        assert query(sample_data.inlet.composition).values() == ['3200ppm', '10%', '*']

        # sequence indices
        assert query(sample_data.tags).values() == [
            'CH4-oxidation',
            'channel',
            'light-off',
        ]

    def test_children(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )
        # the `children` function only returns nodes that are key/value
        # mappings or sequences themselves

        # children contains one mapping
        items = query(sample_data.inlet).children()
        assert len(items) == 1
        assert type(items[0]) == MetadataDict

        # node is a sequence of mappings
        items = query(sample_data.data).children()
        assert len(items) == 2
        assert all([type(v) == MetadataDict for v in items])

        # node contains only leaf nodes (actual metadata values)
        items = query(sample_data.inlet.composition).children()
        assert len(items) == 0

    def test_children_with_recursion(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        items = query(sample_data).children(recursive=True)
        assert len(items) == 6

    def test_find(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tag='root',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[
                    dict(id='A', tag='light-off'),
                    dict(id='B', tag='light-out'),
                ],
            ),
        )

        # by default `find` will recurse over the children of its children
        items = query(sample_data).find(lambda node: query(node).defines('tag'))
        assert (
            sum(1 for _ in items) == 3
        )  # combination of sum and generator avoids materializing a list

        # exclude parent node
        items = query(sample_data).find(
            lambda node: query(node).defines('tag'), include_self=False
        )
        assert sum(1 for _ in items) == 2

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

    def test_find_ignores_private_keys_by_default(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tag='root',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                __process__=dict(id='process'),
                data=[
                    dict(id='A', tag='light-off'),
                    dict(id='B', tag='light-out'),
                ],
            ),
        )

        # by default `find` will recurse over the children of its children
        items = list(
            query(sample_data).find(lambda node: query(node).defines('id')),
        )
        assert len(items) == 2

        assert items[0].id == 'A'
        assert items[1].id == 'B'

    def test_find_include_private_keys(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tag='root',
                tags=['CH4-oxidation', 'channel', 'light-off'],
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                __process__=dict(id='process'),
                data=[
                    dict(id='A', tag='light-off'),
                    dict(id='B', tag='light-out'),
                ],
            ),
        )

        # by default `find` will recurse over the children of its children
        items = list(
            query(sample_data, include_private_keys=True).find(
                lambda node: query(node).defines('id')
            ),
        )
        assert len(items) == 3

        assert items[0].id == 'process'
        assert items[1].id == 'A'
        assert items[2].id == 'B'
