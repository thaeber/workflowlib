from rdmlibpy.metadata import Metadata, MetadataDict, MetadataList


class TestAccessors:
    def test_mapping_node_type(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # accessing a mapping object (dict like) should return
        # a mapping node
        assert isinstance(sample_data, MetadataDict)
        assert isinstance(sample_data.inlet, MetadataDict)
        assert isinstance(sample_data.data[0], MetadataDict)

    def test_sequence_node_type(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # accessing a sequence object (list like) should return
        # a sequence node
        assert isinstance(sample_data.data, MetadataList)

    def test_leaf_node_type(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # accessing a leaf node, that is, an actual metadata
        # value returns the value itself
        assert isinstance(sample_data.inlet.flow_rate, str)

    def test_access_existing_element(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # access by indexer
        assert sample_data['date'] == '2024-05-14'

        # access by attribute
        assert sample_data.date == '2024-05-14'

    def test_access_inherited_element(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # access by indexer
        assert sample_data['inlet']['date'] == '2024-05-14'

        # access by attribute
        assert sample_data.inlet.date == '2024-05-14'

    def test_access_list_element_by_index(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        assert sample_data.data[0] is not None
        assert sample_data.data[0].id == 'A'

    def test_access_inherited_element_from_list_node(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # access by indexer
        assert sample_data.data['date'] == '2024-05-14'

        # access by attribute
        assert sample_data.data.date == '2024-05-14'

    def test_overwrite_inherited_key(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tag='on root node',
                inlet=dict(flow_rate='1.0L/min', temperature='293K'),
                data=[
                    dict(id='A', tag='light-off'),
                    dict(id='B', tag='light-out'),
                ],
            ),
        )

        # re-defined keys will overwrite inherited values

        # 'tag' defined on root node
        assert sample_data.tag == 'on root node'

        # 'tag' inherited from root node
        assert sample_data.data.tag == 'on root node'

        # 'tag' overridden on child node
        assert sample_data.data[0].tag == 'light-off'

    def test_in_operator_for_mapping_node(self):
        sample_data = Metadata(
            dict(CH4='3200ppm', O2='10%', N2='*'),
        )

        contains_Ar = 'Ar' in sample_data
        assert contains_Ar == False

        contains_CH4 = 'CH4' in sample_data
        assert contains_CH4 == True

        does_not_contain_NH3 = 'NH3' not in sample_data
        assert does_not_contain_NH3 == True

        does_not_contain_CH4 = 'CH4' not in sample_data
        assert does_not_contain_CH4 == False

    def test_in_operator_for_list_node(self):
        sample_data = Metadata(
            ['CH4', 'O2', 'N2'],
        )

        contains_Ar = 'Ar' in sample_data
        assert contains_Ar == False

        contains_CH4 = 'CH4' in sample_data
        assert contains_CH4 == True

        does_not_contain_NH3 = 'NH3' not in sample_data
        assert does_not_contain_NH3 == True

        does_not_contain_CH4 = 'CH4' not in sample_data
        assert does_not_contain_CH4 == False


class TestIteration:
    def test_len(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(NO='400ppm', CO='800ppm', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # len of key/value map
        assert len(sample_data.inlet.composition) == 3

        # len of sequence
        assert len(sample_data.data) == 2

    def test_iterate_mapping_node(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # iteration of mapping node yields the keys
        # (similar to normal dict objects)
        items = list(iter(sample_data.inlet.composition))
        assert items == ['CH4', 'O2', 'N2']

    def test_iterate_list_node(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
            ),
        )

        # iteration of list node yields the values
        # (similar to normal list objects)
        items = list(iter(sample_data.tags))
        assert items == ['CH4-oxidation', 'channel', 'light-off']

    def test_items_of_mapping_node(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                inlet=dict(
                    flow_rate='1.0L/min',
                    temperature='293K',
                    composition=dict(CH4='3200ppm', O2='10%', N2='*'),
                ),
                data=[dict(id='A'), dict(id='B')],
            ),
        )

        # iterate key/value pairs
        # (similar to normal dict objects)
        items = list(iter(sample_data.inlet.composition.items()))
        assert items == [('CH4', '3200ppm'), ('O2', '10%'), ('N2', '*')]

    def test_items_of_list_node(self):
        sample_data = Metadata(
            dict(
                date='2024-05-14',
                tags=['CH4-oxidation', 'channel', 'light-off'],
            ),
        )

        # iterate over index/value pairs
        # (convenience to obtain)
        items = list(iter(sample_data.tags.items()))
        assert items == [(0, 'CH4-oxidation'), (1, 'channel'), (2, 'light-off')]

    def test_mapping_iteration_wraps_child_containers(self):
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
        # during iteration, child containers (mapping, sequence) will
        # be wrapped in a corresponding MetadataNode instance

        # iterate key/value map
        types = [type(value) for _, value in sample_data.inlet.items()]
        assert types == [str, str, MetadataDict]

    def test_list_iteration_wraps_child_containers(self):
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
        # during iteration, child containers (mapping, sequence) will
        # be wrapped in a corresponding MetadataNode instance

        # iterate sequence
        types = [type(value) for value in sample_data.data]
        assert types == [MetadataDict, MetadataDict]
