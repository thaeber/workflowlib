from rdmlibpy.metadata import MetadataDict, MetadataList


class TestAccessors:
    def test_load_sample_data(self, sample_data):
        assert sample_data is not None

    def test_mapping_node_type(self, sample_data):
        # accessing a mapping object (dict like) should return
        # a mapping node
        assert isinstance(sample_data, MetadataDict)
        assert isinstance(sample_data.inlet, MetadataDict)
        assert isinstance(sample_data.data[0], MetadataDict)

    def test_sequence_node_type(self, sample_data):
        # accessing a sequence object (list like) should return
        # a sequence node
        assert isinstance(sample_data.data, MetadataList)

    def test_leaf_node_type(self, sample_data):
        # accessing a leaf node, that is, an actual metadata
        # value returns the value itself
        assert isinstance(sample_data.inlet.flow_rate, str)

    def test_access_existing_element(self, sample_data):
        # access by indexer
        assert sample_data['date'] == '2023-10-10'

        # access by attribute
        assert sample_data.date == '2023-10-10'

    def test_access_inherited_element(self, sample_data):
        # access by indexer
        assert sample_data['inlet']['date'] == '2023-10-10'

        # access by attribute
        assert sample_data.inlet.date == '2023-10-10'

    def test_access_list_element_by_index(self, sample_data):
        assert sample_data.data[0] is not None
        assert sample_data.data[0].id == '2023-10-10A'

    def test_access_inherited_element_from_list_node(self, sample_data):
        # access by indexer
        assert sample_data.data['date'] == '2023-10-10'

        # access by attribute
        assert sample_data.data.date == '2023-10-10'

    def test_overwrite_inherited_key(self, sample_data):
        # re-defined keys will overwrite inherited values

        # 'tag' defined on root node
        assert sample_data.tag == 'on root node'

        # 'tag' inherited from root node
        assert sample_data.data.tag == 'on root node'

        # 'tag' overridden on child node
        assert sample_data.data[0].tag == 'light-off'

    def test_access_key_which_collides_with_function_name(self, sample_data):
        assert sample_data.defines == "Collision with function name"


class TestIteration:
    def test_len(self, sample_data):
        # len of key/value map
        assert len(sample_data.inlet.composition) == 3

        # len of sequence
        assert len(sample_data.data) == 6

    def test_iteration_yields_key_value_pair(self, sample_data):
        # iteration will always return key/value pairs

        # iterate key/value map
        items = list(iter(sample_data.inlet.composition))
        assert items == [('CH4', '3200ppm'), ('O2', '10%'), ('N2', '*')]

        # iterate sequence
        items = list(iter(sample_data.tags))
        assert items == [(0, 'CH4Oxidation'), (1, 'Channel'), (2, 'LightOff')]

    def test_iterations_wraps_child_containers(self, sample_data):
        # during iteration, child containers (mapping, sequence) will
        # be wrapped in a corresponding MetadataNode instance

        # iterate key/value map
        types = [type(value) for _, value in sample_data.inlet]
        assert types == [str, str, MetadataDict]

        # iterate sequence
        types = [type(value) for _, value in sample_data.data]
        assert all([v == MetadataDict for v in types])
