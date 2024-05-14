from rdmlibpy.metadata import Metadata


class TestMetaGetResolver:
    def test_get_inherited_property(self):
        yaml = """
            date: 2024-01-16
            title: NH3 oxidation over Pt

            data:
              - id: 2024-01-16A
                start: a1234
                steps: &steps
                - loader: tclogger@v1
                  params:
                    start: ${meta.get:start}
                - loader: mksftir@v1
                  params:
                    date: ${meta.get:date}
        """
        meta = Metadata.create(yaml)
        assert meta.data[0].steps[0].params.start == 'a1234'
        assert meta.data[0].steps[1].params.date == '2024-01-16'
