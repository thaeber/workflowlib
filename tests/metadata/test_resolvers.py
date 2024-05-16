import numpy as np
import pytest

from rdmlibpy.metadata import Metadata
from rdmlibpy.metadata.resolvers import _parse_time_delta_string


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


class TestMetaSubtractTimeDelta:
    def test_string_parsing(self):
        assert _parse_time_delta_string('1m') == np.timedelta64(1, 'm')
        assert _parse_time_delta_string('1 m') == np.timedelta64(1, 'm')
        assert _parse_time_delta_string('  1 m  ') == np.timedelta64(1, 'm')

        assert _parse_time_delta_string('1D') == np.timedelta64(1, 'D')
        assert _parse_time_delta_string('1m') == np.timedelta64(1, 'm')
        assert _parse_time_delta_string('2s') == np.timedelta64(2, 's')
        assert _parse_time_delta_string('20ms') == np.timedelta64(20, 'ms')
        assert _parse_time_delta_string('20us') == np.timedelta64(20, 'us')
        assert _parse_time_delta_string('20ns') == np.timedelta64(20, 'ns')
        assert _parse_time_delta_string('20ps') == np.timedelta64(20, 'ps')
        assert _parse_time_delta_string('20fs') == np.timedelta64(20, 'fs')
        assert _parse_time_delta_string('20as') == np.timedelta64(20, 'as')

        with pytest.raises(ValueError):
            _parse_time_delta_string('')

        with pytest.raises(ValueError):
            _parse_time_delta_string('   ')

        with pytest.raises(ValueError):
            _parse_time_delta_string('m')

        with pytest.raises(ValueError):
            _parse_time_delta_string('1')

        with pytest.raises(ValueError):
            _parse_time_delta_string('1Y')

    def test_subtract_time_delta(self):
        meta = Metadata.create(
            """
            date: 2024-01-16
            title: NH3 oxidation over Pt

            start: ${meta.subtract-timedelta:2024-01-16T12:00,12m}
            """
        )
        assert meta.start == '2024-01-16T11:48'

    def test_subtract_time_delta2(self):
        meta = Metadata.create(
            """
            date: 2024-01-16
            title: NH3 oxidation over Pt

            start: ${meta.subtract-timedelta:2024-01-16T12:00,12ms}
            """
        )
        assert meta.start == '2024-01-16T11:59:59.988'

    def test_nested_resolvers(self):
        meta = Metadata.create(
            """
            date: 2024-01-16
            title: NH3 oxidation over Pt

            start: 2024-01-16T12:00
            data:
              - nested: ${meta.subtract-timedelta:${meta.get:start},12m}
            """
        )
        assert meta.data[0].nested == '2024-01-16T11:48'
