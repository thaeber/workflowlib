from pathlib import Path

import numpy as np
import pandas as pd

from workflowlib.loaders import MksFTIRLoader


class TestMksFTIRLoader:
    def test_create_loader(self):
        loader = MksFTIRLoader()

        assert loader.name == 'mks.ftir'
        assert loader.version == '1'
        assert loader.decimal == ','
        assert loader.separator == '\t'
        assert loader.parse_dates == {'timestamp': ['Date', 'Time']}
        assert loader.date_format == '%d.%m.%Y %H:%M:%S,%f'
        assert loader.options == dict(
            header='infer',
        )

    def test_load_single(self, data_path: Path):
        loader = MksFTIRLoader()
        df = loader.run(
            source=data_path / 'mks_ftir/2024-01-16-conc.prn',
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 16  # type: ignore
        # assert df.columns[0] == 'timestamp'  # type: ignore
        # assert df.columns[1] == 'Spectrum'
        assert 'timestamp' in df.columns
        assert df['timestamp'].dtype == np.dtype('<M8[ns]')  # type: ignore
        assert df['timestamp'].iloc[0] == np.datetime64('2024-01-16T10:05:21')
