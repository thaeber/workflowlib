import os
from pathlib import Path
import pytest
from rdmlibpy.metadata import Metadata


@pytest.fixture(scope='function')
def data_path():
    path = Path(__file__).parent / 'data'
    return path.absolute()


@pytest.fixture(scope='function')
def sample_data():
    """Loads sample metadata"""
    # setup environment variable used by sample data
    os.environ['RAW_DATA_PATH'] = '.'

    # load sample data
    filename = Path(__file__).parent / 'data/metadata/2023-10-10.yaml'
    metadata = Metadata.load_yaml(filename)
    return metadata
