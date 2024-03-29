from pathlib import Path
import pytest
from metalib2 import Metadata


@pytest.fixture(scope='function')
def data_path():
    path = Path(__file__).parent / 'data'
    return path
