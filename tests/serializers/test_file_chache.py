from pathlib import Path

import numpy as np
import pandas as pd

from rdmlibpy.serializers import FileCache


class TestFileCache:
    def test_create_loader(self):
        cache = FileCache()

        assert cache.name == 'file.cache'
        assert cache.version == '1'
