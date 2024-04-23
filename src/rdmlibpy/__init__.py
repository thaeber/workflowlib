# set package version
__version__ = '0.1.6'

import pint_pandas

from . import dataframes, loaders, serializers
from .process import DelegatedSource
from .registry import register
from .workflow import Workflow, run

# set default (short) format for saving/loading units
pint_pandas.PintType.ureg.default_format = "P~"
