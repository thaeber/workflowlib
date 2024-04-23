from typing import Dict, List, Mapping, Sequence

import numpy as np
import pandas as pd

from ..process import Transform


class SelectColumns(Transform):
    name: str = 'dataframe.select.columns'
    version: str = '1'

    def run(
        self,
        source: pd.DataFrame,
        select: None | str | List[str] | Dict[str, str] = None,
    ):
        if isinstance(select, str):
            return source[[select]]
        elif isinstance(select, Sequence):
            return source[list(select)]
        elif isinstance(select, Mapping):
            return source[list(select.keys())].rename(columns=dict(select))
        else:
            return source


class SelectTimespan(Transform):
    name: str = 'dataframe.select.timespan'
    version: str = '1'

    def run(self, source: pd.DataFrame, column: str, start=None, stop=None):
        col = source[column]
        if (start is not None) and (stop is not None):
            start = np.datetime64(start)
            stop = np.datetime64(stop)
            return source.loc[(start <= col) & (col <= stop)]
        elif start is None:
            stop = np.datetime64(stop)
            return source.loc[col <= stop]
        elif stop is None:
            start = np.datetime64(start)
            return source.loc[start <= col]
        else:
            return source
