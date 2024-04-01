# %%
from pathlib import Path

import pandas as pd
from ._utils import _select_columns, _select_timespan

# %%


def load_tclogger_output_v1(source: str | Path, **kws):
    # load data as Dataframe
    filename = Path(source)
    df = pd.read_csv(filename, sep=';', index_col='timestamp', parse_dates=True)

    # select timespan
    df = _select_timespan(
        df,
        start=kws.get('start', None),
        stop=kws.get('stop', None),
    )

    # select specified columns
    if 'select' in kws:
        df = _select_columns(df, kws['select'])

    return df.to_xarray()
