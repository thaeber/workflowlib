# %%
from pathlib import Path
import pandas as pd

try:
    from ._utils import _select_columns, _select_timespan
except:
    from ch4oxlib.loaders._utils import _select_columns, _select_timespan


# %%
def load_mks_ftir_prn_v1(source: str | Path, decimal=',', separator='\t', **kws):
    filename = Path(source)
    df = pd.read_csv(
        filename,
        sep=separator,
        decimal=decimal,
        parse_dates={'timestamp': [1, 2]},
        dayfirst=True,
    )
    df.set_index('timestamp', inplace=True)

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


# df = load_mks_ftir_prn_v1(
#     r'\\os.lsdf.kit.edu\kit\itcp\projects\cathlen\2023-10-CH4-Oxidation\raw\2023-10-11\FTIR\2023-10-11-conc.prn'
# )
# df
# for col in df.columns:
#     print(col)

# %%
