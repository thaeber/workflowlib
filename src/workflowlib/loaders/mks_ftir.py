from typing import Any, Dict


from ..dataframes.loaders import DataFrameReadCSVBase, ParseDatesType


class MksFTIRLoader(DataFrameReadCSVBase):
    name: str = 'mks.ftir'
    version: str = '1'

    decimal: str = ','
    separator: str = '\t'
    parse_dates: ParseDatesType = {'timestamp': ['Date', 'Time']}
    date_format: str = '%d.%m.%Y %H:%M:%S,%f'
    options: Dict[str, Any] = dict(
        header='infer',
    )
