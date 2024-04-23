from typing import Any, Dict, List


from ...dataframes.io import DataFrameReadCSVBase, ParseDatesType


class ChannelEurothermLoggerLoader(DataFrameReadCSVBase):
    name: str = 'channel.eurotherm'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    parse_dates: ParseDatesType = ['timestamp']
    date_format: str = 'ISO8601'
    options: Dict[str, Any] = dict(
        names=['timestamp', 'temperature'],
    )
