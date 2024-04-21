from typing import Any, Dict, List


from ...dataframes.loaders import DataFrameReadCSVBase, ParseDatesType


class ChannelTCLoggerLoader(DataFrameReadCSVBase):
    name: str = 'channel.tclogger'
    version: str = '1'

    decimal: str = '.'
    separator: str = ';'
    parse_dates: ParseDatesType = ['timestamp']
    date_format: str = 'ISO8601'
    options: Dict[str, Any] = dict(
        header='infer',
    )
