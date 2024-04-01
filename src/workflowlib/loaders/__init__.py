from ..registry import register
from .csv_loader import (
    ChannelEurothermLoggerLoader,
    ChannelTCLoggerLoader,
    CSVLoader,
    HidenRGALoader,
    MksFTIRLoader,
)

register(CSVLoader())
register(ChannelTCLoggerLoader())
register(ChannelEurothermLoggerLoader())
register(HidenRGALoader())
register(MksFTIRLoader())
