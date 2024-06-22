from ..registry import register
from .channel.eurotherm import (
    ChannelEurothermLoggerLoader,
    ChannelEurothermLoggerLoaderV1_1,
)
from .channel.tclogger import ChannelTCLoggerLoader
from .hiden_rga import HidenRGALoader
from .mks_ftir import MksFTIRLoader

register(ChannelEurothermLoggerLoader())
register(ChannelEurothermLoggerLoaderV1_1())
register(ChannelTCLoggerLoader())
register(HidenRGALoader())
register(MksFTIRLoader())
