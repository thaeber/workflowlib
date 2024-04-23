from ..registry import register
from .channel.eurotherm import ChannelEurothermLoggerLoader
from .channel.tclogger import ChannelTCLoggerLoader
from .hiden_rga import HidenRGALoader
from .mks_ftir import MksFTIRLoader

register(ChannelEurothermLoggerLoader())
register(ChannelTCLoggerLoader())
register(HidenRGALoader())
register(MksFTIRLoader())
