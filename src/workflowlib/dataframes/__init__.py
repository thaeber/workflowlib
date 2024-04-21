from ..registry import register
from .loaders import DataFrameReadCSV
from .selection import SelectColumns, SelectTimespan
from .transforms import (
    DataFrameFillNA,
    DataFrameInterpolate,
    DataFrameJoin,
    DataFrameSetIndex,
    DataFrameUnits,
)
from .writers import DataFrameWriteCSV

register(DataFrameReadCSV())

register(DataFrameFillNA())
register(DataFrameInterpolate())
register(DataFrameJoin())
register(DataFrameSetIndex())
register(DataFrameUnits())

register(SelectColumns())
register(SelectTimespan())

register(DataFrameWriteCSV())
