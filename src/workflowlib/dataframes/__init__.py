from ..registry import register
from .io import DataFrameReadCSV, DataFrameWriteCSV
from .selection import SelectColumns, SelectTimespan
from .transforms import (
    DataFrameFillNA,
    DataFrameInterpolate,
    DataFrameJoin,
    DataFrameSetIndex,
    DataFrameUnits,
)

register(DataFrameReadCSV())

register(DataFrameFillNA())
register(DataFrameInterpolate())
register(DataFrameJoin())
register(DataFrameSetIndex())
register(DataFrameUnits())

register(SelectColumns())
register(SelectTimespan())

register(DataFrameWriteCSV())
