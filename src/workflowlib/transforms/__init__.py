from ..registry import register
from .dataframe import (
    DataFrameFillNA,
    DataFrameInterpolate,
    DataFrameJoin,
    DataFrameSetIndex,
    DataFrameUnits,
)
from .selection import SelectColumns, SelectTimespan

register(DataFrameFillNA())
register(DataFrameInterpolate())
register(DataFrameJoin())
register(DataFrameSetIndex())
register(DataFrameUnits())

register(SelectColumns())
register(SelectTimespan())
