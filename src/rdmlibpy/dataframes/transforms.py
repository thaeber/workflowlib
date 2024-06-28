from typing import Iterable, List, Literal, Mapping

import numpy as np
import pandas as pd
import pint_pandas
from omegaconf import OmegaConf
from pandas._typing import JoinHow

from ..process import Transform


class DataFrameSetIndex(Transform):
    name: str = 'dataframe.setindex'
    version: str = '1'
    sort: bool = True

    def run(
        self,
        source: pd.DataFrame,
        index_var: None | str | List[str] = None,
    ):
        if index_var is None:
            return source
        else:
            df = source.set_index(index_var)
            if self.sort:
                df.sort_index(inplace=True)
            return df


class DataFrameJoin(Transform):
    name: str = 'dataframe.join'
    version: str = '1'

    def interpolate(self, df: pd.DataFrame):
        # check if indices are datetime64
        if np.issubdtype(df.index.dtype, np.datetime64):  # type: ignore
            x = (df.index - df.index[0]).total_seconds()
        else:
            x = df.index

        # interpolate columns
        for col in df.columns:
            if isinstance(df[col].values, pint_pandas.PintArray):
                isnan = np.isnan(df[col].pint.m)
                xp = x[~isnan]
                yp = df[col].pint.m[~isnan]
                try:
                    df[col] = pint_pandas.PintArray(
                        np.interp(x, xp, yp), dtype=df[col].dtype
                    )
                except ValueError:
                    values = df[col].pint.m.values
                    df[col] = pint_pandas.PintArray(
                        np.full_like(values, np.nan), dtype=df[col].dtype
                    )
            else:
                isnan = np.isnan(df[col])
                xp = x[~isnan]
                yp = df[col][~isnan]
                try:
                    df[col] = np.interp(x, xp, yp)
                except ValueError:
                    df[col] = np.full_like(df[col].values, np.nan)

        return df

    def run(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        how: JoinHow = 'outer',
        interpolate: bool = False,
    ):
        if interpolate:
            # joined = left.join(right, how='outer').interpolate(method='index')
            joined = left.join(right, how='outer')
            joined = self.interpolate(joined)
            if how == 'left':
                return left[[]].join(joined, how='left')
            elif how == 'right':
                return right[[]].join(joined, how='left')
            elif how in ['inner', 'outer']:
                return joined
            else:
                return left.join(right, how=how).interpolate()
        else:
            return left.join(right, how=how)


class DataFrameInterpolate(Transform):
    name: str = 'dataframe.interpolate'
    version: str = '1'

    def run(
        self,
        df: pd.DataFrame,
        include: None | Iterable[str] = None,
        exclude: None | Iterable[str] = None,
    ):
        # check if indices are datetime64
        if np.issubdtype(df.index.dtype, np.datetime64):  # type: ignore
            x = (df.index - df.index[0]).total_seconds()
        else:
            x = df.index

        if not include:
            include = list(df.columns)
        if not exclude:
            exclude = []

        # interpolate columns
        for col in include:
            if col in exclude:
                continue
            if isinstance(df[col].values, pint_pandas.PintArray):
                isnan = np.isnan(df[col].pint.m)
                xp = x[~isnan]
                yp = df[col].pint.m[~isnan]
                try:
                    df[col] = pint_pandas.PintArray(
                        np.interp(x, xp, yp), dtype=df[col].dtype
                    )
                except ValueError:
                    df[col] = pint_pandas.PintArray(
                        np.full_like(df[col].pint.m.values, np.nan), dtype=df[col].dtype
                    )
            else:
                isnan = np.isnan(df[col])
                xp = x[~isnan]
                yp = df[col][~isnan]
                try:
                    df[col] = np.interp(x, xp, yp)
                except ValueError:
                    df[col] = np.full_like(df[col].values, np.nan)

        return df


FillMethod = Literal['forward', 'backward']


class DataFrameFillNA(Transform):
    name: str = 'dataframe.fillna'
    version: str = '1'

    def run(
        self,
        df: pd.DataFrame,
        include: None | Iterable[str] = None,
        exclude: None | Iterable[str] = None,
        method: FillMethod = 'forward',
    ):
        if not include:
            include = list(df.columns)
        if not exclude:
            exclude = []

        # interpolate columns
        for col in include:
            if col in exclude:
                continue
            if method == 'forward':
                df[col] = df[col].ffill()
            elif method == 'backward':
                df[col] = df[col].bfill()
            else:
                raise ValueError(f'Invalid fill method: {method}')

        return df


class DataFrameUnits(Transform):
    name: str = 'dataframe.units'
    version: str = '1'

    def run(
        self,
        source: pd.DataFrame,
        units: Mapping[str, str] | None = None,
        default_unit: str | None = None,
    ):
        def set_unit(col, unit):
            source[col] = pint_pandas.PintArray(source[col], dtype=f'pint[{unit}]')

        for col in source.columns:
            if (units is not None) and (col in units):
                set_unit(col, units[col])
            elif default_unit is not None:
                set_unit(col, default_unit)
            else:
                # do nothing
                pass
        return source


class DataFrameAttributes(Transform):
    name: str = 'dataframe.set.attrs'
    version: str = '1'

    def run(self, source: pd.DataFrame, **kwargs):
        # make deep copy of attributes
        # (roundtrip serialization to yaml)
        attrs = OmegaConf.to_object(
            OmegaConf.create(
                OmegaConf.to_yaml(
                    OmegaConf.create(kwargs),
                ),
            ),
        )

        source.attrs.update(attrs)  # type: ignore
        return source
