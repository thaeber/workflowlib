from __future__ import annotations

import os
from typing import (
    Any,
    Iterator,
    Protocol,
    Sequence,
    SupportsIndex,
    TypeVar,
    Union,
    overload,
)

# list-like

# from https://github.com/hauntsaninja/useful_types
# includes Sequence-like objects but excludes str and bytes
_T_co = TypeVar("_T_co", covariant=True)


class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co:
        ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]:
        ...

    def __contains__(self, value: object, /) -> bool:
        ...

    def __len__(self) -> int:
        ...

    def __iter__(self) -> Iterator[_T_co]:
        ...

    def index(self, value: Any, /, start: int = 0, stop: int = ...) -> int:
        ...

    def count(self, value: Any, /) -> int:
        ...

    def __reversed__(self) -> Iterator[_T_co]:
        ...


ListLike = Union[SequenceNotStr, range]


# filenames and file-like-objects
AnyStr_co = TypeVar("AnyStr_co", str, bytes, covariant=True)
AnyStr_contra = TypeVar("AnyStr_contra", str, bytes, contravariant=True)


class BaseBuffer(Protocol):
    @property
    def mode(self) -> str:
        # for _get_filepath_or_buffer
        ...

    def seek(self, __offset: int, __whence: int = ...) -> int:
        # with one argument: gzip.GzipFile, bz2.BZ2File
        # with two arguments: zip.ZipFile, read_sas
        ...

    def seekable(self) -> bool:
        # for bz2.BZ2File
        ...

    def tell(self) -> int:
        # for zip.ZipFile, read_stata, to_stata
        ...


class ReadBuffer(BaseBuffer, Protocol[AnyStr_co]):
    def read(self, __n: int = ...) -> AnyStr_co:
        # for BytesIOWrapper, gzip.GzipFile, bz2.BZ2File
        ...


class WriteBuffer(BaseBuffer, Protocol[AnyStr_contra]):
    def write(self, __b: AnyStr_contra) -> Any:
        # for gzip.GzipFile, bz2.BZ2File
        ...

    def flush(self) -> Any:
        # for gzip.GzipFile, bz2.BZ2File
        ...


class ReadCsvBuffer(ReadBuffer[AnyStr_co], Protocol):
    def __iter__(self) -> Iterator[AnyStr_co]:
        # for engine=python
        ...

    def fileno(self) -> int:
        # for _MMapWrapper
        ...

    def readline(self) -> AnyStr_co:
        # for engine=python
        ...

    @property
    def closed(self) -> bool:
        # for enine=pyarrow
        ...


FilePath = Union[str, os.PathLike]
