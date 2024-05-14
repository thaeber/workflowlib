# %%
from __future__ import annotations

import collections.abc
from pathlib import Path
from typing import Any, ByteString, Mapping, Optional, Sequence, Type

from omegaconf import OmegaConf


# %%
def _get_type_of(class_or_object: Any) -> Type[Any]:
    type_ = class_or_object
    if not isinstance(type_, type):
        type_ = type(class_or_object)
    assert isinstance(type_, type)
    return type_


class MetadataNode:
    def __init__(self, parent: Optional['MetadataNode'], container):
        self._parent: Optional[MetadataNode] = parent
        self._container: Any = container

    @staticmethod
    def _wrap_container(
        parent: Optional[MetadataNode], container: Mapping[str, Any] | Sequence[Any]
    ) -> MetadataNode:
        if isinstance(container, Mapping):
            return MetadataDict(parent, container)
        elif isinstance(container, Sequence):
            return MetadataList(parent, container)
        else:
            raise ValueError('Container must be a mapping or a sequence.')

    @staticmethod
    def _needs_wrapping(container: Any):
        if isinstance(container, (str, ByteString)):
            # special treatment of str/bytes since it would be treated as a sequence of characters
            return False
        elif isinstance(container, Mapping) or isinstance(container, Sequence):
            return True
        else:
            return False

    @staticmethod
    def _get_child_node_impl(parent: Optional[MetadataNode], item: Any):
        if isinstance(item, (str, ByteString)):
            return item
        if isinstance(item, Mapping):
            return MetadataDict(parent, item)
        elif isinstance(item, Sequence):
            return MetadataList(parent, item)
        else:
            return item

    def _wrap_child_node(self, item: Any):
        if MetadataNode._needs_wrapping(item):
            return MetadataNode._wrap_container(self, item)
        else:
            return item

    def _try_get_item(self, key: str | int):
        try:
            return self._container[key]
        except:
            return None

    def _iter_inheritance_chain(self, key: str | int):
        # yield item on current node
        if (item := self._try_get_item(key)) is not None:
            yield item

        # yield items on parents
        node = self._parent
        while node is not None:
            if (
                isinstance(node, MetadataDict)
                and (item := node._try_get_item(key)) is not None
            ):
                yield item

            # move up the inheritance chain
            node = node._parent

    def _get_impl(
        self, key: str | int, *, inherit: bool = True, merge: bool = False
    ) -> Any:
        if not inherit:
            # no inheritance, just return the value on the current node
            item = self._container[key]
        elif not merge:
            items = self._iter_inheritance_chain(key)
            try:
                item = next(items)
            except StopIteration:
                # there is no key with that name/value, just return None
                item = None
        else:
            # handle merging
            raise NotImplementedError(
                'Merging of inherited containers has not been implemented'
            )

        return self._wrap_child_node(item)

    def __getattr__(self, key: str) -> Any:
        """
        Allow accessing metadata values as attributes
        """
        if key == "__name__":
            raise AttributeError()
        return self._get_impl(key)

    def __repr__(self):
        return repr(self._container)

    def __getitem__(self, key: str):
        return self._get_impl(key)

    def __len__(self) -> int:
        return len(self._container)


class MetadataDict(MetadataNode, collections.abc.Mapping):
    def __init__(self, parent: None | MetadataNode, container: Mapping[str, Any]):
        super().__init__(parent, container)

    def __iter__(self):
        for key in self._container:
            yield key

    def __defines__(self, keys: str | Sequence[str]):
        if isinstance(keys, str):
            keys = [keys]
        return all([key in self._container for key in keys])


class MetadataList(MetadataNode, collections.abc.Sequence):
    def __init__(self, parent: None | MetadataNode, container: Sequence[Any]):
        super().__init__(parent, container)

    def __iter__(self):
        for item in self._container:
            yield self._wrap_child_node(item)

    def items(self):
        for index, item in enumerate(self._container):
            yield (index, self._wrap_child_node(item))


class Metadata:
    def __new__(
        cls,
        container: Mapping[str, Any] | Sequence[Any],
    ):
        return MetadataNode._wrap_container(None, container)

    @staticmethod
    def create(yaml_string: str) -> MetadataNode:
        conf = OmegaConf.create(yaml_string)
        return Metadata(conf)

    @staticmethod
    def load_yaml(filename: str | Path):
        filename = Path(filename)
        conf = OmegaConf.load(filename)
        return Metadata(conf)

    @staticmethod
    def to_yaml(metadata: MetadataNode, *, resolve: bool = False):
        return OmegaConf.to_yaml(metadata._container, resolve=resolve)

    @staticmethod
    def to_container(metadata: MetadataNode, *, resolve: bool = True):
        return OmegaConf.to_container(metadata._container, resolve=resolve)
