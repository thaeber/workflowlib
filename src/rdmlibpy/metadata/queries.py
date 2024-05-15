from typing import Any, Callable, Dict, List, Sequence

from .metadata import MetadataDict, MetadataNode


class MetadataQuery:
    def __init__(self, node: MetadataNode, *, include_private_keys=False):
        if not isinstance(node, MetadataNode):
            raise TypeError("`node` must be of type `MetadataNode`")
        self._node = node
        self._include_private_keys = include_private_keys

    def _key_is_valid(self, key):
        if self._include_private_keys:
            return True
        elif isinstance(key, str):
            return not (key.startswith('__') and key.endswith('__'))
        else:
            return True

    def defines(self, keys: str | Sequence[str]):
        """
        Check if the given key or keys is/are defined on the node
        itself (inherited keys are ignored).

        The function only works on mapping nodes. For all other
        node types it will always return `False`.
        """
        node = self._node
        if not isinstance(node, MetadataDict):
            return False
        if isinstance(keys, str):
            keys = [keys]
        assert isinstance(node, MetadataDict)
        return node.__defines__(keys)

    def find(
        self,
        predicate: Callable[[MetadataNode], bool],
        *,
        include_self=True,
        recursive=True
    ):
        if include_self and predicate(self._node):
            yield self._node

        # iterate over children
        for node in self.iter_children(recursive=recursive):
            if predicate(node):
                yield node

    def keys(self) -> List[str] | List[int]:
        return [key for key, _ in self._node.items() if self._key_is_valid(key)]

    def values(self) -> List[Any]:
        return [value for key, value in self._node.items() if self._key_is_valid(key)]

    def kvdict(self) -> Dict[str, Any]:
        return {
            key: value for key, value in self._node.items() if self._key_is_valid(key)
        }

    def iter_children(self, *, recursive: bool = False):
        """
        Iterates over all child nodes. Only child nodes that represent
        key/value mappings or sequences are returned.
        """
        for node in self.values():
            if isinstance(node, MetadataNode):
                yield node
                if recursive:
                    yield from MetadataQuery(node).iter_children(recursive=recursive)

    def children(self, *, recursive: bool = False):
        """Returns a list of child nodes. The list only includes child nodes
        that are key/value mappings or sequences themselves."""
        return list(iter(self.iter_children(recursive=recursive)))


def query(node: MetadataNode, *, include_private_keys=False):
    return MetadataQuery(node, include_private_keys=include_private_keys)
