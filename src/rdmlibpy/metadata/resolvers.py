from omegaconf import DictConfig, ListConfig, OmegaConf
from omegaconf.base import Box
from omegaconf.errors import OmegaConfBaseException


def _get_inherited_property(key, node: Box, *, default=None):
    parent = node._get_parent()
    if (parent is not None) and (isinstance(parent, (DictConfig, ListConfig))):
        try:
            return parent[key]
        except OmegaConfBaseException:
            return _get_inherited_property(key, parent, default=default)
    else:
        return default


def get_metadata_property(name, *, _parent_):
    return _get_inherited_property(name, _parent_)


def register_custom_resolvers():
    OmegaConf.register_new_resolver('meta.get', get_metadata_property, replace=True)
