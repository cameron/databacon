
import math
import functools
import inspect

from datahog.const import search
from mummy.schemas import _validate_schema

import .types
from bits import Bits


__all__ = ['prop', 'relation', 'lookup', 'children']



def relation(target):
  cls = None
  list_cls = types.Relation.List

  # `target` is a reference to a previously-defined relationship, e.g.:
  #   `brothers = db.relation(Brother.sisters)`
  if inspect.isclass(target) and issubclass(target, types.Relation.List):
      cls = subclass(target.of_type, {'forward': False})
      list_cls = target

  # `target` is a string reference to a later-defined class, e.g.:
  #   `brothers = db.relation('Brother')`
  elif isinstance(target, str):
    cls = subclass(types.Relation)
    types.rels_pending_cls.setdefault(target, []).append(cls)

  # `target` is a reference to a user-defined sublcass of types.GuidDict, e.g.:
  #   `brothers = db.relation(Brother)`
  else:
    cls = subclass(types.Relation, {'rel_cls': target})

  cls = subclass(list_cls, {'of_type': cls})
  return cls


def _lookup(plural=False, _kind=None, _search_mode=None, loose=None, uniq_to_parent=False):
  meta = {'search': _search_mode, 'phonetic_loose': loose}
  attrs = {'_meta': meta}
  if _kind == types.Alias and uniq_to_parent:
    attrs['uniq_to_parent'] = True
  cls = subclass(_kind, attrs)
  if plural:
    cls = subclass(cls.List, {'of_type': cls})
    cls.of_type.plural = cls # eww circular ref
  return cls


class lookup(object):
  alias = staticmethod(functools.partial(_lookup, _kind=types.Alias))
  prefix = staticmethod(
    functools.partial(_lookup, _kind=types.Name, _search_mode=search.PREFIX))
  phonetic = staticmethod(
    functools.partial(_lookup, _kind=types.Name, _search_mode=search.PHONETIC))


def children(child_cls):
  attrs = {}
  if inspect.isclass(child_cls):
    attrs['of_type'] = child_cls
  elif isinstance(child_cls, str):
    attrs['_pending_cls_name'] = child_cls
  return subclass(types.Node.List, attrs)
