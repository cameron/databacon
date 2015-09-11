# fields.py
# 
# Methods for defining datahog types (flags, relationships, properties, aliases,
# and names) as methods on databacon classes. 

import math
import functools
import inspect

from datahog.const import search
from mummy.schemas import _validate_schema

import metaclasses
import datahog_wrappers as dhw
import flags


__all__ = ['prop', 'relation', 'lookup', 'children']


subcls_id_ctr = 0
def subclass(base, attrs=None):
  global subcls_id_ctr
  subcls_id_ctr += 1
  return type('%s-rename-%s' % (base.__name__, subcls_id_ctr), (base,), attrs or {})


def prop(schema):
  if not _validate_schema(schema):
    raise TypeError("prop expects a valid mummy schema")
  return subclass(dhw.Prop, {'schema': schema})


def relation(target):
  cls = None
  list_cls = dhw.Relation.List

  # `target` is a reference to a previously-defined relationship, e.g.:
  #   `brothers = db.relation(Brother.sisters)`
  if inspect.isclass(target) and issubclass(target, dhw.Relation.List):
      cls = subclass(target.of_type, {'forward': False})
      list_cls = target

  # `target` is a string reference to a later-defined class, e.g.:
  #   `brothers = db.relation('Brother')`
  elif isinstance(target, str):
    cls = subclass(dhw.Relation)
    metaclasses.rels_pending_cls.setdefault(target, []).append(cls)

  # `target` is a reference to a user-defined sublcass of dhw.GuidDict, e.g.:
  #   `brothers = db.relation(Brother)`
  else:
    cls = subclass(dhw.Relation, {'rel_cls': target})

  cls = subclass(list_cls, {'of_type': cls})
  return cls


def _lookup(plural=False, _kind=None, _search_mode=None, loose=None, uniq_to_parent=False):
  meta = {'search': _search_mode, 'phonetic_loose': loose}
  attrs = {'_meta': meta}
  if _kind == dhw.Alias and uniq_to_parent:
    attrs['uniq_to_parent'] = True
  cls = subclass(_kind, attrs)
  if plural:
    cls = subclass(cls.List, {'of_type': cls})
    cls.of_type.plural = cls # eww circular ref
  return cls


class lookup(object):
  alias = staticmethod(functools.partial(_lookup, _kind=dhw.Alias))
  prefix = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PREFIX))
  phonetic = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PHONETIC))


def children(child_cls):
  attrs = {}
  if inspect.isclass(child_cls):
    attrs['of_type'] = child_cls
  elif isinstance(child_cls, str):
    attrs['_pending_cls_name'] = child_cls
  return subclass(dhw.Node.List, attrs)
