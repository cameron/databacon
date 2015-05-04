# fields.py
# 
# Methods for defining datahog types (flags, relationships, properties, aliases,
# and names) as methods on databacon classes. 

import math
import functools

from datahog.const import search
from mummy.schemas import _validate_schema

import metaclasses
import datahog_wrappers as dhw
import flags


_cls_id = 0
def _subclass(base, attrs):
  ''' Solely to ensure uniqueness. '''
  global _cls_id
  _cls_id += 1
  return type('%s-%s' % (base.__name__, _cls_id), (base,), attrs)


def prop(schema):
  if not _validate_schema(schema):
    raise TypeError("prop expects a valid mummy schema")
  return _subclass(dhw.Prop, {'schema': schema})


def relation(target, directed=None):
  # accessor for an existing relationship
  if isinstance(target, dhw.Relation.collection):
    if directed:
      raise Exception("Cannot redefine directedness for an existing relation.")
    coll_cls = target
    if coll_cls.directed:
      coll_cls = _subclass(coll_cls, {'directed': not coll_cls.directed})
    return coll_cls

  # create a new relationship context
  cls_str = isinstance(target, str) and target or target.__name__
  attrs = {'_rel_cls_str': cls_str, 'directed': directed}
  cls = _subclass(dhw.Relation, attrs)
  return _subclass(dhw.Relation.collection, {'_dh_cls': cls})


def _lookup(plural=False, _kind=None, _search_mode=None, loose=None):
  meta = {'search': _search_mode, 'phonetic_loose': loose}
  cls = _subclass(_kind, {'_meta': meta})
  if plural:
    return _subclass(_kind.collection, {'_dh_cls': cls})
  return cls


class lookup(object):
  alias = staticmethod(functools.partial(_lookup, _kind=dhw.Alias))
  prefix = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PREFIX))
  phonetic = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PHONETIC))


def children(child_cls_name):
  return _subclass(dhw.ChildCollection, {'_dh_cls_str': child_cls_name})
