# fields.py
# 
# Methods for defining datahog types (flags, relationships, properties, aliases,
# and names) as methods on databacon classes. 

import math
import functools
import inspect
import sys

import datahog
from datahog.const import search

from . import metaclasses
from . import datahog_wrappers as dhw
from . import flags


__all__ = ['prop', 'relation', 'lookup', 'children', 'lock']


def _validate_schema(*args):
  # TODO replace mummy schema validation
  return True

def prop(schema):
  if not _validate_schema(schema):
    raise TypeError("prop expects a valid mummy schema")
  return subclass(dhw.Prop, schema=schema)


def relation(target):
  list_cls = dhw.Relation.List

  # brothers = db.relation(Brother.sisters)
  if inspect.isclass(target) and issubclass(target, dhw.Relation.List):
    cls = subclass(target.of_type, _meta=target.of_type._meta, undirected_subclass=True)
    list_cls = target
 
  # brothers = db.relation('Brother')
  elif isinstance(target, str):
    cls = subclass(dhw.Relation)
    metaclasses.rels_pending_cls.setdefault(target, []).append(cls)

  # brothers = db.relation(Brother)
  else:
    cls = subclass(dhw.Relation, rel_cls=target)

  cls = subclass(list_cls, of_type=cls)
  return cls


def _lookup(plural=False, _kind=None, _search_mode=None, loose=None, uniq_to_rel=None):
  meta = {'search': _search_mode, 'phonetic_loose': loose}
  attrs = {'_meta': meta}
  if _kind == dhw.Alias and uniq_to_rel:
    attrs['uniq_to_rel'] = uniq_to_rel
  cls = subclass(_kind, **attrs)
  if plural:
    cls = subclass(cls.List, of_type=cls)
    cls.of_type.plural = cls # eww circular ref
  return cls


class lookup(object):
  alias = staticmethod(functools.partial(_lookup, _kind=dhw.Alias))
  prefix = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PREFIX))
  phonetic = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PHONETIC))


def lock():
  return subclass(dhw.Lock)


subcls_id_ctr = 0
def subclass(base, **attrs):
  global subcls_id_ctr
  subcls_id_ctr += 1
  return type('%s-rename-%s' % (base.__name__, subcls_id_ctr), (base,), attrs)


