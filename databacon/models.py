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


class flag(object):
  class int(flags.FlagDef):
    def __init__(self, max_val=None, bits=None, default=0):
      if isinstance(bits, int):
        self.size = bits
        self.max_val = math.pow(2, self.size)
      elif isinstance(max_val, int):
        self.max_val = max_val
        self.size = int(math.ceil(math.log(self.max_val, 2)))
      else:
        raise TypeError("Invalid args. Try int(max_val=int) or int(bits=int)")

      self.default = default

      super(flag.int, self).__init__()


  class enum(flags.FlagDef):
    @staticmethod
    def all_strs(reduced, val):
      return type(val) is str and reduced


    def __init__(self, *enum_strs, **kwargs):
      if not reduce(self.all_strs, enum_strs, True):
          raise TypeError('enum expects a list of strings.')

      self.default = kwargs.get('default', None) or enum_strs[0]
      if self.default not in enum_strs:
        raise TypeError('enum default value must be passed as a positional argument.')

      self.enum_strs = enum_strs
      self.size = int(math.ceil(math.log(len(self.enum_strs), 2)))
      self.max_val = math.pow(2, self.size + 1) - 1

      super(flag.enum, self).__init__()
  

    def _int_to_val(self, int):
      return self.enum_strs[int]


  class bool(flags.FlagDef):
    def __init__(self, default):
      if type(default) is not bool:
        raise TypeError("bool expects a boolean default value.")

      self.size = 1
      self.max_val = 1

      super(flag.bool, self).__init__()


    def _int_to_val(self, int):
      return bool(int)


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


def _lookup(plural=False, _kind=dhw.Alias, _search_mode=None, loose=None):
  meta = {'search': _search_mode, 'phonetic_loose': loose}
  cls = _subclass(_kind, {'meta': meta})
  if plural:
    return _subclass(_kind.collection, {'_dh_cls': cls})
  return cls


class lookup(object):
  alias = staticmethod(_lookup)
  prefix = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PREFIX))
  phonetic = staticmethod(
    functools.partial(_lookup, _kind=dhw.Name, _search_mode=search.PHONETIC))


def children(child_cls_name):
  return _subclass(dhw.ChildCollection, {'_dh_cls_str': child_cls_name})
