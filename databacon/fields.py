import inspect
import math
from mummy.schemas import _validate_schema
from datahog.const import search


class Field(object):
  ''' Base class for model field definition classes, which are like saplings
  that will later turn into classes found in types.py, like Prop.

  These classes are a bit awkward. Ideally, to avoid including logic here that
  is more appropriately group with classes in types.py, these classes would be
  map to the __new__ methods of the classes they turn into (see the `defines`
  property), but because we'd like to do things like give the dynamically
  created classes names that include their owners' names, we defer creating new
  classes until the owner class's metaclass's __new__ invocation.

  '''
  def __init__(self, *args, **kwargs):
    self.flags = FlagPole()


class prop(Field):
  def __init__(self, schema):
    if not _validate_schema(schema):
      raise TypeError("prop expects a valid mummy schema")
    self.schema = schema
    super(prop, self).__init__()


class relation(Field):
  def __init__(self, cls):
    if not (inspect.isclass(cls) or type(cls) is str):
      raise TypeError("relation expects a class reference or class name (string).")
    self.cls = cls
    super(relation, self).__init__()


class lookup(object):
  class LookupField(Field):
    def __init__(self, plural=False):
      self.plural = plural


  class prefix(LookupField):
    search_mode = search.PREFIX


  class phonetic(LookupField):
    search_mode = search.PHONETIC
    def __init__(self, loose=False, **kwargs):
      self.phonetic_loose = loose
      super(lookup.phonetic, self).__init__(**kwargs)


  class alias(LookupField):
    def __init__(self, local=False, **kwargs):
      self.local = local
      super(lookup.alias, self).__init__(**kwargs)


class flag(object):
  class FlagDef(object):
    def __init__(self):
      self.max_val = None


    def _int_to_val(self, int):
      return int
  

  class int(FlagDef):
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


  class enum(FlagDef):
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


  class bool(FlagDef):
    def __init__(self, default):
      if type(default) is not bool:
        raise TypeError("bool expects a boolean default value.")

      self.size = 1
      self.max_val = 1

      super(flag.bool, self).__init__()


    def _int_to_val(self, int):
      return bool(int)


class FlagsDef(object):
  MAX_BITS = 16


  def __init__(self):
    self._fields = {}
    self._flag_idxs = {}
    self._bit_idx = 0 # first unoccupied bit in the field
    super(FlagsDef, self).__init__()


  def __setattr__(self, name, value):
    if name.startswith('_'):
      return super(FlagsDef, self).__setattr__(name, value)

    if isinstance(value, flag.FlagDef):
      self._def_flag(name, value)

    super(FlagsDef, self).__setattr__(name, value)


  def _def_flag(self, flag_name, flag_def):
    if self._bit_idx + flag_def.size > self.MAX_BITS:
      raise exc.FlagDefOverflow
    self.__fields[flag_name] = flag_def
    flag_def.start = self._bit_idx
    self._bit_idx += flag_def.size

    if flag_def is flag.enum:
      self._make_enum_refs(flag_name, flag_def.enum_strs)

  def _make_enum_refs(self, enum_name, enum_strs):
    enum_lookup = Attrable()
    for enum_str in enum_strs:
      setattr(enum_lookup, enum_str, enum_str)
    setattr(self, enum_name, enum_lookup)


class Attrable(object):
  pass


flags = FlagsDef
