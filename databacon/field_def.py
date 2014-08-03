
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


flags = FlagPole


class relation(Field):
  def __init__(self, cls_str):
    if types(cls_str) is not str:
      raise TypeError("relation expects a string class name.")
    self.cls_str = cls_str
    super(relation, self).__init__()


class lookup(object):

  class prefix(Field):
    search_mode = search.PREFIX
    

  class phonetic(Field):
    search_mode = search.PHONETIC
    def __init__(self, loose=False):
      self.phonetic_loose = loose
      super(phonetic, self).__init__()


  class alias(Field):
    def __init__(self, local=False):
      self.local = local
      super(alias, self).__init__()


class flag(object):


  class FlagDef(object):
    def __init__(self):
      self.max_val = None

    def _int_to_val(self, int):
      return int
  

  class int(FlagDef):
    def __init__(self, max_val, default=0):
      if type(max_val) is not int:
        raise TypeError("int expects an integer as a max value.")

      self.max_val = max_val
      self.default = 0
      self.size = int(math.ceil(math.log(self.max_val, 2)))

      super(flag.int, self).__init__()


  class enum(FlagDef):


    @staticmethod
    def all_strs(val, red):
      return type(val) is str and red


    def __init__(self, *enum_strs, **kwargs):
      
      if not reduce(self.all_strs, (type(e) for e in enum_strs)):
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

