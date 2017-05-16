import math

from datahog.const import flag as dh_flag
from lang import Attrable
import db


class Field(object):

  def __init__(self, max_val, default=0):
    self.default = default
    self.max_val = max_val
    self.size = int(math.ceil(math.log(self.max_val + 1, 2)))
    self.first_bit = None


  def value_from_flags(self, flags):
    return reduce(lambda x, y: x | (1 << y - 1), flags, 0) >> self.first_bit


  def flags_from_value(self, value):
    value = value << self.first_bit
    return set(f for f in self.flags if (1 << (f - 1)) & value), self.flags


  @property
  def flags(self):
    return set(range(self.first_bit + 1, self.first_bit + self.size + 1))



class Fields(object):
  class int(Field):
    def __init__(self, max_val=None, bits=None, default=0):
      max_val = max_val and max_val or (math.pow(2, bits) - 1)
      super(Fields.int, self).__init__(max_val, default)


  class enum(Field):
    def __init__(self, *enum_strs, **kwargs):
      types = set(type(s) for s in enum_strs)
      if len(types) != 1 or types.pop() != str:
          raise TypeError('enum expects a list of strings.')
      self.enum_strs = enum_strs
      super(Fields.enum, self).__init__(len(self.enum_strs))
  

    def value_from_flags(self, flags):
      return self.enum_strs[super(Fields.enum, self).value_from_flags(flags)]


    def flags_from_value(self, value):
      val = self.enum_strs.index(value)
      return super(Fields.enum, self).flags_from_value(val) 


  class bool(Field):
    def __init__(self, default=False):
      if type(default) is not bool:
        raise TypeError("bool expects a boolean default value.")
      super(Fields.bool, self).__init__(1, default)


    def value_from_flags(self, flags):
      return bool(len(flags))


    def flags_from_value(self, value):
      return super(Fields.bool, self).flags_from_value(int(value))


class Layout(object):
  '''Instances of Layout collect the various flag fields defined
   on the flag object during class definition.

  class User(db.Entity):
    # flags is an instance of Layout
    flags.verified = ...

    username = db.lookup.alias()

    # username.flags is also an instance of Layout
    username.flags.is_primary = ...

  '''

  max_bits = 16


  def __init__(self, **fields):
    self.fields = {}
    self.next_free_bit = 0
    self.frozen = False


  def __setattr__(self, name, val):
#    if name == 'similarity': import pdb; pdb.set_trace()
    if not isinstance(val, Field):
      return super(Layout, self).__setattr__(name, val)
    self.set_flag_field(name, val)


  def __getattr__(self, name):
    return self.fields[name]

  def set_flag_field(self, field_name, field_def):
    if self.frozen:
      raise Exception("Can't add fields to a frozen FlagFieldDef.")
    if self.next_free_bit + field_def.size > self.max_bits:
      raise exceptions.FlagFieldDefOverflow
    self.fields[field_name] = field_def
    field_def.first_bit = self.next_free_bit
    self.next_free_bit += field_def.size
    if isinstance(field_def, Fields.enum):
      self.make_enum_refs(field_name, field_def.enum_strs)


  def freeze(self, ctx):
    if self.frozen:
      return
    self.frozen = True
    for name, field in self.fields.iteritems():
      for flag in range(field.first_bit, field.first_bit + field.size):
        dh_flag.set_flag(flag + 1, ctx)


  def make_enum_refs(self, enum_name, enum_strs):
    enum_lookup = Attrable()
    for enum_str in enum_strs:
      setattr(enum_lookup, enum_str, enum_str)
    setattr(self, enum_name, enum_lookup)


  def __call__(self, **fields):
    ''' Useful in the case that you want to create a single flags instance that 
    can be passed to many node instances. E.g.,
    verification_email_flags = User.verification_email.flags(
      status=User.verification_email.flags.status.sent)
    '''
    flags = Flags(fields=self.fields)
    for field_name, field_val in fields.iteritems():
      setattr(flags, field_name, field_val)
    flags._dirty_flags = flags._flags_set
    return flags


class Flags(object):
  ''' Instances of this class appear at the `flags` attr of nodes, properties, 
  relations, etc. '''

  def __init__(self, owner=None, fields=None):
    self._owner = owner # TODO weakref
    self._dirty_mask = set() # set of all flag values that need to be saved
    if not fields:
      fields = owner and owner.__class__.flags.fields
    self._fields = fields
    if not self._owner:
      self._tmp_set = set()    


  def __setattr__(self, name, value):
    if name.startswith('_'):
      return super(Flags, self).__setattr__(name, value)

    if name in self._fields:
      return self._set_field(name, value)

    return super(Flags, self).__setattr__(name, value)


  # goal: simplify flag storage

  # - get rid of the need for tmp_set by instantiate a default value
  #   for flags (and value?) when Dict is instantiated
  
  @property
  def _flags_set(self):
    if self._owner:
      return self._owner._dh['flags']
    return self._tmp_set


  def __getattr__(self, name):
    if self._fields and name in self._fields:
      field = self._fields[name]
      return field.value_from_flags(self._flags_set.intersection(field.flags))
    raise AttributeError


  def _set_field(self, name, value):
    ''' Handles, e.g., user.verification_email.flags.status = 'sent' '''
    field = self._fields[name]
    flags, dirty = field.flags_from_value(value)

    self._flags_set.difference_update(dirty)
    self._dirty_mask.update(dirty)
    self._flags_set.update(flags)


  def __call__(self, name, value=None, **kwargs):
    # TODO better api: flag_name/value in kwargs
    if value:
      self._set_field(name, value)
      self.save(**kwargs)
    else:
      return getattr(self, name)


  def _get_add_clear_flags(self):
    add, clear = [], []
    for flag in self._dirty_mask:
      if flag in self._flags_set:
        add.append(flag)
      else:
        clear.append(flag)
    return add, clear


  def save(self, **kwargs):
    self._owner.save_flags(*self._get_add_clear_flags())
    self._dirty_mask = set()
      

