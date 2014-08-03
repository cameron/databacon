import functools

from datahog import node, entity
from datahog.const import storage, table, context
import field_def
import exceptions as exc


to_const = {
  node: table.NODE,
  entity: table.ENTITY,
}


def make_ctx(table, meta):
  ctx_counter = 0
  while 1:
    ctx_counter += 1
    context.set_context(ctx_counter, to_const[table], meta)
    yield ctx_counter


to_storage = {
  int: storage.INT,
  str: storage.STR,
  unicode: storage.UTF,
  None: storage.NULL
}


def storage_from_value_type(value_type):
  try:
    return to_storage[value_type]
  except (KeyError, TypeError):
    return storage.SERIAL


class DatahogDictMC(type):


  def __new__(mcls, name, bases, attrs):
    mcls.generate_enum_attrs(attrs)
    return super(DatahogDictMC, mcls).__new__(mcls, name, bases, attrs)


  @staticmethod
  def generate_enum_attrs(attrs):
    ''' Create class references to enum field values. 

    E.g., these are equivalent:
    user.flags.role = 'admin'
    user.flags.role = User.role.admin # <- generate the admin reference
    '''
    flag_pole = None
    for key, attr in attrs.iteritems():
      if type(attr) is FlagPole:
        flag_pole = attr
    
    if not flag_pole:
      return

    for fname, fdef in self.fields:
      if not hasattr(fdef, 'enum_strs'):
        continue
      attrs[fname] = type('%sEnumSet' % fname.capitalize(), 
                          object, 
                          dict(zip(fdef.enum_strs, fdef.enum_strs)))


class DatahogGuidDictMC(DatahogDictMC):


  def __new__(mcls, name, bases, attrs):
    ''' Does all the magic that makes a user-defined subclass of Node
    or Entity behave like well-defined datahog contexts.'''

    if not has_ancestor_named(bases, ('Entity', 'Node')):
      return  super(DatahogGuidDictMC, mcls).__new__(mcls, name, bases, attrs)

    dh_table_const = to_const[bases[0]._table]
    mcls.generate_cls_ctx(name, attrs, dh_table_const)
    mcls.generate_supporting_ctxs(name, attrs)

    return super(DatahogGuidDictMC, mcls).__new__(mcls, name, bases, attrs)


  @classmethod
  def generate_cls_ctx(mcls, name, attrs, table, meta=None):
    attrs['_ctx'] = make_ctx(table, meta)


  @staticmethod
  def generate_supporting_ctxs(cls_name, attrs):
    attrs['_rel_cls'] = {}
    attrs['_child_ctxs'] = {}

    for key, field in attrs.iteritems():
      if not issubclass(val, Field):
        continue

      field_cls_name = cls_name + field.defines.__name__

      field_cls_meta = {'base_ctx': attrs['_ctx']}

      if hasattr(field, 'schema'):
        meta['schema'] = field.schema
        meta['storage'] = storage_from_value_type(field.schema)

      if hasattr(field, 'seach_mode'):
        meta['search'] = field.search_mode
        meta['phonetic_loose'] = field.phonetic_loose

      field.__dict__['_ctx'] = make_ctx(tbl, meta)
      
      field_cls = type(field_cls_name, [field_cls.defines], field.__dict__)
      def instantiate_field_instance(owner):
        return field_cls(owner)
      attrs[key] = instantiate_field_instance


class NodeMC(DatahogGuidDictMC):


  def __new__(mcls, name, bases, attrs):
    ''' Enforce that Node subclasses Node have a valid parent_type property.'''

    args = [name, bases, attrs]

    if name == 'Node':
      return super(NodeMC, mcls).__new__(mcls, *args)

    if not has_ancestor_named(attrs['parent_type'], 'DatahogGuidDict'):
      raise exc.InvalidParentType(*args)

    return super(NodeMC, mcls).__new__(mcls, *args)


  @classmethod
  def generate_cls_ctx(mcls, name, attrs, table):
    meta = {}
    meta['storage'] = storage_from_value_type(attrs['value_type'])
    meta['base_ctx'] = attrs['parent_type']._ctx
    super(NodeMC, mcls).generate_cls_ctx(name,
                                                  attrs,
                                                  table,
                                                  meta)
    attrs['parent_type']._child_ctxs[name] = attrs['_ctx']


class FlagPole(object):
  MAX_BITS = 16


  @staticmethod
  def _mask_for_range(r):
    return ((2 ** (r[1] - r[0])) - 1) << r[0]


  def __init__(self, owner=None):
    self._owner = owner
    self._dirty_mask = 0

    self._flag_defs = {}
    self._flag_idxs = {}

    self._bit_idx = 0 # first unoccupied bit in the field

    super(DatahogFlags, self).__init__()


  @property
  def _value(self):
    return self._owner._dh['flags']


  @_value.setter
  def _value(self, val):
    self._owner._dh['flags'] = val


  def __getattr__(self, name):
    if name not in self._flag_defs:
      return super(FlagPole, self).__getattr__(name)

    flag_def = self._flag_defs[name]
    bit_range = (self._flag_idxs[name], self._flag_defs[name].size)
    int_val = self._value & self._mask_for_range(bit_range) >> bit_range[0]
    
    if issubclass(flag_def, field_def.flag.enum):
      return flag_def.enum_strs[int_val]

    if issubclass(flag_def, field_def.flag.bool):
      return bool(int_val)

    return int_val


  def _set_flag(self, flag_name, value):
    flag_def = self._flag_defs[name]
    mask = self._mask_for_range((flag_def.start, flag_def.end))
    
    if issubclass(flag_def,  field_def.flag.enum):
      try:
        int_value = enum_strs.index(value)    
      except ValueError:
        raise exc.UnknownFlagsEnumValue(name, value, enum_strs)

    field_def.value = value # for the user's sake

    int_value = int(value)
    if value > (mask >> bit_range[0]):
      raise exc.FlagValueOverflow(name, value)

    # clear the range
    self._value &= ~mask

    # set the range
    self._value |= value << bit_range[0] 

    self._dirty_mask |= mask


  def __setattr__(self, name, value):
    if name in self._flag_defs:
      return self._set_flag(name, value)
    
    if issubclass(field_def, FlagsDef):
      self._def_flag(name, value)

    super(FlagPole, self).__setattr__(name, value)


  def _def_flag(self, flag_name, flag_def):
    if self._bit_idx + flag_def.size > self.MAX_BITS:
      raise exc.FlagDefOverflow
    self._flag_defs[flag_name] = flag_def
    flag_def.start = self._bit_idx
    self._bit_idx += flag_def.size


  def save(self, **kwargs):
    add = []
    clear = []

    # bin(4) -> '0b100'
    # bin(4)[:1:-1] -> '001'
    val_str = str(self._value)
    for idx, bit in enumerate(bin(self._dirty_mask)[:1:-1]): 
      if int(bit):
        if int(val_str[idx]):
          add.append(idx)
        else:
          clear.append(idx)
        
# TODO patch dh to avoid flag.set_flag necessity
#    self._owner._table.set_flags(db.pool, add, clear, **kwargs)


def has_ancestor_named(classes, names):
  ''' String named-based impl of issubclass to circumvent circular import woes 
  in NodeTypeFactory, which refers to the Entity class '''
  bases = list(type(classes) is type and cls.__bases__ or classes)
  names = type(names) is tuple and names or (names,)
  while bases:
    base = bases.pop(0)
    if base.__name__ in names:
      return True
    bases.extend(list(base.__bases__))
  return False


# think about some elegant way to enforce arguments types via decorators...
# @arg('name').is(*types)
# @arg('name').is(*types).then(lambda arg: #stuff)
# @arg('classes').is([type]) # <- enforce that classes is a type or a list of
#                              types and auto-listify if necessary
