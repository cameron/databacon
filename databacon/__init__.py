

import inspect
import functools
import math

import mummy
from datahog import alias, node, relationship, prop, entity, name
from datahog.const import storage, table, context
from datahog.pool import GreenhouseConnPool


# TODONT use a global connection object
# (makes integration tests impossible to parallelize)
dbpool = None
for api in (alias, node, relationship, prop, entity, name):
  for func_name in (api.__all__ + ['set_flags']):
    func = getattr(api, func_name)
    if inspect.isfunction(func):
      def wrap(f):
        @functools.wraps(f)
        def api_wrapper(*args, **kwargs):
          return f(dbpool, *args, **kwargs)
        return api_wrapper
      setattr(api, func_name, wrap(func))


_to_storage = {
  int: storage.INT,
  str: storage.STR,
  unicode: storage.UTF,
  None: storage.NULL
}

def _storage_from_value_type(value_type):
  try:
    return _to_storage[value_type]
  except (KeyError, TypeError):
    return storage.SERIAL


class CannotIncrementNonNumericValue(Exception):
  ''' The increment method is reserved for nodes and properties with 
  value_type int.'''


class ClassDefMissingParentTypeProperty(Exception):
  ''' Subclasses of Node must specify a parent_type property
  that is itself a subclass of Entity or Node.'''


class NodeDoesNotExist(Exception):
  ''' Cannot update a node that doesn't exist. '''


class NodeDoesNotExist(Exception):
  ''' Cannot update a prop that doesn't exist. '''


class WillNotUpdateStaleNode(Exception):
  ''' The node's in-memory and on-disk values are out of sync. Either refresh
  the node before saving, or class node.save(force=True).'''


class FlagValueOverflow(Exception):
  pass


class EntityTypeFactory(mummy.schemas._validated_schema):
  context = 0

  @classmethod
  def new_ctx(mcls):
    mcls.context += 1
    return mcls.context

  def __new__(mcls, name, bases, cls_dict):
    ''' Take a class definition and generate all the appropriate datahog 
    contexts. '''

    if name in ('Entity', 'Node'):
      return super(EntityTypeFactory, mcls).__new__(mcls, name, bases, cls_dict)

    value_type = mcls.generate_class_ctx(cls_dict, bases[0]._table)

    if value_type:
      # make the new class behave like the value it stores
      bases += (type(value_type) is type and value_type or type(value_type),)

      # see parent class mummy.Message
      cls_dict['SCHEMA'] = cls_dict['value_type'] 
    

    cls_dict['_rel_flag_types'] = {}

    mcls.generate_supporting_ctxs(cls_dict)
    mcls.generate_enum_attrs(cls_dict)

    return super(EntityTypeFactory, mcls).__new__(mcls, name, bases, cls_dict)


  @classmethod
  def generate_class_ctx(mcls, cls_dict, table):
    cls_dict['_ctx'] = mcls.new_ctx()
    parent_type = cls_dict.get('parent_type', None)
    value_type = cls_dict.get('value_type', None)

    meta = {}
    if parent_type:
      meta['base_ctx'] = parent_type._ctx
    if value_type:
      meta['storage'] = _storage_from_value_type(value_type)
      meta['schema'] = value_type

    context.set_context(cls_dict['_ctx'], table, meta)
    return value_type

  @classmethod
  def generate_enum_attrs(mcls, cls_dict):
    ''' Create class references to enum field values. 

    E.g., these are equivalent:
    user.flags.role = 'admin'
    user.flags.role = User.role.admin # <- generate the admin reference
    '''
    for flag_type in (cls_dict.get('flag_types', None) or []):
      if type(flag_type) is tuple and \
         flag_type[1] is list:
        enum_name, values = flag_type
        lookup = object()
        for value in values:
          setattr(lookup, value, value)
        cls_dict[enum_name] = lookup

  @classmethod
  def generate_supporting_ctxs(mcls, cls_dict):
    tables = [('alias_types', table.ALIAS), 
              ('name_types', table.NAME),
              ('prop_types', table.PROPERTY)]

    for key, tbl in tables:
      for field_name, field_def in (cls_dict.get(key, None) or {}).iteritems():
        value_type = field_def.get('value_type', None)
        meta = {'base_ctx': cls_dict['_ctx']}

        if value_type:
          meta['schema'] = value_type
          meta['storage'] = field_def.get('storage', _storage_from_value_type(value_type))
        if 'search_mode' in field_def:
          meta['search_mode'] = field_def['search_mode']

        field_def['ctx'] = context.set_context(mcls.new_ctx(), tbl, meta)

    cls_dict['_prop_ctx_to_name'] = to_name = {}
    for prop_name, prop_type in (cls_dict.get('prop_types', None) or {}).iteritems():
      cls_dict['_prop_ctx_to_name'][prop_type['ctx']] = prop_name


class NodeTypeFactory(EntityTypeFactory):
  def __new__(mcls, name, bases, cls_dict):
    if name in ('Node'):
      return super(EntityTypeFactory, mcls).__new__(mcls, name, bases, cls_dict)
    if not issubclass(cls_dict['parent_type'], Entity):
      raise ClassDefMissingParentTypeProperty
    return super(NodeTypeFactory, mcls).__new__(mcls, name, bases, cls_dict)


class Entity(mummy.Message):
  __metaclass__ = EntityTypeFactory
  _table = table.ENTITY

  # set by the metaclass
  _ctx = None 

  # set by subclasses
  flag_types = None
  prop_types = None
  alias_types = None
  name_types = None

  @classmethod
  def relation(cls, rel_cls, flag_types=[]):
    ''' Associates a flag schema with a the given cls and rel_cls pair '''
    cls._rel_flag_types[rel_cls] = flag_types

  def __init__(self, entity_dict=None):
    if not entity_dict:
      entity_dict = entity.create(self._ctx)
    self.guid = entity_dict['guid']
    api = functools.partial(entity.set_flags, self.guid, self._ctx)
    self.flags = Flags(entity_dict['flags'], self.flag_types, api)

  def children(self, child_type, **kwargs):
    children = node.get_children(self.guid, 
                                 self.child_types[child_type]['ctx'],
                                 **kwargs)
    return [child_type(node) for node in children]

  def aliases(self, alias_type, **kwargs):
    aliases = alias.list(self.guid, self.alias_types[alias_type]['ctx'], **kwargs)
    return [Alias(alias_dict) for alias_dict in aliases]

  def names(self, name_type, **kwargs):
    names = name.list(self.guid, self.name_types[name_type]['ctx'], **kwargs)
    return [Name(name_dict) for name_dict in names]

  def props(self, **kwargs):
    prop_dicts = prop.get_list(self.guid, 
                               [prop['ctx'] for prop in self.prop_types], **kwargs)
    props = {}
    for prop_dict in prop_dicts:
      props[self._prop_ctx_to_name[prop_dict['ctx']]] = Prop(prop_dict)
    return props

  def prop(prop_type, **kwargs):
    return Prop(prop.get(self.guid, self.prop_types[prop_type]['ctx'], **kwargs))

  def relations(self, rel_cls, **kwargs):
    return Relations(rel_cls, 
                     self._rel_flag_types.get(rel_cls, None),
                     relationship.list(self.guid, 
                                       self.rel_types[rel_cls.__name__]['ctx'], 
                                       **kwargs))

  def add_relation(self, guid_obj, flags=None, **kwargs):
    return relation.create(self._rels[guid_obj._ctx], 
                           self.guid, guid_obj.guid, 
                           **kwargs)

  def add_alias(self, type, value, flags=None, **kwargs):
    return alias.set(self.guid, 
                     self.alias_types[type]['ctx'], 
                     value, 
                     flags=flags,
                     **kwargs)

  def add_name(self, type, value, flags=None, **kwargs):
    return name.create(self.guid, 
                       self.name_types[type], 
                       value, 
                       flags=None, 
                       **kwargs)


_table_to_api = {
  table.NODE: node,
  table.ENTITY: entity
}

class Node(Entity): 
  __metaclass__ = NodeTypeFactory
  _table = table.NODE

  parent_type = None 

  def __init__(self, parent, value=None, node_dict=None, **kwargs):
    if not node_dict:
      node_dict = node.create(parent.guid, self._ctx, value, **kwargs)
    
    self.guid = node_dict['guid']
    api = functools.partial(node.set_flags, self.guid, self._ctx)
    self.flags = Flags(node_dict['flags'], self.flag_types, api)

    t = type(self.value_type)
    if t is type:
      self = node_dict['value']
    elif t is list:
      self.extend(node_dict['value'])
    elif t is dict:
      self.update(node_dict['value'])

    super(Node, self).__init__(value)

  def increment(self, **kwargs):
    if not self.value_type is int:
      raise CannotIncrementNonNumericValue()
    new_val = node.increment(self.guid, self._ctx, **kwargs)
    if new_val is None:
      raise NodeDoesNotExist(self)
    self = new_val
    return self

  def shift(self, index, **kwargs):
    return node.shift(self.guid, self._ctx, self.parent.guid, index, **kwargs)


  def move(self, new_parent):
    moved =  node.move(self.guid, 
                       self._ctx, 
                       self.parent.guid, 
                       new_parent.guid, 
                       **kwargs)
    if moved:
      self.parent = new_parent
    return moved
    

  def save(self, force=False, **kwargs):
    result = node.update(self.guid, 
                         self._ctx, 
                         self, 
                         old_value=force and _missing or self, 
                         **kwargs)
    if not result:
      if force:
        raise NodeDoesNotExist(node)
      else:
        raise NodeValueHasChanged(node)


class Relations(list):

  def __init__(self, rel_cls, rel_flag_types, rels):
    self = [Relation(r, rel_flag_types, table) for r in rels]
    self._rel_cls = rel_cls
    self._api = _table_to_api[rel_cls._table]

  def nodes(self, **kwargs):
    ctx = rels[0]['ctx']
    guid_ctx_pairs = zip([ctx]*len(rels), [r['rel_id'] for r in rels])
    return [self.rel_cls(dict) for dict in api.batch_get(guid_ctx_pairs, **kwargs)]


class Relation(object):

  def __init__(self, rel_dict, flag_types, table):
    self.flags = Flags(rel_dict['flags'], flag_types)
    self._base_id = rel_dict['base_id']
    self._rel_id = rel_dict['rel_id']
    self._ctx = rel_dict['ctx']
    self._api = _table_to_api[table]

  def shift(self, index, forward):
    return relatioship.shift(self._base_id, 
                             self._rel_id, 
                             self._ctx, 
                             forward, 
                             index, 
                             **kwargs)

  def node(self, **kwargs):
    return self._api.get(self._rel_id, self._ctx, **kwargs)


class Flags(object):

  MAX_BITS = 16

  def __init__(self, int_value, layout, set_flags_partial):
    self._parse_layout(layout)
    self._value = int_value or 0
    self._api = set_flags_partial
    self._dirty_mask = 0

  def _parse_layout(self, layout):
    self._fields = {}
    bit_pos = 0
    for field in layout:
      field_name = bit_range = enum_strs = None

      # flag
      if type(field) is str:
        field_name = field
        bit_range = (bit_pos, bit_pos + 1)

      # enum
      elif type(field[1]) in (list, tuple):
        field_name = field[0]
        enum_strs = field[1]
        bit_range = (bit_pos, bit_pos + math.ceil(math.log(len(enum_strs), 2)))

      # bit field
      else:
        field_name = field[0]
        field_size = field[1]
        bit_range = (bit_pos, bit_pos + field_size)
      
      if bit_range[1] > Flags.MAX_BITS:
        raise FlagTypesOverflow()

      self._fields[field_name] = (bit_range, enum_strs)
      bit_pos += bit_range[1] - bit_range[0]
        

  def __getattr__(self, name):
    if name not in self._fields:
      raise AttributeError

    bit_range, enum_strs = self._fields[name]
    field_int = self._value & self._mask_for_range(bit_range) >> bit_range[0]

    if enum_strs:
      return enum_values[field_int]

    if bit_range[1] - bit_range[0] == 1:
      return Boolean(field_int)

    return field_int

  def __setattr__(self, name, value):
    if name.startswith('_'):
      return super(Flags, self).__setattr__(name, value)

    if name not in self._fields:
      raise AttributeError

    bit_range, enum_strs = self._fields[name]
    mask = self._mask_for_range(bit_range)

    if enum_strs:
      try:
        value = enum_strs.index(value)    
      except ValueError:
        raise UnknownFlagsEnumValue(name, value, enum_strs)

    value = int(value)
    if value > (mask >> bit_range[0]):
      raise FlagValueOverflow(name, value)

    # clear the range
    self._value &= ~mask

    # set the range
    self._value |= value << bit_range[0] 

    self._dirty_mask |= mask

  @staticmethod
  def _mask_for_range(r):
    return ((2 ** (r[1] - r[0])) - 1) << r[0]

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
        
    self._api(add, clear, **kwargs)


class AliasAndName(str):
  _api = None # subclasses override

  def __init__(self, dict, flags_layout):
    self.base_id = dict['base_id']
    self.ctx = dict['ctx']
    api = functools.partial(self._api, self.base_id, self.ctx, dict['value'])
    self.flags = Flags(dict['flags'], flags_layout, api)
    super(AliasAndName, self).__init__(dict['value'])

  def shift(self, index, **kwargs):
    return self._api.shift(self.base_id, self.ctx, self, index, **kwargs)

  def remove(self, **kwargs):
    return self._api.remove(self.base_id, self.ctx, self, **kwargs)


class Alias(AliasAndName):
  _api = alias


class Name(AliasAndName):
  _api = name
    

class Prop(mummy.Message):
  def __init__(self, prop_dict):
    self._base_id = prop_dict['base_id']
    self._ctx = prop_dict['ctx']
    self.flags = Flags(prop_dict['flags'], self.flag_types)

  def increment(self, **kwargs):
    if not self.value_type is int:
      raise CannotIncrementNonNumericValue()
    new_val = prop.increment(self._base_id, self._ctx, **kwargs)
    if new_val is None:
      raise PropDoesNotExist(self)
    self = new_val
    return self

  def remove(self, force=False):
    return prop.remove(self._base_id, self._ctx, value=self)

  def save(self):
    # TODO any reason props don't support the _missing pattern in set?
    return prop.set(self._base_id, self._ctx, self)


def connect(shard_config):
  global dbpool 
  dbpool = GreenhouseConnPool(shard_config)
  dbpool.start()
  if not dbpool.wait_ready(2.):
    raise Exception("postgres connection timeout")
  return dbpool

# TODO 
# - implement uniqueToOwner alias flag
# - class lookup methods (alias, name search, id)
# - make subclasses behave like their value_type
