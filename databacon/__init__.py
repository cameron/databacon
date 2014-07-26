



_to_storage = {
  int: storage.INT,
  str: storage.STR
  unicode: storage.UTF,
  None: storage.NULL
}

def _storage_from_value_type(f):
  return _to_storage.get(kwargs.get('schema', ''), storage.SERIAL)


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


class EntityTypeFactory(type):
  context = 0

  @staticmethod
  def new_ctx():
    context += 1
    return context

  def __new__(mcls, name, bases, cls__dict):
    cls_dict['ctx'] = new_ctx()

    parent_type = cls_dict.get('parent_type', None)
    value_type = cls_dict['value_type']
    context.set_context(cls_dict['ctx'], cls_dict['table'], {
      'base_ctx': parent_type and parent_type.ctx or None,
      'storage': _storage_from_value_type(cls_dict['value_type'])
      'schema': cls_dict['value_type'],
    })

    dict['SCHEMA'] = dict['value_type']  # for parent class mummy.Message

    mcls.generate_ctxs(dict)
    mcls.generate_enum_attrs(dict)

    return super(EntityTypeFactory, mcls).__new__(name, bases, dict)

  def generete_enum_attrs(cls_dict):
    ''' Create easy references to enum field values. 

    E.g., for an enum on the `User` class called `role` with the values 'admin',
    'staff', and 'user', generate the following references:

    `User.role.admin, User.role.staff, User.role.user`
    
    such that the following two statements are equivalent:

    `user.flags.role = User.role.admin 
    user.flags.role = 'admin'`
    '''
    for flag_type in cls_dict['flag_types']:
      if type flag_type is tuple and
      flag_type[1] is list:
        enum_name, values = flag_type
        lookup = object()
        for value in values:
          setattr(lookup, value, value)
        cls_dict[enum_name] = lookup

  def generate_ctxs(mcls, cls_dict):
    tables = [('alias_types', table.ALIAS), 
              ('name_types', table.NAME),
              ('prop_types', table.PROPERTY)]
    for key, table in tables
      for field_def in cls_dict[key]:
        schema = field_def.get('schema', None),
        field_def['ctx'] = context.set_context(mcls.new_ctx(), table.ALIAS, {
          'base_ctx': cls_dict['ctx'],
          'search': field_def.get('search_mode', None),
          'schema': schema,
          'storage': field_def.get('storage', _storage_from_schema(schema))
        })

    self._prop_ctx_to_name = to_name = {}
    for prop_name, prop_type in cls_dict['prop_types'].iteritems():
      self._prop_ctx_to_name[prop_type['ctx']] = prop_name


class NodeTypeFactory(EntityTypeFactory):
  def __new__(mcls, name, bases, dict):
    if not issubclass(dict.get('parent_type', None), Entity):
      raise ClassDefMissingParentTypeProperty
    return super(NodeTypeFactory, self).__new__(name, bases, dict)


class Entity(mummmy.Message):
  __metaclass__ = EntityTypeFactory
  _table = table.ENTITY
  _rels = {}
  _ctx = None # set by the metaclass

  # set by subclasses
  flag_types = None
  prop_types = None
  alias_types = None
  name_types = None

  @classmethod
  def relation(cls, name, rel_cls, flags=[]):
    ''' Associates a flag schema with a the given cls and rel_cls pair '''
    cls.rels[name] = {'ctx': rel_cls.ctx, 'flag_types': flags }


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
                               [prop['ctx'] for prop in self.prop_types])
    props = {}
    for prop_dict in prop_dicts:
      props[self._prop_ctx_to_name[prop_dict['ctx']]] = Prop(prop_dict)
    return props

  def prop(prop_type, **kwargs):
    return Prop(prop.get(self.guid, self.prop_types[prop_type]['ctx'], **kwargs))

  def relations(self, rel_cls, **kwargs):
    return Relations(rel_cls, 
                     relationship.list(self.guid, 
                                       self.rel_types[rel_cls.__name__]['ctx'], 
                                       **kwargs),
                     rel_cls._table)

  def add_relation(self, guid_obj, flags=None, **kwargs):
    return relation.create(self._rels[guid_obj._ctx], 
                           self.guid, guid_obj.guid, 
                           **kwargs)

  def add_alias(self, type, value, flags=None, **kwargs):
    return alias.set(self.guid, 
                     self.alias_types[type]['ctx'], 
                     value, 
                     flags=flags
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
    self.flags = Flags(node_dict['flags'], self.flag_types)

    t = type(self.value_type)
    if t is type:
      self = node_dict['value']
    elif t is list:
      self.extend(node_dict['value']):
    elif t is dict:
      self.update(node_dict['value'])

    super(mummy.Message, self).__init__(value)

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

  def __init__(self, rel_cls, rels, table):
    self = [Relation(r) for r in rels]
    self._rel_cls = rel_cls
    self._api = _table_to_api[table]

  def nodes(self, **kwargs):
    ctx = rels[0]['ctx']
    guid_ctx_pairs = zip([ctx]*len(rels), [r['rel_id'] for r in rels])
    return [self.rel_cls(dict) for dict in api.batch_get(guid_ctx_pairs, **kwargs)]


class Relation(object):

  def __init__(self, rel_dict, table):
    self.flags = Flags(...)
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

  def __init__(self, int_value, layout, api):
    ''' `api` will be a partial call to set_flags, with the partial
    including all the arguments except the flag value itself.

    E.g., for the relationship table, api will be a partial
    of relationship.set_flags(base_id, rel_id, ctx).'''

    self._parse_layout(layout)
    self._value = int_value
    self._api = api
    self._dirty_mask = 0

  def _parse_layout(self, layout):
    self._fields = {}
    bit_pos = 0
    for field in layout:
      field_name = bit_range = enum_strs = None

      # flag
      if type(field) is str:
        field_name = field_name
        bit_range = (bit_pos, bit_pos + 1)

      # enum
      elif type(field[1]) is list:
        field_name = field[0]
        enum_values = field[1]
        bit_range = (bit_pos, bit_pos + math.ceil(math.log(len(enum_strs), 2)))

      # bit field
      else:
        field_name = field[0]
        field_size = field[1]
        bit_range = (bit_pos, bit_pos + field_size)
      
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
      object.__setattr(name, value)

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

  def _mask_for_range(r):
    return (r[1] - r[0]) ** 2 - 1 << r[0]

  def save(self, **kwargs):
    self._api.set_flags(self._dirty_mask, self._value, **kwargs)


class AliasAndName(str):
  _api = None # subclasses override

  def __init__(self, dict, flags_layout):
    self.flags = Flags(dict['flags'], flags_layout)
    self.base_id = dict['base_id']
    self.ctx = dict['ctx']
    super(str, self).__init__(dict['value'])

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

