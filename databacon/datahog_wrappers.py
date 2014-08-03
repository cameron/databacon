import math

from datahog import node, entity, alias, name, prop, relationship
import exceptions as exc
import db


class DatahogDict(object):
  ''' Base class for all classes that wrap datahog dicts, stored at `self._dh`
  
  All datahog objects share a flags key and a remove operation. They all also
  have a context value (a type identifier defined by databacon), and a table
  (e.g., node).

  Subsets of datahog objects have shared keys and operations, which you will 
  find handled in subclases below.
  '''
  flag_types = None
  _table = None
  _ctx = None 
  _remove_arg_strs = None 
  _id_arg_strs = None


  def __init__(self, dh):
    self._dh = dh
    self.flags = DatahogFlags(self)
    super(DatahogDict, self).__init__()


  @property
  def _dh(self):
    return self.__dh


  @_dh.setter
  def _dh(self, dh):
    if dh is None:
      raise exc.DatahogDictCannotBeNone(self)
    self.__dh = dh


  @property
  def _id_args(self):
    return [self._dh[key] for key in self._id_arg_strs]


  @property
  def _remove_args(self):
    return [self._dh[key] for key in self._remove_arg_strs]


  def remove(self, **kwargs):
    args = self._ids_args + [self._ctx] + self._remove_args
    return self._table.remove(db.pool, *args, **kwargs)


class DatahogValueDict(DatahogDict):
  ''' Adds value access for datahog dicts that have values:
  nodes, props, names, and aliases '''
  schema = None


  @property
  def value(self):
    return self._dh['value']


  @value.setter
  def value(self, value):
    # TODO mummy validation
    self._dh['value'] = value


  def increment(self, **kwargs):
    if not self.schema is int:
      raise exc.CannotIncrementNonNumericValue()
    
    args = self._id_args + [self._ctx]
    new_val = self._table.increment(db.pool, *args, **kwargs)
    if new_val is None:
      raise exc.DoesNotExist(self)
    self.value = new_val
    return self


# save method names
# - node: update
# - alias/name: none (must remove/create a new one)
# - prop: set


class DatahogGuidDict(DatahogDict):
  __metaclass__ = DatahogGuidDictMC
  _id_arg_strs = ('guid', )


  @property
  def guid(self):
    return self._dh['guid']


  def children(self, child_cls, **kwargs):
    children, offset = node.get_children(db.pool,
                                         self.guid,
                                         self._child_ctxs[child_cls.__name__],
                                         **kwargs)
    return [child_cls(dict=n) for n in children]


  def aliases(self, alias_type, **kwargs):
    aliases = alias.list(db.pool,
                         self.guid, 
                         self.alias_types[alias_type]['ctx'], **kwargs)
    return [Alias(dict=alias_dict) for alias_dict in aliases]


  def names(self, name_type, **kwargs):
    names = name.list(db.pool, 
                      self.guid, 
                      self.name_types[name_type]['ctx'], **kwargs)
    return [Name(dict=name_dict) for name_dict in names]


  def props(self, **kwargs):
    prop_dicts = prop.get_list(db.pool,
                               self.guid, 
                               [prop['ctx'] for prop in self.prop_types], **kwargs)
    props = {}
    for prop_dict in prop_dicts:
      props[self._prop_ctx_to_name[prop_dict['ctx']]] = Prop(dict=prop_dict)
    return props


  def prop(prop_type, **kwargs):
    return Prop(dict=prop.get(db.pool, 
                              self.guid, 
                              self.prop_types[prop_type]['ctx'], **kwargs))


  def relations(self, rel_cls, **kwargs):
    return Relations(rel_cls, 
                     self._rel_flag_types.get(rel_cls, None),
                     relationship.list(db.pool,
                                       self.guid, 
                                       self.rel_types[rel_cls.__name__]['ctx'], 
                                       **kwargs))


  def add_relation(self, guid_obj, flags=None, **kwargs):
    return relation.create(db.pool,
                           self._rels[guid_obj._ctx], 
                           self.guid, guid_obj.guid, **kwargs)


  def add_alias(self, type, value, flags=None, **kwargs):
    return alias.set(db.pool,
                     self.guid, 
                     self.alias_types[type]['ctx'], 
                     value, 
                     flags=flags, **kwargs)


  def add_name(self, type, value, flags=None, **kwargs):
    return name.create(db.pool, self.guid, 
                       self.name_types[type], 
                       value, 
                       flags=flags, **kwargs)


class DatahogBaseIdDict(DatahogDict):
  _id_arg_strs = ('base_id',)


  def __init__(self, owner=None, dh=None):
    self.owner = owner
    super(DatahogBaseIdDict, self).__init__(dh or self._fetch_dict)


  @property
  def base_id(self):
    return self.owner.base_id


class DatahogRelIdDict(DatahogBaseIdDict):
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None


  @property
  def rel_id(self):
    return self._dh['rel_id']


  def shift(self, index, forward=True):
    table = (forward and self._from_cls or self._to_cls)._table
    return table.shift(db.pool,
                       self.base_id, 
                       self.guid, 
                       self._ctx, 
                       forward, 
                       index, **kwargs)
    

  def node(self, **kwargs):
    return self._to_cls._table.get(db.pool, 
                                   self.rel_id,
                                   self._ctx, **kwargs)




class DatahogLookupDict(DatahogBaseIdDict, DatahogValueDict):
  _remove_args = ('value',)


  def shift(self, index, **kwargs):
    return self._table.shift(db.pool,
                             self.base_id, 
                             self._ctx, 
                             self.value, 
                             index, **kwargs)

class DatahogEntityDict(dh.DatahogGuidDict):
  _table = entity
  def __init__(self, dict=None, **kwargs):
    super(Entity, self).__init__(dict or entity.create(db.pool,
                                                       self._ctx, **kwargs))


class DatahogNodeDict(dh.DatahogGuidDict, dh.DatahogValueDict): 
  _table = node
  _remove_arg_strs = ('base_id', )

  parent_type = None 

  def __init__(self, parent=None, value=None, dict=None, **kwargs):
    super(Node, self).__init__(dict or node.create(db.pool,
                                                   parent.guid,
                                                   self._ctx,
                                                   value, **kwargs))

  def shift(self, index):
    return self._table.shift(db.pool,
                             self.guid, 
                             self._ctx, 
                             self.base_id, 
                             index, **kwargs)

  def move(self, new_parent):
    moved =  node.move(db.pool,
                       self.guid, 
                       self._ctx, 
                       self.parent.guid, 
                       new_parent.guid, **kwargs)
    if moved:
      self.parent = new_parent
    return moved

  def save(self, force=False, **kwargs):
    result = node.update(db.pool,
                         self.guid, 
                         self._ctx, 
                         self, 
                         old_value=force and _missing or self, **kwargs)
    if not result:
      if force:
        raise exc.DoesNotExist(node)
      else:
        raise exc.NodeValueHasChanged(node)
    return result


class Relations(list):
  def __init__(self, rel_cls, rel_flag_types, rels):
    super(Relations, self).__init__(map(rel_cls, rels))

  def nodes(self, **kwargs):
    ctx = self[0]._ctx
    guid_ctx_pairs = zip([ctx]*len(rels), [r['rel_id'] for r in rels])
    dicts = self._table.batch_get(db.pool, guid_ctx_pairs, **kwargs)
    return [self.rel_cls(dict=dict) for dict in dicts]


class DatahogRelation(dh.DatahogRelIdDict):
  pass


class DatahogAlias(dh.DatahogLookupDict):
  _table = alias


class DatahogName(dh.DatahogLookupDict):
  _table = name
    

class DatahogProp(dh.DatahogValueDict, dh.DatahogBaseIdDict):
  _table = prop

  def __init__(self, **kwargs):
    self.ignore_remove_race = False
    super(Prop, self).__init__(**kwargs)


  def _get(self, **kwargs):
    return prop.get(self.base_id, self._ctx, **kwargs)


  @property
  def _remove_args(self):
    if self.ignore_remove_race:
      return []
    return [self.value]


  def remove(self, ignore_remove_race=None, **kwargs):
    was = self.ignore_remove_race
    if ignore_remove_race is not None:
      self.ignore_remove_race = ignore_remove_race
    super(Prop, self).remove(**kwargs)
    self.ignore_remove_race = was


  def save(self):
    # TODO any reason props don't support the _missing pattern in set?
    return prop.set(db.pool, self._base_id, self._ctx, self)
