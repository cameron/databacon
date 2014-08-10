import math

from datahog import node, entity, alias, name, prop, relationship
import exceptions as exc
import fields
import db


class DHDict(object):
  ''' Base class for all classes that wrap datahog dicts, stored at `self._dh`
  
  All datahog objects share a flags attr and a remove method. They all also
  have a context value (a type identifier defined by databacon), and a table
  (e.g., node).

  Subsets of datahog objects have other shared attrs and methods, which you will 
  find handled in subclases below.
  '''
  _table = None
  _ctx = None 
  _remove_arg_strs = None 
  _id_arg_strs = None


  def __init__(self, dh, flags=None):
    self._dh = dh

    # TODO allow any class attr that is an instance of FlagsDef
    if isinstance(type(self).flags, fields.FlagsDef):
      self.flags = flags or DHFlags(self.flags._fields)
      self.flags._table = self._table

    super(DHDict, self).__init__()

        
  @property
  def _dh(self):
    return self.__dh


  @_dh.setter
  def _dh(self, dh):
    if dh is None:
      raise exc.DHDictCannotBeNone(self)
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


class DHValueDict(DHDict):
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


  def save(self, **kwargs):
    return self._save(self._id_args, self._ctx, self.value, **kwargs)

# save method names
# - node: update
# - alias/name: none (must remove/create a new one)
# - prop: set


class DHGuidDict(DHDict):
  _id_arg_strs = ('guid', )


  def __init__(self, dh):
    super(DHGuidDict, self).__init__(dh)
    self._init_dh_fields()


  def _init_dh_fields(self):
    for name, field in self._dh_fields.iteritems():
      setattr(self, name, field(owner=self))


  @property
  def guid(self):
    return self._dh['guid']


  def children(self, child_cls, **kwargs):
    children, offset = node.get_children(db.pool,
                                         self.guid,
                                         child_cls._ctx,
                                         **kwargs)
    return [child_cls(dh=n) for n in children]


class DHPosDict(object):
  def shift(self, index, *args, table=None):
    table = table or self._table
    args = [db.pool] + self._id_args + [self._ctx] + args + [index]
    return table.shift(*args, **kwargs)
    

class DHBaseIdDict(DHDict, DHPostDict):
  _id_arg_strs = ('base_id',)


  def __init__(self, owner=None, dh=None):
    self._owner = owner
    super(DHBaseIdDict, self).__init__(dh or self._fetch_dict)


  @property
  def base_id(self):
    return self._owner.base_id



class DHRelation(DHBaseIdDict):
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None


  @property
  def rel_id(self):
    return self._dh['rel_id']


  def shift(self, index, forward=True, **kwargs):
    table = (forward and self._to_cls or self._from_cls)._table
    super(DHRelation, self).shift(index, forward, table=table)


  def node(self, **kwargs):
    return self._to_cls._table.get(db.pool, 
                                   self.rel_id,
                                   self._ctx, **kwargs)


class DHLookupDict(DHBaseIdDict, DHValueDict):
  _remove_args = ('value',)
  schema = str

  def shift(self, index, **kwargs):
    return self._table.shift(db.pool,
                             self.base_id, 
                             self._ctx, 
                             self.value, 
                             index, **kwargs)

class DHEntityDict(DHGuidDict):
  _table = entity

  def __init__(self, dh=None, **kwargs):
    super(DHEntityDict, self).__init__(dh or entity.create(db.pool,
                                                       self._ctx, **kwargs))


class DHNodeDict(DHGuidDict, DHValueDict, DHPosDict): 
  _table = node
  _remove_arg_strs = ('base_id', )
  _save = node.update

  parent = None 


  def __init__(self, parent=None, value=None, dh=None, **kwargs):
    if value or dh:
      super(Node, self).__init__(dh or node.create(db.pool,
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


class Collection(object):
  _page_size = 100
  _dh_cls = None # a subclass of DHRelation, DHProp, DHAlias, or DHName
  
  def __init__(self, owner):
    self._owner = owner


  def __getitem__(self, idx):
    return self._wrap_result(self._table.list(db.pool, 
                                              self._owner.guid,
                                              self._owner._ctx,
                                              start=idx,
                                              limit=1, **kwargs))

  def __call__(self, **kwargs):
    kwargs.setdefault('start', 0)
    kwargs.setdefault('limit', self._page_size)
    for result_set in self._list(**kwargs):
      for result in result_set:
        yield self._wrap_result(result, **kwargs)


  def _wrap_result(self, result, **kwargs):
    return self._dh_cls(result)


  def _list(**kwargs):
    results = [None]
    start = 0
    while start % kwargs['limit'] == 0 and len(results):
      results, start = self._table.list(db.pool, 
                                        self._owner.guid, 
                                        self._owner._ctx, 
                                        **kwargs)
      if len(results):
        yield results
      else:
        raise StopIteration
    raise StopIteration


  def add(self, value, **kwargs):
    stored = self._dh_cls(self.add(db.pool,
                                   self._owner.guid,
                                   self._dh_cls._ctx,
                                   value, **kwargs))
    if stored:
      return self._dh_cls()


class AliasCollection(Collection):
  add = alias.set


class NameCollection(Collection):
  add = name.create


class RelCollection(Collection):
  _guid_cls_str = None


  @property
  def _guid_cls(self):
    return type(self._owner).__metaclass__.name_to_cls(self._guid_cls_str)


  def _wrap_result(self, result, nodes=False, **kwargs):
    if nodes:
      return (self._dh_cls(result[0]), self._guid_cls(result[1]))
    return self._dh_cls(result)


  def _list(self, nodes=False, **kwargs):
    for rel_set in super(RelCollection, self)._list(**kwargs):
      nid_ctx_pairs = [(rel['rel_id'], self._dh_cls._ctx) for rel in rel_set]
      for idx, node in enumerate(node.batch_get(db.pool, nid_ctx_pairs, **kwargs)):
        yield rel_set[idx], node


  def add(self, dh_instance, directed=False, **kwargs):
    if not directed:
      relation.create(db.pool, self._dh_cls._ctx, dh_instance.guid, self._owner.guid, **kwargs)
    return self._dh_cls(relation.create(db.pool, 
                                        self._dh_cls._ctx
                                        self._owner.guid, 
                                        dh_instance.guid, **kwargs))


class DHAlias(DHLookupDict):
  _table = alias


class DHName(DHLookupDict):
  _table = name
    

class DHProp(DHValueDict, DHBaseIdDict):
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
    # TODO travis, any reason props don't support the _missing pattern in set?
    return prop.set(db.pool, self._base_id, self._ctx, self)


class DHFlags(object):
  def __init__(self, fields):
    self._dirty_mask = 0
    self._fields = fields

  @property
  def _fields(self):
    return self._owner.flags._fields
  

  @property
  def _int_value(self):
    return self._owner._dh['flags']


  @_value.setter
  def _int_value(self, val):
    self._owner._dh['flags'] = val


  @staticmethod
  def _mask_for_range(r):
    return ((2 ** (r[1] - r[0])) - 1) << r[0]


  def __setattr__(self, name, value):
    if name.startswith('_'):
      return super(DHFlags, self).__setattr__(name, value)

    if name in self._fields:
      return self._set_flag(name, value)
    
    super(DHFlags, self).__setattr__(name, value)


  def __getattr__(self, name):
    if name.startswith('_'):
      return self.__dict__[name]

    if name not in self._fields:
      raise AttributeError

    flag_def = self._fields[name]
    bit_range = (self._flag_idxs[name], self._fields[name].size)
    int_val = self._value & self._mask_for_range(bit_range) >> bit_range[0]
    
    if isinstance(flag_def, fields.flag.enum):
      return flag_def.enum_strs[int_val]

    if isinstance(flag_def, fields.flag.bool):
      return bool(int_val)

    return int_val


  def _set_flag(self, flag_name, value):
    flag_def = self._fields[flag_name]
    flag_idx_start = self._flag_idxs[flag_name]
    flag_idx_end = flag_idx_start + flag_def.size
    mask = self._mask_for_range((flag_idx_start, flag_idx_end))
    
    if isinstance(flag_def,  fields.flag.enum):
      try:
        int_value = enum_strs.index(value)    
      except ValueError:
        raise exc.UnknownFlagsEnumValue(name, value, enum_strs)

    int_value = int(value)
    if value > (mask >> bit_range[0]):
      raise exc.FlagValueOverflow(name, value)

    # clear the range
    self._value &= ~mask

    # set the range
    self._value |= value << bit_range[0] 

    self._dirty_mask |= mask


  def __call__(self, flag_name, value=None, **kwargs):
    if value:
      self._set_flag(flag_name, value)
      self.save(**kwargs)
    else:
      return getattr(self, flag_name)


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
      
