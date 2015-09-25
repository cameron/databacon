import math
import functools

from datahog import node, entity, alias, name, prop, relationship

import exceptions as exc
import db
import metaclasses
from flags import Flags

_missing = node._missing
guid_prefix = lambda dhw, s: '%s:%s' % (dhw.guid, s)

class Dict(object):
  ''' base class for all classes that wrap datahog dicts, which are 
  stored at `self._dh`.
  
  all datahog objects share a flags attr and a remove method. they all also
  have a context value (a type identifier defined by databacon), and a table
  (e.g., node).

  subsets of datahog objects have other shared attrs and methods, which are
  handled in subclases below.
  '''

  _table = None
  _ctx = None 
  _remove_arg_strs = None 
  _id_arg_strs = None
  _dh = None

  def __init__(self, flags=None, dh=None):
    self._dh = dh or {}
    self.flags = flags or Flags(self)
    self.__ctx = None
        
  @property
  def _ctx(self):
    return self.__ctx

  @_ctx.setter
  def _ctx(self, ctx):
    self.__ctx = ctx

  @property
  def _id_args(self):
    return [self._dh[key] for key in self._id_arg_strs]


  @property
  def _remove_args(self):
    return [self._dh[key] for key in self._remove_arg_strs]


  def remove(self, **kw):
    args = self._ids_args + [self._ctx] + self._remove_args
    return self._table.remove(db.pool, *args, **dhkw(kw))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.user_cls_by_name[cls_name]

  
  def save_flags(self, add, clear, **kw):
    args = [db.pool] + self._id_args + [self._ctx, add, clear]
    self._table.set_flags(*args, **dhkw(kw))


  def json(self):
    ''' Not the best name -- actually returns a jsonable dict (flags are a set,
    which doesn't serialize to json).'''
    json = self._dh.copy()
    json['flags'] = list(json['flags'])
    del json['ctx']
    return json


class List(object):
  __metaclass__ = metaclasses.ListMC
  default_page_size = 100
  of_type = None
  _owner = None

  def __init__(self, owner, *args, **kw):
    self._owner = owner
    super(List, self).__init__(*args, **kw)


  def __getitem__(self, idx):
    return self._wrap_result(
      self._get_page(
        db.pool, self._owner.guid, self.of_type._ctx, start=idx, limit=1)[0][0])


  def __call__(self, **kw):
    kw.setdefault('start', 0)
    kw.setdefault('limit', self.default_page_size)
    for page, offset in self._pages(**kw):
      for result in page:
        yield self._wrap_result(result, **dhkw(kw, blacklist=True))


  def _wrap_result(self, result, **kw):
    return self.of_type(dh=result, owner=self._owner, **kw)


  def _get_page(self, *args, **kw):
    return self.of_type._table.list(*args, **dhkw(kw))


  def _pages(self, **kw):
    results = [None]
    offset = 0
    while offset % kw['limit'] == 0 and len(results):
      results, offset = self._get_page(
        db.pool, self._owner.guid, self.of_type._ctx, **kw)
      if len(results):
        yield results, offset
      else:
        raise StopIteration
    raise StopIteration


  def add(self, value, flags=None, **kw):
    return self._add(db.pool, 
                     self._owner.guid,
                     self.of_type._ctx,
                     value,
                     flags=flags._flags_set,
                     **dhkw(kw))


  def __getattr__(self, name):
    ''' For node.children.by_child_alias lookups '''
    if name.startswith('by_'):
      lookup = getattr(self.of_type, name, None)
      if lookup:
        return functools.partial(lookup, scope_to_parent=self._owner)
    raise AttributeError


class ValueDict(Dict):
  ''' adds value access for datahog dicts that have values:
  nodes, props, names, and aliases '''
  __metaclass__ = metaclasses.ValueDictMC
  schema = None


  def __init__(self, *args, **kwargs):
    super(ValueDict, self).__init__(*args, **kwargs)
    self.old_value = self.value

  def default_value(self):
    return ({
      int: 0,
      str: '',
      unicode: u'',
      type(None): None,
    }).get(type(self.schema) is type and self.schema or type(self.schema), None)
    

  @property
  def value(self):
    return self._dh.get('value')


  @value.setter
  def value(self, value):
    self._dh['value'] = value


  def __call__(self, value=_missing, flags=None, force=False, **kwargs):
    if value is _missing:
      self._dh = self._get(**kwargs)
      return self

    self.value = value
    self.save(force=force)
    
    if flags:
      self.flags = flags
      # TODO this feels WET 
      self._dh['flags'] = flags._tmp_set
      self.flags._owner = self
      self.flags.save()
    return self

  def increment(self, **kw):
    if not self.schema is int:
      raise exc.CannotIncrementNonnumericValue()
    
    args = self._id_args + [self._ctx]
    new_val = self._table.increment(db.pool, *args, **dhkw(kw))
    if new_val is None:
      raise exc.DoesNotExist(self)
    self.value = new_val
    return self

# TODO unify save() methods
#  - and .save() flags
#  - list of save method names
#   - node: update
#   - alias/name: none (must remove/create a new one)
#   - prop: set


class GuidDict(Dict):
  _id_arg_strs = ('guid',)
  _remove_arg_strs = ('guid',)
  

  def __init__(self, flags=None, dh=None):
    super(GuidDict, self).__init__(flags=flags, dh=dh)
    self._instantiate_attr_classes()


  def _instantiate_attr_classes(self):
    attrs = [a for a in dir(self) if '__' not in a]
    for attr, val in [(a, getattr(self, a)) for a in attrs]:
      if isinstance(val, type) and issubclass(val, (Dict, List)):
        self.__dict__[attr] = val(owner=self)


  @property
  def guid(self):
    return self._dh['guid']


  # TODO 
  # merge with the child accessors / lists?
  # def children(self, child_cls, **kw):
  #   children, offset = node.get_children(db.pool,
  #                                        self.guid,
  #                                        child_cls._ctx,
  #                                        **dhkw(kw))
  #   return [child_cls(dh=n) for n in children]


  @classmethod
  def _by_guid(cls, ids, **kw):
    ids = type(ids) in (list, tuple) and ids or (ids,)
    if type(ids[0]) not in (list, tuple):
      ids = [(id,cls._ctx) for id in ids]
    return cls._table.batch_get(db.pool, 
                                ids,
                                **dhkw(kw))
    

  @classmethod
  def by_guid(cls, ids, **kw):
    dicts = cls._by_guid(ids, **kw)
    if type(ids) in (list, tuple):
      return [cls(dh=dh) for dh in dicts]
    else:
      return cls(dh=dicts[0])
    

class PosDict(Dict):


  def shift(self, *args, **kw):
    args = [db.pool] + self._id_args + [self._ctx] + list(args)
    return self._table.shift(*args, **dhkw(kw))
    

class BaseIdDict(PosDict):
  _id_arg_strs = ('base_id',)
  base_cls = None
  _owner = None

  def __init__(self, owner=None, dh=None):
    self._owner = owner
    dh = dh or {'base_id': owner.guid}
    super(BaseIdDict, self).__init__(dh=dh)


  @property
  def base_id(self):
    return self._owner._dh['guid']


class Relation(BaseIdDict):
  __metaclass__ = metaclasses.RelationMC
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None
  forward = True
  rel_cls = None


  @property
  def rel_id(self):
    return self._dh['rel_id']


  @property
  def base_id(self):
    return self._dh['base_id']


  def shift(self, index, **kw):
    super(Relation, self).shift(self.forward, index, **kw)


  def node(self, **kw):
    if self.forward:
      guid, cls = self.rel_id, self.rel_cls
    else:
      guid, cls = self.base_id, self.base_cls
    return cls(dh=cls._table.get(
      db.pool, 
      guid,
      cls._ctx, **dhkw(kw)))


  class List(List):


    def _wrap_result(self, result, nodes=False, **kw):
      if nodes:
        return (self.of_type(dh=result[0], owner=self._owner), 
                self.of_type.base_cls(dh=result[1], owner=self._owner))
      return self.of_type(dh=result, owner=self._owner)


    def _pages(self, nodes=False, **kw):
      for page, offset in super(Relation.List, self)._pages(**kw):
        if nodes:
          if self.of_type.forward:
            cls = self.of_type.rel_cls
            id_key = 'rel_id'
          else:
            cls = self.of_type.base_cls
            id_key = 'base_id'
          ctx = cls._ctx
          id_ctx_pairs = [(rel[id_key], ctx) for rel in page]
          timeout = kw.get('timeout')
          guid_dicts = cls._by_guid(id_ctx_pairs, timeout=timeout)
          yield (zip(page, guid_dicts), offset)
        else:
          yield page, offset


    def _get_page(self, *args, **kw):
      kw['forward'] = self.of_type.forward
      return super(Relation.List, self)._get_page(*args, **kw)


    def add(self, db_instance, flags=None, **kw):
      if self.of_type.forward:
        base_id, rel_id = self._owner.guid, db_instance.guid
      else:
        base_id, rel_id = db_instance.guid, self._owner.guid
      return relationship.create(db.pool, 
                                 self.of_type._ctx,
                                 base_id,
                                 rel_id,
                                 flags=flags and flags._flags_set or None,
                                 **dhkw(kw))


class LookupDict(BaseIdDict, ValueDict):
  _remove_arg_strs = ('value',)

  # is this schema appropriate? don't see schema in travis's test_name
  # possible that it's part of the user-facing API?
  # schema = str

  def _get(self, **kw):
    entries = self._table.list(db.pool, self.base_id, self._ctx, **dhkw(kw))[0]
    if not entries:
      return {'base_id': self.base_id}
    self._fetched_value = entries[0]['value'] # for remove during save
    return entries[0]


class Entity(GuidDict):
  __metaclass__ = metaclasses.GuidMC
  _table = entity


  def __init__(self, dh=None, **kw):
    super(Entity, self).__init__(
      dh=dh or entity.create(
        db.pool,
        self._ctx, **dhkw(kw)))


class Node(GuidDict, ValueDict, PosDict): 
  __metaclass__ = metaclasses.NodeMC
  _table = node
  _save = node.update
  parent = None 


  def __init__(self, value=_missing, parent=None, dh=None, **kw):
    self.parent = parent
    if not dh:
      if value is _missing:
        value = self.default_value()
      dh = node.create(db.pool, 
                       parent.guid,
                       self._ctx,
                       value,
                       **dhkw(kw))
    super(Node, self).__init__(dh=dh)


  def move(self, new_parent, **kw):
    moved =  node.move(
      db.pool, self.guid, self._ctx, self.parent.guid, new_parent.guid, **dhkw(kw))
    if moved:
      self.parent = new_parent
    return moved


  def save(self, force=False, **kw):
    old_value = force and _missing or self.old_value
    result = node.update(db.pool,
                         self.guid,
                         self._ctx,
                         self.value,
                         old_value=old_value,
                         **dhkw(kw))
    if not result:
      if force:
        raise exc.DoesNotExist(node)
      else:
        raise exc.WillNotUpdateStaleNode(node)

    self.old_value = self.value
    return self

  class List(List):
    def _wrap_result(self, *args, **kw):
      return super(Node.List, self)._wrap_result(*args, parent=self._owner, **kw)

    def _get_page(self, *args, **kw):
      return node.get_children(*args, **dhkw(kw))

    @property
    def flags(self):
      raise AttributeError


class Alias(LookupDict):
  _table = alias
  _fetched_value = None
  uniq_to_parent = None


  def __call__(self, value=_missing, **kwargs):
    if value is not _missing and self.uniq_to_parent:
      value = guid_prefix(self._owner.parent, value)
    return super(Alias, self).__call__(value=value, **kwargs)


  @property
  def value(self):
    val = LookupDict.value.fget(self)
    if val and self.uniq_to_parent:
      return val.replace('%s:' % self._owner.parent.guid, '')
    return val

  @value.setter
  def value(self, value):
    if self.uniq_to_parent:
      if not value.startswith('%s:' % self._owner.parent.guid):
        value = guid_prefix(self._owner.parent, value)
    self._dh['value'] = value
    LookupDict.value.fset(self, value)


  class List(List):
    def _add(self, *args, **kwargs):
      if self.of_type.uniq_to_parent:
        args[3] = self._uniq_to_parent_val(args[3])
      return alias.set(*args, **kwargs)


  def save(self, **kw):
    # Q: does this need some dirty-checking logic?
    if self._fetched_value:
      alias.remove(
        db.pool, self.base_id, self._ctx, self._fetched_value, **dhkw(kw))
    alias.set(db.pool, self.base_id, self._ctx, self._dh['value'], **dhkw(kw))


  @classmethod
  def lookup(cls, value, scope_to_parent=None, **kw):
    if scope_to_parent:
      value = guid_prefix(scope_to_parent, value)
    dh_alias = alias.lookup(db.pool, value, cls._ctx, **dhkw(kw))
    if dh_alias:
      return cls.base_cls.by_guid(dh_alias['base_id'])


class Name(LookupDict):
  _table = name

  class List(List):
    add = name.create

  @classmethod
  def lookup(cls, value, **kw):
    dh_names, offset = name.search(db.pool, value, cls._ctx, **dhkw(kw))
    if len(dh_names):
      return cls.base_cls.by_guid([n['base_id'] for n in dh_names])


  def save(self, **kw):
    # it might happen that the user saves before fetching, 
    # in which case we need to remove the existing entry
    if not self._dh['value']:
      existing_name = name.list(db.pool, self.base_id, self._ctx, limit=1)
      if existing_name and existing_name[0]['value']:
        name.remove(db.pool, self.base_id, self._ctx, existing_name['value'])
    name.create(db.pool, self.base_id, self._ctx, self.value, **dhkw(kw))


class Prop(ValueDict, BaseIdDict):
  _table = prop

  def __init__(self, **kw):
    self.ignore_remove_race = False
    super(Prop, self).__init__(**kw)


  def _get(self, **kw):
    return prop.get(db.pool, self.base_id, self._ctx, **dhkw(kw))


  @property
  def _remove_args(self):
    if self.ignore_remove_race:
      return []
    return [self.value]


  def remove(self, ignore_remove_race=None, **kw):
    was = self.ignore_remove_race
    if ignore_remove_race is not None:
      self.ignore_remove_race = ignore_remove_race
    super(Prop, self).remove(**kw)
    self.ignore_remove_race = was


  def save(self, **kwargs):
    # TODO travis, any reason props don't support the _missing pattern in set?
    return prop.set(db.pool, self.base_id, self._ctx, self.value)


dh_kwargs = ['timeout',
             'forward_index',
             'reverse_index',
             'by',
             'index',
             'limit',
             'start',
             'forward']
def dhkw(kw, blacklist=False):
  ''' Intersect or exclude datahog kwargs '''
  pass_thru = {}
  for key, val in kw.iteritems():
    if key in dh_kwargs and not blacklist:
      pass_thru[key] = val
    elif key not in dh_kwargs and blacklist:
      pass_thru[key] = val
  return pass_thru
