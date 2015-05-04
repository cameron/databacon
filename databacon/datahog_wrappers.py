import math

from datahog import node, entity, alias, name, prop, relationship

import exceptions as exc
import db
import metaclasses
from flags import Flags

_no_arg = {}

class Dict(object):
  ''' base class for all classes that wrap datahog dicts, stored at `self._dh`
  
  all datahog objects share a flags attr and a remove method. they all also
  have a context value (a type identifier defined by databacon), and a table
  (e.g., node).

  subsets of datahog objects have other shared attrs and methods, which you will 
  find handled in subclases below.
  '''
  _table = None
  _ctx = None 
  _remove_arg_strs = None 
  _id_arg_strs = None


  def __init__(self, flags=None, dh=None):
    self._dh = dh or {}
    self.flags = flags or Flags(self)

        
  @property
  def _id_args(self):
    return [self._dh[key] for key in self._id_arg_strs]


  @property
  def _remove_args(self):
    return [self._dh[key] for key in self._remove_arg_strs]


  def remove(self, **kw):
    args = self._ids_args + [self._ctx] + self._remove_args
    return self._table.remove(db.pool, *args, **_dhkw(kw))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.cls_by_name[cls_name]

  
  def save_flags(self, add, clear, **kw):
    args = [db.pool] + self._id_args + [self._ctx, add, clear]
    self._table.set_flags(*args, **_dhkw(kw))


class Collection(object):
  _page_size = 100
  _dh_cls = None # a subclass of relation, alias, name, or node (not prop)

  
  def __init__(self, owner):
    self._owner = owner


  def __getitem__(self, idx):
    return self._wrap_result(
      self._get_item(
        db.pool, self._owner.guid, self._owner._ctx, start=idx, limit=1, 
        **kw))


  def __call__(self, **kw):
    kw.setdefault('start', 0)
    kw.setdefault('limit', self._page_size)
    for page, offset in self._pages(**kw):
      for result in page:
        yield self._wrap_result(result, **kw)


  def _wrap_result(self, result, **kw):
    return self._dh_cls(dh=result)


  def _get_item(self, *args, **kw):
    pass


  def _get_page(self, *args, **kw):
    return self._dh_cls._table.list(*args, **_dhkw(kw))


  def _pages(self, **kw):
    results = [None]
    start = 0
    while start % kw['limit'] == 0 and len(results):
      results, start = self._get_page(
        db.pool, self._owner.guid, self._dh_cls._ctx, **kw)
      if len(results):
        yield results, start
      else:
        raise StopIteration
    raise StopIteration


  def add(self, value, **kw):
    stored = self._dh_cls(self.add( # TODO
      db.pool, self._owner.guid, self._dh_cls._ctx, value, **_dhkw(kw)))
    if stored:
      # TODO
      return self._dh_cls()


class FlaggedCollection(Collection):
  __metaclass__ = metaclasses.FlaggedCollectionMC


class ValueDict(Dict):
  ''' adds value access for datahog dicts that have values:
  nodes, props, names, and aliases '''
  __metaclass__ = metaclasses.ValueDictMC
  schema = None


  @property
  def value(self):
    return self._dh['value']


  @value.setter
  def value(self, value):
    self._dh['value'] = value


  def __call__(self, value=_no_arg, **kwargs):
    if value is  _no_arg:
      self._dh = self._get(**kwargs)
      return self
    self.value = value
    self.save()


  def increment(self, **kw):
    if not self.schema is int:
      raise exc.CannotIncrementNonnumericValue()
    
    args = self._id_args + [self._ctx]
    new_val = self._table.increment(db.pool, *args, **_dhw(kw))
    if new_val is none:
      raise exc.DoesNotExist(self)
    self.value = new_val
    return self

# save method names
# - node: update
# - alias/name: none (must remove/create a new one)
# - prop: set


class GuidDict(Dict):
  _id_arg_strs = ('guid', )


  def __init__(self, flags=None, dh=None):
    super(GuidDict, self).__init__(flags=flags, dh=dh)
    self._init_dh_fields()


  def _init_dh_fields(self):
    for attr, val in [(a, getattr(self, a)) for a in dir(self) if '_' not in a]:
      if isinstance(val, type) and issubclass(val, (Dict, Collection)):
        self.__dict__[attr] = val(owner=self)


  @property
  def guid(self):
    return self._dh['guid']


  def children(self, child_cls, **kw):
    children, offset = node.get_children(db.pool,
                                         self.guid,
                                         child_cls._ctx,
                                         **_dhkw(kw))
    return [child_cls(dh=n) for n in children]


  @classmethod
  def by_guid(cls, ids, **kw):
    # TODO look into merging logic with collections
    if hasattr(ids, '__contains__'):
      dh_dicts = cls._table.batch_get(db.pool, 
                           [(id,cls._ctx) for id in ids], 
                           **_dhkw(kw))
      return [cls(dh=dh) for dh in dh_dicts]
    else:
      return cls(cls._table.get(db.pool, ids, cls._ctx))
    

class PosDict(Dict):
  def shift(self, *args, **kw):
    table = kw.get('table', self._table)
    args = [db.pool] + self._id_args + [self._ctx] + args
    return table.shift(*args, **_dhkw(kw))
    

class BaseIdDict(PosDict):
  _id_arg_strs = ('base_id',)


  def __init__(self, owner=None, dh=None):
    self._owner = owner
    super(BaseIdDict, self).__init__(dh)


  @property
  def base_id(self):
    return self._owner._dh['guid']


class Relation(BaseIdDict):
  __metaclass__ = metaclasses.RelationMC
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None


  @property
  def rel_id(self):
    return self._dh['rel_id']


  def shift(self, index, **kw):
    table = (forward and self._to_cls or self._from_cls)._table
    super(Relation, self).shift(forward, index, table=table, **kw)


  @property
  def rel_cls(self):
    return self._cls_by_name[self._rel_cls_str]


  def node(self, **kw):
    return self.rel_cls(
      self.rel_cls._table.get(
        db.pool, 
        self.rel_id,
        self._ctx, **_dhkw(kw)))


  class collection(FlaggedCollection):
    _guid_cls_str = None
    directed = False
    forward = True

    @property
    def _guid_cls(self):
      return self._owner.cls_by_name(self._guid_cls_str)


    def _wrap_result(self, result, nodes=False, **kw):
      if nodes:
        return (self._dh_cls(dh=result[0]), self._guid_cls(dh=result[1]))
      return self._dh_cls(result)


    def _pages(self, nodes=False, **kw):
      for page in super(RelCollection, self)._pages(**kw):
        if not nodes:
          yield page
        else:
          nid_ctx_pairs = [(rel['rel_id'], self._dh_cls._ctx) for rel in page]
          yield zip(page, node.batch_get(
            db.pool, nid_ctx_pairs, **_dhkw(kw)))


    def add(self, dh_instance, directed=False, **kw):
      if not directed:
        relation.create(
          db.pool, self._dh_cls._ctx, dh_instance.guid, self._owner.guid, 
          **_dhkw(kw))
      return self._dh_cls(relation.create(
        db.pool, self._dh_cls._ctx, self._owner.guid, dh_instance.guid, 
        **_dhkw(kw)))



class LookupDict(BaseIdDict, ValueDict):
  _remove_args = ('value',)

  # is this appropriate? don't see schema in travis's test_name
#  schema = str

  def save_flags(self, add, clear, **kw):
    self._table.set_flags(
      db.pool, self._id_args, self.value, add, clear, 
      **_dhkw(kw))


class Entity(GuidDict):
  __metaclass__ = metaclasses.GuidMC
  _table = entity

  def __init__(self, dh=None, **kw):
    super(Entity, self).__init__(
      dh=dh or entity.create(
        db.pool,
        self._ctx, **_dhkw(kw)))


class Node(GuidDict, ValueDict, PosDict): 
  __metaclass__ = metaclasses.NodeMC
  _table = node
  _remove_arg_strs = ('base_id',)
  _save = node.update

  parent = None 


  def __init__(self, parent=None, value=None, dh=None, **kw):
    self.parent = parent
    super(Node, self).__init__(
      dh=dh or node.create(db.pool, parent.guid, self._ctx, value, **_dhkw(kw)))


  def move(self, new_parent, **kw):
    moved =  node.move(
      db.pool, self.guid, self._ctx, self.parent.guid, new_parent.guid, **_dhkw(kw))
    if moved:
      self.parent = new_parent
    return moved


  def save(self, force=False, **kw):
    result = node.update(
      db.pool, self.guid, self._ctx, self, old_value=force and _missing or self, 
      **_dhkw(kw))
    if not result:
      if force:
        raise exc.DoesNotExist(node)
      else:
        raise exc.WillNotUpdateStaleNode(node)
    return result


class ChildCollection(Collection):
  _dh_cls_str = None

  
  @property
  def _dh_cls(self):
    return self._owner._cls_by_name(self._dh_cls_str)

  def _get_page(self, *args, **kw):
    return node.get_children(*args, **_dhkw(kw))


  def list(self, **kw):
    raise NotImplemented("Waiting on a use case to motivate API design")


class Alias(LookupDict):
  _table = alias
  _fetched_value = None

  class collection(FlaggedCollection):
    add = alias.set


  def _get(self, **kw):
    aliases = alias.list(db.pool, self.base_id, self._ctx, **_dhkw(kw))[0]
    if not aliases:
      return {'base_id': self.base_id}
    self._fetched_value = aliases[0]['value'] # for remove during save
    return aliases[0]


  def save(self, **kw):
    if self._fetched_value:
      alias.remove(
        db.pool, self.base_id, self._ctx, self._fetched_value, **_dhkw(kw))
    alias.set(db.pool, self.base_id, self._ctx, self.value, **_dhkw(kw))


class Name(LookupDict):
  _table = name


  class collection(FlaggedCollection):
    add = name.create


  def save(self, **kw):
    # it might happen that the user saves before fetching, 
    # in which case we need to remove the existing entry
    if not self._dh['value']:
      existing_name = name.list(db.pool, self.base_id, self._ctx, limit=1)
      if existing_name and existing_name[0]['value']:
        name.remove(db.pool, self.base_id, self._ctx, existing_name['value'])
    name.create(db.pool, self.base_id, self._ctx, self.value, **_dhkw(kw))


class Prop(ValueDict, BaseIdDict):
  _table = prop

  def __init__(self, **kw):
    self.ignore_remove_race = False
    super(Prop, self).__init__(**kw)


  def _get(self, **kw):
    return prop.get(db.pool, self.base_id, self._ctx, **_dhkw(kw))


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


  def save(self):
    # TODO travis, any reason props don't support the _missing pattern in set?
    return prop.set(db.pool, self._base_id, self._ctx, self)


_allowed = ['timeout', 'forward_index', 'reverse_index', 'by', 'index', 'limit', 'start']
def _dhkw(kw):
  return dict((k, v) for k,v in kw.iteritems() if k in _allowed)
