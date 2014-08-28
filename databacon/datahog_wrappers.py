import math

from datahog import node, entity, alias, name, prop, relationship

import exceptions as exc
import db
import metaclasses
import flags as flgs 


class Field(object):
  pass


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
    self._dh = dh
    self.flags._owner = self

        
  @property
  def _id_args(self):
    return [self._dh[key] for key in self._id_arg_strs]


  @property
  def _remove_args(self):
    return [self._dh[key] for key in self._remove_arg_strs]


  def remove(self, **kwargs):
    args = self._ids_args + [self._ctx] + self._remove_args
    return self._table.remove(db.pool, *args, **_dhkw(kwargs))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.cls_by_name[cls_name]


class Collection(Field):
  _page_size = 100
  _dh_cls = None # a subclass of relation, alias, name, or node (not prop)

  
  def __init__(self, owner):
    self._owner = owner


  def __getitem__(self, idx):
    return self._wrap_result(
      self._get_item(
        db.pool, self._owner.guid, self._owner._ctx, start=idx, limit=1, 
        **kwargs))


  def __call__(self, **kwargs):
    kwargs.setdefault('start', 0)
    kwargs.setdefault('limit', self._page_size)
    for page, offset in self._pages(**kwargs):
      for result in page:
        yield self._wrap_result(result, **kwargs)


  def _wrap_result(self, result, **kwargs):
    return self._dh_cls(dh=result)


  def _get_item(self, *args, **kwargs):
    pass


  def _get_page(self, *args, **kwargs):
    return self._dh_cls._table.list(*args, **_dhkw(kwargs))


  def _pages(self, **kwargs):
    results = [None]
    start = 0
    while start % kwargs['limit'] == 0 and len(results):
      results, start = self._get_page(
        db.pool, self._owner.guid, self._dh_cls._ctx, **kwargs)
      if len(results):
        yield results, start
      else:
        raise StopIteration
    raise StopIteration


  def add(self, value, **kwargs):
    stored = self._dh_cls(self.add( # TODO
      db.pool, self._owner.guid, self._dh_cls._ctx, value, **_dhkw(kwargs)))
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
    # todo mummy validation
    self._dh['value'] = value


  def increment(self, **kwargs):
    if not self.schema is int:
      raise exc.cannotincrementnonnumericvalue()
    
    args = self._id_args + [self._ctx]
    new_val = self._table.increment(db.pool, *args, **_dhw(kwargs))
    if new_val is none:
      raise exc.doesnotexist(self)
    self.value = new_val
    return self


  def save(self, **kwargs):
    return self._save(self._id_args, self._ctx, self.value, **kwargs)

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


  def children(self, child_cls, **kwargs):
    children, offset = node.get_children(db.pool,
                                         self.guid,
                                         child_cls._ctx,
                                         **_dhkw(kwargs))
    return [child_cls(dh=n) for n in children]


class PosDict(Dict):
  def shift(self, *args, **kwargs):
    table = kwargs.get('table', self._table)
    args = [db.pool] + self._id_args + [self._ctx] + args
    return table.shift(*args, **_dhkw(kwargs))
    

class BaseIdDict(PosDict, Field):
  _id_arg_strs = ('base_id',)


  def __init__(self, owner=None, dh=None):
    self._owner = owner
    super(BaseIdDict, self).__init__(dh)


  @property
  def base_id(self):
    return self._owner.base_id


class Relation(BaseIdDict):
  __metaclass__ = metaclasses.RelationMC
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None


  @property
  def rel_id(self):
    return self._dh['rel_id']


  def shift(self, index, **kwargs):
    table = (forward and self._to_cls or self._from_cls)._table
    super(Relation, self).shift(forward, index, table=table, **kwargs)


  @property
  def rel_cls(self):
    return self._cls_by_name[self._rel_cls_str]


  def node(self, **kwargs):
    return self.rel_cls(
      self.rel_cls._table.get(
        db.pool, 
        self.rel_id,
        self._ctx, **_dhkw(kwargs)))


  class collection(FlaggedCollection):
    _guid_cls_str = None
    directed = False
    forward = True

    @property
    def _guid_cls(self):
      return self._owner.cls_by_name(self._guid_cls_str)


    def _wrap_result(self, result, nodes=False, **kwargs):
      if nodes:
        return (self._dh_cls(dh=result[0]), self._guid_cls(dh=result[1]))
      return self._dh_cls(result)


    def _pages(self, nodes=False, **kwargs):
      for page in super(RelCollection, self)._pages(**kwargs):
        if not nodes:
          yield page
        else:
          nid_ctx_pairs = [(rel['rel_id'], self._dh_cls._ctx) for rel in page]
          yield zip(page, node.batch_get(db.pool, nid_ctx_pairs, **_dhkw(kwargs)))


    def add(self, dh_instance, directed=False, **kwargs):
      if not directed:
        relation.create(
          db.pool, self._dh_cls._ctx, dh_instance.guid, self._owner.guid, **_dhkw(kwargs))
      return self._dh_cls(relation.create(
        db.pool, self._dh_cls._ctx, self._owner.guid, dh_instance.guid, **_dhkw(kwargs)))



class LookupDict(BaseIdDict, ValueDict):
  _remove_args = ('value',)
  schema = str


class Entity(GuidDict):
  __metaclass__ = metaclasses.GuidMC
  _table = entity


  def __init__(self, dh=None, **kwargs):
    super(Entity, self).__init__(
      dh=dh or entity.create(
        db.pool,
        self._ctx, **_dhkw(kwargs)))


class Node(GuidDict, ValueDict, PosDict): 
  __metaclass__ = metaclasses.NodeMC
  _table = node
  _remove_arg_strs = ('base_id', )
  _save = node.update

  parent = None 


  def __init__(self, parent=None, value=None, dh=None, **kwargs):
    self.parent = parent
    super(Node, self).__init__(
      dh=dh or node.create(db.pool, parent.guid, self._ctx, value, **_dhkw(kwargs)))


  def move(self, new_parent, **kwargs):
    moved =  node.move(
      db.pool, self.guid, self._ctx, self.parent.guid, new_parent.guid, **_dhkw(kwargs))
    if moved:
      self.parent = new_parent
    return moved


  def save(self, force=False, **kwargs):
    result = node.update(
      db.pool, self.guid, self._ctx, self, old_value=force and _missing or self, 
      **_dhkw(kwargs))
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

  def _get_page(self, *args, **kwargs):
    return node.get_children(*args, **_dhkw(kwargs))


  def list(self, **kwargs):
    raise NotImplemented("Waiting on a use case to motivate API design")


class Alias(LookupDict):
  _table = alias
  class collection(FlaggedCollection):
    add = alias.set


class Name(LookupDict):
  _table = name
  class collection(FlaggedCollection):
    add = name.create


class Prop(ValueDict, BaseIdDict):
  _table = prop

  def __init__(self, **kwargs):
    self.ignore_remove_race = False
    super(Prop, self).__init__(**kwargs)


  def _get(self, **kwargs):
    return prop.get(self.base_id, self._ctx, **_dhkw(kwargs))


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


_allowed = ['timeout', 'forward_index', 'reverse_index', 'by', 'index', 'limit', 'start']
def _dhkw(kwargs):
  return dict((k, v) for k,v in kwargs.iteritems() if k in _allowed)
