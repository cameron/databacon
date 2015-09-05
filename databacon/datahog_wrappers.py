import math
import functools

from datahog import node, entity, alias, name, prop, relationship

import exceptions as exc
import db
import metaclasses
from flags import Flags

_no_arg = {}

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
    return self._table.remove(db.pool, *args, **_dhkw(kw))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.user_cls_by_name[cls_name]

  
  def save_flags(self, add, clear, **kw):
    args = [db.pool] + self._id_args + [self._ctx, add, clear]
    self._table.set_flags(*args, **_dhkw(kw))


class List(object):
  __metaclass__ = metaclasses.ListMC
  default_page_size = 100
  of_type = None


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
        yield self._wrap_result(result, **kw)


  def _wrap_result(self, result, **kw):
    return self.of_type(dh=result, owner=self._owner)


  def _get_page(self, *args, **kw):
    return self.of_type._table.list(*args, **_dhkw(kw))


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
    return self._add(db.pool, self._owner.guid, self.of_type._ctx, value, flags=flags._flags_set, **_dhkw(kw))



class ValueDict(Dict):
  ''' adds value access for datahog dicts that have values:
  nodes, props, names, and aliases '''
  __metaclass__ = metaclasses.ValueDictMC
  schema = None


  @classmethod
  def default_value(self):
    return ({
      int: 0,
      str: '',
      type(None): None,
    }).get(type(self.schema) is type and self.schema or type(self.schema), {})
    

  @property
  def value(self):
    return self._dh['value']


  @value.setter
  def value(self, value):
    self._dh['value'] = value


  def __call__(self, value=_no_arg, flags=None, **kwargs):
    if value is _no_arg:
      # TODO test this case
      self._dh = self._get(**kwargs)
      return self

    self.value = value
    self.save()
    
    if flags:
      # TODO this feels super gross
      self.flags = flags
      self._dh['flags'] = flags._tmp_set
      self.flags._owner = self
      self.flags.save()


  def increment(self, **kw):
    if not self.schema is int:
      raise exc.CannotIncrementNonnumericValue()
    
    args = self._id_args + [self._ctx]
    new_val = self._table.increment(db.pool, *args, **_dhkw(kw))
    if new_val is None:
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
    self._instantiate_attr_classes()


  def _instantiate_attr_classes(self):
    for attr, val in [(a, getattr(self, a)) for a in dir(self) if '_' not in a]:
      if isinstance(val, type) and issubclass(val, (Dict, List)):
        self.__dict__[attr] = val(owner=self)


  @property
  def guid(self):
    return self._dh['guid']


  # TODO merge with the child accessors / lists?
  def children(self, child_cls, **kw):
    children, offset = node.get_children(db.pool,
                                         self.guid,
                                         child_cls._ctx,
                                         **_dhkw(kw))
    return [child_cls(dh=n) for n in children]


  @classmethod
  def by_guid(cls, ids, **kw):
    # TODO merge with lists?
    if hasattr(ids, '__contains__'):
      dh_dicts = cls._table.batch_get(db.pool, 
                           [(id,cls._ctx) for id in ids], 
                           **_dhkw(kw))
      return [cls(dh=dh) for dh in dh_dicts]
    else:
      return cls(cls._table.get(db.pool, ids, cls._ctx))
    

class PosDict(Dict):


  def shift(self, *args, **kw):
    table = kw.get('table', self._table) # TODO necessary?
    args = [db.pool] + self._id_args + [self._ctx] + list(args)
    return table.shift(*args, **_dhkw(kw))
    

class BaseIdDict(PosDict):
  _id_arg_strs = ('base_id',)
  base_cls = None
  

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

#  @classmethod
#  def __name__(self):
    # TODO this needs uniqification along with the relationship instantiation
#    return 'Relation.%s-%s' % (from_cls.__name__, _rel_cls_str)

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
      cls._ctx, **_dhkw(kw)))


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
            id_key, ctx = 'rel_id', self.of_type.rel_cls._ctx
          else:
            id_key, ctx = 'base_id', self.of_type.base_cls._ctx
          nid_ctx_pairs = [(rel[id_key], ctx) for rel in page]
          yield (zip(page, node.batch_get(
            db.pool, nid_ctx_pairs, timeout=kw.get('timeout', None))), offset)
        else:
          yield page, offset


    def _get_page(self, *args, **kw):
      kw['forward'] = self.of_type.forward
      return super(Relation.List, self)._get_page(*args, **kw)


    def add(self, dh_instance, flags=None, **kw):
      if self.of_type.forward:
        base_id, rel_id = self._owner.guid, dh_instance.guid
      else:
        base_id, rel_id = dh_instance.guid, self._owner.guid
      return relationship.create(db.pool, 
                                 self.of_type._ctx,
                                 base_id,
                                 rel_id,
                                 flags=flags and flags._flags_set or None,
                                 **_dhkw(kw))


class LookupDict(BaseIdDict, ValueDict):
  _remove_args = ('value',)

  # is this schema appropriate? don't see schema in travis's test_name
  # possible that it's part of the user-facing API?
  # schema = str

  # TODO why is this here and not shared with prop (if not also value? rel?)
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
    if not dh:
      dh = node.create(db.pool, 
                       parent.guid,
                       self._ctx,
                       value or self.default_value(),
                       **_dhkw(kw))
    super(Node, self).__init__(dh=dh)


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


  class List(List):
    def _get_page(self, *args, **kw):
      return node.get_children(*args, **_dhkw(kw))

    @property
    def flags(self):
      raise AttributeError


class Alias(LookupDict):
  _table = alias
  _fetched_value = None


  class List(List):
    def _add(self, *args, **kwargs):
      return alias.set(*args, **kwargs)


  def _get(self, **kw):
    aliases = alias.list(db.pool, self.base_id, self._ctx, **_dhkw(kw))[0]
    if not aliases:
      return {'base_id': self.base_id} # TODO can we avoid this?
    self._fetched_value = aliases[0]['value'] # for remove during save
    return aliases[0]


  def save(self, **kw):
    # Q: does this need some dirty-checking logic?
    if self._fetched_value:
      alias.remove(
        db.pool, self.base_id, self._ctx, self._fetched_value, **_dhkw(kw))
    alias.set(db.pool, self.base_id, self._ctx, self.value, **_dhkw(kw))




class Name(LookupDict):
  _table = name

  class List(List):
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
    return prop.set(db.pool, self.base_id, self._ctx, self.value)


_allowed = ['timeout', 'forward_index', 'reverse_index', 'by', 'index', 'limit', 'start', 'forward']
def _dhkw(kw):
  return dict((k, v) for k,v in kw.iteritems() if k in _allowed)
