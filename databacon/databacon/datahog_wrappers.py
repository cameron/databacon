import math
import functools

from datahog import node, alias, name, prop, relationship
import greenhouse

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
    self._dh.setdefault('flags', set())
    self.flags = flags or Flags(owner=self)

        
  @property
  def _id_args(self):
    return [self._dh[key] for key in self._id_arg_strs]


  @property
  def _remove_args(self):
    return [self._dh[key] for key in self._remove_arg_strs]


  def remove(self, **kw):
    # TODO test
    args = self._id_args + [self._ctx] + self._remove_args
    return self._table.remove(db.pool, *args, **dhkw(kw))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.user_cls_by_name[cls_name]

  
  def save_flags(self, add, clear, **kw):
    args = [db.pool] + self._id_args + [self._ctx, add, clear]
    res = self._table.set_flags(*args, **dhkw(kw))
    if res is None:
      raise Exception('flags not saved')


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
        db.pool, self._owner.guid, self.of_type._ctx, start=idx, limit=1)[0][0], edges='only')


  def __call__(self, **kw):
    # TODO test multipage results (there was a bug that cause infinite looping)
    kw.setdefault('start', 0)
    kw.setdefault('limit', self.default_page_size)
    for page, offset in self._pages(**kw):
      for result in page:
        yield self._wrap_result(result, edges=kw.get('edges', None))
 

  def _wrap_result(self, result, edges=None):
    return self.of_type(dh=result, owner=self._owner)


  def _get_page(self, *args, **kw):
    return self.of_type._table.list(*args, **dhkw(kw))


  def _pages(self, **kw):
    results = [None]
    while len(results):
      results, kw['start'] = self._get_page(
        db.pool, self._owner.guid, self.of_type._ctx, **kw)
      if len(results):
        yield results, kw['start']
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


class ValueDict(Dict):
  ''' adds value access for datahog dicts that have values:
  nodes, props, names, and aliases '''
  __metaclass__ = metaclasses.ValueDictMC
  schema = None


  def __init__(self, *args, **kwargs):
    kwargs.setdefault('dh', {}).setdefault('value', self.default_value())
    super(ValueDict, self).__init__(*args, **kwargs)
    if type(self.value) is dict:
      self.old_value = self.value.copy()
    elif type(self.value) is list:
      self.old_value = list(self.value)
    else:
      self.old_value = self.value

  def default_value(self):
    return ({
      int: 0,
      str: '',
      unicode: u'',
      type(None): None,
      dict: {},
      list: []
    }).get(type(self.schema) is type and self.schema or type(self.schema), None)
    

  @property
  def value(self):
    return self._dh.get('value')


  @value.setter
  def value(self, value):
    self._dh['value'] = value


  def __call__(self, value=_missing, force_overwrite=False, lazy=True, **kwargs):
    if value is _missing and getattr(self, 'base_id', self._owner.guid): 
      if lazy and self.value:
        return self
      self._get(**kwargs)
      return self

    self.value = value is _missing and self.default_value() or value
    self.save(force_overwrite=force_overwrite)
    
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


class GuidDict(Dict):
  _id_arg_strs = ('id',)
  _remove_arg_strs = ('id',)

  def __init__(self, *args, **kwargs):
    super(GuidDict, self).__init__(flags=kwargs.get('flags', None), dh=kwargs.get('dh', None))
    self._instantiate_attr_classes()


  # if this gets to be a performance problem...
  # - look into slots
  # - or possibly just make it lazy via getattr
  def _instantiate_attr_classes(self):
    for attr, val in [(a, getattr(self, a)) for a in self._datahog_attrs]:
      if isinstance(val, type) and issubclass(val, (Dict, List)):
        self.__dict__[attr] = val(owner=self)


  @property
  def guid(self):
    return self._dh.get('id', None)


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
    dh = dh or {}
    dh.setdefault('base_id', getattr(owner, 'guid', None))
    super(BaseIdDict, self).__init__(dh=dh)


  @property
  def base_id(self):
    return self._dh.get('base_id', None)


class Relation(BaseIdDict, ValueDict):
  __metaclass__ = metaclasses.RelationMC
  _id_arg_strs = ('base_id', 'rel_id')
  _remove_arg_strs = tuple()
  _table = relationship
  rel_cls = None

  # databacon doesn't expose directed relations yet, but it's possible to do so
  # by setting `forward = False` on a backwards-facing subclass of Relation.
  forward = True 


  @property
  def rel_id(self):
    return self._dh['rel_id']


  def shift(self, index, **kw):
    super(Relation, self).shift(self.forward, index, **kw)


  def save(self, force_overwrite=None, **kw):
    # TODO old_value=_missing support for set (like node.update)
    # TODO force_overwrite
    return relationship.update(db.pool, self.base_id, self.rel_id, self._ctx, self.value, **kw)


  def node(self, which='base', **kw):
    # grossness related to undirected relationships
    other_cls = 'base' if getattr(self, 'undirected_subclass', False) else 'rel'
    other_id = 'rel' if getattr(self, 'undirected_subclass', False) else 'base'
    cls = getattr(self, '%s_cls' % other_cls)
    id = getattr(self, '%s_id' % other_id)
    return cls.by_guid(id)

  class List(List):
    def _wrap_result(self, result, edges=None):
      edge, node = None, None
      if not edges:
        node = result
      elif edges == 'only':
        edge = result
      elif edges:
        edge, node = result

      if node:
        node_cls = self._node_cls()
        node = node_cls(dh=node, owner=self._owner)
      if edge:
        edge = self.of_type(dh=edge, owner=self._owner)

      if edges and not edges == 'only':
        return edge, node

      return filter(lambda i: i, (edge, node))[0]


    def _pages(self, edges=None, **kw):
      if edges not in (True, None, False, 'only'):
        raise Exception('Invalid `over` value. Pass one of `True`, `False`, `None`, or \'only\'')

      for edges_page, offset in super(Relation.List, self)._pages(**kw):
        if edges == 'only':
          yield edges_page, offset
        else:
          nodes = self._nodes_for_page(edges_page, edges=edges, **kw)
          
          if not edges:
            yield nodes, offset
          else:
            yield zip(edges_page, nodes), offset


    def _node_cls(self):
      ''' TODO This particular inelegance is related the the undirected
      relationships mess.'''
      return type(self._owner) is self.of_type.base_cls and \
        self.of_type.rel_cls or self.of_type.base_cls


    def _nodes_for_page(self, page, **kw):
      if self.of_type.forward:
        # TODO could/should probably fix this during field subclassing rather than here 
        cls = self._node_cls()
        id_key = 'rel_id'
      else: # in theory this else is dead until directed relationships come back
        cls = self.of_type.base_cls
        id_key = 'base_id'
      ctx = cls._ctx
      id_ctx_pairs = [(rel[id_key], ctx) for rel in page]
      return cls._by_guid(id_ctx_pairs, timeout=kw.get('timeout'))


    def _get_page(self, *args, **kw):
      kw['forward'] = self.of_type.forward
      return super(Relation.List, self)._get_page(*args, **kw)


    def get(self, other=None, guid=None, **kw):
      dh = relationship.get(
        db.pool,
        self.of_type._ctx,
        self._owner.guid,
        other and other.guid or guid, **dhkw(kw))
      return dh and self.of_type(dh=dh) or None


    def add(self, other=None, guid=None, value=None, flags=None, **kw):
      guid = other and other.guid or guid
      if not guid:
        raise Exception('nothing to add')

      if self.of_type.forward:
        base_id, rel_id = self._owner.guid, guid
      else:
        base_id, rel_id = guid, self._owner.guid

      return relationship.create(db.pool, 
                                 self.of_type._ctx,
                                 base_id,
                                 rel_id,
                                 value=value,
                                 # TODO where is forward in this mess?
                                 flags=flags and flags._flags_set or None,
                                 **dhkw(kw))

    def remove(self, other=None, guid=None):
      guid = other and other.guid or guid
      return relationship.remove(db.pool,
                                 self._owner.guid,
                                 guid,
                                 self.of_type._ctx)


# TODO merge GuidDict and Node
class Node(GuidDict, ValueDict, PosDict): 
  __metaclass__ = metaclasses.NodeMC
  _table = node
  _save = node.update
  parent = None 


  def __init__(self, *args, **kw):
    self.parent = kw.get('parent', None)
    had_dh = dh = kw.get('dh', None)
    if not dh:
      dh = node.create(db.pool, 
                       self._ctx,
                       kw.get('value', self.default_value()),
                       base_id=getattr(self.parent,'guid', None),
                       **dhkw(kw))
    super(Node, self).__init__(dh=dh)
    if not had_dh and hasattr(self, 'new'):
      self.new(*args, **kw)


  def parent_guid(self):
    return 


  def move(self, new_parent, **kw):
    moved =  node.move(
      db.pool, self.guid, self._ctx, self.parent.guid, new_parent.guid, **dhkw(kw))
    if moved:
      self.parent = new_parent
    return moved


  def save(self, force_overwrite=False, **kw):
    old_value = force_overwrite and _missing or self.old_value
    result = node.update(db.pool,
                         self.guid,
                         self._ctx,
                         self.value,
                         old_value=old_value,
                         **dhkw(kw))
    if not result:
      if force_overwrite:
        raise exc.DoesNotExist(node)
      else:
        raise exc.WillNotUpdateStaleNode(node)
    
    self.old_value = self.value
    return self


class LookupDict(BaseIdDict, ValueDict):
  _remove_arg_strs = ('value',)

  def _get(self, **kw):
    entries = self._table.list(db.pool, self.base_id, self._ctx, **dhkw(kw))[0]
    if not entries:
      self._dh = {'base_id': self.base_id}
      return
    self._fetched_value = entries[0]['value'] # for remove during save
    self._dh = entries[0]


class Alias(LookupDict):
  _table = alias
  _fetched_value = None
  uniq_to_rel = None


  @property
  def uniq_to(self):
    return list(getattr(self._owner, self.uniq_to_rel)())[0]


  def __call__(self, value=_missing, **kwargs):
    if value is not _missing and self.uniq_to_rel:
      value = guid_prefix(self.uniq_to, value)
    return super(Alias, self).__call__(value=value, **kwargs)


  @property
  def value(self):
    val = LookupDict.value.fget(self)
    if val and self.uniq_to_rel:
      return val.replace('%s:' % self.uniq_to.guid, '')
    return val

  @value.setter
  def value(self, value):
    if self.uniq_to_rel:
      if not value.startswith('%s:' % self.uniq_to.guid):
        value = guid_prefix(self.uniq_to, value)
    self._dh['value'] = value
    LookupDict.value.fset(self, value)


  class List(List):
    def _add(self, *args, **kwargs):
      if self.of_type.uniq_to_rel:
        # TODO wat is this
        args[3] = self._uniq_to_parent_val(args[3])
      return alias.set(*args, **kwargs)


  def save(self, **kw):
    # Q: does this need some dirty-checking logic?
    if self._fetched_value:
      alias.remove(
        db.pool, self.base_id, self._ctx, self._fetched_value, **dhkw(kw))
    alias.set(db.pool, self.base_id, self._ctx, self._dh['value'], **dhkw(kw))


  # TODO deal with scope_to_parent
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
    dh = prop.get(db.pool, self.base_id, self._ctx, **dhkw(kw))
    if dh:
      self._dh = dh


  @property
  def base_id(self):
    return super(Prop, self).base_id or self._owner.guid

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
    # TODO old_value=_missing support for set (like node.update)
    return prop.set(db.pool, self.base_id, self._ctx, self.value)


class Lock(Prop):
  schema = int 
  
  def acquire(self, timeout=10., retry_after=1.):
    while not self.increment(limit=1):
      greenhouse.scheduler.pause_for(retry_after)
      timeout -= retry_after
      if timeout <= 0:
        raise exc.LockAcquisitionTimeout()

  def release(self):
    self(0)

  def __enter__(self):
    self.acquire()

  def __exit__(self, exc_type, exc_val, tb):
    if exc_val:
      raise exc_val
    self.release()


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
