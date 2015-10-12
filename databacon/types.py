import inspect
import types
import math
import functools
import weakref

from datahog import (
  node, 
  alias, 
  name, 
  prop, 
  relationship, 
  table, 
  context, 
  storage,
  const,
)

import exceptions as exc
import db # TODO this is weird..
from util import (
  dhkw, 
  guid_prefix,
  _missing, 
  table_to_const, 
  type_to_storage
)



class RowMC(type):
  user_types = {}

  def __new__(mcls, name, bases, attrs):
    attrs.setdefault('_meta', {})
    cls = super(RowMC, mcls).__new__(mcls, name, bases, attrs)
    RowMC.user_types[name] = cls
    return cls


  ctx_counter = 0
  @staticmethod
  def next_ctx_int():
    ctx_counter += 1
    return ctx_counter


  # consider calling this method waaay later, like just before schema definition,
  # to standardize the ctx definition process and remove a lot of the deferred
  # special casing
  @classmethod
  def define_row_ctx(mcls, cls):
    dh.context.set_context(mcls.next_ctx_int(),
                           table_to_const[cls._table], cls._meta)
    setattr(cls, '_ctx', RowMC.ctx_counter)
    for attr, val in cls.__dict__:
      if type(val) is Bits:
        cls._bits = val
        cls._bits.freeze(cls._ctx)



rels_pending_cls = {}
lists_pending_cls = {}

class RelationMC(RowMC):
  @classmethod
  def define_row_ctx(mcls, cls):
    ''' Runs once per accessor defined for a relationship. '''
    if cls.base_cls and cls.rel_cls:
      cls._meta['base_ctx'] = cls.base_cls._ctx
      cls._meta['rel_ctx'] = cls.rel_cls._ctx
      super(RelationMC, mcls).define_row_ctx(cls)



class ValueRowMC(RowMC):
  @staticmethod
  def _storage_from_schema(schema):
    ''' Map python types to datahog storage constants. '''
    try:
      return ValueRowMC.to_storage[schema]
    except (KeyError, TypeError):
      return dh.storage.SERIAL


  @classmethod
  def define_row_ctx(mcls, cls):
    cls._meta['storage'] = ValueRowMC._storage_from_schema(cls.schema)
    if cls.schema and cls._meta['storage'] == dh.storage.SERIAL:
      cls._meta['schema'] = cls.schema
    super(ValueRowMC, mcls).define_row_ctx(cls)



class NodeMC(ValueRowMC):
  def __new__(mcls, name, bases, attrs):
    args = [name, bases, attrs]

    # TODO move to attr processing, use shard_affinity flag
    parent = attrs.get('parent')
    if parent and parent.__metaclass__ != NodeMC:
      raise exc.InvalidParentType(*args)

    cls = super(GuidMC, mcls).__new__(mcls, name, bases, attrs)

    mcls.define_row_ctx(cls)
    mcls.resolve_pending_cls(cls)
    mcls.finalize_attr_classes(cls, attrs)

    if hasattr(cls, 'flags'):
      cls.flags.freeze(cls._ctx)

    return cls


  def resolve_pending_cls(cls):
    ''' Some relationships and lists are awaiting the creation of a 
    rel_cls/of_type class. '''

    cls_name = cls.__name__
    if cls_name in rels_pending_cls:
      for rel in rels_pending_cls[cls_name]:
        rel.rel_cls = cls
        rel.define_row_ctx(rel)
      del rels_pending_cls[cls_name]

    if cls_name in lists_pending_cls:
      for list_cls in lists_pending_cls[cls_name]:
        list_cls.of_type = cls
      del lists_pending_cls[cls_name]
  

  def finalize_attr_classes(cls, attrs):
    ''' For each attr class that is a types.* subclass:
    - assign a more meaningful class name 
    - assign a base_ctx + define a datahog context
    
    TODO
    - enforce only one shard_affinity field
    - enforce only one of each ValueRow and Bits/Int/Bool/Enum for store_on_row

    '''
    
    for attr, ctx_cls in attrs.iteritems():
      
      if not issubclass(ctx_cls, (BaseIdRow, List)):
        continue

      # Special case for List subclasses...
      if hasattr(ctx_cls, 'of_type'):
        list_cls, ctx_cls = ctx_cls, ctx_cls.of_type

        if ctx_cls:
          # ...that already have a concrete class on `of_type`, e.g.:
          #   `children = db.children(ChildCls)`
          #   `emails = db.alias(plural=True)`
          #   `names = db.name(plural=True)`
          #   `thing1 = db.relationship('Cls')`
          #   `thing2 = db.relationship(Cls)`
          #
          # ...and avoiding those that don't, e.g.:
          #   `children = db.children('ChildCls')`
          #
          # ...so that we can give the List subclass a more meaningful name

          list_cls.__name__ = '%s%s%s' % (attr.capitalize(), 
                                          ctx_cls.__bases__[0].__name__,
                                          'List')
          list_cls.__module__ = cls.__module__
        else:
          print list_cls
          lists_pending_cls\
            .setdefault(list_cls._pending_cls_name, [])\
            .append(list_cls)
          ctx_cls = list_cls # sorry

      # Replace the field's temporary class name with something meaningful.
      # E.g.:
      #   'Relation-0' -> 'DocTermsRelation'
      #   'Alias-0' -> 'UserUsernameAlias'
      if 'rename' in ctx_cls.__name__:
        ctx_cls.__name__ = '%s%s' % (attr.capitalize(),
                                     ctx_cls.__name__.split('-')[0])
        ctx_cls.__module__ = '%s.%s' % (cls.__module__, cls.__name__)


      if issubclass(ctx_cls, BaseIdRow):

        # subclasses of relation might already have a context
        if type(ctx_cls._ctx) is int:
          continue

        ctx_cls.base_cls = cls
        ctx_cls._meta['base_ctx'] = cls._ctx

        ctx_cls.__metaclass__.define_row_ctx(ctx_cls)

        # setup class methods for looking up 
        # instances by name and alias
        if has_ancestor_named(ctx_cls, 'LookupRow'):
          setattr(cls, 'by_%s' % attr, ctx_cls.lookup)


  @classmethod
  def define_row_ctx(mcls, cls):
    # TODO shift to _parent set by shard_affinity flagged attr
    if cls.parent is not None:
      cls._meta['base_ctx'] = cls.parent._ctx
    return super(NodeMC, mcls).define_row_ctx(cls)



class ListMC(type):
  def __getattr__(cls, name):
    if name == 'flags':
      return cls.of_type.flags
    raise AttributeError



class Field(object):
  def __init__(self, *args, **kwargs):
    self.args = args
    self.kwargs = kwargs
    if not kwargs.get('store_on_row'):
      self.bits = Bits()
    self.cls = cls


class Object(object):
  # TODO what about the Object(Prop) class?

  @classmethod
  def Field(cls, *args, **kwargs):
    ''' TODO
    - String(lookup=String.index.unique)
    - String(lookup=String.index.unique_with_parent_id)
    - String(lookup=String.index.prefix)
    - String(lookup=String.index.phonetic)
    - String(unicode=False)
    - Prop(store_on_row=True)
    - Node(relation=...)
    '''
    return Field(cls, *args, **kwargs)



class FutureType(Object):
  ''' A placeholder reference for an as-yet undefined class. '''
  def __init__(self, cls_name):
    self.cls_name = cls_name



class Row(Object):
  ''' Base for all classes that manage datahog rows, which live at `self._row`.

  All datahog objects share a flags attr, a remove method, a context value,
  (a type identifier defined by databacon), and a table (e.g., node).

  Datahog's `node`, `alias`, `name`, `relation` and `prop` objects have other 
  shared attrs and methods, which are handled in subclases of Row.
  '''

  _table = None
  _ctx = None
  _remove_arg_strs = None 
  _id_arg_strs = None
  __row = None

  

  # TODO remove flags kwarg if that use case is really dead
  def __init__(self, flags=None, _row=None):
    self._row = _row or {}
    # TODO instantiate BitsProxy
    self.flags = flags or Flags(self)


  @property
  def _id_args(self):
    return (self._row[key] for key in self._id_arg_strs,)


  @property
  def _remove_args(self):
    return (self._row[key] for key in self._remove_arg_strs,)


  def remove(self, **kw):
    args = self._ids_args + (self._ctx,) + self._remove_args
    return self._table.remove(db.pool, *args, **dhkw(kw))

  
  @classmethod
  def _cls_by_name(cls, cls_name):
    return cls.__metaclass__.user_types[cls_name]

  
  @property
  def _row(self):
    return self.__row

  @_row.setter(self, _row):
    self.__row = _row or {}
    self._row_flags = _row.get('flags'], set())

  @property
  def _row_flags(self):
    return self._row['flags']


  @_row_flags.setter
  def _set_row_flags(self, flags_set):
    self._row['flags'] = flags_set
    self._fetched_flags = flags_set.copy()


  def save(self, **kw):
    add, clear = [], []
    for flag in self._dirty_flags:
      if flag in self._flags_set:
        add.append(flag)
      else:
        clear.append(flag)
    if not add and not clear:
      return

    args = [db.pool] + self._id_args + [self._ctx, add, clear]
    self._row_flags = self._table.set_flags(*args, **dhkw(kw))
    

  def json(self):
    ''' Not the best name -- actually returns a jsonable dict (flags are a set,
    which doesn't serialize to json).'''
    json = self._row.copy()
    json['flags'] = list(json['flags'])
    del json['ctx']
    return json



class List(object):
  __metaclass__ = ListMC
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
    return self.of_type(_row=result, owner=self._owner, **kw)


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



class ValueRow(Row):
  ''' adds value access for datahog objects that have values:
  nodes, props, names, and aliases '''
  __metaclass__ = ValueRowMC
  schema = None # stored_on_node will modify schema behavior/needs
  _fetched_value = None

  def default_value(self):
    return ({
      int: 0,
      str: '',
      unicode: u'',
      type(None): None,
    }).get(type(self.schema) is type and self.schema or type(self.schema), None)
    

  @property
  def value(self):
    return self._row.get('value')


  @value.setter
  def value(self, value):
    self._row['value'] = value

  @Row._row.setter
  def _set_row(self, _row):
    self._fetched_value = _row['value']
    Row._row.fset(self, _row)

  def __call__(self, value=_missing, flags=None, force=False, **kwargs):
    if value is _missing:
      self._row = self._get(**kwargs)
      return self

    self.value = value
    self.save(force=force)
    
    # TODO probably remove this, related to flags in Row(Object)
    if flags:
      self.flags = flags
      self._row['flags'] = flags._tmp_set
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



class PosRow(Row):
  def shift(self, *args, **kw):
    args = [db.pool] + self._id_args + [self._ctx] + list(args)
    return self._table.shift(*args, **dhkw(kw))
    


class BaseIdRow(PosRow):
  _id_arg_strs = ('base_id',)
  base_cls = None
  _owner = None

  def __init__(self, owner=None, _row=None):
    self._owner = owner
    _row = _row or {'base_id': owner.guid}
    super(BaseIdRow, self).__init__(_row=_row)


  @property
  def base_id(self):
    return self._owner._row['guid']



class Relation(BaseIdRow):
  __metaclass__ = RelationMC
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None
  forward = True
  rel_cls = None


  @property
  def rel_id(self):
    return self._row['rel_id']


  @property
  def base_id(self):
    return self._row['base_id']


  def shift(self, index, **kw):
    super(Relation, self).shift(self.forward, index, **kw)


  def node(self, **kw):
    if self.forward:
      guid, cls = self.rel_id, self.rel_cls
    else:
      guid, cls = self.base_id, self.base_cls
    return cls(_row=cls._table.get(
      db.pool, 
      guid,
      cls._ctx, **dhkw(kw)))


  class List(List):
    def _wrap_result(self, result, nodes=False, **kw):
      if nodes:
        return (self.of_type(_row=result[0], owner=self._owner), 
                self.of_type.base_cls(_row=result[1], owner=self._owner))
      return self.of_type(_row=result, owner=self._owner)


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



class LookupRow(BaseIdRow, ValueRow):
  _remove_arg_strs = ('value',)

  # TODO use List._get_page 
  def _get(self, **kw):
    entries = self._table.list(db.pool, self.base_id, self._ctx, **dhkw(kw))[0]
    if not entries:
      return {'base_id': self.base_id}
    self._fetched_value = entries[0]['value'] # for remove during save
    return entries[0]



class Node(ValueRow, PosRow): 
  __metaclass__ = NodeMC
  _table = node
  _save = node.update
  parent = None 
  _id_arg_strs = ('guid',)
  _remove_arg_strs = ('guid',)
  

  def __init__(self, value=_missing, **kw):
    if not row:
      if value is _missing:
        value = self.default_value()
      row = node.create(db.pool, 
                       parent.guid,
                       self._ctx,
                       value,
                       **dhkw(kw))
    super(Node, self).__init__(**kw)
    self._instantiate_attr_classes()


  def _instantiate_attr_classes(self):
    attrs = [a for a in dir(self) if '__' not in a]
    for attr, val in [(a, getattr(self, a)) for a in attrs]:
      if isinstance(val, type) and issubclass(val, (Row, List)):
        self.__dict__[attr] = val(owner=self)


  @property
  def guid(self):
    return self._row['guid']


  @classmethod
  def _by_guid(cls, ids, props=None, **kw):
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
      return [cls(_row=row) for row in dicts]
    else:
      return cls(_row=dicts[0])



  def move(self, new_parent, **kw):
    moved =  node.move(
      db.pool, self.guid, self._ctx, self.parent.guid, new_parent.guid, **dhkw(kw))
    if moved:
      self.parent = new_parent
    return moved


  # TODO what does this mean in a world where the node value is not a 
  # featured feature?
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
    super(Node, self).save(*kw)
    return self


  class List(List):
    def _wrap_result(self, *args, **kw):
      return super(Node.List, self)._wrap_result(*args, parent=self._owner, **kw)


    def _get_page(self, *args, **kw):
      return node.get_children(*args, **dhkw(kw))


    @property
    def flags(self):
      raise AttributeError



class Prop(ValueRow, BaseIdRow):
  _table = prop

  def __init__(self, **kw):
    self.ignore_remove_race = False
    super(Prop, self).__init__(**kw)


  def _get(self, **kw):
    # TODO store_on_node changes
    return prop.get(db.pool, self.base_id, self._ctx, **dhkw(kw))


  @property
  def _remove_args(self):
    if self.ignore_remove_race:
      return ()
    return (self.value,)


  def remove(self, ignore_remove_race=None, **kw):
    # TODO disable for store_on_node
    # TODO this is weird. maybe just pass ignore_remove_race to .remove()
    was = self.ignore_remove_race
    if ignore_remove_race is not None:
      self.ignore_remove_race = ignore_remove_race
    super(Prop, self).remove(**kw)
    self.ignore_remove_race = was


  def save(self, **kw):
    # TODO store_on_node changes
    ret = prop.set(db.pool, self.base_id, self._ctx, self.value, dhkw(kw))
    super(Prop, self).save(**kw)
    return ret



class String(Prop):
  schema = unicode 

  class index(object):
    unique = 'unique'
    unique_to_parent = 'unique_to_parent'
    prefix = 'prefix'
    phonetic = 'phonetic'
  

  @classmethod
  def cls_for_field(cls, field):
    ''' Takes an instance of Field and returns a subclass of `cls`. '''
    lookup = field.kwargs.get('lookup')
    if lookup in (String.index.unique, String.index.unique_to_parent):
      field.cls = UniqueString
    elif lookup in (String.index.prefix, String.index.phonetic):
      field.cls = SearchString

    cls = ('TODO', {}, field.cls)
    if lookup == Search.index.unique_to_parent:
      cls.unique_to_parent = True

    if field.kwargs.get('unicode') === False:
      cls.schema = str
  return cls


class UniqueString(String, LookupRow):
  _table = alias
  unique_to_parent = None


  def __call__(self, value=_missing, **kwargs):
    if value is not _missing and self.uniq_to_parent:
      value = guid_prefix(self._owner.parent, value)
    return super(Alias, self).__call__(value=value, **kwargs)


  @property
  def value(self):
    val = LookupRow.value.fget(self)
    if val and self.uniq_to_parent:
      return val.replace('%s:' % self._owner.parent.guid, '')
    return val


  @value.setter
  def value(self, value):
    if self.uniq_to_parent:
      if not value.startswith('%s:' % self._owner.parent.guid):
        value = guid_prefix(self._owner.parent, value)
    self._row['value'] = value
    LookupRow.value.fset(self, value)


  def save(self, **kw):
    # Q: does this need some dirty-checking logic?
    # TODO can this be moved into ValueRow?
    # - should be unified with List._add
    if self._fetched_value:
      alias.remove(
        db.pool, self.base_id, self._ctx, self._fetched_value, **dhkw(kw))
    alias.set(db.pool, self.base_id, self._ctx, self._row['value'], **dhkw(kw))
    super(Alias, self).save(**kw)


  @classmethod
  def get(cls, value, scope_to_parent=None, **kw):
    if scope_to_parent:
      value = guid_prefix(scope_to_parent, value)
    row_alias = alias.lookup(db.pool, value, cls._ctx, **dhkw(kw))
    if row_alias:
      # TODO pass in the alias to hydrate it on the Node instance
      return cls.base_cls.by_guid(row_alias['base_id'])

  @classmethod
  def batch(cls):
    ''' TODO w/ Node.ids.batch'''
    pass


  class List(List):
    def _add(self, *args, **kwargs):
      if self.of_type.uniq_to_parent:
        args[3] = self._uniq_to_parent_val(args[3])
      return alias.set(*args, **kwargs)



class SearchString(String, LookupRow):
  _table = name

  @classmethod
  def search(cls, value, **kw):
    row_names, offset = name.search(db.pool, value, cls._ctx, **dhkw(kw))
    if len(row_names):
      # TODO pass row_names to by_guid to hydrate the Name attr
      # (see same in IndexString)
      return cls.base_cls.by_guid([n['base_id'] for n in row_names])


  def save(self, **kw):
    # it might happen that the user saves before fetching, 
    # in which case we need to remove the existing entry
    # BUT only if this is a singular name field
    if not self._row['value']:
      existing_name = name.list(db.pool, self.base_id, self._ctx, limit=1)
      if existing_name and existing_name[0]['value']:
        name.remove(db.pool, self.base_id, self._ctx, existing_name['value'])
    name.create(db.pool, self.base_id, self._ctx, self.value, **dhkw(kw))
    super(Name, self).save(**kw)


  class List(List):
    add = name.create




class BitsProxy(object):
  ''' Lives on each Object instance, manages modification of the instance's 
  flags set via the instance class's Bits() map.'''
  def __init__(self, owner=None, fields=None):
    self._owner = weakref.ref(owner) 


  def __setattr__(self, name, value):
    if name.startswith('_'):
      return super(BitsProxy, self).__setattr__(name, value)

    bits = self._owner._bits
    if name in bits.ranges:
      return bits.set_range(self, name, value)

    return super(BitsProxy, self).__setattr__(name, value)


  def __getattr__(self, name):
    return self._owner()._bits.get_range_value(name, self._owner()._dh_flags)


  def __call__(self, **kw):
    if not values:
      raise TypeError("BitsProxy()() expects keyword arguments corresponding to BitRanges in the Bits() object.")
    for key, value in kw:
      self._set_range(key, value)
    self._owner.save()



class BitRange(object):
  ''' A BitRange instance lives inside a BitField instance lives on an Object
  subclass (not instance).'''

  def __init__(self, max_val, default=0):
    self.default = default
    self.max_val = max_val
    self.size = int(math.ceil(math.log(self.max_val + 1, 2)))


  def value(self, int):
    return int


  def int(self, value):
    if value is not int or value > self.max_val:
      raise ValueError("%s is to large a value for %s" % (value, self))
    return value



class Int(BitRange):
  def __init__(self, max_val=None, bits=None, default=0):
    max_val = max_val and max_val or (math.pow(2, bits) - 1)
    super(Int, self).__init__(max_val, default)
    


class Enum(BitRange):
  def __init__(self, *enum_strs):
    types = set(type(s) for s in enum_strs)
    if len(types) != 1 or types.pop() != str:
        raise TypeError('Enum expects a list of strings.')

    self.enum_strs = enum_strs
    for enum_str in self.enum_strs:
      setattr(self, enum_str, enum_str)

    super(Enum, self).__init__(max_val=len(self.enum_strs))
  

  def value(self, int):
    return self.enum_strs[int]


  def int(self, value):
    return self.enum_strs.index(value)



class Bool(BitRange):
  def __init__(self, default=False):
    if type(default) is not bool:
      raise TypeError("bool expects a boolean default value.")
    super(Bool, self).__init__(1, default)


  def value(self, int):
    return bool(int)


  def int(self, value):
    if not isinstance(value, bool):
      raise ValueError("%s expects bool, got %s" % (self, value))
    return int(value)



class Bits(object):
  ''' Each Node, Alias, Property, and Relationship has a 16-bit integer field
  that can be packed with a collection of short ints, enums, and booleans.

  Its intent is to make the storage of light metadata less network intensive. 

  class User(Node):
    bits = Bits(store_on_node=True) 
    bits.role = Enum('user', 'admin', 'staff')

    email = Alias()
    email.bits.verified = Bool(False)

    password = String()
    password.bits.recent_failed_logins = Int(max_val=7)

  user = User()

  # calling with value(s) will save the values to the database
  user.bits(role=User.bits.role.admin)

  # as will calling save() on the object that owns the Bits() field
  user.email().bits.verified = True
  user.save()
  '''
  max_bits = 16


  def __init__(self):
    self.next_free_bit = 0
    self.frozen = False
    self.ranges = {} # {'rangeName': (bitRange, flagSet} where flagSet
                     # is a set bit positions occupied by the range


  def __setattr__(self, name, val):
    super(Bits, self).__setattr__(name, val)
    if isinstance(val, BitRange):
      self.define_range(name, val)


  def get_range_value(self, name, flags):
    brange = self.bitRanges[name]
    int = reduce(lambda x, y: x | (1 << y), [b for b in brange.range if b in flags], 0)
    return brange.value(int)

  
  def set_range_value(self, name, value, flags):
    flagSet, brange = self.ranges[name]
    int = brange.int(value) << flagSet[0]
    for flag in flagSet:
      if flag & int:
        flags.add(flag)
      else:
        flags.remove(flag)
    

  def define_range(self, name, brange):
    if self.frozen:
      raise Exception("Can't add ranges after class definition.")
    if self.next_free_bit + brange.size > self.max_bits:
      raise exceptions.FlagFieldDefOverflow
    self.ranges[name] = (brange, set(range(self.next_free_bit, brange.size)))
    

  def freeze(self, ctx):
    self.frozen = True
    for name, brange in self.ranges.iteritems():
      flagSet = brange[0]
      for flag in flagSet:
        const.flag.set_flag(flag + 1, ctx)




class Bool(Prop):
  schema = bool



class Int(Prop):
  schema = int



class Bits(Int):
  pass



class Float(Prop):
  schema = float



class Enum(Prop):
  schema = []



