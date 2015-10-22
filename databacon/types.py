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
from util import (
  dhkw, 
  guid_prefix,
  _missing, 
  table_to_const, 
  mummy_const,
  pool,
  attrs,
)


__all__ = ['Node', 'Enum', 'String', 'Bool', 'Float', 'Int', 'Prop', 'Bits']



class TypeMC(type):
  awaiting = {}
  def await_class_by_name(cls, name, attr):
    awaiting.setdefault(name, []).append((cls, attr))



class RowMC(TypeMC):
  def __new__(mcls, name, bases, attrs):
    cls = super(RowMC, mcls).__new__(mcls, name, bases, attrs)
    if name in (k for k in globals().keys() if 91 > ord(k[0]) > 65):
      print name
      cls.setup_user_cls()
    return cls


  def setup_user_cls(cls):
    if not hasattr(cls, '_meta'):
      setattr(cls, '_meta', {})
    cls.define_row_ctx()
    

  ctx_counter = 0
  @staticmethod
  def next_ctx_int():
    RowMC.ctx_counter += 1
    return RowMC.ctx_counter


  def define_row_ctx(cls):
    if cls._ctx:
      return
    context.set_context(cls.next_ctx_int(),
                           table_to_const[cls._table], cls._meta)
    setattr(cls, '_ctx', RowMC.ctx_counter)

    cls.freeze_flags()
    cls.resolve_future_type()


  def freeze_flags(cls):
    for attr, val in cls.__dict__:
      if type(val) is Bits.Field:
        cls._bits = val
        cls._bits.freeze(cls._ctx)

    
  def resolve_future_type(cls):
    for waiting, attr in self.awaiting.get(cls.__name__,[]):
      setattr(waiting, attr, cls)



class BaseIdRowMC(RowMC):
  _base_cls = None

  def define_row_ctx(cls):
    if 'base_id' in cls._meta:
      super(BaseIdRowMC, cls).define_row_ctx()


  @property
  def base_cls(cls):
    return cls._base_cls


  @base_cls.setter
  def set_base_cls(cls, base_cls):
    cls._base_cls = base_cls
    cls._meta['base_ctx'] = base_cls._ctx
    cls.define_row_ctx()



class RelIdRowMC(BaseIdRowMC):
  _rel_cls = None

  @property 
  def rel_cls(cls):
    return cls._rel_cls


  @rel_cls.setter
  def set_rel_cls(cls, rel_cls):
    cls._rel_cls = rel_cls
    cls._meta['rel_ctx'] = cls.rel_cls._ctx
    cls.define_row_ctx()


  def define_row_ctx(cls):
    if 'rel_cls' in cls._meta:
      super(RelIdRowMC, cls).define_row_ctx()



class NodeRowMC(RowMC):
  def setup_user_cls(cls):
    cls.find_parent_cls()
    cls.subclass_fields()


  def find_parent_cls(cls):
    for attr, field in cls.__dict__.iteritems():
      if field.shard_affinity:
        if cls._parent:
          raise TypeError("shard_affinity already set to be %s." % cls._parent)
        if isinstance(field.cls, FutureType):
          raise TypeError("shard_affinity and FutureType do not mix.")
        attrs['parent_cls'] = field.cls


  def subclass_fields(cls):
    ''' All fields representing datahog rows need their own datahog context. All
    subclasses of types.Row get their own context upon class creation, so here
    we'll subclass the field types to setup those contexts, and we'll
    create subclasses of RelIdRow where appropriate. 
    '''
    for attr, field in cls.__dict__.iteritems():
      field_cls, list_cls = None, None
      
      if type(field) is list:
        field = field[0]
        list_cls = field.cls.List

      if not isinstance(field, Field):
        print 'Skipping ', field
        continue

      if field.shard_affinity and list_cls:
        raise TypeError("shard_affinity can only be used on a 1:X relationship.")

      field_cls = field.cls

      # PropRows
      if issubclass(field_cls, PropRow):
        field_cls = type('%s%s' % (attr, field_cls.__name__), {'base_cls': cls}, (field_cls,))

      # fields of NodeRows require RelIdRows
      elif issubclass(field_cls, (NodeRow, FutureType)):
        rel_name = '%s%sRel' % (cls.__name__, field.cls.__name__)
        if field.relation:
          if issubclass(field.cls, FutureType):
            raise TypeError("relation and FutureType do not mix.")
          field_cls = type(rel_name, {'foward': False}, (field.relation,))
          field_cls.rel_cls = cls
        else:
          field_cls = type(rel_name, {}, (RelIdRow,))
          field_cls.base_cls = cls
          if issubclass(field.cls, FutureType):
            field_cls.await_class_by_name(field.cls.future_name, 'rel_cls')
          else:
            field_cls.rel_cls = field.cls
            
      if list_cls:
        list_cls = type('%sList' % field_cls.__name__,
                        {'of_type': field_cls}, field_cls.List)
        setattr(cls, attr, list_cls)
      else:
        setattr(cls, attr, field_cls)



class ListMC(type):
#  def __new__(mcls, name, attrs, bases):
    # register pending class if of_type is a FutureType
#    pass


  # TODO expose relation flags via List class method
  # bits = User.docs.meta(bool='val', int=1, ...)
  # user.docs.add(doc, bits=bits)
  def __getattr__(cls, name):
    if name == 'flags':
      return cls.of_type.flags
    raise AttributeError



class Field(object):
  def __init__(self, cls, *args, **kwargs):
    self.args = args
    self.kwargs = kwargs
    self.bits = Bits()
    self.cls = cls



class Type(object):

  @classmethod
  def field(cls, *args, **kwargs):
    print cls
    ''' TODO
    - Prop(store_on_row=True)
    - NodeRow(relation=...)
    '''
    return cls.Field(cls, *args, **kwargs)



class FutureType(Type):
  ''' A placeholder reference for an as-yet undefined class. '''
  pass



def Future(name):
    return type(name, {'future_name': name}, (FutureType,))



class Row(Type):
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

  

  def __init__(self, _row=None, **kwargs):
    self._row = _row or {}
    for key, value in kwargs.iteritems():
      if isinstance(getattr(type(self), key), Bits.Field):
        self._row_flags = value
      else:
        setattr(self, key, value) # TODO this requires descriptors 


  @property
  def _id_args(self):
    return (self._row[key] for key in self._id_arg_strs)


  @property
  def _remove_args(self):
    return (self._row[key] for key in self._remove_arg_strs)


  def remove(self, **kw):
    args = self._ids_args + (self._ctx,) + self._remove_args
    return self._table.remove(db.pool, *args, **dhkw(kw))

  
  @property
  def _row(self):
    return self.__row

  @_row.setter
  def set_row(self, _row):
    self.__row = _row or {}
    self._row_flags = _row.get('flags', set())

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
  schema = None # stored_on_node will modify schema behavior/needs
  _fetched_value = None
  _default_value = _missing

  @classmethod
  def define_row_ctx(cls):
    cls._meta['storage'] = mummy_const(cls.schema)
    if cls.schema and cls._meta['storage'] == dh.storage.SERIAL:
      cls._meta['schema'] = cls.schema
    super(ValueRow, cls).define_row_ctx()



  def default_value(self):
    if not self._default_value == _missing:
      return self._default_value
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



class OrderedRow(Row):
  def shift(self, *args, **kw):
    args = [db.pool] + self._id_args + [self._ctx] + list(args)
    return self._table.shift(*args, **dhkw(kw))
    


class BaseIdRow(Row):
  __metaclass__ = BaseIdRowMC
  _id_arg_strs = ('base_id',)
  base_cls = None
  _owner = None


  def __init__(self, owner=None, _row=None):
    self._owner = owner
    _row = _row or {'base_id': owner.guid}
    super(BaseIdRow, self).__init__(_row=_row)


  @property
  def base_id(self):
    return self._row['base_id']



class RelIdRow(BaseIdRow, OrderedRow):
  __metaclass__ = RelIdRowMC
  _id_arg_strs = ('base_id', 'rel_id')
  _table = relationship
  _rel_cls_str = None
  forward = True
  rel_cls = None

  # TODO
  # - _edge property for singular relations
  # - descriptor


  @property
  def rel_id(self):
    return self._row['rel_id']


  def shift(self, index, **kw):
    super(RelIdRow, self).shift(self.forward, index, **kw)


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
      for page, offset in super(RelIdRow.List, self)._pages(**kw):
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
      return super(RelIdRow.List, self)._get_page(*args, **kw)


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



class LookupRow(BaseIdRow, ValueRow, OrderedRow):
  _remove_arg_strs = ('value',)


  # TODO use List._get_page 
  def _get(self, **kw):
    entries = self._table.list(db.pool, self.base_id, self._ctx, **dhkw(kw))[0]
    if not entries:
      return {'base_id': self.base_id}
    self._fetched_value = entries[0]['value'] # for remove during save
    return entries[0]



class NodeRow(ValueRow, OrderedRow): 
  __metaclass__ = NodeRowMC
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
    super(NodeRow, self).__init__(**kw)
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
    super(NodeRow, self).save(*kw)
    return self


  class List(List):
    # TODO merge this with Relation.List
    def _wrap_result(self, *args, **kw):
      return super(NodeRow.List, self)._wrap_result(*args, parent=self._owner, **kw)


    def _get_page(self, *args, **kw):
      return node.get_children(*args, **dhkw(kw))


    @property
    def flags(self):
      raise AttributeError



class PropRow(BaseIdRow, ValueRow):
  _table = prop


  def __init__(self, **kw):
    self.ignore_remove_race = False
    super(PropRow, self).__init__(**kw)


  def shift(self):
    raise AttributeError("shift not implemented for PropRows")


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
    super(PropRow, self).remove(**kw)
    self.ignore_remove_race = was


  def save(self, **kw):
    # TODO store_on_node changes
    ret = prop.set(db.pool, self.base_id, self._ctx, self.value, dhkw(kw))
    super(PropRow, self).save(**kw)
    return ret



class String(PropRow):
  schema = unicode 


  class Field(Field):
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

    if field.kwargs.get('unicode') == False:
      cls.schema = str
    return cls



class UniqueString(String, LookupRow):
  _table = alias
  unique_to_parent = False


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
    ''' TODO w/ NodeRow.ids.batch'''
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




class Bits(Type):
  ''' Bits is special case type exposing the smallint (2 bytes) flags column of 
  every underlying datahog row as a collection of Int, Enum, and Bool fields. Its
  intent is to make the storage of light metadata less network intensive.

  # Defining a Bits interface:

  class User(NodeRow):
    meta = Bits.Field() # NodeRow subclasses must create the field explicitly
    meta.role = Enum.Field('USER', 'ADMIN', 'STAFF')

    email = String(index=String.index.unique)

    # subclasses of PropRow and RelIdRow have a Bits.Field() instance
    # assigned to .bits by default
    email.bits.verification_status = Enum.Field('UNSENT', 'SENT', 'CONFIRMED')

    # You can also move the Bits.Field() to another attribute,
    # and re-assign another type to .bits.
    email.meta = Bits.Field()
    email.meta.primary = Bool.Field()
    email.bits = [Some.Field()]

    # Now that you've seen Bool and Enum, let's look at the Int field:
    password = String()
    password.bits.max_val_int = Int.Field(max_val=7)
    password.bits.fix_bit_int = Int.Field(bits=3)

  # Now, let's look at consumption:

  user = User()

  # calling the bits field with keyword argument(s) will update the flags column
  # in the user's database row
  user.bits(role=User.bits.role.admin)

  # as will calling save() on the object that owns the Bits() field
  user.bits.role = User.bits.role.user
  user.save()

  # On that note, here's one example that might be surprising until you wrap 
  # your head around the underlying storage model
  user.email.bits.verification_status = 'sent'
  user.email.save() # saves the changes made to the bits field, 
                    # and to user.email.value
  '''
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



  class Field(object):
    max_bits = 16

    def __init__(self, *args):
      self.next_free_bit = 0
      self.frozen = False
      self.ranges = {} # {'rangeName': (bitRange, flagSet} where flagSet
                       # is a set of bit positions occupied by the range


    def __setattr__(self, name, val):
      super(Bits.Field, self).__setattr__(name, val)
      if isinstance(val, Bits.Field.BitRange):
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


    def __call__(self, **kwargs):
      ''' E.g.:
      bits = User.bits(role='admin')
      user = User(bits=bits)'''
      row_flags = set()
      for key, val in kwargs.iteritems():
        self.set_range_value(key, val, row_flag)
      return row_flags


    class BitRange(object):
      ''' Base class for the special-case field types used to defines
      a Bits.Field() interface. '''
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
      def __init__(self, max=None, bits=None, default=0):
        max = max and max or (math.pow(2, bits) - 1)
        super(Bits.Field.Int, self).__init__(max, default)
    


    class Enum(BitRange):
      def __init__(self, *enum_strs):
        types = set(type(s) for s in enum_strs)
        if len(types) != 1 or types.pop() != str:
          raise TypeError('Enum expects a list of strings.')

        self.enum_strs = enum_strs
        for enum_str in self.enum_strs:
          setattr(self, enum_str, enum_str)

        super(Bits.Field.Enum, self).__init__(max_val=len(self.enum_strs))
  

      def value(self, int):
        return self.enum_strs[int]


      def int(self, value):
        return self.enum_strs.index(value)



    class Bool(BitRange):
      def __init__(self, default=False):
        if type(default) is not bool:
          raise TypeError("bool expects a boolean default value.")
        super(Bits.Field.Bool, self).__init__(1, default)


      def value(self, int):
        return bool(int)


      def int(self, value):
        if not isinstance(value, bool):
          raise ValueError("%s expects bool, got %s" % (self, value))
        return int(value)



class Node(NodeRow):
  pass



class Prop(PropRow):
  pass



class Float(PropRow):
  schema = float



class Enum(PropRow):
  schema = []



class Bool(PropRow):
  schema = bool



class Object(PropRow):
  schema = dict



class Int(PropRow):
  schema = int
