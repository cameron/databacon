import types

from datahog import node, entity, alias, prop, relationship, name
from datahog.const import storage, table, context

import flags
import exceptions as exc

# BLARG
do_not_apply_metaclass = [
  'Node', 'Entity', 'Prop', 'Alias', 'AliasRelation', 'NameRelation',
  'Name', 'Relation', 'LookupDict', 'ValueDict', 'PosDict', 'BaseIdDict',
  'GuidDict', 'Dict'
]


def only_for_user_defined_subclasses(mc):
  ''' Ensures that databacon internal classes are not treated like user-defined
  subclasses, which should generate new datahog context constants. '''
  old__new__ = mc.__new__
  def new__new__(mcls, name, bases, attrs):
    if name in do_not_apply_metaclass:
      return super(mc, mcls).__new__(mcls, name, bases, attrs)
    return old__new__(mcls, name, bases, attrs)
  mc.__new__ = staticmethod(new__new__)
  return mc


@only_for_user_defined_subclasses
class DictMC(type):
  cls_by_name = {}

  to_const = {
    node: table.NODE,
    entity: table.ENTITY,
    alias: table.ALIAS,
    prop: table.PROPERTY,
    relationship: table.RELATIONSHIP,
    name: table.NAME
  }


  ctx_counter = 0
  @staticmethod
  def make_ctx(table, meta):
    DictMC.ctx_counter += 1
    context.set_context(DictMC.ctx_counter, DictMC.to_const[table], meta)
    return DictMC.ctx_counter


  def __new__(mcls, name, bases, attrs):
    attrs.setdefault('flags', flags.Flags())
    attrs.setdefault('_meta', {})
    cls = super(DictMC, mcls).__new__(mcls, name, bases, attrs)
    mcls.define_dh_ctx(cls)
    DictMC.cls_by_name[name] = cls
    return cls


  @classmethod
  def define_dh_ctx(mcls, cls):
    setattr(cls, '_ctx', DictMC.make_ctx(cls._table, cls._meta))


@only_for_user_defined_subclasses
class RelationMC(DictMC):
  def __new__(mcls, name, bases, attrs):
    if not isinstance(attrs['_rel_cls_str'], str):
      raise TypeError("relation expects a string class name.")
    return super(RelationMC, mcls).__new__(mcls, name, bases, attrs)


class ValueDictMC(DictMC):

  to_storage = {
    int: storage.INT,
    str: storage.STR,
    unicode: storage.UTF,
    None: storage.NULL
  }


  @staticmethod
  def _storage_from_schema(schema):
    try:
      return ValueDictMC.to_storage[schema]
    except (KeyError, TypeError):
      return storage.SERIAL


  @classmethod
  def define_dh_ctx(mcls, cls):
    cls._meta['storage'] = ValueDictMC._storage_from_schema(cls.schema)
    if cls.schema and cls._meta['storage'] == storage.SERIAL:
      cls._meta['schema'] = cls.schema
    super(ValueDictMC, mcls).define_dh_ctx(cls)


@only_for_user_defined_subclasses
class GuidMC(DictMC):
  def __new__(mcls, name, bases, attrs):
    cls = super(GuidMC, mcls).__new__(mcls, name, bases, attrs)
    if hasattr(cls, 'flags'):
      cls.flags.freeze(cls._ctx)
    for attr, val in attrs.iteritems():
      if hasattr(val, '_ctx') and isinstance(val.flags, flags.Flags):
        val.flags.freeze(val._ctx)
    return cls


@only_for_user_defined_subclasses
class NodeMC(ValueDictMC, GuidMC):

  @classmethod
  def define_dh_ctx(mcls, cls):
    if cls.parent is not None:
      cls._meta['base_ctx'] = cls.parent._ctx
    return super(NodeMC, mcls).define_dh_ctx(cls)


  def __new__(mcls, name, bases, attrs):
    args = [name, bases, attrs]

    if not has_ancestor_named(attrs['parent'], ('Node', 'Entity')):
      raise exc.InvalidParentType(*args)

    return super(NodeMC, mcls).__new__(mcls, *args)


class FlaggedCollectionMC(type):
  # TODO
  def __new__(mcls, name, bases, attrs):
    cls = attrs.get('_dh_cls', None)
    if cls:
      attrs['flags'] = attrs['_dh_cls'].flags
    return super(FlaggedCollectionMC, mcls).__new__(mcls, name, bases, attrs)


def has_ancestor_named(classes, names):
  ''' String named-based impl of issubclass to circumvent circular import woes 
  in NodeDictMC, which wants to refer to the Entity class. '''
  bases = list(isinstance(classes, type) and classes.__bases__ or classes)
  names = type(names) is tuple and names or (names,)
  while bases:
    base = bases.pop(0)
    if base.__name__ in names:
      return True
    bases.extend(list(base.__bases__))
  return False


