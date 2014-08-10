import functools

from datahog import node, entity, alias, prop, relationship, name
from datahog.const import storage, table, context
import fields
import datahog_wrappers as dhw
import exceptions as exc


to_const = {
  node: table.NODE,
  entity: table.ENTITY,
  alias: table.ALIAS,
  prop: table.PROPERTY,
  relationship: table.RELATIONSHIP,
  name: table.NAME
}


ctx_counter = 0
def make_ctx(table, meta):
  global ctx_counter
  ctx_counter += 1
  context.set_context(ctx_counter, to_const[table], meta)
  return ctx_counter


to_storage = {
  int: storage.INT,
  str: storage.STR,
  unicode: storage.UTF,
  None: storage.NULL
}


def storage_from_schema(schema):
  try:
    return to_storage[schema]
  except (KeyError, TypeError):
    return storage.SERIAL


to_wrapper = {
  fields.lookup.alias: dhw.DHAlias,
  fields.lookup.prefix: dhw.DHName,
  fields.lookup.phonetic: dhw.DHName,
  fields.prop: dhw.DHProp,
  fields.relation: dhw.DHRelation,
}

to_collection = {
  dhw.DHAlias: dhw.DHAliasCo
}

class DatahogDictMC(type):
  name_to_cls = {} # used by relations, which are defined using string names


  def __new__(mcls, name, bases, attrs):
    if name in ('Node', 'Entity', 'Alias', 'Prop'):
      return super(DatahogGuidDictMC, mcls).__new__(mcls, name, bases, attrs)

    mcls.define_dh_ctx(name, attrs, bases[0]._table)
    mcls.
    cls = super(DatahogDictMC, mcls).__new__(mcls, name, bases, attrs)
    mcls.name_to_cls[cls.__name__] = cls
    return cls


  @classmethod
  def define_dh_ctx(mcls, name, attrs, table, meta=None):
    meta = {}
    if 'parent' in attrs:
      meta['base_ctx'] = attrs['parent']._ctx

    if 'schema' in attrs:
      meta['schema'] = attrs['schema']
      meta['storage'] = storage_from_schema(mtea['schema'])

    if 'seach_mode' in attrs:
      meta['search'] = field.search_mode
      meta['phonetic_loose'] = field.phonetic_loose

    attrs['_ctx'] = make_ctx(table, meta)


class DatahogGuidDictMC(DatahogDictMC):


  def __new__(mcls, name, bases, attrs):
    ''' Does all the magic that makes a user-defined subclass of Node
    or Entity behave like well-defined datahog contexts.'''

    if name in ('Node', 'Entity'):
      return super(DatahogGuidDictMC, mcls).__new__(mcls, name, bases, attrs)

    mcls.make_field_classes(name, attrs)
    return super(DatahogGuidDictMC, mcls).__new__(mcls, name, bases, attrs)


  @staticmethod
  def make_field_classes(cls_name, attrs):
    ''' Transform field.* classes, which are used by the user to define
    field types on their custom classes, into dhw.DH* classes, which encapsulate
    datahog objects and methods. '''
    attrs['_dh_fields'] = {}

    def fields():
      for key, val in attrs.iteritems():
        if isinstance(val, fields.Field):
          yield name, field

    for name, field in fields():
      field_cls_base = to_wrapper[type(field)]
      field_cls_name = cls_name + field_cls_base.__name__
      field_cls = type(field_cls_name, (field_cls_base,), field.__dict__)
      if issubclass(field_cls, (dhw.Alias, dhw.Name, dhw.Relation)):
        collection_cls = to_collection[field_cls]
        # RESUME setting up colleciton classes
      attrs['_dh_fields'][name] = field_cls


class DatahogNodeDictMC(DatahogGuidDictMC):


  def __new__(mcls, name, bases, attrs):
    ''' Enforce that Node subclasses Node have a valid parent property.'''

    args = [name, bases, attrs]

    if name == 'Node':
      return super(DatahogNodeDictMC, mcls).__new__(mcls, *args)

    if not has_ancestor_named(attrs['parent'], ('Node', 'Entity')):
      raise exc.InvalidParentType(*args)

    return super(DatahogNodeDictMC, mcls).__new__(mcls, *args)


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
