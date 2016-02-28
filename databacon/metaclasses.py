# metaclasses.py
#
# Responsible for taking a user-defined subclass of databacon.Entity/Node,
# creating the appropriate datahog context values, and returning
# a class that behaves as specified.

import inspect
import types

import flags
import datahog as dh
import exceptions as exc



def only_for_user_defined_subclasses(mc):
  '''
  Stops metaclasses from creating datahog context data for databacon's intermediate
  classes (Node et al).
  '''

  do_not_apply_metaclass = [
    'Node', 'Entity', 'Prop', 'Alias', 'Name', 'Relation', 'LookupDict',
    'ValueDict', 'PosDict', 'BaseIdDict', 'GuidDict', 'Dict', 'List'
  ]

  old__new__ = mc.__new__
  def new__new__(mcls, name, bases, attrs):
    if name in do_not_apply_metaclass:
      return super(mc, mcls).__new__(mcls, name, bases, attrs)
    return old__new__(mcls, name, bases, attrs)
  mc.__new__ = staticmethod(new__new__)
  return mc


@only_for_user_defined_subclasses
class DictMC(type):
  user_cls_by_name = {}

  to_const = {
    dh.node: dh.table.NODE,
    dh.entity: dh.table.ENTITY,
    dh.alias: dh.table.ALIAS,
    dh.prop: dh.table.PROPERTY,
    dh.relationship: dh.table.RELATIONSHIP,
    dh.name: dh.table.NAME
  }


  def __new__(mcls, name, bases, attrs):
    attrs.setdefault('_meta', {})
    cls = super(DictMC, mcls).__new__(mcls, name, bases, attrs)

    # allow flag inheritance
    if not hasattr(cls, 'flags'):
      setattr(cls, 'flags', flags.Layout())

    DictMC.user_cls_by_name[name] = cls
    return cls


  ctx_counter = 0
  @classmethod
  def define_dh_ctx(mcls, cls):
    DictMC.ctx_counter += 1
    print 'define dh ctx', cls.__name__, DictMC.ctx_counter
    print ' ', cls._meta
    dh.context.set_context(DictMC.ctx_counter,
                           DictMC.to_const[cls._table], cls._meta)
    setattr(cls, '_ctx', DictMC.ctx_counter)
    cls.flags.freeze(cls._ctx)


rels_pending_cls = {}
lists_pending_cls = {}

@only_for_user_defined_subclasses
class RelationMC(DictMC):


  @classmethod
  def define_dh_ctx(mcls, cls):
    ''' Define the datahog context necessary to support a relationship between
    two Nodes. Happens at runtime, once per unique relationship.'''
    if cls.base_cls and cls.rel_cls:
      cls._meta['base_ctx'] = cls.base_cls._ctx
      cls._meta['rel_ctx'] = cls.rel_cls._ctx
      super(RelationMC, mcls).define_dh_ctx(cls)


class ValueDictMC(DictMC):
  to_storage = {
    int: dh.storage.INT,
    str: dh.storage.STR,
    unicode: dh.storage.UTF,
    None: dh.storage.NULL,
  }


  @staticmethod
  def _storage_from_schema(schema):
    ''' Map python types to datahog storage constants. '''
    try:
      return ValueDictMC.to_storage[schema]
    except (KeyError, TypeError):
      return dh.storage.SERIAL


  @classmethod
  def define_dh_ctx(mcls, cls):
    cls._meta['storage'] = ValueDictMC._storage_from_schema(cls.schema)
    if cls.schema and cls._meta['storage'] == dh.storage.SERIAL:
      cls._meta['schema'] = cls.schema
    super(ValueDictMC, mcls).define_dh_ctx(cls)


@only_for_user_defined_subclasses
class GuidMC(DictMC):


  def __new__(mcls, name, bases, attrs):
    cls = super(GuidMC, mcls).__new__(mcls, name, bases, attrs)

    mcls.define_dh_ctx(cls)
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
        rel.define_dh_ctx(rel)
      del rels_pending_cls[cls_name]

    if cls_name in lists_pending_cls:
      for list_cls in lists_pending_cls[cls_name]:
        list_cls.of_type = cls
      del lists_pending_cls[cls_name]


  def finalize_attr_classes(cls, attrs):
    ''' For each attr class that is a datahog_wrapper.* subclass:
    - assign a more meaningful class name
    - assign a base_ctx + define a datahog context
    '''

    for attr, ctx_cls in attrs.iteritems():

      # Ducktype subclasses of datahog_wrappers.{BaseIdDict, List}
      # that need datahog contexts, and skip the rest.
      if not (hasattr(ctx_cls, '_ctx') or hasattr(ctx_cls, 'of_type')):
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

          list_cls.__name__ = '%s:%s:%s' % (attr,
                                          ctx_cls.__bases__[0].__name__,
                                          'list')
          list_cls.__module__ = cls.__module__
        else:
          lists_pending_cls\
            .setdefault(list_cls._pending_cls_name, [])\
            .append(list_cls)
          ctx_cls = list_cls # sorry

      # Replace the field's temporary class name with something meaningful.
      # E.g.:
      #   'Relation-0' -> 'DocTermsRelation'
      #   'Alias-0' -> 'UserUsernameAlias'
      if 'rename' in ctx_cls.__name__:
        ctx_cls.__name__ = '%s:%s' % (attr,
                                      ctx_cls.__name__.split('-')[0].lower())
        ctx_cls.__module__ = '%s.%s' % (cls.__module__, cls.__name__)


      if has_ancestor_named(ctx_cls, 'BaseIdDict'):

        # subclasses of relation might already have a context
        if type(ctx_cls._ctx) is int:
          continue

        ctx_cls.base_cls = cls
        ctx_cls._meta['base_ctx'] = cls._ctx

        ctx_cls.__metaclass__.define_dh_ctx(ctx_cls)


        # setup class methods for looking up
        # instances by name and alias
        if has_ancestor_named(ctx_cls, 'LookupDict'):
          setattr(cls, 'by_%s' % attr, ctx_cls.lookup)


@only_for_user_defined_subclasses
class NodeMC(ValueDictMC, GuidMC):

  @classmethod
  def define_dh_ctx(mcls, cls):
    if cls.parent is not None:
      cls._meta['base_ctx'] = cls.parent._ctx
    return super(NodeMC, mcls).define_dh_ctx(cls)


  def __new__(mcls, name, bases, attrs):
    args = [name, bases, attrs]

    if 'parent' in attrs and not has_ancestor_named(attrs['parent'], ('Node', 'Entity')):
      raise exc.InvalidParentType(*args)

    return super(NodeMC, mcls).__new__(mcls, *args)


class ListMC(type):
  def __getattr__(cls, name):
    if name == 'flags':
      return cls.of_type.flags
    raise AttributeError


def has_ancestor_named(classes, names):
  ''' String named-based impl of issubclass to circumvent circular import woes
  in NodeDictMC, which wants to refer to the Entity class. In a growing codebase
  where classes share the same name (but different module paths), this might
  cause some serious head scratching...'''
  bases = list(isinstance(classes, type) and classes.__bases__ or classes)
  names = type(names) is tuple and names or (names,)
  while bases:
    base = bases.pop(0)
    if base.__name__ in names:
      return True
    bases.extend(list(base.__bases__))
  return False
