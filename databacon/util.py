from datahog import (
  node, 
  alias, 
  prop, 
  relationship, 
  name, 
  storage, 
  pool as datahog_pool,
  const,
)


_missing = node._missing


guid_prefix = lambda dhw, s: '%s:%s' % (dhw.guid, s)


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


table_to_const = {
  node: const.table.NODE,
  alias: const.table.ALIAS,
  prop: const.table.PROPERTY,
  relationship: const.table.RELATIONSHIP,
  name: const.table.NAME
}



def mummy_const(schema):
  return ({
    None: const.storage.NULL,
    int: const.storage.INT,
    str: const.storage.STR,
    unicode: const.storage.UTF,
  }).get(schema, const.storage.SERIAL)


def flatten(*args):
  for arg in args:
    if isinstance(arg, collections.Iterable) \
       and not isinstance(el, basestring):
      for sub in flatten(el):
        yield sub
      else:
        yield el


pool = None
def connect(shard_config):
  pool = datahog_pool.GreenhouseConnPool(shard_config)
  pool.start()
  if not pool.wait_ready(2.):
    raise Exception("postgres connection timeout")
  return pool



class attrs(object):
  def __init__(self, **attrs):
    for k, v in attrs.iteritems():
      setattr(self, k, v)
