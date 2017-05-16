
def flatten(*args):
  for arg in args:
    if isinstance(arg, collections.Iterable) \
       and not isinstance(el, basestring):
      for sub in flatten(el):
        yield sub
      else:
        yield el

class Attrable(object):
  pass
