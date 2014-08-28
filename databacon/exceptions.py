class CannotIncrementNonNumericValue(Exception):
  message = '''The increment method is reserved for nodes and properties with 
  value_type int.'''


class InvalidParentType(Exception):
  message = ''' Subclasses of Node must specify a parent_type property
  that is itself a subclass of Entity or Node.'''


class DoesNotExist(Exception):
  message = '''Can't modify a node or property that doesn't exist.'''


class WillNotUpdateStaleNode(Exception):
  message = '''The node's in-memory and on-disk values are out of sync. 
  Either refresh the node before saving, or call node.save(force=True).'''


class FlagValueOverflow(Exception):
  def __init__(self, set_val, field_type, max_val):
    self.message = '%s is greater than the %s field\'s maximum value of %s.' % \
                   (set_val, field_type, max_val)


class FlagDefOverflow(Exception):
  message = 'The flags field is limited to 16 bits.'


class UnknownFlagsEnumValue(Exception):
  def __init__(self, flag_name, flag_value, enum_strs):
    self.message = '''`%s` is not in the set of defined enum values for `%s`. Defined 
    values are `%s`.''' % (flag_name, flag_value, enum_strs)
