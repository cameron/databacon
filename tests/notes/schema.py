# TODO 
# these imports, while sharing the name of classes in the types.py, are in fact
# a different beast, representing a field that will become a class defined in types.py.
# E.g., in databacon/__init__, `Object = functools.partial(FieldFactory, types.Object)`
import databacon as db


class User(db.Node):
  email = db.Alias(null=True)
  email.flags.verification_status = db.Enum('unsent', 
                                         'sent', 
                                         'resent',
                                         'confirmed')

  password = db.String(null=False)
  notes = [db.Type('Note')]
  tags = [db.Type('Tag')]


class Note(db.Node):
  user = User(null=False, shard_affinity=True)
  tags = [db.Type('Tag')]
  text = db.String()


class Tag(db.Node):
  user = User(null=False, shard_affinity=True)
  text = db.Alias(parent_id_prefix=True)
  notes = [Note.tags]

  def __init__(self, text):
    super(Tag, self).__init__(text)
    self.text(text)


