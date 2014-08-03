
import time
import random

import mummy
import databacon as db
from datahog.const import search

# TODONT
db.connect({
  'shards': [{
    'shard': 0,
    'count': 4,
    'host': '10.2.2.2',
    'port': '5432',
    'user': 'legalease',
    'password': '',
    'database': 'legalease',
  }],
  'lookup_insertion_plans': [[(0, 1)]],
  'shard_bits': 8,
  'digest_key': 'super secret',
})

class User(db.Entity):
  flags = db.flags()
  flags.newsletter_sub = db.flag.bool(True)
  flags.role = db.flag.enum('admin', 'staff', 'user')
  flags.corpus_count = db.flag.int(bits=8)
  flags.corpus_count = db.flag.int(255)

  usernames = db.lookup.alias()
  emails = db.lookup.alias()
  emails.flags.verification_status = db.flag.enum('unsent', 
                                                 'sent', 
                                                 'resent',
                                                 'confirmed')

  password = db.prop(str)

  def __init__(self, username, email, password):
    super(User, self).__init__()
    self.add_alias('username', username)
    self.add_alias('email', email)


class Corpus(db.Node):
  parent = User

  schema = {
    'doc_count': int,
    'top_terms': [mummy.OPTIONAL(str)]
  }


class Doc(db.Node):
  parent = Corpus

  schema = {
    'name': str,
    'path': str,
    'term_freqs': { int: float }
  }

  titles = db.lookup.phonetic(loose=True)
  meta = db.prop({
    'reading_level': str,
    'top_terms': [mummy.OPTIONAL(str)]
  })

  scores = db.relation('Doc')
  scores.flags.similarity = db.flags.int(bits=16)

  terms = db.relation('Term')

class Term(db.Node):
  parent = Corpus
  schema = {
    'count': int,
    'term': str
  }


# consumption

uniq = lambda string: string + '%s-%s' % (time.time(), random.random())
user0 = User(uniq('cam@'), uniq('cam'), 'password')
user1 = User(uniq('mac@'), uniq('mac'), 'password') 

user0.flags.newsletter_sub = True
user0.flags.save()

corpus = Corpus(user0)
corpus_same = user0.children(Corpus)[0]
assert corpus.guid == corpus_same.guid

corpus.move(user1)

corpus.children(Doc) # child nodes 
corpus.docs()
doc = corpus.children(Doc)[0] 

doc.flags.flagName = False
doc.flags.enumName = Doc.flags.enumName.val0 # raises if a non-enum value assigned
doc.flags.bitFieldName = 11 # raises if the value overflows the bitfield
doc.flags # { 'flagName': False, 'enumName': Doc.flags.enum.val1, 'bitFieldName': 11 }

doc.scores() # list of docs

doc.relations(Term) # rel entries for the Term ctx
doc.relations(Term).nodes() # and associated nodes (sadly, this also refers to entities)
doc.related_nodes(Term) # straight to the nodes (or entities)
doc.add_relation(term, flags)

rel = doc.relations()[0]
rel.shift(idx, forward)
rel.flags 
rel.node() # of rel_id

doc.aliases('email') # alias objs
doc.aliases('email')[0] # 'cameron@gmail'
doc.aliases('email')[0].flags.verified = Doc.aliases('email').flags.verified.pending
doc.add_alias('email', 'some@email.com', flags)

doc.props()
doc.prop('name') # prop['value']
doc.prop('name').flags # flags dict
doc.prop('name').flags.save() # update flags

doc.save() # updates value and any dirty aliases, names, propertys, or flags contained therein


