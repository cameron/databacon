
import time

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
  flag_types = [
    # 1-bit flag
    'newsletter_sub', 

    # enum (2 bits)
    ('role', ('admin', 'staff', 'user')),

      # int value (max value 256)
    ('corpus_count', 8) 
  ]

  alias_types = {
    'username': {
      'globally_unique': True # False creates aliases that are unique
                              # locally, as in to this user
    },
    'email': {
      'flag_types': [('verification_status', 
                      ('unsent', 'sent', 'resent', 'confirmed'))],
      'unique_to_owner': False
    },
  }

  prop_types = {
    'password': { 'value_type': str }
  }

  def __init__(self, username, email, password):
    super(User, self).__init__()
    self.add_alias('username', username)
    self.add_alias('email', email)


class Corpus(db.Node):
  parent_type = User

  value_type = {
    'doc_count': int,
    'top_terms': [mummy.OPTIONAL(str)]
  }


class Doc(db.Node):
  parent_type = Corpus

  value_type = {
    'name': str,
    'path': str,
    'term_freqs': { int: float }
  }

  name_types = { 
    'title': {
      'search_mode': search.PHONETIC,
      # omitting flags defaults to []
    }
  } 

  prop_types = {
    'meta': {
      'flag_types': ['totallyUnncesaryFlag'],
      'value_type': {
        'reading_level': str,
        'top_terms': [mummy.OPTIONAL(str)]
      }
    }
  }

Doc.relation(Doc, flag_types=[('similarity', 16)])

class Term(db.Node):
  parent_type = Corpus
  value_type = {
    'count': int,
    'term': str
  }

Term.relation(Doc)

# consumption

t = time.time()
un0 = 'cam-%s' % t
un1 = 'mac-%s' % t
em0 = 'cam@-%s' % t
em1 = 'mac@-%s' % t
user0 = User(em0, un0, 'password')
user1 = User(em1, un1, 'password') 

user0.flags.newsletter_sub = True
user0.flags.save()

import pdb; pdb.set_trace()
corpus = Corpus(user0)
corpus_same = user0.children(Corpus)[0]
assert corpus.guid == corpus_same.guid

corpus.move(user1)

corpus.children(Doc) # child nodes 
corpus.list_children(Doc) # guids
doc = corpus.children(Doc)[0] 

doc.flags.flagName = False
doc.flags.enumName = Doc.flags.enumName.val0 # raises if a non-enum value assigned
doc.flags.bitFieldName = 11 # raises if the value overflows the bitfield
doc.flags # { 'flagName': False, 'enumName': Doc.flags.enum.val1, 'bitFieldName': 11 }

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


