import databacon as db
import mummy
import os


db.connect({
  'shards': [{
    'shard': 0,
    'count': 4,
    'host': os.environ['DATABACON_DB_1_PORT_5432_TCP_ADDR'],
    'port': os.environ['DATABACON_DB_1_PORT_5432_TCP_PORT'],
    'user': 'legalease',
    'password': '',
    'database': 'legalease',
  }],
  'lookup_insertion_plans': [[(0, 1)]],
  'shard_bits': 8,
  'digest_key': 'super secret',
})


class User(db.Node):
  flags = db.flags()
  flags.newsletter_sub = db.flag.bool(True)
  flags.role = db.flag.enum('admin', 'staff', 'user')
  flags.corpus_count = db.flag.int(bits=8)
  flags.alternate_corpus_count = db.flag.int(max_val=5)

  username = db.lookup.alias()

  emails = db.lookup.alias(plural=True)
  emails.flags.verification_status = db.flag.enum('unsent',
                                                  'sent',
                                                  'resent',
                                                  'confirmed')

  password = db.prop(str)
  password.flags.two_factor = db.flag.bool(False)

  corpora = db.children('Corpus')


class Corpus(db.Node):
  parent = User

  docs = db.children('Doc')

  # TODO if this accessor is not defined, and the terms.string field
  # is a db.lookup.alias(uniq_to_parent=True), it's impossible to
  # lookup terms by their string aliases. Either throw an error if the
  # accessor is missing, or automatically generate an accessor.
  terms = db.children('Term')


class Doc(db.Node):
  parent = Corpus

  flags = db.flags()
  flags.length = db.flag.int(bits=16)

  schema = {
    'path': str,
    mummy.OPTIONAL('top_terms'): [int]
  }

  title = db.lookup.phonetic(loose=True)

  scores = db.relation('Doc')
  scores.flags.similarity = db.flag.int(bits=16)

  terms = db.relation('Term')
  terms.flags.count = db.flag.int(bits=12)


class Term(db.Node):
  parent = Corpus

  schema = int # number of docs in the corpus that include this term

  string = db.lookup.alias(uniq_to_parent=True)

  # define an accessor for the already-declared relationship Doc.terms
  docs = db.relation(Doc.terms)

  special_docs = db.relation(Doc)
