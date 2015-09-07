import databacon as db
import mummy

db.connect({
  'shards': [{
    'shard': 0,
    'count': 4,
    'host': '12.12.12.12',
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

  # convenience accessor for the corpus.children(Doc) generator
  docs = db.children('Doc')


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
  string = db.lookup.alias()

  # define an accessor for the already-declared relationship Doc.terms
  docs = db.relation(Doc.terms) 

  special_docs = db.relation(Doc)
