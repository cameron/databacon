from databacon import (
  Node,
  Flags,
  Bool,
  Int, 
  Enum,
  Alias,
  Enum,
  String,
  Bits,
  Type,
  Name,
  ConnectionPool,
)


ConnectionPool({
  'shards': [{
    'shard': 0,
    'count': 4,
    'host': '12.12.12.12',
    'port': '6543',
    'user': 'legalease',
    'password': '',
    'database': 'legalease',
  }],
  'lookup_insertion_plans': [[(0, 1)]],
  'shard_bits': 8,
  'digest_key': 'super secret',
})


class User(Node):
  bits = Bits.Field(store_on_row=True)
  bits.newsletter_sub = Bool.Field(default=True)
  bits.role = Enum.Field('ADMIN', 'STAFF', 'USER')
  bits.corpus_count = Int.Field(bits=8) 
  bits.alternate_corpus_count = Int.Field(max=5) 
  username = String.Field(lookup=String.index.unique)
  emails = [String.Field(lookup=String.index.unique)]
  emails.bits.verification_status = Enum.Field('UNSENT', 
                                               'SENT', 
                                               'RESENT',
                                               'CONFIRMED')
  password = String.Field()
  password.bits.two_factor = Bool.Field(default=False)
  corpora = [FutureType('Corpus').Field()]



class Corpus(Node):
  user = User.Field(null=False, shard_afinity=True, relation=User.corpora)
  docs = [FutureType('Doc').Field()]
  terms = [FutureType('Term').Field()]



class Doc(Node):
  class file(Object):
    path = String.Field(null=False)
    updated = Date.Field()
  word_count = Int.Field(bits=16, store_on_row=True)
  corpus = Corpus.Field(null=False, shard_affinty=True, relation=Corpus.docs)
  title = String.Field(index=String.index.phonetic, loose=True)
  scores = [FutureType('Doc').Field()]
  scores.similarity = Int.Field(bits=16, store_on_row=True)
  terms = [FutureType('Term').Field()]
  terms.bits.count = Int.Field(bits=12)

''' Field args

- shard_affinity (NodeMC:finalize_attrs)
  - determines the _parent attr 
  - if defined on a FutureType, the owner's dh context can't be defined until the 
    the FutureType is resolved
- relation (NodeMC:finalize_attrs)
  - identifies an existing relation to subclass
- bits, max (Int)
- default value (ValueRow)
- store_on_row (ValueRow)
- null (BaseIdRow, _owner)
  - _owner can't be created without an assigned value/rel
    - which can't be removed without a replacement
    - also implies that the constructor needs to either
      - not save automatically (ew)
      - accept field values as kwargs and assign to self
'''

class Term(Node):
  corpus1 = Corpus.Field(null=False, shard_affinity=True, relation=Corpus.terms)
  corpus2 = Corpus.Field( *args, **kwargs)
  deprecation_warning(corpus2, 'Just a warning')
  string = String.Field(lookup=String.index.unique_to_parent)
  doc_count = Int.Field(store_on_row=True)
  docs = [Doc.Field(relation=Doc.terms)]
  different_docs = [Doc.Field()]

