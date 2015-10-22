from databacon import (
  Node,
  Bool,
  Int, 
  Enum,
  String,
  Bits,
  connect,
)


connect({
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
  bits = Bits()
  bits.newsletter_sub = Bits.Bool(default=True)
  bits.role = Bits.Enum('ADMIN', 'STAFF', 'USER')
  bits.corpus_count = Bits.Int(bits=8) 
  bits.alternate_corpus_count = Bits.Int(max=5) 
  username = String(lookup=String.index.unique)
  emails = [String(lookup=String.index.unique)]
  emails.bits.verification_status = Bits.Enum('UNSENT', 
                                              'SENT', 
                                              'RESENT',
                                              'CONFIRMED')
  password = String()
  password.bits.two_factor = Bits.Bool(default=False)
  corpora = [Node('Corpus')]
  corpora.bits.hidden = Bits.Bool()


class Corpus(Node):
  user = User(null=False, shard_afinity=True, relation=User.corpora)
  docs = [Node('Doc')]
  terms = [Node('Term')]


class Doc(Node):
  class file(Object):
    path = String(null=False)
    updated = Date()
  bits = Bits()
  bits.word_count = Bits.Int()
  bits.file_ok = Bits.Bool()
  corpus = Corpus(null=False, shard_affinty=True, relation=Corpus.docs)
  title = String(index=String.index.phonetic, loose=True)
  scores = [Node('Doc')]
  scores.bits.similarity = Bits.Int(bits=16)
  terms = [Node('Term')]
  terms.bits.count = Bits.Int(bits=12)


class Term(Node):
  corpus1 = Corpus(null=False, shard_affinity=True, relation=Corpus.terms)
  string = String(lookup=String.index.unique_to_parent)
  meta = Bits()
  meta.doc_count = Bits.Int()
  docs = [Doc(relation=Doc.terms)]
  different_docs = [Doc()]

