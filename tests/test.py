#!/usr/bin/env python

import time
import random

import mummy
import databacon as db

# Extremely rudimentary tests to drive databacon API development

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

  # It's conceivable that you would want to have multiple kinds of relationships
  # between two classes. E.g., people might be both friends and neighbors, so we'll
  # need a new rel context and relation subclass for this accessor.
  # 
  # TODO
  # This is also an example of a one-sided relationship. In other words,
  # there is no accessor on the Doc class for this relationship. One-sided
  # relations should be declarable as below, but also using a class string, i.e.,
  # ahead of the related class's definition, which means we'll need
  # to check for incomplete relationships in the define_dh_ctx method of each user-
  # defined subclass.
  # TODO test special_docs 
  special_docs = db.relation(Doc)


uniq = lambda string: string + '%s-%s' % (time.time(), random.random())


### Databacon API Notes
#
# - anywhere you see () or [], expect a network operation (or two, in the case
#   of fetching related nodes with nodes=True) to occur. 
# - anywhere you see (), you can pass datahog kwargs through to the underlying
#   datahog call. mostly, this refers to timeout, but sometimes refers to
#   forward_/reverse_index (relationship.create), by (prop/node.increment),
#   index (node.move, alias.create, name.create), limit and start 
#   (node.list/get_children, name.search/list, alias.list)
# - do not try to bend the cache. simply realize there is no cache.

###
### Nodes & Entities
###

# Creation
user0 = User()
corpus0 = Corpus(user0)
doc0 = Doc(corpus0, {'path': '/path/to/original.file'})
assert user0.guid != corpus0.guid != doc0.guid != None

# Fetching Child Nodes
for child in user0.corpora():
  assert child.guid == corpus0.guid

# TODO 
#for child in user0.children(Corpus):
#  assert child.guid == corpus0.guid

# Listing Children TODO
#assert len(user0.list_children(Corpus)) == 1
#assert len(user0.list_corpora()) == 1


# Moving Child Nodes
user1 = User() 
corpus0.move(user1)
assert corpus0.parent.guid == user1.guid


###
### Flags (exists on all DH* instances)
### 

# Setting and saving
user0.flags.role = 'user'
user0.flags.newsletter_sub = True
user0.flags.save()
fresh = User.by_guid(user0.guid)
assert fresh.flags.role == user0.flags.role
assert fresh.flags.newsletter_sub == user0.flags.newsletter_sub

# More concisely (includes a call to .save())
user0.flags('role', 'admin') 

# Retrieving values
assert user0.flags.role == user0.flags('role') == 'admin'

# Passing in at obj creation (more examples of this later)
user_flags = User.flags(role='user')
user2 = User(flags=user_flags)


###
### Aliases, Names, and Properties (Singular & Plural Value Objs)
###

# Singular Alias/Name
username = user0.username()
username.value = uniq("cam")
username.save()

# Lookup user by username (datahog alias)
# TODO
# assert User.by_username(new_name).guid == user0.guid

# TODO
# Lookup document by title (datahog name)
#doc0.title('porcine storage mechanisms, or, pig pens')
#assert Doc.by_title('porcine storage').guid == doc0.guid

# Plural Alias/Name (& and passing flags to object creation)
email_flags = User.emails.flags(verification_status='sent') # CONSIDER .prepareFlags() ?
address = uniq("cam@")
user0.emails.add(address, flags=email_flags)

# Visit plural alias/names
for email in user0.emails():
  assert email.value == address
  assert email.flags.verification_status == email_flags.verification_status

# Property
pass_flags = User.password.flags(two_factor=True)
user0.password('new_password', flags=pass_flags)
assert user0.password().value == 'new_password'

user0.password('newer_password') # replaces 'new_password'
pw = user0.password() # fetch it fresh
assert pw.value == 'newer_password'
assert pw.flags.two_factor == True

# See Incrementing an Integer Schema'd object below

###
### Relationships
###

doc1 = Doc(corpus0, {'path': '/to/file1'})
doc2 = Doc(corpus0, {'path': '/to/file2'})

# Create
score0_int = int(.4 * Doc.scores.flags.similarity.max_val)
score0_flags = Doc.scores.flags(similarity=score0_int)
assert doc0.scores.add(doc1, flags=score0_flags) == True

# Generator of relation objects
for rel in doc0.scores():
  assert rel.flags.similarity == score0_int
  assert rel.node().guid == doc1.guid

# Fetch nodes along with the relation 
for score, node in doc0.scores(nodes=True):
  assert node.guid == score.node().guid

# Access single relations
assert doc0.scores[0].flags.similarity == score0_int

# Modify relation order
# (Careful! O(N) db ops with number of displaced relations.)
score1_int = 2
assert doc0.scores.add(doc2, flags=Doc.scores.flags(similarity=score1_int)) == True
assert doc0.scores[1].flags.similarity == score1_int
doc0.scores[1].shift(0)
assert doc0.scores[0].flags.similarity == score1_int


# Relationships & Incrementing an Integer Schema
word = uniq("word")
term = Term(parent=corpus0)
term.string(word)
doc0_term_count = 3 # imagine that "word" occurs 3 times in doc0
term_flags = Doc.terms.flags(count=doc0_term_count)
doc0.terms.add(term, flags=term_flags)

# Creating a relation that already exists should return False
assert term.docs.add(doc0) == False

# should be the same relationship
assert term.docs[0].base_id == doc0.guid

term.increment() # A term obj's int value is the denormalized count of all 
                 # incoming document relations.
# TODO
# assert Term.by_string(word).value == 1

# Lookup incoming relationships
for doc_term_rel in term.docs(): 
  assert doc_term_rel.flags.count == doc0_term_count

# Lookup incoming relationships and nodes
for doc_term_rel, doc in term.docs(nodes=True):
  assert doc.guid == doc0.guid
#
