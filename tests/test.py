
import time
import random

import mummy
import databacon as db

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
  # flags.alternate_corpus_count = db.flag.int(200) # 200 being the max val

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

  scores = db.relation('Doc') # undirected (4-row) relation
  scores.flags.similarity = db.flag.int(bits=16)

  terms = db.relation('Term', directed=True) # directed (2-row) relation
  terms.flags.count = db.flag.int(bits=12)


class Term(db.Node):
  parent = Corpus
  schema = int # number of docs in the corpus that include this term
  string = db.lookup.alias()

  # define an accessor for an existing relationship context
  docs = db.relation(Doc.terms) 

  # It's conceivable that you would want to have multiple kinds of relationships
  # between two classes. E.g., people might be both friends and neighbors.
  different_docs = db.relation(Doc)


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
# or UNIMPLEMENTED
#for child in user0.children(Corpus):
#  assert child.guid == corpus0.guid

# Listing Children UNIMPLEMENTED
#assert len(user0.list_children(Corpus)) == 1
#assert len(user0.list_corpora()) == 1


# Moving Child Nodes
user1 = User() 
corpus0.move(user1)
assert corpus0.parent.guid == user1.guid # they'll actually be the same object


###
### Flags (exists on all DH* instances)
### 

# Setting and saving
user0.flags.role = 'user'
user0.flags.newsletter_sub = True
user0.flags.save()
assert User.by_guid(user0.guid).flags.role == user0.flags.role

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

# More concisely (includes call to .save())
new_name = uniq("camcam")
user0.username(new_name) # replaces the old value because username is defined
                         # as a singular alias

# Lookup user by username (datahog alias)
assert User.by_username(new_name).guid == user0.guid

# Lookup document by title (datahog name)
doc0.title('porcine storage mechanisms, or, pig pens')
assert Doc.by_title('porcine storage').guid == doc0.guid

# Plural Alias/Name (& and passing flags to object creation)
email_flags = User.emails.flags(verification_status='sent')
address = uniq("cam@")
email = user0.emails.add(address, flags=email_flags)

# Visit plural alias/names
for email in user0.emails():
  assert email.value == address
  assert email.flags._int_value == email_flags._int_value

# Property
pass_flags = User.password.flags(two_factor=True)
user0.password('new_password', flags=pass_flags)
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
score = int(.4 * Doc.scores.flags.score.max_val)
score_flags = Doc.scores.flags(similarity=score)
score0 = doc0.scores.add(doc1, flags=score_flags) 

# Generator of relation objects
for score in doc0.scores():
  assert score.flags.similarity == score
  assert score.node().guid == doc1.guid

# Fetch nodes along with the relation 
for score, node in doc0.scores(nodes=True):
  assert node.guid == score.node().guid

# Access single relations
assert doc0.scores[0].flags.similarity == score

# Modify relation order (careful! O(N) db ops with number of displaced relations)
score1 = doc0.scores.add(doc2)
score1.shift(0)

# Directed Relationships & Incrementing an Integer Schema
term = Term(corpus0)
term.string("word")
doc0_term_count = 3 # imagine that "word" occurs 3 times in doc0
term_flags = Doc.terms.flags(count=doc0_term_count)
doc0.terms.add(term, flags=term_flags)

# Creating a relation that already exists should return None.
#
# If Doc.terms were an undirected relationship, this should succeed, 
# but because Doc.terms was defined on Doc with directed=True, and
# because Term.docs was defined as the backdoor to the Doc.terms relationship
# context, term.docs.add calls relationship.create with forward=False, and fails
# where it encounters a row that already exists.
assert term.docs.add(doc0) == None

term.increment() # A term obj's int value is the denormalized count of all 
                 # incoming document relations.
assert Term.by_string("word").value == 1

# Lookup incoming relationships
# term.docs() calls relationship.list with forward=False. See large note
# about directed relationships above. 
for doc_term_rel in term.docs(): 
  assert doc_term_rel.flags.count == doc0_term_count

# Lookup incoming relationships and nodes
for doc, doc_term_rel in term.docs(nodes=True):
  assert doc.guid == doc0.guid
