#!/usr/bin/env python

import sys 

def info(type, value, tb):
    if hasattr(sys, 'ps1') or not sys.stderr.isatty():
    # we are in interactive mode or we don't have a tty-like
    # device, so we call the default hook
        sys.__excepthook__(type, value, tb)
    else:
        import traceback, pdb
        # we are NOT in interactive mode, print the exception
        traceback.print_exception(type, value, tb)

        # then start the debugger in post-mortem mode.
        # pdb.pm() # deprecated
        pdb.post_mortem(tb) # more modern

sys.excepthook = info


import time
import random

from schema import User, Corpus, Doc, Term

uniq = lambda s: '%s-%s-%s' % (s, time.time(), random.random())



# Setup
user0, user1 = User(), User()
corpus0 = Corpus(parent=user0)
corpus1 = Corpus(parent=user1)

term = Term(parent=corpus0)
word = uniq("word")
term.string(word)

doc0 = Doc({'path': '/path/to/original.file'}, parent=corpus0)
doc1 = Doc({'path': '/to/file1'}, parent=corpus0)
doc2 = Doc({'path': '/to/file2'}, parent=corpus0)

###
### Nodes & Entities
###

assert user0.guid != corpus0.guid != doc0.guid != None

# Fetching Child Nodes
for corpus in user0.corpora():
  assert corpus.guid == corpus0.guid
  assert corpus.parent == user0
  docs = list(corpus.docs())
  assert len(docs) == 3 
  for doc in docs:
    assert doc.guid in (doc0.guid, doc1.guid, doc2.guid)


# Moving Child Nodes
corpus0.move(user1)
assert corpus0.parent.guid == user1.guid


# Updating Node Values
doc0({'path': 'one'})
doc00 = Doc.by_guid(doc0.guid)
assert doc0.value['path'] == doc00.value['path']
doc00({'path': 'another'})

# Updating stale node values should fail
exc = None
try:
  doc0({'path': 'should fail'})
except Exception, e:
  pass
assert e != None

# Forcing an update should not fail
doc0({'path': 'brute force'}, force=True)
assert Doc.by_guid(doc0.guid).value == doc0.value


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
assert User.by_guid(user0.guid).flags.role == 'admin'

# Retrieving values
assert user0.flags.role == user0.flags('role') == 'admin'

# Passing flags at instance creation (more examples of this later)
user_flags = User.flags(role='user')
user2 = User(flags=user_flags)


###
### Aliases, Names, and Properties (Singular & Plural Value Objs)
###

# Singular Alias
username = user0.username()
username.value = uniq("cam")
username.save()

# Lookup user by alias
assert User.by_username(username.value).guid == user0.guid

# Lookup document by title (datahog name)
title = uniq('porcine storage mechanisms, or, pig pens')
doc0.title(title)
found_it = False
for doc in Doc.by_title(title):
  ''' Iteration here is the result of a bad habit: running the tests repeatedly
  on the same database. '''
  found_it = doc.guid == doc0.guid
  if found_it:
    break
assert found_it

# Plural Alias/Name (& and passing flags to object creation)
email_flags = User.emails.flags(verification_status='sent')
address = uniq("cam@")
user0.emails.add(address, flags=email_flags)

# Visit plural alias/names
for email in user0.emails():
  assert email.value == address
  assert email.flags.verification_status == email_flags.verification_status

# uniq_to_parent
assert corpus0.terms.by_string(word).guid == term.guid
assert corpus1.terms.by_string(word) == None

# Property
pass_flags = User.password.flags(two_factor=True)
user0.password('new_password', flags=pass_flags)
assert user0.password().value == 'new_password'

user0.password('newer_password') # replaces 'new_password'
pw = user0.password() # fetch it fresh
assert pw.value == 'newer_password'
assert pw.flags.two_factor == True


###
### Relationships
###


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
doc0_term_count = 3 # imagine that "word" occurs 3 times in doc0
term_flags = Doc.terms.flags(count=doc0_term_count)
doc0.terms.add(term, flags=term_flags)

# Creating a relation that already exists should return False
assert term.docs.add(doc0) == False

# should be the same relationship
assert term.docs[0].rel_id == doc0.guid

# increment() for int schemas
# (the term's int represents a denormalized count of docs it occurs in)
term.increment()
assert corpus0.terms.by_string(word).value == 1

term.increment()
assert corpus0.terms.by_string(word).value == 2

# Lookup incoming relationships
for doc_term_rel in term.docs():
  assert doc_term_rel.flags.count == doc0_term_count

# Lookup incoming relationships and nodes
for doc_term_rel, doc in term.docs(nodes=True):
  assert doc.guid == doc0.guid


#
# One-sided relationship
#

# term.docs has one entry, so let's make sure that it didn't
# bleed into the separate special_docs relation.
exc = None
try:
  term.special_docs[0]
except IndexError, e:
  exc = e
assert e != None

# create a special doc relation and make sure it didn't bleed into
# the other docs relation
term.special_docs.add(doc1)
for rel in term.docs():
  assert rel.rel_id != doc1.guid

# make sure it the relation was added as expected
assert term.special_docs[0].rel_id == doc1.guid


''' 
TODO
- test index manipulation for names/aliases/rels
  - index/forward_index/reverse_index in create/add calls
  - shift()
- node.remove()
- multi page lists
- plural alias
'''
