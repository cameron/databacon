# Example Schema Definition


class User(datahog.entity):
  flag_types = [
    # 1-bit flag
    'flagName', 

      # 2-bit enum
    ('enumName',
     ['enumVal1', 'enumVal2', 'enumVal3',]),

      # 4-bit field
    ('bitFieldName', 4) 
  ]

  alias_types = {
    'username': {
      'flag_types': [ flags-ish ],
      'locallyUnique': False # Setting to True will generate aliases
                             # like "base_id:<alias>" -- useful for document titles, but not 
                             # usernames
    }
  }


class Corpus(datahog.node):
  value_type = mummy-ish
  parent_type = User


class Doc(datahog.node):
  parent_type = Corpus
  value_type = {
    'name': str,
    'path': str,
    'tfs': { int: float }
  }
  name_types = {'title': None} # defaults { 'title': { flags: [], search_mode: PREFIX}}
  prop_types = {
    'propName': {
      'flag_types': [ flags-ish ]
      'value_type': mummy-ish
    }
  }

Doc.relation(Doc, flags=[ ... ])


class Term(datahog.node):
  parent_type = Corpus
  value_type = {
    'count': int,
    'term': str
  }


# consumption

user1 = User.by('username')(username1)
user2 = User.by('username')(username2)

corpus = user1.children(Corpus)[0]

corpus.move(user2)

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
doc.aliases('email')[0] 'cameron@gmail'
doc.aliases('email')[0].flags.verified = Doc.aliases('email').flags.verified.pending
doc.add_alias('email', 'some@email.com', flags)

doc.props()
doc.prop('name') # prop['value']
doc.prop('name').flags # flags dict
doc.prop('name').flags.save() # update flags

doc.save() # updates value and any dirty aliases, names, propertys, or flags contained therein


