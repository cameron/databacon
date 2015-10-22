# Complex graph traversal queries in English, Cypher, GraphQL, and databacon
# for the purposes of databacon API design


## English
#    londoners who own iphone5s and speak more than 1 language

## Cypher
#    start
#      london=node:node_auto_index(name="London")
#      iphone5=node:node_auto_index(name="iphone5")
#    match
#      person-[:USES_PHONE]->iphone5,
#      person-[:LIVES_IN]->london,
#      person-[:SPEAKS]->language,
#    with count(*) as numberoflanguages, person
#    where numberoflanguages > 1
#    return person.name

## GraphQL
# 
# {
#   user(city: "London", phone: "iPhone5", polyglot: true) {
#     name
#   }
# }

## Databacon (desired)
for person in Phone.get('iPhone5') \
                   .users(prop='languages') \
                   .intersect(
                     City.get('London')
                     .inhabitants()):
  # interesting to note this could be optimized by creating a 
  # node that represents a multilingual badge
  # - manage membership by watching the languages edge collection?
  if len(person.languages) > 1:
    return person




# Q1: all of a user's tags
### Cypher
# start
#   user=node:node_auto_index(name="username")
# match
#   person-[:SPEAKS]->language,
# where numberoflanguages > 1
# return person.name

### databacon
tags = User.get('willhelm').docs().tags()


# Q2: a city with a Neotech employee that speaks english and has three as his operator
# 
# Cypher
#
# start
#   neo=node:node_auto_index(name='Neo Tech'),
#   english=node:node_auto_index(name='English'),
#   person=node:node_auto_index(name='Three'),
# match
#   person-[:LIVES_IN]->city,
#   person-[:HAS_AS_HOME_OPERATOR]->three,
#   person-[:SPEAKS]->english,
#   person-[:WORKS_FOR]->neo,
# return
#   city.name, person.name

Company.get('neotech').employees() \
.language('english') \ # ambiguity here is whether we are filtering or traversing. argument seems to make it unambiguous, actually
.operator('three') \
.city()

# Q2.1: All languages spoken by neotech employees
Company.get('neotech').employees().language()

# Q2.2: All languages spoken by neotech employees that have more than three direct reports
Company.get('neotech').employees()...?
Company.get('neotech').employees().reports() # <-- filter vs traverse is hard here


# Q3: all newspapers that have published pieces in the past week on the us bombing
# of a drs with brdrs that do not mention US involvement 
# (assumes topics are nodes w/edges to articles)


with db.crawl() as g:
  Topics.get('drs w/o brders', 'bombing').articles()
without = Topics.get('usa').articles()



# all son's daughters of 'paychaud'
# 
 = User.get('paychaud').


# maybe?

with graph.crawl() as subg:
  sg.push(model.rels()) #... ?


# for txns
with graph.txn() as txn:
  model0.query(..., txn=txn)
  model1.mutation(..., txn=txn)
  model2.mutation(..., txn=txn)
# retries and failure?
# ...


# Graph Traversal Syntax Features
# see gremlin docs
# - traverse (select nodes via edges)
# - filter, intersect ()
# - negate (does not have prop/edge)


# Questions
# - how did libor use nodes to support union relationships?
#    A: with an extra network op
#    - people and pets are both characters

# Implementation TODO
# - bacon:  
#   - support cypher queries AND gql queries
#     - (default is query for both is against all known classes, optional whitelist kwarg)
#     - databacon.graph.cypher
#     - databacon.graph.gql
# - syrup (caching)
#   - cache graph paritions in neo4j instances for awesome cypher queries
#   - reflect on itemserv
#   - naive but maybe quick + effective: use neo4j as an LRU for edges
#   - test for cache utility by measuring the hit rate on the top n most-accessed items, where n is the number of items that can be fit into the cache
#     - good hurustic for/against LRU (warn when thrashing)
#   - fast traversals: store graph in memory (rels and type annotations) 
#     - Q: how much of the graph can we fit? 
#        - research minimal storage graph represntations
#        - neo4j's 2^35 address space is smaller, by a ton



# Implementation Thoughts
# - just nuts: read iteration block of code from within _enter_ or _iter_, replace with 'pass', and return an empty iterator, using the stolen block as an argument to a coroutine (this would be easy in ruby's blocks?)
#   

# Out There Thoughts
# - graphql docs should be
#    - exposed by the api itself when running in debug mode
#    - generated from source
# - does jsx signal the rise of the multisyntax file mode?
#   - what would it look like to define views next to models? like it because it
#     groups semantic concerns, not implementation details (file type/language),
#     just like the pixinote code base
