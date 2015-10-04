# Databacon

## Roadmap

* graphql support
* treebalancing
* replication/failover
  * (and failed txn cleanup)
* push/realtime
  * postgres CREATE TRIGGER, LISTEN and NOTIFY
* query forwarding for efficient, complex graph queries
    * for chained/nested/dependent queries, let the client fire off a fully-formed description of the data it wants, and then the python process on each shard will handle forwarding the dependent lookups to the appropriate shards, each shard returning data to the client directly.

### GraphQL


## Python API

### Features

#### Would Be Rad

* `with db.txn() as txn: ...` for arbitrary txns
* subclass the schema type as the first base
  * so that props, aliases, names, and nodes all behave like their value
    * e.g., user.username().value -> user.username
    * NB: this would break the principle of indicating network operations
      with () or []. if this is acceptable, consider using __iter__
      to implement list generators, as well
    * see note about descriptors below
* iterate over properties of related/child nodes without grabbing the node
  * e.g.
    * `for doc_prop in user.docs.prop():`
    * `for rel, doc_props in user.docs(props=('some_prop',)):`
* futures / dep graph (see datafarm wishlist)


#### Nice To Have

* m:n relationship specificity
* network logging to help users understand how much traffic is being generated
* unify str/unicode schemas
* warn the user about misspelled/missing child/relation classes


### Implementation

* unify child iteration with name/alias/rel iteration
* if instance size becomes a problem, look into __slots__
* use descriptors to simplify implementation
    * would obviate the need to return subclasses of dhw.* as
    fields, and further the need to instantiate them as instances are created
    * enables the simplier user.username pattern mentioned in the API wishlist


### End User Notes

 - () and [] indicate one (or two, in some cases) network operations
   - no cache. () and [] will always fetch from the network
 - () always accept datahog kwargs. mostly, this refers to timeout,
   but sometimes refers to forward_/reverse_index (relationship.create),
   by (prop/node.increment), index (node.move, alias.create, name.create),
   limit and start (node.list/get_children, name.search/list, alias.list)
