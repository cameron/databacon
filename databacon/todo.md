# Tasks

## Design
* `unique_to_parent` aliases (scoped to an ancestor of owner's object, like a user)
* iterating over properties of related/child nodes without grabbing the node
  * e.g.
    * `for doc_prop in user.docs.prop():`
    * `for rel, doc_props in user.docs(props=('some_prop',)):`
* unify child iteration with name/alias/rel iteration


# Wish List

* tree-scaling + rebalancing
* backups + failover
* failed node txn cleanup
* m:n relationships
* futures / dep graph
