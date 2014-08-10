# Tasks

## Design
* `unique_to_parent` aliases (scoped to an ancestor of owner's object, like a user)
* iterating over properties of related/child nodes without grabbing the node
  * e.g.
    * `for doc_prop in user.docs.prop():`
    * `for rel, doc_props in user.docs(props=('some_prop',)):`

## Implementation
*** handle flags passed at creation time ***
* class lookup methods (alias, name, id)
* store datahog meta context info in datahog to allow extension of schema
* collection subclass creation and instantiation (for name/alias/rel)
* directed and undirected relationships (e.g., updating 2 sets of flags for undirected)

# Stretch Goals

* m:n relationships
* futures
