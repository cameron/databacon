# Databacon

## Python API

# On Deck Dep Chain

- on_node=True storage
- Class.bits(preset='values')

### Cleanup
- unify String, Bool, Int, Bits, Float, Object behind Prop
- implement fields as descriptors
  - `user.email -> <UnfetchedEmail>`
  - `user.email() -> 'email@domain.com'`'
  - `user.email -> 'email@domain.com''`
  - `user.email('set@something.com')`
  - this pattern is generally nice for exposing the node value and flags
    objects without new network op semantics. they will be available without
    (), like any other fetched value. () will also force fetch/save
- Password field
- class lookup methods
  - user = User.email.get(email)
    - hydrate user.email (same for name)
  - users = User.email.batch([emails])
  - docs = Doc.title.search(title)
  - obj = Guid.id.get() # when will .id or .ids ever get called by the user? seems like admin util
  - objs = Guid.id.batch(ids)
- _parent should refer to the shard_affinity object

### GraphQL Support
- list access (incl. batch, search, rel/child)
  - array methods: insert/append/pop/unshift
  - list() generates nodes, hydrates node.edge with the rel/edge
    - list(with=('name', 'email', 'password')) to batch fetch props
    - NB node value column is most efficient for iteration
    - `for node in other.rel():
        assert node.edge.node.guid == other.guid and node.edge.node != other`
  - list.edges() for rels only (and only in the case of rels/children, not alias/name)
  - list.props('name', 'email', 'password') for props only (no nodes)
    - this is going to be way less efficient before implementing batch property fetching by guid
    - and lazy fetch the flags/value on read
  - greenhouse.map will help with batch fetching
- interfaces

### Low-Priority Implementation TODOs
- warn the user about Type('MisspelledClass')
- network op logging

## Roadmap
- api prep for graphql
- graphql support
- caching layers
  - listservon top of rel lists
- scaled (100s of nodes) in-memory data structures with configurable persistence
  - giant hash map
  - queues
- web crawler demo
- migrations
- txns: `with db.txn() as commit:`
- treebalancing
- replication/failover
  - failed txn cleanup
- push/realtime
  - postgres CREATE TRIGGER, LISTEN and NOTIFY
- query forwarding for efficient, complex graph queries
    - for chained/nested/dependent queries, let the client fire off a fully-formed description of the data it wants, and then a client process on each shard will handle forwarding the dependent lookups to the appropriate shards, each shard returning data to the client directly.
    - requires a client-client-server setup (python->(python->postgres))


### User Notes
 - () and [] indicate one (or two, in some cases) network operations
   - no cache. () and [] will always fetch from the network
 - () always accept datahog kwargs. mostly, this refers to timeout,
   but sometimes refers to forward_/reverse_index (relationship.create),
   by (prop/node.increment), index (node.move, alias.create, name.create),
   limit and start (node.list/get_children, name.search/list, alias.list)
