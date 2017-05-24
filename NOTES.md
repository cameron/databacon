# GATHER DEALBREAKERS
- how to handle schema modifications?
- 2to3
  - mummy is borked (no attr subversion on sys module)
  * gevent
  - relationship
    - flags (eg muted must not be synced, or must have muted_high and muted_low)
    - value (seems like it'd be easy to implement syncing just as with flags, or even easier without)

# easy
- prevent adding relations of the incorrect context type
- rename the dh object/kwarg to row
- support for environment-based shard-config
- string name -> class hash for user classes on the databacon module
  - eg, for making a generic url id -> instance converter
- encode permissions in id (not sure if this needs to be part of databacon or not)
- rename guid to id (along with absorbing the interface improvements from graphql branch?)
- Singleton subclass of Node
- Class.attr.flags is referencable but not part of the public API (.flag is correct)

# medium
- save unsaved node properties via node.save()
- absorb the updates from the graphql branch
- relationships
  * lookup relationships with WHERE flags clause for awesome filtering (travis says yay)
    - should just take an additional and clause
  - enum for forward/backward/none?
  * cardinality
    - when designing: make it sympathetic with the List() API
  - maintain sorted order by node property
    - kwarg on .add() that executes a bisect search or something 
  - traverse relationships without fetching node
    - eg, user -> groups of friends w/friend nodes (not necessary to grab group nodes)
  - flags
    - allow them to be unsyncrhonized
  - warn the user about misspelled rel class accessors?
  ? what should [] do? nodes? edges? both?
    - doing node implies 2 network ops
    - not doing node implies `list(node.rels())[0]` :(
- node
  - select all nodes of type
- props
  * bulk fetch
- jsonification
  - flags
  ? return all fetched attrs
  ? fetch some default set of attrs
  ? rels
    - lists of dh rows
    - separate bag of nodes
      - makes possible: graph crawl to arbitrary depth :)
- use descriptors to simplify implementation
    - would obviate the need to return subclasses of dhw.* as
    fields, and further the need to instantiate them as instances are created
    - enables the simplier user.username pattern mentioned below
- subclass the schema type as the first base
  - so that props, aliases, names, and nodes all behave like their value
    - e.g., user.username().value -> user.username
    - NB: this would break the principle of indicating network operations
      with () or []. if this is acceptable, consider using __iter__
      to implement list generators, as well
    - see note about descriptors below
- iterate over properties of related/child nodes without grabbing the node
  - e.g.
    - `for doc_prop in user.docs.prop():`
    - `for rel, doc_props in user.docs(props=('some_prop',)):`

# hard
- `with db.txn() as txn:` for arbitrary op txns
- unify child iteration with name/alias/rel iteration
- futures / dep graph
- network logging to help users understand how much traffic is being generated
- tree-scaling + rebalancing
- availability/replication/failover/backups
- failed txn cleanup


# Production/Deploy TODO
- find out where to watch for two phase commit zombies


# For Thought
- __init__ and __create__
  P: i've several times forgotten that __init__ can't accept any args, b/c if it does then it's impossible
    for lists to create instances with just a `dh` kwarg.
  P: it's also possible call super in the wrong place, or forget to
  A?: add a __create__ method that __init__ delegates to when dh is not passed
- what would it take to make datahog ctx generation stable in the face of renames + reorders?
  - an id per field would allow stable sorting to prevent changes in attr order (or python handling of
    attrs) from affecting ctx generation