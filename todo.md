# Cleanup
- update docstrings in context.py:set_context to reflect new union rels
- finish removing dregs of entity (eg util.ctx_base())


# Features 
- batch property/rel fetching for list of nodes

# Maybe
- prop.set _missing
- treebalancing
- arbitrary txn wrapper: `with db.txn() as set:`
- monitoring
  - http://russ.garrett.co.uk/2015/10/02/postgres-monitoring-cheatsheet/

# Revolution?
- get rid value/num on the node table
- get rid of the node table
- collapse edge into relationship into property
- relationships themselves must have properties
- three tables:
  - property:
    - node_id
    - node_ctx
    - peer_id
    - peer_ctx
    - edge_ctx
    - num
    - value
    - pos
    - forward
    - flags
    - time_removed
    ? version
    ? time_created/_updated
  - alias + name
- go?

# Marketing and Growth Projects/Features
- ingest a SQL schema
