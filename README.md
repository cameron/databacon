# Databacon

Just the best name for a database you've never heard of.

### But actually

This project is mostly dead. The repo exists as a portfolio artifact and curio for database geeks who will find it a precursor to Uber's [_Schemaless_](https://eng.uber.com/schemaless-part-one/) approach to MySQL. An overview of its features, design, and origin follow.

## Bottom-Up

### Postgres

Underneath it all, a simple Postgres schema describes nodes, edges, and properties, and leaves all the fun like **sharding** and **schema enforcement** to subsequent layers.

This schema, and in fact the whole stack, was inspired by work done at Slide, which used a similar approach to drive dozens of facebook apps that encompassed more than 80% of the 2010 Facebook userbase.

### Datahog

The [fat Python client](http://github.com/teepark/datahog) that reads from and writes to Postgres shards; in formal CAP terms, chooses consistency over availability.

### Databacon

Wraps datahog in a beautiful, class-based interface.

## Example

```python
# schema.py

from databacon import Node, lookup, prop, relation

class User(Node):
  name = prop(str)
  age = prop(int)
  email = lookup.alias()    # alias is an indexed string field
  friends = relation('User')
```

```python
# do-stuff.py

from schema import User

user0, user1 = User(), User()

user0.friends.add(user1)

for friend in user0.friends():
  assert friend.guid == user1.guid
```
There's a more feature-complete example modeling a corpus of documents in [tests/](tests/).

## Setup

Note the shenanigens w/the datahog dep.
```
git clone http://github.com/teepark/datahog && pushd datahog && git checkout 5d51fc8d9d && popd
git clone http://github.com/cameron/databacon
pip install -r requirements.txt
pip install -e .../datahog
docker-compose up
PYTHONPATH="./databacon:$PYTHONPATH" tests/test.py
```
If you don't see any output, that means it worked :)
