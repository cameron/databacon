import inspect
import functools

from datahog import alias, node, relationship, prop, entity, name
from metaclasses import NodeMC, DatahogGuidDictMC
import datahog_wrappers as dhw
import db


class Entity(dhw.DatahogEntity)
  __metaclass__ = DatahogGuidDictMC


class Node(dhw.DatahogNode):
  __metaclass__ = NodeMC



  
# TODO
# - attr instantiation

# Stretch
# - plurality (aliases, props, names)
# - cardinality (rels)
