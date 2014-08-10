import inspect
import functools

from metaclasses import DatahogNodeDictMC, DatahogGuidDictMC
import datahog_wrappers as dhw


class Entity(dhw.DatahogEntityDict):
  __metaclass__ = DatahogGuidDictMC


class Node(dhw.DatahogNodeDict):
  __metaclass__ = DatahogNodeDictMC

