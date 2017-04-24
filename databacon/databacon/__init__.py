
import sys

# TODO remove
# PYTHONPATH is not affecting sys.path, need to import
# datahog from a non-egg source so i can debug appropriately
sys.path.insert(0, '/Users/cam/src/_thirdparty/datahog')

from .fields import relation, lookup, prop, children, lock
from .datahog_wrappers import Node
from .flags import Layout as flags, Fields as flag
from .db import connect
