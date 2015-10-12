
import sys

# TODO remove
# PYTHONPATH is not affecting sys.path, need to import
# datahog from a non-egg source so i can debug appropriately
sys.path.insert(0, '/Users/cam/src/_thirdparty/datahog')

import types
from .flags import (
  BitField as flags, 
  BitRanges as flag
)
from .db import connect as ConnectionPool
