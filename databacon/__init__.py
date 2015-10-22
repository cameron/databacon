
import sys

# TODO remove
# PYTHONPATH is not affecting sys.path, need to import
# datahog from a non-egg source so i can debug appropriately
sys.path.insert(0, '/Users/cam/src/_thirdparty/datahog')

from util import connect
from types import *
print types.__all__
for cls_name in types.__all__:
  locals()[cls_name] = getattr(locals()[cls_name], 'field')

