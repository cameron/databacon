

from .models import Entity, Node
from .field_def import relation, lookup, prop, flag, flags
from .db import connect

# TODO 
# - implement uniqueToBaseId alias flag
# - class lookup methods (alias, name search, id)
# - clearer error reporting for invalid mummy schemas
# - generators for iterating over child/prop/alias/rel lists
