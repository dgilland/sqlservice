"""The sqlservice package."""

__version__ = "1.3.0"

from .client import SQLClient
from .core import destroy, make_identity, save, transaction
from .model import ModelBase, as_declarative, declarative_base
from .query import SQLQuery
