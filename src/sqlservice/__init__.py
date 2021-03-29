"""The sqlservice package."""

__version__ = "1.2.2"

from .client import SQLClient
from .core import destroy, make_identity, save, transaction
from .model import ModelBase, as_declarative, declarative_base
from .query import SQLQuery
