"""The sqlservice package."""

__version__ = "2.0.0a1"

from . import event
from .database import Database
from .model import ModelBase, ModelMeta, as_declarative, declarative_base, model_to_dict
from .session import Session
