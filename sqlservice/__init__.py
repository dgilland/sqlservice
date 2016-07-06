# -*- coding: utf-8 -*-
"""The sqlservice package.
"""

from .__pkg__ import (
    __description__,
    __url__,
    __version__,
    __author__,
    __email__,
    __license__)

from .client import SQLClient
from .model import ModelBase, declarative_base
from .query import Query
from .service import SQLService
from . import event
