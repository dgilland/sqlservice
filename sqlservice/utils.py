# -*- coding: utf-8 -*-
"""
Utilities
---------

The utilities module.
"""

from collections import Iterable
from functools import wraps

from ._compat import string_types


def classonce(meth):
    """Decorator that executes a class method once, stores the results at the
    class level, and subsequently returns those results for every future method
    call.
    """
    @wraps(meth)
    def decorated(cls, *args, **kargs):
        cached_attr = '__{0}'.format(meth.__name__)
        if not hasattr(cls, cached_attr):
            result = meth(cls, *args, **kargs)
            setattr(cls, cached_attr, result)
        return getattr(cls, cached_attr)
    return decorated


def is_sequence(obj):
    """Test if `obj` is an iterable but not ``dict`` or ``str``. Mainly used to
    determine if `obj` can be treated like a ``list`` for iteration purposes.
    """
    return (isinstance(obj, Iterable) and
            not isinstance(obj, string_types) and
            not isinstance(obj, dict))
