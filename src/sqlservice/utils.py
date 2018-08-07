# -*- coding: utf-8 -*-
"""
Utilities
---------

The utilities module.
"""

from functools import wraps

from ._compat import Iterable, Mapping, string_types, iteritems


class FrozenDict(Mapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)

    def copy(self):
        return self._dict.copy()

    def __getitem__(self, key):
        return self._dict.__getitem__(key)

    def __contains__(self, item):
        return self._dict.__contains__(item)

    def __iter__(self):
        return self._dict.__iter__()

    def __len__(self):
        return self._dict.__len__()

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self._dict)


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
    """Test if `obj` is an iterable but not ``dict`` or ``str``. This function
    is mainly used to determine if `obj` can be treated like a ``list`` for
    iteration purposes.
    """
    return (isinstance(obj, Iterable) and
            not isinstance(obj, string_types) and
            not isinstance(obj, dict))
