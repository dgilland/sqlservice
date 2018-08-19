"""
Utilities
---------

The utilities module.
"""

from collections.abc import Mapping, Sequence
from functools import wraps
import types


class FrozenDict(Mapping):
    """A frozen dictionary implementation that prevents the object from being
    mutated. This is primarily used when defining a dict-like object as a class
    attribute that shouldn't be mutated by subclasses.
    """
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


def flatten(seq):
    """Flatten `seq` a single level deep."""
    for item in seq:
        if is_sequence(item):
            for itm in item:
                yield itm
        else:
            yield item


def is_sequence(value):
    """Test if `value` is a sequence but ``str``. This function is mainly used
    to determine if `value` can be treated like a ``list`` for iteration
    purposes.
    """
    return (is_generator(value) or
            (isinstance(value, Sequence) and not isinstance(value, str)))


def is_generator(value):
    """Return whether `value` is a generator or generator-like."""
    return (isinstance(value, types.GeneratorType) or
            (hasattr(value, '__iter__') and hasattr(value, '__next__') and
             not hasattr(value, '__getitem__')))
