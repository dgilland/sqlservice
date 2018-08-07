# -*- coding: utf-8 -*-
# flake8: noqa
# pylint: skip-file
"""Python 2/3 compatibility
"""

from functools import wraps

import sys
import warnings


PY2 = sys.version_info[0] == 2


if PY2:
    from collections import Iterable, Mapping

    text_type = unicode
    string_types = (str, unicode)
    integer_types = (int, long)

    def iterkeys(d):
        return d.iterkeys()

    def itervalues(d):
        return d.itervalues()

    def iteritems(d):
        return d.iteritems()
else:
    from collections.abc import Iterable, Mapping

    text_type = str
    string_types = (str,)
    integer_types = (int,)

    def iterkeys(d):
        return iter(d.keys())

    def itervalues(d):
        return iter(d.values())

    def iteritems(d):
        return iter(d.items())


def deprecated(message):
    """Decorator that will warn when using a deprecated function."""
    def decorator(func):
        @wraps(func)
        def decorated(*args, **kargs):
            warnings.warn('{!r} is a deprecated function. {}'
                          .format(func.__name__, message),
                          category=DeprecationWarning,
                          stacklevel=2)
            return func(*args, **kargs)
        return decorated
    return decorator
