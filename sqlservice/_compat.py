# flake8: noqa
# pylint: skip-file
"""Python 2/3 compatibility

    Some py2/py3 compatibility support based on a stripped down
    version of six so we don't have to depend on a specific version
    of it.

    Borrowed from
    https://github.com/mitsuhiko/flask/blob/master/flask/_compat.py
"""

import sys


PY2 = sys.version_info[0] == 2


if PY2:
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
    text_type = str
    string_types = (str,)
    integer_types = (int,)

    def iterkeys(d):
        return iter(d.keys())

    def itervalues(d):
        return iter(d.values())

    def iteritems(d):
        return iter(d.items())
