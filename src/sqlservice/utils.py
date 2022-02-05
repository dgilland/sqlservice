"""
Utilities
---------

The utilities module.
"""

from functools import wraps
import inspect
import typing as t


def is_iterable_but_not_string(value: t.Any) -> bool:
    """Return whether `value` is an iterable but not string/bytes."""
    return (hasattr(value, "__iter__") and not isinstance(value, (str, bytes))) or is_generator(
        value
    )


def is_generator(value: t.Any) -> bool:
    """Return `value` is a generator."""
    return inspect.isgeneratorfunction(value) or inspect.isgenerator(value)


def raise_for_class_if_not_supported(func):  # pragma: no cover
    """
    Raise NotImplementedError if the version of SQLAlchemy installed doesn't support a feature.

    This is intended to be used on a class instance method/property so the first argument of the
    wrapped function is assumed to be "self".
    """

    @wraps(func)
    def _decorated(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if result is NotImplemented:
            raise NotImplementedError(
                f"{self.__class__.__name__}.{func.__name__} is not supported for SQLAlchemy>=1.4"
            )
        return result

    return _decorated
