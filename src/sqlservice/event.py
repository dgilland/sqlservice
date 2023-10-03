"""
Event
-----

The event module with declarative ORM event decorators and event registration.
"""

from functools import wraps
import inspect
import typing as t

from sqlalchemy.event import listen


POSITIONAL_PARAM_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


class Event:
    """Universal event class used when registering events."""

    def __init__(self, name, attribute, listener, kwargs):
        self.name = name
        self.attribute = attribute
        self.listener = listener
        self.kwargs = kwargs


class EventDecorator:
    """Base class for event decorators that attaches metadata to function object so that
    :func:`register` can find the event definition."""

    event_names: t.Tuple[str, ...] = ()

    def __init__(self, **event_kwargs: t.Any):
        self.attribute = None
        self.event_kwargs = event_kwargs

    def __call__(self, func):
        """
        Function decorator that attaches an `__events__` attribute hook which is expected when
        registering a method as an event handler.

        See :func:`register` for details on how this is implemented.
        """
        if not hasattr(func, "__events__"):
            # Set initial value to list so function can handle multiple events.
            func.__events__ = []

        # Attach event objects to function for register() for find.
        func.__events__.extend(
            Event(name, self.attribute, self._make_listener(func), self.event_kwargs)
            for name in self.event_names
        )

        # Return function so that it's passed on to sa.event.listen().
        return func

    def _make_listener(self, func):
        # We want event listeners to work without them having to define all the event callback
        # arguments. We can inspect the function signature and determine the argument parameters
        # the function has so that we only pass that number of arguments when the event fires.
        sig = inspect.signature(func)

        if any(param.kind == inspect.Parameter.VAR_POSITIONAL for param in sig.parameters.values()):
            # If the function defines var-args (i.e. "*args"), then we can pass all arguments.
            args_count = None
        else:
            # However, if the function does not have var-args, then we need to count the number of
            # positional arguments defined and only pass that many.
            positional_params = [
                param for param in sig.parameters.values() if param.kind in POSITIONAL_PARAM_KINDS
            ]
            args_count = len(positional_params)

        # This slice will return all args up to args_count when args_count is an int or all args
        # when args_count is None.
        args_slice = slice(args_count)

        @wraps(func)
        def _listener(*args):
            return func(*args[args_slice])

        return _listener


class AttributeEventDecorator(EventDecorator):
    """Base class for attribute event decorators."""

    def __init__(self, attribute: t.Any, **event_kwargs: t.Any):
        super().__init__(**event_kwargs)
        self.attribute = attribute
        self.event_kwargs = event_kwargs


class MapperEventDecorator(EventDecorator):
    """Base class for mapper event decorators."""

    def _make_listener(self, func):
        func = super()._make_listener(func)

        @wraps(func)
        def _listener(mapper, connection, target):
            return func(target, connection, mapper)

        return _listener


def register(cls: type, dct: dict) -> None:
    """Register events defined on a class during metaclass creation."""
    events: t.List[Event] = []

    # Add events that were added via @<event> decorator
    for value in dct.values():
        events.extend(getattr(value, "__events__", []))

    if not events:
        return

    for event in events:
        obj = getattr(cls, event.attribute) if event.attribute else cls
        listen(obj, event.name, event.listener, **event.kwargs)


##
# Attribute Events
# http://docs.sqlalchemy.org/en/latest/orm/events.html#attribute-events
##


class on_init_scalar(AttributeEventDecorator):
    """Event decorator for the ``init_scalar`` event."""

    event_names = ("init_scalar",)


class on_init_collection(AttributeEventDecorator):
    """Event decorator for the ``init_collection`` event."""

    event_names = ("init_collection",)


class on_set(AttributeEventDecorator):
    """Event decorator for the ``set`` event."""

    event_names = ("set",)


class on_modified(AttributeEventDecorator):
    """Event decorator for the ``modified`` event."""

    event_names = ("modified",)


class on_append(AttributeEventDecorator):
    """Event decorator for the ``append`` event."""

    event_names = ("append",)


class on_bulk_replace(AttributeEventDecorator):
    """Event decorator for the ``bulk_replace`` event."""

    event_names = ("bulk_replace",)


class on_remove(AttributeEventDecorator):
    """Event decorator for the ``remove`` event."""

    event_names = ("remove",)


class on_dispose_collection(AttributeEventDecorator):
    """Event decorator for the ``dispose_collection`` event."""

    event_names = ("dispose_collection",)


##
# Mapper Events
# http://docs.sqlalchemy.org/en/latest/orm/events.html#mapper-events
##


class before_delete(MapperEventDecorator):
    """Event decorator for the ``before_delete`` event."""

    event_names = ("before_delete",)


class before_insert(MapperEventDecorator):
    """Event decorator for the ``before_insert`` event."""

    event_names = ("before_insert",)


class before_update(MapperEventDecorator):
    """Event decorator for the ``before_update`` event."""

    event_names = ("before_update",)


class before_save(MapperEventDecorator):
    """Event decorator for the ``before_insert`` and ``before_update`` events."""

    event_names = ("before_insert", "before_update")


class after_delete(MapperEventDecorator):
    """Event decorator for the ``after_delete`` event."""

    event_names = ("after_delete",)


class after_insert(MapperEventDecorator):
    """Event decorator for the ``after_insert`` event."""

    event_names = ("after_insert",)


class after_update(MapperEventDecorator):
    """Event decorator for the ``after_update`` event."""

    event_names = ("after_update",)


class after_save(MapperEventDecorator):
    """Event decorator for the ``after_insert`` and ``after_update`` events."""

    event_names = ("after_insert", "after_update")


##
# Instance Events
# http://docs.sqlalchemy.org/en/latest/orm/events.html#instance-events
##


class on_expire(EventDecorator):
    """Event decorator for the ``expire`` event."""

    event_names = ("expire",)


class on_load(EventDecorator):
    """Event decorator for the ``load`` event."""

    event_names = ("load",)


class on_refresh(EventDecorator):
    """Event decorator for the ``refresh`` event."""

    event_names = ("refresh",)
