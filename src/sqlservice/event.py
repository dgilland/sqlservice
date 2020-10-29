"""
Event
-----

The event module with declarative ORM event decorators and event registration.
"""

from sqlalchemy.event import listen


class Event:
    """Universal event class used when registering events."""

    def __init__(self, name, attribute, listener, kwargs):
        self.name = name
        self.attribute = attribute
        self.listener = listener
        self.kwargs = kwargs


class EventDecorator:
    """Base class for event decorators that attaches metadata to function object so that
    :func:`register` can find the event definition.
    """

    event_names = ()

    def __init__(self, **event_kwargs):
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
            Event(name, self.attribute, func, self.event_kwargs) for name in self.event_names
        )

        # Return function so that it's passed on to sa.event.listen().
        return func


class AttributeEventDecorator(EventDecorator):
    """Base class for an attribute event decorators."""

    def __init__(self, attribute, **event_kwargs):
        self.attribute = attribute
        self.event_kwargs = event_kwargs


def register(cls, dct):
    """Register events defined on a class during metaclass creation."""
    events = []

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


class before_delete(EventDecorator):
    """Event decorator for the ``before_delete`` event."""

    event_names = ("before_delete",)


class before_insert(EventDecorator):
    """Event decorator for the ``before_insert`` event."""

    event_names = ("before_insert",)


class before_update(EventDecorator):
    """Event decorator for the ``before_update`` event."""

    event_names = ("before_update",)


class before_save(EventDecorator):
    """Event decorator for the ``before_insert`` and ``before_update`` events."""

    event_names = ("before_insert", "before_update")


class after_delete(EventDecorator):
    """Event decorator for the ``after_delete`` event."""

    event_names = ("after_delete",)


class after_insert(EventDecorator):
    """Event decorator for the ``after_insert`` event."""

    event_names = ("after_insert",)


class after_update(EventDecorator):
    """Event decorator for the ``after_update`` event."""

    event_names = ("after_update",)


class after_save(EventDecorator):
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
