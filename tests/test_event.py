import pytest
from sqlalchemy import Column, Integer, event as sa_event

from sqlservice import ModelBase, as_declarative, event


parametrize = pytest.mark.parametrize


class FakeEventDecorator(event.EventDecorator):
    event_names = ("test",)


class FakeMapperEventDecorator(event.MapperEventDecorator):
    event_names = ("test",)


@as_declarative()
class EventBase(ModelBase):
    pass


class EventModel(EventBase):
    __tablename__ = "test_events"

    id = Column(Integer(), primary_key=True)

    @event.on_set("id")
    def on_set(self, value, oldvalue, initator):
        pass

    @event.on_append("id")
    def on_append(self, value, oldvalue, initator):
        pass

    @event.on_bulk_replace("id")
    def on_bulk_replace(self, value, oldvalue, initator):
        pass

    @event.on_remove("id")
    def on_remove(self, value, initator):
        pass

    @event.on_init_scalar("id")
    def on_init_scalar(self, value, dict_):
        pass

    @event.on_init_collection("id")
    def on_init_collection(self, collection, collection_adapter):
        pass

    @event.on_dispose_collection("id")
    def on_dispose_collection(self, collection, collection_adapter):
        pass

    @event.on_modified("id")
    def on_modified(self, initator):
        pass

    @event.before_delete()
    def before_delete(self, connection, mapper):
        pass

    @event.before_insert()
    def before_insert(self, connection, mapper):
        pass

    @event.before_update()
    def before_update(self, connection, mapper):
        pass

    @event.before_save()
    def before_save(self, connection, mapper):
        pass

    @event.after_delete()
    def after_delete(self, connection, mapper):
        pass

    @event.after_insert()
    def after_insert(self, connection, mapper):
        pass

    @event.after_update()
    def after_update(self, connection, mapper):
        pass

    @event.after_save()
    def after_save(self, connection, mapper):
        pass

    @event.on_expire()
    def on_expire(self, attrs):
        pass

    @event.on_load()
    def on_load(self, context):
        pass

    @event.on_refresh()
    def on_refresh(self, context, attrs):
        pass


@parametrize(
    "target, event_name, listener",
    # pylint: disable=no-member
    [
        (EventModel.id, "set", EventModel.on_set.__events__[0].listener),
        (EventModel.id, "append", EventModel.on_append.__events__[0].listener),
        (EventModel.id, "remove", EventModel.on_remove.__events__[0].listener),
        (EventModel.id, "init_scalar", EventModel.on_init_scalar.__events__[0].listener),
        (EventModel.id, "init_collection", EventModel.on_init_collection.__events__[0].listener),
        (
            EventModel.id,
            "dispose_collection",
            EventModel.on_dispose_collection.__events__[0].listener,
        ),
        (EventModel.id, "modified", EventModel.on_modified.__events__[0].listener),
        (EventModel.id, "bulk_replace", EventModel.on_bulk_replace.__events__[0].listener),
        (EventModel, "before_delete", EventModel.before_delete.__events__[0].listener),
        (EventModel, "before_insert", EventModel.before_insert.__events__[0].listener),
        (EventModel, "before_update", EventModel.before_update.__events__[0].listener),
        (EventModel, "before_insert", EventModel.before_save.__events__[0].listener),
        (EventModel, "before_update", EventModel.before_save.__events__[1].listener),
        (EventModel, "after_delete", EventModel.after_delete.__events__[0].listener),
        (EventModel, "after_insert", EventModel.after_insert.__events__[0].listener),
        (EventModel, "after_update", EventModel.after_update.__events__[0].listener),
        (EventModel, "after_insert", EventModel.after_save.__events__[0].listener),
        (EventModel, "after_update", EventModel.after_save.__events__[1].listener),
        (EventModel, "expire", EventModel.on_expire.__events__[0].listener),
        (EventModel, "load", EventModel.on_load.__events__[0].listener),
        (EventModel, "refresh", EventModel.on_refresh.__events__[0].listener),
    ],
    # pylint: enable=no-member
)
def test_events(target, event_name, listener):
    """Test that event listeners are properly registered."""
    assert sa_event.contains(target, event_name, listener)


@parametrize(
    "func, expected_call_args_count",
    [
        (lambda: None, 0),
        (lambda a: (a,), 1),
        (lambda a, b: (a, b), 2),
        (lambda a, b, c: (a, b, c), 3),
        (lambda a, b, c, d: (a, b, c, d), 4),
        (lambda a, b, c, d, *, e=1, f=2: (a, b, c, d), 4),
        (lambda *args: args, 10),
    ],
)
def test_event_decorator__makes_listener_that_limits_call_args(func, expected_call_args_count):
    FakeEventDecorator()(func)
    listener = func.__events__[0].listener  # pylint: disable=no-member

    all_args = tuple(range(10))
    result = listener(*all_args)

    if expected_call_args_count > 0:
        assert result == all_args[:expected_call_args_count]
    else:
        assert result is None


def test_mapper_event_decorator__makes_listener_that_reorders_call_args():
    def func(a, b, c):
        return a, b, c

    FakeMapperEventDecorator()(func)
    listener = func.__events__[0].listener  # pylint: disable=no-member

    args = (1, 2, 3)
    expected_result = (3, 2, 1)
    result = listener(*args)
    assert result == expected_result
