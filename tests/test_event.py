# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa

from sqlservice import event
from sqlservice._compat import PY2

from .fixtures import Model, parametrize


class EventModel(Model):
    __tablename__ = 'test_events'

    id = sa.Column(sa.types.Integer(), primary_key=True)

    @event.on_set('id')
    def on_set(self, value, oldvalue, initator):
        pass

    @event.on_append('id')
    def on_append(self, value, oldvalue, initator):
        pass

    @event.on_remove('id')
    def on_remove(self, value, oldvalue, initator):
        pass

    @event.before_delete()
    def before_delete(mapper, connection, self):
        pass

    @event.before_insert()
    def before_insert(mapper, connection, self):
        pass

    @event.before_update()
    def before_update(mapper, connection, self):
        pass

    @event.before_save()
    def before_save(mapper, connection, self):
        pass

    @event.after_delete()
    def after_delete(mapper, connection, self):
        pass

    @event.after_insert()
    def after_insert(mapper, connection, self):
        pass

    @event.after_update()
    def after_update(mapper, connection, self):
        pass

    @event.after_save()
    def after_save(mapper, connection, self):
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


@parametrize('target,event,listener', [
    (EventModel.id, 'set', EventModel.on_set),
    (EventModel.id, 'append', EventModel.on_append),
    (EventModel.id, 'remove', EventModel.on_remove),
    (EventModel, 'before_delete', EventModel.before_delete),
    (EventModel, 'before_insert', EventModel.before_insert),
    (EventModel, 'before_update', EventModel.before_update),
    (EventModel, 'before_insert', EventModel.before_save),
    (EventModel, 'before_update', EventModel.before_save),
    (EventModel, 'after_delete', EventModel.after_delete),
    (EventModel, 'after_insert', EventModel.after_insert),
    (EventModel, 'after_update', EventModel.after_update),
    (EventModel, 'after_insert', EventModel.after_save),
    (EventModel, 'after_update', EventModel.after_save),
    (EventModel, 'expire', EventModel.on_expire),
    (EventModel, 'load', EventModel.on_load),
    (EventModel, 'refresh', EventModel.on_refresh),
])
def test_events(target, event, listener):
    """Test that event listeners are properly registered."""
    if PY2:
        for evt_cls in sa.event.base._registrars[event]:
            tgt = evt_cls._accept_with(target)
            if tgt is not None:
                break
        assert tgt
    else:
        assert sa.event.contains(target, event, listener)
