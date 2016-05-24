# -*- coding: utf-8 -*-

from contextlib import ExitStack

import pytest
import sqlalchemy as sa

from .fixtures import (
    AModel,
    MyTestError,
    parametrize,
    random_alpha
)


def getattr_path(obj, path):
    """Get nested `obj` attributes using dot-path syntax (e.g.
    path='some.nested.attr').
    """
    attr = obj

    for item in path.split('.'):
        attr = getattr(attr, item)

    return attr


@parametrize('proxy,attr', [
    ('metadata', 'Model.metadata'),
    ('tables', 'Model.metadata.tables'),
    ('url', 'engine.url'),
    ('database', 'engine.url.database'),
])
def test_database_manager_proxy_property(db, proxy, attr):
    """Test that database manager's proxy attributes map to expected
    real attribute.
    """
    assert getattr(db, proxy) is getattr_path(db, attr)


@parametrize('attr', [
    'add',
    'add_all',
    'close',
    'commit',
    'delete',
    'execute',
    'flush',
    'merge',
    'query',
    'refresh',
    'rollback',
    'transaction',
])
def test_database_manager_property(db, attr):
    """Test that database manager's proxy attributes map to expected
    real attribute.
    """
    assert hasattr(db, attr)
    assert getattr(db, attr)


def test_nonnested_trans_commit(db):
    """Test that a non-nested transaction commits."""
    model = AModel(name=random_alpha())

    with db.transaction():
        db.add(model)

    assert db.query(AModel).get(model.id) is model


def test_nonnested_trans_rollback(db):
    """Test that a non-nested transaction rolls back."""
    model = AModel(name=random_alpha())

    with pytest.raises(sa.orm.exc.FlushError):
        with db.transaction():
            db.add(model)
            db.flush()
            db.add(AModel(id=model.id, name=model.name))


@parametrize('depth', [
    1, 2, 3, 4, 5,
])
def test_nested_trans_single_commit(db, commit_event, depth):
    """Test that a nested transaction results in a single commit."""
    with ExitStack() as stack:
        for _ in range(depth):
            stack.enter_context(db.transaction())
            db.add(AModel(name=random_alpha()))

    assert db.query(AModel).count() == depth
    assert commit_event.call_count == 1


@parametrize('depth', [
    1, 2, 3, 4, 5,
])
def test_nested_trans_single_rollback_on_rollback(db, rollback_event, depth):
    """Test that a nested transaction results in a single rollback when
    database error encountered.
    """
    with pytest.raises(sa.exc.IntegrityError):
        with ExitStack() as stack:
            for i in range(depth):
                idx = i + 1
                stack.enter_context(db.transaction())
                db.add(AModel(id=idx, name=random_alpha()))

            # Intentionally add duplicate primary key to cause IntegrityError.
            db.add(AModel(id=idx, name=random_alpha()))

    assert db.query(AModel).count() == 0
    assert rollback_event.call_count == 1


@parametrize('depth', [
    1, 2, 3, 4, 5,
])
def test_nested_trans_single_rollback_before_commit(db, rollback_event, depth):
    """Test that a nested transaction results in a single rollback when an
    exception occurs before commit is issued.
    """
    with pytest.raises(MyTestError):
        with ExitStack() as stack:
            for i in range(depth):
                stack.enter_context(db.transaction())

            raise MyTestError(('Exception occurs at the bottom-most context '
                               'before commit issued.'))

    assert db.query(AModel).count() == 0
    assert rollback_event.call_count == 1


def test_readonly_trans(db, commit_event):
    """Test that a readonly transaction doesn't commit."""
    with db.transaction(readonly=True):
        db.add(AModel())

    assert commit_event.call_count == 0


def test_nested_trans_outer_readonly(db, commit_event):
    """Test that a write transaction nested inside a readonly transaction
    doesn't commit.
    """
    with db.transaction(readonly=True):
        with db.transaction():
            db.add(AModel())

    assert commit_event.call_count == 0


def test_nested_trans_inner_readonly(db, commit_event):
    """Test that a readonly transaction nested inside a write transaction does
    commit.
    """
    with db.transaction():
        with db.transaction(readonly=True):
            db.add(AModel())

    assert commit_event.call_count == 1


@parametrize('pagination,limit,offset', [
    (15, 15, None),
    ((15,), 15, None),
    ((15, 1), 15, None),
    ((15, 2), 15, 15),
    ((15, 3), 15, 30),
    ((15, -1), 15, None),
    ((15, -2), 15, None),
    ((15, -3), 15, None),
])
def test_query_paginate(db, pagination, limit, offset):
    query = db.query(AModel).paginate(pagination)

    assert query._limit == limit
    assert query._offset == offset
