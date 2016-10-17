# -*- coding: utf-8 -*-

from collections import deque
import random
import os
import string
import sys

import pydash as pyd
import pytest
import mock

import sqlalchemy as sa
from sqlalchemy.orm.collections import attribute_mapped_collection

from sqlservice import SQLClient, declarative_base


# Alias long method name.
parametrize = pytest.mark.parametrize


Model = declarative_base()


class AModel(Model):
    __tablename__ = 'test_a'

    id = sa.Column(sa.types.Integer(),
                   primary_key=True,
                   default=lambda val: int(pyd.unique_id()))
    name = sa.Column(sa.types.String(50))
    c_id = sa.Column(sa.types.Integer(), sa.ForeignKey('test_c.id'))

    c = sa.orm.relation('CModel')
    ds = sa.orm.relation('DModel')
    d_map = sa.orm.relation(
        'DModel',
        collection_class=attribute_mapped_collection('id'))


class BModel(Model):
    __tablename__ = 'test_b'

    id1 = sa.Column(sa.types.Integer(),
                    primary_key=True,
                    default=lambda val: int(pyd.unique_id()))
    id2 = sa.Column(sa.types.Integer(),
                    primary_key=True,
                    default=lambda val: int(pyd.unique_id()))
    name = sa.Column(sa.types.String(50))


class CModel(Model):
    __tablename__ = 'test_c'

    id = sa.Column(sa.types.Integer(),
                   primary_key=True,
                   default=lambda val: int(pyd.unique_id()))
    name = sa.Column(sa.types.String(50))


class DModel(Model):
    __tablename__ = 'test_d'

    id = sa.Column(sa.types.Integer(),
                   primary_key=True,
                   default=lambda val: int(pyd.unique_id()))
    name = sa.Column(sa.types.String(50))
    a_id = sa.Column(sa.types.Integer(), sa.ForeignKey('test_a.id'))


@pytest.fixture
def db():
    config = {
        'SQL_DATABASE_URI': 'sqlite://',
        'SQL_ECHO': False
    }
    _db = SQLClient(config, model_class=Model)
    _db.create_all()

    yield _db

    _db.shutdown()
    _db.drop_all()


@pytest.fixture
def filedb(tmpdir):
    dbpath = str(tmpdir.mkdir(random_alpha()).join('file.db'))
    config = {
        'SQL_DATABASE_URI': 'sqlite:///{0}'.format(dbpath)
    }

    _filedb = SQLClient(config, model_class=Model)
    _filedb.create_all()

    yield _filedb

    _filedb.shutdown()
    os.remove(dbpath)


@pytest.fixture
def commit_event(db):
    """Test fixture that uses a ``MagicMock`` as an event listener for database
    session commits. Useful for counting commit events using
    ``commit_event.call_count``.
    """
    counter = mock.MagicMock()
    sa.event.listen(db.session, 'after_commit', counter)
    return counter


@pytest.fixture
def rollback_event(db):
    """Test fixture that uses a ``MagicMock`` as an event listener for database
    session rollbacks. Useful for counting rollback events using
    ``rollback_event.call_count``.
    """
    counter = mock.MagicMock()
    sa.event.listen(db.session, 'after_rollback', counter)
    return counter


class MyTestError(Exception):
    """Test error that can be used to raise inside a test function so that you
    can do pytest.raises and know that this exception was the one that was
    raised.
    """
    pass


def random_alpha(n=8):
    """Return random set of ASCII letters with length `n`."""
    return ''.join(random.SystemRandom().choice(string.ascii_letters)
                   for _ in range(n))
