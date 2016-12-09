# -*- coding: utf-8 -*-

import pydash as pyd
import pytest
import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import DeclarativeMeta

from sqlservice import core, declarative_base

from .fixtures import AModel, Model, parametrize


def test_declarative_base_metadata_as_arg():
    """Test that declarative_base() accepts a metadata argument."""
    _metadata = MetaData()
    Model = declarative_base(metadata=_metadata)

    assert Model.metadata is _metadata


def test_declarative_base_metadata_on_class():
    """Test that delcarative_base() accepts metadata defined on class."""
    _metadata = MetaData()

    class Base(object):
        metadata = _metadata

    Model = declarative_base(Base)

    assert Model.metadata is _metadata


def test_declarative_base_metadata_precedence():
    """Test that delcarative_base() applies precedence to metadata option."""
    metadata1 = MetaData()
    metadata2 = MetaData()

    class Base(object):
        metadata = metadata1

    Model1 = declarative_base(Base)

    assert Model1.metadata is metadata1

    Model2 = declarative_base(Base, metadata=metadata2)

    assert Model2.metadata is metadata2


def test_declarative_base_metaclass_as_arg():
    """Test that declarative_base() accepts a metaclass argument."""
    class MetaClass(DeclarativeMeta):
        pass

    Model = declarative_base(metaclass=MetaClass)

    assert Model.metaclass is MetaClass


def test_declarative_base_metaclass_on_class():
    """Test that delcarative_base() accepts metaclass defined on class."""
    class MetaClass(DeclarativeMeta):
        pass

    class Base(object):
        metaclass = MetaClass

    Model = declarative_base(Base)

    assert Model.metaclass is MetaClass


def test_declarative_base_metaclass_precedence():
    """Test that delcarative_base() applies precedence to metaclass option."""
    class MetaClass1(DeclarativeMeta):
        pass

    class MetaClass2(DeclarativeMeta):
        pass

    class Base(object):
        metaclass = MetaClass1

    Model1 = declarative_base(Base)

    assert Model1.metaclass is MetaClass1

    Model2 = declarative_base(Base, metaclass=MetaClass2)

    assert Model2.metaclass is MetaClass2


@parametrize('model,expected', [
    (AModel({'id': 1, 'name': 'a'}), ((AModel.columns()['id'], 1),))
])
def test_model_identity_map(model, expected):
    """Test that model has an identity map equal to its primary key columns and
    values.
    """
    assert model.identity_map() == expected


@parametrize('model,expected', [
    (AModel({'name': 'a',
             'c': {'name': 'b'},
             'ds': [{'id': 1, 'name': 'd1'},
                    {'id': 2, 'name': 'd2'}]}),
     {'name': 'a',
      'c': {'name': 'b'},
      'ds': [{'id': 1, 'name': 'd1'},
             {'id': 2, 'name': 'd2'}],
      'd_map': {1: {'id': 1, 'name': 'd1'},
                2: {'id': 2, 'name': 'd2'}}}),
    (AModel({'name': 'a',
             'c': None}),
     {'name': 'a',
      'c': {}}),
])
def test_model_to_dict(db, model, expected):
    """Test that a model can be serialized to a dict."""
    db.save(model)
    model = (db.query(model.__class__)
             .filter(core.identity_map_filter(model))
             .options(sa.orm.eagerload('*'))
             .first())

    assert pyd.is_match(model.to_dict(), expected)
    assert pyd.is_match(dict(model), expected)
