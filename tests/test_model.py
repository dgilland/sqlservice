# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import DeclarativeMeta

from sqlservice import declarative_base


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
