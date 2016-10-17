# -*- coding: utf-8 -*-
"""
Model
-----

The declarative base model class for SQLAlchemy ORM.
"""

from collections import deque

import sqlalchemy as sa
from sqlalchemy.ext import declarative
from sqlalchemy.util._collections import ImmutableProperties

from . import core, event
from .utils import classonce, is_sequence
from ._compat import iteritems


class ModelMeta(declarative.DeclarativeMeta):
    """Model metaclass that prepares model classes for event registration
    hooks.
    """
    def __new__(mcs, name, bases, dct):
        cls = declarative.DeclarativeMeta.__new__(mcs, name, bases, dct)
        return cls

    def __init__(cls, name, bases, dct):
        declarative.DeclarativeMeta.__init__(cls, name, bases, dct)

        if hasattr(cls, '__table__'):
            event.register(cls, dct)


class ModelBase(object):
    """Declarative base for all ORM model classes."""
    metaclass = ModelMeta
    metadata = None

    def __init__(self, data=None, **kargs):
        self.update(data, **kargs)

    def update(self, data=None, **kargs):
        """Update model by positional ``dict`` or keyword arguments.

        Note:
            If both `data` and keyword arguments are passed in, the keyword
            arguments take precedence.

        Args:
            data (dict, optional): Data to update model with.
            **kargs (optional): Mapping of attributes to values to update model
                with.

        Raises:
            - TypeError: If `data` is not ``None`` or not a ``dict``.
        """
        if data is None:
            data = {}

        if not isinstance(data, dict):  # pragma: no cover
            raise TypeError('Positional argument must be a dict for {0}'
                            .format(self.__class__.__name__))

        data = data.copy()
        data.update(kargs)

        relations = self.relationships().keys()
        field_order = deque()

        # Collect and order data fields in a pseudo-deterministic order where
        # column updates occur before relationship updates but order within
        # those types is indeterministic.
        for field, value in iteritems(data):
            if field in relations:
                # Set relationships last.
                field_order.append((field, value))
            elif hasattr(self, field):
                # Set non-relationships first.
                field_order.appendleft((field, value))

        for field, value in field_order:
            self._set_field(field, value)

    def _set_field(self, field, value):
        """Set model field with value."""
        model_attr = getattr(self, field, None)

        if hasattr(model_attr, 'update') and value and isinstance(value, dict):
            model_attr.update(value)
        elif field in self.relationships().keys():
            self._set_relationship_field(field, value)
        else:
            setattr(self, field, value)

    def _set_relationship_field(self, field, value):
        """Set model relationships field with value."""
        relation_attr = getattr(self.__class__, field)
        uselist = relation_attr.property.uselist
        relation_class = relation_attr.property.mapper.class_

        if uselist:
            if not isinstance(value, (list, tuple)):  # pragma: no cover
                value = [value]

            # Convert each value instance to relationship class.
            value = [relation_class(val) if not isinstance(val, relation_class)
                     else val
                     for val in value]
        elif value and isinstance(value, dict):
            # Convert single value object to relationship class.
            value = relation_class(value)
        elif not value and isinstance(value, dict):
            # If value is {} and we're trying to update a relationship
            # attribute, then we need to set to None to nullify relationship
            # value.
            value = None

        setattr(self, field, value)

    @classmethod
    @classonce
    def class_mapper(cls):
        """Return class mapper instance of model."""
        return sa.inspect(cls)

    @classmethod
    @classonce
    def columns(cls):
        """Return model columns as ``dict`` like ``OrderProperties`` object."""
        return cls.class_mapper().columns

    @classmethod
    @classonce
    def pk_columns(cls):
        """Return tuple of primary key(s) for model."""
        return cls.class_mapper().primary_key

    @classmethod
    @classonce
    def relationships(cls):
        """Return ORM relationships"""
        return cls.class_mapper().relationships

    @classmethod
    @classonce
    def descriptors(cls):
        """Return all ORM descriptors"""
        dscrs = cls.class_mapper().all_orm_descriptors
        return ImmutableProperties({key: dscr for key, dscr in dscrs.items()
                                    if not dscr.is_mapper})

    def descriptors_to_dict(self):
        """Return a ``dict`` that maps data loaded in :attr:`__dict__` to this
        model's descriptors. The data contained in :attr:`__dict__` represents
        the model's state that has been loaded from the database. Accessing
        values in :attr:`__dict__` will prevent SQLAlchemy from issuing
        database queries for any ORM data that hasn't been loaded from the
        database already.

        Note:
            The ``dict`` returned will contain model instances for any
            relationship data that is loaded. To get a ``dict`` containing all
            non-ORM objects, use :meth:`to_dict`.

        Returns:
            dict
        """
        descriptors = self.descriptors()
        return {key: value for key, value in iteritems(self.__dict__)
                if key in descriptors}

    def to_dict(self):
        """Return a ``dict`` of the current model's state (i.e. data returned
        is limited to data already fetched from the database) if some state
        is loaded. If no state is loaded, perform a session refresh on the
        model which will result in a database query. For any relationship data
        that is loaded, ``to_dict`` be called recursively for those objects.

        Returns:
            dict
        """
        session = sa.orm.object_session(self)
        data = self.descriptors_to_dict()

        if not data and session:
            session.refresh(self)
            data = self.descriptors_to_dict()

        for key, value in iteritems(data):
            relationships = self.relationships()

            if hasattr(value, 'to_dict'):
                # Nest call to child to_dict methods.
                value = value.to_dict()
            elif is_sequence(value):
                # Nest calls to child to_dict methods for sequence values.
                value = [val.to_dict() if hasattr(val, 'to_dict') else val
                         for val in value]
            elif isinstance(value, dict):
                # Nest calls to child to_dict methods for dict values.
                value = {ky: val.to_dict() if hasattr(val, 'to_dict') else val
                         for ky, val in iteritems(value)}
            elif key in relationships and value is None:
                # Instead of returning a null relationship value as ``None``,
                # return it as an empty dict. This gives a more consistent
                # representation of the relationship value type (i.e. a non-
                # null relationship value would be a dict).
                value = {}

            data[key] = value

        return data

    def identity(self):
        """Return primary key identity of model instance. If there is only a
        single primary key defined, this method returns the primary key value.
        If there are multiple primary keys, a tuple containing the primary key
        values is returned.
        """
        return core.primary_identity_value(self)

    def identity_map(self):
        """Return primary key identity map of model instance as an ordered dict
        mapping each primary key column to the corresponding primary key value.
        """
        return core.primary_identity_map(self)

    def __getitem__(self, key):
        """Proxy getitem to getattr. Allows for self[key] getters."""
        return getattr(self, key)

    def __setitem__(self, key, value):
        """Proxy setitem to setattr. Allows for self[key] setters."""
        setattr(self, key, value)

    def __iter__(self):
        """Iterator that yields table columns as strings."""
        return iteritems(self.to_dict())

    def __contains__(self, key):
        """Return whether `key` is a model descriptor."""
        return key in self.descriptors()

    def __repr__(self):  # pragma: no cover
        """Return representation of instance with mapped columns to values."""
        columns = self.columns()
        values = ', '.join(['{0}={1}'.format(col, repr(getattr(self, col)))
                            for col in columns.keys()])
        return '<{0}({1})>'.format(self.__class__.__name__, values)


def declarative_base(cls=ModelBase, metadata=None, metaclass=None):
    """Function and decorator that converts a normal class into a SQLAlchemy
    declarative base class.

    Args:
        cls (type): A type to use as the base for the generated declarative
            base class. May be a class or tuple of classes. Defaults to
            :class:`ModelBase`.
        metadata (MetaData, optional): An optional MetaData instance. All
            Table objects implicitly declared by subclasses of the base will
            share this MetaData. A MetaData instance will be created if none is
            provided. If not passed in, `cls.metadata` will be used if set.
            Defaults to ``None``.
        metaclass (DeclarativeMeta, optional): A metaclass or ``__metaclass__``
            compatible callable to use as the meta type of the generated
            declarative base class. If not passed in, `cls.metaclass` will be
            used if set. Defaults to ``None``.

    Returns:
        class: Declarative base class
    """
    if metadata is None:
        metadata = getattr(cls, 'metadata', None)

    if metaclass is None:
        metaclass = getattr(cls, 'metaclass', None)

    options = {'cls': cls,
               'name': cls.__name__}

    if hasattr(cls, '__init__'):
        options['constructor'] = cls.__init__

    if metadata:
        options['metadata'] = metadata

    if metaclass:
        options['metaclass'] = metaclass

    Base = declarative.declarative_base(**options)

    if metaclass:
        Base.metaclass = metaclass

    return Base
