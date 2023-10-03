"""
Model
-----

The declarative base model class for SQLAlchemy ORM.
"""

from abc import ABCMeta
import typing as t

import sqlalchemy as sa
from sqlalchemy import MetaData, orm
from sqlalchemy.orm import NO_VALUE, DeclarativeMeta, declarative_base as _declarative_base
from sqlalchemy.sql import Delete, Insert, Select, Update

from . import event
from .utils import is_iterable_but_not_string


class DeclarativeModel(metaclass=ABCMeta):  # noqa: B024
    @classmethod
    def __subclasshook__(cls, class_):
        if cls is DeclarativeModel:
            return isinstance(class_, DeclarativeMeta)
        return NotImplemented  # pragma: no cover


class ModelMeta(DeclarativeMeta):
    """Model metaclass that prepares model classes for event registration hooks."""

    def __new__(mcs, name, bases, dct):
        cls = DeclarativeMeta.__new__(mcs, name, bases, dct)
        return cls

    def __init__(cls, name, bases, dct):
        DeclarativeMeta.__init__(cls, name, bases, dct)

        if hasattr(cls, "__table__"):
            event.register(cls, dct)


class ModelBase:
    """Declarative base for all ORM model classes."""

    metadata: t.ClassVar[MetaData]
    registry: t.ClassVar[orm.registry]
    __table__: t.Optional[sa.Table]
    __mapper__: orm.Mapper

    def __init__(self, **kwargs: t.Any):
        self.set(**kwargs)

    def set(self, **kwargs: t.Any) -> None:
        """Update model using keyword arguments."""
        cls = type(self)
        for k, v in kwargs.items():
            if not hasattr(cls, k):
                raise TypeError(f"{k!r} is an invalid keyword argument for {cls.__name__}")
            setattr(self, k, v)

    def pk(self) -> t.Tuple[t.Any, ...]:
        """Return primary key identity of model instance."""
        return self.__mapper__.primary_key_from_instance(self)

    def to_dict(
        self, *, exclude_relationships: bool = False, lazyload: bool = False
    ) -> t.Dict[str, t.Any]:
        """
        Serialize ORM loaded data to dictionary.

        Only the loaded data, i.e. data previously fetched from the database, will be serialized.
        Lazy-loaded columns and relationships will be excluded to avoid extra database queries.

        By default, only table columns will be included. To include relationship fields, set
        ``include_relationships=True``. This will nest ``to_dict()`` calls to the relationship
        models.
        """
        serializer = ModelSerializer(exclude_relationships=exclude_relationships, lazyload=lazyload)
        return serializer.to_dict(self)

    def __iter__(self):
        """Iterator that yields the items from ``self.dict().items()``."""
        yield from self.to_dict().items()

    def __repr__(self) -> str:
        """Return representation of model."""
        data = self.to_dict(exclude_relationships=True)
        values = ", ".join(f"{k}={v!r}" for k, v in data.items())
        return f"{type(self).__name__}({values})"

    @classmethod
    def select(cls) -> Select:
        """Return instance of ``sqlalchemy.select(Model)`` for use in querying."""
        return sa.select(cls)

    @classmethod
    def insert(cls) -> Insert:
        """Return instance of ``sqlalchemy.insert(Model)`` for use in querying."""
        return sa.insert(cls)

    @classmethod
    def update(cls) -> Update:
        """Return instance of ``sqlalchemy.update(Model)`` for use in querying."""
        return sa.update(cls)

    @classmethod
    def delete(cls) -> Delete:
        """Return instance of ``sqlalchemy.delete(Model)`` for use in querying."""
        return sa.delete(cls)


class ModelSerializer:
    def __init__(self, *, exclude_relationships: bool = False, lazyload: bool = False):
        self.exclude_relationships = exclude_relationships
        self.lazyload = lazyload

    def to_dict(self, model: ModelBase) -> t.Dict[str, t.Any]:
        ctx: t.Dict[str, t.Any] = {"seen": set()}
        return self.from_value(ctx, model)

    def from_value(self, ctx: dict, value: t.Any) -> t.Any:
        if isinstance(value, DeclarativeModel):
            value = self.from_model(ctx, value)
        elif isinstance(value, dict):
            value = self.from_dict(ctx, value)
        elif is_iterable_but_not_string(value):
            value = self.from_iterable(ctx, value)
        return value

    def from_model(self, ctx: dict, value: t.Any) -> t.Dict[str, t.Any]:
        ctx.setdefault("seen", set())
        ctx["seen"].add(value)

        state: orm.state.InstanceState = sa.inspect(value)
        mapper: orm.Mapper = sa.inspect(type(value))

        fields = mapper.columns.keys()
        if not self.exclude_relationships:
            fields += mapper.relationships.keys()

        data = {}
        for key in fields:
            loaded_value = state.attrs[key].loaded_value

            if (
                loaded_value is NO_VALUE or loaded_value is orm.LoaderCallableStatus.NO_VALUE
            ) and self.lazyload:
                loaded_value = state.attrs[key].value

            if (loaded_value is NO_VALUE or loaded_value is orm.LoaderCallableStatus.NO_VALUE) or (
                isinstance(loaded_value, DeclarativeModel) and loaded_value in ctx["seen"]
            ):
                continue

            data[key] = self.from_value(ctx, loaded_value)

        return data

    def from_dict(self, ctx: dict, value: dict) -> dict:
        return {k: self.from_value(ctx, v) for k, v in value.items()}

    def from_iterable(self, ctx: dict, value: t.Iterable) -> list:
        return [self.from_value(ctx, v) for v in value]


def declarative_base(
    cls: t.Type[ModelBase] = ModelBase,
    *,
    metadata: t.Optional[MetaData] = None,
    metaclass: t.Optional[t.Type[DeclarativeMeta]] = None,
    **kwargs: t.Any,
) -> t.Type[ModelBase]:
    """
    Function that converts a normal class into a SQLAlchemy declarative base class.

    Args:
        cls: A type to use as the base for the generated declarative base class. May be a class or
            tuple of classes. Defaults to :class:`ModelBase`.
        metadata: An optional MetaData instance. All Table objects implicitly declared by subclasses
            of the base will share this MetaData. A MetaData instance will be created if none is
            provided. Defaults to ``None`` which will associate a new metadata instance with the
            returned declarative base class.
        metaclass: A metaclass or ``__metaclass__`` compatible callable to use as the meta type of
            the generated declarative base class. Defaults to :class:`ModelMeta`.

    Keyword Args:
        All other keyword arguments are passed to ``sqlalchemy.ext.declarative.declarative_base``.
    """
    if metaclass is None:
        metaclass = ModelMeta

    kwargs.setdefault("name", cls.__name__)

    if hasattr(cls, "__init__"):
        kwargs.setdefault("constructor", cls.__init__)

    return _declarative_base(  # type: ignore
        cls=cls, metadata=metadata, metaclass=metaclass, **kwargs
    )


def as_declarative(
    *,
    metadata: t.Optional[MetaData] = None,
    metaclass: t.Optional[t.Type[DeclarativeMeta]] = ModelMeta,
    **kwargs: t.Any,
) -> t.Callable[[t.Type[ModelBase]], t.Type[ModelBase]]:
    """Decorator version of :func:`declarative_base`."""

    def decorate(cls):
        return declarative_base(cls, metadata=metadata, metaclass=metaclass, **kwargs)

    return decorate
