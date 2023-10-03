from pathlib import Path
import random
import string
import typing as t

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sqlservice import AsyncDatabase, Database, ModelBase, as_declarative


try:
    from sqlalchemy.orm.collections import attribute_keyed_dict
except ImportError:
    from sqlalchemy.orm.collections import (  # type: ignore
        attribute_mapped_collection as attribute_keyed_dict,
    )


def unique_id():
    if not hasattr(unique_id, "_id"):
        unique_id._id = 0
    unique_id._id += 1
    return unique_id._id


@as_declarative()
class ModelOld(ModelBase):
    pass


@as_declarative()
class Model(ModelBase):
    pass


class User(Model):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(sa.Integer(), primary_key=True)
    name: Mapped[t.Optional[str]] = mapped_column(sa.String())
    active: Mapped[bool] = mapped_column(sa.Boolean(), default=True)

    addresses: Mapped[t.List["Address"]] = relationship("Address", back_populates="user")
    group_memberships: Mapped[t.List["GroupMembership"]] = relationship(
        "GroupMembership", back_populates="user"
    )
    items: Mapped[t.List["GroupMembership"]] = relationship("Item")


class Address(Model):
    __tablename__ = "addresses"

    id: Mapped[int] = mapped_column(sa.Integer(), primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.Integer(), sa.ForeignKey("users.id"), nullable=False)
    addr: Mapped[t.Optional[str]] = mapped_column(sa.String())
    zip_code: Mapped[t.Optional[str]] = mapped_column(sa.String())

    user: Mapped[User] = relationship("User", back_populates="addresses")


class Group(Model):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(sa.types.Integer(), primary_key=True)
    name: Mapped[str] = mapped_column(sa.types.String(), unique=True)

    memberships: Mapped[t.List["GroupMembership"]] = relationship(
        "GroupMembership", back_populates="group"
    )


class GroupMembership(Model):
    __tablename__ = "group_memberships"

    group_id = mapped_column(sa.Integer(), sa.ForeignKey("groups.id"), primary_key=True)
    user_id = mapped_column(sa.Integer(), sa.ForeignKey("users.id"), primary_key=True)

    user: Mapped[User] = relationship("User", back_populates="group_memberships")
    group: Mapped[Group] = relationship("Group", back_populates="memberships")


class Item(Model):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(sa.Integer(), primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.Integer(), sa.ForeignKey("users.id"), nullable=False)

    notes: Mapped[t.List["Note"]] = relationship(
        "Note", collection_class=attribute_keyed_dict("keyword")
    )


class Note(Model):
    __tablename__ = "notes"

    id: Mapped[int] = mapped_column(sa.Integer(), primary_key=True)
    item_id: Mapped[int] = mapped_column(sa.Integer(), sa.ForeignKey("items.id"), nullable=False)
    keyword: Mapped[str] = mapped_column(sa.String())


@pytest.fixture()
def db() -> t.Generator[Database, None, None]:
    uri = "sqlite://"
    echo = False
    _db = Database(uri, model_class=Model, echo=echo)
    _db.create_all()
    yield _db
    _db.close()


@pytest_asyncio.fixture()
async def async_db() -> t.AsyncGenerator[AsyncDatabase, None]:
    uri = "sqlite+aiosqlite://"
    echo = False
    _db = AsyncDatabase(uri, model_class=Model, echo=echo)
    await _db.create_all()
    yield _db
    await _db.close()


@pytest.fixture()
def filedb(tmp_path: Path) -> t.Generator[Database, None, None]:
    dbpath = tmp_path / "test.db"
    uri = f"sqlite:///{dbpath}"
    _db = Database(uri, model_class=Model)
    _db.create_all()
    yield _db
    _db.close()


@pytest_asyncio.fixture()
async def async_filedb(tmp_path: Path) -> t.AsyncGenerator[AsyncDatabase, None]:
    dbpath = tmp_path / "test_async.db"
    uri = f"sqlite+aiosqlite:///{dbpath}"
    _db = AsyncDatabase(uri, model_class=Model)
    await _db.create_all()
    yield _db
    await _db.close()


def random_alpha(n=8):
    """Return random set of ASCII letters with length `n`."""
    return "".join(random.SystemRandom().choice(string.ascii_letters) for _ in range(n))


def is_subdict(subset: dict, superset: dict) -> bool:
    """Return whether one dict is a subset of another."""
    if isinstance(subset, dict):
        return all(
            key in superset and is_subdict(val, superset[key]) for key, val in subset.items()
        )

    if isinstance(subset, list) and isinstance(superset, list) and len(superset) == len(subset):
        return all(is_subdict(subitem, superset[idx]) for idx, subitem in enumerate(subset))

    # Assume that subset is a plain value if none of the above match.
    return subset == superset


def create_users(db: Database, count: int = 3, overrides: t.Optional[dict] = None) -> t.List[User]:
    if overrides is None:
        overrides = {}
    users = [User(id=i, **overrides) for i in range(1, count + 1)]
    with db.begin(expire_on_commit=False) as session:
        session.add_all(users)
    return users


async def async_create_users(
    async_db: AsyncDatabase, count: int = 3, overrides: t.Optional[dict] = None
) -> t.List[User]:
    if overrides is None:
        overrides = {}
    users = [User(id=i, **overrides) for i in range(1, count + 1)]
    async with async_db.begin(expire_on_commit=False) as session:
        session.add_all(users)
    return users


def create_model_collection() -> dict:
    @as_declarative()
    class Model(ModelBase):
        pass

    class A(Model):
        __tablename__ = "a"
        id = sa.Column(sa.types.Integer(), primary_key=True)

    class B(Model):
        __tablename__ = "b"
        id = sa.Column(sa.types.Integer(), primary_key=True)

    class C(Model):
        __tablename__ = "c"
        id = sa.Column(sa.types.Integer(), primary_key=True)

    models = [A, B, C]
    return {"model_class": Model, "models": models, "tables": [model.__table__ for model in models]}
