import os
import random
import string
from unittest import mock

from packaging import version
import pytest
import sqlalchemy as sa
from sqlalchemy.orm.collections import attribute_mapped_collection

from sqlservice import SQLClient, declarative_base


# Alias long method name.
parametrize = pytest.mark.parametrize


Model = declarative_base()
DupModel = declarative_base()


def unique_id():
    if not hasattr(unique_id, "_id"):
        unique_id._id = 0
    unique_id._id += 1
    return unique_id._id


class AModel(Model):
    __tablename__ = "test_a"

    id = sa.Column(sa.types.Integer(), primary_key=True, default=unique_id)
    name = sa.Column(sa.types.String(50))
    text = sa.Column(sa.types.String(50))
    label = sa.Column(sa.types.String(50))
    c_id = sa.Column(sa.types.Integer(), sa.ForeignKey("test_c.id"))

    c = sa.orm.relation("CModel")
    ds = sa.orm.relation("DModel")
    d_map = sa.orm.relation(
        "DModel", collection_class=attribute_mapped_collection("id"), viewonly=True
    )


class BModel(Model):
    __tablename__ = "test_b"

    id1 = sa.Column(sa.types.Integer(), primary_key=True, default=unique_id)
    id2 = sa.Column(sa.types.Integer(), primary_key=True, default=unique_id)
    name = sa.Column(sa.types.String(50))


class CModel(Model):
    __tablename__ = "test_c"

    id = sa.Column(sa.types.Integer(), primary_key=True, default=unique_id)
    name = sa.Column(sa.types.String(50))


class DModel(Model):
    __tablename__ = "test_d"

    id = sa.Column(sa.types.Integer(), primary_key=True, default=unique_id)
    name = sa.Column(sa.types.String(50))
    a_id = sa.Column(sa.types.Integer(), sa.ForeignKey("test_a.id"))


class DupAModel(DupModel):
    __tablename__ = "test_dup_a"
    id = sa.Column(sa.types.Integer(), primary_key=True)


@pytest.fixture
def db():
    config = {"SQL_DATABASE_URI": "sqlite://", "SQL_ECHO": False}
    _db = SQLClient(config, model_class=Model)
    _db.create_all()

    yield _db

    _db.disconnect()
    _db.drop_all()


@pytest.fixture
def filedb(tmpdir):
    dbpath = str(tmpdir.mkdir(random_alpha()).join("file.db"))
    config = {"SQL_DATABASE_URI": f"sqlite:///{dbpath}"}

    _filedb = SQLClient(config, model_class=Model)
    _filedb.create_all()

    yield _filedb

    _filedb.disconnect()
    os.remove(dbpath)


@pytest.fixture
def commit_event(db):
    """
    Test fixture that uses a ``MagicMock`` as an event listener for database session commits.

    Useful for counting commit events using ``commit_event.call_count``.
    """
    counter = mock.MagicMock()
    sa.event.listen(db.session, "after_commit", counter)
    return counter


@pytest.fixture
def rollback_event(db):
    """
    Test fixture that uses a ``MagicMock`` as an event listener for database session rollbacks.

    Useful for counting rollback events using ``rollback_event.call_count``.
    """
    counter = mock.MagicMock()
    sa.event.listen(db.session, "after_rollback", counter)
    return counter


class MyTestError(Exception):
    """Test error that can be used to raise inside a test function so that you can do pytest.raises
    and know that this exception was the one that was raised."""

    pass


def random_alpha(n=8):
    """Return random set of ASCII letters with length `n`."""
    return "".join(random.SystemRandom().choice(string.ascii_letters) for _ in range(n))


def is_subdict(subset, superset):
    """Return whether one dict is a subset of another."""
    if isinstance(subset, dict):
        return all(
            key in superset and is_subdict(val, superset[key]) for key, val in subset.items()
        )

    if isinstance(subset, list) and isinstance(superset, list) and len(superset) == len(subset):
        return all(is_subdict(subitem, superset[idx]) for idx, subitem in enumerate(subset))

    # Assume that subset is a plain value if none of the above match.
    return subset == superset


def is_sqlalchemy_14_plus():
    """Return whether the SQLAlchemy>=1.4."""
    sa_ver = version.parse(sa.__version__)
    sa_ver_14_plus = version.parse("1.4")
    return sa_ver >= sa_ver_14_plus


def skip_if_sqlalchemy_14_plus():
    """Skip test if SQLAlchemy>=1.4."""
    return pytest.mark.skipif(is_sqlalchemy_14_plus(), reason="Skip for SQLAlchemy>=1.4")


def skip_if_not_sqlalchemy_14_plus():
    """Skip test if SQLAlchemy<1.4."""
    return pytest.mark.skipif(not is_sqlalchemy_14_plus(), reason="Skip for SQLAlchemy<1.4")
