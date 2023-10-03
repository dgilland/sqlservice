from contextlib import contextmanager
from pathlib import Path
import typing as t

import mock
import pytest
from pytest import param
import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import QueuePool, SingletonThreadPool

from sqlservice import Database, Session

from .fixtures import create_model_collection


parametrize = pytest.mark.parametrize


def create_engine_mock(scalar: t.Optional[mock.Mock] = None) -> mock.MagicMock:
    mocked_engine = mock.MagicMock(spec=Engine)
    mocked_engine.connect.return_value = mock.MagicMock(spec=Connection)
    if scalar:
        mocked_engine.connect.return_value.__enter__.return_value.scalar = scalar
    return mocked_engine


@contextmanager
def mock_db(db: Database, engine: t.Optional[mock.MagicMock] = None):
    if engine is None:
        engine = create_engine_mock()

    with mock.patch.object(db, "engine", new=engine):
        yield


def test_database_ping__returns_true_on_success(filedb: Database):
    assert filedb.ping() is True


def test_database_ping__retries_on_invalidated_connection(filedb: Database):
    mocked_scalar = mock.Mock(
        side_effect=[DBAPIError(None, None, Exception(), connection_invalidated=True), 1]
    )
    mocked_engine = create_engine_mock(scalar=mocked_scalar)

    with mock_db(filedb, engine=mocked_engine):
        assert filedb.ping() is True


def test_database_ping__raises_exception_on_failure(db: Database):
    mocked_scalar = mock.Mock(side_effect=DBAPIError(None, None, Exception()))
    mocked_engine = create_engine_mock(scalar=mocked_scalar)

    with mock_db(db, engine=mocked_engine):
        with pytest.raises(DBAPIError):
            db.ping()


def test_database_ping__raises_when_invalidated_connection_retry_fails(db: Database):
    mocked_scalar = mock.Mock(
        side_effect=DBAPIError(None, None, Exception(), connection_invalidated=True)
    )
    mocked_engine = create_engine_mock(scalar=mocked_scalar)

    with mock_db(db, engine=mocked_engine):
        with pytest.raises(DBAPIError):
            db.ping()


def test_database_create_all():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    db = Database("sqlite://", model_class=model_class)
    db.create_all()

    with db.connect() as conn:
        result = conn.execute(sa.text("SELECT name FROM sqlite_master"))
        table_names = result.scalars().all()

    assert len(table_names) == len(model_tables)
    for table_name in table_names:
        assert table_name in db.tables


def test_database_drop_all():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    db = Database("sqlite://", model_class=model_class)
    conn = db.connect()
    db.create_all()

    count_tables = sa.text("SELECT COUNT(name) FROM sqlite_master")
    assert conn.execute(count_tables).scalar_one() == len(model_tables)
    db.drop_all()
    assert conn.execute(count_tables).scalar_one() == 0


def test_database_reflect(tmp_path: Path):
    db_file = tmp_path / "reflect.db"
    uri = f"sqlite:///{db_file}"

    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    Database(uri, model_class=model_class).create_all()
    db = Database(uri)
    assert len(db.tables) == 0

    db.reflect()

    assert len(db.tables) == len(model_tables)
    model_tables_by_name = {table.name: table for table in model_tables}

    for table_name, _table in db.tables.items():
        assert table_name in model_tables_by_name


def test_database_tables():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    db = Database("sqlite://", model_class=model_class)
    assert len(db.tables) == len(model_tables)

    for table_name, table in db.tables.items():
        assert table in model_tables
        model_table = model_tables[model_tables.index(table)]
        assert model_table.name == table_name


def test_database_models():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    models = model_collection["models"]

    db = Database("sqlite://", model_class=model_class)
    assert len(db.models) == len(models)

    for _model_name, model in db.models.items():
        orig_model = models[models.index(model)]
        assert model is orig_model


def test_database_session__returns_new_session_object(db: Database):
    session = db.session()
    assert isinstance(session, Session)
    assert not session.in_transaction()

    other_session = db.session()
    assert other_session is not session


@parametrize(
    "option, default_value, override_value",
    [
        param("autoflush", True, False),
        param("expire_on_commit", True, False),
        param("twophase", False, True),
    ],
)
def test_database_session__can_override_default_options(
    db: Database, option, default_value, override_value
):
    session = db.session()
    option_value = getattr(session, option)
    assert (
        option_value == default_value
    ), f"Expected session.{option} to be {default_value!r}, not {option_value!r}"

    session = db.session(**{option: override_value})
    option_value = getattr(session, option)
    assert (
        option_value == override_value
    ), f"Expected session.{option} to be {override_value!r}, not {option_value!r}"


def test_database_begin__starts_new_transaction(db: Database):
    with db.begin() as session:
        assert isinstance(session, Session)
        assert session.in_transaction()

        with db.begin() as other_session:
            assert other_session is not session


def test_database_close__disposes_engine(filedb: Database):
    with mock_db(filedb):
        filedb.close()
        assert filedb.engine.dispose.called  # type: ignore


@parametrize(
    "uri, expected_uri",
    [
        param("sqlite://", "sqlite://"),
        param("sqlite:///db.db", "sqlite:///db.db"),
        param(
            "sqlite:///user:password@db.db:1234?foo=bar",
            "sqlite:///user:password@db.db:1234?foo=bar",
        ),
    ],
)
def test_database_uri(uri: str, expected_uri: str):
    db = Database(uri)
    assert db.uri == expected_uri


@parametrize(
    "uri, expected_name",
    [
        param("sqlite://", None),
        param("sqlite:///db.db", "db.db"),
    ],
)
def test_database_name(uri: str, expected_name: t.Optional[str]):
    db = Database(uri)
    assert db.name == expected_name


@parametrize(
    "uri, rep",
    [
        param("sqlite://", "Database('sqlite://')"),
        param("sqlite:///db.db", "Database('sqlite:///db.db')"),
    ],
)
def test_database_repr(uri: str, rep: str):
    db = Database(uri)
    assert repr(db) == rep


@parametrize(
    "key, value, kind",
    [
        param("autoflush", True, "session"),
        param("expire_on_commit", True, "session"),
        param("isolation_level", "SERIALIZABLE", "engine"),
        param("pool_size", 1, "engine"),
        param("pool_timeout", 30, "engine"),
        param("pool_recycle", True, "engine"),
        param("pool_pre_ping", True, "engine"),
        param("poolclass", SingletonThreadPool, "engine"),
        param("max_overflow", 5, "engine"),
        param("paramstyle", "named", "engine"),
        param("execution_options", {}, "engine"),
        param("echo", True, "engine"),
        param("echo_pool", True, "engine"),
    ],
)
def test_database_settings(key: str, value: t.Any, kind: str):
    settings = {key: value}
    # NOTE: Using poolclass so that pool_* options can be used with sqlite.
    settings.setdefault("poolclass", QueuePool)
    db = Database("sqlite://", **settings)

    assert getattr(db.settings, key) == value
    assert db.settings[key] == value
    assert dict(db.settings)[key] == value

    if kind == "session":
        assert db.settings.get_session_options()[key] == value
    elif kind == "engine":
        assert db.settings.get_engine_options()[key] == value
    else:
        raise RuntimeError(f"kind must be one of 'session' or 'engine', not {kind!r}")


def test_database_settings__accepts_session_options_dict():
    session_options = {"autoflush": False}
    other_options = {"expire_on_commit": True, "autoflush": True}
    expected_options = {**other_options, **session_options}

    db = Database("sqlite://", session_options=session_options, **other_options)
    assert db.settings.get_session_options() == expected_options


def test_database_settings__accepts_engine_options_dict():
    engine_options = {"echo": True}
    other_options = {"echo": False, "echo_pool": True}
    expected_options = {**other_options, **engine_options}

    db = Database("sqlite://", engine_options=engine_options, **other_options)
    assert db.settings.get_engine_options() == expected_options


def test_database_settings__len():
    db = Database("sqlite://")
    assert len(db.settings) == len(db.settings.__dict__)
