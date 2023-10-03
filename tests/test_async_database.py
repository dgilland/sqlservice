from contextlib import contextmanager
from pathlib import Path
import typing as t

import mock
import pytest
from pytest import param
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.pool import QueuePool, SingletonThreadPool

from sqlservice import AsyncDatabase, AsyncSession

from .fixtures import create_model_collection


parametrize = pytest.mark.parametrize


def create_async_engine_mock(scalar: t.Optional[mock.Mock] = None) -> mock.MagicMock:
    mocked_engine = mock.MagicMock(spec=AsyncEngine)
    mocked_engine.connect.return_value = mock.MagicMock(spec=AsyncConnection)
    if scalar:
        mocked_engine.connect.return_value.__aenter__.return_value.scalar = scalar
    return mocked_engine


@contextmanager
def mock_async_db(db: AsyncDatabase, engine: t.Optional[mock.MagicMock] = None):
    if engine is None:
        engine = create_async_engine_mock()

    with mock.patch.object(db, "engine", new=engine):
        yield


async def test_async_database_ping__returns_true_on_success(async_filedb: AsyncDatabase):
    assert await async_filedb.ping() is True


async def test_async_database_ping__retries_on_invalidated_connection(async_filedb: AsyncDatabase):
    mocked_scalar = mock.AsyncMock(
        side_effect=[DBAPIError(None, None, Exception(), connection_invalidated=True), 1]
    )
    mocked_engine = create_async_engine_mock(scalar=mocked_scalar)

    with mock_async_db(async_filedb, engine=mocked_engine):
        assert await async_filedb.ping() is True


async def test_async_database_ping__raises_exception_on_failure(async_db: AsyncDatabase):
    mocked_scalar = mock.AsyncMock(side_effect=DBAPIError(None, None, Exception()))
    mocked_engine = create_async_engine_mock(scalar=mocked_scalar)

    with mock_async_db(async_db, engine=mocked_engine):
        with pytest.raises(DBAPIError):
            await async_db.ping()


async def test_async_database_ping__raises_when_invalidated_connection_retry_fails(
    async_db: AsyncDatabase,
):
    mocked_scalar = mock.AsyncMock(
        side_effect=DBAPIError(None, None, Exception(), connection_invalidated=True)
    )
    mocked_engine = create_async_engine_mock(scalar=mocked_scalar)

    with mock_async_db(async_db, engine=mocked_engine):
        with pytest.raises(DBAPIError):
            await async_db.ping()


async def test_async_database_create_all():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    async_db = AsyncDatabase("sqlite+aiosqlite://", model_class=model_class)
    await async_db.create_all()

    async with async_db.connect() as conn:
        result = await conn.execute(sa.text("SELECT name FROM sqlite_master"))
        table_names = result.scalars().all()

    assert len(table_names) == len(model_tables)
    for table_name in table_names:
        assert table_name in async_db.tables


async def test_async_database_drop_all():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    async_db = AsyncDatabase("sqlite+aiosqlite://", model_class=model_class)
    conn = await async_db.connect()
    await async_db.create_all()

    count_tables = sa.text("SELECT COUNT(name) FROM sqlite_master")
    assert (await conn.execute(count_tables)).scalar_one() == len(model_tables)
    await async_db.drop_all()
    assert (await conn.execute(count_tables)).scalar_one() == 0


async def test_async_database_reflect(tmp_path: Path):
    db_file = tmp_path / "reflect_async.db"
    uri = f"sqlite+aiosqlite:///{db_file}"

    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    await AsyncDatabase(uri, model_class=model_class).create_all()
    async_db = AsyncDatabase(uri)
    assert len(async_db.tables) == 0

    await async_db.reflect()

    assert len(async_db.tables) == len(model_tables)
    model_tables_by_name = {table.name: table for table in model_tables}

    for table_name, _table in async_db.tables.items():
        assert table_name in model_tables_by_name


def test_async_database_tables():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    model_tables = model_collection["tables"]

    async_db = AsyncDatabase("sqlite+aiosqlite://", model_class=model_class)
    assert len(async_db.tables) == len(model_tables)

    for table_name, table in async_db.tables.items():
        assert table in model_tables
        model_table = model_tables[model_tables.index(table)]
        assert model_table.name == table_name


def test_async_database_models():
    model_collection = create_model_collection()
    model_class = model_collection["model_class"]
    models = model_collection["models"]

    async_db = AsyncDatabase("sqlite+aiosqlite://", model_class=model_class)
    assert len(async_db.models) == len(models)

    for _model_name, model in async_db.models.items():
        orig_model = models[models.index(model)]
        assert model is orig_model


def test_async_database_session__returns_new_session_object(async_db: AsyncDatabase):
    session = async_db.session()
    assert isinstance(session, AsyncSession)
    assert not session.in_transaction()

    other_session = async_db.session()
    assert other_session is not session


@parametrize(
    "option, default_value, override_value",
    [
        param("autoflush", True, False),
        param("expire_on_commit", False, True),
        param("twophase", False, True),
    ],
)
def test_async_database_session__can_override_default_options(
    async_db: AsyncDatabase, option, default_value, override_value
):
    session = async_db.session()
    option_value = getattr(session.sync_session, option)
    assert (
        option_value == default_value
    ), f"Expected session.{option} to be {default_value!r}, not {option_value!r}"

    session = async_db.session(**{option: override_value})
    option_value = getattr(session.sync_session, option)
    assert (
        option_value == override_value
    ), f"Expected session.{option} to be {override_value!r}, not {option_value!r}"


async def test_async_database_begin__starts_new_transaction(async_db: AsyncDatabase):
    async with async_db.begin() as session:
        assert isinstance(session, AsyncSession)
        assert session.in_transaction()

        async with async_db.begin() as other_session:
            assert other_session is not session


async def test_async_database_close__disposes_engine(async_filedb: AsyncDatabase):
    with mock_async_db(async_filedb):
        await async_filedb.close()
        assert async_filedb.engine.dispose.called  # type: ignore


@parametrize(
    "uri, expected_uri",
    [
        param("sqlite+aiosqlite://", "sqlite+aiosqlite://"),
        param("sqlite+aiosqlite:///db.db", "sqlite+aiosqlite:///db.db"),
        param(
            "sqlite+aiosqlite:///user:password@db.db:1234?foo=bar",
            "sqlite+aiosqlite:///user:password@db.db:1234?foo=bar",
        ),
    ],
)
def test_async_database_uri(uri: str, expected_uri: str):
    async_db = AsyncDatabase(uri)
    assert async_db.uri == expected_uri


@parametrize(
    "uri, expected_name",
    [
        param("sqlite+aiosqlite://", None),
        param("sqlite+aiosqlite:///db.db", "db.db"),
    ],
)
def test_async_database_name(uri: str, expected_name: t.Optional[str]):
    async_db = AsyncDatabase(uri)
    assert async_db.name == expected_name


@parametrize(
    "uri, rep",
    [
        param("sqlite+aiosqlite://", "AsyncDatabase('sqlite+aiosqlite://')"),
        param("sqlite+aiosqlite:///db.db", "AsyncDatabase('sqlite+aiosqlite:///db.db')"),
    ],
)
def test_async_database_repr(uri: str, rep: str):
    async_db = AsyncDatabase(uri)
    assert repr(async_db) == rep


@parametrize(
    "key, value, kind",
    [
        param("autoflush", True, "session"),
        param("expire_on_commit", False, "session"),
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
def test_async_database_settings(key: str, value: t.Any, kind: str):
    settings = {key: value}
    # NOTE: Using poolclass so that pool_* options can be used with sqlite.
    settings.setdefault("poolclass", QueuePool)
    async_db = AsyncDatabase("sqlite+aiosqlite://", **settings)

    assert getattr(async_db.settings, key) == value
    assert async_db.settings[key] == value
    assert dict(async_db.settings)[key] == value

    if kind == "session":
        assert async_db.settings.get_session_options()[key] == value
    elif kind == "engine":
        assert async_db.settings.get_engine_options()[key] == value
    else:
        raise RuntimeError(f"kind must be one of 'session' or 'engine', not {kind!r}")


def test_async_database_settings__accepts_session_options_dict():
    session_options = {"autoflush": False}
    other_options = {"expire_on_commit": True, "autoflush": True}
    expected_options = {**other_options, **session_options}

    async_db = AsyncDatabase(
        "sqlite+aiosqlite://", session_options=session_options, **other_options
    )
    assert async_db.settings.get_session_options() == expected_options


def test_async_database_settings__accepts_engine_options_dict():
    engine_options = {"echo": True}
    other_options = {"echo": False, "echo_pool": True}
    expected_options = {**other_options, **engine_options}

    async_db = AsyncDatabase("sqlite+aiosqlite://", engine_options=engine_options, **other_options)
    assert async_db.settings.get_engine_options() == expected_options


def test_async_database_settings__len():
    async_db = AsyncDatabase("sqlite+aiosqlite://")
    assert len(async_db.settings) == len(async_db.settings.__dict__)
