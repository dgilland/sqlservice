"""
Async Database
--------------

The async_database module provides an asyncio version of :class:`sqlservice.database.Database`.
"""

from contextlib import asynccontextmanager
import typing as t

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import Pool

from .async_session import AsyncSession
from .database_abc import DatabaseABC
from .database_settings import DatabaseSettings
from .model import ModelBase, declarative_base


class AsyncDatabase(DatabaseABC):
    """
    Asynchronous Database engine and ORM session management class.

    The primary purpose of this class is to provide SQLAlchemy database connections and ORM sessions
    via a single interface that is compatible with asyncio.

    Connections and session are provided using the factory methods:

    - :meth:`.connect` - Return a new database engine connection object.
    - :meth:`.session` - Return a new database session object.
    - :meth:`.begin` - Start a new database session transaction and return session object.

    In addition, model metadata operations are available:

    - :meth:`.create_all` - Create all database tables defined on base ORM model class.
    - :meth:`.drop_all` - Drop all database tables defined on declarative base ORM model class.
    - :meth:`.reflect` - Reflect database schema from database connection.

    Dictionary access to the underlying table objects and model classes from the declarative base
    class are at:

    - :attr:`.tables`
    - :attr:`.models`

    Note:
        This class uses the new
        `2.0 style <https://docs.sqlalchemy.org/en/14/glossary.html#term-2.0-style>`_ SQLAlchemy
        API. Learn more at
        `SQLAlchemy 1.4 / 2.0 Tutorial <https://docs.sqlalchemy.org/en/14/tutorial/>`_.
    """

    engine: AsyncEngine

    def __init__(
        self,
        uri: str,
        *,
        model_class: t.Optional[t.Type[ModelBase]] = None,
        session_class: t.Type[AsyncSession] = AsyncSession,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = False,  # expire_on_commit=False for asyncio
        isolation_level: t.Optional[str] = None,
        pool_size: t.Optional[int] = None,
        pool_timeout: t.Optional[t.Union[int, float]] = None,
        pool_recycle: t.Optional[t.Union[int, float]] = None,
        pool_pre_ping: t.Optional[bool] = None,
        poolclass: t.Optional[t.Type[Pool]] = None,
        max_overflow: t.Optional[int] = None,
        paramstyle: t.Optional[str] = None,
        execution_options: t.Optional[t.Dict[str, t.Any]] = None,
        echo: t.Optional[t.Union[bool, str]] = None,
        echo_pool: t.Optional[t.Union[bool, str]] = None,
        engine_options: t.Optional[t.Dict[str, t.Any]] = None,
        session_options: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        """
        Args:
            uri: Database connection URI in RFC 1738 spec format. See
                https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls.

        Keyword Arguments:
            model_class: Declarative base class for SQLAlchemy models. If not given, then a default
                base class will be generated.
            session_class: Session class to use for session factory.
            autoflush: When ``True``, all query operations will issue a ``flush()`` call to the
                ``Session`` before proceeding. This is a convenience feature so that ``flush()``
                need not be called repeatedly in order for database queries to retrieve results.
                Defaults to ``True``.
            expire_on_commit: When ``False`` this prevents all instances from expiring after each
                ``commit()`` so that there will be no implicit I/O as a result of lazy-loading which
                could cause issues under asyncio. Defaults to ``False``.
            isolation_level: String parameter interpreted by various dialects in order to affect the
                transaction isolation level of the database connection. The parameter essentially
                accepts some subset of these string arguments: ``"SERIALIZABLE"``,
                ``"REPEATABLE READ"``, ``"READ COMMITTED"``, ``"READ UNCOMMITTED"`` and
                ``"AUTOCOMMIT"``. Behavior here varies per backend, and individual dialects should
                be consulted directly. Defaults to ``None``.
            pool_size: The size of the database pool. Defaults to the engine's default (usually
                ``5``).
            pool_timeout: Specifies the connection timeout for the pool. Defaults to ``10``.
            pool_recycle: Number of seconds after which a connection is automatically recycled.
            pool_pre_ping: When ``True` will enable SQLAlchemy's connection pool “pre-ping” feature
                that tests connections for liveness upon each checkout. Defaults to ``False``.
            poolclass: A `sqlalchemy.pool.Pool` subclass, which will be used to create a connection
                pool instance using the connection parameters given in the URL.
            max_overflow: Controls the number of connections that can be created after the pool
                reached its maximum size. When those additional connections are returned to the
                pool, they are disconnected and discarded.
            paramstyle: The paramstyle to use when rendering bound parameters. Defaults to ``None``
                which uses the one recommended by the DBAPI. When given it should be one of
                ``"qmark"``, ``"numeric"``, ``"named"``, ``"format"``, or ``"pyformat"``,
            execution_options: Dictionary of execution options which will be applied to all
                connections.
            echo: When ``True`` have SQLAlchemy log all SQL statements. When ``"debug"`` the logging
                will include result rows. Defaults to ``False``.
            echo_pool: When ``True`` have SQLAlchemy log all checkouts/checkins of the connection
                pool. When ``"debug"`` the logging will include pool checkouts and checkins.
                Defaults to ``False``.
            engine_options: Additional engine options use when creating the database engine.
            session_options: Additional session options use when creating the database session.
        """
        if model_class is None:
            model_class = declarative_base()

        self.settings = DatabaseSettings(
            uri=uri,
            autoflush=autoflush,
            expire_on_commit=expire_on_commit,
            isolation_level=isolation_level,
            pool_size=pool_size,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
            poolclass=poolclass,
            max_overflow=max_overflow,
            paramstyle=paramstyle,
            execution_options=execution_options,
            echo=echo,
            echo_pool=echo_pool,
            engine_options=engine_options,
            session_options=session_options,
        )
        self.model_class = model_class
        self.session_class = session_class
        self.engine = self.create_engine()
        self.sessionmaker = self.create_sessionmaker()

    def create_engine(self) -> AsyncEngine:
        """Return instance of SQLAlchemy async-engine using database settings."""
        return create_async_engine(self.url, **self.settings.get_engine_options())

    async def create_all(self, **kwargs: t.Any) -> None:
        """Create all database schema defined in declarative base class."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all, **kwargs)

    async def drop_all(self, **kwargs: t.Any) -> None:
        """Drop all database schema defined in declarative base class."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.drop_all, **kwargs)

    async def reflect(self, **kwargs: t.Any) -> None:
        """Reflect database schema from database connection."""
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.reflect, **kwargs)

    def session(
        self,
        *,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> AsyncSession:
        """Return new async-session instance using database settings."""
        if autoflush is not None:
            kwargs["autoflush"] = autoflush
        if expire_on_commit is not None:
            kwargs["expire_on_commit"] = expire_on_commit
        return self.sessionmaker(**kwargs)

    @asynccontextmanager
    async def begin(
        self,
        *,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> t.AsyncGenerator[AsyncSession, None]:
        """
        Async context manager that begins a new session transaction.

        Commit and rollback logic will be handled automatically.
        """
        session = self.session(autoflush=autoflush, expire_on_commit=expire_on_commit, **kwargs)
        async with session.begin():
            yield session

    def connect(self) -> AsyncConnection:
        """Return new async-connection instance using database settings."""
        return self.engine.connect()

    async def close(self) -> None:
        """Close engine connection."""
        await self.engine.dispose()

    async def ping(self) -> bool:
        """Return whether database can be accessed."""
        async with self.connect() as conn:
            try:
                await conn.scalar(select(1))
            except DBAPIError as exc:
                # Catch SQLAlchemy's DBAPIError, which is a wrapper for the DBAPI's exception. It
                # includes a "connection_invalidated" attribute which specifies if this connection
                # is a "disconnect" condition, which is based on inspection of the original
                # exception by the dialect in use.
                if exc.connection_invalidated:
                    # Run the same SELECT again. The connection will re-validate itself and
                    # establish a new connection. The disconnect detection here also causes the
                    # whole connection pool to be invalidated so that all stale connections are
                    # discarded.
                    await conn.scalar(select(1))
                else:
                    raise
        return True
