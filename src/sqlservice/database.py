"""
Database
--------

The database module that provides a unified class interface to SQLAlchemy engine connection and
session objects.
"""

from collections.abc import Mapping
from contextlib import contextmanager
import typing as t

from sqlalchemy import Table, create_engine, select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import DeclarativeMeta, sessionmaker
from sqlalchemy.pool import Pool
from sqlalchemy.sql.schema import MetaData

from .model import ModelBase, declarative_base
from .session import Session


try:
    from sqlalchemy.future import Connection, Engine
except ImportError:  # pragma: no cover
    from sqlalchemy.engine import Connection, Engine


class DatabaseSettings(Mapping):
    """Container class for :class:`SQLClient` configuration options for SQLAlchemy engine and
    session objects."""

    def __init__(
        self,
        uri: str,
        *,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        isolation_level: t.Optional[str] = None,
        pool_size: t.Optional[int] = None,
        pool_timeout: t.Optional[t.Union[int, float]] = None,
        pool_recycle: t.Optional[t.Union[int, float]] = None,
        pool_pre_ping: t.Optional[bool] = None,
        poolclass: t.Optional[t.Type[Pool]] = None,
        max_overflow: t.Optional[int] = None,
        paramstyle: t.Optional[str] = None,
        encoding: t.Optional[str] = None,
        echo: t.Optional[bool] = None,
        echo_pool: t.Optional[bool] = None,
        engine_options: t.Optional[t.Dict[str, t.Any]] = None,
        session_options: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.uri = uri
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.isolation_level = isolation_level
        self.pool_size = pool_size
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping
        self.poolclass = poolclass
        self.max_overflow = max_overflow
        self.paramstyle = paramstyle
        self.encoding = encoding
        self.echo = echo
        self.echo_pool = echo_pool
        self.engine_options = engine_options or {}
        self.session_options = session_options or {}

    def get_engine_options(self) -> t.Dict[str, t.Any]:
        """Return dictionary of options for configuring a SQLAlchemy engine."""
        opts = {
            "echo": self.echo,
            "echo_pool": self.echo_pool,
            "encoding": self.encoding,
            "isolation_level": self.isolation_level,
            "pool_size": self.pool_size,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "poolclass": self.poolclass,
            "max_overflow": self.max_overflow,
            "paramstyle": self.paramstyle,
            **self.engine_options,
        }
        return _omit_none(opts)

    def get_session_options(self) -> t.Dict[str, t.Any]:
        """Return dictionary of options for configuring a SQLAlchemy session."""
        opts = {
            "autoflush": self.autoflush,
            "expire_on_commit": self.expire_on_commit,
            **self.session_options,
        }
        return _omit_none(opts)

    def __getitem__(self, item):
        return self.__dict__[item]

    def __iter__(self) -> t.Iterator[str]:
        return iter(self.__dict__)

    def __len__(self) -> int:
        return len(self.__dict__)


class Database:
    """
    Database engine and ORM session management class.

    The primary purpose of this class is to provide SQLAlchemy database connections and ORM sessions
    via a single interface.

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

    Args:
        uri: Database connection URI in RFC 1738 spec format. See
            https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls.

    Keyword Arguments:
        model_class: Declarative base class for SQLAlchemy models. If not given, then a default
            base class will be generated.
        session_class: Session class to use for session factory.
        autoflush: When ``True``, all query operations will issue a ``flush()`` call to the
            ``Session`` before proceeding. This is a convenience feature so that ``flush()`` need
            not be called repeatedly in order for database queries to retrieve results. Defaults to
            ``True``.
        expire_on_commit: When ``True`` all instances will be fully expired after each ``commit()``,
            so that all attribute/object access after a completed transaction will load from the
            most recent database state. Defaults to ``True``.
        isolation_level: String parameter interpreted by various dialects in order to affect the
            transaction isolation level of the database connection. The parameter essentially
            accepts some subset of these string arguments: ``"SERIALIZABLE"``,
            ``"REPEATABLE READ"``, ``"READ COMMITTED"``, ``"READ UNCOMMITTED"`` and
            ``"AUTOCOMMIT"``. Behavior here varies per backend, and individual dialects should be
            consulted directly. Defaults to ``None``.
        pool_size: The size of the database pool. Defaults to the engine's default (usually ``5``).
        pool_timeout: Specifies the connection timeout for the pool. Defaults to ``10``.
        pool_recycle: Number of seconds after which a connection is automatically recycled.
        pool_pre_ping: When ``True` will enable SQLAlchemy's connection pool “pre-ping” feature that
            tests connections for liveness upon each checkout. Defaults to ``False``.
        poolclass: A `sqlalchemy.pool.Pool` subclass, which will be used to create a connection pool
            instance using the connection parameters given in the URL.
        max_overflow: Controls the number of connections that can be created after the pool reached
            its maximum size. When those additional connections are returned to the pool, they are
            disconnected and discarded.
        paramstyle: The paramstyle to use when rendering bound parameters. Defaults to ``None``
            which uses the one recommended by the DBAPI. When given it should be one of ``"qmark"``,
            ``"numeric"``, ``"named"``, ``"format"``, or ``"pyformat"``,
        encoding: The string encoding used by SQLAlchemy for string encode/decode operations which
            occur within SQLAlchemy, outside of the DBAPI. Defaults to `utf-8`.
        echo: When ``True`` have SQLAlchemy log all SQL statements. Defaults to ``False``.
        echo_pool: When ``True`` have SQLAlchemy log all log all checkouts/checkins of the
            connection pool. Defaults to ``False``.
        engine_options: Additional engine options use when creating the database engine.
        session_options: Additional session options use when creating the database session.
    """

    def __init__(
        self,
        uri: str,
        *,
        model_class: t.Optional[t.Union[t.Type[ModelBase], DeclarativeMeta]] = None,
        session_class: t.Type[Session] = Session,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        isolation_level: t.Optional[str] = None,
        pool_size: t.Optional[int] = None,
        pool_timeout: t.Optional[t.Union[int, float]] = None,
        pool_recycle: t.Optional[t.Union[int, float]] = None,
        pool_pre_ping: t.Optional[bool] = None,
        poolclass: t.Optional[t.Type[Pool]] = None,
        max_overflow: t.Optional[int] = None,
        paramstyle: t.Optional[str] = None,
        encoding: t.Optional[str] = None,
        echo: t.Optional[bool] = None,
        echo_pool: t.Optional[bool] = None,
        engine_options: t.Optional[t.Dict[str, t.Any]] = None,
        session_options: t.Optional[t.Dict[str, t.Any]] = None,
    ):
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
            encoding=encoding,
            echo=echo,
            echo_pool=echo_pool,
            engine_options=engine_options,
            session_options=session_options,
        )
        self.model_class = model_class
        self.session_class = session_class
        self.url: URL = make_url(uri)
        self.engine = self.create_engine()
        self.sessionmaker = self.create_sessionmaker()

    @property
    def uri(self) -> str:
        """Return database uri."""
        return str(self.url)

    @property
    def name(self) -> str:
        """Return engine's database name."""
        return self.url.database

    @property
    def metadata(self) -> MetaData:
        """Return model metadata."""
        return self.model_class.metadata

    @property
    def tables(self) -> t.Dict[str, Table]:
        """Return dictionary of table instances corresponding to ORM model classes indexed by table
        name."""
        return dict(self.metadata.tables)

    @property
    def models(self) -> t.Dict[str, DeclarativeMeta]:
        """Return dictionary of ORM model classes indexed by class' module path."""
        return {
            f"{mapper.class_.__module__}.{mapper.class_.__name__}": mapper.class_
            for mapper in self.model_class.registry.mappers
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self.url)!r})"

    def create_engine(self) -> Engine:
        """Return instance of SQLAlchemy engine using database settings."""
        return create_engine(self.url, future=True, **self.settings.get_engine_options())

    def create_sessionmaker(self) -> sessionmaker:
        """Return instance of SQLAlchemy sessionmaker using database settings."""
        return sessionmaker(
            self.engine,
            class_=self.session_class,
            future=True,
            **self.settings.get_session_options(),
        )

    def create_all(self, **kwargs: t.Any) -> None:
        """Create all database schema defined in declarative base class."""
        self.metadata.create_all(self.engine, **kwargs)

    def drop_all(self, **kwargs: t.Any) -> None:
        """Drop all database schema defined in declarative base class."""
        self.metadata.drop_all(self.engine, **kwargs)

    def reflect(self, **kwargs: t.Any) -> None:
        """Reflect database schema from database connection."""
        self.metadata.reflect(self.engine, **kwargs)

    def session(
        self,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> Session:
        """Return new session instance using database settings."""
        if autoflush is not None:
            kwargs["autoflush"] = autoflush
        if expire_on_commit is not None:
            kwargs["expire_on_commit"] = expire_on_commit
        return self.sessionmaker(**kwargs)

    @contextmanager
    def begin(
        self,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> t.Iterator[Session]:
        """
        Context manager that begins a new session transaction.

        Commit and rollback logic will be handled automatically.
        """
        session = self.session(autoflush=autoflush, expire_on_commit=expire_on_commit, **kwargs)
        with session.begin():  # pylint: disable=no-member
            yield session

    def connect(self) -> Connection:
        """Return new connection instance using database settings."""
        return self.engine.connect()

    def close(self):
        """Close engine connection."""
        self.engine.dispose()

    def ping(self) -> bool:
        """Return whether database can be accessed."""
        with self.connect() as conn:
            try:
                conn.scalar(select(1))
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
                    conn.scalar(select(1))
                else:
                    raise
        return True


def _omit_none(obj: dict) -> dict:
    return {key: value for key, value in obj.items() if value is not None}
