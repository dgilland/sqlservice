"""
Database Settings
-----------------

TODO
"""

import typing as t
from typing import Mapping

from sqlalchemy.engine import URL, make_url
from sqlalchemy.pool import Pool


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
        execution_options: t.Optional[t.Dict[str, t.Any]] = None,
        echo: t.Optional[t.Union[bool, str]] = None,
        echo_pool: t.Optional[t.Union[bool, str]] = None,
        engine_options: t.Optional[t.Dict[str, t.Any]] = None,
        session_options: t.Optional[t.Dict[str, t.Any]] = None
    ):
        self.url: URL = make_url(uri)
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
        self.execution_options = execution_options
        self.echo = echo
        self.echo_pool = echo_pool
        self.engine_options = engine_options or {}
        self.session_options = session_options or {}

    def get_engine_options(self) -> t.Dict[str, t.Any]:
        """Return dictionary of options for configuring a SQLAlchemy engine."""
        opts = {
            "echo": self.echo,
            "echo_pool": self.echo_pool,
            "isolation_level": self.isolation_level,
            "pool_size": self.pool_size,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "poolclass": self.poolclass,
            "max_overflow": self.max_overflow,
            "paramstyle": self.paramstyle,
            "execution_options": self.execution_options,
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


def _omit_none(obj: dict) -> dict:
    return {key: value for key, value in obj.items() if value is not None}
