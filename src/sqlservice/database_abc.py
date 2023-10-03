"""
Database Abstract Base Class
----------------------------

TODO
"""

from abc import ABC, abstractmethod
import typing as t

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeMeta, Session, sessionmaker

from .database_settings import DatabaseSettings
from .model import ModelBase


class DatabaseABC(ABC):
    settings: DatabaseSettings
    session_class: t.Union[t.Type[Session], t.Type[AsyncSession]]
    model_class: t.Type[ModelBase]
    engine: t.Union[Engine, AsyncEngine]

    @property
    def url(self) -> URL:
        """Return database url."""
        return self.settings.url

    @property
    def uri(self) -> str:
        """Return database uri."""
        return str(self.url)

    @property
    def name(self) -> t.Optional[str]:
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
            f"{mapper.class_.__module__}.{mapper.class_.__name__}": mapper.class_  # type: ignore
            for mapper in self.model_class.registry.mappers
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self.url)!r})"

    def create_sessionmaker(self) -> sessionmaker:
        """Return instance of SQLAlchemy sessionmaker using database settings."""
        return sessionmaker(
            self.engine,  # type: ignore
            class_=self.session_class,
            future=True,
            **self.settings.get_session_options(),
        )

    @abstractmethod
    def create_engine(self) -> t.Union[Engine, AsyncEngine]:  # pragma: no cover
        pass

    @abstractmethod
    def create_all(
        self, **kwargs: t.Any
    ) -> t.Union[None, t.Coroutine[t.Any, t.Any, None]]:  # pragma: no cover
        pass

    @abstractmethod
    def drop_all(
        self, **kwargs: t.Any
    ) -> t.Union[None, t.Coroutine[t.Any, t.Any, None]]:  # pragma: no cover
        pass

    @abstractmethod
    def reflect(
        self, **kwargs: t.Any
    ) -> t.Union[None, t.Coroutine[t.Any, t.Any, None]]:  # pragma: no cover
        pass

    @abstractmethod
    def session(self) -> t.Union[Session, AsyncSession]:  # pragma: no cover
        pass

    @abstractmethod
    def begin(
        self,
        *,
        autoflush: t.Optional[bool] = None,
        expire_on_commit: t.Optional[bool] = None,
        **kwargs: t.Any,
    ) -> t.Union[
        t.ContextManager[Session], t.AsyncContextManager[AsyncSession]
    ]:  # pragma: no cover
        pass

    @abstractmethod
    def connect(self) -> t.Union[Connection, AsyncConnection]:  # pragma: no cover
        pass

    @abstractmethod
    def close(self) -> t.Union[None, t.Coroutine[t.Any, t.Any, None]]:  # pragma: no cover
        pass

    @abstractmethod
    def ping(self) -> t.Union[bool, t.Coroutine[t.Any, t.Any, bool]]:  # pragma: no cover
        pass
