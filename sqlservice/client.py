# -*- coding: utf-8 -*-
"""
Client
------

The database client module.
"""

from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.exc import UnmappedError
from sqlalchemy.orm.session import Session
from sqlalchemy.engine.url import make_url

from .model import declarative_base
from .query import Query


class SQLClient(object):
    """Database client for interacting with a database.

    The following configuration values can be passed into a new
    :class:`SQLClient` instance as a ``dict``.

    ====================  =====================================================
    **SQL_DATABASE_URI**  URI used to connect to the database. Defaults
                          to ``sqlite://``.
    **SQL_ECHO**          When ``True`` have SQLAlchemy echo all SQL
                          statements. Defaults to ``False``.
    **SQL_POOL_SIZE**     The size of the database pool. Defaults to the
                          engine's default (usually ``5``).
    **SQL_POOL_TIMEOUT**  Specifies the connection timeout for the pool.
                          Defaults to ``10``.
    **SQL_POOL_RECYCLE**  Number of seconds after which a connection is
                          automatically recycled.
    **SQL_MAX_OVERFLOW**  Controls the number of connections that can be
                          created after the pool reached its maximum size. When
                          those additional connections are returned to the
                          pool, they are disconnected and discarded.
    **SQL_AUTOCOMMIT**    When ``True``, the ``Session`` does not keep a
                          persistent transaction running, and will acquire
                          connections from the engine on an as-needed basis,
                          returning them immediately after their use. Defaults
                          to ``False``.
    **SQL_AUTOFLUSH**     When ``True``, all query operations will issue a
                          ``flush()`` call to the ``Session`` before
                          proceeding. This is a convenience feature so that
                          ``flush()`` need not be called repeatedly in order
                          for database queries to retrieve results.
    ====================  =====================================================

    Args:
        config (dict): Database engine configuration options.
        Model (object): A SQLAlchemy ORM declarative base model.
    """
    def __init__(self, config=None, Model=None):
        if Model is None:
            Model = declarative_base()

        self.Model = Model

        self.config = {
            'SQL_DATABASE_URI': 'sqlite://',
            'SQL_ECHO': False,
            'SQL_POOL_SIZE': None,
            'SQL_POOL_TIMEOUT': None,
            'SQL_POOL_RECYCLE': None,
            'SQL_MAX_OVERFLOW': None,
            'SQL_AUTOCOMMIT': False,
            'SQL_AUTOFLUSH': True
        }

        self.config.update(config or {})

        engine_options = self.make_engine_options()
        session_options = self.make_session_options()

        self.engine = self.create_engine(config['SQL_DATABASE_URI'],
                                         engine_options)
        self.session = self.create_session(self.engine, session_options)

    def create_engine(self, uri, options=None):
        """Factory function to create a database engine using `config` options.

        Args:
            config (dict): Database client configuration.

        Returns:
            Engine: SQLAlchemy engine instance.
        """
        if options is None:
            options = {}

        return sa.create_engine(make_url(uri), **options)

    def create_session(self,
                       bind,
                       options=None,
                       session_class=Session,
                       query_class=Query):
        """Factory function to create a scoped session using `bind`.

        Args:
            bind (Engine|Connection): Database engine or connection instance.
            options (dict, optional): Session configuration options.
            session_class (obj, optional): Session class to use when creating
                new session instances. Defaults to :class:`.Session`.
            query_class (obj, optional): Query class used for ``session.query``
                instances. Defaults to :class:`.Query`.

        Returns:
            Session: SQLAlchemy session instance bound to `bind`.
        """
        if options is None:  # pragma: no cover
            options = {}

        if query_class:
            options['query_cls'] = query_class

        session_factory = orm.sessionmaker(bind=bind,
                                           class_=session_class,
                                           **options)

        return orm.scoped_session(session_factory)

    def make_engine_options(self):
        """Return engine options from :attr:`config` for use in
        ``sqlalchemy.create_engine``.
        """
        return self._make_options((
            ('SQL_ECHO', 'echo'),
            ('SQL_POOL_SIZE', 'pool_size'),
            ('SQL_POOL_TIMEOUT', 'pool_timeout'),
            ('SQL_POOL_RECYCLE', 'pool_recycle'),
            ('SQL_MAX_OVERFLOW', 'max_overflow')
        ))

    def make_session_options(self):
        """Return session options from :attr:`config` for use in
        ``sqlalchemy.orm.sessionmaker``.
        """
        return self._make_options((
            ('SQL_AUTOCOMMIT', 'autocommit'),
            ('SQL_AUTOFLUSH', 'autoflush')
        ))

    def _make_options(self, key_mapping):
        """Return mapped :attr:`config` options using `key_mapping` which is a
        tuple having the form ``((<config_key>, <sqlalchemy_key>), ...)``.
        Where ``<sqlalchemy_key>`` is the corresponding option keyword for a
        SQLAlchemy function.
        """
        return {opt_key: self.config[cfg_key]
                for cfg_key, opt_key in key_mapping
                if self.config.get(cfg_key) is not None}

    def get_metadata(self):
        """Return `MetaData` from :attr:`model` or raise an exception if
        :attr:`model` was never given.
        """
        if self.metadata is None:  # pragma: no cover
            raise UnmappedError('Missing declarative base model')
        return self.metadata

    def create_all(self):
        """Create all metadata (tables, etc) contained within :attr:`metadata`.
        """
        metadata = self.get_metadata()
        metadata.create_all(self.engine)

    def drop_all(self):
        """Drop all metadata (tables, etc) contained within :attr:`metadata`.
        """
        metadata = self.get_metadata()
        metadata.drop_all(self.engine)

    @property
    def session(self):
        """Proxy to threadlocal session object returned by scoped session
        object.

        Note:
            Generally, the scoped session is sufficient to work with directly.
            However, the scoped session doesn't provide access to the custom
            session class used by the session factory. This property returns an
            instance of our custom session class. Multiple calls to the scoped
            session always returns the same active theadlocal session (i.e.
            ``self._Session() is self._Session()``).

        See Also:
            http://docs.sqlalchemy.org/en/latest/orm/contextual.html
        """
        return self._Session()

    @session.setter
    def session(self, Session):
        """Set private :attr:`_Session`."""
        self._Session = Session

    @property
    def metadata(self):
        """Return `MetaData` from :attr:`model` or ``None``."""
        return getattr(self.Model, 'metadata', None)

    @property
    def tables(self):
        """Proxy property or ORM metadata's tables ``dict``."""
        return self.metadata.tables

    @property
    def url(self):
        """Proxy property to database engine's database URL."""
        return self.engine.url

    @property
    def database(self):
        """Proxy property to database engine's database name."""
        return self.engine.url.database

    @property
    def add(self):
        """Proxy property to :meth:`session.add`."""
        return self.session.add

    @property
    def add_all(self):
        """Proxy property to :meth:`session.add_all`."""
        return self.session.add_all

    @property
    def delete(self):
        """Proxy property to :meth:`session.delete`."""
        return self.session.delete

    def delete_all(self, instances):
        """Mark multiple instances for deletion."""
        if not isinstance(instances, (list, tuple)):
            instances = [instances]

        for instance in instances:
            self.delete(instance)

    @property
    def merge(self):
        """Proxy property to :meth:`session.merge`."""
        return self.session.merge

    @property
    def execute(self):
        """Proxy property to :meth:`session.execute`."""
        return self.session.execute

    @property
    def close(self):
        """Proxy property to :meth:`session.close`."""
        return self.session.close

    @property
    def close_all(self):
        """Proxy property to :meth:`_Session.close_all`."""
        return self._Session.close_all

    @property
    def remove(self):
        """Proxy propery to :meth:`_Session.remove`."""
        return self._Session.remove

    def shutdown(self):
        """Shut down all database sessions and connections."""
        self.close_all()
        self.remove()
        self.engine.dispose()

    def commit(self):
        """Commit a session transaction but rollback if an error occurs. This
        helps ensure that the session is not left in an unsable state.
        """
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    @property
    def flush(self):
        """Proxy property to :meth:`session.flush`."""
        return self.session.flush

    @property
    def refresh(self):
        """Proxy property to :meth:`session.refresh`."""
        return self.session.refresh

    @property
    def rollback(self):
        """Proxy property to :meth:`session.rollback`."""
        return self.session.rollback

    @property
    def query(self):
        """Proxy property to :meth:`session.query`."""
        return self.session.query

    @contextmanager
    def transaction(self, readonly=False):
        """Nestable session transaction context manager where only a single
        commit will be issued once all contexts have been exited. If an
        exception occurs either at commit time or before, the transaction will
        be rolled back and the original exception re-raised.

        Yields:
            :attr:`session`
        """
        # Keep track of nested calls to this context manager using this
        # "trans_count" counter. Data stored in session.info will be local to
        # that session and persist through its lifetime.
        self.session.info.setdefault('trans_count', 0)

        # Bump count every time context is entered.
        self.session.info['trans_count'] += 1

        if not readonly:
            # Disable autoflush during write transactions. Autoflush can cause
            # issues when setting ORM relationship values in cases where
            # consistency is only maintained at commit time but would fail if
            # an autoflush occurred beforehand.
            self.session.autoflush = False

        try:
            yield self.session
        except Exception:
            # Only rollback if we haven't rolled back yet (i.e. one
            # rollback only per nested transaction set).
            if self.session.info['trans_count'] > 0:
                self.rollback()

            # Reset trans_count to zero to prevent other rollbacks as the
            # exception bubbles up the call stack.
            self.session.info['trans_count'] = 0

            raise
        else:
            self.session.info['trans_count'] -= 1

            # Paranoia dictates that we compare with "<=" instead of "==".
            # Only commit once our trans counter reaches zero.
            if not readonly and self.session.info['trans_count'] <= 0:
                self.commit()
        finally:
            # Restore autoflush setting once transaction is over.
            if self.session.info['trans_count'] <= 0:
                self.session.autoflush = self.config['SQL_AUTOFLUSH']

            # Reset counter in case we some how got below 0.
            if self.session.info['trans_count'] < 0:
                self.session.info['trans_count'] = 0
