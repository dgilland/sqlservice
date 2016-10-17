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

from . import core
from .model import declarative_base
from .query import Query
from .service import SQLService
from ._compat import iteritems, string_types


class SQLClient(object):
    """Database client for interacting with a database.

    The following configuration values can be passed into a new
    :class:`SQLClient` instance as a ``dict``.

    ========================  =================================================
    **SQL_DATABASE_URI**      URI used to connect to the database. Defaults
                              to ``sqlite://``.
    **SQL_ECHO**              When ``True`` have SQLAlchemy log all SQL
                              statements. Defaults to ``False``.
    **SQL_ECHO_POOL**         When ``True`` have SQLAlchemy log all log all
                              checkouts/checkins of the connection pool.
                              Defaults to ``False``.
    **SQL_ENCODING**          The string encoding used by SQLAlchemy for string
                              encode/decode operations which occur within
                              SQLAlchemy, outside of the DBAPI. Defaults to
                              `utf-8`.
    **SQL_CONVERT_UNICODE**   When ``True`` it sets the default behavior of
                              ``convert_unicode`` on the ``String`` type to
                              ``True``, regardless of a setting of ``False`` on
                              an individual ``String`` type, thus causing all
                              ``String`` -based columns to accommodate Python
                              unicode objects.
    **SQL_ISOLATION_LEVEL**   String parameter interpreted by various dialects
                              in order to affect the transaction isolation
                              level of the database connection. The parameter
                              essentially accepts some subset of these string
                              arguments: ``"SERIALIZABLE"``,
                              ``"REPEATABLE_READ"``, ``"READ_COMMITTED"``,
                              ``"READ_UNCOMMITTED"`` and ``"AUTOCOMMIT"``.
                              Behavior here varies per backend, and individual
                              dialects should be consulted directly. Defaults
                              to ``None``.
    **SQL_POOL_SIZE**         The size of the database pool. Defaults to the
                              engine's default (usually ``5``).
    **SQL_POOL_TIMEOUT**      Specifies the connection timeout for the pool.
                              Defaults to ``10``.
    **SQL_POOL_RECYCLE**      Number of seconds after which a connection is
                              automatically recycled.
    **SQL_MAX_OVERFLOW**      Controls the number of connections that can be
                              created after the pool reached its maximum size.
                              When those additional connections are returned to
                              the pool, they are disconnected and discarded.
    **SQL_AUTOCOMMIT**        When ``True``, the ``Session`` does not keep a
                              persistent transaction running, and will acquire
                              connections from the engine on an as-needed
                              basis, returning them immediately after their
                              use. Defaults to ``False``.
    **SQL_AUTOFLUSH**         When ``True``, all query operations will issue a
                              ``flush()`` call to the ``Session`` before
                              proceeding. This is a convenience feature so that
                              ``flush()`` need not be called repeatedly in
                              order for database queries to retrieve results.
    **SQL_EXPIRE_ON_COMMIT**  When ``True`` all instances will be fully expired
                              after each ``commit()``, so that all
                              attribute/object access subsequent to a completed
                              transaction will load from the most recent
                              database state. Defaults to ``True``.
    ========================  =================================================

    Args:
        config (dict): Database engine configuration options.
        model_class (object): A SQLAlchemy ORM declarative base model.
        service_class (object, optional): Service class used to register model
            service instances. If provided, it should be have the same
            initialization signature as :class:`.SQLService`. Defaults to
            :class:`.SQLService`.
    """
    def __init__(self,
                 config=None,
                 model_class=None,
                 service_class=SQLService):
        if model_class is None:  # pragma: no cover
            model_class = declarative_base()

        self.model_class = model_class
        self.service_class = service_class

        self.config = {
            'SQL_DATABASE_URI': 'sqlite://',
            'SQL_ECHO': False,
            'SQL_ECHO_POOL': False,
            'SQL_ENCODING': None,
            'SQL_CONVERT_UNICODE': None,
            'SQL_ISOLATION_LEVEL': None,
            'SQL_POOL_SIZE': None,
            'SQL_POOL_TIMEOUT': None,
            'SQL_POOL_RECYCLE': None,
            'SQL_MAX_OVERFLOW': None,
            'SQL_AUTOCOMMIT': False,
            'SQL_AUTOFLUSH': True,
            'SQL_EXPIRE_ON_COMMIT': True
        }

        self.config.update(config or {})

        engine_options = self.make_engine_options()
        session_options = self.make_session_options()

        self.engine = self.create_engine(self.config['SQL_DATABASE_URI'],
                                         engine_options)
        self.session = self.create_session(self.engine, session_options)

        self._services = {}
        self._register_all_services()

    def create_engine(self, uri, options=None):
        """Factory function to create a database engine using `config` options.

        Args:
            config (dict): Database client configuration.

        Returns:
            Engine: SQLAlchemy engine instance.
        """
        if options is None:  # pragma: no cover
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
            ('SQL_ECHO_POOL', 'echo_pool'),
            ('SQL_ENCODING', 'ecoding'),
            ('SQL_CONVERT_UNICODE', 'convert_unicode'),
            ('SQL_ISOLATION_LEVEL', 'isolation_level'),
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
            ('SQL_AUTOFLUSH', 'autoflush'),
            ('SQL_EXPIRE_ON_COMMIT', 'expire_on_commit')
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

    @property
    def url(self):
        """Proxy property to database engine's database URL."""
        return self.engine.url

    @property
    def database(self):
        """Proxy property to database engine's database name."""
        return self.engine.url.database

    def get_metadata(self):
        """Return `MetaData` from :attr:`model` or raise an exception if
        :attr:`model` was never given.
        """
        if self.metadata is None:  # pragma: no cover
            raise UnmappedError('Missing declarative base model')
        return self.metadata

    @property
    def metadata(self):
        """Return `MetaData` from :attr:`model` or ``None``."""
        return getattr(self.model_class, 'metadata', None)

    @property
    def tables(self):
        """Return ``dict`` of table instances found in :attr:`metadata` with
        table names as keys and corresponding table objects as values.
        """
        return self.metadata.tables

    @property
    def models(self):
        """Return model registry ``dict`` with model names as keys and
        corresponding model classes as values.
        """
        models = getattr(self.model_class, '_decl_class_registry', None)

        if models:
            models = {name: model for name, model in iteritems(models)
                      if not name.startswith('_sa_')}

        return models

    @property
    def services(self):
        """Return service registry ``dict`` with model names as keys and
        corresponding model service classes as values.
        """
        return self._services

    def create_all(self):
        """Create all metadata (tables, etc) contained within :attr:`metadata`.
        """
        self.get_metadata().create_all(self.engine)

    def drop_all(self):
        """Drop all metadata (tables, etc) contained within :attr:`metadata`.
        """
        self.get_metadata().drop_all(self.engine)

    def reflect(self):
        """Reflect tables from database into :attr:`metadata`."""
        self.get_metadata().reflect(self.engine)

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
        except Exception:  # pragma: no cover
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
        with core.transaction(self.session, readonly=readonly):
            yield self.session

    def save(self, models, before=None, after=None, identity=None):
        """Save `models` into the database using insert, update, or
        upsert-on-primary-key.

        The `models` argument can be any of the following:

        - Model instance
        - ``list``/``tuple`` of Model instances

        Args:
            models (mixed): Models to save to database.
            before (function, optional): Function to call before each model is
                saved via ``session.add``. Function should have signature
                ``before(model, is_new)``.
            after (function, optional): Function to call after each model is
                saved via ``session.add``. Function should have signature
                ``after(model, is_new)``.
            identity (function, optional): Function used to return an idenity
                map for a given model. Function should have the signature
                ``identity(model)``. By default
                :func:`.core.primary_identity_map` is used.

        Returns:
            Model: If a single item passed in.
            list: A ``list`` of Model instaces if multiple items passed in.
        """
        if not isinstance(models, (list, tuple)):
            _models = [models]
        else:
            _models = models

        for idx, model in enumerate(_models):
            if type(model) not in self.models.values():
                raise TypeError('Type of value given to save() method is not '
                                'a valid SQLALchemy declarative class. '
                                'Item with index {0} and with value "{1}" is '
                                'an instance of "{2}".'
                                .format(idx, model, type(model)))

        return core.save(self.session,
                         models,
                         before=before,
                         after=after,
                         identity=identity)

    def destroy(self, data, model_class=None, synchronize_session=False):
        """Delete bulk records from `data`.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict`` objects
        - ``list``/``tuple`` of :attr:`model_class` instances

        If a ``dict`` or ``list`` of ``dict`` is passed in, then `model_class`
        must be provided

        Args:
            data (mixed): Data to delete from database.
            synchronize_session (bool|str): Argument passed to
                ``Query.delete``.

        Returns:
            int: Number of deleted records.
        """
        return core.destroy(self.session, data, model_class=model_class)

    def _register_all_services(self):
        """Register all model services using model names/classes from
        :attr:`models`.
        """
        if not self.metadata or not self.models:  # pragma: no cover
            return

        for model_name, model_class in iteritems(self.models):
            self._register_service(model_name, model_class)

    def _register_service(self, model_name, model_class):
        """Register model service using `model_name` as the key and
        `model_class` as the argument to :attr:`service_class`. Once a service
        is registered, it won't be created again.
        """
        if model_name not in self._services:
            self._services[model_name] = (
                self.service_class(self, model_class))

        return self._services[model_name]

    def __getitem__(self, item):
        """Return :attr:`service_class` instance corresponding to `item`.

        Args:
            item (str): Attribute corresponding to string name of model class.

        Returns:
            :attr:`service_class`: Instance of :attr:`service_class`
                initialized with model class.

        Raises:
            AttributeError: When item doesn't correspond to model class
                name found in :attr:`metadata`.
        """
        if not isinstance(item, string_types):
            # If anything other than a string is supplied, use the item's
            # __name__ as the model name to index to.
            item = getattr(item, '__name__', item)
        return getattr(self, item)

    def __getattr__(self, attr):
        """Return :attr:`service_class` instance corresponding to `attr`.

        Args:
            attr (str): Attribute corresponding to string name of model class.

        Returns:
            :attr:`service_class`: Instance of :attr:`service_class`
                initialized with model class.

        Raises:
            AttributeError: When attribute doesn't correspond to model class
                name found in :attr:`metadata`.
        """
        if attr not in self.models:  # pragma: no cover
            raise AttributeError('Model name, "{0}", is not a recognized '
                                 'model. Valid names are: {1}'
                                 .format(attr, ', '.join(self.models)))

        return self._register_service(attr, self.models[attr])
