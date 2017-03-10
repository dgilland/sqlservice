# -*- coding: utf-8 -*-
"""
Client
------

The database client module.
"""

from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative.clsregistry import _MultipleClassMarker
from sqlalchemy.orm.exc import UnmappedError
from sqlalchemy.orm.session import Session
from sqlalchemy.engine.url import make_url

from . import core
from .model import declarative_base
from .query import SQLQuery
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
        query_class (Query, optional): SQLAlchemy Query derived class to use as
            the default class when creating a new query object.
        session_class (Session, optional): SQLAlchemy Session derived class to
            use by the session maker.
        session_options (dict, optional): Additional session options use when
            creating the database session.
    """
    def __init__(self,
                 config=None,
                 model_class=None,
                 query_class=SQLQuery,
                 session_class=Session,
                 session_options=None,
                 engine_options=None):
        if model_class is None:  # pragma: no cover
            model_class = declarative_base()

        self.model_class = model_class
        self.query_class = query_class
        self.session_class = session_class

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

        engine_options = self.make_engine_options(engine_options)
        session_options = self.make_session_options(session_options)

        self.engine = self.create_engine(self.config['SQL_DATABASE_URI'],
                                         engine_options)
        self.session = self.create_session(self.engine,
                                           session_options,
                                           session_class=self.session_class,
                                           query_class=self.query_class)
        self.update_models_registry()

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
                       query_class=SQLQuery):
        """Factory function to create a scoped session using `bind`.

        Args:
            bind (Engine|Connection): Database engine or connection instance.
            options (dict, optional): Session configuration options.
            session_class (obj, optional): Session class to use when creating
                new session instances. Defaults to :class:`.Session`.
            query_class (obj, optional): Query class used for ``session.query``
                instances. Defaults to :class:`.SQLQuery`.

        Returns:
            Session: SQLAlchemy session instance bound to `bind`.
        """
        if options is None:  # pragma: no cover
            options = {}

        if query_class:
            options['query_cls'] = query_class

        scopefunc = options.pop('scopefunc', None)
        session_factory = orm.sessionmaker(bind=bind,
                                           class_=session_class,
                                           **options)

        return orm.scoped_session(session_factory, scopefunc=scopefunc)

    def make_engine_options(self, extra_options=None):
        """Return engine options from :attr:`config` for use in
        ``sqlalchemy.create_engine``.
        """
        options = self._make_options((
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

        if extra_options:
            options.update(extra_options)

        return options

    def make_session_options(self, extra_options=None):
        """Return session options from :attr:`config` for use in
        ``sqlalchemy.orm.sessionmaker``.
        """
        options = self._make_options((
            ('SQL_AUTOCOMMIT', 'autocommit'),
            ('SQL_AUTOFLUSH', 'autoflush'),
            ('SQL_EXPIRE_ON_COMMIT', 'expire_on_commit')
        ))

        if extra_options:
            options.update(extra_options)

        return options

    def _make_options(self, key_mapping):
        """Return mapped :attr:`config` options using `key_mapping` which is a
        tuple having the form ``((<config_key>, <sqlalchemy_key>), ...)``.
        Where ``<sqlalchemy_key>`` is the corresponding option keyword for a
        SQLAlchemy function.
        """
        return {opt_key: self.config[cfg_key]
                for cfg_key, opt_key in key_mapping
                if self.config.get(cfg_key) is not None}

    def update_models_registry(self):
        """Update :attr:`models` registry as computed from :attr:`model_class`.
        """
        self.models = self.create_models_registry(self.model_class)

    def create_models_registry(self, model_class):
        """Return model registry ``dict`` with model names as keys and
        corresponding model classes as values.
        """
        models = {}
        class_registry = getattr(model_class, '_decl_class_registry', None)

        if not class_registry:
            return models

        for name, model in iteritems(class_registry):
            if name.startswith('_sa_'):
                continue

            if isinstance(model, _MultipleClassMarker):
                # Handle case where there are multiple ORM models with the same
                # base class name but located in different submodules.
                model = list(model)

                if len(model) == 1:  # pragma: no cover
                    models[name] = model[0]
                else:
                    for obj in list(model):
                        modobj = '{0}.{1}'.format(obj.__module__, obj.__name__)
                        models[modobj] = obj
            else:
                models[name] = model

        return models

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
    def prepare(self):
        """Proxy property to :meth:`session.prepare`."""
        return self.session.prepare

    @property
    def no_autoflush(self):
        """Proxy property to :meth:`session.no_autoflush`."""
        return self.session.no_autoflush

    @property
    def scalar(self):
        """Proxy property to :meth:`session.scalar`."""
        return self.session.scalar

    @property
    def close(self):
        """Proxy property to :meth:`session.close`."""
        return self.session.close

    @property
    def close_all(self):
        """Proxy property to :meth:`_Session.close_all`."""
        return self._Session.close_all

    @property
    def invalidate(self):
        """Proxy property to :meth:`session.invalidate`."""
        return self.session.invalidate

    @property
    def is_active(self):
        """Proxy property to :attr:`session.is_active`."""
        return self.session.is_active

    @property
    def is_modified(self):
        """Proxy property to :meth:`session.is_modified`."""
        return self.session.is_modified

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
    def rollback(self):
        """Proxy property to :meth:`session.rollback`."""
        return self.session.rollback

    @property
    def flush(self):
        """Proxy property to :meth:`session.flush`."""
        return self.session.flush

    @property
    def refresh(self):
        """Proxy property to :meth:`session.refresh`."""
        return self.session.refresh

    @property
    def expire(self):
        """Proxy property to :meth:`session.expire`."""
        return self.session.expire

    @property
    def expire_all(self):
        """Proxy property to :meth:`session.expire`."""
        return self.session.expire_all

    def expunge(self, *instances):
        """Remove all `instances` from :attr:`session`."""
        for instance in instances:
            self.session.expunge(instance)

    @property
    def expunge_all(self):
        """Proxy property to :meth:`session.expunge`."""
        return self.session.expunge_all

    @property
    def prune(self):
        """Proxy property to :meth:`session.prune`."""
        return self.session.prune

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
            as_list = False
        else:
            _models = models
            as_list = True

        for idx, model in enumerate(_models):
            if type(model) in self.models.values():
                continue

            self.update_models_registry()

            if type(model) not in self.models.values():
                if as_list:
                    idx_msg = 'Item with index {0} and value '.format(idx)
                else:
                    idx_msg = ''

                raise TypeError(
                    'Type of value given to save() method is not a recognized '
                    'SQLALchemy declarative class that derives from {0}. '
                    '{1}"{2}" is an instance of "{3}".'
                    .format(self.model_class,
                            idx_msg,
                            model,
                            type(model)))

        return core.save(self.session,
                         models,
                         before=before,
                         after=after,
                         identity=identity)

    def bulk_insert(self, mapper, mappings):
        """Perform a bulk insert into table/statement represented by `mapper`
        while utilizing a special syntax that replaces the tradtional
        ``executemany()`` DBAPI call with a multi-row VALUES clause for a
        single INSERT statement.

        See :meth:`bulk_insert_many` for bulk inserts using ``executemany()``.

        Args:
            mapper: An ORM class or SQLAlchemy insert-statement object.
            mappings (list): List of ``dict`` objects to insert.

        Returns:
            ResultProxy
        """
        if hasattr(mapper, '__table__'):
            insert_stmt = mapper.__table__.insert()
        else:
            insert_stmt = mapper
        return self.execute(insert_stmt.values(mappings))

    def bulk_insert_many(self, mapper, mappings):
        """Perform a bulk insert into table/statement represented by `mapper`
        while utilizing the ``executemany()`` DBAPI call.

        See :meth:`bulk_insert` for bulk inserts using a multi-row VALUES
        clause for a single INSERT statement.

        Args:
            mapper: An ORM class or SQLAlchemy insert-statement object.
            mappings (list): List of ``dict`` objects to insert.

        Returns:
            ResultProxy
        """
        if hasattr(mapper, '__table__'):
            insert_stmt = mapper.__table__.insert()
        else:
            insert_stmt = mapper
        return self.execute(insert_stmt.values(), mappings)

    @property
    def bulk_insert_mappings(self):
        """Proxy property to :meth:`session.bulk_insert_mappings`."""
        return self.session.bulk_insert_mappings

    @property
    def bulk_save_objects(self):
        """Proxy property to :meth:`session.bulk_save_objects`."""
        return self.session.bulk_save_objects

    @property
    def bulk_update_mappings(self):
        """Proxy property to :meth:`session.bulk_update_mappings`."""
        return self.session.bulk_update_mappings

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
        return core.destroy(self.session,
                            data,
                            model_class=model_class,
                            synchronize_session=synchronize_session)

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
            # Potentially, this model could have been imported after creation
            # of this class. Since we got a bad attribute, let's go ahead and
            # update the registry and try again.
            self.update_models_registry()

        if attr not in self.models:  # pragma: no cover
            raise AttributeError(
                'The attribute "{0}" is not an attribute of {1} nor is it a '
                'unique model class name in the declarative model class '
                'registry of {2}. Valid model names are: {3}. If a model name '
                'is shown as a full module path, then that model class name '
                'is not unique and cannot be referenced via attribute access.'
                .format(attr,
                        self.__class__.__name__,
                        self.model_class,
                        ', '.join(self.models)))

        return self.query(self.models[attr])
