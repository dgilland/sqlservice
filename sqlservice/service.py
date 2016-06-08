# -*- coding: utf-8 -*-
"""
Service
-------

The service class module.
"""

from functools import wraps

import pydash as pyd
from sqlalchemy import and_, or_, orm

from . import core



class ServiceBase(object):
    """Base service class."""
    def __init__(self, db):
        self._db = db

    @property
    def db(self):
        """Proxy property to database client."""
        return self._db


class SQLService(ServiceBase):
    """SQL service class for an ORM model."""
    #: ORM model class. **MUST BE SET BY SUBCLASS**
    model_class = None

    def query(self):
        """Return a session query object using :attr:`model_class`."""
        return self.db.query(self.model_class)

    def query_one(self):
        """Return a session query object to use with :meth:`get` and
        :meth:`find_one` methods which both return a maximum of one record.
        """
        return self.query()

    def query_many(self):
        """Return a session query object to use with :meth:`find` which can
        return many records.
        """
        return self.query()

    def default_order_by(self):
        """Return default order by expression to be used as a default."""
        return self.model_class.pk_columns()

    def new(self, data=None):
        """Return a new model instance from a ``dict`` using
        :attr:`model_class` to create it.

        Args:
            data (dict): Mapping of model columns to values.

        Return:
            :attr:`model_class` instance
        """
        return self.model_class(data)

    @transaction(readonly=True)
    def get(self, ident):
        """Return a single model or ``None`` given `ident` value.

        Possible values of `ident` are:

        - ``str``/``numeric``: Value of primary key
        - ``tuple``/``list``: Values corresponding to primary keys. Useful when
            model has multiple primary keys.
        - ``dict``: Mapping containing primary key column names and values. Can
            be used to select models with single or multiple primary keys.

        Args:
            ident (mixed): Object containing primary key value(s).

        Returns:
            :attr:`model_class`: When primary key record exists.
            None: When primary key record does not exist.
        """
        return self.find_one(core.identity_filter(ident, self.model_class))

    @transaction(readonly=True)
    def find_one(self, *criterion, **criterion_kargs):
        """Return a single model or ``None`` given `criterion` ``dict`` or
        keyword arguments.

        Args:
            criterion (dict, optional): Filter-by dict.
            **criterion_kargs (optional): Mapping of filter-by arguments.

        Returns:
            :attr:`model_class`: When filtered record exists.
            None: When filtered record does not exist.
        """
        criterion = list(criterion) + [criterion_kargs]
        order_by = self.default_order_by()
        return (self.query_one()
                .search(*criterion, order_by=order_by)
                .top())

    @transaction(readonly=True)
    def find(self, *criterion, per_page=None, page=None, order_by=None):
        """Return list of models matching `criterion`.

        Args:
            *criterion (sqlaexpr, optional): SQLA expression to filter against.

        Keyword Args:
            paginate (tuple(per_page, page), optional): Tuple containing
                ``(per_page, page)`` arguments for pagination. Defaults to
                ``None``.
            order_by (sqlaexpr, optional): Order by expression. Defaults to
                ``None``.

        Returns:
            list: List of :attr:`model_class`
        """
        if order_by is None:
            order_by = self.default_order_by()

        return (self.query_many()
                .search(*criterion,
                        per_page=per_page,
                        page=page,
                        order_by=order_by)
                .all())

    def save(self, data):
        """Save `data` into the database using insert, update, or
        upsert-on-primary-key.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict``s
        - ``list``/``tuple`` of :attr:`model_class` instances

        This method will attempt to do the "right" thing by mapping any items
        in `data` that have their primary key set with the corresponding record
        in the database if it exists.

        Args:
            data (mixed): Data to save to database.

        Returns:
            :attr:`model_class`: If a single item passed in.
            list: A ``list`` of :attr:`model_class` instaces if multiple items
                passed in.
        """
        if not data:
            return

        if isinstance(data, (list, tuple)):
            models = [self.new(item) if not isinstance(item, self.model_class)
                      else item
                      for item in data]
        elif not isinstance(data, self.model_class):
            models = self.new(data)
        else:
            models = data

        return self.db.save(models)

    def destroy(self, data, synchronize_session=False):
        """Delete bulk records from `data`.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict``s
        - ``list``/``tuple`` of :attr:`model_class` instances

        Args:
            data (mixed): Data to delete from database.
            synchronize_session (bool|str): Argument passed to
                ``Query.delete``.

        Returns:
            int: Number of deleted records.
        """
        return self.db.destroy(data, model_class=self.model_class)