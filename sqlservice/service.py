# -*- coding: utf-8 -*-
"""
Service
-------

The service class module.
"""

from functools import wraps

from sqlalchemy import and_, or_, orm

from . import core


class SQLService(object):
    """SQL service class for an ORM model."""

    def __init__(self, db, model_class):
        self.db = db
        self.model_class = model_class

    def query(self):
        """Return a session query object using :attr:`model_class`."""
        return self.db.query(self.model_class)

    def new(self, data=None):
        """Return a new model instance from a ``dict`` using
        :attr:`model_class` to create it.

        Args:
            data (dict): Mapping of model columns to values.

        Return:
            :attr:`model_class` instance
        """
        return self.model_class(data)

    def count(self):
        """Return total count of records in database."""
        return self.query().count()

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

        with self.db.transaction(readonly=True):
            return (self.query()
                    .search(*criterion)
                    .top())

    def find(self, *criterion, **kargs):
        """Return list of models matching `criterion`.

        Args:
            *criterion (sqlaexpr, optional): SQLA expression to filter against.

        Keyword Args:
            per_page (int, optional): Number of results to return per page.
                Defaults to ``None`` (i.e. no limit).
            page (int, optional): Which page offset of results to return.
                Defaults to ``1``.
            order_by (sqlaexpr, optional): Order by expression. Defaults to
                ``None``.

        Returns:
            list: List of :attr:`model_class`
        """
        with self.db.transaction(readonly=True):
            return self.query().search(*criterion, **kargs).all()

    def save(self, data, before=None, after=None):
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
            before (function, optional): Function to call before each model is
                saved via ``session.add``. Function should have signature
                ``before(model, is_new)``.
            after (function, optional): Function to call after each model is
                saved via ``session.add``. Function should have signature
                ``after(model, is_new)``.

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

        return self.db.save(models, before, after)

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
