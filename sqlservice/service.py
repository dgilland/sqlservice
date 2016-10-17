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

    def save(self, data, before=None, after=None):
        """Save `data` into the database using insert, update, or
        upsert-on-primary-key.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict`` objects
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
            list: A ``list`` of :attr:`model_class` when multiple items passed.
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

        return self.db.save(models, before=before, after=after)

    def destroy(self, data, synchronize_session=False):
        """Delete bulk records from `data`.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict`` objects
        - ``list``/``tuple`` of :attr:`model_class` instances

        Args:
            data (mixed): Data to delete from database.
            synchronize_session (bool|str): Argument passed to
                ``Query.delete``.

        Returns:
            int: Number of deleted records.
        """
        return self.db.destroy(data, model_class=self.model_class)

    def __getattr__(self, attr):
        """Proxy attribute access to a :meth:`query` instance."""
        query = self.query()

        try:
            return getattr(query, attr)
        except AttributeError:  # pragma: no cover
            raise AttributeError(
                "Neither {0} nor {1} objects have attribute '{2}'"
                .format(type(self).__name__, type(query).__name__, attr))
