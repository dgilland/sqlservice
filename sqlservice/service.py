# -*- coding: utf-8 -*-
"""
Service
-------

The service class module.
"""

from functools import wraps

import pydash as pyd
from sqlalchemy import and_, or_, orm


def transaction(_func=None, *, readonly=False):
    """Transaction decorator that wraps a function call inside a
    :class:`SQLClient` transaction.

    Warning:
        This decorator can only be used on a class that has a
        :class:`SQLClient` instance set at ``self.db``.
    """
    def decorator(func):
        @wraps(func)
        def decorated(self, *args, **kargs):
            with self.db.transaction(readonly=readonly):
                ret = func(self, *args, **kargs)
            return ret
        return decorated

    if callable(_func):
        return decorator(_func)
    else:
        return decorator


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
        return self.find_one(self._filter_by_ident(ident))

    @transaction(readonly=True)
    def find_one(self, *criterion, **criterion_kargs):
        """Return a single model or ``None`` given `filter_by` ``dict``.

        Args:
            filter_by (dict, optional): Filter-by dict.
            **filter_by_kargs (optional): Mapping of filter-by arguments.

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

    @transaction
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
            list: A ``list`` of :attr:`model_class` instaces if multiple items
                passed in.
            :attr:`model_class`: If a single item passed in.
        """
        if not data:
            return

        if not isinstance(data, (list, tuple)):
            # Data was not passed in a list/tuple so we'll want to return it
            # the same.
            as_list = False
            data = [data]
        else:
            as_list = True

        # This is what we'll return from this function so that the order stays
        # the same as what was given.
        models = [item if isinstance(item, self.model_class)
                  else self.new(item)
                  for item in data]

        # Model instances that should follow the "insert" path.
        insertable = []

        # Model instances that should follow the "update" path.
        updatable = []

        # Model instances that have their primary key(s) set which may already
        # exist in the database. These models will either be inserted or
        # updated, but some database querying will be required to make that
        # determination.
        mergeable = []

        # Parition models into `insertable` or `mergeable` buckets.
        for idx, model in enumerate(models):
            if model.identity() is not None:
                # Primary key(s) are set so might be mergeable.
                # Keep track of original `idx` because we'll need to update
                # the `models` list with the merged instance.
                mergeable.append((idx, model))
            else:
                # No primary key set so add to the insert list.
                insertable.append(model)

        if mergeable:
            # Before we attempt to merge models with existing database records,
            # we want to bulk fetch all of the potentially mergeable models.
            # Doing so will put those models into the session registry which
            # means that when we later call `merge()`, there won't be a
            # database fetch since we've pre-loaded them.
            pk_criteria = self._filter_on_primary_key([merge[1]
                                                       for merge in mergeable])
            existing = self.query().filter(pk_criteria).all()
            existing_index = {model.identity(): model for model in existing}

            for idx, model in mergeable:
                if model in self.db.session:
                    updatable.append(model)
                elif model.identity() in existing_index:
                    models[idx] = model = self.db.merge(model)
                    updatable.append(model)
                else:
                    insertable.append(model)

        self._add_all(insertable, new_record=True)
        self._add_all(updatable, new_record=False)

        return models if as_list else models[0]

    def _add_all(self, models, new_record=True):
        """Add multiple `models` into database.

        Args:
            model (Model): Model instance.
            new_record (bool): Indicates whether `models` are new or existing
                records in database.

        Returns:
            :class:`model_class`
        """
        for model in models:
            self._add(model, new_record=new_record)

    @transaction
    def _add(self, model, new_record=True):
        """Add `model` to database session while calling :meth:`before_save`
        and :meth:`after_save` before and after respectively.

        Args:
            model (Model): Model instance.
            new_record (bool): Indicates whether `model` is a new or existing
                record in database.
        """
        self.before_save(model, new_record=new_record)
        self.db.add(model)
        self.after_save(model, new_record=new_record)
        return model

    def before_save(self, model, new_record):
        """Stub method called before :meth:`save`. Method should not return
        anything. Method is called within the :meth:`save` transaction so any
        uncaught exception here will cause that transaction to rollback.

        Args:
            model (Model): Model instance.
            new_record (bool): Indicates whether `model` is a new or existing
                record in database.
        """
        pass

    def after_save(self, model, new_record):
        """Stub method called after :meth:`save` but before the transaction is
        committed. Method should not return anything. Method is called within
        the :meth:`save` transaction so any uncaught exception here will cause
        that transaction to rollback.

        Args:
            model (Model): Model instance.
            new_record (bool): Indicates whether `model` is a new or existing
                record in database.
        """
        pass

    @transaction
    def delete(self, data):
        """Delete bulk records from `data`.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict``s
        - ``list``/``tuple`` of :attr:`model_class` instances

        Args:
            data (mixed): Data to delete from database.

        Returns:
            int: Number of deleted records.
        """
        delete_count = (self.query()
                        .filter(self._filter_on_primary_key(data))
                        .options(orm.lazyload('*'))
                        .delete(synchronize_session=False))
        return delete_count

    def _filter_by_ident(self, ident):
        """Return filter-by ``dict`` based on `ident` value mapped to primary
        key(s).

        Possible values of `ident` are:

        - ``str``/``numeric``: Value of primary key
        - ``tuple``/``list``: Values corresponding to primary keys. Useful when
            model has multiple primary keys.
        - ``dict``: Mapping containing primary key column names and values. Can
            be used to select models with single or multiple primary keys.

        Args:
            ident (mixed): Object containing primary key value(s).

        Returns:
            dict
        """
        primary_keys = self.model_class.pk_columns()

        if isinstance(ident, dict):
            criteria = [col == ident.get(col.name)
                        for col in primary_keys]
        elif isinstance(ident, (tuple, list)):
            criteria = [col == pyd.get(ident, idx)
                        for idx, col in enumerate(primary_keys)]
        else:
            criteria = [primary_keys[0] == ident]

        return and_(*criteria)

    def _filter_on_primary_key(self, models):
        """Given a set of `models` that have their primary key(s) set and that
        may or may not exist in the database, return a filter that queries for
        those records.

        Args:
            model_class (Model): ORM model class to query against.
            models (list): List of ``dict`` or `model_class` instances to query
                for.

        Returns:
            sqlalchemy.sql.elements.BinaryExpression
        """
        if not isinstance(models, list):
            models = [models]

        pk_columns = self.model_class.pk_columns()

        if len(pk_columns) > 1:
            # Handle the case where there are multiple primary keys. This
            # requires a more complex query than the simpler "where primary_key
            # in (...)".
            pk_criteria = self._filter_on_many_primary_key(models)
        else:
            # Handle single primary key query.
            pk_criteria = self._filter_on_one_primary_key(models)

        return pk_criteria

    def _filter_on_one_primary_key(self, models):
        """Return filter criteria for models with many primary keys."""
        pk_col = self.model_class.pk_columns()[0]
        try:
            ids = pyd.pluck(models, pk_col.name)
        except ValueError:
            # Handles case where `models` is a list of ids already.
            ids = models

        return pk_col.in_(ids)

    def _filter_on_many_primary_key(self, models):
        """Return filter criteria for models with one primary key."""
        pk_cols = self.model_class.pk_columns()
        pk_criteria = []

        def obj_pk_index(idx, col):
            return col.name

        def idx_pk_index(idx, col):
            return idx

        for model in models:
            # AND each primary key value together to query for that record
            # uniquely.
            pk_index = (idx_pk_index if isinstance(model, tuple)
                        else obj_pk_index)
            pk_criteria.append(
                and_(*(col == pyd.get(model, pk_index(idx, col))
                       for idx, col in enumerate(pk_cols))))

        # Our final query is an OR query that ANDs each of the primary keys
        # from each model.
        return or_(*pk_criteria)
