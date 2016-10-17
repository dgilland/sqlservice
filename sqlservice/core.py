# -*- coding: utf-8 -*-

from collections import defaultdict
from contextlib import contextmanager

import pydash as pyd
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import DeclarativeMeta

from ._compat import iteritems


@contextmanager
def transaction(session, readonly=False):
    """Nestable session transaction context manager where only a single
    commit will be issued once all contexts have been exited. If an
    exception occurs either at commit time or before, the transaction will
    be rolled back and the original exception re-raised.

    Args:
        session (Session): SQLAlchemy session object.

    Yields:
        :attr:`session`
    """
    session.info.setdefault('autoflush', session.autoflush)

    # Keep track of nested calls to this context manager using this
    # "trans_count" counter. Data stored in session.info will be local to
    # that session and persist through its lifetime.
    session.info.setdefault('trans_count', 0)

    # Bump count every time context is entered.
    session.info['trans_count'] += 1

    if not readonly:
        # Disable autoflush during write transactions. Autoflush can cause
        # issues when setting ORM relationship values in cases where
        # consistency is only maintained at commit time but would fail if
        # an autoflush occurred beforehand.
        session.autoflush = False

    try:
        yield session
    except Exception:
        # Only rollback if we haven't rolled back yet (i.e. one
        # rollback only per nested transaction set).
        if session.info['trans_count'] > 0:
            session.rollback()

        # Reset trans_count to zero to prevent other rollbacks as the
        # exception bubbles up the call stack.
        session.info['trans_count'] = 0

        raise
    else:
        session.info['trans_count'] -= 1

        # Paranoia dictates that we compare with "<=" instead of "==".
        # Only commit once our trans counter reaches zero.
        if not readonly and session.info['trans_count'] <= 0:
            try:
                session.commit()
            except Exception:
                session.rollback()
                raise
    finally:
        # Restore autoflush setting once transaction is over.
        if session.info['trans_count'] <= 0:
            session.autoflush = session.info['autoflush']
            session.info['trans_count'] = 0


def save(session, models, identity=None, before=None, after=None):
    """Save `models` into the database using insert, update, or
    upsert on `identity`.

    The `models` argument can be any of the following:

    - Model instance
    - ``list``/``tuple`` of Model instances

    The required function signature and return of `identity` is:

    ::

        def identity(model):
            return ((column1, value1), (column2, value2), ... (colN, valN))

    For example, a model with a single primary key, the return might look like:

    ::

        model = Model(id=1)
        identity(model) == ((Model.id, 1))

    And for a model with a composite primary key:

    ::

        model = Model(id1=1, id2=2)
        identity(model) == ((Model.id1, 1), (Model.id2, 2))

    This requirement is necessary so that the :func:`save` function can
    correctly generate a query filter to select existing records to determine
    which of those records should be updated vs inserted.

    If no `identity` function is provided, then :func:`primary_identity_map`
    will be used which will result in upserting on primary key.

    Args:
        session (Session): SQLAlchemy session object.
        models (mixed): Models to save to database.
        identity (function, optional): Function used to return an idenity map
            for a given model. Function should have the signature
            ``identity(model)``. By default :func:`primary_identity_map` is
            used.
        before (function, optional): Function to call before each model is
            saved via ``session.add``. Function should have signature
            ``before(model, is_new)``.
        after (function, optional): Function to call after each model is
            saved via ``session.add``. Function should have signature
            ``after(model, is_new)``.

    Returns:
        Model: If a single item passed in.
        list: A ``list`` of Model instaces if multiple items passed in.
    """
    if not isinstance(models, (list, tuple)):
        # Data was not passed in a list/tuple so we'll want to return it
        # the same.
        as_list = False
        models = [models]
    else:
        as_list = True

    if identity is None:
        identity = primary_identity_map

    # Model instances that should follow the "insert" path.
    insertable = []

    # Model instances that should follow the "update" path.
    updatable = []

    # Model instances that have their primary key(s) set which may already
    # exist in the database. These models will either be inserted or
    # updated, but some database querying will be required to make that
    # determination.
    mergeable = defaultdict(list)

    # Parition models into `addable` or `mergeable` buckets.
    for idx, model in enumerate(models):
        model_class = type(model)

        if identity(model) is not None:
            # Primary key(s) are set so might be mergeable.
            # Keep track of original `idx` because we'll need to update
            # the `models` list with the merged instance.
            mergeable[model_class].append((idx, model))
        else:
            # No primary key set so add to the insert list.
            insertable.append(model)

    if mergeable:
        # Before we attempt to merge models with existing database records,
        # we want to bulk fetch all of the potentially mergeable models.
        # Doing so will put those models into the session registry which
        # means that when we later call `merge()`, there won't be a
        # database fetch since we've pre-loaded them.
        for model_class, class_models in iteritems(mergeable):
            criteria = identity_map_filter(
                [model for _, model in class_models],
                identity=identity)

            existing = session.query(model_class).filter(criteria).all()
            existing = {identity(model): model for model in existing}

            for idx, model in class_models:
                if model in session:
                    updatable.append(model)
                elif identity(model) in existing:
                    models[idx] = model = session.merge(model)
                    updatable.append(model)
                else:
                    insertable.append(model)

    with transaction(session):
        _add(session, insertable, is_new=True, before=before, after=after)
        _add(session, updatable, is_new=False, before=before, after=after)

    return models if as_list else models[0]


def _add(session, models, is_new=None, before=None, after=None):
    """Add `model` into database using `session`.

    .. note::

        Function is primarily used by :func:`save` to make support for
        `before` and `after` handlers easier.

    Args:
        models (list): Model instances.
        is_new (bool): Indicates whether `models` are new or existing
            records in database. Value is strictly used to indicate to
            `before` and `after` functions whether models are new or
            not.
        before (function, optional): Function to call before each model is
            saved via ``session.add``. Function should have signature
            ``before(model, is_new)``.
        after (function, optional): Function to call after each model is
            saved via ``session.add``. Function should have signature
            ``after(model, is_new)``.
    """
    if not isinstance(models, (list, tuple)):  # pragma: no cover
        models = [models]

    for model in models:
        if before:
            before(model, is_new)

        session.add(model)

        if after:
            after(model, is_new)


def destroy(session, data, model_class=None, synchronize_session=False):
    """Delete bulk `data`.

    The `data` argument can be any of the following:

    - Single instance of `model_class`
    - List of `model_class` instances
    - Primary key value (single value or ``tuple`` of values for composite
      keys)
    - List of primary key values.
    - Dict containing primary key(s) mapping
    - List of dicts with primary key(s) mappings

    If a non-`model_class` instances are passed in, then `model_class` is
    required to know which table to delete from.

    Args:
        session (Session): SQLAlchemy session object.
        data (mixed): Data to delete from database.
        synchronize_session (bool|str): Argument passed to
            ``Query.delete``.

    Returns:
        int: Number of deleted records.
    """
    if not isinstance(data, list):
        data = [data]

    valid_model_class = isinstance(model_class, DeclarativeMeta)

    mapped_data = defaultdict(list)
    for idx, item in enumerate(data):
        item_class = type(item)

        if not isinstance(item_class, DeclarativeMeta) and valid_model_class:
            class_ = model_class
        else:
            class_ = item_class

        if not isinstance(class_, DeclarativeMeta):
            raise TypeError('Type of value given to destory() function is not '
                            'a valid SQLALchemy declarative class and/or '
                            'model class argument is not valid. '
                            'Item with index {0} and with value "{1}" is '
                            'an instance of "{2}" and model class is {3}.'
                            .format(idx, item, type(item), model_class))

        mapped_data[class_].append(item)

    delete_count = 0

    with transaction(session):
        for model_class, data in iteritems(mapped_data):
            count = (session.query(model_class)
                     .filter(primary_key_filter(data, model_class))
                     .options(orm.lazyload('*'))
                     .delete(synchronize_session=synchronize_session))
            delete_count += count

    return delete_count


def primary_key_filter(data, model_class):
    """Given a set of `data` that have their primary key(s) set and that
    may or may not exist in the database, return a filter that queries for
    those records.

    Args:
        data (list): List of ``dict`` or `model_class` instances to query.
        model_class (Model): ORM model class to query against.

    Returns:
        sqlalchemy.sql.elements.BinaryExpression
    """
    if not isinstance(data, list):  # pragma: no cover
        data = [data]

    pk_columns = model_class.pk_columns()

    if len(pk_columns) > 1:
        # Handle the case where there are multiple primary keys. This
        # requires a more complex query than the simpler "where primary_key
        # in (...)".
        pk_criteria = _many_primary_key_filter(data, model_class)
    else:
        # Handle single primary key query.
        pk_criteria = _one_primary_key_filter(data, model_class)

    return pk_criteria


def _one_primary_key_filter(data, model_class):
    """Return filter criteria for models with one primary key."""
    pk_col = mapper_primary_key(model_class)[0]
    try:
        ids = pyd.pluck(data, pk_col.name)
    except ValueError:
        # Handles case where `data` is a list of ids already.
        ids = data

    return pk_col.in_(ids)


def _many_primary_key_filter(data, model_class):
    """Return filter criteria for models with many primary keys."""
    pk_cols = mapper_primary_key(model_class)
    pk_criteria = []

    def obj_pk_index(idx, col):
        return col.name

    def idx_pk_index(idx, col):
        return idx

    for item in data:
        # AND each primary key value together to filter for that record
        # uniquely.
        pk_index = (idx_pk_index if isinstance(item, tuple)
                    else obj_pk_index)
        pk_criteria.append(
            sa.and_(*(col == pyd.get(item, pk_index(idx, col))
                      for idx, col in enumerate(pk_cols))))

    # Our final filter is an OR filter that ANDs each of the primary keys
    # from each model.
    return sa.or_(*pk_criteria)


def identity_filter(ident, model_class):
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
        model_class (object): Model class to produce filter for.

    Returns:
        dict
    """
    pk_cols = mapper_primary_key(model_class)

    if isinstance(ident, dict):
        criteria = (col == ident.get(col.name)
                    for col in pk_cols)
    elif isinstance(ident, (tuple, list)):
        criteria = (col == pyd.get(ident, idx)
                    for idx, col in enumerate(pk_cols))
    else:
        criteria = (pk_cols[0] == ident,)

    return sa.and_(*criteria)


def mapper_primary_key(model_class):
    """Return primary keys of `model_class`."""
    try:
        return sa.inspect(model_class).primary_key
    except Exception:  # pragma: no cover
        pass


def primary_identity_map(model):
    """Return identity-map of a model as a N-element tuple where N is the
    number of primary key columns. Each element of the tuple is a 2-element
    tuple containing the primary key column and the corresponding model value.
    """
    pk_columns = mapper_primary_key(model.__class__)
    identity = sa.inspect(model.__class__).identity_key_from_instance(model)[1]

    if all(val is None for val in identity) or not pk_columns:
        identity = None
    else:
        identity = tuple((pk_col, identity[idx])
                         for idx, pk_col in enumerate(pk_columns))

    return identity


def primary_identity_value(model):
    """Return primary key identity of model instance. If there is only a
    single primary key defined, this function returns the primary key value.
    If there are multiple primary keys, a tuple containing the primary key
    values is returned.
    """
    id_map = primary_identity_map(model)

    if id_map:
        identity = tuple(val for col, val in id_map)
    else:  # pragma: no cover
        identity = None

    if identity and len(identity) == 1:
        identity = identity[0]

    return identity


def identity_map_filter(models, identity=None):
    """Return SQLAlchemy filter expression for a list of `models` based on the
    identity map returned by the given `identity` function.
    """
    if identity is None:
        identity = primary_identity_map

    if not isinstance(models, (tuple, list)):
        models = [models]

    return sa.or_(sa.and_(col == val
                          for col, val in identity(model))
                  for model in models)
