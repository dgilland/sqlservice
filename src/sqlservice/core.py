
from collections import defaultdict
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import DeclarativeMeta

from .utils import is_sequence


@contextmanager
def transaction(session, commit=True, rollback=False, autoflush=None):
    """Nestable session transaction context manager where only a single
    commit will be issued once all contexts have been exited. If an
    exception occurs either at commit time or before, the transaction will
    be rolled back and the original exception re-raised.

    Args:
        session (Session): SQLAlchemy session object.
        commit (bool, optional): Whether to commit the transaction or leave it
            open. Defaults to ``True``.
        rollback (bool, optional): Whether to rollback the transaction.
            Defaults to ``False``. WARNING: This overrides `commit`.
        autoflush (bool, optional): Whether to override ``session.autoflush``.
            Original ``session.autoflush`` will be restored after transaction.
            Defaults to ``None`` which doesn't modify ``session.autoflush``.

    Yields:
        :attr:`session`
    """
    # Keep track of nested calls to this context manager using this
    # "trans_count" counter. Data stored in session.info will be local to
    # that session and persist through its lifetime.
    session.info.setdefault('trans_count', 0)

    # Bump count every time context is entered.
    session.info['trans_count'] += 1

    # Keep track of original autoflush setting so it can be restored after
    # transaction.
    original_autoflush = session.autoflush

    if commit and autoflush is not None:
        session.autoflush = autoflush

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

        # Only finalize transaction once our counter reaches zero.
        if session.info['trans_count'] <= 0:
            if rollback:
                session.rollback()
            elif commit:
                try:
                    session.commit()
                except Exception:
                    session.rollback()
                    raise
    finally:
        # Restore autoflush to original value.
        session.autoflush = original_autoflush


def save(session, models, before=None, after=None, identity=None):
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
        before (function, optional): Function to call before each model is
            saved via ``session.add``. Function should have signature
            ``before(model, is_new)``.
        after (function, optional): Function to call after each model is
            saved via ``session.add``. Function should have signature
            ``after(model, is_new)``.
        identity (function, optional): Function used to return an idenity map
            for a given model. Function should have the signature
            ``identity(model)``. By default :func:`primary_identity_map` is
            used.

    Returns:
        Model: If a single item passed in.
        list: A ``list`` of Model instaces if multiple items passed in.
    """
    if not is_sequence(models):
        as_list = False
        models = [models]
    else:
        as_list = True
        models = list(models)

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
        if identity(model) is not None:
            # Primary key(s) are set so might be mergeable.
            # Keep track of original `idx` because we'll need to update
            # the `models` list with the merged instance.
            mergeable[model.__class__].append((idx, model))
        else:
            # No primary key set so add to the insert list.
            insertable.append(model)

    if mergeable:
        # Before we attempt to merge models with existing database records,
        # we want to bulk fetch all of the potentially mergeable models.
        # Doing so will put those models into the session registry which
        # means that when we later call `merge()`, there won't be a
        # database fetch since we've pre-loaded them.
        for model_class, class_models in mergeable.items():
            criteria = identity_map_filter(
                (model for _, model in class_models), identity=identity)
            query = session.query(model_class).filter(criteria)
            existing = {identity(model): model for model in query}

            for idx, model in class_models:
                ident = identity(model)

                if model in session:
                    updatable.append(model)
                elif ident in existing:
                    models[idx] = _force_merge(session, existing[ident], model)
                    updatable.append(models[idx])
                else:
                    insertable.append(model)

    with transaction(session):
        _add(session, insertable, is_new=True, before=before, after=after)
        _add(session, updatable, is_new=False, before=before, after=after)

    return models if as_list else models[0]


def _force_merge(session, model, new_model):
    """Force merge an existing `model` with a `new_model` by copying the
    primary key values from `model` to `new_model` before calling
    ``session.merge(model)``.
    """
    pk_cols = mapper_primary_key(model.__class__)

    for col in pk_cols:
        setattr(new_model, col.name, getattr(model, col.name, None))

    return session.merge(new_model)


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
    if not is_sequence(models):  # pragma: no cover
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
    if not is_sequence(data) or isinstance(data, tuple):
        data = [data]

    valid_model_class = isinstance(model_class, DeclarativeMeta)

    mapped_data = defaultdict(list)

    for idx, item in enumerate(data):
        item_class = item.__class__

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
                            .format(idx, item, item_class, model_class))

        mapped_data[class_].append(item)

    delete_count = 0

    with transaction(session):
        for model_class, data in mapped_data.items():
            count = (session.query(model_class)
                     .filter(primary_key_filter(data, model_class))
                     .options(orm.lazyload('*'))
                     .delete(synchronize_session=synchronize_session))
            delete_count += count

    return delete_count


def bulk_insert(session, mapper, mappings):
    """Perform a bulk insert into table/statement represented by `mapper`
    while utilizing a special syntax that replaces the tradtional
    ``executemany()`` DBAPI call with a multi-row VALUES clause for a
    single INSERT statement.

    See :meth:`bulk_insert_many` for bulk inserts using ``executemany()``.

    Args:
        session (Session): SQLAlchemy session object.
        mapper: An ORM class or SQLAlchemy insert-statement object.
        mappings (list): List of ``dict`` objects to insert.

    Returns:
        ResultProxy
    """
    if hasattr(mapper, '__table__'):
        insert_stmt = mapper.__table__.insert()
    else:
        insert_stmt = mapper
    return session.execute(insert_stmt.values(mappings))


def bulk_insert_many(session, mapper, mappings):
    """Perform a bulk insert into table/statement represented by `mapper`
    while utilizing the ``executemany()`` DBAPI call.

    See :meth:`bulk_insert` for bulk inserts using a multi-row VALUES
    clause for a single INSERT statement.

    Args:
        session (Session): SQLAlchemy session object.
        mapper: An ORM class or SQLAlchemy insert-statement object.
        mappings (list): List of ``dict`` objects to insert.

    Returns:
        ResultProxy
    """
    if hasattr(mapper, '__table__'):
        insert_stmt = mapper.__table__.insert()
    else:
        insert_stmt = mapper
    return session.execute(insert_stmt.values(), mappings)


def bulk_common_update(session, mapper, key_columns, mappings):
    """Perform a bulk UPDATE on common shared values among `mappings`. What
    this means is that if multiple records are being updated to the same
    values, then issue only a single update for that value-set using the
    identity of the records in the WHERE clause.

    Args:
        session (Session): SQLAlchemy session object.
        mapper: An ORM class or SQLAlchemy insert-statement object.
        key_columns (tuple): A tuple of SQLAlchemy columns that represent
            the identity of each row (typically this would be a table's
            primary key values but they can be any set of columns).
        mappings (list): List of ``dict`` objects to update.

    Returns:
        list[ResultProxy]
    """
    if not isinstance(key_columns, (list, tuple)):
        key_columns = (key_columns,)

    if hasattr(mapper, '__table__'):
        update_stmt = mapper.__table__.update()
    else:
        update_stmt = mapper

    key_col_tuple = sa.tuple_(*key_columns)
    key_col_names = tuple(col.key for col in key_columns)
    identity = _bulk_identity_factory(key_columns)

    updates = defaultdict(list)

    for mapping in mappings:
        data_key = tuple((key, val) for key, val in mapping.items()
                         if key not in key_col_names)
        updates[data_key].append(identity(mapping))

    results = []
    with transaction(session):
        for data_key, identities in updates.items():
            data = dict(data_key)
            stmt = (update_stmt
                    .where(key_col_tuple.in_(identities))
                    .values(data))
            results.append(session.execute(stmt))

    return results


def bulk_diff_update(session,
                     mapper,
                     key_columns,
                     previous_mappings,
                     mappings):
    """Perform a bulk INSERT/UPDATE on the difference between `mappings`
    and `previous_mappings` such that only the values that have changed are
    included in the update. If a mapping in `mappings` doesn't exist in
    `previous_mappings`, then it will be inclued in the bulk INSERT. The
    bulk INSERT will be deferred to :func:`bulk_insert`. The bulk UPDATE
    will be deferred to :func:`bulk_common_update`.

    Args:
        session (Session): SQLAlchemy session object.
        mapper: An ORM class or SQLAlchemy insert-statement object.
        key_columns (tuple): A tuple of SQLAlchemy columns that represent
            the identity of each row (typically this would be a table's
            primary key values but they can be any set of columns).
        previous_mappings (list): List of ``dict`` objects that represent
            the previous values of all mappings found for this update set
            (i.e. these are the current database records).
        mappings (list): List of ``dict`` objects to update.

    Returns:
        list[ResultProxy]
    """
    results = []

    if not mappings:  # pragma: no cover
        return results

    if not isinstance(key_columns, (list, tuple)):
        key_columns = (key_columns,)

    key_col_names = tuple(col.key for col in key_columns)
    identity = _bulk_identity_factory(key_columns)
    previous_mappings_by_key = {identity(previous_mapping): previous_mapping
                                for previous_mapping in previous_mappings}

    ins_mappings = []
    upd_mappings = []

    for mapping in mappings:
        previous_mapping = previous_mappings_by_key.get(identity(mapping))

        if previous_mapping:
            changed = {key: value for key, value in mapping.items()
                       if previous_mapping.get(key) != value}

            if not changed:
                continue

            mapping = {key: value for key, value in mapping.items()
                       if key in changed or key in key_col_names}

            upd_mappings.append(mapping)
        else:
            ins_mappings.append(mapping)

    if not any((ins_mappings, upd_mappings)):
        return results

    with transaction(session):
        if upd_mappings:
            result = bulk_common_update(session,
                                        mapper,
                                        key_columns,
                                        upd_mappings)

        if ins_mappings:
            result = [bulk_insert(session, mapper, ins_mappings)]

        results.extend(result)

    return results


def _bulk_identity_factory(key_columns):
    """Return a function that accepts a dict mapping and returns its identity
    corresponding its key values mapped by `key_columns`.
    """
    return lambda mapping: tuple(mapping.get(col.key) for col in key_columns)


def primary_key_filter(items, model_class):
    """Given a set of `items` that have their primary key(s) set and that
    may or may not exist in the database, return a filter that queries for
    those records.

    Args:
        items (list): List of ``dict`` or `model_class` instances to query.
        model_class (Model): ORM model class to query against.

    Returns:
        sqlalchemy.sql.elements.BinaryExpression
    """
    if not is_sequence(items) or isinstance(items, tuple):  # pragma: no cover
        items = [items]

    pk_columns = model_class.pk_columns()

    if len(pk_columns) > 1:
        # Handle the case where there are multiple primary keys. This
        # requires a more complex query than the simpler "where primary_key
        # in (...)".
        pk_criteria = _many_primary_key_filter(items, model_class)
    else:
        # Handle single primary key query.
        pk_criteria = _one_primary_key_filter(items, model_class)

    return pk_criteria


def _one_primary_key_filter(items, model_class):
    """Return filter criteria for models with one primary key."""
    pk_col = mapper_primary_key(model_class)[0]
    return pk_col.in_(
        item[pk_col.name] if isinstance(item, (dict, model_class)) else item
        for item in items)


def _many_primary_key_filter(items, model_class):
    """Return filter criteria for models with many primary keys."""
    pk_cols = mapper_primary_key(model_class)
    pk_criteria = []

    def obj_pk_index(idx, col):
        return col.name

    def idx_pk_index(idx, col):
        return idx

    for item in items:
        # AND each primary key value together to filter for that record
        # uniquely.
        pk_index = idx_pk_index if isinstance(item, tuple) else obj_pk_index
        pk_criteria.append(
            sa.and_(col == item[pk_index(idx, col)]
                    for idx, col in enumerate(pk_cols)))

    # Our final filter is an OR filter that ANDs each of the primary keys
    # from each model.
    return sa.or_(*pk_criteria)


def make_identity(*columns):
    """Factory function that returns an identity function that can be used in
    :func:`save`. The identity function returns an identity-tuple mapping from
    a model instance with the given `columns` and their values.
    """
    def identity(model):
        return tuple((col, getattr(model, col.key)) for col in columns)
    return identity


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

    if not is_sequence(models):
        models = [models]

    return sa.or_(sa.and_(col == val
                          for col, val in identity(model))
                  for model in models)
