"""
Session
-------

The database session module.
"""

from collections import defaultdict
import typing as t

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.sql import ClauseElement, Executable

from .model import DeclarativeModel


T = t.TypeVar("T")


class Session(orm.Session):
    """
    Manages persistence operations for ORM-mapped objects.

    See Also:
        For full SQLAlchemy documentation: https://docs.sqlalchemy.org/en/latest/orm/session.html
    """

    def all(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.List[t.Any]:
        """
        Return list of objects from execution of `statement`.

        If `statement` is a selectable that returns ORM models, then the model objects will be
        returned.

        If a joined-load is used that requires uniquification, then ``Result.unique()`` will
        automatically be called.

        Args:
            statement: An executable statement such as ``sqlalchemy.select``.
            params: Optional dictionary or list of dictionaries containing bound parameter values.
                If a single dictionary, single-row execution occurs; if a list of dictionaries, an
                "executemany" will be invoked. The keys in each dictionary must correspond to
                parameter names present in the statement.
            execution_options: Optional dictionary of execution options, which will be associated
                with the statement execution.  This dictionary can provide a subset of the options
                that are accepted by ``sqlalchemy._future.Connection.execution_options``, and may
                also provide additional options understood only in an ORM context.
            bind_arguments: Dictionary of additional arguments to determine the bind. May include
                "mapper", "bind", or other custom arguments. Contents of this dictionary are passed
                to the ``Session.get_bind`` method.
        """
        result = self.execute(
            statement, params, execution_options=execution_options, bind_arguments=bind_arguments
        )
        if isinstance(result, sa.engine.CursorResult):
            items = result.all()
        else:
            if (
                result.raw.context.compiled.compile_state
                and result.raw.context.compiled.compile_state.multi_row_eager_loaders
            ):
                result = result.unique()
            items = result.scalars().all()
        return items

    def first(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Optional[t.Any]:
        """
        Return first result of `statement` or ``None`` if no results.

        If `statement` is a selectable that returns ORM models, then the model object will be
        returned.

        Args:
            statement: An executable statement such as ``sqlalchemy.select``.
            params: Optional dictionary or list of dictionaries containing bound parameter values.
                If a single dictionary, single-row execution occurs; if a list of dictionaries, an
                "executemany" will be invoked.  The keys in each dictionary must correspond to
                parameter names present in the statement.
            execution_options: Optional dictionary of execution options, which will be associated
                with the statement execution.  This dictionary can provide a subset of the options
                that are accepted by ``sqlalchemy._future.Connection.execution_options``, and may
                also provide additional options understood only in an ORM context.
            bind_arguments: Dictionary of additional arguments to determine the bind. May include
                "mapper", "bind", or other custom arguments. Contents of this dictionary are passed
                to the ``Session.get_bind`` method.
        """
        result = self.execute(
            statement, params, execution_options=execution_options, bind_arguments=bind_arguments
        )
        if isinstance(result, sa.engine.CursorResult):
            item = result.first()
        else:
            item = result.scalar()
        return item

    def one(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Any:
        """
        Return exactly one result or raise an exception.

        If `statement` is a selectable that returns ORM models, then the model object will be
        returned.

        Args:
            statement: An executable statement such as ``sqlalchemy.select``.
            params: Optional dictionary or list of dictionaries containing bound parameter values.
                If a single dictionary, single-row execution occurs; if a list of dictionaries, an
                "executemany" will be invoked.  The keys in each dictionary must correspond to
                parameter names present in the statement.
            execution_options: Optional dictionary of execution options, which will be associated
                with the statement execution.  This dictionary can provide a subset of the options
                that are accepted by ``sqlalchemy._future.Connection.execution_options``, and may
                also provide additional options understood only in an ORM context.
            bind_arguments: Dictionary of additional arguments to determine the bind. May include
                "mapper", "bind", or other custom arguments. Contents of this dictionary are passed
                to the ``Session.get_bind`` method.
        """
        result = self.execute(
            statement, params, execution_options=execution_options, bind_arguments=bind_arguments
        )
        if isinstance(result, sa.engine.CursorResult):
            item = result.one()
        else:
            item = result.scalar_one()
        return item

    def one_or_none(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Optional[t.Any]:
        """
        Return exactly one result or ``None`` if no results or raise if more than one result.

        If `statement` is a selectable that returns ORM models, then the model object will be
        returned.

        Args:
            statement: An executable statement such as ``sqlalchemy.select``.
            params: Optional dictionary or list of dictionaries containing bound parameter values.
                If a single dictionary, single-row execution occurs; if a list of dictionaries, an
                "executemany" will be invoked.  The keys in each dictionary must correspond to
                parameter names present in the statement.
            execution_options: Optional dictionary of execution options, which will be associated
                with the statement execution.  This dictionary can provide a subset of the options
                that are accepted by ``sqlalchemy._future.Connection.execution_options``, and may
                also provide additional options understood only in an ORM context.
            bind_arguments: Dictionary of additional arguments to determine the bind. May include
                "mapper", "bind", or other custom arguments. Contents of this dictionary are passed
                to the ``Session.get_bind`` method.
        """
        result = self.execute(
            statement, params, execution_options=execution_options, bind_arguments=bind_arguments
        )
        if isinstance(result, sa.engine.CursorResult):
            item = result.one_or_none()
        else:
            item = result.scalar_one_or_none()
        return item

    def save(self, model: T) -> T:
        """
        Save model in the database using insert, update, or upsert on primary key(s).

        See Also:
            See :meth:`.Session.save_all` for more details.

        Args:
            model: Models to save to database.

        Raises:
            - ``TypeError``: On validation errors.
            - ``sqlalchemy.exc.*``: On execution errors.
        """
        if not isinstance(model, DeclarativeModel):
            raise TypeError(f"save not supported for object of type {type(model)}")
        return self.save_all([model])[0]

    def save_all(self, models: t.Union[t.Iterable[t.Any], t.Any]) -> t.List[t.Any]:  # noqa: C901
        """
        Save `models` into the database using insert, update, or upsert on primary key(s).

        It's not required that each model be of the same model class. A mixture of model classes are
        allowed.

        The "upsert" will only occur for models that have their primary key(s) set. Upsert on keys
        other than primary keys is not supported. The "upsert" itself occurs at the
        application-layer only and does not take advantage of any database specific upsert support.
        Therefore, it is possible that there could be a race-condition that would result in an
        ``IntegrityError`` if a model's primary key is set, but not found in the database, but is
        then inserted into the database by another process before this method can insert it.

        If a corresponding model instance with the same primary key(s) as one of the models exists
        in the current sessions but not in the models list, then the model in the models list will
        be merged with the corresponding instance in the session state.

        If multiple instances of a model class in models have the same primary-key, an exception
        will be raised.

        Warning:
            Saving will result in 1 "SELECT" query for every unique model class in the list of
            models. The maximum returned result from each query would be the total not of entities
            of each model class in the list of models.

        Args:
            models: Models to save to database.

        Raises:
            - ``TypeError``: On validation errors.
            - ``sqlalchemy.exc.*``: On execution errors.
        """
        if isinstance(models, DeclarativeModel):
            models = [models]
        else:
            # Cast to list since we may end up modifying the list below and we don't want to mutate
            # passed in arguments.
            models = list(models)

        if not models:
            raise TypeError("save requires at least one object")

        invalid_types = {type(model) for model in models if not isinstance(model, DeclarativeModel)}
        if invalid_types:
            raise TypeError(f"save_all not supported for objects of types {invalid_types}")

        # Model instances that should follow the "insert" path.
        insertable = []

        # Model instances that should follow the "update" path.
        updatable = []

        # Model instances that have their primary key(s) set which may already exist in the
        # database. These models will either be inserted or updated, but some database querying will
        # be required to make that determination.
        mergeable: t.Dict[type, list] = defaultdict(list)

        # Use dictionary of primary-keys to keep track of unique primary key values between models
        # to enforce that all models given must correspond to unique records.
        primary_keys: t.Dict[type, set] = defaultdict(set)

        # Partition models into `insertable` or `mergeable` buckets.
        for idx, model in enumerate(models):
            pk = model_pk(model)

            if any(v is None for v in pk):
                # Primary key not set so add to the insert list.
                insertable.append(model)
            else:
                model_class = type(model)
                if pk in primary_keys[model_class]:
                    raise TypeError(f"save_all duplicate primary-key set for {model_class}: {pk}")

                # Model's primary key is set so it might be mergeable. Keep track of original index
                # because we'll need to update the `models` list with the merged instance.
                mergeable[model_class].append((idx, model_pk(model), model))
                primary_keys[model_class].add(pk)

        # Before we attempt to merge models with existing database records, we want to bulk
        # fetch all of the potentially mergeable models. Doing so will put those models into the
        # session registry which means that when we later call `merge()`, there won't be a
        # database fetch since we've pre-loaded them.
        for model_class, model_set in mergeable.items():
            criteria = pk_filter(*(model for *_, model in model_set))
            stmt = sa.select(model_class).where(criteria)
            results = self.execute(stmt).scalars()
            existing = {model_pk(model): model for model in results}

            for idx, pk, model in model_set:
                if model in self:
                    updatable.append(model)
                elif pk in existing:
                    models[idx] = force_merge(self, existing[pk], model)
                    updatable.append(models[idx])
                else:
                    insertable.append(model)

        self.add_all(insertable)
        self.add_all(updatable)

        return models


def model_pk(model: t.Any) -> t.Tuple[t.Any, ...]:
    """Return tuple of primary-key values for given model instance."""
    mapper: orm.Mapper = sa.inspect(type(model))
    return mapper.primary_key_from_instance(model)


def pk_filter(*models) -> ClauseElement:
    """
    Return SQL filter expression over primary-key values of given models.

    The filter will have the form:

    ::

        (pk_col1 = models[0].pk_col1 ... AND pk_colN = models[0].pk_colN) OR
        (pk_col1 = models[1].pk_col1 ... AND pk_colN = models[1].pk_colN) ... OR
        (pk_col1 = models[M].pk_col1 ... AND pk_colN = models[M].pk_colN)
    """
    mappers: t.Dict[type, orm.Mapper] = {}
    all_pk_filters = []

    for model in models:
        model_class = type(model)
        mapper = mappers.get(model_class)
        if mapper is None:
            mapper = sa.inspect(model_class)
            mappers[model_class] = mapper

        pk_filters = zip(mapper.primary_key, mapper.primary_key_from_instance(model))
        all_pk_filters.append(sa.and_(*(col == val for col, val in pk_filters)))

    return sa.or_(*all_pk_filters)


def force_merge(
    session: orm.Session, model: DeclarativeModel, new_model: DeclarativeModel
) -> DeclarativeModel:
    """Force merge an existing `model` with a `new_model` by copying the primary key values from
    `model` to `new_model` before calling ``session.merge(model)``."""
    mapper: orm.Mapper = sa.inspect(type(model))
    attrs_by_col_name = {
        col_attr.expression.name: attr for attr, col_attr in mapper.column_attrs.items()
    }
    pk_pairs = zip(mapper.primary_key, mapper.primary_key_from_instance(model))
    for col, val in pk_pairs:
        attr = attrs_by_col_name[col.name]
        setattr(new_model, attr, val)
    return session.merge(new_model)