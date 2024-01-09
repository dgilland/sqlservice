"""
Async Session
-------------
"""

import typing as t

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession as AsyncSessionBase
from sqlalchemy.sql import Executable

from .model import DeclarativeModel
from .session import copy_model_pk, iter_mergeable_models_by_class, model_pk
from .utils import maybe_apply_unique_filtering


T = t.TypeVar("T")


class AsyncSession(AsyncSessionBase):
    """
    Manages persistence operations for ORM-mapped objects using asyncio.

    See Also:
        https://docs.sqlalchemy.org/en/latest/orm/extensions/asyncio.html
    """

    pass

    async def all(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Sequence[t.Union[sa.Row[t.Any], t.Any]]:
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
        result = await self.execute(
            statement,
            params,
            execution_options=execution_options,  # type: ignore
            bind_arguments=bind_arguments,  # type: ignore
        )
        if isinstance(result, sa.engine.CursorResult):
            # Non-ORM model query.
            items = result.all()
        else:
            # ORM model query.
            result = maybe_apply_unique_filtering(result)
            items = result.scalars().all()
        return items

    async def first(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Optional[t.Union[sa.Row[t.Any], t.Any]]:
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
        result = await self.execute(
            statement,
            params,
            execution_options=execution_options,  # type: ignore
            bind_arguments=bind_arguments,  # type: ignore
        )
        if isinstance(result, sa.engine.CursorResult):
            # Non-ORM model query.
            item = result.first()
        else:
            # ORM model query.
            result = maybe_apply_unique_filtering(result)
            item = result.scalar()
        return item

    async def one(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Union[sa.Row[t.Any], t.Any]:
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
        result = await self.execute(
            statement,
            params,
            execution_options=execution_options,  # type: ignore
            bind_arguments=bind_arguments,  # type: ignore
        )
        if isinstance(result, sa.engine.CursorResult):
            # Non-ORM model query.
            item = result.one()
        else:
            # ORM model query.
            result = maybe_apply_unique_filtering(result)
            item = result.scalar_one()
        return item

    async def one_or_none(
        self,
        statement: Executable,
        params: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        execution_options: t.Optional[t.Mapping[str, t.Any]] = None,
        bind_arguments: t.Optional[t.Mapping[str, t.Any]] = None,
    ) -> t.Optional[t.Union[sa.Row[t.Any], t.Any]]:
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
        result = await self.execute(
            statement,
            params,
            execution_options=execution_options,  # type: ignore
            bind_arguments=bind_arguments,  # type: ignore
        )
        if isinstance(result, sa.engine.CursorResult):
            # Non-ORM model query.
            item = result.one_or_none()
        else:
            # ORM model query.
            result = maybe_apply_unique_filtering(result)
            item = result.scalar_one_or_none()
        return item

    async def save(self, model: T) -> T:
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
        return (await self.save_all([model]))[0]

    async def save_all(self, models: t.Union[t.Iterable[t.Any], t.Any]) -> t.List[t.Any]:
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
            # Cast to list since we may end up modifying the list below, and we don't want to mutate
            # passed in arguments.
            models = list(models)

        if not models:
            raise TypeError("save_all requires at least one object")

        invalid_types = {type(model) for model in models if not isinstance(model, DeclarativeModel)}
        if invalid_types:
            raise TypeError(f"save_all not supported for objects of types {invalid_types}")

        for model_group, select_models_stmt in iter_mergeable_models_by_class(models):
            result = await self.execute(select_models_stmt)
            result = maybe_apply_unique_filtering(result)
            items = result.scalars()

            existing_models_by_pk = {model_pk(model): model for model in items}
            for idx, pk, model in model_group:
                # pylint: disable=unsupported-membership-test
                if model not in self and pk in existing_models_by_pk:
                    copy_model_pk(existing_models_by_pk[pk], model)
                    models[idx] = await self.merge(model)

        self.add_all(models)  # pylint: disable=no-member

        return models
