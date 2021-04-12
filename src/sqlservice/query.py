"""
Query
-----

The query module.
"""

import sqlalchemy as sa
from sqlalchemy import orm

from . import core
from .utils import flatten, is_sequence, raise_for_class_if_not_supported


class SQLQuery(orm.Query):
    """Extended SQLAlchemy query class."""

    @property
    @raise_for_class_if_not_supported
    def entities(self):  # pragma: no cover
        """
        Return list of entity classes for query.

        Warning:
            This is no longer supported in SQLAlchemy>=1.4.
        """
        try:
            return self._entities
        except AttributeError:
            return NotImplemented

    @property
    @raise_for_class_if_not_supported
    def join_entities(self):  # pragma: no cover
        """
        Return list of the joined entity classes for query.

        Warning:
            This is no longer supported in SQLAlchemy>=1.4.
        """
        try:
            return self._join_entities
        except AttributeError:
            return NotImplemented

    @property
    def mapper_entities(self):
        """Return mapper entities for query."""
        try:
            return tuple(self._mapper_entities)
        except AttributeError:  # pragma: no cover
            return tuple(
                raw_col._annotations["parententity"]
                for raw_col in self._raw_columns
                if "parententity" in raw_col._annotations
            )

    @property
    def model_class(self):
        """Return primary model class if query generated using ``session.query(model_class)`` or
        ``None`` otherwise."""
        try:
            entity = self._only_full_mapper_zero("")
        except Exception:
            class_ = None
        else:
            class_ = entity.mapper.class_

        return class_

    @property
    def model_classes(self):
        """Return model classes used as selectable for query."""
        return tuple(entity.mapper.class_ for entity in self.mapper_entities)

    @property
    def join_model_classes(self):
        """Return model classes contained in joins for query."""
        try:
            return tuple(enity.mapper.class_ for enity in self._join_entities if enity.mapper)
        except AttributeError:  # pragma: no cover
            return tuple(
                join[0]._annotations["parententity"].class_
                for join in self._legacy_setup_joins
                if "parententity" in join[0]._annotations
            )

    @property
    @raise_for_class_if_not_supported
    def all_entities(self):  # pragma: no cover
        """
        Return list of all entities for query.

        Warning:
            This is no longer supported in SQLAlchemy>=1.4.
        """
        try:
            return tuple(list(self.entities) + list(self.join_entities))
        except NotImplementedError:
            return NotImplemented

    @property
    def all_model_classes(self):
        """Return list of all model classes for query."""
        return tuple(list(self.model_classes) + list(self.join_model_classes))

    def _only_model_class_zero(self, methname):
        """Return :attr:`model_class` or raise an exception."""
        model_class = self.model_class

        if not model_class:  # pragma: no cover
            raise sa.exc.InvalidRequestError(
                f"{methname}() can only be used against a single mapped class."
            )

        return model_class

    def save(self, data, before=None, after=None, identity=None):
        """
        Save `data` into the database using insert, update, or upsert-on-primary-key.

        Warning:
            This requires that the ``Query`` has been generated using ``Query(<ModelClass>)``;
            otherwise, and exception will be raised.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict`` objects
        - ``list``/``tuple`` of :attr:`model_class` instances

        This method will attempt to do the "right" thing by mapping any items in `data` that have
        their primary key set with the corresponding record in the database if it exists.

        Args:
            data (mixed): Data to save to database.
            before (function, optional): Function to call before each model is saved via
                ``session.add``. Function should have signature ``before(model, is_new)``.
            after (function, optional): Function to call after each model is saved via
                ``session.add``. Function should have signature ``after(model, is_new)``.
            identity (function, optional): Function used to return an idenity map for a given model.
                Function should have the signature ``identity(model)``. Defaults to
                :func:`.core.primary_identity_map`.

        Returns:
            :attr:`model_class`: If a single item passed in.
            list: A ``list`` of :attr:`model_class` when multiple items passed.

        Raises:
            InvalidRequestError: When :attr:`model_class` is ``None``.
        """
        model_class = self._only_model_class_zero("save")

        if is_sequence(data):
            models = (
                model_class(item) if not isinstance(item, model_class) else item for item in data
            )
        elif not isinstance(data, model_class):
            models = model_class(data)
        else:
            models = data

        return core.save(self.session, models, before=before, after=after, identity=identity)

    def destroy(self, data, synchronize_session=False):
        """
        Delete bulk records identified by `data`.

        Warning:
            This requires that the ``Query`` has been generated using ``Query(<ModelClass>)``;
            otherwise, and exception will be raised.

        The `data` argument can be any of the following:

        - ``dict``
        - :attr:`model_class` instance
        - ``list``/``tuple`` of ``dict`` objects
        - ``list``/``tuple`` of :attr:`model_class` instances

        Args:
            data (mixed): Data to delete from database.
            synchronize_session (bool|str): Argument passed to ``Query.delete``.

        Returns:
            int: Number of deleted records.

        Raises:
            InvalidRequestError: When :attr:`model_class` is ``None``.
        """
        if not data:
            return

        model_class = self._only_model_class_zero("destroy")

        return core.destroy(
            self.session,
            data,
            model_class=model_class,
            synchronize_session=synchronize_session,
        )

    def paginate(self, pagination):
        """
        Return paginated query.

        Args:
            pagination (tuple|int): A ``tuple`` containing ``(per_page, page)`` or an ``int`` value
                for ``per_page``.

        Returns:
            Query: New :class:`Query` instance with ``limit`` and ``offset`` parameters applied.
        """
        query = self
        page = 1
        per_page = None

        if isinstance(pagination, (list, tuple)):
            per_page = pagination[0] if len(pagination) > 0 else per_page
            page = pagination[1] if len(pagination) > 1 else page
        else:
            per_page = pagination

        if per_page:
            # Disable false-positive until fixed upstream.
            query = query.limit(per_page)

        if page and page > 1 and per_page:
            query = query.offset((page - 1) * per_page)

        return query

    def search(self, *criterion, **kwargs):
        """
        Return search query object.

        Args:
            *criterion (sqlaexpr, optional): SQLA expression to filter against.

        Keyword Args:
            per_page (int, optional): Number of results to return per page. Defaults to ``None``
                (i.e. no limit).
            page (int, optional): Which page offset of results to return. Defaults to ``1``.
            order_by (sqlaexpr, optional): Order by expression. Defaults to ``None``.

        Returns:
            Query: New :class:`Query` instance with criteria and parameters applied.
        """
        order_by = kwargs.get("order_by")
        page = kwargs.get("page")
        per_page = kwargs.get("per_page")

        model_class = self.model_class

        if order_by is None and model_class:
            order_by = core.mapper_primary_key(self.model_class)

        query = self

        for criteria in flatten(criterion):
            # If we have keyword (dict) criteria, we want to apply it to the base model (if present)
            # instead of the last joined model.
            if isinstance(criteria, dict) and model_class:
                criteria = (getattr(model_class, key) == val for key, val in criteria.items())

            if isinstance(criteria, dict):
                query = query.filter_by(**criteria)
            else:
                if not is_sequence(criteria):
                    query = query.filter(criteria)
                else:
                    query = query.filter(*criteria)

        if order_by is not None:
            if not is_sequence(order_by):
                query = query.order_by(order_by)
            else:
                query = query.order_by(*order_by)

        if per_page or page:
            query = query.paginate((per_page, page))

        return query

    def find_one(self, *criterion, **criterion_kwargs):
        """
        Return a single model or ``None`` given `criterion` ``dict`` or keyword arguments.

        Args:
            criterion (dict, optional): Filter-by dict.
            **criterion_kwargs (optional): Mapping of filter-by arguments.

        Returns:
            :attr:`model_class`: When filtered record exists.
            None: When filtered record does not exist.
        """
        criterion = list(criterion) + [criterion_kwargs]

        return self.search(*criterion).first()

    def find(self, *criterion, **kwargs):
        """
        Return list of models matching `criterion`.

        Args:
            *criterion (sqlaexpr, optional): SQLA expression to filter against.

        Keyword Args:
            per_page (int, optional): Number of results to return per page. Defaults to ``None``
                (i.e. no limit).
            page (int, optional): Which page offset of results to return. Defaults to ``1``.
            order_by (sqlaexpr, optional): Order by expression. Defaults to ``None``.

        Returns:
            list: List of :attr:`model_class`
        """
        return self.search(*criterion, **kwargs).all()
