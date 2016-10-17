# -*- coding: utf-8 -*-
"""
Query
-----

The query module.
"""

import pydash as pyd
import sqlalchemy as sa
from sqlalchemy import orm

from . import core


class Query(orm.Query):
    """Extended SQLAlchemy query class."""

    @property
    def entities(self):
        """Return list of entity classes for query."""
        return self._entities

    @property
    def join_entities(self):
        """Return list of the joined entity classes for query."""
        return self._join_entities

    @property
    def mapper_entities(self):
        """Return mapper entities for query."""
        return tuple(self._mapper_entities)

    @property
    def model_classes(self):
        """Return model classes used as selectable for query."""
        return tuple(entity.mapper.class_ for entity in self.mapper_entities)

    @property
    def join_model_classes(self):
        """Return model classes contained in joins for query."""
        return tuple(enity.mapper.class_ for enity in self._join_entities
                     if enity.mapper)

    @property
    def all_entities(self):
        """Return list of all entities for query."""
        return tuple(list(self.entities) +
                     list(self.join_entities))

    @property
    def all_model_classes(self):
        """Return list of all model classes for query."""
        return tuple(list(self.model_classes) +
                     list(self.join_model_classes))

    def paginate(self, pagination):
        """Return paginated query.

        Args:
            pagination (tuple|int): A ``tuple`` containing ``(per_page, page)``
                or an ``int`` value for ``per_page``.

        Returns:
            Query: New :class:`Query` instance with ``limit`` and ``offset``
                parameters applied.
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
            query = query.limit(per_page)

        if page and page > 1 and per_page:
            query = query.offset((page - 1) * per_page)

        return query

    def search(self, *criterion, **kargs):
        """Return search query object.

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
            Query: New :class:`Query` instance with criteria and parameters
                applied.
        """
        order_by = kargs.get('order_by')
        page = kargs.get('page')
        per_page = kargs.get('per_page')

        if order_by is None and self.model_classes:
            order_by = core.mapper_primary_key(self.model_classes[0])

        query = self

        for criteria in pyd.flatten(criterion):
            if isinstance(criteria, dict):
                query = query.filter_by(**criteria)
            else:
                query = query.filter(criteria)

        if order_by is not None:
            if not isinstance(order_by, (list, tuple)):
                order_by = [order_by]
            query = query.order_by(*order_by)

        if per_page or page:
            query = query.paginate((per_page, page))

        return query

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

        return self.search(*criterion).first()

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
        return self.search(*criterion, **kargs).all()

    def chain(self):
        """Return pydash chaining instance with items returned by
        :meth:`all`.

        See Also:
            `pydash's <http://pydash.readthedocs.org/>`_ documentation on
            `chaining <http://pydash.readthedocs.org/en/latest/chaining.html>`_
        """
        return pyd.chain(self.all())

    def index_by(self, callback=None):
        """Index items returned by :meth:`all` using `callback`."""
        return pyd.index_by(self.all(), callback)

    def stack_by(self, callback=None):
        """Group items returned by :meth:`all` using `callback`."""
        return pyd.group_by(self.all(), callback)

    def map(self, callback=None):
        """Map `callback` to each item returned by :meth:`all`."""
        return pyd.map_(self.all(), callback)

    def reduce(self, callback=None, initial=None):
        """Reduce :meth:`all` using `callback`."""
        return pyd.reduce_(self.all(), callback, initial)

    def reduce_right(self, callback=None, initial=None):
        """Reduce reversed :meth:`all` using `callback`."""
        return pyd.reduce_right(self.all(), callback, initial)

    def pluck(self, column):
        """Pluck `column` attribute values from :meth:`all` results and
        return as list.
        """
        return pyd.pluck(self.all(), column)
