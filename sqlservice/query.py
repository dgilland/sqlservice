# -*- coding: utf-8 -*-
"""
Query
-----

The query module.
"""

from sqlalchemy import orm


class Query(orm.Query):
    """Extended SQLAlchemy query class."""

    def top(self, count=1):
        """Return top query results up to `count` records. If ``count == 1``,
        then return a single record or ``None``, otherwise, return a ``list``
        of records.

        Warning:
            This method **does not** apply a ``LIMIT`` to the query. It is
            generally meant to be used when it's expected that the query result
            will return a single record or very few records but where the query
            contains one-to-many type joins. Depending on the loading strategy
            used in the query, setting a ``LIMIT`` on one-to-many type joins
            can result in the query not returning the full set of records for
            the "many" side of the join. By not applying a ``LIMIT``, we can
            rely on SQLAlchemy to map the joined records properly so that when
            we return a subset of the records, those records will have their
            relationship records fully populated.

        Args:
            count (int, optional): Return this maximum number of records.
                Defaults to ``1``.

        Returns:
            object: When ``count == 1``.
            list: When ``count != 1``.
        """
        results = self.all()

        if results and count == 1:
            results = results[0]
        elif count == 1:
            results = None
        else:
            results = results[:count]

        return results

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

    def search(self, *criterion, per_page=None, page=None, order_by=None):
        """Return search query object.

        Args:
            *criterion (sqlaexpr, optional): SQLA expression to filter against.

        Keyword Args:
            paginate (tuple(per_page, page), optional): Tuple contain
                ``(per_page, page)`` arguments for pagination. Defaults to
                ``None``.
            order_by (sqlaexpr, optional): Order by expression. Defaults to
                ``None``.

        Returns:
            Query: New :class:`Query` instance with criteria and parameters
                applied.
        """
        query = self

        for criteria in criterion:
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
