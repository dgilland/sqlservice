"""
Async Session
-------------
"""

from sqlalchemy.ext.asyncio import AsyncSession as AsyncSessionBase


class AsyncSession(AsyncSessionBase):
    """
    Manages persistence operations for ORM-mapped objects using asyncio.

    See Also:
        https://docs.sqlalchemy.org/en/latest/orm/extensions/asyncio.html
    """

    pass
