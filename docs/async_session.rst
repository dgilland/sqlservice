Async Session
=============

The :class:`sqlservice.async_session.AsyncSession` class is the main interface for asynchronously querying a database. It extends ``sqlalchemy.ext.asyncio.AsyncSession`` to provide additional methods for common ORM queries.

Executing Select Statements
---------------------------

The following methods will execute a select-statement and return results. If the select-statement is based on ORM-models (e.g. ``User.select()``), then the results will automatically be converted into ORM-model instances.

- :meth:`sqlservice.async_session.AsyncSession.all`
- :meth:`sqlservice.async_session.AsyncSession.first`
- :meth:`sqlservice.async_session.AsyncSession.one`
- :meth:`sqlservice.async_session.AsyncSession.one_or_none`

.. code-block:: python

    from datetime import datetime, timedelta

    import sqlalchemy as sa

    session = db.session()

    users = await session.all(User.select())
    users = await session.all(
        sa.text("SELECT * FROM users WHERE timestamp > :timestamp"),
        params={"timestamp": datetime.now() - timedelta(days=1)}
    )

Saving Models
-------------

The  :meth:`sqlservice.async_session.AsyncSession.save` and  :meth:`sqlservice.async_session.AsyncSession.save_all` method can be used to save model instances. These method differ from ``AsyncSession.add`` and ``AsyncSession.add_all`` in that they will automatically upsert records based on the model's primary key(s). This allows models that were not loaded from the database to be automatically inserted or updated independent of the database backend.

- :meth:`sqlservice.async_session.AsyncSession.save`
- :meth:`sqlservice.async_session.AsyncSession.save_all`

.. code-block:: python

    user_1 = User(id=4, name='Max')
    user_2 = User(id=8, name='Jack')
    user_3 = User(id=1, name='Wes'')

    saved_user_1 = await session.save(user_1)
    assert save_user_1 is user_1

    saved_users_2_3 = await session.save_all([user_2, user_3])
    assert saved_users_2_3[0] is user_2
    assert saved_users_2_3[1] is user_3
