Session
=======

The :class:`sqlservice.session.Session` class is the main interface for querying a database. It extends ``sqlalchemy.orm.Session`` to provide additional methods for common ORM queries.

Executing Select Statements
---------------------------

The following methods will execute a select-statement and return results. If the select-statement is based on ORM-models (e.g. ``User.select()``), then the results will automatically be converted into ORM-model instances.

- :meth:`sqlservice.session.Session.all`
- :meth:`sqlservice.session.Session.first`
- :meth:`sqlservice.session.Session.one`
- :meth:`sqlservice.session.Session.one_or_none`

.. code-block:: python

    from datetime import datetime, timedelta

    import sqlalchemy as sa

    session = db.session()

    users = session.all(User.select())
    users = session.all(
        sa.text("SELECT * FROM users WHERE timestamp > :timestamp"),
        params={"timestamp": datetime.now() - timedelta(days=1)}
    )

Saving Models
-------------

The  :meth:`sqlservice.session.Session.save` and  :meth:`sqlservice.session.Session.save_all` method can be used to save model instances. These method differ from ``Session.add`` and ``Session.add_all`` in that they will automatically upsert records based on the model's primary key(s). This allows models that were not loaded from the database to be automatically inserted or updated independent of the database backend.

- :meth:`sqlservice.session.Session.save`
- :meth:`sqlservice.session.Session.save_all`

.. code-block:: python

    user_1 = User(id=4, name='Max')
    user_2 = User(id=8, name='Jack')
    user_3 = User(id=1, name='Wes'')

    saved_user_1 = session.save(user_1)
    assert save_user_1 is user_1

    saved_users_2_3 = session.save_all([user_2, user_3])
    assert saved_users_2_3[0] is user_2
    assert saved_users_2_3[1] is user_3
