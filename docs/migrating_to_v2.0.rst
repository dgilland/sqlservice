.. _migrating_to_v2.0:

Migrating to v2.0
=================

The sqlservice ``v2`` release represents a major design change with many breaking changes. It has been overhauled to implement the patterns that are recommended by SQLAlchemy's 2.0 Core and ORM usage guidelines. Migrating from sqlservice ``v1`` to ``v2`` will require many changes and possibly some new design pattern implementations. This guide will help with the transition to ``2.0``.

.. seealso::

    Learn more about the new 2.0 style from the `SQLAlchemy 1.4 / 2.0 Tutorial <https://docs.sqlalchemy.org/en/14/tutorial/>`_.


New Database Class Replaces SQLClient
-------------------------------------

**breaking change**

.. code-block:: python

    # sqlservice 1.x
    from sqlservice import SQLClient

    db_v1 = SQLClient("sqlite://")


    # sqlservice 2.0
    from sqlservice import Database

    db = Database("sqlite://")


The old ``SQLClient`` class was essentially a singleton session instance that used a session that was threadlocal. Using ``db_v1.session`` within the same thread would always refer to the same session instance. Having two or more sessions generated from the same ``SQLClient.engine`` in a single thread wasn't supported. It also had many proxy properties to the underlying SQLAlchemy Session object so that ``db_v1.session.<method>`` could be shortened to ``db_v1.<method>``.

The new ``Database`` class is more like a factory wrapper around the underlying SQLAlchemy Engine that better supports the SQLAlchemy 2.0 session management usage patterns. SQLAlchemy Session and Connection instances are created through ``Database`` context-managers. There is no longer a session instance maintained within a ``Database`` instance. Additional Session level features are instead added to the new ``sqlservice.Session`` class that extends SQLAlchemy's Session class.


Executing a Query
+++++++++++++++++

The differences between executing queries using connections and sessions are minimal.

Using sqlservice 1.x:

.. code-block:: python

    import sqlalchemy as sa

    # 1.x

    # using Connection.execute
    with db_v1.engine.connect() as conn:
        conn.execute("SELECT 1")

    # using Connection transaction
    with db_v1.engine.begin() as conn:
        conn.execute("SELECT 1")
    # COMMIT emitted after context manager exits

    # using Session.execute
    db_v1.execute("SELECT 1")

    # using Session transaction
    with db_v1.transaction():
        db_v1.session.execute("SELECT 1")


Using sqlservice 2.0:

.. code-block:: python

    # 2.0

    # using Connection.execute
    with db.connect() as conn:
        conn.execute(sa.text("SELECT 1"))

    # using Connection transaction
    with db.engine.begin() as conn:
        conn.execute(sa.text("SELECT 1"))
    # COMMIT emitted after context manager exits

    # using Session.execute
    with db.session() as session:
        session.execute(sa.text("SELECT 1"))

    # using Session transaction
    with db.begin() as session:
        session.execute(sa.select(1))


Managing an ORM Transaction
+++++++++++++++++++++++++++

The custom transaction method/decorator available in sqlservice 1.x has been removed in favor of using a SQLAlchemy Session object directly.

Using sqlservice 1.x:

.. code-block:: python

    with db_v1.transaction():
        user1 = User()
        user2 = User()
        db_v1.add(user1)
        db_v1.add(user2)
        db_v1.add_all([User(), User()])


Using sqlservice 2.0:

.. code-block:: python

    with db.begin() as session:
        user1 = User()
        user2 = User()
        session.add(user1)
        session.add(user2)
        session.add_all([User(), User()])


Save Method Moved to Session & before, after, and identity Arguments Removed
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The ``SQLClient.save`` method has moved to ``sqlservice.Session``.

Using sqlservice 1.x:

.. code-block:: python

    user = User()
    db_v1.save(
        user,
        before=lambda: 'execute before saving',
        after=lambda: 'execute after saving',
        identity=lambda model: 'return custom identifier for model'
    )


Using sqlservice 2.0:

.. code-block:: python

    user = User()
    with db.begin() as session:
        session.save(user)
        # before, after, and identity removed


Bulk Save Functionality Moved to New save_all Method
++++++++++++++++++++++++++++++++++++++++++++++++++++

Bulk saving of models is now done with ``sqlservice.Session.save_all``.

Using sqlservice 1.x:

.. code-block:: python

    users = [User(), User(), User()]
    db_v1.save(users)


Using sqlservice 2.0:

.. code-block:: python

    users = [User(), User(), User()]
    with db.begin() as session:
        session.save_all(users)


Query Class Removed
-------------------

Since SQLAlchemy 1.4, the ``session.query`` pattern is considered legacy and will be removed in its 2.0 version. Similarly, it has also been removed in sqlservice 2.0.


Model Class is Leaner
---------------------

In sqlservice 1.x, a model could be instantiated/updated using either a single dictionary argument or multiple keyword-arguments. Extra dictionary keys or keyword-arguments not mapped to the class were ignored. This is has changed in sqlservice 2.0:

- ``Model.__init__()`` and ``Model.set()`` (formerly ``Model.update()``) only support keyword-arguments. Passing a dictionary instance is no longer supported. **breaking change**
- However, creation of a model using a dictionary can be done using ``Model.from_dict()``.
- Using extra keyword-arguments or dictionary keys (when using ``Model.from_dict()``) when creating or updating a model will now raise an exception. **breaking change**
- Model is no longer scriptable (i.e. ``model_instance["column_name"]`` is not supported). **breaking change**

Other breaking changes:

- ``Model.update()`` renamed to ``Model.set()``. ``Model.update()`` is now a ``classmethod`` that returns a ``sqlalchemy.Update`` instance for use in query building. **breaking change**
- ``Model.identity()`` renamed to ``Model.pk()``. **breaking change**
- ``Model.identity_map()`` removed. **breaking change**
- Class methods that proxied ``sqlalchemy.orm.Mapper`` attributes have been removed. Use ``sqlalchemy.inspect(MyModel)`` directly instead. **breaking change**
- The class attribute ``Model.__dict_args__`` as a way to customize the ``Model.to_dict()`` serialization has been removed. Use of a custom serialization implementation or a serialization library is recommended instead. **breaking change**


Events
------

The event decorators have been made easier to use with the following changes:

- Decorated methods no longer require all event callback arguments to be defined in the method signature. For example, if the sqlalchemy event emitter would send 4 arguments, the sqlservice event-decorated method could define just 1 argument in its function signature and not cause an exception when called.
- The mapper based events (``before_delete``, ``before_insert``, ``before_update``, ``before_save``, ``after_delete``, ``after_insert``, ``after_update``, and ``after_save``) have their callback argument order reversed so that the first argument would correspond to the ``self`` argument of the class. This means that before ``v2``, the callback argument order was ``(mapper, connection, self)`` but in ``v2`` it is ``(self, connection, mapper)``. This was done so that the class method definitions would conform to the standard of having ``self`` as the first argument. **breaking change**
