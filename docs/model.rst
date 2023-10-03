Model
=====

The ``Model`` is the base ORM class that represents your database schema. SQLAlchemy provides a very basic default ORM model class when one calls ``sqlalchemy.ext.declarative.declarative_base``. SQLService extends the basic ORM class to provide a few extra to handle some basic use cases.

The general approach when using ``sqlservice.ModelBase`` is to use it as the base class for your own custom ``Model`` class that extends/overrides ``ModelBase`` to fit your specific needs.

.. code-block:: python

    # in models/base.py
    from sqlalchemy import MetaData
    from sqlservice import ModelBase, as_declarative, declarative_base

    metadata = MetaData()

    @as_declarative(metadata=metadata)
    class Model(ModelBase):
        pass

    # Or using the declarative_base function...
    # Model = declarative_base(ModelBase, metadata=metadata)


.. note::

    All keyword arguments to ``sqlservice.as_declarative`` and ``sqlservice.declarative_base`` will be passed to ``sqlalchemy.ext.declarative.declarative_base``.

From here you can use ``Model`` as the base class for your ORM models.

.. code-block:: python

    # in models/user.py
    from sqlalchemy import Column, types

    from .base import Model

    class User(Model):
        __tablename__ = "user"

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))

        about = orm.relationship("UserAbout", uselist=False)
        devices = orm.relationship("UserDevice")


    class UserAbout(Model):
        __tablename__ = "user_about"

        user_id = Column(types.Integer(), ForeignKey("user.id"), primary_key=True)
        nickname = Column(types.String(100))
        hometown = Column(types.String(100))


    class UserDevice(Model):
        __tablename__ = "user_device"

        id = Column(types.Integer(), primary_key=True)
        user_id = Column(types.Integer(), ForeignKey("user.id"), nullable=False)
        name = Column(types.String(100))

        keys = orm.relationship("UserDeviceKey")


    class UserDeviceKey(Model):
        __tablename__ = "user_device_key"

        id = Column(types.Integer(), primary_key=True)
        device_id = Column(types.Integer(), ForeignKey("user_device.id"), nullable=False)
        key = Column(types.String(100))


Instantiation and Updating
--------------------------

What does ``ModelBase`` provide for you? Out of the box, you'll be able to do things like:

Create a new instance from keyword arguments:

.. code-block:: python

    user = User(name="Bob", email="bob@example.com")


.. note::

    Under the hood ``ModelBase.__init__`` calls ``ModelBase.set()`` so anything ``set()`` does, ``__init__`` does too.


Update an instance using keyword arguments:

.. code-block:: python

    user.set(name="Bob Smith")


Set relationship and nested relationships:

.. code-block:: python

    user = User(
        name="Bob Smith",
        email="bobsmith@example.com",
        about=UserAbout(nickname="Bobby", hometown="Example City"),
        devices=[
            UserDevice(name="device1", keys=[UserDeviceKey(key="key1a"), UserDeviceKey(key="key1b")]),
            UserDevice(name="device2", keys=[UserDeviceKey(key="key2a"), UserDeviceKey(key="key2b")]),
        ]
    )

    user
    # User(id=None, name='Bob Smith', email='bobsmith@example.com')

    user.about
    # UserAbout(user_id=None, nickname='Bobby', hometown='Example City')

    user.devices
    # [UserDevice(id=None, user_id=None, name='device1'),
    #  UserDevice(id=None, user_id=None, name='device2')]

    user.devices[0].keys
    # [UserDeviceKey(id=None, device_id=None, key='key1a'),
    #  UserDeviceKey(id=None, device_id=None, key='key1b')]

    user.devices[1].keys
    # [UserDeviceKey(id=None, device_id=None, key='key2a'),
    #  UserDeviceKey(id=None, device_id=None, key='key2b')]


.. warning::

    Depending on whether you've set up relationship cascades, calling ``ModelBase.set()`` on relationships can result in integrity errors since SQLAlchemy will nullify orphaned relationship models when they are replaced.

    .. code-block:: python

        user.set(devices=[UserDevice(name="device3")])

        with db.begin() as session:
            session.save(user)

        # sqlalchemy.exc.IntegrityError: (raised as a result of Query-invoked autoflush;
        # consider using a session.no_autoflush block if this flush is occurring
        # prematurely) (sqlite3.IntegrityError) NOT NULL constraint failed:
        # user_device.user_id [SQL: 'UPDATE user_device SET user_id=? WHERE
        # user_device.id = ?'] [parameters: ((None, 1), (None, 2))]


Dictionary Serialization
------------------------

Want to serialize your models to ``dict`` objects?

.. code-block:: python

    # Using to_dict() method.
    user.to_dict()

    # Or using dict() builtin.
    dict(user)

    # {
    #     "id": 1,
    #     "name": "Bob Smith",
    #     "email": "bobsmith@example.com",
    #     "about": {"nickname": "Bo", "hometown": "Example City"},
    #     "devices": [
    #         {"id": 1, "name": "device1", "user_id": 1},
    #         {"id": 2, "name": "device2", "user_id": 1},
    #     ],
    # }


As you can see, relationships are serialized too.

But how does this handle lazy loaded models? When serializing, the only data that is serialized is what is already loaded from that database and set on the model instance. This is done to avoid triggering a large number of individual queries on lazily loaded attributes. Essentially, ``Model.to_dict()`` by default only looks at what's already loaded in the object's state and never touches any attributes directly (which could lead to additional database queries). So it's up to you to ensure that your model is loaded with the data you want to be serialized before calling ``to_dict()``. However, as a convenience, if the potential performance impact of lazyloading data is not a concern, then ``Model.to_dict(lazyload=True)`` can be used serialize all columns and relationships regardless of their loaded state. To exclude relationships even if they are loaded, use ``Model.to_dict(exclude_relationships=True)``.

Need to serialize things with more fine-grained control? Then it's recommended to use a separate serialization library like one of the following:

- `marshmallow <https://marshmallow.readthedocs.io>`_ with or without `marshmallow-sqlalchemy <https://marshmallow-sqlalchemy.readthedocs.io>`_
- `pydantic <https://pydantic-docs.helpmanual.io/>`_ with or without `pydantic-sqlalchemy <https://github.com/tiangolo/pydantic-sqlalchemy>`_


Object Identity
---------------

You can get the primary key identity of any model object:

.. code-block:: python

    user.pk()
    # 1


.. note::

    If the model has multiple primary keys, a tuple is returned.


Core-style Querying
-------------------

As an alternative to using ``sqlalchemy.select(User)``, ``sqlalchemy.insert(User)``, ``sqlalchemy.update(User)``, and ``sqlalchemy.delete(User)`` to build a query, the class methods ``Model.select()``, ``Model.insert()``, ``Model.update()``, and ``Model.delete()`` can be used as shorthand instead.

.. code-block:: python

    import sqlalchemy as sa

    with db.session() as session:
        # Instead of this...
        result = session.execute(sa.select(User).join(UserAbout))
        session.execute(sa.insert(User).values(...))
        session.execute(sa.update(User).values(...))
        session.execute(sa.delete(User))

        # You can do this...
        result = session.execute(User.select().join(UserAbout))
        session.execute(User.insert().values(...))
        session.execute(User.update().values(...))
        session.execute(User.delete())
