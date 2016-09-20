Model
=====

The ``Model`` is the basic ORM class that represents your database schema. SQLAlchemy provides a very basic default ORM model class when one calls ``sqlalchemy.ext.declarative.declarative_base``. SQLService does one better and provides the basic tools for handling your basic use cases.

The general approach to using ``sqlservice.ModelBase`` is to use it as the base class for your own custom ``Model`` class that extends/overrides ``ModelBase`` to fit your specific needs.

.. code-block:: python

    # in models/base.py
    from sqlservice import ModelBase, declarative_base

    @declarative_base
    class Model(ModelBase):
        pass

    # declarative_base can also be used as a regular function
    # Model = declarative_base(ModelBase)

From there you can use ``Model`` as the base class for your ORM model classes.

.. code-block:: python

    # in models/user.py
    from sqlalchemy import Column, types

    from .base import Model

    class User(Model):
        __tablename__ = 'user'

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))


Instantiation and Updating
--------------------------

What does ``ModelBase`` provide for you? Out of the box, you'll be able to do things like:

Create a new instance from a ``dict`` or keyword arguments:

.. code-block:: python

    user = User({'name': 'Bob', 'email': 'bob@example.com})
    user = User(name='Bob', email='bob@example.com')


.. note:: Under the hood ``ModelBase.__init__`` calls ``update()`` so anything ``update()`` does, ``__init__`` does too.


Update using attribute or item setters:

.. code-block:: python

    user.name = 'Bob Paulson'
    user['name'] = 'Robert Paulson'


Update an instance using a ``dict`` or keyword arguments:

.. code-block:: python

    user.update(name='Bob Smith')
    user.update({'email': 'bobsmith@example.com'})


The ``update()`` method is powerful enough to work with relationships and nested relationships. Consider the following:

.. code-block:: python

    # in models/user.py
    from sqlalchemy import Column, ForeignKey, types, orm

    from .base import Model

    class User(Model):
        __tablename__ = 'user'

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))

        about = orm.relation('UserAbout', uselist=False)
        devices = orm.relation('UserDevice')

    class UserAbout(Model):
        __tablename__ = 'user_about'

        user_id = Column(types.Integer(), ForeignKey('user.id'), primary_key=True)
        nickname = Column(types.String(100))
        hometown = Column(types.String(100))

    class UserDevice(Model):
        __tablename__ = 'user_device'

        id = Column(types.Integer(), primary_key=True)
        user_id = Column(types.Integer(), ForeignKey('user.id'), nullable=False)
        name = Column(types.String(100))

        keys = orm.relation('UserDeviceKey')

    class UserDeviceKey(Model):
        __tablename__ = 'user_device_key'

        id = Column(types.Integer(), primary_key=True)
        device_id = Column(types.Integer(),
                           ForeignKey('user_device.id'),
                           nullable=False))
        key = Column(types.String(100))


You can now easily create a user, user devices, and device keys with a single data structure without having to use the relationship classes directly.

.. code-block:: python

    data = {
        'name': 'Bob Smith',
        'email': 'bobsmith@example.com',
        'about': {
            'nickname': 'Bobby',
            'hometown': 'Example City'
        },
        'devices': [
            {'name': 'device1', 'keys': [{'key': 'key1a'}, {'key': 'key1b'}]},
            {'name': 'device2', 'keys': [{'key': 'key2a'}, {'key': 'key2b'}]}
        ]
    }
    user = User(data)

    user
    # <User(id=None, name='Bob Smith', email='bobsmith@example.com')>

    user.about
    # <UserAbout(user_id=None, nickname='Bobby', hometown='Example City')>

    user.devices
    # [<UserDevice(id=None, user_id=None, name='device1')>,
       <UserDevice(id=None, user_id=None, name='device2')>]

    user.devices[0].keys
    # [<UserDeviceKey(id=None, device_id=None, key='key1a')>,
       <UserDeviceKey(id=None, device_id=None, key='key1b')>]

    user.devices[1].keys
    # [<UserDeviceKey(id=None, device_id=None, key='key2a')>,
       <UserDeviceKey(id=None, device_id=None, key='key2b')>]


This is because ``ModelBase.update()`` works really hard to map ``dict`` keys to the correct relationship model class to automatically create new model instances from those ``dict`` objects. It works for relationships that are ``1:1`` or ``1:M``.

In addition, when you update the model with relationship data, it will nest calls to the relationship class' ``update()`` methods.

.. code-block:: python

    user.update({'about': {'nickname': 'Bo'}})
    user.about
    # <UserAbout(user_id=None, nickname='Bo', hometown='Example City')>


.. warning::

    Depending on whether you've set up relationship cascades, calling ``update()`` on relationships can result in integrity errors since SQLAlchemy will nullify orphaned relationship models when they are replaced.

    .. code-block:: python

        user.update({'devices': [{'name': 'device3'}]})
        db.save(user)

        # sqlalchemy.exc.IntegrityError: (raised as a result of Query-invoked autoflush;
        # consider using a session.no_autoflush block if this flush is occurring
        # prematurely) (sqlite3.IntegrityError) NOT NULL constraint failed:
        # user_device.user_id [SQL: 'UPDATE user_device SET user_id=? WHERE
        # user_device.id = ?'] [parameters: ((None, 1), (None, 2))]


Dictionary Serialization
------------------------

Want to serialize your models to ``dict`` objects?

.. code-block:: python

    user.to_dict()
    dict(user)
    # {'id': 1,
       'devices': [{'id': 1, 'name': 'device1', 'user_id': 1}, {'id': 2, 'name': 'device2', 'user_id': 1}],
       'name': 'Bob Smith',
       'email': 'bobsmith@example.com'}

As you can see, relationships are serialized too.

But how does this handle lazy loaded models? When serializing the only data that is serialized is what is already loaded. This is done to avoid triggerring a large number of individual queries on lazily loaded attributes. Essentially, ``Model.to_dict()`` only looks at what's already present in ``user.__dict__`` and never touches any attributes directly (which could lead to additional queries). So it's up to you to ensure that your model is loaded with the data you want to be serialized before calling ``to_dict()``.


Object Identity
---------------

You can get the primary key identity of any model object:

.. code-block:: python

    user.identity()
    # 1


.. note:: If the model has multiple primary keys, a tuple is returned


Class Methods and Properties
----------------------------

The ``Model`` class includes other useful methods as well:


.. code-block:: python

    User.class_mapper()
    # <Mapper at 0x7fd9e7443b70; User>

    User.columns()
    # (Column('id', Integer(), table=<user>, primary_key=True, nullable=False),
       Column('name', String(length=100), table=<user>),
       Column('email', String(length=100), table=<user>))

    User.pk_columns()
    # (Column('id', Integer(), table=<user>, primary_key=True, nullable=False),)

    User.relationships()
    # (<RelationshipProperty at 0x7fd9ead007b8; about>,
       <RelationshipProperty at 0x7fd9e7421f28; devices>)

    for descriptor in User.descriptors():
        (str(descriptor), repr(descriptor))
    # User.about, <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x7fd9e743f728>
    # User.devices, <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x7fd9e743f780>
    # User.name, <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x7fd9e743f938>
    # User.email, <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x7fd9e743f9e8>
    # User.id, <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x7fd9e743f888>
