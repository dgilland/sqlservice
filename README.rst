**********
sqlservice
**********

|version| |travis| |coveralls| |license|


The missing SQLAlchemy ORM service layer.


Links
=====

- Project: https://github.com/dgilland/sqlservice
- Documentation: http://sqlservice.readthedocs.io
- PyPI: https://pypi.python.org/pypi/sqlservice/
- TravisCI: https://travis-ci.org/dgilland/sqlservice


Introduction
============

So what exactly is ``sqlservice`` and what does "the missing SQLAlchemy ORM service layer" even mean? The "service layer" in this context is that part of your application that forms your core domain logic. This is where your ORM models meet your database session and all the magic happens.


Features
--------

This library is meant to enhanced your usage of SQLAlchemy. SQLAlchemy is great and this library tries to build upon that by providing useful abstractions on top of it.

- Database client similar to `Flask-SQLAlchemy <http://flask-sqlalchemy.pocoo.org/>`_ and `alchy.DatabaseManager <http://alchy.readthedocs.io/en/latest/api.html#alchy.manager.Manager>`_ that helps manage an ORM scoped session.
- Base class for a declarative ORM Model that makes updating model columns and relationships easier and convert to a dictionary a breeze.
- A decorator based event registration for SQLAlchemy ORM events that can be used at the model class level. No need to register the event handler outside of the class definition.
- A base service class that provides a unified way to save database records as either ORM models or plain dictionaries.
- An application-side nestable transaction context-manager that helps implement pseudo-subtransactions for those that want implicit transaction demarcation, i.e. ``autocommit=False``, without the use of ``subtransactions=True``.
- And more!


History
-------

This library's direct predecessor is `alchy <https://github.com/dgilland/alchy>`_ which itself started as a drop-in replacement for `Flask-SQLAlchemy <http://flask-sqlalchemy.pocoo.org/>`_ combined with new functionality centering around the "fat-model" style. This library takes a different approach and encourages a "fat-service" style. As such, it is primarily a rewrite of alchy with some of its features ported over and improved, some of its features removed, and other features added. With alchy, one's primary interface with the database was through a model class. Whereas with sqlservice, one's primary interface with the database is through a service class.


Requirements
------------

- Python >= 3.4
- `SQLAlchemy <http://www.sqlalchemy.org/>`_ >= 1.0.0
- `pydash <http://pydash.readthedocs.io>`_ >= 3.4.3


Quickstart
==========

First, install using pip:


::

    pip install sqlservice


Then, define some ORM models:

.. code-block:: python

    import re

    from sqlalchemy import Column, ForeignKey, orm, types

    from sqlservice import ModelBase, declarative_base, event


    Model = declarative_base(ModelBase)

    class User(Model):
        __tablename__ = 'user'

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))
        phone = Column(types.String(10))

        roles = orm.relation('UserRole')

        @event.on_set('phone', retval=True)
        def on_set_phone(self, value, oldvalue, initator):
            # Strip non-numeric characters from phone number.
            return re.sub('[^0-9]', '', value)

    class UserRole(Model):
        __tablename__ = 'user_role'

        id = Column(types.Integer(), primary_key=True)
        user_id = Column(types.Integer(), ForeignKey('user.id'), nullable=False)
        role = Column(types.String(25), nullable=False)


Next, configure a database client:

.. code-block:: python

    from sqlservice import SQLClient

    config = {
        'SQL_DATABASE_URI': 'sqlite:///db.sql',
        'SQL_ECHO': True,
        'SQL_POOL_SIZE': 5,
        'SQL_POOL_TIMEOUT': 30,
        'SQL_POOL_RECYCLE': 3600,
        'SQL_MAX_OVERFLOW': 10,
        'SQL_AUTOCOMMIT': False,
        'SQL_AUTOFLUSH': True
    }

    db = SQLClient(config, Model=Model)


Create a service class for our models:

.. code-block:: python

    from sqlservice import SQLService


    class UserService(SQLService):
        model_class = User


Prepare the database by creating all tables:

.. code-block:: python

    db.create_all()


Finally (whew!), start interacting with the database:

.. code-block:: python

    user_service = UserService(db)

    # Insert a new record in the database.
    data = {'name': 'Jenny', 'email': 'jenny@example.com', 'phone': '555-867-5309'}
    user = user_service.save(data)


    # Fetch records.
    assert user is user_service.get(data.id)
    assert user is user_service.find_one(id=user.id)
    assert user is user_service.find(User.id == user.id)[0]

    # Serialize to a dict.
    assert user.to_dict() == {'id': 1,
                              'name': 'Jenny',
                              'email': 'jenny@example.com',
                              'phone': '5558675309'}

    assert dict(user) == user.to_dict()

    # Update the record and save.
    user.phone = '222-867-5309'
    user_service.save(user)

    # Upsert on primary key automatically.
    assert user is user_service({'id': 1,
                                 'name': 'Jenny',
                                 'email': 'jenny@example.com',
                                 'phone': '5558675309'})

    # Delete the model.
    user_service.delete(user)
    # OR user_service.delete([user])
    # OR user_service.delete(user.id)
    # OR user_service.delete(dict(user))


For more details, please see the full documentation at http://sqlservice.readthedocs.org.



.. |version| image:: http://img.shields.io/pypi/v/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/

.. |travis| image:: http://img.shields.io/travis/dgilland/sqlservice/master.svg?style=flat-square
    :target: https://travis-ci.org/dgilland/sqlservice

.. |coveralls| image:: http://img.shields.io/coveralls/dgilland/sqlservice/master.svg?style=flat-square
    :target: https://coveralls.io/r/dgilland/sqlservice

.. |license| image:: http://img.shields.io/pypi/l/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/
