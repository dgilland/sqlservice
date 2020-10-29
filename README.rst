sqlservice
**********

|version| |build| |coveralls| |license|


The missing SQLAlchemy ORM interface.


Links
=====

- Project: https://github.com/dgilland/sqlservice
- Documentation: http://sqlservice.readthedocs.io
- PyPI: https://pypi.python.org/pypi/sqlservice/
- Github Actions: https://github.com/dgilland/sqlservice/actions


Introduction
============

So what exactly is ``sqlservice`` and what does "the missing SQLAlchemy ORM interface" even mean? SQLAlchemy is a fantastic library and features a superb ORM layer. However, one thing SQLAlchemy lacks is a unified interface for easily interacting with your database through your ORM models. This is where ``sqlservice`` comes in. It's interface layer on top of SQLAlchemy's session manager and ORM layer that provides a single point to manage your database connection/session, create/reflect/drop your database objects, and easily persist/destroy model objects.

Features
--------

This library is meant to enhance your usage of SQLAlchemy. SQLAlchemy is great and this library tries to build upon that by providing useful abstractions on top of it.

- Database client that helps manage an ORM scoped session.
- Base class for a declarative ORM Model that makes updating model columns and relationships easier and converting to a dictionary a breeze.
- Decorator-based event register for SQLAlchemy ORM events that can be used at the model class level. No need to register the event handler outside of the class definition.
- An application-side nestable transaction context-manager that helps implement pseudo-subtransactions for those that want implicit transaction demarcation, i.e. session autocommit, without using session subtransactions.
- And more!


Requirements
------------

- Python >= 3.4
- `SQLAlchemy <http://www.sqlalchemy.org/>`_ >= 1.0.0


Quickstart
==========

First, install using pip:


::

    pip3 install sqlservice


Then, define some ORM models:

.. code-block:: python

    import re

    from sqlalchemy import Column, ForeignKey, orm, types

    from sqlservice import declarative_base, event


    Model = declarative_base()

    class User(Model):
        __tablename__ = "user"

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))
        phone = Column(types.String(10))

        roles = orm.relation("UserRole")

        @event.on_set("phone", retval=True)
        def on_set_phone(self, value, oldvalue, initator):
            # Strip non-numeric characters from phone number.
            return re.sub("[^0-9]", "", value)

    class UserRole(Model):
        __tablename__ = "user_role"

        id = Column(types.Integer(), primary_key=True)
        user_id = Column(types.Integer(), ForeignKey("user.id"), nullable=False)
        role = Column(types.String(25), nullable=False)


Next, configure the database client:

.. code-block:: python

    from sqlservice import SQLClient

    config = {
        "SQL_DATABASE_URI": "sqlite:///db.sql",
        "SQL_ISOLATION_LEVEL": "SERIALIZABLE",
        "SQL_ECHO": True,
        "SQL_ECHO_POOL": False,
        "SQL_CONVERT_UNICODE": True,
        "SQL_POOL_SIZE": 5,
        "SQL_POOL_TIMEOUT": 30,
        "SQL_POOL_RECYCLE": 3600,
        "SQL_MAX_OVERFLOW": 10,
        "SQL_AUTOCOMMIT": False,
        "SQL_AUTOFLUSH": True,
        "SQL_EXPIRE_ON_COMMIT": True
    }

    db = SQLClient(config, model_class=Model)


Prepare the database by creating all tables:

.. code-block:: python

    db.create_all()


Finally (whew!), start interacting with the database.

Insert a new record in the database:

.. code-block:: python

    data = {'name': 'Jenny', 'email': 'jenny@example.com', 'phone': '555-867-5309'}
    user = db.User.save(data)


Fetch records:

.. code-block:: python

    assert user is db.User.get(data.id)
    assert user is db.User.find_one(id=user.id)
    assert user is db.User.find(User.id == user.id)[0]


Serialize to a ``dict``:

.. code-block:: python

    assert user.to_dict() == {
        "id": 1,
        "name": "Jenny",
        "email": "jenny@example.com",
        "phone": "5558675309"
    }

    assert dict(user) == user.to_dict()


Update the record and save:

.. code-block:: python

    user.phone = '222-867-5309'
    db.User.save(user)


Upsert on primary key automatically:

.. code-block:: python

    assert user is db.User(
        {
            "id": 1,
            "name": "Jenny",
            "email": "jenny@example.com",
            "phone": "5558675309"
        }
    )


Destroy the model record:

.. code-block:: python

    db.User.destroy(user)
    # OR db.User.destroy([user])
    # OR db.User.destroy(user.id)
    # OR db.User.destroy([user.id])
    # OR db.User.destroy(dict(user))
    # OR db.User.destroy([dict(user)])


For more details, please see the full documentation at http://sqlservice.readthedocs.io.



.. |version| image:: http://img.shields.io/pypi/v/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/

.. |build| image:: https://img.shields.io/github/workflow/status/dgilland/sqlservice/Main/master?style=flat-square
    :target: https://github.com/dgilland/sqlservice/actions

.. |coveralls| image:: http://img.shields.io/coveralls/dgilland/sqlservice/master.svg?style=flat-square
    :target: https://coveralls.io/r/dgilland/sqlservice

.. |license| image:: http://img.shields.io/pypi/l/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/
