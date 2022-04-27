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

So what exactly is ``sqlservice`` and what does "the missing SQLAlchemy ORM interface" even mean? SQLAlchemy is a fantastic library and features a superb ORM layer. However, one thing SQLAlchemy lacks is a unified interface for easily interacting with your database. This is where ``sqlservice`` comes in. It's interface layer on top of SQLAlchemy's session manager and ORM layer that provides a single point to manage your database connection/session, create/reflect/drop your database objects, and easily manage model objects.

Features
--------

This library is meant to enhance your usage of SQLAlchemy. SQLAlchemy is great and this library tries to build upon that by providing useful abstractions on top of it.

- Database client that provides SQLAlchemy 2.0-style connection and session factories, a single settings configuration for engines and sessions, and easy access to ORM models, tables, and db-schema related operations.
- Asyncio-compatible database client with same features as regular client.
- Session class enhancements to perform application-side upserts from primary-keys, automatically convert ORM model query results to ORM model instances, and other query unwrapping methods.
- Declarative ORM base class with lazy-loading aware dictionary serialization, class-method helpers for generating SQLAlchemy 2.0-style queries, and decorator-based SQLAlchemy ORM event registration system that can be used at the model class definition level.
- And more!


Requirements
------------

- Python >= 3.7
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
    # Or using the as_declarative class-decorator
    # from sqlservice import ModelBase, as_declarative
    #
    # @as_declarative()
    # class Model(ModelBase):
    #     # can define custom base methods for all models.
    #     pass

    class User(Model):
        __tablename__ = "user"

        id = Column(types.Integer(), primary_key=True)
        name = Column(types.String(100))
        email = Column(types.String(100))
        phone = Column(types.String(10))

        roles = orm.relation("UserRole")

        @event.on_set("phone", retval=True)
        def on_set_phone(self, value):
            # Strip non-numeric characters from phone number.
            return re.sub("[^0-9]", "", value)

    class UserRole(Model):
        __tablename__ = "user_role"

        id = Column(types.Integer(), primary_key=True)
        user_id = Column(types.Integer(), ForeignKey("user.id"), nullable=False)
        role = Column(types.String(25), nullable=False)


Next, configure the database client:

.. code-block:: python

    from sqlservice import Database


    db = Database(
        uri="sqlite:///db.sql",
        isolation_level="SERIALIZABLE",
        echo=True,
        echo_pool=False,
        pool_size=5,
        pool_timeout=30,
        pool_recycle=3600,
        max_overflow=10,
        autoflush=True,
        expire_on_commit=True,
        model_class=Model
    )


Prepare the database by creating all tables:

.. code-block:: python

    db.create_all()


Finally (whew!), start interacting with the database.

Insert a new record in the database:

.. code-block:: python

    user = User(name="Jenny", email="jenny@example.com", phone="555-867-5309")

    with db.begin() as session:
        session.add(user)


Fetch records:

.. code-block:: python

    with db.session() as session:
        assert user is session.get(User, user.id)
        assert user is session.first(User.select().where(User.id == user.id))
        assert user is session.all(User.select().where(User.id == user.id).limit(1))[0]


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

    with db.begin():
        user.phone = "222-867-5309"


Upsert on primary key automatically:

.. code-block:: python

    with db.begin() as session:
        other_user = User(id=1, email="jenny8675309@example.com")
        session.save(other_user)
        assert other_user is user


For more details, please see the full documentation at http://sqlservice.readthedocs.io.



.. |version| image:: http://img.shields.io/pypi/v/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/

.. |build| image:: https://img.shields.io/github/workflow/status/dgilland/sqlservice/Main/master?style=flat-square
    :target: https://github.com/dgilland/sqlservice/actions

.. |coveralls| image:: http://img.shields.io/coveralls/dgilland/sqlservice/master.svg?style=flat-square
    :target: https://coveralls.io/r/dgilland/sqlservice

.. |license| image:: http://img.shields.io/pypi/l/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/
