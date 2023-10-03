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

- Sync and asyncio database clients to manage ORM sessions with enhanced session classes.
- Base class for a declarative ORM Model that makes updating model columns and relationships easier and converting to a dictionary a breeze.
- Decorator-based event register for SQLAlchemy ORM events that can be used at the model class level. No need to register the event handler outside of the class definition.
- And more!


Requirements
------------

- Python >= 3.7
- `SQLAlchemy <http://www.sqlalchemy.org/>`_ >= 2.0


Quickstart
==========

First, install using pip:


::

    pip install sqlservice


Then, define some ORM models:

.. code-block:: python

    import re
    import typing as t

    from sqlalchemy import ForeignKey, orm, types
    from sqlalchemy.orm import Mapped, mapped_column

    from sqlservice import declarative_base, event


    Model = declarative_base()

    class User(Model):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(types.Integer(), primary_key=True)
        name: Mapped[t.Optional[str]] = mapped_column(types.String(100))
        email: Mapped[t.Optional[str]] = mapped_column(types.String(100))
        phone: Mapped[t.Optional[str]] = mapped_column(types.String(10))

        roles: Mapped[t.List["UserRole"]] = orm.relationshipship("UserRole")

        @event.on_set("phone", retval=True)
        def on_set_phone(self, value):
            # Strip non-numeric characters from phone number.
            return re.sub("[^0-9]", "", value)


    class UserRole(Model):
        __tablename__ = "user_role"

        id: Mapped[int] = mapped_column(types.Integer(), primary_key=True)
        user_id: Mapped[int] = mapped_column(types.Integer(), ForeignKey("user.id"), nullable=False)
        role: Mapped[str] = mapped_column(types.String(25), nullable=False)


Next, configure the database client:

.. code-block:: python

    from sqlservice import AsyncDatabase, Database

    db = Database(
        "sqlite:///db.sql",
        model_class=Model,
        isolation_level="SERIALIZABLE",
        echo=True,
        echo_pool=False,
        pool_size=5,
        pool_timeout=30,
        pool_recycle=3600,
        max_overflow=10,
        autoflush=True,
    )

    # Same options as above are supported but will default to compatibility with SQLAlchemy asyncio mode.
    async_db = AsyncDatabase("sqlite:///db.sql", model_class=Model)


Prepare the database by creating all tables:

.. code-block:: python

    db.create_all()
    await async_db.create_all()


Finally (whew!), start interacting with the database.

Insert a new record in the database:

.. code-block:: python

    user = User(name='Jenny', email=jenny@example.com, phone='555-867-5309')
    with db.begin() as session:
        session.save(user)

    async with db.begin() as session:
        await session.save(user)


Fetch records:

.. code-block:: python

    session = db.session()
    assert user is session.get(User, user.id)
    assert user is session.first(User.select())
    assert user is session.all(User.select().where(User.id == user.id)[0]


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
    with db.begin() as session:
        session.save(user)

    async with async_db.begin() as session:
        await session.save(user)


Upsert on primary key automatically:

.. code-block:: python

    other_user = User(id=1, name="Jenny", email="jenny123@example.com", phone="5558675309")
    with db.begin() as session:
        session.save(other_user)
    assert user is other_user

For more details, please see the full documentation at http://sqlservice.readthedocs.io.



.. |version| image:: http://img.shields.io/pypi/v/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/

.. |build| image:: https://img.shields.io/github/actions/workflow/status/dgilland/sqlservice/main.yml?branch=master&style=flat-square
    :target: https://github.com/dgilland/sqlservice/actions

.. |coveralls| image:: http://img.shields.io/coveralls/dgilland/sqlservice/master.svg?style=flat-square
    :target: https://coveralls.io/r/dgilland/sqlservice

.. |license| image:: http://img.shields.io/pypi/l/sqlservice.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqlservice/
