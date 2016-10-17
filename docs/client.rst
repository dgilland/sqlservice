Client
======

The heart and soul of sqlservice is the :mod:`sqlservice.client` module. It is a gateway to your databases that seamlessly integrates with your declarative Model's metadata to provide an abstraction layer on top of it.

Before we get to the good stuff, let's first start by creating our client database object:

.. code-block:: python

    from sqlservice import SQLClient

    # Let's assume you've define the User, UserAbout, UserDevice, and UserDeviceKey
    # as illustated in the "Model" section of the docs in a module called models.py
    # with a declarative base named Model.
    from models import Model

    config = {'SQL_DATABASE_URI': 'sqlite://'}
    db = SQLClient(config, model_class=Model)


.. note:: For details on the available configuration values, see :class:`sqlservice.client.SQLClient`.


Behind the scenes when ``SQLClient`` is instantiated, several important steps take place:

- A SQLAlchemy engine object is created at ``db.engine``.
- A thread-local, scoped ORM session is created at ``db.session``.
- Instances of :class:`sqlservice.service.SQLService` are created for all declarative model classes associated with ``Model`` that are accessible at ``db.<model_class>`` (more on this below).


Engine
------

Nothing fancy here. The ``db.engine`` is created using ``sqlalchemy.create_engine``.


Session
-------

The ORM session at ``db.session`` is actually a proxy to the object returned by ``sqlalchemy.orm.scoped_session(session_factory)()`` which is the thread-local session object. This proxy attribute is used so that ``db.session`` always returns an instance of the ``Session`` class used by ``orm.sessionmaker``.

You can directly access the session at ``db.session``, but there are several convenience proxy attributes available at the top-level of each ``SQLClient`` instance:

===========  ================  ===========
SQLClient    Session Object    Description
===========  ================  ===========
db.add       session.add       Add an ORM object to the session.
db.add_all   session.add_all   Add a list of ORM objects to the session.
db.delete    session.delete    Mark ORM object for deletion.
db.merge     session.merge     Merge an existing ORM instance with one loaded from the database.
db.execute   session.execute   Execute SQL statement.
db.close     session.close     Close the session. This will rollback any open transactions.
db.flush     session.flush     Flush the session.
db.refresh   session.refresh   Refresh an ORM object by querying the database.
db.commit    session.commit    Commit the session. **NOTE:** The commit call is wrapped in a try/except block that calls ``db.session.rollback()`` on exception before re-raising.
db.rollback  session.rollback  Rollback the session.
db.query     session.query     Perform an ORM query.
===========  ================  ===========

In addition, there are also proxies to the scoped session object:

============  =====================  ===========
SQLClient     Scoped Session Object  Description
============  =====================  ===========
db.remove     Session.remove         Dispose of the current scoped session.
db.close_all  Session.close_all      Close all thread-local session objects.
============  =====================  ===========

And if you want to completely close all sessions and terminate the engine connection, use ``shutdown()``, which will close all thread-local sessions, dispose of the scoped session, and dispose of the engine connection:

.. code-block:: python

    db.shutdown()


Session Query
-------------

The default query class for ``db.query``/``db.session.query`` uses :class:`sqlservice.query.Query` which provides additional methods beyond SQLAlchemy's base query class.

You can paginate results with ``db.query.paginate()``:

.. code-block:: python

    # Return the first 25 results
    db.query(User).paginate(25)
    db.query(User).paginate((25, 1))

    # Return the second 25 results
    db.query(User).paginate((25, 2))


You can filter, paginate, and order results in a single method call with ``db.query.search()``:

.. code-block:: python

    # Criteria is passed in by position and can be a dict-mapping to query.filter_by()
    # or a query expression.
    db.query(User).search({'name': 'Bob'}, User.email.like('%@gmail.com')).all()

    # Pagination and ordering is by keyword argument.
    db.query(User).search(per_page=25, page=2, order_by=User.name).all()


You can `pydash <https://github.com/dgilland/pydash>`_ methods like ``chain()``, ``pluck()``, ``reduce()``, and more:

.. code-block:: python

    # Return a list of users' names.
    db.query(User).pluck('name')

    # Convert a list of users into a dict indexed by id.
    db.query(User).index_by('id')
    db.query(User).index_by(lambda user: user.id)

    # Start a pydash chaining sequence to return a comma-delimited string of ids.
    db.query(User).chain().pluck('id').map(str).join(',').value()

    # Map a function.
    db.query(User).map(repr)

    # Group items into a dict of lists indexed by a column.
    db.query(User).stack_by('name')
    # {'Bob': [<User>, <User>], 'Mike': [<User>]}

    # Or group according to a callback function.
    db.query(User).stack_by(lambda user: user.name[0])
    # {'B': [<User>, <User>], 'M': [<User>]}

    # Reduce users by a callback function
    db.query(User).reduce(lambda result, user: result + user.name, initial='')
    db.query(User).reduce_right(lambda result, user: result + user.name, initial='')


For more details, see the :mod:`sqlservice.query` module.


Application-Level Nested Transactions
-------------------------------------

Some times you may find yourself with several methods that are all self-contained within a transaction:

.. code-block:: python

    def insert_company(db, data):
        with db.transaction():
            db.save(Company(data))

    def insert_company_ledger(db, data):
        with db.transaction():
            db.save(CompanyLedger(data))

    def insert_initial_order(db, data):
        with db.transaction():
            db.save(Order(data))

In all cases, you want to ensure that any of these methods called in isolation will take place within a database transaction. But in addition, you want any combination of these function calls to also be within a single transaction and not in three separate transactions. Essentiall you want behavior like the following:

.. code-block:: python

    def create_company(db, data):
        with db.transaction():
            insert_company(db, data['company'])

    def create_company_and_ledger(db, data):
        with db.transaction():
            insert_company(db, data['company'])
            insert_company_ledger(db, data['ledger'])

    def create_company_and_ledger_and_order(db, data):
        with db.transaction():
            insert_company(db, data['company'])
            insert_company_ledger(db, data['ledger'])
            insert_initial_order(db, data['ledger'])


But you don't want each transaction context to commit if it's a nested transaction.

Not to worry because that's exactly how ``db.transaction`` works. It maintains a session-local transaction count based on the number of times ``db.transaction`` is called so that there will only be a single commit in the top-most transaction context. This means you can define small, transactionally safe functions that can be used on their own or combined with others into larger transactions without having to worry about any of the nested transactions from committing.


ORM Models
----------

Whenever the declarative base Model is passed into ``SQLCLient``, its metadata is available at ``db.metadata``. Several ``metadata`` based methods are then accessible.


Create Model Tables
+++++++++++++++++++

Create all ORM model tables with:

.. code-block:: python

    db.create_all()


This will issue the appropriate SQL DDL statments that can get your database up and running quickly. For full migration integration, see `alembic <http://alembic.zzzcomputing.com/>`_.


Drop Model Tables
+++++++++++++++++

Drop all ORM model tables with:

.. code-block:: python

    db.drop_all()


Reflect Models
++++++++++++++

Reflect existing database schema without predefining ORM models or Table objects:

.. code-block:: python

    db.reflect()
    print(db.tables)


ORM Model Services
------------------

When creating an instance of ``SQLClient``, model services are automatically created and instances of ``SQLService`` classes are stored for each model and are accessible via attribute access. This allows you to proxy ``SQLService`` attributes for a model with:

.. code-block:: python

    db.User.<SQLService attribute>


So now you can save a model:

.. code-block:: python

    # Using a dict.
    user = db.User.save({'name': 'Elliot', 'email': 'mr@example.com'})

    # Using a model.
    user['name'] += ' Alderson'
    db.User.save(user)

    # Using multiple dicts and models.
    users = db.User.save([{...}, {...}, User(...), User(...)])


You can delete a model:

.. code-block:: python

    # Using a primary key value.
    db.User.destroy(134)

    # Using a dict with the primary key.
    db.User.destroy({'id': 134})

    # Using a model.
    db.User.destroy(user)

    # Using multiple values.
    db.User.destroy([134, {'id': 135}, user])


For more details, see the :mod:`sqlservice.service` module.


Generic ORM Model Methods
-------------------------

While working with model services is the recommended way to interact with ORM models, you can save and destroy any ORM model using the ``db.save()`` and ``db.destroy()`` methods directly.


save()
++++++

You can save any ORM model instance with ``db.save()``:

.. code-block:: python

    # Save a single user
    db.save(user1)

    # Define before/after functions around saving a user.
    def before_save_user(model, is_new):
        pass

    def after_save_user(model, is_new):
        pass

    # Save a single user while calling before_save_user() before user is saved
    # and after_save_user() after user is saved.
    db.save(user1, before=before_save_user, after=after_save_user)

    # Save multiple models.
    # NOTE: If before/after supplied, it will be called for each individual model
    # saved.
    db.save([user1, user2, company1, company2])


When saving the SQL client will perform an upsert using the primary key values (if set) of the model(s) being saved. As a result of this, a database query will be issued to select any existing records that may match the models being saved based on their primary key values. This allows you to save model objects that are not yet associated with the SQLAlchemy session's identity map without having to first fetch the object.

This behavior can be overridden by supplying a custom "identity" function that will be applied to the model(s) being saved. The "identity" function must accept a single argument, a model, and return an identity mapping tuple where each tuple item is a 2-element tuple containing a model column object and its value.

For example, if we wanted to upsert using a user's email address, then the identity function would be:

.. code-block:: python

    def user_identity_by_email(model):
        return ((User.email, model.email),)


If you wanted to upsert using a combination of the user's email address and their name, then the identity function function would be:

.. code-block:: python

    def user_identity_by_email_name(model):
        return ((User.email, model.email),
                (User.name, model.name))


You would then pass one of these functions to ``save()``:

.. code-block:: python

    db.save(user, identity=user_identity_by_email)


This effectively allows you to easily create your own upsert methods independent of the database-backend.


destroy()
+++++++++

.. code-block:: python

    # Destroy a single user.
    db.destroy(user1)

    # Destroy multiple models.
    db.destroy([user1, user2, company1, company2])

    # Destroy using primary key only.
    db.destroy(3618, model_class=User)
    db.destroy(3618, model_class=User, synchronize_session=True)


For more details, see the :mod:`sqlservice.client` module.
