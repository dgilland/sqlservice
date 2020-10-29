Event
=====

SQLAlchemy features an ORM event API but one thing that is lacking is a way to register event handlers in a declarative way inside the Model's class definition. To bridge this gap, this module contains a collection of decorators that enable this kind of functionality.

Instead of having to write event registration like this:

.. code-block:: python

    from sqlalchemy import event

    from myproject import Model


    class User(Model):
        _id = Column(types.Integer(), primary_key=True)
        email = Column(types.String())


    def set_email_listener(target, value, oldvalue, initiator):
        print("received 'set' event for target: {0}".format(target))
        return value


    def before_insert_listener(mapper, connection, target):
        print("received 'before_insert' event for target: {0}".format(target))


    event.listen(User.email, "set", set_email_listener, retval=True)
    event.listen(User, "before_insert", before_insert_listener)


Model Events allows one to write event registration more succinctly as:

.. code-block:: python

    from sqlservice import event

    from myproject import Model


    class User(Model):
        _id = Column(types.Integer(), primary_key=True)
        email = Column(types.String())

        @event.on_set("email", retval=True)
        def on_set_email(target, value, oldvalue, initiator):
            print "received set event for target: {0}".format(target)
            return value

        @event.before_insert()
        def before_insert(mapper, connection, target):
            print ("received 'before_insert' event for target: {0}".format(target))

For details on each event type's expected function signature, see
`SQLAlchemy's ORM Events <http://docs.sqlalchemy.org/en/latest/orm/events.html>`_.

For a full listing of sqlservice event decorators, see the :mod:`sqlservice.event`.
