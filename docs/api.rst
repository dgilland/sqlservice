API Reference
=============

.. testsetup::

    from sqlservice import *

The sqlservice package imports commonly used objects into it's top-level namespace:

.. code-block:: python

    from sqlservice import (
        ModelBase,
        SQLClient,
        SQLQuery,
        as_declarative,
        declarative_base,
        destroy,
        event,
        make_identity,
        save,
        transaction,
    )


.. automodule:: sqlservice.client
    :members:

.. automodule:: sqlservice.query
    :members:

.. automodule:: sqlservice.model
    :members:

.. automodule:: sqlservice.event
    :members:

.. automodule:: sqlservice.core
    :members:
