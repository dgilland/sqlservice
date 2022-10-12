API Reference
=============

.. testsetup::

    from sqlservice import *

The sqlservice package imports commonly used objects into it's top-level namespace:

.. code-block:: python

    from sqlservice import (
        AsyncDatabase,
        AsyncSession,
        Database,
        ModelBase,
        ModelMeta,
        Session,
        as_declarative,
        declarative_base,
    )


.. automodule:: sqlservice.database
    :members:

.. automodule:: sqlservice.session
    :members:

.. automodule:: sqlservice.async_database
    :members:

.. automodule:: sqlservice.async_session
    :members:
    :exclude-members: sync_session

.. automodule:: sqlservice.model
    :members:

.. automodule:: sqlservice.event
    :members:
