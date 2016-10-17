Changelog
=========


v0.6.2 (2016-10-17)
-------------------

- Expose ``identity`` argument in ``SQLClient.save`` and ``SQLService.save``.


v0.6.1 (2016-10-17)
-------------------

- Fix bug where the ``models`` variable was mistakenly redefined during loop iteration in ``core.save``.


v0.6.0 (2016-10-17)
-------------------

- Add ``identity`` argument to ``save`` method to allow a custom identity function to support upserting on something other than just the primary key values.
- Make ``Query`` entity methods ``entities``, ``join_entities``, and ``all_entities`` return entity objects instead of model classes. (**breaking change**)
- Add ``Query`` methods ``model_classes``, ``join_model_classes``, and ``all_model_classes`` return the model classes belonging to a query.


v0.5.1 (2016-09-28)
-------------------

- Fix issue where calling ``<Model>.update(data)`` did not correctly update a relationship field when both ``<Model>.<relationship-column>`` and ``data[<relationship-column>]`` were both instances of a model class.


v0.5.0 (2016-09-20)
-------------------

- Allow ``Service.find_one``, ``Service.find``, and ``Query.search`` to accept a list of lists as the criterion argument.
- Rename ModelBase metaclass class attribute from ``ModelBase.Meta`` to ``ModelBase.metaclass``. (**breaking change**)
- Add support for defining the ``metadata`` object on ``ModelBase.metadata`` and having it used when calling ``declarative_base``.
- Add ``metadata`` and ``metaclass`` arguments to ``declarative_base`` that taken precedence over the corresponding class attributes set on the passed in declarative base type.
- Rename Model argument/attribute in ``SQLClient`` to ``__init__`` to ``model_class``. (**breaking change**)
- Remove ``Query.top`` method. (**breaking change**)
- Proxy ``SQLService.__getattr__`` to ``getattr(SQLService.query(), attr)`` so that ``SQLService`` now acts as a proxy to a query instance that uses its ``model_class`` as the primary query entity.
- Move ``SQLService.find`` and ``SQLService.find_one`` to ``Query``.
- Improve docs.


v0.4.3 (2016-07-11)
-------------------

- Fix issue where updating nested relationship values can lead to conflicting state assertion error in SQLAlchemy's identity map.


v0.4.2 (2016-07-11)
-------------------

- Fix missing ``before`` and ``after`` callback argument passing from ``core.save`` to ``core._add``.


v0.4.1 (2016-07-11)
-------------------

- Fix missing ``before`` and ``after`` callback argument passing from ``SQLService.save`` to ``SQLClient.save``.


v0.4.0 (2016-07-11)
-------------------

- Add support for ``before`` and ``after`` callbacks in ``core.save``, ``SQLClient.save``, and ``SQLService.save`` which are invoked before/after ``session.add`` is called for each model instance.


v0.3.0 (2016-07-06)
-------------------

- Support additional engine and session configuration values for ``SQLClient``.

  - New engine config options:

    - ``SQL_ECHO_POOL``
    - ``SQL_ENCODING``
    - ``SQL_CONVERT_UNICODE``
    - ``SQL_ISOLATION_LEVEL``

  - New session config options:

    - ``SQL_EXPIRE_ON_COMMIT``

- Add ``SQLClient.reflect`` method.
- Rename ``SQLClient.service_registry`` and ``SQLClient.model_registry`` to ``services`` and ``models``. (**breaking change**)
- Support ``SQLClient.__getitem__`` as proxy to ``SQLClient.__getattr__`` where both ``db[User]`` and ``db['User']`` both map to ``db.User``.
- Add ``SQLService.count`` method.
- Add ``Query`` methods:

  - ``index_by``: Converts ``Query.all()`` to a ``dict`` of models indexed by ``callback`` (`pydash.index_by <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.index_by>`_)
  - ``stack_by``: Converts ``Query.all()`` to a ``dict`` of lists of models indexed by ``callback`` (`pydash.group_by <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.group_by>`_)
  - ``map``: Maps ``Query.all()`` to a ``callback`` (`pydash.map_ <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.map_>`_)
  - ``reduce``: Reduces ``Query.all()`` through ``callback`` (`pydash.reduce_ <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.reduce_>`_)
  - ``reduce_right``: Reduces ``Query.all()`` through ``callback`` from right (`pydash.reduce_right <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.reduce_right>`_)
  - ``pluck``: Retrieves value of of specified property from all elements of ``Query.all()`` (`pydash.pluck <http://pydash.readthedocs.io/en/latest/api.html#pydash.collections.pluck>`_)
  - ``chain``: Initializes a chain object with ``Query.all()`` (`pydash.chain <http://pydash.readthedocs.io/en/latest/api.html#pydash.chaining.chain>`_)

- Rename ``Query`` properties: (**breaking change**)

  - ``model_classes`` to ``entities``
  - ``joined_model_classes`` to ``join_entities``
  - ``all_model_classes`` to ``all_entities``


v0.2.0 (2016-06-15)
-------------------

- Add Python 2.7 compatibility.
- Add concept of ``model_registry`` and ``service_registry`` to ``SQLClient`` class:

  - ``SQLClient.model_registry`` returns mapping of ORM model names to ORM model classes bound to ``SQLClient.Model``.
  - ``SQLService`` instances are created with each model class bound to declarative base, ``SQLClient.Model`` and stored in ``SQLClient.service_registry``.
  - Access to each model class ``SQLService`` instance is available via attribute access to ``SQLClient``. The attribute name corresponds to the model class name (e.g. given a ``User`` ORM model, it would be accessible at ``sqlclient.User``.

- Add new methods to ``SQLClient`` class:

  - ``save``: Generic saving of model class instances similar to ``SQLService.save`` but works for any model class instance.
  - ``destroy``: Generic deletion of model class instances or ``dict`` containing primary keys where model class is explicitly passed in. Similar to ``SQLService.destroy``.

- Rename ``SQLService.delete`` to ``destroy``. (**breaking change**)
- Change ``SQLService`` initialization signature to ``SQLService(db, model_class)`` and remove class attribute ``model_class`` in favor of instance attribute. (**breaking change**)
- Add properties to ``SQLClient`` class:

  - ``service_registry``
  - ``model_registry``

- Add properties to ``Query`` class:

  - ``model_classes``: Returns list of model classes used to during ``Query`` creation.
  - ``joined_model_classes``: Returns list of joined model classes of ``Query``.
  - ``all_model_classes``: Returns ``Query.model_classes`` + ``Query.joined_model_classes``.

- Remove methods from ``SQLService`` class: (**breaking change**)

  - ``query_one``
  - ``query_many``
  - ``default_order_by`` (default order by determination moved to ``Query.search``)

- Remove ``sqlservice.service.transaction`` decorator in favor of using transaction context manager within methods. (**breaking change**)
- Fix incorrect passing of ``SQL_DATABASE_URI`` value to ``SQLClient.create_engine`` in ``SQLClient.__init__``.


v0.1.0 (2016-05-24)
-------------------

- First release.
