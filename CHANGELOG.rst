Changelog
=========


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
