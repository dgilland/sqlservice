# -*- coding: utf-8 -*-

import mock
import pytest
import sqlalchemy as sa

import pydash as pyd
from pydash.chaining import Chain

from sqlservice import core

from .fixtures import AModel, BModel, CModel, DModel, parametrize, random_alpha


DATASET = [
    {AModel: {'name': random_alpha()},
     BModel: {'name': random_alpha()}},
    {AModel: {'name': random_alpha(),
              'c': {'name': random_alpha()},
              'ds': [{'name': random_alpha()},
                     {'name': random_alpha()}]},
     BModel: {'name': random_alpha()}},
    {AModel: {'id': 1000, 'name': random_alpha()},
     BModel: {'id1': 1000, 'id2': 1000, 'name': random_alpha()}},
]


@pytest.fixture(params=[
    AModel,
    BModel
])
def model_query(request, db):
    """Test fixture that parametrizes service class instances for model
    classes.
    """
    return db[request.param]


@pytest.fixture(params=DATASET)
def data_pool(request):
    """Test fixture that parametrizes a common dataset for testing service
    CRUD.
    """
    return request.param


@pytest.fixture(params=DATASET)
def model_pool(request, db):
    """Test fixture that parametrizes model records while inserting them into
    the database first.
    """
    _model = {}
    for model_class, data in request.param.items():
        _model[model_class] = model_class(data)
        db.add(_model[model_class])
    db.commit()

    return _model


@pytest.fixture(params=[
    {AModel: 10, BModel: 10},
])
def models_pool(request, db):
    """Test fixture that parametrizes multiple model records while inserting
    them into the database first. Mainly used for bulk loading of data.
    """
    _models = {}
    for model_class, count in request.param.items():
        _models[model_class] = [model_class({'name': random_alpha()})
                                for _ in range(count)]
        db.add_all(_models[model_class])
    db.commit()
    return _models


def test_model_query_classes(db):
    """Test SQLQuery.model_classes/join_model_classes/all_model_classes."""
    model_classes = (AModel,)
    join_model_classes = (CModel, DModel)

    query = db.query(*model_classes).join(*join_model_classes)

    assert query.model_classes == model_classes
    assert query.join_model_classes == join_model_classes
    assert query.all_model_classes == tuple(list(query.model_classes) +
                                            list(query.join_model_classes))


def test_query_entities(db):
    """Test SQLQuery.entities/join_entities/all_entities."""
    entities = [AModel.id]
    join_entities = [CModel, DModel]

    query = db.query(*entities).join(*join_entities)

    assert [ent.expr for ent in query.entities] == entities
    assert [ent.mapper.class_ for ent in query.join_entities] == join_entities
    assert query.all_entities == tuple(list(query.entities) +
                                       list(query.join_entities))


def test_find(model_query, models_pool):
    """Test basic find call."""
    models = models_pool[model_query.model_class]
    ret = model_query.find()

    assert len(ret) == len(models)
    assert set(ret) == set(models)


def test_find_criteria_as_filter(model_query, models_pool):
    """Test that find criteria can be passed in as a filter expression."""
    models = models_pool[model_query.model_class]

    for model in models:
        criteria = (getattr(model_query.model_class, key) == model[key]
                    for key in model.columns().keys())
        ret = model_query.find(*criteria)

        assert len(ret) == 1
        assert ret[0] is model


def test_find_one_criteria_as_filter(model_query, models_pool):
    """Test that find_one criteria can be passed in as a filter expression."""
    models = models_pool[model_query.model_class]

    for model in models:
        criteria = (getattr(model_query.model_class, key) == model[key]
                    for key in model.columns().keys())
        ret = model_query.find_one(*criteria)

        assert ret is model


def test_find_criteria_as_filter_by(model_query, models_pool):
    """Test that find criteria can be passed in as a filter-by dict."""
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find({key: model[key]
                               for key in model.columns().keys()})

        assert len(ret) == 1
        assert ret[0] is model


def test_find_one_criteria_as_filter_by(model_query, models_pool):
    """Test that find_one criteria can be passed in as a filter-by dict."""
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find_one({key: model[key]
                                   for key in model.columns().keys()})

        assert ret is model


def test_find_criteria_as_filter_and_filter_by(model_query, models_pool):
    """Test that find criteria can be passed as both filter expression and
    filter-by dict.
    """
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find({'name': model.name},
                               model_query.model_class.name == model.name)

        assert len(ret) == 1
        assert ret[0] is model


def test_find_one_criteria_as_filter_and_filter_by(model_query, models_pool):
    """Test that find_one criteria can be passed as both filter expression and
    filter-by dict.
    """
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find_one({'name': model.name},
                                   model_query.model_class.name == model.name)

        assert ret is model


def test_find_criteria_as_list_of_lists(model_query, models_pool):
    """Test that find criteria can be passed as both filter expression and
    filter-by dict.
    """
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find([{'name': model.name},
                               model_query.model_class.name == model.name])

        assert len(ret) == 1
        assert ret[0] is model

        ret = model_query.find([{'name': model.name}],
                               [model_query.model_class.name == model.name])

        assert len(ret) == 1
        assert ret[0] is model


def test_find_one_criteria_as_list_of_lists(model_query, models_pool):
    """Test that find_one criteria can be passed as both filter expression and
    filter-by dict.
    """
    models = models_pool[model_query.model_class]

    for model in models:
        ret = model_query.find_one([{'name': model.name},
                                   model_query.model_class.name == model.name])

        assert ret is model

        ret = model_query.find_one(
            [{'name': model.name}],
            [model_query.model_class.name == model.name])

        assert ret is model


@parametrize('per_page,page,index', [
    (3, 0, slice(0, 3)),
    (3, None, slice(0, 3)),
    (3, 1, slice(0, 3)),
    (3, 2, slice(3, 6)),
])
def test_find_paginate(model_query, models_pool, per_page, page, index):
    """Test that find can paginate results."""
    ret = model_query.find(per_page=per_page, page=page)
    recs = model_query.all()

    assert ret == recs[index]


@parametrize('order_by', [
    {AModel: AModel.name,
     BModel: BModel.name},
    {AModel: AModel.name.desc(),
     BModel: BModel.name.desc()},
    {AModel: sa.text('name'),
     BModel: sa.text('name')},
    {AModel: sa.text('name DESC'),
     BModel: sa.text('name DESC')},
    {AModel: [AModel.name, AModel.id],
     BModel: [BModel.name, BModel.id1]},
])
def test_find_order_by(model_query, models_pool, order_by):
    """Test that find and order results."""
    order_by = order_by[model_query.model_class]

    recs = (model_query
            .order_by(*([order_by] if not isinstance(order_by, list)
                        else order_by))
            .all())
    ret = model_query.find(order_by=order_by)

    assert ret == recs


@parametrize('model_class,data,callback,expected', [
    (AModel,
     [{'name': 'a'}, {'name': 'b'}],
     lambda model: model.name * 2,
     ['aa', 'bb']),
])
def test_query_map(db, model_class, data, callback, expected):
    """Test SQLQuery.map."""
    db.save([model_class(item) for item in data])
    assert db.query(model_class).map(callback) == expected


@parametrize('model_class,data,callback,initial,expected', [
    (AModel,
     [{'name': 'a'}, {'name': 'b'}],
     lambda ret, model: ret + model.name,
     '',
     'ab'),
])
def test_query_reduce(db, model_class, data, callback, initial, expected):
    """Test SQLQuery.reduce."""
    db.save([model_class(item) for item in data])
    result = db.query(model_class).reduce(callback, initial=initial)
    assert result == expected


@parametrize('model_class,data,callback,initial,expected', [
    (AModel,
     [{'name': 'a'}, {'name': 'b'}],
     lambda ret, model: ret + model.name,
     '',
     'ba'),
])
def test_query_reduce_right(db,
                            model_class,
                            data,
                            callback,
                            initial,
                            expected):
    """Test SQLQuery.reduce_right."""
    db.save([model_class(item) for item in data])
    result = db.query(model_class).reduce_right(callback, initial=initial)
    assert result == expected


@parametrize('model_class,data,column,expected', [
    (AModel, [{'name': 'a'}, {'name': 'b'}], 'name', ['a', 'b']),
])
def test_query_pluck(db, model_class, data, column, expected):
    """Test SQLQuery.pluck."""
    db.save([model_class(item) for item in data])
    assert db.query(model_class).pluck(column) == expected


@parametrize('model_class,data', [
    (AModel, [{'name': 'a'}, {'name': 'b'}]),
])
def test_query_chain(db, model_class, data):
    """Test SQLQuery.chain."""
    db.save([model_class(item) for item in data])
    chain = db.query(model_class).chain()

    assert isinstance(chain, Chain)

    keys = list(data[0].keys())
    results = (chain
               .map(dict)
               .map(lambda item: pyd.pick(item, keys))
               .value())

    assert results == data


@parametrize('model_class,data,callback,expected', [
    (AModel,
     [{'name': 'a'}, {'name': 'b'}],
     'name',
     {'a': {'name': 'a'}, 'b': {'name': 'b'}}),
])
def test_query_index_by(db, model_class, data, callback, expected):
    """Test SQLQuery.index_by."""
    db.save([model_class(item) for item in data])
    results = db.query(model_class).index_by(callback)

    for key, value in results.items():
        assert pyd.pick(dict(value), pyd.keys(expected[key])) == expected[key]


@parametrize('model_class,data,callback,expected', [
    (AModel,
     [{'name': 'a'}, {'name': 'b'}, {'name': 'a'}],
     'name',
     {'a': [{'name': 'a'}, {'name': 'a'}], 'b': [{'name': 'b'}]}),
])
def test_query_stack_by(db, model_class, data, callback, expected):
    """Test SQLQuery.stack_by."""
    db.save([model_class(item) for item in data])
    results = db.query(model_class).stack_by(callback)

    for key, items in results.items():
        items = [pyd.pick(dict(item), pyd.keys(expected[key][0]))
                 for item in items]
        assert items == expected[key]


@parametrize('pagination,limit,offset', [
    (15, 15, None),
    ((15,), 15, None),
    ((15, 1), 15, None),
    ((15, 2), 15, 15),
    ((15, 3), 15, 30),
    ((15, -1), 15, None),
    ((15, -2), 15, None),
    ((15, -3), 15, None),
])
def test_query_paginate(db, pagination, limit, offset):
    """Test SQLQuery.paginate."""
    query = db.query(AModel).paginate(pagination)

    assert query._limit == limit
    assert query._offset == offset


def test_insert_model(db, model_query, data_pool):
    """Test query model insert."""
    data = data_pool[model_query.model_class]
    model = model_query.save(data)
    dbmodels = db.query(model_query.model_class).all()

    assert len(dbmodels) == 1
    assert model is dbmodels[0]


def test_insert_all_models(db, model_query, data_pool):
    """Test query model insert."""
    if model_query.model_class is AModel:
        return

    data = data_pool[model_query.model_class]

    for col in model_query.model_class.pk_columns():
        if col.name in data:
            del data[col.name]

    models = model_query.save([data] * 5)
    dbmodels = db.query(model_query.model_class).all()

    assert len(models) == len(dbmodels)
    assert set(models) == set(dbmodels)


@parametrize('insert_stmt,mappings', [
    (AModel,
     [{'name': random_alpha()},
      {'name': random_alpha()},
      {'name': random_alpha()}]),
    (AModel.__table__.insert(),
     [{'name': random_alpha()},
      {'name': random_alpha()},
      {'name': random_alpha()}]),
])
def test_bulk_insert(db, insert_stmt, mappings):
    """Test bulk insert of data."""
    db.bulk_insert(insert_stmt, mappings)

    for mapping in mappings:
        assert db[AModel].filter_by(**mapping).one()


@parametrize('insert_stmt,mappings', [
    (AModel,
     [{'name': random_alpha()},
      {'name': random_alpha()},
      {'name': random_alpha()}]),
    (AModel.__table__.insert(),
     [{'name': random_alpha()},
      {'name': random_alpha()},
      {'name': random_alpha()}]),
])
def test_bulk_insert_many(db, insert_stmt, mappings):
    """Test bulk insert many of data."""
    db.bulk_insert_many(insert_stmt, mappings)

    for mapping in mappings:
        assert db[AModel].filter_by(**mapping).one()


def test_update_model(db, model_query, model_pool):
    """Test query model update."""
    model = model_pool[model_query.model_class]
    new_values = {'name': random_alpha()}
    model.update(new_values)

    model_query.save(model)

    dbmodels = db.query(model_query.model_class).all()

    assert len(dbmodels) == 1

    dbmodel = dbmodels[0]

    assert model is dbmodel

    for key, value in new_values.items():
        assert dbmodel[key] == value


def test_update_model_relationship(db):
    """Test that model.update() can update relationship data."""
    data = {'name': random_alpha(),
            'c': {'name': random_alpha()}}

    model = AModel(data)
    db.save(model)

    new_name = random_alpha()

    model.update({'c': {'name': new_name}})
    db.save(model)

    assert model.c.name == new_name


def test_update_model_relationship_set_null(db):
    """Test that model.update() can set relationship to NULL with empty dict.
    """
    data = {'name': random_alpha(),
            'c': {'name': random_alpha()}}

    model = AModel(data)
    db.save(model)

    model.update({'c': {}})
    db.save(model)

    assert model.c is None


def test_update_all_models(db, model_query, models_pool):
    """Test query model update all."""
    models = models_pool[model_query.model_class]
    new_values = {'name': random_alpha()}

    for model in models:
        model.update(new_values)

    model_query.save(models)

    dbmodels = db.query(model_query.model_class).all()

    assert len(models) == len(dbmodels)
    assert set(models) == set(dbmodels)

    for model in dbmodels:
        for key, value in new_values.items():
            assert model[key] == value


def test_save(db, model_query, models_pool, data_pool):
    """Test query model save method."""
    data = (models_pool[model_query.model_class] +
            [data_pool[model_query.model_class]])

    model_query.save(data)

    dbmodels = db.query(model_query.model_class).all()

    assert len(dbmodels) == len(data)


@parametrize('identity', [
    lambda model: ((AModel.name, model.name),),
    core.make_identity(AModel.name)
])
def test_save_by_identity(db, identity):
    """Test saving by a custom identity function."""
    model1 = AModel({'name': 'a'})
    db.save(model1)

    model2 = db.save(AModel({'name': 'a', 'text': 'foobar'}),
                     identity=identity)

    assert model1 is model2
    assert model1.text == 'foobar'


@parametrize('data', [
    {AModel: [{'id': 1}, {'id': 1}],
     BModel: [{'id1': 1, 'id2': 2}, {'id1': 1, 'id2': 2}]},
])
def test_save_duplicate_primary_key_error(db, model_query, data):
    """Test that IntegrityError raised when duplicate primary key records are
    added at the same time.
    """
    data = data[model_query.model_class]

    with pytest.raises(sa.exc.IntegrityError):
        model_query.save(data)


def test_save_upsert_dict(db, model_query, models_pool):
    """Test that model_query save upserts existing records."""
    models = models_pool[model_query.model_class]
    data = [dict(model) for model in models]

    ret = model_query.save(data)
    dbmodels = db.query(model_query.model_class).all()

    assert len(ret) == len(models) == len(dbmodels)
    assert set(ret) == set(models) == set(dbmodels)


def test_before_save_model(db, model_query, model_pool):
    """Test that model_query save has before method hook."""
    before = mock.MagicMock()

    model = model_query.save(model_pool[model_query.model_class],
                             before=before)

    before.assert_called_once_with(model, False)


def test_before_save_data(db, model_query, data_pool):
    """Test that model_query save has before method hook."""
    before = mock.MagicMock()

    model = model_query.save(data_pool[model_query.model_class], before=before)

    before.assert_called_once_with(model, True)


def test_after_save_model(db, model_query, model_pool):
    """Test that model_query save has after method hook."""
    after = mock.MagicMock()

    model = model_query.save(model_pool[model_query.model_class], after=after)

    after.assert_called_once_with(model, False)


def test_after_save_data(db, model_query, data_pool):
    """Test that model_query save has after method hook."""
    after = mock.MagicMock()

    model = model_query.save(data_pool[model_query.model_class], after=after)

    after.assert_called_once_with(model, True)


def test_destroy_primary_key(db, model_query, model_pool):
    """Test that model is deleted using primary key."""
    model = model_pool[model_query.model_class]
    ident = model.identity()
    count = model_query.destroy(ident)

    assert model_query.count() == 0
    assert count == 1


def test_destroy_dict(db, model_query, model_pool):
    """Test that a single dict is deleted."""
    model = model_pool[model_query.model_class]
    data = dict(model)
    count = model_query.destroy(data)

    assert model_query.count() == 0
    assert count == 1


def test_destroy_model(db, model_query, model_pool):
    """Test that a single model is deleted."""
    model = model_pool[model_query.model_class]
    count = model_query.destroy(model)

    assert model_query.count() == 0
    assert count == 1


def test_destroy_many_primary_keys(db, model_query, models_pool):
    """Test that model is deleted using primary key."""
    models = models_pool[model_query.model_class]
    idents = [model.identity() for model in models]
    count = model_query.destroy(idents)

    assert model_query.count() == 0
    assert count == len(models)


def test_destroy_many_dicts(db, model_query, models_pool):
    """Test that many dicts are deleted."""
    models = models_pool[model_query.model_class]
    data = [dict(model) for model in models]
    count = model_query.destroy(data)

    assert model_query.count() == 0
    assert count == len(models)


def test_destroy_many_models(db, model_query, models_pool):
    """Test that many models are deleted."""
    models = models_pool[model_query.model_class]
    count = model_query.destroy(models)

    assert model_query.count() == 0
    assert count == len(models)


def test_count(model_query, models_pool):
    """Test that SQLModel_query.count returns total count from database."""
    models = models_pool[model_query.model_class]
    assert model_query.count() == len(models)


def test_model_contains():
    """Test that model contains columns."""
    model = AModel()
    columns = model.columns()

    assert len(columns) > 0

    for col in columns:
        assert col.name in model


def test_model_get_set_item():
    """Test that models can be modified using get/set item syntax."""
    model = AModel()
    model['name'] = 'foo'
    assert model['name'] == 'foo'


@parametrize('value', [{}, [], False, None, 0])
def test_empty_destroy(model_query, value):
    """Test that destroying an empty value returns None."""
    assert model_query.destroy(value) is None
