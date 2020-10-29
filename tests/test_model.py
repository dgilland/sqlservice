from unittest import mock

import pydash as pyd
import pytest
import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import DeclarativeMeta

from sqlservice import as_declarative, core, declarative_base
from sqlservice.model import ModelBase, ModelMeta

from .fixtures import AModel, CModel, DModel, Model, parametrize


def test_declarative_base():
    """Test declarative_base()."""

    class MetaClass(DeclarativeMeta):
        pass

    metadata = MetaData()

    Model = declarative_base(metadata=metadata, metaclass=MetaClass)

    assert Model.__bases__[0] is ModelBase
    assert Model.metadata is metadata
    assert Model.metaclass is MetaClass


def test_as_declarative():
    """Test as_declarative()."""

    @as_declarative()
    class Model:
        pass

    assert Model.metaclass is ModelMeta


@parametrize("model,expected", [(AModel({"id": 1, "name": "a"}), ((AModel.columns()["id"], 1),))])
def test_model_identity_map(model, expected):
    """Test that model has an identity map equal to its primary key columns and values."""
    assert model.identity_map() == expected


@parametrize(
    "model,expected",
    [
        (
            AModel(
                {
                    "name": "a",
                    "c": {"name": "b"},
                    "ds": [{"id": 1, "name": "d1"}, {"id": 2, "name": "d2"}],
                }
            ),
            {
                "name": "a",
                "c": {"name": "b"},
                "ds": [{"id": 1, "name": "d1"}, {"id": 2, "name": "d2"}],
                "d_map": {1: {"id": 1, "name": "d1"}, 2: {"id": 2, "name": "d2"}},
            },
        ),
        (AModel({"name": "a", "c": None}), {"name": "a", "c": {}}),
    ],
)
def test_model_to_dict(db, model, expected):
    """Test that a model can be serialized to a dict."""
    db.save(model)
    model = (
        db.query(model.__class__)
        .filter(core.identity_map_filter(model))
        .options(sa.orm.eagerload("*"))
        .first()
    )

    assert pyd.is_match(model.to_dict(), expected)
    assert pyd.is_match(dict(model), expected)


@parametrize(
    "adapters,data,expected",
    [
        (
            {list: lambda models, col, _: [model.id for model in models]},
            {"ds": [DModel(id=1), DModel(id=2)]},
            {"ds": [1, 2]},
        ),
        (
            {"ds": lambda models, col, _: [model.id for model in models]},
            {"ds": [DModel(id=1), DModel(id=2)]},
            {"ds": [1, 2]},
        ),
        ({str: lambda val, *_: val[0]}, {"name": "foo"}, {"name": "f"}),
        ({"name": lambda val, *_: val[0]}, {"name": "foo"}, {"name": "f"}),
        (
            {"name": lambda val, *_: val[0]},
            {"name": "foo", "text": "bar"},
            {"name": "f", "text": "bar"},
        ),
        ({"text": None}, {"name": "foo", "text": "bar"}, {"name": "foo"}),
        (
            {"CModel": lambda c, *_: {"name": c.name}},
            {"c": CModel(id=1, name="foo")},
            {"c": {"name": "foo"}},
        ),
    ],
)
def test_model_to_dict_args_adapters(db, adapters, data, expected):
    """Test that Model.__dict_args__['exclude_sequence_types'] can be used to skip nested dict
    serialization of those types."""
    args = {"adapters": adapters}
    expected = data if expected is True else expected

    with mock.patch.object(AModel, "__dict_args__", new=args):
        model = AModel(data)
        assert dict(model) == expected
