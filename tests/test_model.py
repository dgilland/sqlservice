import typing as t

import mock
import pytest
from pytest import param
from sqlalchemy import MetaData, orm
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql import Delete, Insert, Select, Update

from sqlservice import Database, ModelBase, ModelMeta, as_declarative, declarative_base

from .fixtures import Address, Group, GroupMembership, Item, Note, User


parametrize = pytest.mark.parametrize


def assert_declarative_base(base_class, name):
    assert issubclass(base_class, ModelBase)
    assert isinstance(base_class.metadata, MetaData)
    assert isinstance(base_class.registry, orm.registry)
    assert base_class.registry.metadata is base_class.metadata
    assert isinstance(base_class, ModelMeta)
    assert isinstance(base_class, DeclarativeMeta)
    assert base_class.__name__ == name


def test_declarative_base():
    Base = declarative_base()
    assert_declarative_base(Base, name="ModelBase")


def test_declarative_base__returns_new_base_class():
    Base1 = declarative_base()
    Base2 = declarative_base()
    assert Base1 is not Base2


def test_declarative_base__can_override_defaults():
    class MetaClass(ModelMeta):
        pass

    metadata = MetaData()
    Base = declarative_base(metadata=metadata, metaclass=MetaClass, name="Base")

    assert_declarative_base(Base, name="Base")
    assert Base.metadata is metadata
    assert isinstance(Base, MetaClass)


def test_as_declarative__decorates_class_as_base():
    @as_declarative()
    class Model(ModelBase):
        pass

    assert_declarative_base(Model, name="Model")


def test_as_declarative__can_override_defaults():
    metadata = MetaData()

    class MetaClass(ModelMeta):
        pass

    @as_declarative(metaclass=MetaClass, metadata=metadata, name="Base")
    class Base(ModelBase):
        pass

    assert_declarative_base(Base, name="Base")
    assert Base.metadata is metadata  # pylint: disable=no-member
    assert isinstance(Base, MetaClass)


def test_model_select():
    expr = User.select()
    assert isinstance(expr, Select)
    froms = expr.get_final_froms()
    assert froms == [User.__table__]


def test_model_insert():
    expr = User.insert()
    assert isinstance(expr, Insert)
    assert expr.table == User.__table__


def test_model_update():
    expr = User.update()
    assert isinstance(expr, Update)
    assert expr.table == User.__table__


def test_model_delete():
    expr = User.delete()
    assert isinstance(expr, Delete)
    assert expr.table == User.__table__


@parametrize(
    "model, expected_pk",
    [
        param(User(id=1), (1,)),
        param(GroupMembership(group_id=1, user_id=2), (1, 2)),
    ],
)
def test_model_pk(model: ModelBase, expected_pk: t.Tuple[t.Any, ...]):
    assert model.pk() == expected_pk


def test_model_set():
    user = User()
    assert user.id is None
    assert user.name is None
    assert user.addresses == []
    assert user.group_memberships == []

    user_id = 1
    name = "n"
    addresses = [Address(addr="a", zip_code="1")]

    user.set(id=user_id, name=name, addresses=addresses)
    assert user.id == user_id
    assert user.name == name
    assert user.addresses == addresses
    assert user.group_memberships == []


def test_model_update__raises_on_invalid_keywords():
    with pytest.raises(TypeError):
        User(invalid=True)

    with pytest.raises(TypeError):
        User().set(invalid=True)


@parametrize(
    "model, args, expected",
    [
        param(User(), {}, {}, id="no_values"),
        param(User(id=1), {}, {"id": 1}, id="single_col_set"),
        param(User(id=1, name="n"), {}, {"id": 1, "name": "n"}, id="multi_cols_set"),
        param(
            User(id=1, name="n", addresses=[Address()]),
            {},
            {"id": 1, "name": "n", "addresses": [{}]},
            id="1:M_without_data",
        ),
        param(
            User(id=1, name="n", addresses=[Address()]),
            {"exclude_relationships": True},
            {"id": 1, "name": "n"},
            id="1:M_exclude_relationships",
        ),
        param(
            User(id=1, name="n", addresses=[Address(addr="a")]),
            {},
            {"id": 1, "name": "n", "addresses": [{"addr": "a"}]},
            id="1:M_with_some_data",
        ),
        param(
            User(id=1, name="n", addresses=[Address(addr="a", zip_code="1")]),
            {},
            {"id": 1, "name": "n", "addresses": [{"addr": "a", "zip_code": "1"}]},
            id="1:M_with_more_data",
        ),
    ],
)
def test_model_to_dict__with_non_persisted_model(model: ModelBase, args: dict, expected: dict):
    assert model.to_dict(**args) == expected


@parametrize(
    "model, args, load_options, expected",
    [
        param(User(id=1), {}, [], {"id": 1, "name": None, "active": True}, id="single_col_set"),
        param(
            User(id=1, name="n"),
            {},
            [],
            {"id": 1, "name": "n", "active": True},
            id="multi_cols_set",
        ),
        param(
            User(id=1, name="n", addresses=[Address()]),
            {"exclude_relationships": True},
            [orm.joinedload("*")],
            {"id": 1, "name": "n", "active": True},
            id="1:M_loaded_exclude_relationships",
        ),
        param(
            User(id=1, name="n", addresses=[Address()]),
            {},
            [],
            {"id": 1, "name": "n", "active": True},
            id="1:M_lazy_loaded_include_relationships",
        ),
        param(
            User(id=1, name="n", addresses=[Address(addr="a")]),
            {},
            [orm.joinedload(User.addresses)],  # type: ignore
            {
                "id": 1,
                "name": "n",
                "active": True,
                "addresses": [{"id": 1, "user_id": 1, "addr": "a", "zip_code": None}],
            },
            id="M_loaded_include_relationships",
        ),
        param(
            User(id=1, name="n", addresses=[Address(addr="a", zip_code="1")]),
            {},
            [orm.joinedload("*")],
            {
                "id": 1,
                "name": "n",
                "active": True,
                "addresses": [{"id": 1, "user_id": 1, "addr": "a", "zip_code": "1"}],
                "group_memberships": [],
                "items": [],
            },
            id="1:M_all_loaded_include_relationships",
        ),
        param(
            User(
                id=1,
                name="n",
                addresses=[Address(addr="a", zip_code="1"), Address(addr="b", zip_code="2")],
                group_memberships=[
                    GroupMembership(group=Group(name="g1")),
                    GroupMembership(group=Group(name="g2")),
                ],
            ),
            {},
            [],
            {"id": 1, "name": "n", "active": True},
            id="nested_relationships_not_loaded",
        ),
        param(
            User(
                id=1,
                name="n",
                addresses=[Address(addr="a", zip_code="1"), Address(addr="b", zip_code="2")],
                group_memberships=[
                    GroupMembership(group=Group(name="g1")),
                    GroupMembership(group=Group(name="g2")),
                ],
            ),
            {},
            [orm.joinedload("*")],
            {
                "id": 1,
                "name": "n",
                "active": True,
                "addresses": [
                    {"id": 1, "user_id": 1, "addr": "a", "zip_code": "1"},
                    {"id": 2, "user_id": 1, "addr": "b", "zip_code": "2"},
                ],
                "group_memberships": [
                    {"group_id": 1, "user_id": 1, "group": {"id": 1, "name": "g1"}},
                    {"group_id": 2, "user_id": 1, "group": {"id": 2, "name": "g2"}},
                ],
                "items": [],
            },
            id="nested_relationships_all_loaded",
        ),
        param(
            User(
                id=1,
                name="n",
                addresses=[Address(addr="a", zip_code="1"), Address(addr="b", zip_code="2")],
                group_memberships=[
                    GroupMembership(group=Group(name="g1")),
                    GroupMembership(group=Group(name="g2")),
                ],
                items=[
                    Item(
                        notes={
                            "k1": Note(keyword="k1"),
                            "k2": Note(keyword="k2"),
                            "k3": Note(keyword="k3"),
                        }
                    )
                ],
            ),
            {"lazyload": True},
            [orm.lazyload("*")],
            {
                "id": 1,
                "name": "n",
                "active": True,
                "addresses": [
                    {"id": 1, "user_id": 1, "addr": "a", "zip_code": "1"},
                    {"id": 2, "user_id": 1, "addr": "b", "zip_code": "2"},
                ],
                "group_memberships": [
                    {
                        "group_id": 1,
                        "user_id": 1,
                        "group": {
                            "id": 1,
                            "name": "g1",
                            "memberships": [{"group_id": 1, "user_id": 1}],
                        },
                    },
                    {
                        "group_id": 2,
                        "user_id": 1,
                        "group": {
                            "id": 2,
                            "name": "g2",
                            "memberships": [{"group_id": 2, "user_id": 1}],
                        },
                    },
                ],
                "items": [
                    {
                        "id": 1,
                        "user_id": 1,
                        "notes": {
                            "k1": {"id": 1, "item_id": 1, "keyword": "k1"},
                            "k2": {"id": 2, "item_id": 1, "keyword": "k2"},
                            "k3": {"id": 3, "item_id": 1, "keyword": "k3"},
                        },
                    }
                ],
            },
            id="nested_relationships_lazy_loaded",
        ),
    ],
)
def test_model_to_dict__loaded_from_database(
    db: Database, model: ModelBase, args: dict, load_options: list, expected: dict
):
    with db.begin() as session:
        session.add(model)

    with db.session() as session:
        fetched_model = session.first(type(model).select().options(*load_options))
        assert fetched_model is not None
        assert fetched_model.to_dict(**args) == expected


def test_model_to_dict__is_called_during_dict_conversion():
    user = User(id=1, name="n", addresses=[Address()])

    with mock.patch.object(user, "to_dict") as mocked_to_dict:
        dict(user)

    assert mocked_to_dict.called


def test_model_iter():
    user = User(id=1, name="n", addresses=[Address()])
    assert dict(user.__iter__()) == user.to_dict()


def test_model_repr():
    user = User(id=1, name="n", addresses=[Address()])
    assert repr(user) == "User(id=1, name='n')"
