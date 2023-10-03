import typing as t

import pytest
from pytest import param
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import joinedload

from sqlservice import Database

from .fixtures import Address, Group, User, create_users


# Make pylint happy.
func: t.Callable  # type: ignore

parametrize = pytest.mark.parametrize


def test_session_all__returns_all_models(db: Database):
    users = create_users(db)

    with db.session() as session:
        results = session.all(sa.select(User))
        assert len(results) == len(users)
        for model in results:
            assert isinstance(model, User)


def test_session_all__uniquifies_joinedload_results(db: Database):
    users = [
        User(id=1, addresses=[Address(id=11), Address(id=12), Address(id=13)]),
        User(id=2, addresses=[Address(id=21), Address(id=22), Address(id=23)]),
        User(id=3, addresses=[Address(id=31), Address(id=32), Address(id=33)]),
    ]
    with db.begin() as session:
        session.add_all(users)

    stmt = sa.select(User).options(joinedload(User.addresses))  # type: ignore
    with db.session() as session:
        results = session.all(stmt)
        assert len(results) == len(users)


def test_session_all__returns_all_rows_from_non_orm_query(db: Database):
    inactive_users_by_id = {user.id: user for user in create_users(db, overrides={"active": False})}

    with db.session() as session:
        results = session.all(
            sa.text("SELECT * FROM users WHERE active = :active"), params={"active": False}
        )
        for user in results:
            assert not isinstance(user, User)

            inactive_user = inactive_users_by_id.get(user.id)
            assert inactive_user

            user_mapping = user._mapping
            for key, value in dict(inactive_user).items():
                assert user_mapping[key] == value


def test_session_first__returns_first_result_or_none(db: Database):
    create_users(db)

    with db.session() as session:
        result = session.first(sa.select(User))
        assert isinstance(result, User)

        result = session.first(sa.select(User).where(User.id == 0))
        assert result is None


def test_session_first__returns_first_result_or_none_from_non_orm_query(db: Database):
    users = create_users(db)
    first_user = users[0]

    with db.session() as session:
        result = session.first(
            sa.text("SELECT * FROM users WHERE active = :active ORDER BY id"),
            params={"active": True},
        )
        assert result is not None
        assert not isinstance(result, User)
        assert result.id == first_user.id
        assert result.name == first_user.name

        result = session.first(
            sa.text("SELECT * FROM users WHERE id = :id ORDER BY id"), params={"id": 0}
        )
        assert result is None


def test_session_one__returns_one_model_or_raises(db: Database):
    users = create_users(db)
    user = users[0]

    with db.session() as session:
        result = session.one(sa.select(User).where(User.id == user.id))
        assert isinstance(result, User)
        assert result.id == user.id

    with pytest.raises(MultipleResultsFound):
        with db.session() as session:
            session.one(sa.select(User))

    with pytest.raises(NoResultFound):
        with db.session() as session:
            session.one(sa.select(User).where(User.id == 0))


def test_session_one__returns_one_row_or_raises_from_non_orm_query(db: Database):
    users = create_users(db)
    user = users[0]

    with db.session() as session:
        result = session.one(sa.text("SELECT * FROM users WHERE id = :id"), params={"id": user.id})
        assert not isinstance(result, User)
        assert result.id == user.id
        assert result.name == user.name

    with pytest.raises(MultipleResultsFound):
        with db.session() as session:
            session.one(sa.text("SELECT * FROM users"))

    with pytest.raises(NoResultFound):
        with db.session() as session:
            session.one(sa.text("SELECT * FROM users WHERE id = :id"), params={"id": 0})


def test_session_one_or_none__returns_one_model_or_none_or_raises(db: Database):
    users = create_users(db)
    user = users[0]

    with db.session() as session:
        result = session.one_or_none(sa.select(User).where(User.id == user.id))
        assert isinstance(result, User)
        assert result.id == user.id

        result = session.one_or_none(sa.select(User).where(User.id == 0))
        assert result is None

    with pytest.raises(MultipleResultsFound):
        with db.session() as session:
            session.one_or_none(sa.select(User))


def test_session_one_or_none__returns_one_row_or_none_or_raises(db: Database):
    users = create_users(db)
    user = users[0]

    with db.session() as session:
        result = session.one_or_none(
            sa.text("SELECT * FROM users WHERE id = :id"), params={"id": user.id}
        )
        assert result is not None
        assert not isinstance(result, User)
        assert result.id == user.id
        assert result.name == user.name

        result = session.one_or_none(
            sa.text("SELECT * FROM users WHERE id = :id"), params={"id": 0}
        )
        assert result is None

    with pytest.raises(MultipleResultsFound):
        with db.session() as session:
            session.one_or_none(sa.text("SELECT * FROM users"))


def test_session_save__returns_model_instance(db: Database):
    user = User()
    with db.session() as session:
        result = session.save(user)
        assert result is user


def test_session_save__does_not_commit(db: Database):
    with db.session() as session:
        session.save(User())

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 0


def test_session_save__inserts_new_with_pk(db: Database):
    user = User(id=1, name="n")
    with db.begin() as session:
        session.save(user)

    assert user.id == 1
    assert user.name == "n"

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 1


def test_session_save__inserts_new_without_pk(db: Database):
    user = User(name="n")
    with db.begin() as session:
        session.save(user)

    assert user.id == 1
    assert user.name == "n"

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 1


def test_session_save__updates_existing(db: Database):
    existing_user = create_users(db, count=1)[0]
    assert existing_user.id == 1
    assert existing_user.name is None

    new_user = User(id=1, name="n")
    with db.begin() as session:
        session.save(existing_user)

    assert new_user.id == 1
    assert new_user.name == "n"

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 1


def test_session_save__updates_existing_in_same_session(db: Database):
    with db.session() as session:
        user = User(id=1)
        session.add(user)
        session.commit()

        user.name = "n"  # type: ignore
        session.save(user)
        session.commit()

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 1


@parametrize(
    "value, exception",
    [
        param(None, TypeError),
        param({}, TypeError),
        param([], TypeError),
        param(set(), TypeError),
        param([User()], TypeError),
        param({User()}, TypeError),
    ],
)
def test_session_save__raises_on_invalid_type(
    db: Database, value: t.Any, exception: t.Type[Exception]
):
    with pytest.raises(exception):
        with db.session() as session:
            session.save(value)


def test_session_save_all__returns_list(db: Database):
    users = [User(), User(), User()]
    with db.begin() as session:
        result = session.save_all(users)
        assert isinstance(result, list)
        assert result == users

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == len(users)


def test_session_save_all__accepts_single_model_and_returns_list(db: Database):
    user = User()
    with db.begin() as session:
        result = session.save_all(user)
        assert isinstance(result, list)
        assert result == [user]

    with db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert session.one(select_count) == 1


def test_session_save_all__inserts_and_updates_multiple_types(db: Database):
    existing_users = [User(name="uA"), User(name="uB"), User(name="uC")]
    new_users = [User(name="u1"), User(name="u2"), User(name="u3")]

    existing_groups = [Group(name="gA"), Group(name="gB"), Group(name="gC")]
    new_groups = [Group(name="g1"), Group(name="g2"), Group(name="g3")]

    with db.begin(expire_on_commit=False) as session:
        session.save_all(existing_users)
        session.save_all(existing_groups)

    all_models = existing_users + existing_groups + new_users + new_groups  # type: ignore
    all_users = existing_users + new_users
    all_groups = existing_groups + new_groups

    with db.begin() as session:
        session.save_all(all_models)

    with db.session() as session:
        count_users = sa.select(func.count(User.id))
        assert session.one(count_users) == len(all_users)

        count_groups = sa.select(func.count(Group.id))
        assert session.one(count_groups) == len(all_groups)


def test_session_save_all__raises_on_duplicate_primary_keys_in_list(db: Database):
    users = [User(id=1), User(id=1)]

    with pytest.raises(TypeError):
        with db.begin() as session:
            session.save_all(users)


@parametrize(
    "value, exception",
    [
        param(None, TypeError),
        param({}, TypeError),
        param([], TypeError),
        param(set(), TypeError),
        param([{}], TypeError),
        param([[]], TypeError),
        param([None], TypeError),
        param({None}, TypeError),
        param([User(), {}], TypeError),
        param([User(), None], TypeError),
    ],
)
def test_session_save_all__raises_on_invalid_type(
    db: Database, value: t.Any, exception: t.Type[Exception]
):
    with pytest.raises(exception):
        with db.session() as session:
            session.save_all(value)
