import typing as t

import pytest
from pytest import param
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import joinedload

from sqlservice import AsyncDatabase

from .fixtures import Address, Group, User, async_create_users


# Make pylint happy.
func: t.Callable  # type: ignore

parametrize = pytest.mark.parametrize


async def test_async_session_all__returns_all_models(async_db: AsyncDatabase):
    users = await async_create_users(async_db)

    async with async_db.session() as session:
        results = await session.all(sa.select(User))
        assert len(results) == len(users)
        for model in results:
            assert isinstance(model, User)


async def test_session_all__uniquifies_joinedload_results(async_db: AsyncDatabase):
    users = [
        User(id=1, addresses=[Address(id=11), Address(id=12), Address(id=13)]),
        User(id=2, addresses=[Address(id=21), Address(id=22), Address(id=23)]),
        User(id=3, addresses=[Address(id=31), Address(id=32), Address(id=33)]),
    ]
    async with async_db.begin() as session:
        session.add_all(users)

    stmt = sa.select(User).options(joinedload(User.addresses))  # type: ignore
    async with async_db.session() as session:
        results = await session.all(stmt)
        assert len(results) == len(users)


async def test_session_all__returns_all_rows_from_non_orm_query(async_db: AsyncDatabase):
    inactive_users_by_id = {
        user.id: user for user in (await async_create_users(async_db, overrides={"active": False}))
    }

    async with async_db.session() as session:
        results = await session.all(
            sa.text("SELECT * FROM users WHERE active = :active"), params={"active": False}
        )
        for user in results:
            assert not isinstance(user, User)

            inactive_user = inactive_users_by_id.get(user.id)
            assert inactive_user

            user_mapping = user._mapping
            for key, value in dict(inactive_user).items():
                assert user_mapping[key] == value


async def test_session_first__returns_first_result_or_none(async_db: AsyncDatabase):
    await async_create_users(async_db)

    async with async_db.session() as session:
        result = await session.first(sa.select(User))
        assert isinstance(result, User)

        result = await session.first(sa.select(User).where(User.id == 0))
        assert result is None


async def test_session_first__returns_first_result_or_none_from_non_orm_query(
    async_db: AsyncDatabase,
):
    users = await async_create_users(async_db)
    first_user = users[0]

    async with async_db.session() as session:
        result = await session.first(
            sa.text("SELECT * FROM users WHERE active = :active ORDER BY id"),
            params={"active": True},
        )
        assert result is not None
        assert not isinstance(result, User)
        assert result.id == first_user.id
        assert result.name == first_user.name

        result = await session.first(
            sa.text("SELECT * FROM users WHERE id = :id ORDER BY id"), params={"id": 0}
        )
        assert result is None


async def test_session_one__returns_one_model_or_raises(async_db: AsyncDatabase):
    users = await async_create_users(async_db)
    user = users[0]

    async with async_db.session() as session:
        result = await session.one(sa.select(User).where(User.id == user.id))
        assert isinstance(result, User)
        assert result.id == user.id

    with pytest.raises(MultipleResultsFound):
        async with async_db.session() as session:
            await session.one(sa.select(User))

    with pytest.raises(NoResultFound):
        async with async_db.session() as session:
            await session.one(sa.select(User).where(User.id == 0))


async def test_session_one__returns_one_row_or_raises_from_non_orm_query(async_db: AsyncDatabase):
    users = await async_create_users(async_db)
    user = users[0]

    async with async_db.session() as session:
        result = await session.one(
            sa.text("SELECT * FROM users WHERE id = :id"), params={"id": user.id}
        )
        assert result is not None
        assert not isinstance(result, User)
        assert result.id == user.id
        assert result.name == user.name

    with pytest.raises(MultipleResultsFound):
        async with async_db.session() as session:
            await session.one(sa.text("SELECT * FROM users"))

    with pytest.raises(NoResultFound):
        async with async_db.session() as session:
            await session.one(sa.text("SELECT * FROM users WHERE id = :id"), params={"id": 0})


async def test_session_one_or_none__returns_one_model_or_none_or_raises(async_db: AsyncDatabase):
    users = await async_create_users(async_db)
    user = users[0]

    async with async_db.session() as session:
        result = await session.one_or_none(sa.select(User).where(User.id == user.id))
        assert isinstance(result, User)
        assert result.id == user.id

        result = await session.one_or_none(sa.select(User).where(User.id == 0))
        assert result is None

    with pytest.raises(MultipleResultsFound):
        async with async_db.session() as session:
            await session.one_or_none(sa.select(User))


async def test_session_one_or_none__returns_one_row_or_none_or_raises(async_db: AsyncDatabase):
    users = await async_create_users(async_db)
    user = users[0]

    async with async_db.session() as session:
        result = await session.one_or_none(
            sa.text("SELECT * FROM users WHERE id = :id"), params={"id": user.id}
        )
        assert result is not None
        assert not isinstance(result, User)
        assert result.id == user.id
        assert result.name == user.name

        result = await session.one_or_none(
            sa.text("SELECT * FROM users WHERE id = :id"), params={"id": 0}
        )
        assert result is None

    with pytest.raises(MultipleResultsFound):
        async with async_db.session() as session:
            await session.one_or_none(sa.text("SELECT * FROM users"))


async def test_session_save__returns_model_instance(async_db: AsyncDatabase):
    user = User()
    async with async_db.session() as session:
        result = await session.save(user)
        assert result is user


async def test_session_save__does_not_commit(async_db: AsyncDatabase):
    async with async_db.session() as session:
        await session.save(User())

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 0


async def test_session_save__inserts_new_with_pk(async_db: AsyncDatabase):
    user = User(id=1, name="n")
    async with async_db.begin() as session:
        await session.save(user)

    assert user.id == 1
    assert user.name == "n"

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 1


async def test_session_save__inserts_new_without_pk(async_db: AsyncDatabase):
    user = User(name="n")
    async with async_db.begin() as session:
        await session.save(user)

    assert user.id == 1
    assert user.name == "n"

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 1


async def test_session_save__updates_existing(async_db: AsyncDatabase):
    existing_user = (await async_create_users(async_db, count=1))[0]
    assert existing_user.id == 1
    assert existing_user.name is None

    new_user = User(id=1, name="n")
    async with async_db.begin() as session:
        await session.save(existing_user)

    assert new_user.id == 1
    assert new_user.name == "n"

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 1


async def test_session_save__updates_existing_in_same_session(async_db: AsyncDatabase):
    async with async_db.session() as session:
        user = User(id=1)
        session.add(user)
        await session.commit()

        user.name = "n"  # type: ignore
        await session.save(user)
        await session.commit()

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 1


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
async def test_session_save__raises_on_invalid_type(
    async_db: AsyncDatabase, value: t.Any, exception: t.Type[Exception]
):
    with pytest.raises(exception):
        async with async_db.session() as session:
            await session.save(value)


async def test_session_save_all__returns_list(async_db: AsyncDatabase):
    users = [User(), User(), User()]
    async with async_db.begin() as session:
        result = await session.save_all(users)
        assert isinstance(result, list)
        assert result == users

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == len(users)


async def test_session_save_all__accepts_single_model_and_returns_list(async_db: AsyncDatabase):
    user = User()
    async with async_db.begin() as session:
        result = await session.save_all(user)
        assert isinstance(result, list)
        assert result == [user]

    async with async_db.session() as session:
        select_count = sa.select(func.count(User.id))
        assert await session.one(select_count) == 1


async def test_session_save_all__inserts_and_updates_multiple_types(async_db: AsyncDatabase):
    existing_users = [User(name="uA"), User(name="uB"), User(name="uC")]
    new_users = [User(name="u1"), User(name="u2"), User(name="u3")]

    existing_groups = [Group(name="gA"), Group(name="gB"), Group(name="gC")]
    new_groups = [Group(name="g1"), Group(name="g2"), Group(name="g3")]

    async with async_db.begin() as session:
        await session.save_all(existing_users)
        await session.save_all(existing_groups)

    all_models = existing_users + existing_groups + new_users + new_groups  # type: ignore
    all_users = existing_users + new_users
    all_groups = existing_groups + new_groups

    async with async_db.begin() as session:
        await session.save_all(all_models)

    async with async_db.session() as session:
        count_users = sa.select(func.count(User.id))
        assert await session.one(count_users) == len(all_users)

        count_groups = sa.select(func.count(Group.id))
        assert await session.one(count_groups) == len(all_groups)


async def test_session_save_all__raises_on_duplicate_primary_keys_in_list(async_db: AsyncDatabase):
    users = [User(id=1), User(id=1)]

    with pytest.raises(TypeError):
        async with async_db.begin() as session:
            await session.save_all(users)


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
async def test_session_save_all__raises_on_invalid_type(
    async_db: AsyncDatabase, value: t.Any, exception: t.Type[Exception]
):
    with pytest.raises(exception):
        async with async_db.session() as session:
            await session.save_all(value)
