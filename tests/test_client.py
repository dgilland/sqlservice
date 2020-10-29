from unittest import mock

import pytest
import sqlalchemy as sa

from sqlservice import SQLClient

from .fixtures import AModel, DupAModel, DupModel, MyTestError, parametrize, random_alpha


def getattr_path(obj, path):
    """Get nested `obj` attributes using dot-path syntax (e.g. path='some.nested.attr')."""
    attr = obj

    for item in path.split("."):
        attr = getattr(attr, item)

    return attr


@parametrize(
    "proxy,attr",
    [
        ("metadata", "model_class.metadata"),
        ("tables", "model_class.metadata.tables"),
        ("url", "engine.url"),
        ("database", "engine.url.database"),
    ],
)
def test_sql_client_proxy_property(db, proxy, attr):
    """Test that database manager's proxy attributes map to expected real attribute."""
    assert getattr(db, proxy) is getattr_path(db, attr)


@parametrize(
    "attr",
    [
        "add",
        "add_all",
        "close",
        "close_all",
        "invalidate",
        "commit",
        "delete",
        "execute",
        "scalar",
        "flush",
        "merge",
        "query",
        "refresh",
        "rollback",
        "transaction",
        "expire",
        "expire_all",
        "expunge_all",
        "prune",
        "bulk_insert_mappings",
        "bulk_save_objects",
        "bulk_update_mappings",
        "is_active",
        "is_modified",
        "no_autoflush",
        "prepare",
    ],
)
def test_sql_client_property(db, attr):
    """Test that database manager's proxy attributes map to expected real attribute."""
    assert hasattr(db, attr)
    assert getattr(db, attr)


def test_ping(filedb):
    """Test that SQLClinet.ping() returns True on success."""
    assert filedb.ping() is True


def test_ping_exception(db):
    """Test that SQLClient.ping() raises and exception on failure."""
    mocked_engine = mock.Mock()
    mocked_conn = mock.Mock(scalar=mock.Mock(side_effect=sa.exc.DBAPIError(None, None, None)))
    mocked_engine.connect.return_value = mocked_conn

    with mock.patch.object(db, "engine", new=mocked_engine):
        with pytest.raises(sa.exc.DBAPIError):
            db.ping()


def test_ping_exception_invalidated(db):
    """Test that SQLClient.ping() raises and exception on failure with an invalidated connection."""
    mocked_engine = mock.Mock()
    mocked_conn = mock.Mock(
        scalar=mock.Mock(
            side_effect=sa.exc.DBAPIError(None, None, None, connection_invalidated=True)
        )
    )
    mocked_engine.connect.return_value = mocked_conn

    with mock.patch.object(db, "engine", new=mocked_engine):
        with pytest.raises(sa.exc.DBAPIError):
            db.ping()


def test_transaction_commit(db):
    """Test that a non-nested transaction commits."""
    model = AModel(name=random_alpha())

    with db.transaction():
        db.add(model)

    assert db.query(AModel).get(model.id) is model


def test_transaction_error_rollback(db):
    """Test that a non-nested transaction rolls back."""
    model = AModel(name=random_alpha())

    with pytest.raises(sa.orm.exc.FlushError):
        with db.transaction():
            db.add(model)
            db.flush()
            db.add(AModel(id=model.id, name=model.name))


def test_transaction_nested_single_commit(db, commit_event):
    """Test that a nested transaction results in a single commit."""
    with db.transaction():
        db.add(AModel(name=random_alpha()))
        with db.transaction():
            db.add(AModel(name=random_alpha()))
            with db.transaction():
                db.add(AModel(name=random_alpha()))

    assert db.query(AModel).count() == 3
    assert commit_event.call_count == 1


def test_transaction_nested_single_rollback_on_rollback(db, rollback_event):
    """Test that a nested transaction results in a single rollback when database error
    encountered."""
    with pytest.raises(sa.exc.IntegrityError):
        with db.transaction():
            db.add(AModel(id=3, name=random_alpha()))
            with db.transaction():
                db.add(AModel(id=2, name=random_alpha()))
                with db.transaction():
                    db.add(AModel(id=1, name=random_alpha()))

            # Intentionally add duplicate primary key to cause IntegrityError.
            db.add(AModel(id=1, name=random_alpha()))

    assert db.query(AModel).count() == 0
    assert rollback_event.call_count == 1


def test_transaction_nested_single_rollback_before_commit(db, rollback_event):
    """Test that a nested transaction results in a single rollback when an exception occurs before
    commit is issued."""
    with pytest.raises(MyTestError):
        with db.transaction():
            with db.transaction():
                with db.transaction():
                    raise MyTestError(
                        "Exception occurs at the bottom-most context before commit issued."
                    )

    assert rollback_event.call_count == 1


def test_transaction_commit_false(db, commit_event, rollback_event):
    """Test that a no-commit transaction doesn't commit."""
    with db.transaction(commit=False):
        db.add(AModel())

    assert commit_event.call_count == 0
    assert rollback_event.call_count == 0


def test_transaction_nested_outer_commit_false(db, commit_event, rollback_event):
    """Test that a commit transaction nested inside a no-commit transaction doesn't commit."""
    with db.transaction(commit=False):
        with db.transaction():
            db.add(AModel())

    assert commit_event.call_count == 0
    assert rollback_event.call_count == 0


def test_transaction_nested_inner_commit_false(db, commit_event, rollback_event):
    """Test that a no-commit transaction nested inside a write transaction does commit."""
    with db.transaction():
        with db.transaction(commit=False):
            db.add(AModel())

    assert commit_event.call_count == 1
    assert rollback_event.call_count == 0


def test_transaction_rollback(db, commit_event, rollback_event):
    """Test that a rollback transaction rolls back."""
    with db.transaction(rollback=True):
        db.add(AModel())

    assert commit_event.call_count == 0
    assert rollback_event.call_count == 1


def test_transaction_nested_outer_rollback(db, commit_event, rollback_event):
    """Test that a commit transaction nested inside a rollback transaction rolls back."""
    with db.transaction(rollback=True):
        with db.transaction():
            db.add(AModel())

    assert commit_event.call_count == 0
    assert rollback_event.call_count == 1


def test_transaction_nested_inner_rollback(db, commit_event, rollback_event):
    """Test that a rollback transaction nested inside a write transaction does commit."""
    with db.transaction():
        with db.transaction(rollback=True):
            db.add(AModel())

    assert commit_event.call_count == 1
    assert rollback_event.call_count == 0


@parametrize(
    "config_autoflush,autoflush,expected",
    [
        (True, False, False),
        (True, True, True),
        (True, None, True),
        (False, False, False),
        (False, True, True),
        (False, None, False),
    ],
)
def test_transaction_autoflush_false(config_autoflush, autoflush, expected):
    """Test that transactions can override default session autoflush."""
    db = SQLClient({"SQL_AUTOFLUSH": config_autoflush})

    assert db.session.autoflush is config_autoflush

    with db.transaction(autoflush=autoflush):
        assert db.session.autoflush is expected

        # Nested transactions override previous one.
        with db.transaction(autoflush=not autoflush):
            assert db.session.autoflush is not autoflush

        assert db.session.autoflush is expected

    assert db.session.autoflush is config_autoflush


def test_reflect(filedb):
    """Test that table metadata can be reflected with an explicit declarative base model."""
    rdb = SQLClient(filedb.config)

    assert len(rdb.tables) == 0

    rdb.reflect()

    assert len(rdb.tables) > 0
    assert set(rdb.tables.keys()) == set(filedb.tables.keys())

    for tablename, table in filedb.tables.items():
        assert rdb.tables[tablename].name == table.name


def test_config_string():
    """Test that a database URI string can be used to configure SQLClient."""
    uri = "sqlite:///test.db"
    db = SQLClient(uri)

    assert db.config["SQL_DATABASE_URI"] == uri
    assert str(db.url) == uri


def test_session_options():
    """Test that SQLClient's session can be configured with extra options."""
    db = SQLClient(session_options={"autocommit": True})
    assert db.session.autocommit is True

    db = SQLClient(session_options={"autocommit": False})
    assert db.session.autocommit is False


def test_engine_options():
    """Test that SQLClient's engine can be configured with extra options."""
    db = SQLClient(engine_options={"echo": True})
    assert db.engine.echo is True

    db = SQLClient(engine_options={"echo": False})
    assert db.engine.echo is False


def test_duplicate_model_class_name():
    """Test that duplicate model class names are supported by SQLClient model registry."""
    # Since we're going to shadow the same model name, we need an alias to it
    # for testing.
    global DupAModel
    _DupAModel = DupAModel

    class DupAModel(DupModel):
        __tablename__ = "test_dup_dup_a"
        id = sa.Column(sa.types.Integer(), primary_key=True)

    db = SQLClient(model_class=DupModel)
    db.create_all()

    assert "tests.fixtures.DupAModel" in db.models
    assert db.models["tests.fixtures.DupAModel"] is _DupAModel

    assert "tests.test_client.DupAModel" in db.models
    assert db.models["tests.test_client.DupAModel"] is DupAModel

    model1 = DupAModel()
    assert db.save(model1) is model1

    model2 = _DupAModel()
    assert db.save(model2) is model2

    del DupAModel


def test_save_with_generator(db):
    """Test save method using generator as input."""
    names = [str(i) for i in range(5)]
    models = (AModel({"name": name}) for name in names)
    db.save(models)

    dbmodels = db.query(AModel).order_by("name").all()

    for i, model in enumerate(dbmodels):
        assert model.name == names[i]


@parametrize("value", [{}, [{}]])
def test_save_invalid_type(db, value):
    """Test that save with an invalid type raises an exception."""
    with pytest.raises(TypeError):
        db.save(value)


@parametrize("value", [{}, [{}]])
def test_destroy_invalid_type(db, value):
    """Test that destroy with an invalid type raises an exception."""
    with pytest.raises(TypeError):
        db.destroy(value)


def test_expunge_handles_multiple_instances(db):
    """Test that SQLClient.expunge can expunge multiple instances."""
    models = [AModel(), AModel(), AModel()]
    db.save(models)

    for model in models:
        assert model in db.session

    db.expunge(*models)

    for model in models:
        assert model not in db.session


def test_repr(db):
    """Test repr() of SQLClient."""
    assert repr(db) == "<SQLClient('sqlite://')>"
