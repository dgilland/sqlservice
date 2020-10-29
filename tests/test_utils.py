import pytest

from sqlservice.utils import FrozenDict


def test_frozendict_getitem():
    frozen_d = FrozenDict({"a": 1})
    assert frozen_d["a"] == frozen_d._dict["a"]


def test_frozendict_immutability():
    frozen_d = FrozenDict()
    with pytest.raises(TypeError):
        frozen_d["a"] = 1  # pylint: disable=unsupported-assignment-operation


def test_frozendict_contains():
    frozen_d = FrozenDict({"a": 1})
    assert "a" in frozen_d
    assert "b" not in frozen_d


def test_frozendict_iter():
    frozen_d = FrozenDict({"a": 1})
    assert list(iter(frozen_d)) == list(iter(frozen_d._dict))


def test_frozendict_copy():
    frozen_d = FrozenDict({"a": 1})
    frozen_d_copy = frozen_d.copy()
    assert isinstance(frozen_d_copy, dict)
    assert frozen_d_copy == {"a": 1}


def test_frozendict_len():
    frozen_d = FrozenDict({"a": 1})
    assert len(frozen_d) == len(frozen_d._dict)


def test_frozendict_repr():
    frozen_d = FrozenDict({"a": 1})
    assert repr(frozen_d) == "<FrozenDict {'a': 1}>"
