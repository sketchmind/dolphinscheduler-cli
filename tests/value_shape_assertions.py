"""Type-narrowing assertions for structured service result values."""

from collections.abc import Mapping, Sequence


def assert_mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def assert_sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value
