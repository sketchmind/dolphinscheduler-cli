from collections.abc import Callable

import pytest

from dsctl.errors import UserInputError
from dsctl.services._validation import (
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
)


def test_require_non_empty_text_includes_suggestion() -> None:
    with pytest.raises(UserInputError, match="must not be empty") as exc_info:
        require_non_empty_text("   ", label="workflow name")

    assert exc_info.value.suggestion == "Pass one non-empty workflow name value."


def test_require_positive_int_includes_suggestion() -> None:
    with pytest.raises(
        UserInputError,
        match="must be greater than or equal to 1",
    ) as exc_info:
        require_positive_int(0, label="page_no")

    assert exc_info.value.suggestion == (
        "Pass page_no as an integer greater than or equal to 1."
    )


def test_require_non_negative_int_includes_suggestion() -> None:
    with pytest.raises(
        UserInputError,
        match="must be greater than or equal to 0",
    ) as exc_info:
        require_non_negative_int(-1, label="parallelism")

    assert exc_info.value.suggestion == (
        "Pass parallelism as an integer greater than or equal to 0."
    )


@pytest.mark.parametrize(
    ("validator", "value", "minimum", "input_hint"),
    [
        (require_positive_int, 0, 1, "--interval-seconds"),
        (require_non_negative_int, -1, 0, "--timeout-seconds"),
    ],
)
def test_integer_validator_uses_executable_input_hint(
    validator: Callable[..., int],
    value: int,
    minimum: int,
    input_hint: str,
) -> None:
    with pytest.raises(UserInputError) as exc_info:
        validator(value, label="internal_name", input_hint=input_hint)

    assert exc_info.value.suggestion == (
        f"Pass {input_hint} as an integer greater than or equal to {minimum}."
    )
