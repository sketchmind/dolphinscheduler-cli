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
