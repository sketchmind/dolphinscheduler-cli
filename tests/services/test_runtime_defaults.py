import pytest
from tests.fakes import (
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
)

from dsctl.errors import ConflictError
from dsctl.services._runtime_defaults import (
    load_project_preference_defaults_from_operations,
)


def test_load_project_preference_defaults_returns_none_when_disabled() -> None:
    operations = FakeProjectPreferenceAdapter(
        project_preferences=[
            FakeProjectPreference(
                id=1,
                code=1,
                project_code_value=7,
                state=0,
                preferences_value='{"tenant":"tenant-pref"}',
            )
        ]
    )

    defaults = load_project_preference_defaults_from_operations(
        operations,
        project_code=7,
    )

    assert defaults is None


def test_load_project_preference_defaults_rejects_invalid_json() -> None:
    operations = FakeProjectPreferenceAdapter(
        project_preferences=[
            FakeProjectPreference(
                id=1,
                code=1,
                project_code_value=7,
                state=1,
                preferences_value="{invalid",
            )
        ]
    )

    with pytest.raises(
        ConflictError,
        match="Stored project preference must be valid JSON",
    ):
        load_project_preference_defaults_from_operations(
            operations,
            project_code=7,
        )


@pytest.mark.parametrize(
    "preferences_value",
    [
        pytest.param('["not","an","object"]', id="array"),
        pytest.param('"not an object"', id="string"),
        pytest.param("17", id="number"),
        pytest.param("true", id="boolean"),
        pytest.param("null", id="null"),
    ],
)
def test_load_project_preference_defaults_rejects_non_object_json(
    preferences_value: str,
) -> None:
    operations = FakeProjectPreferenceAdapter(
        project_preferences=[
            FakeProjectPreference(
                id=1,
                code=1,
                project_code_value=7,
                state=1,
                preferences_value=preferences_value,
            )
        ]
    )

    with pytest.raises(
        ConflictError,
        match="Stored project preference must be one JSON object",
    ) as exc_info:
        load_project_preference_defaults_from_operations(
            operations,
            project_code=7,
        )

    assert exc_info.value.details == {
        "projectCode": 7,
        "field": "preferences",
    }
    assert exc_info.value.suggestion == (
        "Fix the remote value with `dsctl project-preference update` before retrying."
    )
