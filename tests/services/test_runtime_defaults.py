import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ConflictError
from dsctl.services._runtime_defaults import load_project_preference_defaults


def test_load_project_preference_defaults_returns_none_when_disabled() -> None:
    with fake_service_runtime(
        FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")]),
        profile=make_profile(),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=1,
                    code=1,
                    project_code_value=7,
                    state=0,
                    preferences_value='{"tenant":"tenant-pref"}',
                )
            ]
        ),
    ) as runtime:
        defaults = load_project_preference_defaults(runtime, project_code=7)

    assert defaults is None


def test_load_project_preference_defaults_rejects_invalid_json() -> None:
    with (
        fake_service_runtime(
            FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")]),
            profile=make_profile(),
            project_preference_adapter=FakeProjectPreferenceAdapter(
                project_preferences=[
                    FakeProjectPreference(
                        id=1,
                        code=1,
                        project_code_value=7,
                        state=1,
                        preferences_value="{invalid",
                    )
                ]
            ),
        ) as runtime,
        pytest.raises(
            ConflictError,
            match="Stored project preference must be valid JSON",
        ),
    ):
        load_project_preference_defaults(runtime, project_code=7)
