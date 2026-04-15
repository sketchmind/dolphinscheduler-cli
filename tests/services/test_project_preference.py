from collections.abc import Mapping
from pathlib import Path

import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import UserInputError
from dsctl.services import project_preference as project_preference_service
from dsctl.services import runtime as runtime_service


def _install_project_preference_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    project_preference_adapter: FakeProjectPreferenceAdapter,
    context: SessionContext | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            project_preference_adapter=project_preference_adapter,
            profile=make_profile(),
            context=context,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="warehouse"),
        ]
    )


@pytest.fixture
def fake_project_preference_adapter() -> FakeProjectPreferenceAdapter:
    return FakeProjectPreferenceAdapter(
        project_preferences=[
            FakeProjectPreference(
                id=11,
                code=101,
                project_code_value=7,
                preferences_value='{"taskPriority":"MEDIUM"}',
                user_id_value=1,
                state=1,
                create_time_value="2026-04-11 10:00:00",
                update_time_value="2026-04-11 10:05:00",
            )
        ]
    )


def test_get_project_preference_result_returns_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_preference_adapter: FakeProjectPreferenceAdapter,
) -> None:
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=fake_project_preference_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = project_preference_service.get_project_preference_result()

    assert result.data == {
        "id": 11,
        "code": 101,
        "projectCode": 7,
        "preferences": '{"taskPriority":"MEDIUM"}',
        "userId": 1,
        "state": 1,
        "createTime": "2026-04-11 10:00:00",
        "updateTime": "2026-04-11 10:05:00",
    }
    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "context",
        }
    }


def test_get_project_preference_result_returns_null_when_absent(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=FakeProjectPreferenceAdapter(project_preferences=[]),
    )

    result = project_preference_service.get_project_preference_result(
        project="warehouse"
    )

    assert result.data is None
    assert result.resolved == {
        "project": {
            "code": 8,
            "name": "warehouse",
            "description": None,
            "source": "flag",
        }
    }


def test_update_project_preference_result_normalizes_inline_json(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_preference_adapter: FakeProjectPreferenceAdapter,
) -> None:
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=fake_project_preference_adapter,
    )

    result = project_preference_service.update_project_preference_result(
        project="etl-prod",
        preferences_json=' { "taskPriority": "HIGH", "warningGroupId": 7 } ',
    )
    data = _mapping(result.data)

    assert data["projectCode"] == 7
    assert data["state"] == 1
    assert data["preferences"] == '{"taskPriority":"HIGH","warningGroupId":7}'


def test_update_project_preference_result_accepts_file_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    preference_file = tmp_path / "preference.json"
    preference_file.write_text('{"taskPriority":"LOW"}', encoding="utf-8")
    adapter = FakeProjectPreferenceAdapter(project_preferences=[])
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=adapter,
    )

    result = project_preference_service.update_project_preference_result(
        project="warehouse",
        file=preference_file,
    )

    assert result.data == {
        "id": 1,
        "code": 1,
        "projectCode": 8,
        "preferences": '{"taskPriority":"LOW"}',
        "userId": None,
        "state": 1,
        "createTime": None,
        "updateTime": None,
    }


def test_update_project_preference_result_rejects_unreadable_file(
    tmp_path: Path,
) -> None:
    missing_file = tmp_path / "missing.json"

    with pytest.raises(UserInputError, match="could not be read") as exc_info:
        project_preference_service.update_project_preference_result(
            project="warehouse",
            file=missing_file,
        )

    assert exc_info.value.suggestion == (
        "Verify the --file path exists and is readable, then retry."
    )


def test_update_project_preference_result_requires_exactly_one_input() -> None:
    with pytest.raises(UserInputError, match="requires exactly one input source"):
        project_preference_service.update_project_preference_result(project="etl-prod")


def test_update_project_preference_result_rejects_non_object_json() -> None:
    with pytest.raises(UserInputError, match="must be a JSON object"):
        project_preference_service.update_project_preference_result(
            project="etl-prod",
            preferences_json='["not","an","object"]',
        )


def test_enable_project_preference_result_warns_when_row_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    adapter = FakeProjectPreferenceAdapter(project_preferences=[])
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=adapter,
    )

    result = project_preference_service.enable_project_preference_result(
        project="warehouse"
    )
    warning = (
        "Project preference state update was accepted, but the project has no "
        "stored project preference."
    )

    assert result.data is None
    assert adapter.state_updates == [(8, 1)]
    assert result.warnings == [warning]
    assert result.warning_details == [
        {
            "code": "project_preference_missing",
            "message": warning,
            "projectCode": 8,
            "requestedState": 1,
        }
    ]


def test_disable_project_preference_result_updates_existing_row(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_preference_adapter: FakeProjectPreferenceAdapter,
) -> None:
    _install_project_preference_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_preference_adapter=fake_project_preference_adapter,
    )

    result = project_preference_service.disable_project_preference_result(
        project="etl-prod"
    )

    assert _mapping(result.data)["state"] == 0
    assert fake_project_preference_adapter.state_updates == [(7, 0)]
    assert result.warnings == []
    assert result.warning_details == []
