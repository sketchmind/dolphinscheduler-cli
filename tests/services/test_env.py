from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeEnvironment,
    FakeEnvironmentAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiResultError, ConflictError, UserInputError
from dsctl.services import env as env_service
from dsctl.services import runtime as runtime_service


def _install_env_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeEnvironmentAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            environment_adapter=adapter,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_list_environments_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(code=7, name="prod", description="prod env"),
            FakeEnvironment(code=9, name="test", description="test env"),
        ]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.list_environments_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert {
        "total": data["total"],
        "totalPage": data["totalPage"],
        "pageSize": data["pageSize"],
        "currentPage": data["currentPage"],
        "pageNo": data["pageNo"],
    } == {
        "total": 2,
        "totalPage": 2,
        "pageSize": 1,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert list(items) == [
        {
            "id": None,
            "code": 7,
            "name": "prod",
            "config": None,
            "description": "prod env",
            "workerGroups": None,
            "operator": None,
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_list_environments_result_can_auto_exhaust_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(code=7, name="prod"),
            FakeEnvironment(code=9, name="test"),
        ]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.list_environments_result(page_size=1, all_pages=True)
    data = _mapping(result.data)

    assert data["total"] == 2
    assert data["totalPage"] == 1
    assert data["pageSize"] == 2
    assert [_mapping(item)["name"] for item in _sequence(data["totalList"])] == [
        "prod",
        "test",
    ]


def test_get_environment_result_resolves_name_then_fetches_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod", description="prod env")]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.get_environment_result("prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "environment": {"code": 7, "name": "prod", "description": "prod env"}
    }
    assert data["code"] == 7
    assert data["name"] == "prod"


def test_create_environment_result_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(environments=[])
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.create_environment_result(
        name="prod",
        config='{"JAVA_HOME":"/opt/java"}',
        description="production",
        worker_groups=["default", "gpu"],
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "environment": {"code": 1, "name": "prod", "description": "production"}
    }
    assert data["code"] == 1
    assert data["config"] == '{"JAVA_HOME":"/opt/java"}'
    assert data["workerGroups"] == ["default", "gpu"]


def test_create_environment_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod", config="existing")]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        env_service.create_environment_result(
            name="prod",
            config="next",
        )


def test_update_environment_result_preserves_omitted_fields_and_can_clear_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(
                code=7,
                name="prod",
                description="prod env",
                config='{"JAVA_HOME":"/opt/java"}',
                worker_groups_value=["default", "gpu"],
            )
        ]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.update_environment_result(
        "prod",
        config='{"JAVA_HOME":"/opt/java-21"}',
        description=None,
        worker_groups=[],
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "environment": {"code": 7, "name": "prod", "description": "prod env"}
    }
    assert data["name"] == "prod"
    assert data["description"] is None
    assert data["config"] == '{"JAVA_HOME":"/opt/java-21"}'
    assert data["workerGroups"] == []


def test_update_environment_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod", config="cfg")]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        env_service.update_environment_result("prod")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --name, --config, "
        "--description, --clear-description, --worker-group, or "
        "--clear-worker-groups."
    )


def test_update_environment_result_maps_upstream_input_rejection_with_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod", config="cfg")]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    def reject_update(
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str],
    ) -> FakeEnvironment:
        del code, name, config, description, worker_groups
        raise ApiResultError(
            result_code=130015,
            result_message="workerGroups invalid",
        )

    monkeypatch.setattr(adapter, "update", reject_update)

    with pytest.raises(UserInputError, match="workerGroups invalid") as exc_info:
        env_service.update_environment_result(
            "prod",
            config='{"JAVA_HOME":"/opt/java-21"}',
        )

    assert exc_info.value.suggestion == (
        "Verify --name, --config, and --worker-group values, then retry."
    )


def test_delete_environment_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod", description="prod env")]
    )
    _install_env_service_fakes(monkeypatch, adapter)

    result = env_service.delete_environment_result("prod", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "environment": {"code": 7, "name": "prod", "description": "prod env"}
    }
    assert data == {
        "deleted": True,
        "environment": {"code": 7, "name": "prod", "description": "prod env"},
    }
