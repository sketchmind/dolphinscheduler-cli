from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeAlertGroup,
    FakeAlertGroupAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    UserInputError,
)
from dsctl.services import alert_group as alert_group_service
from dsctl.services import runtime as runtime_service


def _install_alert_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeAlertGroupAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            alert_group_adapter=adapter,
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


def _alert_groups() -> list[FakeAlertGroup]:
    return [
        FakeAlertGroup(
            id=21,
            group_name_value="ops",
            alert_instance_ids_value="7,8",
            description="ops alerts",
            create_user_id_value=1,
        ),
        FakeAlertGroup(
            id=22,
            group_name_value="etl",
            alert_instance_ids_value="9",
            description="etl alerts",
            create_user_id_value=1,
        ),
    ]


def test_list_alert_groups_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    result = alert_group_service.list_alert_groups_result(page_size=1)
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
            "id": 21,
            "groupName": "ops",
            "alertInstanceIds": "7,8",
            "description": "ops alerts",
            "createTime": None,
            "updateTime": None,
            "createUserId": 1,
        }
    ]


def test_get_alert_group_result_resolves_name_then_fetches_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    result = alert_group_service.get_alert_group_result("ops")
    data = _mapping(result.data)

    assert result.resolved == {
        "alertGroup": {
            "id": 21,
            "groupName": "ops",
            "description": "ops alerts",
        }
    }
    assert data == {
        "id": 21,
        "groupName": "ops",
        "alertInstanceIds": "7,8",
        "description": "ops alerts",
        "createTime": None,
        "updateTime": None,
        "createUserId": 1,
    }


def test_create_alert_group_result_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])
    _install_alert_group_service_fakes(monkeypatch, adapter)

    result = alert_group_service.create_alert_group_result(
        name="platform",
        instance_ids=[7, 8, 7],
        description="platform alerts",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "alertGroup": {
            "id": 1,
            "groupName": "platform",
            "description": "platform alerts",
        }
    }
    assert data == {
        "id": 1,
        "groupName": "platform",
        "alertInstanceIds": "7,8",
        "description": "platform alerts",
        "createTime": None,
        "updateTime": None,
        "createUserId": 1,
    }


def test_create_alert_group_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        alert_group_service.create_alert_group_result(name="ops")


def test_create_alert_group_result_preserves_generic_upstream_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])
    _install_alert_group_service_fakes(monkeypatch, adapter)

    def broken_create(
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> FakeAlertGroup:
        del group_name, description, alert_instance_ids
        raise ApiResultError(
            result_code=10027,
            result_message="create alert group error",
        )

    monkeypatch.setattr(adapter, "create", broken_create)

    with pytest.raises(ApiResultError, match="create alert group error") as exc_info:
        alert_group_service.create_alert_group_result(name="ops")

    assert exc_info.value.result_code == 10027


def test_update_alert_group_result_preserves_name_and_clears_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    result = alert_group_service.update_alert_group_result(
        "ops",
        description=None,
        instance_ids=[8, 9, 8],
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "alertGroup": {
            "id": 21,
            "groupName": "ops",
            "description": "ops alerts",
        }
    }
    assert data == {
        "id": 21,
        "groupName": "ops",
        "alertInstanceIds": "8,9",
        "description": None,
        "createTime": None,
        "updateTime": None,
        "createUserId": 1,
    }


def test_update_alert_group_result_rejects_no_effective_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        alert_group_service.update_alert_group_result("ops", name="ops")

    assert exc_info.value.suggestion == (
        "Pass a different --name, --description, or --instance-id value, or use "
        "--clear-description/--clear-instance-ids to remove stored values."
    )


def test_delete_alert_group_result_requires_force() -> None:
    with pytest.raises(UserInputError, match="requires --force"):
        alert_group_service.delete_alert_group_result("ops", force=False)


def test_delete_alert_group_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    result = alert_group_service.delete_alert_group_result("ops", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "alertGroup": {
            "id": 21,
            "groupName": "ops",
            "description": "ops alerts",
        }
    }
    assert data == {
        "deleted": True,
        "alertGroup": {
            "id": 21,
            "groupName": "ops",
            "description": "ops alerts",
        },
    }


def test_delete_alert_group_result_rejects_default_group_deletion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(
        alert_groups=[
            FakeAlertGroup(
                id=1,
                group_name_value="default",
                alert_instance_ids_value="",
                description="default alerts",
                create_user_id_value=1,
            )
        ]
    )
    _install_alert_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(InvalidStateError, match="default alert group") as exc_info:
        alert_group_service.delete_alert_group_result("default", force=True)

    assert exc_info.value.suggestion == (
        "Choose a non-default alert group; DolphinScheduler does not allow "
        "deleting the default group."
    )


def test_update_alert_group_result_maps_description_too_long_to_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    def reject_description(
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> FakeAlertGroup:
        del alert_group_id, group_name, description, alert_instance_ids
        raise ApiResultError(
            result_code=1400004,
            result_message="description too long",
        )

    monkeypatch.setattr(adapter, "update", reject_description)

    with pytest.raises(
        UserInputError,
        match="description was rejected by the upstream API",
    ) as exc_info:
        alert_group_service.update_alert_group_result(
            "ops",
            description="x" * 256,
        )

    assert exc_info.value.suggestion == (
        "Shorten --description and retry the same alert-group command."
    )
