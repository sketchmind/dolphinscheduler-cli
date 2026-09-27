import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeAlertGroup,
    FakeAlertGroupAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    UserInputError,
)
from dsctl.services import alert_group as alert_group_service
from dsctl.upstream.alert_groups import (
    ALERT_GROUP_DOMAIN,
    AlertGroupDomain,
)


def _install_alert_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeAlertGroupAdapter,
    *,
    ds_version: str = "3.4.1",
) -> None:
    adapter.association = (
        "legacy-alert-type" if ds_version == "1.3.9" else "plugin-instance-ids"
    )
    domain = AlertGroupDomain(alert_groups=adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        alert_group_service,
        expected_domain=ALERT_GROUP_DOMAIN,
        runtime_domain=domain,
        profile_factory=lambda: make_profile(ds_version=ds_version),
    )


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
            "groupType": None,
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
        "groupType": None,
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
        "groupType": None,
        "description": "platform alerts",
        "createTime": None,
        "updateTime": None,
        "createUserId": 1,
    }


def test_139_create_alert_group_projects_group_type_instead_of_instance_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])
    _install_alert_group_service_fakes(
        monkeypatch,
        adapter,
        ds_version="1.3.9",
    )

    result = alert_group_service.create_alert_group_result(
        name="platform",
        group_type="SMS",
        description="platform alerts",
    )

    assert _mapping(result.data)["groupType"] == "SMS"
    assert _mapping(result.data)["alertInstanceIds"] is None


@pytest.mark.parametrize(
    ("ds_version", "group_type", "instance_ids", "message"),
    [
        ("1.3.9", None, None, "requires --group-type"),
        ("1.3.9", "EMAIL", [7], "does not support --instance-id"),
        ("3.4.1", "EMAIL", None, "does not support --group-type"),
    ],
)
def test_create_alert_group_rejects_nonrepresentable_version_association(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
    group_type: str | None,
    instance_ids: list[int] | None,
    message: str,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])
    _install_alert_group_service_fakes(
        monkeypatch,
        adapter,
        ds_version=ds_version,
    )

    with pytest.raises(UserInputError, match=message):
        alert_group_service.create_alert_group_result(
            name="platform",
            group_type=group_type,
            instance_ids=instance_ids,
        )

    assert adapter.alert_groups == []


def test_create_alert_group_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError) as exc_info:
        alert_group_service.create_alert_group_result(name="ops")

    assert exc_info.value.to_payload()["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10012,
        "result_message": "alarm group already exists",
    }


@pytest.mark.parametrize("result_code", [10027, 998877])
def test_create_alert_group_result_preserves_generic_upstream_failure(
    monkeypatch: pytest.MonkeyPatch,
    result_code: int,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])
    _install_alert_group_service_fakes(monkeypatch, adapter)

    upstream_error = ApiResultError(
        result_code=result_code,
        result_message="create alert group error",
    )

    def broken_create(
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> FakeAlertGroup:
        del group_name, description, alert_instance_ids, group_type
        raise upstream_error

    monkeypatch.setattr(adapter, "create", broken_create)

    with pytest.raises(ApiResultError, match="create alert group error") as exc_info:
        alert_group_service.create_alert_group_result(name="ops")

    assert exc_info.value is upstream_error
    assert exc_info.value.to_payload()["source"] == upstream_error.source


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
        "groupType": None,
        "description": None,
        "createTime": None,
        "updateTime": None,
        "createUserId": 1,
    }


def test_139_update_alert_group_preserves_or_changes_group_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(
        alert_groups=[
            FakeAlertGroup(
                id=21,
                group_name_value="ops",
                group_type_value="EMAIL",
                description="ops alerts",
            )
        ]
    )
    _install_alert_group_service_fakes(
        monkeypatch,
        adapter,
        ds_version="1.3.9",
    )

    renamed = alert_group_service.update_alert_group_result("ops", name="platform")
    assert _mapping(renamed.data)["groupType"] == "EMAIL"

    changed = alert_group_service.update_alert_group_result(
        "platform",
        group_type="SMS",
    )
    assert _mapping(changed.data)["groupType"] == "SMS"


def test_update_alert_group_result_rejects_no_effective_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=_alert_groups())
    _install_alert_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        alert_group_service.update_alert_group_result("ops", name="ops")

    assert exc_info.value.suggestion == (
        "Pass a different --name, --description, --instance-id, or --group-type "
        "value, or use --clear-description/--clear-instance-ids to remove "
        "stored values."
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
    assert _mapping(exc_info.value.to_payload()["source"])["result_code"] == 130030


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
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> FakeAlertGroup:
        del alert_group_id, group_name, description, alert_instance_ids, group_type
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
    assert _mapping(exc_info.value.to_payload()["source"])["result_code"] == 1400004
