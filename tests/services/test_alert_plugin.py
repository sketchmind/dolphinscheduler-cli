import json

import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeAlertPlugin,
    FakeAlertPluginAdapter,
    FakePluginDefine,
    FakeUiPluginAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
)
from dsctl.services import alert_plugin as alert_plugin_service
from dsctl.upstream.alert_plugins import (
    ALERT_PLUGIN_DOMAIN,
    AlertPluginDomain,
)

ALERT_PLUGIN_PARAMS = json.dumps(
    [
        {
            "field": "url",
            "name": "url",
            "title": "Webhook URL",
            "type": "input",
            "value": "https://hooks.example.test/ops",
        }
    ],
    ensure_ascii=False,
)
ALERT_PLUGIN_SCHEMA = json.dumps(
    [
        {
            "field": "url",
            "name": "url",
            "title": "Webhook URL",
            "type": "input",
            "value": None,
            "props": {"placeholder": "Webhook URL"},
            "validate": [{"required": True}],
        }
    ],
    ensure_ascii=False,
)


def _install_alert_plugin_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ui_plugin_adapter: FakeUiPluginAdapter,
    alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    domain = AlertPluginDomain(
        ui_plugins=ui_plugin_adapter,
        alert_plugins=alert_plugin_adapter,
    )

    patch_bound_domain_service_runtime(
        monkeypatch,
        alert_plugin_service,
        expected_domain=ALERT_PLUGIN_DOMAIN,
        runtime_domain=domain,
        profile_factory=make_profile,
    )


@pytest.fixture
def fake_ui_plugin_adapter() -> FakeUiPluginAdapter:
    return FakeUiPluginAdapter(
        plugin_defines=[
            FakePluginDefine(
                id=3,
                plugin_name_value="Slack",
                plugin_type_value="alert",
                plugin_params_value=ALERT_PLUGIN_SCHEMA,
            )
        ]
    )


@pytest.fixture
def fake_alert_plugin_adapter() -> FakeAlertPluginAdapter:
    return FakeAlertPluginAdapter(
        alert_plugins=[
            FakeAlertPlugin(
                id=11,
                plugin_define_id_value=3,
                instance_name_value="slack-ops",
                plugin_instance_params_value=ALERT_PLUGIN_PARAMS,
                instance_type_value="ALERT",
                warning_type_value="ALL",
                alert_plugin_name_value="Slack",
            ),
            FakeAlertPlugin(
                id=12,
                plugin_define_id_value=3,
                instance_name_value="slack-dev",
                plugin_instance_params_value=ALERT_PLUGIN_PARAMS,
                instance_type_value="ALERT",
                warning_type_value="ALL",
                alert_plugin_name_value="Slack",
            ),
        ],
        plugin_names_by_id={3: "Slack"},
    )


def test_list_alert_plugins_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.list_alert_plugins_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 2
    assert data["pageSize"] == 1
    assert data["currentPage"] == 1
    assert list(items) == [
        {
            "id": 11,
            "pluginDefineId": 3,
            "instanceName": "slack-ops",
            "pluginInstanceParams": ALERT_PLUGIN_PARAMS,
            "createTime": None,
            "updateTime": None,
            "instanceType": "ALERT",
            "warningType": "ALL",
            "alertPluginName": "Slack",
        }
    ]


def test_get_alert_plugin_result_resolves_name_then_fetches_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.get_alert_plugin_result("slack-ops")

    assert result.resolved == {
        "alertPlugin": {
            "id": 11,
            "instanceName": "slack-ops",
            "pluginDefineId": 3,
            "alertPluginName": "Slack",
        }
    }
    assert _mapping(result.data)["pluginInstanceParams"] == ALERT_PLUGIN_PARAMS


def test_get_alert_plugin_result_maps_absent_numeric_id_to_not_found(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=FakeAlertPluginAdapter(alert_plugins=[]),
    )

    with pytest.raises(NotFoundError, match="Alert-plugin id 99 was not found"):
        alert_plugin_service.get_alert_plugin_result("99")


def test_get_alert_plugin_schema_result_returns_plugin_definition(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.get_alert_plugin_schema_result("Slack")

    assert result.resolved == {
        "pluginDefine": {
            "id": 3,
            "pluginName": "Slack",
            "pluginType": "alert",
        }
    }
    data = _mapping(result.data)
    assert data["pluginParams"] == ALERT_PLUGIN_SCHEMA
    assert data["pluginParamFields"] == [
        {
            "field": "url",
            "name": "url",
            "title": "Webhook URL",
            "type": "input",
            "required": True,
            "defaultValue": None,
            "options": None,
        }
    ]


def test_list_alert_plugin_definitions_result_returns_supported_definitions(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.list_alert_plugin_definitions_result()

    assert result.resolved == {
        "pluginDefinitions": {
            "pluginType": "ALERT",
            "source": "ui-plugins/query-by-type",
        }
    }
    data = _mapping(result.data)
    assert data["count"] == 1
    assert data["schema_command"] == "alert-plugin schema PLUGIN"
    assert data["definitions"] == [
        {
            "id": 3,
            "pluginName": "Slack",
            "pluginType": "alert",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_alert_plugin_schema_result_accepts_id_and_case_insensitive_name(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    by_id = alert_plugin_service.get_alert_plugin_schema_result("3")
    by_name = alert_plugin_service.get_alert_plugin_schema_result("slack")

    assert _mapping(by_id.data)["pluginParams"] == ALERT_PLUGIN_SCHEMA
    assert _mapping(by_name.data)["pluginName"] == "Slack"


def test_create_alert_plugin_result_returns_refreshed_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.create_alert_plugin_result(
        name="slack-nightly",
        plugin="Slack",
        params_json=ALERT_PLUGIN_PARAMS,
    )

    assert result.resolved == {
        "alertPlugin": {
            "id": 13,
            "instanceName": "slack-nightly",
            "pluginDefineId": 3,
            "alertPluginName": "Slack",
        },
        "pluginDefine": {
            "id": 3,
            "pluginName": "Slack",
            "pluginType": "alert",
        },
    }
    assert _mapping(result.data)["instanceName"] == "slack-nightly"


def test_create_alert_plugin_result_builds_params_from_inline_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.create_alert_plugin_result(
        name="slack-nightly",
        plugin="slack",
        params=["URL=https://hooks.example.test/nightly"],
    )

    data = _mapping(result.data)
    params = json.loads(str(data["pluginInstanceParams"]))
    assert data["instanceName"] == "slack-nightly"
    assert params[0]["field"] == "url"
    assert params[0]["value"] == "https://hooks.example.test/nightly"


def test_create_alert_plugin_result_rejects_non_array_params_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(UserInputError, match="JSON array"):
        alert_plugin_service.create_alert_plugin_result(
            name="slack-nightly",
            plugin="Slack",
            params_json=json.dumps({"url": "https://hooks.example.test/ops"}),
        )


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ([{"value": "missing-field"}], "missing required UI fields"),
    ],
)
def test_create_alert_plugin_result_rejects_unverifiable_raw_params(
    params: list[dict[str, object]],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(UserInputError, match=message) as captured:
        alert_plugin_service.create_alert_plugin_result(
            name="slack-nightly",
            plugin="Slack",
            params_json=json.dumps(params),
        )

    assert "alert-plugin schema" in (captured.value.suggestion or "") or (
        "string value" in (captured.value.suggestion or "")
    )
    assert all(
        item.instanceName != "slack-nightly"
        for item in fake_alert_plugin_adapter.alert_plugins
    )


def test_create_alert_plugin_result_accepts_nested_floats_and_empty_title(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )
    params = [
        {
            "field": "thresholds",
            "type": "input",
            "title": "",
            "value": [0.25, {"ratio": 1.2e20}],
        }
    ]

    result = alert_plugin_service.create_alert_plugin_result(
        name="slack-nightly",
        plugin="Slack",
        params_json=json.dumps(params),
    )

    assert json.loads(str(_mapping(result.data)["pluginInstanceParams"])) == params


def test_create_alert_plugin_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(ConflictError) as exc_info:
        alert_plugin_service.create_alert_plugin_result(
            name="slack-ops",
            plugin="Slack",
            params_json=ALERT_PLUGIN_PARAMS,
        )

    assert exc_info.value.to_payload()["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 110010,
        "result_message": "alert plugin slack-ops already exists",
    }


def test_delete_alert_plugin_referenced_by_group_explains_detail_inspection(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=110012,
        result_message="alarm group associated with this alert instance",
    )
    fake_alert_plugin_adapter.delete_errors_by_id[11] = upstream_error
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(ConflictError) as exc_info:
        alert_plugin_service.delete_alert_plugin_result("slack-ops", force=True)

    payload = exc_info.value.to_payload()
    assert payload["source"] == upstream_error.source
    assert _mapping(payload["details"])["id"] == 11
    suggestion = str(payload["suggestion"])
    assert "dsctl alert-group list --all" in suggestion
    assert "dsctl alert-group get GROUP" in suggestion
    assert "alertInstanceIds" in suggestion
    assert "instance id 11" in suggestion
    assert "--clear-instance-ids" in suggestion
    assert "dsctl alert-group update GROUP --instance-id ID" in suggestion
    assert [item.id for item in fake_alert_plugin_adapter.alert_plugins] == [11, 12]


def test_delete_alert_plugin_unknown_upstream_error_remains_untranslated(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=998877,
        result_message="unrecognized alert plugin error",
    )
    fake_alert_plugin_adapter.delete_errors_by_id[11] = upstream_error
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        alert_plugin_service.delete_alert_plugin_result("slack-ops", force=True)

    assert exc_info.value is upstream_error
    assert exc_info.value.to_payload()["source"] == upstream_error.source


def test_alert_plugin_schema_error_retains_ui_plugin_source(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(NotFoundError) as exc_info:
        alert_plugin_service.get_alert_plugin_schema_result("999")

    assert exc_info.value.to_payload()["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 110004,
        "result_message": "alert plugin define id 999 not found",
    }


def test_alert_plugin_schema_unknown_ui_error_remains_untranslated(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=998877,
        result_message="unrecognized UI plugin error",
    )

    def broken_get(*, plugin_id: int) -> FakePluginDefine:
        assert plugin_id == 3
        raise upstream_error

    monkeypatch.setattr(fake_ui_plugin_adapter, "get", broken_get)
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        alert_plugin_service.get_alert_plugin_schema_result("3")

    assert exc_info.value is upstream_error
    assert exc_info.value.to_payload()["source"] == upstream_error.source


def test_update_alert_plugin_result_preserves_omitted_params(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.update_alert_plugin_result(
        "slack-ops",
        name="slack-ops-renamed",
    )

    data = _mapping(result.data)
    assert data["instanceName"] == "slack-ops-renamed"
    assert data["pluginInstanceParams"] == ALERT_PLUGIN_PARAMS


def test_update_alert_plugin_result_overlays_inline_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.update_alert_plugin_result(
        "slack-ops",
        params=["url=https://hooks.example.test/updated"],
    )

    data = _mapping(result.data)
    params = json.loads(str(data["pluginInstanceParams"]))
    assert data["instanceName"] == "slack-ops"
    assert params[0]["value"] == "https://hooks.example.test/updated"


def test_update_alert_plugin_result_overlays_inline_fields_after_empty_readback(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    current = fake_alert_plugin_adapter.alert_plugins[0]
    fake_alert_plugin_adapter.alert_plugins[0] = FakeAlertPlugin(
        id=current.id,
        plugin_define_id_value=current.pluginDefineId,
        instance_name_value=current.instanceName,
        plugin_instance_params_value="null",
        instance_type_value=current.instanceType,
        warning_type_value=current.warningType,
        alert_plugin_name_value=current.alertPluginName,
    )
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.update_alert_plugin_result(
        "slack-ops",
        params=["url=https://hooks.example.test/updated"],
    )

    params = json.loads(str(_mapping(result.data)["pluginInstanceParams"]))
    assert params[0]["value"] == "https://hooks.example.test/updated"


def test_create_alert_plugin_result_requires_inline_required_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(UserInputError, match="missing required fields"):
        alert_plugin_service.create_alert_plugin_result(
            name="slack-nightly",
            plugin="Slack",
            params=["url="],
        )


def test_delete_alert_plugin_result_requires_force(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(UserInputError, match="requires --force"):
        alert_plugin_service.delete_alert_plugin_result("slack-ops", force=False)


def test_test_alert_plugin_result_uses_current_instance_params(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    result = alert_plugin_service.send_test_alert_plugin_result("slack-ops")

    assert _mapping(result.data) == {"tested": True}
    assert result.resolved == {
        "alertPlugin": {
            "id": 11,
            "instanceName": "slack-ops",
            "pluginDefineId": 3,
            "alertPluginName": "Slack",
        }
    }


def test_test_alert_plugin_result_requires_live_alert_server(
    monkeypatch: pytest.MonkeyPatch,
    fake_ui_plugin_adapter: FakeUiPluginAdapter,
    fake_alert_plugin_adapter: FakeAlertPluginAdapter,
) -> None:
    fake_alert_plugin_adapter.test_send_errors_by_plugin_define_id[3] = ApiResultError(
        result_code=110017,
        result_message="alert server not exist",
    )
    _install_alert_plugin_service_fakes(
        monkeypatch,
        ui_plugin_adapter=fake_ui_plugin_adapter,
        alert_plugin_adapter=fake_alert_plugin_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="requires at least one live alert server",
    ) as exc_info:
        alert_plugin_service.send_test_alert_plugin_result("slack-ops")

    assert exc_info.value.suggestion == (
        "Create or start at least one live alert server before retrying the "
        "alert-plugin test."
    )
