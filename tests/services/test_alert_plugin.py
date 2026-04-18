import json
from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeAlertPlugin,
    FakeAlertPluginAdapter,
    FakePluginDefine,
    FakeProjectAdapter,
    FakeUiPluginAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    UserInputError,
)
from dsctl.services import alert_plugin as alert_plugin_service
from dsctl.services import runtime as runtime_service

ALERT_PLUGIN_PARAMS = json.dumps(
    [
        {
            "field": "url",
            "name": "url",
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
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            ui_plugin_adapter=ui_plugin_adapter,
            alert_plugin_adapter=alert_plugin_adapter,
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
            "title": None,
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
    assert data["schemaCommand"] == "alert-plugin schema PLUGIN"
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

    with pytest.raises(ConflictError):
        alert_plugin_service.create_alert_plugin_result(
            name="slack-ops",
            plugin="Slack",
            params_json=ALERT_PLUGIN_PARAMS,
        )


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
