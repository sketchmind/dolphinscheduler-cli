from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import ALERT_PLUGIN_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    AlertPluginData,
    optional_text,
    serialize_alert_plugin_list_item,
    serialize_plugin_define,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedAlertPluginData
from dsctl.services.resolver import alert_plugin as resolve_alert_plugin
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.upstream.protocol import (
        AlertPluginListItemRecord,
        AlertPluginPayloadRecord,
        PluginDefineRecord,
    )


QUERY_PLUGINS_ERROR = 110003
UPDATE_ALERT_PLUGIN_INSTANCE_ERROR = 110005
DELETE_ALERT_PLUGIN_INSTANCE_ERROR = 110006
GET_ALERT_PLUGIN_INSTANCE_ERROR = 110007
CREATE_ALERT_PLUGIN_INSTANCE_ERROR = 110008
QUERY_ALL_ALERT_PLUGIN_INSTANCE_ERROR = 110009
PLUGIN_INSTANCE_ALREADY_EXISTS = 110010
LIST_PAGING_ALERT_PLUGIN_INSTANCE_ERROR = 110011
DELETE_ALERT_PLUGIN_INSTANCE_ERROR_HAS_ALERT_GROUP_ASSOCIATED = 110012
ALERT_TEST_SENDING_FAILED = 110014
SEND_TEST_ALERT_PLUGIN_INSTANCE_ERROR = 110016
ALERT_SERVER_NOT_EXIST = 110017
USER_NO_OPERATION_PERM = 30001
ALERT_PLUGIN_TYPE = "ALERT"

AlertPluginPageData: TypeAlias = PageData[AlertPluginData]


class DeleteAlertPluginData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    alertPlugin: ResolvedAlertPluginData


class AlertPluginTestData(TypedDict):
    """CLI test confirmation payload."""

    tested: bool


class PluginDefineSelectionData(TypedDict):
    """Resolved alert-plugin definition selector emitted in JSON envelopes."""

    id: int
    pluginName: str
    pluginType: str | None


def list_alert_plugins_result(
    *,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List alert-plugin instances with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    return run_with_service_runtime(
        env_file,
        _list_alert_plugins_result,
        search=normalized_search,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
    )


def get_alert_plugin_result(
    alert_plugin: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one alert-plugin instance."""
    return run_with_service_runtime(
        env_file,
        _get_alert_plugin_result,
        alert_plugin=alert_plugin,
    )


def get_alert_plugin_schema_result(
    plugin: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Fetch one alert-plugin definition schema by name or id."""
    normalized_plugin = require_non_empty_text(plugin, label="alert plugin")
    return run_with_service_runtime(
        env_file,
        _get_alert_plugin_schema_result,
        plugin=normalized_plugin,
    )


def create_alert_plugin_result(
    *,
    name: str,
    plugin: str,
    params_json: str | None = None,
    file: Path | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one alert-plugin instance from DS-native UI param-list JSON."""
    normalized_name = require_non_empty_text(name, label="alert-plugin name")
    normalized_plugin = require_non_empty_text(plugin, label="alert plugin")
    plugin_instance_params = _plugin_instance_params_input(
        params_json=params_json,
        file=file,
        required=True,
    )
    if plugin_instance_params is None:
        message = "Alert-plugin params require exactly one input source"
        raise UserInputError(
            message,
            suggestion="Pass exactly one of --params-json or --file.",
        )
    return run_with_service_runtime(
        env_file,
        _create_alert_plugin_result,
        name=normalized_name,
        plugin=normalized_plugin,
        plugin_instance_params=plugin_instance_params,
    )


def update_alert_plugin_result(
    alert_plugin: str,
    *,
    name: str | None = None,
    params_json: str | None = None,
    file: Path | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one alert-plugin instance while preserving omitted fields."""
    if name is None and params_json is None and file is None:
        message = "Alert-plugin update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass --name or one of --params-json/--file.",
        )

    normalized_name = (
        require_non_empty_text(name, label="alert-plugin name")
        if name is not None
        else None
    )
    plugin_instance_params = _plugin_instance_params_input(
        params_json=params_json,
        file=file,
        required=False,
    )
    return run_with_service_runtime(
        env_file,
        _update_alert_plugin_result,
        alert_plugin=alert_plugin,
        name=normalized_name,
        plugin_instance_params=plugin_instance_params,
    )


def delete_alert_plugin_result(
    alert_plugin: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one alert-plugin instance after explicit confirmation."""
    require_delete_force(force=force, resource_label="Alert-plugin")
    return run_with_service_runtime(
        env_file,
        _delete_alert_plugin_result,
        alert_plugin=alert_plugin,
    )


def send_test_alert_plugin_result(
    alert_plugin: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Send one test alert using one existing alert-plugin instance."""
    return run_with_service_runtime(
        env_file,
        _send_test_alert_plugin_result,
        alert_plugin=alert_plugin,
    )


def _list_alert_plugins_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.alert_plugins
    data: AlertPluginPageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_alert_plugin_list_item,
        resource=ALERT_PLUGIN_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_alert_plugin_api_error(
            error,
            operation="list",
            instance_name=search,
        ),
    )
    return CommandResult(
        data=require_json_object(data, label="alert-plugin list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    alert_plugin: str,
) -> CommandResult:
    resolved_alert_plugin = resolve_alert_plugin(
        alert_plugin,
        adapter=runtime.upstream.alert_plugins,
    )
    fetched_alert_plugin = _require_alert_plugin_list_item(
        runtime,
        alert_plugin_id=resolved_alert_plugin.id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_alert_plugin_list_item(fetched_alert_plugin),
            label="alert-plugin data",
        ),
        resolved={
            "alertPlugin": require_json_object(
                resolved_alert_plugin.to_data(),
                label="resolved alert-plugin",
            )
        },
    )


def _get_alert_plugin_schema_result(
    runtime: ServiceRuntime,
    *,
    plugin: str,
) -> CommandResult:
    plugin_define = _resolve_plugin_define(runtime, plugin=plugin)
    return CommandResult(
        data=require_json_object(
            serialize_plugin_define(plugin_define),
            label="alert-plugin schema data",
        ),
        resolved={
            "pluginDefine": require_json_object(
                _plugin_define_selection_data(plugin_define),
                label="resolved alert-plugin definition",
            )
        },
    )


def _create_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    plugin: str,
    plugin_instance_params: str,
) -> CommandResult:
    plugin_define = _resolve_plugin_define(runtime, plugin=plugin)
    try:
        created_alert_plugin = runtime.upstream.alert_plugins.create(
            plugin_define_id=_plugin_define_id(plugin_define),
            instance_name=name,
            plugin_instance_params=plugin_instance_params,
        )
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="create",
            instance_name=name,
        ) from error

    created_alert_plugin_id = _alert_plugin_id_from_payload(
        runtime,
        payload=created_alert_plugin,
        instance_name=name,
        plugin_define_id=_plugin_define_id(plugin_define),
    )
    refreshed = _require_alert_plugin_list_item(
        runtime,
        alert_plugin_id=created_alert_plugin_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_alert_plugin_list_item(refreshed),
            label="alert-plugin data",
        ),
        resolved={
            "alertPlugin": require_json_object(
                _resolved_alert_plugin_data(refreshed),
                label="resolved alert-plugin",
            ),
            "pluginDefine": require_json_object(
                _plugin_define_selection_data(plugin_define),
                label="resolved alert-plugin definition",
            ),
        },
    )


def _update_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    alert_plugin: str,
    name: str | None,
    plugin_instance_params: str | None,
) -> CommandResult:
    resolved_alert_plugin = resolve_alert_plugin(
        alert_plugin,
        adapter=runtime.upstream.alert_plugins,
    )
    current_alert_plugin = _require_alert_plugin_list_item(
        runtime,
        alert_plugin_id=resolved_alert_plugin.id,
    )
    next_name = current_alert_plugin.instanceName if name is None else name
    next_params = (
        current_alert_plugin.pluginInstanceParams
        if plugin_instance_params is None
        else plugin_instance_params
    )
    if next_name is None or next_params is None:
        message = "Alert-plugin payload was missing required fields"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE, "id": resolved_alert_plugin.id},
        )
    try:
        runtime.upstream.alert_plugins.update(
            alert_plugin_id=resolved_alert_plugin.id,
            instance_name=next_name,
            plugin_instance_params=next_params,
        )
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="update",
            alert_plugin_id=resolved_alert_plugin.id,
            instance_name=next_name,
        ) from error

    refreshed = _require_alert_plugin_list_item(
        runtime,
        alert_plugin_id=resolved_alert_plugin.id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_alert_plugin_list_item(refreshed),
            label="alert-plugin data",
        ),
        resolved={
            "alertPlugin": require_json_object(
                resolved_alert_plugin.to_data(),
                label="resolved alert-plugin",
            )
        },
    )


def _delete_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    alert_plugin: str,
) -> CommandResult:
    resolved_alert_plugin = resolve_alert_plugin(
        alert_plugin,
        adapter=runtime.upstream.alert_plugins,
    )
    try:
        deleted = runtime.upstream.alert_plugins.delete(
            alert_plugin_id=resolved_alert_plugin.id
        )
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="delete",
            alert_plugin_id=resolved_alert_plugin.id,
            instance_name=resolved_alert_plugin.instance_name,
        ) from error
    return CommandResult(
        data=require_json_object(
            DeleteAlertPluginData(
                deleted=deleted,
                alertPlugin=resolved_alert_plugin.to_data(),
            ),
            label="alert-plugin delete data",
        ),
        resolved={
            "alertPlugin": require_json_object(
                resolved_alert_plugin.to_data(),
                label="resolved alert-plugin",
            )
        },
    )


def _send_test_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    alert_plugin: str,
) -> CommandResult:
    resolved_alert_plugin = resolve_alert_plugin(
        alert_plugin,
        adapter=runtime.upstream.alert_plugins,
    )
    current_alert_plugin = _require_alert_plugin_list_item(
        runtime,
        alert_plugin_id=resolved_alert_plugin.id,
    )
    plugin_instance_params = current_alert_plugin.pluginInstanceParams
    if plugin_instance_params is None:
        message = "Alert-plugin payload was missing pluginInstanceParams"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE, "id": resolved_alert_plugin.id},
        )
    try:
        tested = runtime.upstream.alert_plugins.test_send(
            plugin_define_id=current_alert_plugin.pluginDefineId,
            plugin_instance_params=plugin_instance_params,
        )
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="test",
            alert_plugin_id=resolved_alert_plugin.id,
            instance_name=resolved_alert_plugin.instance_name,
        ) from error
    return CommandResult(
        data=require_json_object(
            AlertPluginTestData(tested=tested),
            label="alert-plugin test data",
        ),
        resolved={
            "alertPlugin": require_json_object(
                resolved_alert_plugin.to_data(),
                label="resolved alert-plugin",
            )
        },
    )


def _resolve_plugin_define(
    runtime: ServiceRuntime,
    *,
    plugin: str,
) -> PluginDefineRecord:
    plugin_id = _parse_int(plugin)
    if plugin_id is not None:
        try:
            plugin_define = runtime.upstream.ui_plugins.get(plugin_id=plugin_id)
        except ApiResultError as error:
            raise _translate_ui_plugin_api_error(
                error,
                plugin_id=plugin_id,
            ) from error
        return _require_alert_plugin_define_type(plugin_define)

    try:
        plugin_defines = runtime.upstream.ui_plugins.list(plugin_type=ALERT_PLUGIN_TYPE)
    except ApiResultError as error:
        raise _translate_ui_plugin_api_error(error) from error
    matches = [
        plugin_define
        for plugin_define in plugin_defines
        if plugin_define.pluginName == plugin
    ]
    if not matches:
        message = f"Alert plugin {plugin!r} was not found"
        raise NotFoundError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE, "plugin": plugin},
        )
    if len(matches) > 1:
        message = f"Alert plugin name {plugin!r} is ambiguous"
        raise ConflictError(
            message,
            details={
                "resource": ALERT_PLUGIN_RESOURCE,
                "plugin": plugin,
                "ids": [_plugin_define_id(plugin_define) for plugin_define in matches],
            },
        )
    return matches[0]


def _require_alert_plugin_list_item(
    runtime: ServiceRuntime,
    *,
    alert_plugin_id: int,
) -> AlertPluginListItemRecord:
    try:
        alert_plugins = runtime.upstream.alert_plugins.list_all()
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="get",
            alert_plugin_id=alert_plugin_id,
        ) from error
    for alert_plugin in alert_plugins:
        if alert_plugin.id == alert_plugin_id:
            return alert_plugin
    message = f"Alert-plugin id {alert_plugin_id} was not found"
    raise NotFoundError(
        message,
        details={"resource": ALERT_PLUGIN_RESOURCE, "id": alert_plugin_id},
    )


def _alert_plugin_id_from_payload(
    runtime: ServiceRuntime,
    *,
    payload: AlertPluginPayloadRecord,
    instance_name: str,
    plugin_define_id: int,
) -> int:
    if payload.id is not None:
        return payload.id
    try:
        alert_plugins = runtime.upstream.alert_plugins.list_all()
    except ApiResultError as error:
        raise _translate_alert_plugin_api_error(
            error,
            operation="get",
            instance_name=instance_name,
        ) from error
    for alert_plugin in alert_plugins:
        if (
            alert_plugin.instanceName == instance_name
            and alert_plugin.pluginDefineId == plugin_define_id
        ):
            return alert_plugin.id
    message = "Alert-plugin create/update response was missing the new instance id"
    raise ApiTransportError(
        message,
        details={
            "resource": ALERT_PLUGIN_RESOURCE,
            "instanceName": instance_name,
            "pluginDefineId": plugin_define_id,
        },
    )


def _resolved_alert_plugin_data(
    alert_plugin: AlertPluginListItemRecord,
) -> ResolvedAlertPluginData:
    instance_name = alert_plugin.instanceName
    if instance_name is None:
        message = "Alert-plugin payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE, "id": alert_plugin.id},
        )
    return {
        "id": alert_plugin.id,
        "instanceName": instance_name,
        "pluginDefineId": alert_plugin.pluginDefineId,
        "alertPluginName": alert_plugin.alertPluginName,
    }


def _plugin_define_selection_data(
    plugin_define: PluginDefineRecord,
) -> PluginDefineSelectionData:
    return {
        "id": _plugin_define_id(plugin_define),
        "pluginName": _plugin_define_name(plugin_define),
        "pluginType": plugin_define.pluginType,
    }


def _plugin_define_id(plugin_define: PluginDefineRecord) -> int:
    plugin_id = plugin_define.id
    if plugin_id is None:
        message = "Alert-plugin definition payload was missing id"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE},
        )
    return plugin_id


def _plugin_define_name(plugin_define: PluginDefineRecord) -> str:
    plugin_name = plugin_define.pluginName
    if plugin_name is None:
        message = "Alert-plugin definition payload was missing pluginName"
        raise ApiTransportError(
            message,
            details={"resource": ALERT_PLUGIN_RESOURCE, "id": plugin_define.id},
        )
    return plugin_name


def _plugin_instance_params_input(
    *,
    params_json: str | None,
    file: Path | None,
    required: bool,
) -> str | None:
    if params_json is not None and file is not None:
        message = "Alert-plugin params require exactly one input source"
        raise UserInputError(
            message,
            suggestion="Pass exactly one of --params-json or --file.",
        )
    if params_json is None and file is None:
        if required:
            message = "Alert-plugin params require exactly one input source"
            raise UserInputError(
                message,
                suggestion="Pass exactly one of --params-json or --file.",
            )
        return None

    text = params_json
    if file is not None:
        text = file.read_text(encoding="utf-8")
    if text is None:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        message = "Alert-plugin params must be valid JSON"
        raise UserInputError(
            message,
            suggestion=(
                "Use `alert-plugin schema PLUGIN` to fetch the DS UI params "
                "template, fill the value fields, then retry."
            ),
        ) from error
    if not isinstance(parsed, list):
        message = "Alert-plugin params must be a JSON array"
        raise UserInputError(
            message,
            suggestion=(
                "Use `alert-plugin schema PLUGIN` to fetch the DS UI params "
                "template, fill the value fields, then retry."
            ),
        )
    if any(not isinstance(item, dict) for item in parsed):
        message = "Alert-plugin params must be a JSON array of objects"
        raise UserInputError(
            message,
            suggestion=(
                "Use `alert-plugin schema PLUGIN` to fetch the DS UI params "
                "template, fill the value fields, then retry."
            ),
        )
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))


def _require_alert_plugin_define_type(
    plugin_define: PluginDefineRecord,
) -> PluginDefineRecord:
    if plugin_define.pluginType == ALERT_PLUGIN_TYPE:
        return plugin_define
    message = "Resolved plugin is not an alert plugin definition"
    raise UserInputError(
        message,
        suggestion="Pass an ALERT plugin name or numeric id.",
    )


def _translate_alert_plugin_api_error(
    error: ApiResultError,
    *,
    operation: str,
    alert_plugin_id: int | None = None,
    instance_name: str | None = None,
) -> Exception:
    details: dict[str, object] = {
        "resource": ALERT_PLUGIN_RESOURCE,
        "operation": operation,
    }
    if alert_plugin_id is not None:
        details["id"] = alert_plugin_id
    if instance_name is not None:
        details["instanceName"] = instance_name

    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Alert-plugin {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code in {
        GET_ALERT_PLUGIN_INSTANCE_ERROR,
        QUERY_ALL_ALERT_PLUGIN_INSTANCE_ERROR,
        LIST_PAGING_ALERT_PLUGIN_INSTANCE_ERROR,
    }:
        message = f"Alert-plugin {operation} was rejected by the upstream API"
        return ConflictError(message, details=details)
    if error.result_code == PLUGIN_INSTANCE_ALREADY_EXISTS:
        return ConflictError(
            "Alert-plugin create/update conflicted with an existing instance name",
            details=details,
        )
    if error.result_code in {
        CREATE_ALERT_PLUGIN_INSTANCE_ERROR,
        UPDATE_ALERT_PLUGIN_INSTANCE_ERROR,
        DELETE_ALERT_PLUGIN_INSTANCE_ERROR,
        SEND_TEST_ALERT_PLUGIN_INSTANCE_ERROR,
    }:
        message = f"Alert-plugin {operation} was rejected by the upstream API"
        return ConflictError(message, details=details)
    if (
        error.result_code
        == DELETE_ALERT_PLUGIN_INSTANCE_ERROR_HAS_ALERT_GROUP_ASSOCIATED
    ):
        return ConflictError(
            "Alert-plugin is still referenced by one or more alert groups",
            details=details,
        )
    if error.result_code == ALERT_SERVER_NOT_EXIST:
        return InvalidStateError(
            "Alert-plugin test requires at least one live alert server",
            details=details,
            suggestion=(
                "Create or start at least one live alert server before "
                "retrying the alert-plugin test."
            ),
        )
    if error.result_code == ALERT_TEST_SENDING_FAILED:
        return ConflictError(
            "Alert-plugin test send failed in the upstream alert server",
            details=details,
        )
    return error


def _translate_ui_plugin_api_error(
    error: ApiResultError,
    *,
    plugin_id: int | None = None,
) -> Exception:
    details: dict[str, object] = {"resource": ALERT_PLUGIN_RESOURCE}
    if plugin_id is not None:
        details["plugin_id"] = plugin_id
        return NotFoundError(
            f"Alert plugin id {plugin_id} was not found",
            details=details,
        )
    if error.result_code == QUERY_PLUGINS_ERROR:
        return ConflictError(
            "Alert-plugin schema discovery was rejected by the upstream API",
            details=details,
        )
    return error


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
