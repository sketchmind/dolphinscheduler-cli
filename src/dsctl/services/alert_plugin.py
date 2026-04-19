from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, TypeAlias, TypedDict, cast

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
    StructuredDataValue,
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
QUERY_PLUGINS_RESULT_IS_NULL = 110002
QUERY_PLUGIN_DETAIL_RESULT_IS_NULL = 110004
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


class PluginDefineListItemData(TypedDict):
    """Alert-plugin definition list item emitted by `definition list`."""

    id: int
    pluginName: str
    pluginType: str | None
    createTime: str | None
    updateTime: str | None


class PluginDefinitionListData(TypedDict):
    """Alert-plugin definition discovery payload."""

    definitions: list[PluginDefineListItemData]
    count: int
    schema_command: str


PluginParamItem: TypeAlias = dict[str, StructuredDataValue]
PluginParamFieldData: TypeAlias = dict[str, StructuredDataValue]


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


def list_alert_plugin_definitions_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """List alert-plugin definitions supported by the current DS runtime."""
    return run_with_service_runtime(
        env_file,
        _list_alert_plugin_definitions_result,
    )


def create_alert_plugin_result(
    *,
    name: str,
    plugin: str,
    params_json: str | None = None,
    file: Path | None = None,
    params: list[str] | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one alert-plugin instance from UI params or inline key/value fields."""
    normalized_name = require_non_empty_text(name, label="alert-plugin name")
    normalized_plugin = require_non_empty_text(plugin, label="alert plugin")
    inline_params = _inline_param_items(params)
    _validate_plugin_param_sources(
        params_json=params_json,
        file=file,
        inline_params=inline_params,
        required=True,
    )
    plugin_instance_params = _plugin_instance_params_input(
        params_json=params_json,
        file=file,
        required=False,
    )
    return run_with_service_runtime(
        env_file,
        _create_alert_plugin_result,
        name=normalized_name,
        plugin=normalized_plugin,
        plugin_instance_params=plugin_instance_params,
        inline_params=inline_params,
    )


def update_alert_plugin_result(
    alert_plugin: str,
    *,
    name: str | None = None,
    params_json: str | None = None,
    file: Path | None = None,
    params: list[str] | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Update one alert-plugin instance while preserving omitted fields."""
    inline_params = _inline_param_items(params)
    if name is None and params_json is None and file is None and not inline_params:
        message = "Alert-plugin update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass --name, --param, --params-json, or --file.",
        )
    _validate_plugin_param_sources(
        params_json=params_json,
        file=file,
        inline_params=inline_params,
        required=False,
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
        inline_params=inline_params,
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
            _serialize_plugin_define_schema(plugin_define),
            label="alert-plugin schema data",
        ),
        resolved={
            "pluginDefine": require_json_object(
                _plugin_define_selection_data(plugin_define),
                label="resolved alert-plugin definition",
            )
        },
    )


def _list_alert_plugin_definitions_result(runtime: ServiceRuntime) -> CommandResult:
    try:
        plugin_defines = runtime.upstream.ui_plugins.list(plugin_type=ALERT_PLUGIN_TYPE)
    except ApiResultError as error:
        raise _translate_ui_plugin_api_error(error) from error
    definitions = [
        _serialize_plugin_define_list_item(plugin_define)
        for plugin_define in plugin_defines
    ]
    return CommandResult(
        data=require_json_object(
            PluginDefinitionListData(
                definitions=definitions,
                count=len(definitions),
                schema_command="alert-plugin schema PLUGIN",
            ),
            label="alert-plugin definition list data",
        ),
        resolved={
            "pluginDefinitions": {
                "pluginType": ALERT_PLUGIN_TYPE,
                "source": "ui-plugins/query-by-type",
            }
        },
    )


def _create_alert_plugin_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    plugin: str,
    plugin_instance_params: str | None,
    inline_params: Sequence[str],
) -> CommandResult:
    plugin_define = _resolve_plugin_define(runtime, plugin=plugin)
    if plugin_instance_params is None:
        plugin_instance_params = _plugin_instance_params_from_inline(
            plugin_define,
            inline_params=inline_params,
            existing_params=None,
        )
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
    inline_params: Sequence[str],
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
    next_params: str | None
    if plugin_instance_params is not None:
        next_params = plugin_instance_params
    elif inline_params:
        plugin_define = _resolve_plugin_define(
            runtime,
            plugin=str(current_alert_plugin.pluginDefineId),
        )
        next_params = _plugin_instance_params_from_inline(
            plugin_define,
            inline_params=inline_params,
            existing_params=current_alert_plugin.pluginInstanceParams,
        )
    else:
        next_params = current_alert_plugin.pluginInstanceParams
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
        normalized_plugin = plugin.casefold()
        matches = [
            plugin_define
            for plugin_define in plugin_defines
            if plugin_define.pluginName is not None
            and plugin_define.pluginName.casefold() == normalized_plugin
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
    plugin_define_id = _plugin_define_id(matches[0])
    try:
        detailed_plugin_define = runtime.upstream.ui_plugins.get(
            plugin_id=plugin_define_id
        )
    except ApiResultError as error:
        raise _translate_ui_plugin_api_error(
            error,
            plugin_id=plugin_define_id,
        ) from error
    return _require_alert_plugin_define_type(detailed_plugin_define)


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


def _serialize_plugin_define_schema(
    plugin_define: PluginDefineRecord,
) -> dict[str, StructuredDataValue]:
    data = cast(
        "dict[str, StructuredDataValue]",
        dict(serialize_plugin_define(plugin_define)),
    )
    data["pluginParamFields"] = _plugin_param_field_summaries(
        _plugin_param_template(plugin_define)
    )
    return data


def _serialize_plugin_define_list_item(
    plugin_define: PluginDefineRecord,
) -> PluginDefineListItemData:
    return {
        "id": _plugin_define_id(plugin_define),
        "pluginName": _plugin_define_name(plugin_define),
        "pluginType": plugin_define.pluginType,
        "createTime": plugin_define.createTime,
        "updateTime": plugin_define.updateTime,
    }


def _inline_param_items(params: list[str] | None) -> list[str]:
    if params is None:
        return []
    return [item for item in params if item is not None]


def _validate_plugin_param_sources(
    *,
    params_json: str | None,
    file: Path | None,
    inline_params: Sequence[str],
    required: bool,
) -> None:
    source_count = 0
    if params_json is not None:
        source_count += 1
    if file is not None:
        source_count += 1
    if inline_params:
        source_count += 1

    if required and source_count != 1:
        message = "Alert-plugin params require exactly one input source"
        raise UserInputError(
            message,
            suggestion="Pass exactly one of --param, --params-json, or --file.",
        )
    if not required and source_count > 1:
        message = "Alert-plugin params require at most one input source"
        raise UserInputError(
            message,
            suggestion="Pass only one of --param, --params-json, or --file.",
        )


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


def _plugin_instance_params_from_inline(
    plugin_define: PluginDefineRecord,
    *,
    inline_params: Sequence[str],
    existing_params: str | None,
) -> str:
    template = _plugin_param_template(plugin_define)
    if not template:
        message = "Alert-plugin definition does not expose configurable params"
        raise UserInputError(
            message,
            suggestion=(
                "Use --params-json or --file only when the upstream plugin "
                "requires raw params."
            ),
        )

    existing_values = _plugin_param_values(existing_params)
    inline_values = _canonical_inline_param_values(
        inline_params,
        template=template,
        plugin_define=plugin_define,
    )
    merged_params: list[PluginParamItem] = []
    missing_required_fields: list[str] = []

    for item in template:
        copied = dict(item)
        field = _plugin_param_field(copied)
        if field in existing_values:
            copied["value"] = existing_values[field]
        if field in inline_values:
            copied["value"] = inline_values[field]
        if _plugin_param_required(copied) and not _has_plugin_param_value(
            copied.get("value")
        ):
            missing_required_fields.append(field)
        merged_params.append(copied)

    if missing_required_fields:
        message = "Alert-plugin params are missing required fields"
        raise UserInputError(
            message,
            details={
                "resource": ALERT_PLUGIN_RESOURCE,
                "plugin": _plugin_define_name(plugin_define),
                "missing": missing_required_fields,
            },
            suggestion=(
                "Pass required fields with repeated --param KEY=VALUE options, "
                "or submit the full DS UI params with --params-json/--file."
            ),
        )

    return json.dumps(merged_params, ensure_ascii=False, separators=(",", ":"))


def _plugin_param_template(
    plugin_define: PluginDefineRecord,
) -> list[PluginParamItem]:
    plugin_params = plugin_define.pluginParams
    if plugin_params is None:
        return []
    return _parse_plugin_param_array(
        plugin_params,
        label="alert-plugin definition params",
        plugin_define=plugin_define,
    )


def _plugin_param_values(existing_params: str | None) -> dict[str, StructuredDataValue]:
    if existing_params is None:
        return {}
    values: dict[str, StructuredDataValue] = {}
    for item in _parse_plugin_param_array(
        existing_params,
        label="current alert-plugin params",
        plugin_define=None,
    ):
        field = _plugin_param_field(item)
        if "value" in item:
            values[field] = item["value"]
    return values


def _parse_plugin_param_array(
    value: str,
    *,
    label: str,
    plugin_define: PluginDefineRecord | None,
) -> list[PluginParamItem]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        message = f"{label} must be valid JSON"
        raise ApiTransportError(
            message,
            details=_plugin_param_error_details(plugin_define),
        ) from error
    if not isinstance(parsed, list) or any(
        not isinstance(item, Mapping) for item in parsed
    ):
        message = f"{label} must be a JSON array of objects"
        raise ApiTransportError(
            message,
            details=_plugin_param_error_details(plugin_define),
        )
    return [dict(item) for item in parsed]


def _canonical_inline_param_values(
    inline_params: Sequence[str],
    *,
    template: Sequence[Mapping[str, StructuredDataValue]],
    plugin_define: PluginDefineRecord,
) -> dict[str, str]:
    fields = [_plugin_param_field(item) for item in template]
    exact_fields = {field: field for field in fields}
    lower_fields: dict[str, list[str]] = {}
    for field in fields:
        lower_fields.setdefault(field.casefold(), []).append(field)

    values: dict[str, str] = {}
    for item in inline_params:
        raw_key, separator, raw_value = item.partition("=")
        key = raw_key.strip()
        if not separator or not key:
            message = f"Invalid --param value {item!r}; expected KEY=VALUE"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass alert-plugin params as `--param WebHook=https://...`; "
                    "repeat the option for multiple fields."
                ),
            )
        canonical_key = exact_fields.get(key)
        if canonical_key is None:
            case_matches = lower_fields.get(key.casefold(), [])
            if len(case_matches) == 1:
                canonical_key = case_matches[0]
            elif len(case_matches) > 1:
                message = f"Alert-plugin param field {key!r} is ambiguous"
                raise ConflictError(
                    message,
                    details={
                        "resource": ALERT_PLUGIN_RESOURCE,
                        "plugin": _plugin_define_name(plugin_define),
                        "matches": case_matches,
                    },
                )
        if canonical_key is None:
            message = f"Alert-plugin param field {key!r} is not supported"
            raise UserInputError(
                message,
                details={
                    "resource": ALERT_PLUGIN_RESOURCE,
                    "plugin": _plugin_define_name(plugin_define),
                    "supportedFields": fields,
                },
                suggestion=(
                    "Run `dsctl alert-plugin schema "
                    f"{_plugin_define_name(plugin_define)}` to inspect supported "
                    "fields."
                ),
            )
        if canonical_key in values:
            message = (
                f"Alert-plugin param field {canonical_key!r} was specified more "
                "than once"
            )
            raise UserInputError(
                message,
                suggestion="Pass each alert-plugin param field only once.",
            )
        values[canonical_key] = raw_value
    return values


def _plugin_param_field_summaries(
    params: Sequence[Mapping[str, StructuredDataValue]],
) -> list[PluginParamFieldData]:
    return [
        {
            "field": _plugin_param_field(item),
            "name": item.get("name"),
            "title": item.get("title"),
            "type": item.get("type"),
            "required": _plugin_param_required(item),
            "defaultValue": item.get("value"),
            "options": item.get("options"),
        }
        for item in params
    ]


def _plugin_param_field(item: Mapping[str, StructuredDataValue]) -> str:
    field = item.get("field")
    if not isinstance(field, str) or not field:
        message = "Alert-plugin param schema item was missing field"
        raise ApiTransportError(message, details={"resource": ALERT_PLUGIN_RESOURCE})
    return field


def _plugin_param_required(item: Mapping[str, StructuredDataValue]) -> bool:
    validate = item.get("validate")
    if not isinstance(validate, Sequence) or isinstance(validate, (str, bytes)):
        return False
    for rule in validate:
        if not isinstance(rule, Mapping):
            continue
        required = rule.get("required")
        if required is True:
            return True
        if isinstance(required, str) and required.casefold() == "true":
            return True
    return False


def _has_plugin_param_value(value: StructuredDataValue) -> bool:
    if value is None:
        return False
    return not (isinstance(value, str) and value == "")


def _plugin_param_error_details(
    plugin_define: PluginDefineRecord | None,
) -> dict[str, StructuredDataValue]:
    details: dict[str, StructuredDataValue] = {"resource": ALERT_PLUGIN_RESOURCE}
    if plugin_define is not None:
        details["plugin"] = _plugin_define_name(plugin_define)
        details["pluginDefineId"] = _plugin_define_id(plugin_define)
    return details


def _require_alert_plugin_define_type(
    plugin_define: PluginDefineRecord,
) -> PluginDefineRecord:
    plugin_type = plugin_define.pluginType
    if plugin_type is not None and plugin_type.casefold() == "alert":
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
        if error.result_code == QUERY_PLUGIN_DETAIL_RESULT_IS_NULL:
            return NotFoundError(
                f"Alert plugin id {plugin_id} was not found",
                details=details,
            )
    if error.result_code == QUERY_PLUGINS_ERROR:
        return ConflictError(
            "Alert-plugin schema discovery was rejected by the upstream API",
            details=details,
        )
    if error.result_code == QUERY_PLUGINS_RESULT_IS_NULL:
        return NotFoundError(
            "No alert plugin definitions were returned by the upstream API",
            details=details,
        )
    return error


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
