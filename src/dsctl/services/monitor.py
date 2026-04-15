from __future__ import annotations

from typing import TYPE_CHECKING, Final, TypedDict

from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    serialize_monitor_database,
    serialize_monitor_server,
)
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.services._serialization import MonitorDatabaseData

MONITOR_SERVER_TYPE_ALIASES: Final[dict[str, str]] = {
    "master": "MASTER",
    "worker": "WORKER",
    "alert-server": "ALERT_SERVER",
    "alert_server": "ALERT_SERVER",
    "alertserver": "ALERT_SERVER",
    "MASTER": "MASTER",
    "WORKER": "WORKER",
    "ALERT_SERVER": "ALERT_SERVER",
}

MONITOR_SERVER_TYPE_CHOICES: Final[tuple[str, ...]] = (
    "master",
    "worker",
    "alert-server",
)


class MonitorDatabaseWarningDetail(TypedDict):
    """Structured warning emitted for one degraded monitor database row."""

    code: str
    message: str
    db_type: str | None
    state: str | None


def get_health_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the API server actuator health payload."""
    return run_with_service_runtime(env_file, _get_health_result)


def list_servers_result(
    node_type: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the registry-backed server list for one node type."""
    normalized_node_type = _normalize_server_type(node_type)
    return run_with_service_runtime(
        env_file,
        _list_servers_result,
        node_type=normalized_node_type,
    )


def get_database_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the database health metrics payload."""
    return run_with_service_runtime(env_file, _get_database_result)


def _get_health_result(runtime: ServiceRuntime) -> CommandResult:
    return CommandResult(
        data=require_json_object(
            runtime.http_client.healthcheck(),
            label="monitor health data",
        ),
        resolved={
            "endpoint": runtime.profile.health_url,
            "scope": "api_server",
        },
    )


def _get_database_result(runtime: ServiceRuntime) -> CommandResult:
    payload = runtime.upstream.monitor.list_databases()
    items = [serialize_monitor_database(item) for item in payload]
    warnings, warning_details = _database_warning_payloads(items)
    return CommandResult(
        data=[
            require_json_object(
                item,
                label="monitor database data item",
            )
            for item in items
        ],
        resolved={
            "endpoint": f"{runtime.profile.api_url}/monitor/databases",
        },
        warnings=warnings,
        warning_details=warning_details,
    )


def _list_servers_result(
    runtime: ServiceRuntime,
    *,
    node_type: str,
) -> CommandResult:
    payload = runtime.upstream.monitor.list_servers(node_type=node_type)
    return CommandResult(
        data=[
            require_json_object(
                serialize_monitor_server(item),
                label="monitor server data item",
            )
            for item in payload
        ],
        resolved={
            "node_type": node_type,
        },
    )


def _normalize_server_type(node_type: str) -> str:
    normalized = node_type.strip()
    suggestion = f"Retry with one of: {', '.join(MONITOR_SERVER_TYPE_CHOICES)}."
    if not normalized:
        message = "Server node type must not be empty"
        raise UserInputError(message, suggestion=suggestion)
    resolved = MONITOR_SERVER_TYPE_ALIASES.get(normalized)
    if resolved is not None:
        return resolved
    lowered = normalized.lower()
    resolved = MONITOR_SERVER_TYPE_ALIASES.get(lowered)
    if resolved is not None:
        return resolved
    message = (
        "Unsupported monitor server type; expected one of "
        f"{', '.join(MONITOR_SERVER_TYPE_CHOICES)}"
    )
    raise UserInputError(
        message,
        details={"node_type": node_type},
        suggestion=suggestion,
    )


def _database_warning_payloads(
    items: list[MonitorDatabaseData],
) -> tuple[list[str], list[MonitorDatabaseWarningDetail]]:
    degraded_items = [item for item in items if item["state"] not in (None, "YES")]
    return (
        [
            (
                "database health degraded: "
                f"{item['dbType'] or 'UNKNOWN'} state={item['state'] or 'UNKNOWN'}"
            )
            for item in degraded_items
        ],
        [
            MonitorDatabaseWarningDetail(
                code="monitor_database_degraded",
                message=(
                    "database health degraded: "
                    f"{item['dbType'] or 'UNKNOWN'} state="
                    f"{item['state'] or 'UNKNOWN'}"
                ),
                db_type=(item["dbType"] if isinstance(item["dbType"], str) else None),
                state=item["state"] if isinstance(item["state"], str) else None,
            )
            for item in degraded_items
        ],
    )
