from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import (
    AUDIT_RESOURCE,
    ID_FIRST_RESOURCES,
    NAME_FIRST_RESOURCES,
    PATH_FIRST_RESOURCES,
    RESOURCE_COMMANDS,
    SURFACE_PLANES,
    TASK_INSTANCE_RESOURCE,
    TOP_LEVEL_COMMANDS,
    WORKFLOW_INSTANCE_RESOURCE,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


SELECTION_PRECEDENCE: tuple[str, ...] = ("flag", "context")
SELECTOR_TYPES: dict[str, str] = {
    "opaque_name": "User-provided DS resource name.",
    "name_or_code": "Name-first selector with numeric code shortcut.",
    "name_or_id": "Name-first selector with numeric id shortcut.",
    "resource_path": "DS resource fullName path.",
    "id": "Numeric runtime or schedule id.",
}
CONFIRMATION_ERROR_TYPE = "confirmation_required"
CONFIRMATION_RETRY_OPTION = "--confirm-risk"
ERROR_SOURCE_KIND = "remote"
ERROR_SOURCE_SYSTEM = "dolphinscheduler"
ERROR_SOURCE_LAYERS: dict[str, tuple[str, ...]] = {
    "result": (
        "kind",
        "system",
        "layer",
        "result_code",
        "result_message",
    ),
    "http": (
        "kind",
        "system",
        "layer",
        "status_code",
    ),
}
OUTPUT_SUCCESS_FIELDS: tuple[str, ...] = (
    "ok",
    "action",
    "resolved",
    "data",
    "warnings",
    "warning_details",
)
OUTPUT_ERROR_FIELDS: tuple[str, ...] = (
    *OUTPUT_SUCCESS_FIELDS,
    "error",
)
TOP_LEVEL_COMMAND_SUMMARIES: dict[str, str] = {
    "version": "Return CLI and supported DolphinScheduler version metadata.",
    "context": "Return the effective config profile and stored session context.",
    "doctor": "Return structured local and remote diagnostics for the current runtime.",
    "schema": "Return the stable machine-readable schema for the current CLI surface.",
    "capabilities": "Return stable version and surface capability discovery.",
}


def selection_schema_data() -> dict[str, object]:
    """Return the schema-scoped selection contract."""
    return {
        "precedence": list(SELECTION_PRECEDENCE),
        "selector_types": dict(SELECTOR_TYPES),
        "name_first_resources": list(NAME_FIRST_RESOURCES),
        "path_first_resources": list(PATH_FIRST_RESOURCES),
        "id_first_resources": list(ID_FIRST_RESOURCES),
    }


def selection_capabilities_data() -> dict[str, object]:
    """Return the capabilities-scoped selection contract."""
    return {
        "precedence": list(SELECTION_PRECEDENCE),
        "name_first_resources": list(NAME_FIRST_RESOURCES),
        "path_first_resources": list(PATH_FIRST_RESOURCES),
        "id_first_resources": list(ID_FIRST_RESOURCES),
        "confirmation_retry_option": CONFIRMATION_RETRY_OPTION,
    }


def confirmation_schema_data() -> dict[str, str]:
    """Return the schema-scoped confirmation contract."""
    return {
        "error_type": CONFIRMATION_ERROR_TYPE,
        "retry_option": CONFIRMATION_RETRY_OPTION,
    }


def error_schema_data() -> dict[str, object]:
    """Return the schema-scoped structured error contract."""
    return {
        "fields": [
            "type",
            "message",
            "details",
            "source",
            "suggestion",
        ],
        "source": {
            "field": "error.source",
            "kind": ERROR_SOURCE_KIND,
            "system": ERROR_SOURCE_SYSTEM,
            "layers": {
                name: {"fields": list(fields)}
                for name, fields in ERROR_SOURCE_LAYERS.items()
            },
        },
    }


def error_capabilities_data() -> dict[str, object]:
    """Return the capabilities-scoped structured error support flags."""
    return {
        "structured": True,
        "suggestion": True,
        "source": True,
        "source_kind": ERROR_SOURCE_KIND,
        "source_system": ERROR_SOURCE_SYSTEM,
        "source_layers": list(ERROR_SOURCE_LAYERS),
    }


def output_schema_data() -> dict[str, object]:
    """Return the schema-scoped standard output envelope contract."""
    return {
        "success_fields": list(OUTPUT_SUCCESS_FIELDS),
        "error_fields": list(OUTPUT_ERROR_FIELDS),
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warning_details_aligned": True,
    }


def output_capabilities_data() -> dict[str, object]:
    """Return the capabilities-scoped standard output support flags."""
    return {
        "standard_envelope": True,
        "resolved_metadata": True,
        "warnings": True,
        "warning_details_alignment": True,
        "structured_errors": True,
    }


def self_description_data() -> dict[str, bool]:
    """Return stable self-description capability flags."""
    return {
        "schema": True,
        "template": True,
        "capabilities": True,
    }


def resources_capabilities_data() -> dict[str, object]:
    """Return command-surface discovery grouped by resource slug."""
    return {
        "top_level": list(TOP_LEVEL_COMMANDS),
        "groups": {
            name: {"commands": list(commands)}
            for name, commands in RESOURCE_COMMANDS.items()
        },
    }


def planes_capabilities_data() -> dict[str, list[str]]:
    """Return stable resource planes for the current surface."""
    return {name: list(resources) for name, resources in SURFACE_PLANES.items()}


def monitor_capabilities_data(server_types: Sequence[str]) -> dict[str, object]:
    """Return monitor discovery metadata."""
    return {
        "health": True,
        "database": True,
        "server_types": list(server_types),
    }


def runtime_capabilities_data() -> dict[str, dict[str, list[str]]]:
    """Return runtime-resource command discovery."""
    return {
        AUDIT_RESOURCE: {"commands": list(RESOURCE_COMMANDS[AUDIT_RESOURCE])},
        WORKFLOW_INSTANCE_RESOURCE: {
            "commands": list(RESOURCE_COMMANDS[WORKFLOW_INSTANCE_RESOURCE])
        },
        TASK_INSTANCE_RESOURCE: {
            "commands": list(RESOURCE_COMMANDS[TASK_INSTANCE_RESOURCE])
        },
    }
