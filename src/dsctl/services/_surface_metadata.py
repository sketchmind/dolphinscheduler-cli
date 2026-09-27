from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

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
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.result_navigation import (
    ACTION_INDEX_FIELDS,
    ACTION_INDEX_GROUP_FIELDS,
    ACTION_INDEX_TARGET_FIELDS,
    MAX_ACTION_INDEX_TARGETS,
    MAX_NEXT_ACTIONS,
    NEXT_ACTION_ITEM_FIELDS,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


SELECTION_PRECEDENCE: tuple[str, ...] = ("flag", "context")
SELECTOR_TYPES: dict[str, str] = {
    "opaque_name": "User-provided DS resource name.",
    "name_or_code": "Name-first selector with numeric code shortcut.",
    "name_or_native_identity": (
        "Project name or native numeric identity: id on DS 1.3.9, code on newer "
        "versions."
    ),
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
)
OUTPUT_OPTIONAL_SUCCESS_FIELDS: tuple[str, ...] = (
    "warnings",
    "next_actions",
    "action_index",
)
OUTPUT_ERROR_FIELDS: tuple[str, ...] = (
    *OUTPUT_SUCCESS_FIELDS,
    "error",
)


class SelfDescriptionData(TypedDict):
    """Machine-readable self-description capabilities emitted by the CLI."""

    schema: bool
    template: bool
    capabilities: bool
    command_invocation_source: str
    capabilities_scope: str
    surface_inventory_scope: str
    action_availability_command_pattern: str


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
        "formats": list(COMMAND_CATALOG.global_option("format").input.choices),
        "default_format": "json",
        "format_option": "--format",
        "columns_option": "--columns",
        "compact_json": True,
        "compact_list_encoding": "columns_rows",
        "compact_list_contract": {
            "data_shape_flag": "compact_rows",
            "fields": ["columns", "rows"],
            "column_selection": "top_level_fields",
            "scope_paths": "decoded_logical_collections",
        },
        "json_encoding": "utf-8",
        "default_json_layout": "pretty",
        "error_channel": "stderr",
        "row_diagnostics_channel": "stderr",
        "success_fields": list(OUTPUT_SUCCESS_FIELDS),
        "optional_success_fields": list(OUTPUT_OPTIONAL_SUCCESS_FIELDS),
        "error_fields": list(OUTPUT_ERROR_FIELDS),
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warnings": {"type": "array", "items": "object", "presence": "nonempty"},
        "data_shape_metadata": True,
        "json_column_projection": True,
        "next_actions": {
            "field": "next_actions",
            "presence": "successful_applicable_json_responses_only",
            "max_items": MAX_NEXT_ACTIONS,
            "ordered": True,
            "item_fields": list(NEXT_ACTION_ITEM_FIELDS),
            "command_kind": "complete_shell_invocation",
            "authorization": "advisory",
            "row_output": False,
            "preserves_env_file": True,
        },
        "action_index": {
            "field": "action_index",
            "presence": "successful_applicable_json_responses_only",
            "max_indexed_targets": MAX_ACTION_INDEX_TARGETS,
            "index_fields": list(ACTION_INDEX_FIELDS),
            "target_fields": list(ACTION_INDEX_TARGET_FIELDS),
            "group_fields": list(ACTION_INDEX_GROUP_FIELDS),
            "all_targets_semantics": "all_returned_rows",
            "authorization": "not_evaluated",
            "eligibility": "row_facts_only",
            "row_output": False,
        },
    }


def output_capabilities_data() -> dict[str, object]:
    """Return the capabilities-scoped standard output support flags."""
    return {
        "standard_envelope": True,
        "formats": list(COMMAND_CATALOG.global_option("format").input.choices),
        "default_format": "json",
        "compact_json": True,
        "compact_list_encoding": "columns_rows",
        "json_encoding": "utf-8",
        "default_json_layout": "pretty",
        "error_channel": "stderr",
        "row_diagnostics_channel": "stderr",
        "data_shape_metadata": True,
        "display_columns": True,
        "json_column_projection": True,
        "resolved_metadata": True,
        "warnings": True,
        "structured_warnings": True,
        "structured_errors": True,
        "structured_next_actions": True,
        "structured_action_index": True,
        "max_action_index_targets": MAX_ACTION_INDEX_TARGETS,
    }


def self_description_data() -> SelfDescriptionData:
    """Return stable self-description capability flags."""
    return {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
        "surface_inventory_scope": "installed_cli_surface",
        "action_availability_command_pattern": ("dsctl capabilities --action ACTION"),
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
