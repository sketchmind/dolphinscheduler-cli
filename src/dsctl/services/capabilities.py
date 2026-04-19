from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

from dsctl import __version__
from dsctl.cli_surface import (
    AUDIT_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
)
from dsctl.config import load_selected_ds_version
from dsctl.errors import UserInputError
from dsctl.models.task_spec import supported_typed_task_types
from dsctl.output import CommandResult, require_json_object
from dsctl.services._surface_metadata import (
    error_capabilities_data,
    monitor_capabilities_data,
    output_capabilities_data,
    planes_capabilities_data,
    resources_capabilities_data,
    runtime_capabilities_data,
    selection_capabilities_data,
    self_description_data,
)
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.enums import enum_capabilities_data
from dsctl.services.monitor import MONITOR_SERVER_TYPE_CHOICES
from dsctl.services.template import (
    cluster_config_template_capability_data,
    generic_task_template_types,
    parameter_syntax_index_data,
    supported_task_template_types,
    task_template_metadata,
)
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    VersionSupport,
    VersionSupportData,
    get_default_version_support,
    get_version_support,
    supported_version_metadata,
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject


class DsCapabilitiesData(TypedDict):
    """Selected DS version support metadata emitted by capabilities."""

    current_version: str
    selected_version: str
    contract_version: str
    family: str
    support_level: str
    tested: bool
    supported_versions: list[str]
    versions: list[VersionSupportData]


CAPABILITIES_HEADER_KEYS = ("cli", "ds", "self_description")
CAPABILITIES_SECTION_CHOICES = (
    "selection",
    "output",
    "errors",
    "resources",
    "planes",
    "authoring",
    "schedule",
    "monitor",
    "enums",
    "runtime",
)
CAPABILITIES_SUMMARY_SECTIONS = (
    "resources",
    "planes",
    "runtime",
    "schedule",
    "monitor",
    "enums",
)
AUTHORING_SUMMARY_KEYS = (
    "workflow_yaml_create",
    "workflow_yaml_export",
    "workflow_yaml_lint",
    "workflow_digest",
    "workflow_schedule_block",
    "workflow_dry_run",
    "cluster_config_template",
    "task_template_types",
    "task_authoring_schema",
    "datasource_payload_templates",
    "datasource_template_types",
    "typed_task_specs",
    "generic_task_template_types",
    "untemplated_upstream_task_types",
)


def get_capabilities_result(
    *,
    env_file: str | None = None,
    summary: bool = False,
    section: str | None = None,
) -> CommandResult:
    """Return stable capability discovery for the current CLI surface."""
    if summary and section is not None:
        message = "--summary and --section are mutually exclusive"
        raise UserInputError(
            message,
            suggestion="Pass either --summary or --section SECTION, not both.",
        )
    selected_version = load_selected_ds_version(env_file)
    support = get_version_support(selected_version)
    data = require_json_object(
        _capabilities_data(support),
        label="capabilities data",
    )
    if summary:
        return CommandResult(
            data=_capabilities_summary_data(data),
            resolved={"capabilities": {"view": "summary"}},
        )
    if section is not None:
        normalized_section = section.strip()
        return CommandResult(
            data=_capabilities_section_data(data, normalized_section),
            resolved={
                "capabilities": {
                    "view": "section",
                    "section": normalized_section,
                }
            },
        )
    return CommandResult(
        data=data,
    )


def schema_capabilities_data(*, ds_version: str | None = None) -> dict[str, object]:
    """Return the schema-scoped capabilities subset."""
    support = (
        get_default_version_support()
        if ds_version is None
        else get_version_support(ds_version)
    )
    task_types = list(supported_task_template_types())
    typed_task_specs = list(supported_typed_task_types())
    generic_task_templates = list(generic_task_template_types())
    upstream_task_types = list(upstream_default_task_types())
    upstream_task_types_by_category = {
        category: list(task_types)
        for category, task_types in upstream_default_task_types_by_category().items()
    }
    return {
        "ds": _ds_capabilities_data(support),
        "output": output_capabilities_data(),
        "errors": error_capabilities_data(),
        "self_description": self_description_data(),
        "templates": {
            "workflow": {
                "with_schedule_option": True,
                "raw_template_command": "dsctl template workflow --raw",
            },
            "parameters": parameter_syntax_index_data(),
            "environment": {
                "command": "dsctl template environment",
                "source_options": ["--config TEXT", "--config-file PATH"],
                "target_commands": [
                    "dsctl environment create --name NAME --config-file env.sh",
                    "dsctl environment update ENVIRONMENT --config-file env.sh",
                ],
            },
            "cluster": cluster_config_template_capability_data(),
            "datasource": datasource_template_index_data(),
            "task": {
                "supported_types": task_types,
                "typed_types": typed_task_specs,
                "generic_types": generic_task_templates,
                "templates_by_type": task_template_metadata(),
                "index_command": "dsctl template task",
                "summary_command_pattern": "dsctl task-type get TYPE",
                "schema_command_pattern": "dsctl task-type schema TYPE",
                "raw_template_command_pattern": "dsctl template task TYPE --raw",
            },
        },
        "authoring": {
            "workflow_yaml_create": True,
            "workflow_yaml_export": True,
            "workflow_yaml_lint": True,
            "workflow_digest": True,
            "workflow_schedule_block": True,
            "workflow_dry_run": True,
            "environment_config_template": True,
            "cluster_config_template": True,
            "datasource_payload_templates": True,
            "task_authoring_schema": True,
            "task_authoring_schema_command_pattern": "dsctl task-type schema TYPE",
            "datasource_template_types": datasource_template_index_data()[
                "supported_types"
            ],
            "typed_task_specs": typed_task_specs,
            "generic_task_template_types": generic_task_templates,
            "upstream_default_task_types": upstream_task_types,
            "upstream_default_task_types_by_category": upstream_task_types_by_category,
            "untemplated_upstream_task_types": [
                task_type
                for task_type in upstream_task_types
                if task_type not in task_types
            ],
        },
        "schedule": {
            "preview": True,
            "explain": True,
            "risk_confirmation": True,
        },
        "monitor": monitor_capabilities_data(MONITOR_SERVER_TYPE_CHOICES),
        "enums": enum_capabilities_data(ds_version=support.server_version),
        "runtime": {
            AUDIT_RESOURCE: True,
            WORKFLOW_INSTANCE_RESOURCE: True,
            TASK_INSTANCE_RESOURCE: True,
        },
    }


def _capabilities_data(support: VersionSupport) -> dict[str, object]:
    task_types = list(supported_task_template_types())
    typed_task_specs = list(supported_typed_task_types())
    generic_task_templates = list(generic_task_template_types())
    upstream_task_types = list(upstream_default_task_types())
    upstream_task_types_by_category = {
        category: list(task_types)
        for category, task_types in upstream_default_task_types_by_category().items()
    }
    return {
        "cli": {
            "name": "dsctl",
            "version": __version__,
        },
        "ds": {
            "current_version": support.server_version,
            "selected_version": support.server_version,
            "contract_version": support.contract_version,
            "family": support.family,
            "support_level": support.support_level,
            "tested": support.tested,
            "supported_versions": list(SUPPORTED_VERSIONS),
            "versions": list(supported_version_metadata()),
        },
        "selection": selection_capabilities_data(),
        "output": output_capabilities_data(),
        "errors": error_capabilities_data(),
        "self_description": self_description_data(),
        "resources": resources_capabilities_data(),
        "planes": planes_capabilities_data(),
        "authoring": {
            "workflow_yaml_create": True,
            "workflow_yaml_export": True,
            "workflow_yaml_lint": True,
            "workflow_digest": True,
            "workflow_schedule_block": True,
            "workflow_dry_run": True,
            "parameter_syntax": parameter_syntax_index_data(),
            "environment_config_template": True,
            "cluster_config_template": True,
            "task_template_types": task_types,
            "task_authoring_schema": True,
            "task_authoring_schema_command_pattern": "dsctl task-type schema TYPE",
            "datasource_payload_templates": True,
            "datasource_template_types": datasource_template_index_data()[
                "supported_types"
            ],
            "task_templates": task_template_metadata(),
            "typed_task_specs": typed_task_specs,
            "generic_task_template_types": generic_task_templates,
            "logic_task_types": upstream_task_types_by_category["Logic"],
            "upstream_default_task_types": upstream_task_types,
            "upstream_default_task_types_by_category": upstream_task_types_by_category,
            "untemplated_upstream_task_types": [
                task_type
                for task_type in upstream_task_types
                if task_type not in task_types
            ],
        },
        "schedule": {
            "preview": True,
            "explain": True,
            "risk_confirmation": True,
            "online_offline_lifecycle": True,
        },
        "monitor": monitor_capabilities_data(MONITOR_SERVER_TYPE_CHOICES),
        "enums": enum_capabilities_data(ds_version=support.server_version),
        "runtime": runtime_capabilities_data(),
    }


def _ds_capabilities_data(support: VersionSupport) -> DsCapabilitiesData:
    return {
        "current_version": support.server_version,
        "selected_version": support.server_version,
        "contract_version": support.contract_version,
        "family": support.family,
        "support_level": support.support_level,
        "tested": support.tested,
        "supported_versions": list(SUPPORTED_VERSIONS),
        "versions": list(supported_version_metadata()),
    }


def _capabilities_summary_data(capabilities: JsonObject) -> JsonObject:
    summary = _capabilities_header(capabilities)
    for section in CAPABILITIES_SUMMARY_SECTIONS:
        summary[section] = capabilities[section]
    summary["authoring"] = _authoring_summary(capabilities)
    return summary


def _capabilities_section_data(
    capabilities: JsonObject,
    section: str,
) -> JsonObject:
    if section not in CAPABILITIES_SECTION_CHOICES:
        message = f"Unknown capabilities section: {section}"
        raise UserInputError(
            message,
            details={
                "section": section,
                "available_sections": list(CAPABILITIES_SECTION_CHOICES),
            },
            suggestion=(
                "Run `dsctl capabilities --summary` or pass one section name "
                "from the available_sections list."
            ),
        )
    section_data = capabilities.get(section)
    if section_data is None:
        message = f"Capabilities section is not available: {section}"
        raise UserInputError(message, details={"section": section})
    data = _capabilities_header(capabilities)
    data[section] = section_data
    return data


def _capabilities_header(capabilities: JsonObject) -> JsonObject:
    return {key: capabilities[key] for key in CAPABILITIES_HEADER_KEYS}


def _authoring_summary(capabilities: JsonObject) -> JsonObject:
    authoring_value = capabilities.get("authoring")
    if not isinstance(authoring_value, Mapping):
        message = "capabilities data is missing authoring"
        raise TypeError(message)
    return {
        key: authoring_value[key]
        for key in AUTHORING_SUMMARY_KEYS
        if key in authoring_value
    }
