from __future__ import annotations

from typing import TypedDict

from dsctl import __version__
from dsctl.cli_surface import (
    AUDIT_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
)
from dsctl.config import load_selected_ds_version
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
from dsctl.services.enums import enum_capabilities_data
from dsctl.services.monitor import MONITOR_SERVER_TYPE_CHOICES
from dsctl.services.template import (
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


def get_capabilities_result(*, env_file: str | None = None) -> CommandResult:
    """Return stable capability discovery for the current CLI surface."""
    selected_version = load_selected_ds_version(env_file)
    support = get_version_support(selected_version)
    return CommandResult(
        data=require_json_object(
            _capabilities_data(support),
            label="capabilities data",
        )
    )


def schema_capabilities_data() -> dict[str, object]:
    """Return the schema-scoped capabilities subset."""
    support = get_default_version_support()
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
            },
            "parameters": parameter_syntax_index_data(),
            "task": {
                "supported_types": task_types,
                "typed_types": typed_task_specs,
                "generic_types": generic_task_templates,
                "templates_by_type": task_template_metadata(),
            },
        },
        "authoring": {
            "workflow_yaml_create": True,
            "workflow_yaml_export": True,
            "workflow_yaml_lint": True,
            "workflow_digest": True,
            "workflow_schedule_block": True,
            "workflow_dry_run": True,
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
            "task_template_types": task_types,
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
