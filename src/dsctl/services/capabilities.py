from __future__ import annotations

from difflib import get_close_matches
from typing import TYPE_CHECKING, NotRequired, TypedDict

from dsctl import __version__
from dsctl.cli_surface import (
    AUDIT_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    stable_leaf_actions,
)
from dsctl.command_references import project_command_references
from dsctl.errors import ConfigError, UserInputError
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services._discovery_commands import (
    render_discovery_command,
    render_discovery_reference,
)
from dsctl.services._schema_constraints import constraints_for_action
from dsctl.services._surface_metadata import (
    error_capabilities_data,
    output_capabilities_data,
    planes_capabilities_data,
    resources_capabilities_data,
    runtime_capabilities_data,
    selection_capabilities_data,
    self_description_data,
)
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.enums import (
    candidate_enum_contract_metadata,
    compatible_enum_names,
    enum_capabilities_data,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import (
    cluster_config_template_capability_data,
    generic_task_template_types,
    parameter_syntax_index_data,
    supported_task_template_types,
    task_template_metadata,
)
from dsctl.services.version_resolution import (
    CompatibilityResolution,
    compatibility_details,
    resolve_target,
    selected_target_globals,
)
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    Availability,
    VersionSupport,
    VersionSupportData,
    get_default_version_support,
    get_version_support,
    supported_version_metadata,
)
from dsctl.upstream.observability import supported_monitor_server_types
from dsctl.upstream.read_compatibility import available_read_actions
from dsctl.upstream.schedule_environment import (
    schedule_environment_inheritance_supported,
    schedule_environment_limitation,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.yaml_io import JsonObject, JsonValue


class DsCapabilitiesData(TypedDict):
    """Selected DS version support metadata emitted by capabilities."""

    current_version: str
    selected_version: str
    contract_version: str
    family: str
    support_level: str
    tested: bool
    supported_version_count: int
    supported_versions: NotRequired[list[str]]
    versions: NotRequired[list[VersionSupportData]]
    catalog: JsonObject


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
AUTHORING_AVAILABILITY_SUMMARY_KEYS = (
    "workflow_yaml_create",
    "workflow_yaml_export",
    "workflow_yaml_lint",
    "workflow_yaml_edit",
    "workflow_digest",
    "workflow_schedule_block",
    "workflow_dry_run",
    "workflow_patch_template",
    "workflow_instance_patch_template",
    "workflow_instance_yaml_edit",
    "cluster_config_template",
    "task_authoring_schema",
    "datasource_payload_templates",
)
AUTHORING_INVENTORY_SUMMARY_KEYS = (
    "datasource_template_types",
    "task_template_types",
    "typed_task_specs",
    "generic_task_template_types",
    "untemplated_upstream_task_types",
)


def get_capabilities_result(
    *,
    env_file: str | None = None,
    summary: bool = False,
    section: str | None = None,
    full: bool = False,
    action: str | None = None,
) -> CommandResult:
    """Return bounded capability discovery unless expansion is explicit."""
    selected_views = sum((summary, section is not None, full, action is not None))
    if selected_views > 1:
        message = "--summary, --section, --full, and --action are mutually exclusive"
        raise UserInputError(
            message,
            suggestion=(
                "Pass at most one of --summary, --section SECTION, --full, "
                "or --action ACTION."
            ),
        )
    try:
        resolution = resolve_target(env_file, mode="local")
    except ConfigError as error:
        if error.details.get("reason") != "version_not_resolved":
            raise
        resolution = None
    if resolution is None or isinstance(resolution, CompatibilityResolution):
        return _unresolved_capabilities_result(
            resolution, section=section, full=full, action=action, env_file=env_file
        )
    support = get_version_support(resolution.version)
    if action is not None:
        return _action_capabilities_result(support, action, env_file=env_file)
    if section is not None:
        normalized_section = section.strip()
        return CommandResult(
            data=_capabilities_section_data(
                support, normalized_section, env_file=env_file
            ),
            resolved={
                "capabilities": {
                    "view": "section",
                    "section": normalized_section,
                }
            },
        )
    if full:
        return CommandResult(
            data=_full_capabilities_data(support, env_file=env_file),
            resolved={"capabilities": {"view": "full"}},
        )
    return CommandResult(
        data=_capabilities_summary_data(support, env_file=env_file),
        resolved={"capabilities": {"view": "summary"}},
    )


def _action_capabilities_result(
    support: VersionSupport,
    requested_action: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return one low-token exact-version capability fact."""
    action = requested_action.strip()
    if action not in stable_leaf_actions():
        _raise_unknown_capability_action(action, env_file=env_file)
    data = {
        "cli": {"name": "dsctl", "version": __version__},
        "ds": {
            "selected_version": support.server_version,
            "contract_version": support.contract_version,
            "family": support.family,
            "support_level": support.support_level,
            "tested": support.tested,
        },
        "capability": action_capability_metadata(support, action),
        "links": [
            {
                "rel": "schema",
                "command": render_discovery_command(
                    "schema", values={"command": action}, env_file=env_file
                ),
            },
            {"rel": "help", "command": _capability_help_command(action)},
        ],
    }
    return CommandResult(
        data=require_json_object(data, label="action capability data"),
        resolved={"capabilities": {"view": "action", "action": action}},
    )


def _full_capabilities_data(
    support: VersionSupport,
    *,
    env_file: str | None = None,
) -> JsonObject:
    """Expand the exact action catalog only for the explicit audit view."""
    expanded = _capabilities_data(support, env_file=env_file)
    expanded["action_catalog"] = [
        action_capability_metadata(support, action)
        for action in sorted(stable_leaf_actions())
    ]
    return require_json_object(expanded, label="full capabilities data")


def action_capability_metadata(support: VersionSupport, action: str) -> JsonObject:
    """Include exact runtime-only constraints in action discovery."""
    metadata = support.catalog.action_metadata(action)
    if action == "worker-group.update":
        limitations = [
            item
            for item in constraints_for_action(
                action, ds_version=support.server_version
            )
            if item.get("kind") == "requires_changed_value"
        ]
        if limitations:
            metadata["runtime_constraints"] = require_json_value(
                limitations, label="worker-group runtime constraints"
            )
    return metadata


def _raise_unknown_capability_action(
    action: str, *, env_file: str | None = None
) -> None:
    available = sorted(stable_leaf_actions())
    candidates = [
        {
            "action": candidate,
            "capabilities_command": render_discovery_command(
                "capabilities", values={"action": candidate}, env_file=env_file
            ),
            "schema_command": render_discovery_command(
                "schema", values={"command": candidate}, env_file=env_file
            ),
        }
        for candidate in get_close_matches(action, available, n=3, cutoff=0.5)
    ]
    suggestion = (
        f"Retry with `{candidates[0]['capabilities_command']}`."
        if candidates
        else f"Run `{render_discovery_command('schema', env_file=env_file)}` "
        "to browse the bounded action index."
    )
    message = f"Unknown capability action: {action}"
    raise UserInputError(
        message,
        details={
            "requested": action,
            "available_count": len(available),
            "candidates": candidates,
            "discovery_command": render_discovery_command("schema", env_file=env_file),
        },
        suggestion=suggestion,
    )


def _capability_help_command(action: str) -> str:
    return f"dsctl {action.replace('.', ' ')} --help"


def schema_capabilities_data(
    *, ds_version: str | None = None, env_file: str | None = None
) -> JsonObject:
    """Return the schema-scoped capabilities subset."""
    support = (
        get_default_version_support()
        if ds_version is None
        else get_version_support(ds_version)
    )
    authoring = _authoring_capabilities_data(support, expanded=False, env_file=env_file)
    return require_json_object(
        {
            "ds": _ds_capabilities_data(support),
            "output": output_capabilities_data(),
            "errors": error_capabilities_data(),
            "self_description": self_description_data(),
            "templates": _schema_templates_data(support, authoring, env_file=env_file),
            "authoring": authoring,
            "schedule": _schedule_capabilities_data(
                support,
                include_lifecycle=False,
            ),
            "monitor": _monitor_capabilities_data(support),
            "enums": _enum_capabilities_data(support),
            "runtime": _schema_runtime_capabilities_data(support),
        },
        label="schema capabilities data",
    )


def _capabilities_data(
    support: VersionSupport, *, env_file: str | None = None
) -> JsonObject:
    data: JsonObject = {
        **_capabilities_header_data(support, expanded=True),
    }
    for section in CAPABILITIES_SECTION_CHOICES:
        data[section] = _capabilities_section_value(support, section, env_file=env_file)
    return data


def _capabilities_header_data(
    support: VersionSupport, *, expanded: bool = False
) -> JsonObject:
    """Return version-neutral headers shared by every discovery view."""
    return require_json_object(
        {
            "cli": {
                "name": "dsctl",
                "version": __version__,
            },
            "ds": _ds_capabilities_data(support, expanded=expanded),
            "surface": {
                "inventory_scope": "installed_cli_surface",
                "selected_version_availability_source": "action_catalog",
                "action_availability_command_pattern": (
                    "dsctl capabilities --action ACTION"
                ),
            },
            "self_description": self_description_data(),
        },
        label="capabilities header data",
    )


def _capabilities_section_value(
    support: VersionSupport,
    section: str,
    *,
    env_file: str | None = None,
) -> JsonValue:
    """Build exactly one capability section for the selected version."""
    if section in {"selection", "output", "errors", "resources", "planes"}:
        return _version_neutral_section_value(section)
    return _version_specific_section_value(support, section, env_file=env_file)


def _version_neutral_section_value(section: str) -> JsonValue:
    if section == "selection":
        return require_json_value(
            selection_capabilities_data(),
            label="selection capabilities",
        )
    if section == "output":
        return require_json_value(
            output_capabilities_data(),
            label="output capabilities",
        )
    if section == "errors":
        return require_json_value(
            error_capabilities_data(),
            label="error capabilities",
        )
    if section == "resources":
        return require_json_value(
            resources_capabilities_data(),
            label="resource capabilities",
        )
    if section == "planes":
        return require_json_value(
            planes_capabilities_data(),
            label="plane capabilities",
        )
    message = f"Unknown version-neutral capabilities section: {section}"
    raise ValueError(message)


def _version_specific_section_value(
    support: VersionSupport,
    section: str,
    *,
    env_file: str | None = None,
) -> JsonValue:
    if section == "authoring":
        return _authoring_capabilities_data(support, expanded=True, env_file=env_file)
    if section == "schedule":
        return _schedule_capabilities_data(support, include_lifecycle=True)
    if section == "monitor":
        return _monitor_capabilities_data(support)
    if section == "enums":
        return _enum_capabilities_data(support)
    if section == "runtime":
        return _runtime_capabilities_data(support)
    message = f"Unknown capabilities section: {section}"
    raise ValueError(message)


def _authoring_capabilities_data(
    support: VersionSupport,
    *,
    expanded: bool,
    env_file: str | None = None,
) -> JsonObject:
    """Pair installed authoring inventory with exact-version availability."""
    catalog = get_task_authoring_catalog(support.server_version)
    template_task = _actions_supported(
        support,
        "template.task",
        "task-type.get",
        "task-type.schema",
    )
    task_types = list(supported_task_template_types(catalog=catalog))
    typed_task_specs = list(catalog.reviewed_typed_task_types)
    typed_task_specs.sort()
    generic_task_templates = list(generic_task_template_types(catalog=catalog))
    upstream_task_types = [
        catalog.source_task_type_for_cli(task_type) or task_type
        for task_type in task_types
    ]
    upstream_task_types.extend(
        source_task_type
        for source_task_type in catalog.upstream_task_types
        if source_task_type not in upstream_task_types
    )
    template_metadata = task_template_metadata(catalog=catalog)
    upstream_task_types_by_category: dict[str, list[str]] = {}
    for source_task_type in upstream_task_types:
        cli_task_type = (
            catalog.cli_task_type_for_source(source_task_type) or source_task_type
        )
        metadata = template_metadata.get(cli_task_type)
        entry = catalog.entries.get(cli_task_type)
        category = (
            metadata["category"]
            if metadata is not None
            else entry.category
            if entry is not None
            else "Upstream"
        )
        upstream_task_types_by_category.setdefault(category, []).append(
            source_task_type
        )
    datasource_templates = _actions_supported(support, "template.datasource")
    availability: JsonObject = {
        "workflow_yaml_create": _actions_supported(support, "workflow.create"),
        "workflow_yaml_export": _actions_supported(support, "workflow.export"),
        "workflow_yaml_lint": _actions_supported(support, "lint.workflow"),
        "workflow_yaml_edit": _actions_supported(support, "workflow.edit"),
        "workflow_digest": _actions_supported(support, "workflow.digest"),
        "workflow_schedule_block": _actions_supported(support, "workflow.create"),
        "workflow_dry_run": _actions_supported(support, "workflow.create"),
        "workflow_patch_template": _actions_supported(
            support,
            "template.workflow-patch",
        ),
        "workflow_instance_patch_template": _actions_supported(
            support,
            "template.workflow-instance-patch",
        ),
        "workflow_instance_yaml_edit": _actions_supported(
            support,
            "workflow-instance.edit",
        ),
        "environment_config_template": _actions_supported(
            support,
            "template.environment",
        ),
        "cluster_config_template": _actions_supported(support, "template.cluster"),
        "datasource_payload_templates": datasource_templates,
        "task_authoring_schema": template_task,
    }
    inventory: JsonObject = {
        "task_authoring_schema_command_pattern": "dsctl task-type schema TYPE",
        "datasource_template_types": (
            datasource_template_index_data(version=support.server_version)[
                "supported_types"
            ]
        ),
        "typed_task_specs": typed_task_specs,
        "generic_task_template_types": generic_task_templates,
        "upstream_default_task_types": upstream_task_types,
        "upstream_default_task_types_by_category": upstream_task_types_by_category,
        "untemplated_upstream_task_types": [
            source_task_type
            for source_task_type in upstream_task_types
            if (catalog.cli_task_type_for_source(source_task_type) or source_task_type)
            not in task_types
        ],
    }
    if expanded:
        parameter_syntax: JsonValue = require_json_value(
            _parameter_template_index_data(env_file=env_file),
            label="parameter syntax capabilities",
        )
        inventory.update(
            {
                "parameter_syntax": parameter_syntax,
                "task_template_types": task_types,
                "task_templates": require_json_value(
                    template_metadata,
                    label="task template metadata",
                ),
                "logic_task_types": upstream_task_types_by_category.get("Logic", []),
            }
        )
    return {
        "installed_cli_inventory": inventory,
        "selected_version_availability": availability,
    }


def _parameter_template_index_data(*, env_file: str | None) -> JsonObject:
    data = parameter_syntax_index_data()
    for topic in data["topics"]:
        topic["command"] = render_discovery_reference(
            topic["command"], env_file=env_file
        )
    return require_json_object(data, label="parameter template index")


def _schema_templates_data(
    support: VersionSupport,
    authoring: Mapping[str, JsonValue],
    *,
    env_file: str | None = None,
) -> JsonObject:
    """Return only template contracts executable for the selected DS profile."""
    installed_inventory = require_json_object(
        authoring["installed_cli_inventory"],
        label="installed authoring inventory",
    )
    templates: JsonObject = {}
    if _actions_supported(support, "template.workflow"):
        templates["workflow"] = {
            "with_schedule_option": True,
            "raw_template_command": render_discovery_command(
                "template.workflow", values={"raw": True}, env_file=env_file
            ),
            "export_command_pattern": "dsctl workflow export WORKFLOW",
        }
    if _actions_supported(support, "template.workflow-patch"):
        templates["workflow_patch"] = {
            "raw_template_command": render_discovery_command(
                "template.workflow-patch", values={"raw": True}, env_file=env_file
            ),
            "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
        }
    if _actions_supported(support, "template.workflow-instance-patch"):
        templates["workflow_instance_patch"] = {
            "raw_template_command": render_discovery_command(
                "template.workflow-instance-patch",
                values={"raw": True},
                env_file=env_file,
            ),
            "target_command_pattern": (
                "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                "PROJECT --patch FILE"
            ),
            "file_source_command_pattern": (
                "dsctl workflow-instance export WORKFLOW_INSTANCE --project PROJECT"
            ),
            "file_target_command_pattern": (
                "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                "PROJECT --file FILE"
            ),
        }
    if _actions_supported(support, "template.params"):
        templates["parameters"] = require_json_value(
            _parameter_template_index_data(env_file=env_file),
            label="parameter template metadata",
        )
    if _actions_supported(support, "template.environment"):
        templates["environment"] = {
            "command": render_discovery_command(
                "template.environment", env_file=env_file
            ),
            "source_options": ["--config CONFIG", "--config-file CONFIG_FILE"],
            "target_command_patterns": [
                "dsctl environment create --name NAME --config-file env.sh",
                "dsctl environment update ENVIRONMENT --config-file env.sh",
            ],
        }
    if _actions_supported(support, "template.cluster"):
        templates["cluster"] = require_json_value(
            {
                **cluster_config_template_capability_data(),
                "command": render_discovery_command(
                    "template.cluster", env_file=env_file
                ),
            },
            label="cluster template metadata",
        )
    if _actions_supported(support, "template.datasource"):
        datasource = datasource_template_index_data(version=support.server_version)
        if selected_target_globals(env_file):
            datasource["template_command"] = render_discovery_command(
                "template.datasource",
                values={
                    "ds-version": support.server_version,
                    "type": datasource["default_type"],
                },
                env_file=env_file,
            )
            datasource["type_discovery_command"] = render_discovery_command(
                "template.datasource",
                values={"ds-version": support.server_version},
                env_file=env_file,
            )
        templates["datasource"] = require_json_value(
            datasource, label="datasource template metadata"
        )
    if _actions_supported(
        support,
        "template.task",
        "task-type.get",
        "task-type.schema",
    ):
        catalog = get_task_authoring_catalog(support.server_version)
        templates["task"] = {
            "supported_types": list(supported_task_template_types(catalog=catalog)),
            "typed_types": installed_inventory["typed_task_specs"],
            "generic_types": installed_inventory["generic_task_template_types"],
            "templates_by_type": require_json_value(
                task_template_metadata(catalog=catalog),
                label="task template metadata",
            ),
            "index_command": render_discovery_command(
                "template.task", env_file=env_file
            ),
            "summary_command_pattern": "dsctl task-type get TYPE",
            "schema_command_pattern": "dsctl task-type schema TYPE",
            "raw_template_command_pattern": "dsctl template task TYPE --raw",
        }
    return require_json_object(
        project_command_references(templates),
        label="template capabilities",
    )


def _schedule_capabilities_data(
    support: VersionSupport,
    *,
    include_lifecycle: bool,
) -> JsonObject:
    data: JsonObject = {
        "preview": _actions_supported(support, "schedule.preview"),
        "explain": _actions_supported(support, "schedule.explain"),
        "environment_inheritance": schedule_environment_inheritance_supported(
            support.server_version
        ),
        "risk_confirmation": _actions_supported(
            support,
            "schedule.delete",
            "schedule.offline",
            "schedule.online",
        ),
    }
    limitation = schedule_environment_limitation(support.server_version)
    if limitation is not None:
        data["environment_limitation"] = limitation
    if include_lifecycle:
        data["online_offline_lifecycle"] = _actions_supported(
            support,
            "schedule.offline",
            "schedule.online",
        )
    return data


def _monitor_capabilities_data(support: VersionSupport) -> JsonObject:
    return {
        "health": _actions_supported(support, "monitor.health"),
        "database": _actions_supported(support, "monitor.database"),
        "server_types": (
            list(supported_monitor_server_types(support.server_version))
            if _actions_supported(support, "monitor.server")
            else []
        ),
    }


def _enum_capabilities_data(support: VersionSupport) -> JsonObject:
    if not _actions_supported(support, "enum.names", "enum.list"):
        return {"discovery": False, "names": []}
    return require_json_object(
        enum_capabilities_data(ds_version=support.server_version),
        label="enum capabilities",
    )


def _runtime_capabilities_data(support: VersionSupport) -> JsonObject:
    runtime = runtime_capabilities_data()
    return {
        resource: {
            "commands": [
                command
                for command in commands["commands"]
                if _actions_supported(support, f"{resource}.{command}")
            ]
        }
        for resource, commands in runtime.items()
    }


def _schema_runtime_capabilities_data(
    support: VersionSupport,
) -> JsonObject:
    return {
        resource: any(
            _actions_supported(support, action)
            for action in support.catalog.entries
            if action.startswith(f"{resource}.")
        )
        for resource in (
            AUDIT_RESOURCE,
            WORKFLOW_INSTANCE_RESOURCE,
            TASK_INSTANCE_RESOURCE,
        )
    }


def _actions_supported(support: VersionSupport, *actions: str) -> bool:
    return all(
        support.catalog.entries[action].availability is Availability.SUPPORTED
        for action in actions
    )


def _ds_capabilities_data(
    support: VersionSupport, *, expanded: bool = False
) -> DsCapabilitiesData:
    data: DsCapabilitiesData = {
        "current_version": support.server_version,
        "selected_version": support.server_version,
        "contract_version": support.contract_version,
        "family": support.family,
        "support_level": support.support_level,
        "tested": support.tested,
        "supported_version_count": len(SUPPORTED_VERSIONS),
        "catalog": support.catalog.summary_metadata(),
    }
    if expanded:
        data["supported_versions"] = list(SUPPORTED_VERSIONS)
        data["versions"] = list(supported_version_metadata())
    return data


def _capabilities_summary_data(
    support: VersionSupport, *, env_file: str | None = None
) -> JsonObject:
    summary = dict(_capabilities_header_data(support))
    for section in CAPABILITIES_SUMMARY_SECTIONS:
        summary[section] = _capabilities_section_value(
            support, section, env_file=env_file
        )
    summary["authoring"] = _authoring_summary(
        _authoring_capabilities_data(support, expanded=True, env_file=env_file)
    )
    return require_json_object(summary, label="capabilities summary")


def _capabilities_section_data(
    support: VersionSupport,
    section: str,
    *,
    env_file: str | None = None,
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
                "Run `dsctl capabilities` or pass one section name "
                "from the available_sections list."
            ),
        )
    data = dict(_capabilities_header_data(support))
    data[section] = _capabilities_section_value(support, section, env_file=env_file)
    return require_json_object(data, label="capabilities section")


def _authoring_summary(authoring_value: Mapping[str, JsonValue]) -> JsonObject:
    installed_inventory = require_json_object(
        authoring_value["installed_cli_inventory"],
        label="installed authoring inventory",
    )
    selected_version_availability = require_json_object(
        authoring_value["selected_version_availability"],
        label="selected-version authoring availability",
    )
    return {
        "installed_cli_inventory": {
            key: installed_inventory[key]
            for key in AUTHORING_INVENTORY_SUMMARY_KEYS
            if key in installed_inventory
        },
        "selected_version_availability": {
            key: selected_version_availability[key]
            for key in AUTHORING_AVAILABILITY_SUMMARY_KEYS
            if key in selected_version_availability
        },
    }


def unresolved_ds_data(resolution: CompatibilityResolution | None) -> JsonObject:
    """Public identity stays unknown even when one read contract is admissible."""
    details = (
        compatibility_details(resolution)
        if resolution is not None
        else {
            "ds_version": None,
            "reported_version": None,
            "candidate_versions": [],
            "identification": "unresolved",
            "version_source": "unresolved",
        }
    )
    return {
        **details,
        "selected_version": None,
        "contract_version": None,
        "support_level": "unknown",
        "tested": False,
    }


def compatible_read_actions(
    resolution: CompatibilityResolution | None,
) -> frozenset[str]:
    """Return the observed and semantically reviewed read action intersection."""
    if resolution is None:
        return frozenset()
    return available_read_actions(
        resolution.candidate_versions,
        compatible_operations=resolution.compatible_operations,
    )


def unresolved_action_capability(
    action: str,
    reads: frozenset[str],
    resolution: CompatibilityResolution | None,
) -> JsonObject:
    """Separate local invocation availability from admitted remote reads."""
    if action == "doctor":
        return {
            "action": action,
            "availability": "available_diagnostic",
            "verification": "installed_cli",
            "requires_exact_version": False,
            "authenticated_read_available": action in reads,
            "constraint": (
                "Diagnosis and discovery can run without an exact version; "
                "authenticated checks depend on the refreshed read evidence."
            ),
        }
    if action in {
        "schema",
        "capabilities",
        "context",
        "context.create",
        "context.update",
        "context.list",
        "context.get",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
    } or (action == "version" and resolution is not None):
        return {
            "action": action,
            "availability": "available_local",
            "verification": "installed_cli",
            "requires_exact_version": False,
        }
    if action in {"version", "enum.names", "enum.list"}:
        has_candidates = resolution is not None and bool(resolution.candidate_versions)
        return {
            "action": action,
            "availability": (
                "conditional_local" if action == "enum.list" else "available_local"
            )
            if has_candidates
            else "requires_discovery",
            "verification": "candidate_contracts" if has_candidates else "unknown",
            "requires_exact_version": False,
            "constraint": (
                "The requested enum must have an identical complete contract "
                "across all candidates; values describe generated candidate contracts."
                if has_candidates
                else "Run `dsctl doctor` to refresh discovery before this local query."
            ),
        }
    admitted = action in reads
    return {
        "action": action,
        "availability": "read_compatible" if admitted else "requires_exact_version",
        "verification": "reviewed_read_contract" if admitted else "unknown",
        "requires_exact_version": not admitted,
    }


def _unresolved_capabilities_result(
    resolution: CompatibilityResolution | None,
    *,
    section: str | None,
    full: bool,
    action: str | None,
    env_file: str | None = None,
) -> CommandResult:
    reads = compatible_read_actions(resolution)
    data: JsonObject = {
        "cli": {"name": "dsctl", "version": __version__},
        "ds": unresolved_ds_data(resolution),
        "surface": {
            "inventory_scope": "installed_cli_surface",
            "action_availability_command_pattern": "dsctl capabilities --action ACTION",
        },
        "self_description": require_json_object(
            self_description_data(), label="self description"
        ),
    }
    resolved: JsonObject = {"view": "summary"}
    if action is not None:
        selected = action.strip()
        if selected not in stable_leaf_actions():
            _raise_unknown_capability_action(selected, env_file=env_file)
        data["capability"] = unresolved_action_capability(selected, reads, resolution)
        data["links"] = [
            {
                "rel": "schema",
                "command": render_discovery_command(
                    "schema", values={"command": selected}, env_file=env_file
                ),
            },
            {"rel": "help", "command": _capability_help_command(selected)},
        ]
        resolved = {"view": "action", "action": selected}
    elif section is not None:
        selected_section = section.strip()
        if selected_section not in CAPABILITIES_SECTION_CHOICES:
            message = f"Unknown capabilities section: {selected_section}"
            raise UserInputError(
                message,
                details={
                    "section": selected_section,
                    "sections": list(CAPABILITIES_SECTION_CHOICES),
                },
                suggestion=("Choose a section shown by `dsctl capabilities --help`."),
            )
        if selected_section in {"selection", "output", "errors", "resources", "planes"}:
            data[selected_section] = _version_neutral_section_value(selected_section)
        elif selected_section == "enums":
            data["enums"] = {
                "discovery": True,
                "names": list(
                    compatible_enum_names(
                        resolution.candidate_versions if resolution else ()
                    )
                ),
                **candidate_enum_contract_metadata(),
            }
        else:
            actions = sorted(
                action
                for action in reads
                if (
                    action.startswith("schedule.")
                    if selected_section == "schedule"
                    else not action.startswith("schedule.")
                    if selected_section == "runtime"
                    else False
                )
            )
            data[selected_section] = {
                "exact_version_semantics": "unknown",
                "read_compatible_actions": actions,
                "requires_exact_version": selected_section in {"authoring", "monitor"},
            }
        resolved = {"view": "section", "section": selected_section}
    else:
        data["read_compatible_actions"] = sorted(reads)
        data["action_count"] = len(stable_leaf_actions())
        data["authoring"] = {"requires_exact_version": True}
        if full:
            data["action_catalog"] = [
                unresolved_action_capability(action, reads, resolution)
                for action in sorted(stable_leaf_actions())
            ]
            resolved = {"view": "full"}
    return CommandResult(data=data, resolved={"capabilities": resolved})
