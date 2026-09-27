from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl import cli_surface
from dsctl.services._schema_primitives import (
    CommandSchemaOverride,
    catalog_group,
)
from dsctl.services.datasource_payload import datasource_payload_command_data
from dsctl.services.enums import supported_enum_choices
from dsctl.services.monitor import MONITOR_SERVER_TYPE_CHOICES
from dsctl.services.template import (
    supported_datasource_types,
    supported_parameter_syntax_topics,
    supported_task_template_variants,
)
from dsctl.upstream import SUPPORTED_VERSIONS

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.support.yaml_io import JsonObject


GROUP_SUMMARIES: Mapping[str, str] = {
    cli_surface.CONTEXT_RESOURCE: (
        "Manage named connection contexts and inspect selection."
    ),
    cli_surface.CONFIG_RESOURCE: "Read and change user configuration defaults.",
    cli_surface.ENUM_RESOURCE: "Discover generated DolphinScheduler enums.",
    cli_surface.LINT_RESOURCE: (
        "Run local design-time checks without contacting DolphinScheduler."
    ),
    cli_surface.TASK_TYPE_RESOURCE: (
        "Discover DS task types and local task authoring contracts."
    ),
    cli_surface.ENV_RESOURCE: "Manage DolphinScheduler environments.",
    cli_surface.CLUSTER_RESOURCE: "Manage DolphinScheduler clusters.",
    cli_surface.DATASOURCE_RESOURCE: (
        "Manage DolphinScheduler datasources. Create/update use DS-native JSON "
        "payload files."
    ),
    cli_surface.NAMESPACE_RESOURCE: "Manage DolphinScheduler namespaces.",
    cli_surface.RESOURCE_RESOURCE: "Manage DolphinScheduler file resources.",
    cli_surface.QUEUE_RESOURCE: "Manage DolphinScheduler queues.",
    cli_surface.WORKER_GROUP_RESOURCE: "Manage DolphinScheduler worker groups.",
    cli_surface.TASK_GROUP_RESOURCE: "Manage DolphinScheduler task groups.",
    cli_surface.ALERT_PLUGIN_RESOURCE: (
        "Manage DolphinScheduler alert plugin instances."
    ),
    cli_surface.ALERT_GROUP_RESOURCE: "Manage DolphinScheduler alert groups.",
    cli_surface.TENANT_RESOURCE: "Manage DolphinScheduler tenants.",
    cli_surface.USER_RESOURCE: "Manage DolphinScheduler users.",
    cli_surface.ACCESS_TOKEN_RESOURCE: "Manage DolphinScheduler access tokens.",
    cli_surface.MONITOR_RESOURCE: (
        "Inspect DolphinScheduler platform health and runtime state."
    ),
    cli_surface.AUDIT_RESOURCE: (
        "Inspect DolphinScheduler audit logs and filter metadata."
    ),
    cli_surface.PROJECT_RESOURCE: "Manage DolphinScheduler projects.",
    cli_surface.PROJECT_PARAMETER_RESOURCE: (
        "Manage DolphinScheduler project parameters."
    ),
    cli_surface.PROJECT_PREFERENCE_RESOURCE: (
        "Manage the singleton DolphinScheduler project preference as a "
        "project-level default-value source."
    ),
    cli_surface.PROJECT_WORKER_GROUP_RESOURCE: (
        "Manage DolphinScheduler project worker-group assignments."
    ),
    cli_surface.SCHEDULE_RESOURCE: "Manage DolphinScheduler schedules.",
    cli_surface.TEMPLATE_RESOURCE: (
        "Emit stable templates for workflow authoring and DS-native payloads."
    ),
    cli_surface.WORKFLOW_RESOURCE: "Manage DolphinScheduler workflows.",
    cli_surface.WORKFLOW_INSTANCE_RESOURCE: (
        "Inspect DolphinScheduler workflow instances."
    ),
    cli_surface.TASK_RESOURCE: (
        "Manage DolphinScheduler task definitions inside workflows."
    ),
    cli_surface.TASK_INSTANCE_RESOURCE: (
        "Inspect and control DolphinScheduler task instances."
    ),
}

NESTED_GROUP_SUMMARIES: Mapping[tuple[str, ...], str] = {
    (cli_surface.TASK_GROUP_RESOURCE, "queue"): (
        "Manage DolphinScheduler task-group queues."
    ),
    (cli_surface.ALERT_PLUGIN_RESOURCE, "definition"): (
        "Discover supported alert-plugin definitions, not configured "
        "alert-plugin instances."
    ),
    (cli_surface.USER_RESOURCE, "grant"): ("Grant DolphinScheduler user permissions."),
    (cli_surface.USER_RESOURCE, "revoke"): (
        "Revoke DolphinScheduler user permissions."
    ),
    (cli_surface.WORKFLOW_RESOURCE, "lineage"): (
        "Inspect DolphinScheduler workflow lineage."
    ),
}


def build_schema_group(
    name: str,
    *,
    task_types: Sequence[str] = (),
) -> JsonObject:
    """Build one group from catalog routes plus non-derivable schema facts."""
    summary = GROUP_SUMMARIES[name]
    overrides = _command_overrides(name, task_types)
    nested_summaries = {
        path: nested_summary
        for path, nested_summary in NESTED_GROUP_SUMMARIES.items()
        if path[0] == name
    }
    return catalog_group(
        name,
        summary=summary,
        command_overrides=overrides,
        nested_group_summaries=nested_summaries,
    )


def _command_overrides(
    group_name: str,
    task_types: Sequence[str],
) -> dict[str, CommandSchemaOverride]:
    if group_name == cli_surface.ENUM_RESOURCE:
        return {
            "enum.list": CommandSchemaOverride(
                domain_choices={"enum": supported_enum_choices()}
            )
        }
    if group_name == cli_surface.DATASOURCE_RESOURCE:
        return {
            "datasource.create": CommandSchemaOverride(
                payload=datasource_payload_command_data()
            ),
            "datasource.update": CommandSchemaOverride(
                payload=datasource_payload_command_data()
            ),
        }
    if group_name == cli_surface.MONITOR_RESOURCE:
        return {
            "monitor.server": CommandSchemaOverride(
                domain_choices={"node_type": list(MONITOR_SERVER_TYPE_CHOICES)}
            )
        }
    if group_name == cli_surface.TEMPLATE_RESOURCE:
        return _template_overrides(task_types)
    if group_name == cli_surface.WORKFLOW_RESOURCE:
        return _workflow_overrides()
    if group_name == cli_surface.TASK_RESOURCE:
        return _task_overrides()
    if group_name == cli_surface.WORKFLOW_INSTANCE_RESOURCE:
        return _workflow_instance_overrides()
    if group_name == cli_surface.TASK_INSTANCE_RESOURCE:
        return {
            "task-instance.log": CommandSchemaOverride(
                payload={"raw_option": "--raw", "raw_field": "data.text"}
            )
        }
    return {}


def _template_overrides(
    task_types: Sequence[str],
) -> dict[str, CommandSchemaOverride]:
    return {
        "template.workflow": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "raw_option": "--raw",
                "template_command": "dsctl template workflow --raw",
                "target_command_pattern": "dsctl workflow create --file FILE",
            }
        ),
        "template.workflow-patch": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "raw_option": "--raw",
                "template_command": "dsctl template workflow-patch --raw",
                "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
            }
        ),
        "template.workflow-instance-patch": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "raw_option": "--raw",
                "template_command": "dsctl template workflow-instance-patch --raw",
                "target_command_pattern": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                    "PROJECT --patch FILE"
                ),
            }
        ),
        "template.params": CommandSchemaOverride(
            domain_choices={"topic": supported_parameter_syntax_topics()}
        ),
        "template.datasource": CommandSchemaOverride(
            domain_choices={
                "type": supported_datasource_types(),
                "ds-version": SUPPORTED_VERSIONS,
            }
        ),
        "template.task": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "raw_option": "--raw",
                "template_command_pattern": "dsctl template task TYPE --raw",
                "schema_command_pattern": "dsctl task-type schema TYPE",
                "paste_into": "workflow YAML tasks[]",
            },
            domain_choices={
                "task_type": task_types,
                "variant": supported_task_template_variants(),
            },
        ),
    }


def _workflow_overrides() -> dict[str, CommandSchemaOverride]:
    return {
        "workflow.export": CommandSchemaOverride(
            payload={
                "target_command_patterns": [
                    "dsctl workflow create --file FILE",
                    "dsctl workflow edit WORKFLOW --file FILE",
                ],
                "schedule_on_create": "desired_state",
                "schedule_on_edit": "read_only_snapshot",
            }
        ),
        "workflow.edit": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "source_options": ["--patch PATCH", "--file FILE"],
                "patch_template_command": "dsctl template workflow-patch --raw",
                "file_source_command_pattern": "dsctl workflow export WORKFLOW",
                "file_schedule": "read_only_snapshot",
                "file_template_command": "dsctl template workflow --raw",
                "target_command_patterns": [
                    "dsctl workflow edit WORKFLOW --patch FILE",
                    "dsctl workflow edit WORKFLOW --file FILE",
                ],
            }
        ),
    }


def _task_overrides() -> dict[str, CommandSchemaOverride]:
    return {
        "task.update": CommandSchemaOverride(
            payload={
                "scope": "workflow_definition",
                "resource_scope": "single_existing_task",
                "input_mode": "inline_set",
                "inspect_command_pattern": "dsctl task get TASK --workflow WORKFLOW",
                "supported_keys_command": "dsctl schema --command task.update",
                "target_command_pattern": "dsctl task update TASK --set KEY=VALUE",
                "use_workflow_edit_for": [
                    "create_task",
                    "delete_task",
                    "rename_task",
                    "task_type_change",
                    "multi_task_dag_edit",
                ],
                "use_workflow_instance_edit_for": ["finished_instance_repair"],
            }
        ),
    }


def _workflow_instance_overrides() -> dict[str, CommandSchemaOverride]:
    return {
        "workflow-instance.export": CommandSchemaOverride(
            payload={
                "target_command_pattern": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                    "PROJECT --file FILE"
                ),
            }
        ),
        "workflow-instance.edit": CommandSchemaOverride(
            payload={
                "format": "yaml",
                "source_options": ["--patch PATCH", "--file FILE"],
                "patch_template_command": (
                    "dsctl template workflow-instance-patch --raw"
                ),
                "file_source_command_pattern": (
                    "dsctl workflow-instance export WORKFLOW_INSTANCE --project PROJECT"
                ),
                "target_command_patterns": [
                    (
                        "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                        "PROJECT --patch FILE"
                    ),
                    (
                        "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                        "PROJECT --file FILE"
                    ),
                ],
            }
        ),
    }


__all__ = ["GROUP_SUMMARIES", "build_schema_group"]
