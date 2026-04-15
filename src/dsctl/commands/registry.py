from __future__ import annotations

from collections.abc import Callable

import typer

from dsctl.cli_surface import (
    ACCESS_TOKEN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    AUDIT_RESOURCE,
    CLUSTER_RESOURCE,
    COMMAND_GROUPS,
    DATASOURCE_RESOURCE,
    ENUM_RESOURCE,
    ENV_RESOURCE,
    LINT_RESOURCE,
    MONITOR_RESOURCE,
    NAMESPACE_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_PREFERENCE_RESOURCE,
    PROJECT_RESOURCE,
    PROJECT_WORKER_GROUP_RESOURCE,
    QUEUE_RESOURCE,
    RESOURCE_RESOURCE,
    SCHEDULE_RESOURCE,
    TASK_GROUP_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    TASK_RESOURCE,
    TASK_TYPE_RESOURCE,
    TEMPLATE_RESOURCE,
    TENANT_RESOURCE,
    USE_RESOURCE,
    USER_RESOURCE,
    WORKER_GROUP_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.commands.access_token import register_access_token_commands
from dsctl.commands.alert_group import register_alert_group_commands
from dsctl.commands.alert_plugin import register_alert_plugin_commands
from dsctl.commands.audit import register_audit_commands
from dsctl.commands.capabilities import register_capabilities_commands
from dsctl.commands.cluster import register_cluster_commands
from dsctl.commands.datasource import register_datasource_commands
from dsctl.commands.doctor import register_doctor_commands
from dsctl.commands.enums import register_enum_commands
from dsctl.commands.env import register_env_commands
from dsctl.commands.lint import register_lint_commands
from dsctl.commands.meta import register_meta_commands
from dsctl.commands.monitor import register_monitor_commands
from dsctl.commands.namespace import register_namespace_commands
from dsctl.commands.project import register_project_commands
from dsctl.commands.project_parameter import register_project_parameter_commands
from dsctl.commands.project_preference import register_project_preference_commands
from dsctl.commands.project_worker_group import (
    register_project_worker_group_commands,
)
from dsctl.commands.queue import register_queue_commands
from dsctl.commands.resource import register_resource_commands
from dsctl.commands.schedule import register_schedule_commands
from dsctl.commands.schema import register_schema_commands
from dsctl.commands.task import register_task_commands
from dsctl.commands.task_group import register_task_group_commands
from dsctl.commands.task_instance import register_task_instance_commands
from dsctl.commands.task_type import register_task_type_commands
from dsctl.commands.template import register_template_commands
from dsctl.commands.tenant import register_tenant_commands
from dsctl.commands.use import register_use_commands
from dsctl.commands.user import register_user_commands
from dsctl.commands.worker_group import register_worker_group_commands
from dsctl.commands.workflow import register_workflow_commands
from dsctl.commands.workflow_instance import register_workflow_instance_commands

CommandRegistrar = Callable[[typer.Typer], None]

ROOT_COMMAND_REGISTRARS: tuple[CommandRegistrar, ...] = (
    register_meta_commands,
    register_doctor_commands,
    register_schema_commands,
    register_capabilities_commands,
)
GROUP_COMMAND_REGISTRARS: dict[str, CommandRegistrar] = {
    USE_RESOURCE: register_use_commands,
    ENUM_RESOURCE: register_enum_commands,
    LINT_RESOURCE: register_lint_commands,
    ENV_RESOURCE: register_env_commands,
    CLUSTER_RESOURCE: register_cluster_commands,
    DATASOURCE_RESOURCE: register_datasource_commands,
    NAMESPACE_RESOURCE: register_namespace_commands,
    RESOURCE_RESOURCE: register_resource_commands,
    QUEUE_RESOURCE: register_queue_commands,
    WORKER_GROUP_RESOURCE: register_worker_group_commands,
    TASK_GROUP_RESOURCE: register_task_group_commands,
    ALERT_PLUGIN_RESOURCE: register_alert_plugin_commands,
    ALERT_GROUP_RESOURCE: register_alert_group_commands,
    TENANT_RESOURCE: register_tenant_commands,
    USER_RESOURCE: register_user_commands,
    ACCESS_TOKEN_RESOURCE: register_access_token_commands,
    MONITOR_RESOURCE: register_monitor_commands,
    AUDIT_RESOURCE: register_audit_commands,
    PROJECT_RESOURCE: register_project_commands,
    PROJECT_PARAMETER_RESOURCE: register_project_parameter_commands,
    PROJECT_PREFERENCE_RESOURCE: register_project_preference_commands,
    PROJECT_WORKER_GROUP_RESOURCE: register_project_worker_group_commands,
    SCHEDULE_RESOURCE: register_schedule_commands,
    TEMPLATE_RESOURCE: register_template_commands,
    TASK_TYPE_RESOURCE: register_task_type_commands,
    WORKFLOW_RESOURCE: register_workflow_commands,
    WORKFLOW_INSTANCE_RESOURCE: register_workflow_instance_commands,
    TASK_RESOURCE: register_task_commands,
    TASK_INSTANCE_RESOURCE: register_task_instance_commands,
}
ORDERED_GROUP_COMMAND_REGISTRARS: tuple[CommandRegistrar, ...] = tuple(
    GROUP_COMMAND_REGISTRARS[name] for name in COMMAND_GROUPS
)


def register_all_commands(app: typer.Typer) -> None:
    """Register the full stable command surface on the root app."""
    for register_commands in (
        *ROOT_COMMAND_REGISTRARS,
        *ORDERED_GROUP_COMMAND_REGISTRARS,
    ):
        register_commands(app)
