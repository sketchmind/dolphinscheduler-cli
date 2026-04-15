from __future__ import annotations

from collections.abc import Callable

from dsctl import __version__
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
    TOP_LEVEL_COMMANDS,
    USE_RESOURCE,
    USER_RESOURCE,
    WORKER_GROUP_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._schema_groups_context import (
    project_group as _project_group,
)
from dsctl.services._schema_groups_context import (
    project_parameter_group as _project_parameter_group,
)
from dsctl.services._schema_groups_context import (
    project_preference_group as _project_preference_group,
)
from dsctl.services._schema_groups_context import (
    project_worker_group_group as _project_worker_group_group,
)
from dsctl.services._schema_groups_context import use_group as _use_group
from dsctl.services._schema_groups_design import schedule_group as _schedule_group
from dsctl.services._schema_groups_design import task_group as _task_group
from dsctl.services._schema_groups_design import (
    template_group as _template_group,
)
from dsctl.services._schema_groups_design import (
    workflow_group as _workflow_group,
)
from dsctl.services._schema_groups_governance import (
    access_token_group as _access_token_group,
)
from dsctl.services._schema_groups_governance import (
    alert_group_group as _alert_group_group,
)
from dsctl.services._schema_groups_governance import (
    alert_plugin_group as _alert_plugin_group,
)
from dsctl.services._schema_groups_governance import cluster_group as _cluster_group
from dsctl.services._schema_groups_governance import (
    datasource_group as _datasource_group,
)
from dsctl.services._schema_groups_governance import env_group as _env_group
from dsctl.services._schema_groups_governance import (
    namespace_group as _namespace_group,
)
from dsctl.services._schema_groups_governance import queue_group as _queue_group
from dsctl.services._schema_groups_governance import resource_group as _resource_group
from dsctl.services._schema_groups_governance import (
    task_group_group as _task_group_group,
)
from dsctl.services._schema_groups_governance import tenant_group as _tenant_group
from dsctl.services._schema_groups_governance import user_group as _user_group
from dsctl.services._schema_groups_governance import (
    worker_group_group as _worker_group_group,
)
from dsctl.services._schema_groups_meta import enum_group as _enum_group
from dsctl.services._schema_groups_meta import lint_group as _lint_group
from dsctl.services._schema_groups_meta import task_type_group as _task_type_group
from dsctl.services._schema_groups_runtime import (
    audit_group as _audit_group,
)
from dsctl.services._schema_groups_runtime import (
    monitor_group as _monitor_group,
)
from dsctl.services._schema_groups_runtime import (
    task_instance_group as _task_instance_group,
)
from dsctl.services._schema_groups_runtime import (
    workflow_instance_group as _workflow_instance_group,
)
from dsctl.services._schema_primitives import command as _command
from dsctl.services._schema_primitives import option as _option
from dsctl.services._surface_metadata import (
    TOP_LEVEL_COMMAND_SUMMARIES,
    confirmation_schema_data,
    error_schema_data,
    output_schema_data,
    selection_schema_data,
)
from dsctl.services.capabilities import schema_capabilities_data
from dsctl.services.template import supported_task_template_types
from dsctl.upstream import SUPPORTED_VERSIONS, supported_version_metadata

SchemaGroupBuilder = Callable[[list[str]], dict[str, object]]


def get_schema_result() -> CommandResult:
    """Return the stable machine-readable CLI schema for the current surface."""
    return CommandResult(data=require_json_object(_schema_data(), label="schema data"))


def _schema_data() -> dict[str, object]:
    task_types = list(supported_task_template_types())
    command_groups = _command_groups(task_types)
    return {
        "schema_version": 1,
        "cli": {
            "name": "dsctl",
            "version": __version__,
        },
        "supported_ds_versions": list(SUPPORTED_VERSIONS),
        "ds_versions": list(supported_version_metadata()),
        "global_options": [
            _option(
                "env-file",
                value_type="path",
                description=(
                    "Load DS_* settings from an env file before reading the process "
                    "environment."
                ),
                value_name="PATH",
            )
        ],
        "selection": selection_schema_data(),
        "output": output_schema_data(),
        "errors": error_schema_data(),
        "confirmation": confirmation_schema_data(),
        "capabilities": schema_capabilities_data(),
        "commands": [
            *(
                _command(
                    name,
                    action=name,
                    summary=TOP_LEVEL_COMMAND_SUMMARIES[name],
                )
                for name in TOP_LEVEL_COMMANDS
            ),
            *(command_groups[name] for name in COMMAND_GROUPS),
        ],
    }


def _command_groups(task_types: list[str]) -> dict[str, dict[str, object]]:
    return {
        name: builder(task_types) for name, builder in _schema_group_builders().items()
    }


def _static_group_builder(
    factory: Callable[[], dict[str, object]],
) -> SchemaGroupBuilder:
    def build(_task_types: list[str]) -> dict[str, object]:
        return factory()

    return build


def _schema_group_builders() -> dict[str, SchemaGroupBuilder]:
    return {
        USE_RESOURCE: _static_group_builder(_use_group),
        ENUM_RESOURCE: _static_group_builder(_enum_group),
        LINT_RESOURCE: _static_group_builder(_lint_group),
        TASK_TYPE_RESOURCE: _static_group_builder(_task_type_group),
        ENV_RESOURCE: _static_group_builder(_env_group),
        CLUSTER_RESOURCE: _static_group_builder(_cluster_group),
        DATASOURCE_RESOURCE: _static_group_builder(_datasource_group),
        NAMESPACE_RESOURCE: _static_group_builder(_namespace_group),
        RESOURCE_RESOURCE: _static_group_builder(_resource_group),
        QUEUE_RESOURCE: _static_group_builder(_queue_group),
        WORKER_GROUP_RESOURCE: _static_group_builder(_worker_group_group),
        TASK_GROUP_RESOURCE: _static_group_builder(_task_group_group),
        ALERT_PLUGIN_RESOURCE: _static_group_builder(_alert_plugin_group),
        ALERT_GROUP_RESOURCE: _static_group_builder(_alert_group_group),
        TENANT_RESOURCE: _static_group_builder(_tenant_group),
        USER_RESOURCE: _static_group_builder(_user_group),
        ACCESS_TOKEN_RESOURCE: _static_group_builder(_access_token_group),
        MONITOR_RESOURCE: _static_group_builder(_monitor_group),
        AUDIT_RESOURCE: _static_group_builder(_audit_group),
        PROJECT_RESOURCE: _static_group_builder(_project_group),
        PROJECT_PARAMETER_RESOURCE: _static_group_builder(_project_parameter_group),
        PROJECT_PREFERENCE_RESOURCE: _static_group_builder(_project_preference_group),
        PROJECT_WORKER_GROUP_RESOURCE: _static_group_builder(
            _project_worker_group_group
        ),
        SCHEDULE_RESOURCE: _static_group_builder(_schedule_group),
        TEMPLATE_RESOURCE: _template_group,
        WORKFLOW_RESOURCE: _static_group_builder(_workflow_group),
        WORKFLOW_INSTANCE_RESOURCE: _static_group_builder(_workflow_instance_group),
        TASK_RESOURCE: _static_group_builder(_task_group),
        TASK_INSTANCE_RESOURCE: _static_group_builder(_task_instance_group),
    }
