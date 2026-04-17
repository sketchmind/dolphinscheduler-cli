from pathlib import Path

import pytest

from dsctl.errors import UserInputError
from dsctl.models import supported_typed_task_types
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
from dsctl.services.schema import get_schema_result
from dsctl.services.task_instance import (
    DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
)
from dsctl.services.template import parameter_syntax_index_data, task_template_metadata
from dsctl.services.workflow_instance import (
    DEFAULT_WATCH_INTERVAL_SECONDS,
    DEFAULT_WATCH_TIMEOUT_SECONDS,
)
from dsctl.upstream import (
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)

EXPECTED_TYPED_TASK_TYPES = list(supported_typed_task_types())
EXPECTED_UPSTREAM_TASK_TYPES_BY_CATEGORY = {
    category: list(task_types)
    for category, task_types in upstream_default_task_types_by_category().items()
}
EXPECTED_UPSTREAM_TASK_TYPES = list(upstream_default_task_types())
EXPECTED_TEMPLATE_TASK_TYPES = EXPECTED_UPSTREAM_TASK_TYPES
EXPECTED_GENERIC_TEMPLATE_TASK_TYPES = [
    task_type
    for task_type in EXPECTED_UPSTREAM_TASK_TYPES
    if task_type not in EXPECTED_TYPED_TASK_TYPES
]
EXPECTED_UNTEMPLATED_UPSTREAM_TASK_TYPES: list[str] = []
EXPECTED_TASK_TEMPLATE_METADATA = task_template_metadata()
EXPECTED_PARAMETER_SYNTAX = parameter_syntax_index_data()
EXPECTED_VERSION_METADATA = [
    {
        "server_version": "3.3.2",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": False,
    },
    {
        "server_version": "3.4.0",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": False,
    },
    {
        "server_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": True,
    },
]
EXPECTED_DS_CAPABILITIES = {
    "current_version": "3.4.1",
    "selected_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "tested": True,
    "supported_versions": ["3.3.2", "3.4.0", "3.4.1"],
    "versions": EXPECTED_VERSION_METADATA,
}


def test_schema_result_describes_current_stable_surface() -> None:
    result = get_schema_result()
    data = result.data

    assert isinstance(data, dict)
    assert data["schema_version"] == 1
    assert data["cli"] == {"name": "dsctl", "version": "0.1.0"}
    assert data["supported_ds_versions"] == ["3.3.2", "3.4.0", "3.4.1"]
    assert data["ds_versions"] == EXPECTED_VERSION_METADATA
    assert data["selection"] == {
        "precedence": ["flag", "context"],
        "selector_types": {
            "opaque_name": "User-provided DS resource name.",
            "name_or_code": "Name-first selector with numeric code shortcut.",
            "name_or_id": "Name-first selector with numeric id shortcut.",
            "resource_path": "DS resource fullName path.",
            "id": "Numeric runtime or schedule id.",
        },
        "name_first_resources": [
            "project",
            "env",
            "cluster",
            "datasource",
            "namespace",
            "queue",
            "worker-group",
            "task-group",
            "alert-plugin",
            "alert-group",
            "tenant",
            "user",
            "project-parameter",
            "workflow",
            "task",
        ],
        "path_first_resources": ["resource"],
        "id_first_resources": [
            "schedule",
            "workflow-instance",
            "task-instance",
            "access-token",
        ],
    }
    assert data["output"] == {
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
        ],
        "error_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
            "error",
        ],
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warning_details_aligned": True,
    }

    commands = data["commands"]
    assert isinstance(commands, list)
    assert [item["name"] for item in commands] == [
        "version",
        "context",
        "doctor",
        "schema",
        "capabilities",
        "use",
        "enum",
        "lint",
        "env",
        "cluster",
        "datasource",
        "namespace",
        "resource",
        "queue",
        "worker-group",
        "task-group",
        "alert-plugin",
        "alert-group",
        "tenant",
        "user",
        "access-token",
        "monitor",
        "audit",
        "project",
        "project-parameter",
        "project-preference",
        "project-worker-group",
        "schedule",
        "template",
        "task-type",
        "workflow",
        "workflow-instance",
        "task",
        "task-instance",
    ]
    schema_command = _find_command(commands, "schema")
    schema_options = _require_list(schema_command["options"])
    assert _find_option(schema_options, "group")["description"] == (
        "Return schema for one command group. Values come from "
        "`dsctl capabilities --summary` data.resources.groups keys or full "
        "schema data.commands[].name."
    )
    assert _find_option(schema_options, "command")["description"] == (
        "Return schema for one stable command action."
    )
    capabilities_command = _find_command(commands, "capabilities")
    capabilities_options = _require_list(capabilities_command["options"])
    assert _find_option(capabilities_options, "summary")["default"] is False
    section_choices = _find_option(capabilities_options, "section")["choices"]
    assert isinstance(section_choices, list)
    assert "runtime" in section_choices

    template_group = _find_group(commands, "template")
    params_command = _find_command(template_group["commands"], "params")
    assert params_command["action"] == "template.params"
    params_options = _require_list(params_command["options"])
    topic_option = _find_option(params_options, "topic")
    assert topic_option["choices"] == [
        "overview",
        "property",
        "built-in",
        "time",
        "context",
        "output",
        "all",
    ]
    task_command = _find_command(template_group["commands"], "task")
    task_arguments = _require_list(task_command["arguments"])
    first_task_argument = _require_dict(task_arguments[0])
    task_options = _require_list(task_command["options"])
    variant_option = _find_option(task_options, "variant")
    assert task_command["action"] == "template.task"
    assert first_task_argument["choices"] == EXPECTED_TEMPLATE_TASK_TYPES
    variant_choices = variant_option["choices"]
    assert isinstance(variant_choices, list)
    assert "resource" in variant_choices
    assert "post-json" in variant_choices

    enum_group = _find_group(commands, "enum")
    enum_command_names = [
        _require_dict(item)["name"] for item in _require_list(enum_group["commands"])
    ]
    assert enum_command_names == ["list"]
    enum_list = _find_command(enum_group["commands"], "list")
    enum_arguments = _require_list(enum_list["arguments"])
    first_enum_argument = _require_dict(enum_arguments[0])
    enum_choices = _require_list(first_enum_argument["choices"])
    assert "priority" in enum_choices
    assert "resource-type" in enum_choices

    lint_group = _find_group(commands, "lint")
    lint_command_names = [
        _require_dict(item)["name"] for item in _require_list(lint_group["commands"])
    ]
    assert lint_command_names == ["workflow"]

    task_type_group = _find_group(commands, "task-type")
    task_type_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(task_type_group["commands"])
    ]
    assert task_type_command_names == ["list"]

    env_group = _find_group(commands, "env")
    env_command_names = [
        _require_dict(item)["name"] for item in _require_list(env_group["commands"])
    ]
    assert env_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    cluster_group = _find_group(commands, "cluster")
    cluster_command_names = [
        _require_dict(item)["name"] for item in _require_list(cluster_group["commands"])
    ]
    assert cluster_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    datasource_group = _find_group(commands, "datasource")
    datasource_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(datasource_group["commands"])
    ]
    assert datasource_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "test",
    ]

    schedule_group = _find_group(commands, "schedule")
    schedule_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(schedule_group["commands"])
    ]
    assert schedule_command_names == [
        "list",
        "get",
        "preview",
        "explain",
        "create",
        "update",
        "delete",
        "online",
        "offline",
    ]

    project_parameter_group = _find_group(commands, "project-parameter")
    project_parameter_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(project_parameter_group["commands"])
    ]
    assert project_parameter_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    project_preference_group = _find_group(commands, "project-preference")
    project_preference_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(project_preference_group["commands"])
    ]
    assert project_preference_command_names == [
        "get",
        "update",
        "enable",
        "disable",
    ]

    project_worker_group_group = _find_group(commands, "project-worker-group")
    project_worker_group_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(project_worker_group_group["commands"])
    ]
    assert project_worker_group_command_names == [
        "list",
        "set",
        "clear",
    ]

    access_token_group = _find_group(commands, "access-token")
    access_token_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(access_token_group["commands"])
    ]
    assert access_token_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "generate",
    ]

    namespace_group = _find_group(commands, "namespace")
    namespace_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(namespace_group["commands"])
    ]
    assert namespace_command_names == [
        "list",
        "get",
        "available",
        "create",
        "delete",
    ]

    resource_group = _find_group(commands, "resource")
    resource_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(resource_group["commands"])
    ]
    assert resource_command_names == [
        "list",
        "view",
        "upload",
        "create",
        "mkdir",
        "download",
        "delete",
    ]

    queue_group = _find_group(commands, "queue")
    queue_command_names = [
        _require_dict(item)["name"] for item in _require_list(queue_group["commands"])
    ]
    assert queue_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    worker_group_group = _find_group(commands, "worker-group")
    worker_group_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(worker_group_group["commands"])
    ]
    assert worker_group_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    task_group_group = _find_group(commands, "task-group")
    task_group_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(task_group_group["commands"])
    ]
    assert task_group_command_names == [
        "list",
        "get",
        "create",
        "update",
        "close",
        "start",
        "queue",
    ]
    task_group_queue_group = _find_group(task_group_group["commands"], "queue")
    task_group_queue_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(task_group_queue_group["commands"])
    ]
    assert task_group_queue_command_names == [
        "list",
        "force-start",
        "set-priority",
    ]

    alert_plugin_group = _find_group(commands, "alert-plugin")
    alert_plugin_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(alert_plugin_group["commands"])
    ]
    assert alert_plugin_command_names == [
        "list",
        "get",
        "schema",
        "create",
        "update",
        "delete",
        "test",
    ]

    alert_group_group = _find_group(commands, "alert-group")
    alert_group_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(alert_group_group["commands"])
    ]
    assert alert_group_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    tenant_group = _find_group(commands, "tenant")
    tenant_command_names = [
        _require_dict(item)["name"] for item in _require_list(tenant_group["commands"])
    ]
    assert tenant_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]

    user_group = _find_group(commands, "user")
    user_command_names = [
        _require_dict(item)["name"] for item in _require_list(user_group["commands"])
    ]
    assert user_command_names == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "grant",
        "revoke",
    ]
    user_grant_group = _find_group(_require_list(user_group["commands"]), "grant")
    assert [
        _require_dict(item)["name"]
        for item in _require_list(user_grant_group["commands"])
    ] == ["project", "datasource", "namespace"]
    user_revoke_group = _find_group(_require_list(user_group["commands"]), "revoke")
    assert [
        _require_dict(item)["name"]
        for item in _require_list(user_revoke_group["commands"])
    ] == ["project", "datasource", "namespace"]

    monitor_group = _find_group(commands, "monitor")
    monitor_commands = _require_list(monitor_group["commands"])
    assert [_require_dict(item)["name"] for item in monitor_commands] == [
        "health",
        "server",
        "database",
    ]
    server_command = _find_command(monitor_commands, "server")
    server_arguments = _require_list(server_command["arguments"])
    assert _require_dict(server_arguments[0])["choices"] == [
        "master",
        "worker",
        "alert-server",
    ]

    audit_group = _find_group(commands, "audit")
    audit_command_names = [
        _require_dict(item)["name"] for item in _require_list(audit_group["commands"])
    ]
    assert audit_command_names == [
        "list",
        "model-types",
        "operation-types",
    ]

    workflow_group = _find_group(commands, "workflow")
    workflow_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(workflow_group["commands"])
    ]
    assert workflow_command_names == [
        "list",
        "get",
        "describe",
        "digest",
        "create",
        "edit",
        "online",
        "offline",
        "run",
        "run-task",
        "backfill",
        "delete",
        "lineage",
    ]
    workflow_lineage_group = _find_group(workflow_group["commands"], "lineage")
    workflow_lineage_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(workflow_lineage_group["commands"])
    ]
    assert workflow_lineage_command_names == [
        "list",
        "get",
        "dependent-tasks",
    ]
    workflow_edit = _find_command(workflow_group["commands"], "edit")
    workflow_edit_options = _require_list(workflow_edit["options"])
    assert _find_option(workflow_edit_options, "patch")["required"] is True
    assert _find_option(workflow_edit_options, "dry-run")["default"] is False
    workflow_delete = _find_command(workflow_group["commands"], "delete")
    workflow_delete_options = _require_list(workflow_delete["options"])
    assert _find_option(workflow_delete_options, "force")["default"] is False
    workflow_run_task = _find_command(workflow_group["commands"], "run-task")
    workflow_run_task_options = _require_list(workflow_run_task["options"])
    assert _find_option(workflow_run_task_options, "task")["required"] is True
    assert _find_option(workflow_run_task_options, "scope")["default"] == "self"
    assert _find_option(workflow_run_task_options, "scope")["choices"] == [
        "self",
        "pre",
        "post",
    ]
    assert _find_option(workflow_run_task_options, "dry-run")["default"] is False
    assert (
        _find_option(workflow_run_task_options, "execution-dry-run")["default"] is False
    )
    assert _find_option(workflow_run_task_options, "param")["multiple"] is True
    workflow_backfill = _find_command(workflow_group["commands"], "backfill")
    workflow_backfill_options = _require_list(workflow_backfill["options"])
    assert _find_option(workflow_backfill_options, "scope")["default"] == "self"
    assert _find_option(workflow_backfill_options, "run-mode")["default"] == "serial"
    assert (
        _find_option(workflow_backfill_options, "expected-parallelism-number")[
            "default"
        ]
        == 2
    )
    assert _find_option(workflow_backfill_options, "date")["multiple"] is True

    workflow_instance_group = _find_group(commands, "workflow-instance")
    workflow_instance_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(workflow_instance_group["commands"])
    ]
    assert workflow_instance_command_names == [
        "list",
        "get",
        "parent",
        "digest",
        "update",
        "watch",
        "stop",
        "rerun",
        "recover-failed",
        "execute-task",
    ]
    workflow_instance_update = _find_command(
        workflow_instance_group["commands"],
        "update",
    )
    workflow_instance_update_options = _require_list(
        workflow_instance_update["options"]
    )
    assert _find_option(workflow_instance_update_options, "patch")["required"] is True
    assert (
        _find_option(workflow_instance_update_options, "sync-definition")["default"]
        is False
    )
    workflow_instance_execute_task = _find_command(
        workflow_instance_group["commands"],
        "execute-task",
    )
    workflow_instance_execute_task_options = _require_list(
        workflow_instance_execute_task["options"]
    )
    assert _find_option(workflow_instance_execute_task_options, "scope")["choices"] == [
        "self",
        "pre",
        "post",
    ]

    workflow_group = _find_group(commands, "workflow")
    workflow_create = _find_command(workflow_group["commands"], "create")
    workflow_create_options = _require_list(workflow_create["options"])
    workflow_create_file_description = _require_str(
        _find_option(workflow_create_options, "file")["description"]
    )
    assert "template workflow" in workflow_create_file_description

    workflow_edit = _find_command(workflow_group["commands"], "edit")
    workflow_edit_options = _require_list(workflow_edit["options"])
    workflow_edit_patch_description = _require_str(
        _find_option(workflow_edit_options, "patch")["description"]
    )
    assert "--dry-run" in workflow_edit_patch_description

    task_group = _find_group(commands, "task")
    task_update = _find_command(task_group["commands"], "update")
    task_update_options = _require_list(task_update["options"])
    task_update_set_description = _require_str(
        _find_option(task_update_options, "set")["description"]
    )
    assert "supported keys and examples" in task_update_set_description

    task_instance_group = _find_group(commands, "task-instance")
    task_instance_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(task_instance_group["commands"])
    ]
    assert task_instance_command_names == [
        "list",
        "get",
        "watch",
        "sub-workflow",
        "log",
        "force-success",
        "savepoint",
        "stop",
    ]

    capabilities = data["capabilities"]
    assert capabilities == {
        "ds": EXPECTED_DS_CAPABILITIES,
        "output": {
            "standard_envelope": True,
            "resolved_metadata": True,
            "warnings": True,
            "warning_details_alignment": True,
            "structured_errors": True,
        },
        "errors": {
            "structured": True,
            "suggestion": True,
            "source": True,
            "source_kind": "remote",
            "source_system": "dolphinscheduler",
            "source_layers": ["result", "http"],
        },
        "self_description": {
            "schema": True,
            "template": True,
            "capabilities": True,
            "command_invocation_source": "schema",
            "capabilities_scope": "feature_discovery",
        },
        "templates": {
            "workflow": {"with_schedule_option": True},
            "parameters": EXPECTED_PARAMETER_SYNTAX,
            "task": {
                "supported_types": EXPECTED_TEMPLATE_TASK_TYPES,
                "typed_types": EXPECTED_TYPED_TASK_TYPES,
                "generic_types": EXPECTED_GENERIC_TEMPLATE_TASK_TYPES,
                "templates_by_type": EXPECTED_TASK_TEMPLATE_METADATA,
            },
        },
        "authoring": {
            "workflow_yaml_create": True,
            "workflow_yaml_export": True,
            "workflow_yaml_lint": True,
            "workflow_digest": True,
            "workflow_schedule_block": True,
            "workflow_dry_run": True,
            "typed_task_specs": EXPECTED_TYPED_TASK_TYPES,
            "generic_task_template_types": EXPECTED_GENERIC_TEMPLATE_TASK_TYPES,
            "upstream_default_task_types": EXPECTED_UPSTREAM_TASK_TYPES,
            "upstream_default_task_types_by_category": (
                EXPECTED_UPSTREAM_TASK_TYPES_BY_CATEGORY
            ),
            "untemplated_upstream_task_types": EXPECTED_UNTEMPLATED_UPSTREAM_TASK_TYPES,
        },
        "schedule": {
            "preview": True,
            "explain": True,
            "risk_confirmation": True,
        },
        "monitor": {
            "health": True,
            "database": True,
            "server_types": ["master", "worker", "alert-server"],
        },
        "enums": {
            "discovery": True,
            "names": capabilities["enums"]["names"],
        },
        "runtime": {
            "audit": True,
            "workflow-instance": True,
            "task-instance": True,
        },
    }


def test_schema_result_honors_env_file_ds_version(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text("DS_VERSION=3.3.2\n", encoding="utf-8")

    result = get_schema_result(env_file=str(env_file))
    data = result.data

    assert isinstance(data, dict)
    capabilities = data["capabilities"]
    assert isinstance(capabilities, dict)
    ds_capabilities = capabilities["ds"]
    assert isinstance(ds_capabilities, dict)
    assert ds_capabilities["selected_version"] == "3.3.2"
    assert ds_capabilities["current_version"] == "3.3.2"
    assert ds_capabilities["tested"] is False


def test_schema_result_can_return_one_group() -> None:
    result = get_schema_result(group="task-instance")
    data = result.data

    assert isinstance(data, dict)
    assert "capabilities" not in data
    assert result.resolved == {
        "schema": {
            "view": "group",
            "group": "task-instance",
        }
    }
    commands = _require_list(data["commands"])
    assert len(commands) == 1
    task_instance_group = _require_dict(commands[0])
    assert task_instance_group["kind"] == "group"
    assert task_instance_group["name"] == "task-instance"
    task_instance_list = _find_command(task_instance_group["commands"], "list")
    assert task_instance_list["action"] == "task-instance.list"
    task_instance_options = _require_list(task_instance_list["options"])
    assert "workflow" not in {
        _require_dict(item)["name"] for item in task_instance_options
    }


def test_schema_result_can_return_one_command() -> None:
    result = get_schema_result(command_action="task-instance.list")
    data = result.data

    assert isinstance(data, dict)
    assert "capabilities" not in data
    assert result.resolved == {
        "schema": {
            "view": "command",
            "command": "task-instance.list",
        }
    }
    commands = _require_list(data["commands"])
    assert len(commands) == 1
    task_instance_group = _require_dict(commands[0])
    task_instance_commands = _require_list(task_instance_group["commands"])
    assert len(task_instance_commands) == 1
    task_instance_list = _require_dict(task_instance_commands[0])
    assert task_instance_list["action"] == "task-instance.list"


def test_schema_result_can_return_one_top_level_command() -> None:
    result = get_schema_result(command_action="version")
    data = result.data

    assert isinstance(data, dict)
    commands = _require_list(data["commands"])
    assert len(commands) == 1
    version_command = _require_dict(commands[0])
    assert version_command["kind"] == "command"
    assert version_command["action"] == "version"


def test_schema_result_can_return_group_action_command() -> None:
    result = get_schema_result(command_action="use.clear")
    data = result.data

    assert isinstance(data, dict)
    commands = _require_list(data["commands"])
    assert len(commands) == 1
    use_group = _require_dict(commands[0])
    assert use_group["name"] == "use"
    assert _require_dict(use_group["group_action"])["action"] == "use.clear"
    assert _require_list(use_group["commands"]) == []


def test_schema_result_rejects_conflicting_scope_options() -> None:
    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_schema_result(group="workflow", command_action="workflow.run")


def test_schema_result_rejects_unknown_group() -> None:
    with pytest.raises(UserInputError, match="Unknown schema group") as exc_info:
        get_schema_result(group="missing")

    assert exc_info.value.details["group"] == "missing"
    available_groups = exc_info.value.details["available_groups"]
    assert isinstance(available_groups, list)
    assert "task-instance" in available_groups


def test_schema_result_rejects_unknown_command() -> None:
    with pytest.raises(UserInputError, match="Unknown schema command") as exc_info:
        get_schema_result(command_action="missing.command")

    assert exc_info.value.details["command"] == "missing.command"
    available_commands = exc_info.value.details["available_commands"]
    assert isinstance(available_commands, list)
    assert "task-instance.list" in available_commands


def test_schema_result_describes_group_level_use_clear_action() -> None:
    result = get_schema_result()
    data = result.data

    assert isinstance(data, dict)
    use_group = _find_group(data["commands"], "use")
    group_action = _require_dict(use_group["group_action"])
    group_options = _require_list(group_action["options"])

    assert group_action["action"] == "use.clear"
    assert group_options == [
        {
            "kind": "option",
            "name": "clear",
            "flag": "--clear",
            "type": "boolean",
            "required": False,
            "description": "Clear the selected context scope.",
            "default": False,
        },
        {
            "kind": "option",
            "name": "scope",
            "flag": "--scope",
            "type": "string",
            "required": False,
            "description": "Persisted context layer to update.",
            "default": "project",
            "choices": ["project", "user"],
        },
    ]
    use_command_names = [
        _require_dict(item)["name"] for item in _require_list(use_group["commands"])
    ]
    assert use_command_names == ["project", "workflow"]


def test_schema_defaults_follow_runtime_constants() -> None:
    result = get_schema_result()
    data = result.data

    assert isinstance(data, dict)
    project_group = _find_group(data["commands"], "project")
    project_list = _find_command(project_group["commands"], "list")
    project_list_options = _require_list(project_list["options"])
    project_page_size = _find_option(project_list_options, "page-size")
    assert project_page_size["default"] == DEFAULT_PAGE_SIZE

    workflow_instance_group = _find_group(data["commands"], "workflow-instance")
    watch_command = _find_command(workflow_instance_group["commands"], "watch")
    watch_options = _require_list(watch_command["options"])
    assert _find_option(watch_options, "interval-seconds")["default"] == (
        DEFAULT_WATCH_INTERVAL_SECONDS
    )
    assert _find_option(watch_options, "timeout-seconds")["default"] == (
        DEFAULT_WATCH_TIMEOUT_SECONDS
    )

    task_instance_group = _find_group(data["commands"], "task-instance")
    task_instance_watch = _find_command(task_instance_group["commands"], "watch")
    task_instance_watch_options = _require_list(task_instance_watch["options"])
    assert _find_option(task_instance_watch_options, "interval-seconds")["default"] == (
        DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS
    )
    assert _find_option(task_instance_watch_options, "timeout-seconds")["default"] == (
        DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS
    )


def test_schema_task_update_set_option_exposes_supported_keys() -> None:
    result = get_schema_result()
    data = result.data

    assert isinstance(data, dict)
    task_group = _find_group(data["commands"], "task")
    task_update = _find_command(task_group["commands"], "update")
    task_update_options = _require_list(task_update["options"])
    set_option = _find_option(task_update_options, "set")

    assert set_option["supported_keys"] == [
        "command",
        "cpu_quota",
        "delay",
        "depends_on",
        "description",
        "environment_code",
        "flag",
        "memory_max",
        "priority",
        "retry.interval",
        "retry.times",
        "task_group_id",
        "task_group_priority",
        "timeout",
        "timeout_notify_strategy",
        "worker_group",
    ]
    assert set_option["examples"] == [
        "command=python v2.py",
        "retry.times=5",
        "task_group_id=12",
        "timeout_notify_strategy=FAILED",
    ]


def test_schema_runtime_list_commands_expose_all_pages_option() -> None:
    result = get_schema_result()
    data = result.data

    assert isinstance(data, dict)
    workflow_instance_group = _find_group(data["commands"], "workflow-instance")
    workflow_instance_list = _find_command(
        workflow_instance_group["commands"],
        "list",
    )
    workflow_instance_options = _require_list(workflow_instance_list["options"])
    assert _find_option(workflow_instance_options, "all") == {
        "kind": "option",
        "name": "all",
        "flag": "--all",
        "type": "boolean",
        "required": False,
        "description": "Fetch all remaining pages up to the safety limit.",
        "default": False,
    }

    task_instance_group = _find_group(data["commands"], "task-instance")
    task_instance_list = _find_command(task_instance_group["commands"], "list")
    task_instance_options = _require_list(task_instance_list["options"])
    assert _find_option(task_instance_options, "all") == {
        "kind": "option",
        "name": "all",
        "flag": "--all",
        "type": "boolean",
        "required": False,
        "description": "Fetch all remaining pages up to the safety limit.",
        "default": False,
    }


def _find_group(commands: object, name: str) -> dict[str, object]:
    assert isinstance(commands, list)
    for item in commands:
        mapping = _require_dict(item)
        if mapping["kind"] == "group" and mapping["name"] == name:
            return mapping
    message = f"missing group {name}"
    raise AssertionError(message)


def _find_command(commands: object, name: str) -> dict[str, object]:
    assert isinstance(commands, list)
    for item in commands:
        mapping = _require_dict(item)
        if mapping["kind"] == "command" and mapping["name"] == name:
            return mapping
    message = f"missing command {name}"
    raise AssertionError(message)


def _find_option(options: object, name: str) -> dict[str, object]:
    assert isinstance(options, list)
    for item in options:
        mapping = _require_dict(item)
        if mapping["kind"] == "option" and mapping["name"] == name:
            return mapping
    message = f"missing option {name}"
    raise AssertionError(message)


def _require_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _require_str(value: object) -> str:
    assert isinstance(value, str)
    return value


def _require_list(value: object) -> list[object]:
    assert isinstance(value, list)
    return value
