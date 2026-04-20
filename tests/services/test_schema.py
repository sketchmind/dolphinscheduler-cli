from pathlib import Path

import pytest

from dsctl.errors import UserInputError
from dsctl.models import supported_typed_task_types
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.pagination import DEFAULT_PAGE_SIZE
from dsctl.services.schema import get_schema_result
from dsctl.services.task_instance import (
    DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
)
from dsctl.services.template import (
    cluster_config_template_capability_data,
    parameter_syntax_index_data,
    task_template_metadata,
)
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
    assert data["cli"] == {"name": "dsctl", "version": "0.2.0"}
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
            "environment",
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
        "formats": ["json", "table", "tsv"],
        "default_format": "json",
        "format_option": "--output-format",
        "columns_option": "--columns",
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
        "data_shape_metadata": True,
        "json_column_projection": True,
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
        "environment",
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
        "Return schema for one command group. Discover values with "
        "`dsctl schema --list-groups`."
    )
    assert _find_option(schema_options, "group")["discovery_command"] == (
        "dsctl schema --list-groups"
    )
    assert _find_option(schema_options, "command")["description"] == (
        "Return schema for one stable command action. Discover values with "
        "`dsctl schema --list-commands`."
    )
    assert _find_option(schema_options, "command")["discovery_command"] == (
        "dsctl schema --list-commands"
    )
    assert _find_option(schema_options, "list-groups")["default"] is False
    assert _find_option(schema_options, "list-commands")["default"] is False
    global_options = _require_list(data["global_options"])
    assert _find_option(global_options, "output-format")["choices"] == [
        "json",
        "table",
        "tsv",
    ]
    assert _find_option(global_options, "columns")["value_name"] == "CSV"
    capabilities_command = _find_command(commands, "capabilities")
    capabilities_options = _require_list(capabilities_command["options"])
    assert _find_option(capabilities_options, "summary")["default"] is False
    section_option = _find_option(capabilities_options, "section")
    assert section_option["description"] == (
        "Return one top-level capability section. Supported: selection, output, "
        "errors, resources, planes, authoring, schedule, monitor, enums, runtime. "
        "Discover values with `dsctl schema --command capabilities`."
    )
    assert section_option["discovery_command"] == "dsctl schema --command capabilities"
    section_choices = section_option["choices"]
    assert isinstance(section_choices, list)
    assert "runtime" in section_choices

    template_group = _find_group(commands, "template")
    workflow_command = _find_command(template_group["commands"], "workflow")
    workflow_options = _require_list(workflow_command["options"])
    assert workflow_command["action"] == "template.workflow"
    assert workflow_command["payload"] == {
        "format": "yaml",
        "raw_option": "--raw",
        "template_command": "dsctl template workflow --raw",
        "target_command": "dsctl workflow create --file FILE",
    }
    assert _find_option(workflow_options, "with-schedule")["default"] is False
    assert _find_option(workflow_options, "raw")["default"] is False
    workflow_patch_command = _find_command(
        template_group["commands"],
        "workflow-patch",
    )
    assert workflow_patch_command["action"] == "template.workflow-patch"
    assert workflow_patch_command["payload"] == {
        "format": "yaml",
        "raw_option": "--raw",
        "template_command": "dsctl template workflow-patch --raw",
        "target_command": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    assert workflow_patch_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.lines",
        "default_columns": ["line_no", "line"],
        "column_discovery": "runtime_row_keys",
    }
    workflow_instance_patch_command = _find_command(
        template_group["commands"],
        "workflow-instance-patch",
    )
    assert workflow_instance_patch_command["action"] == (
        "template.workflow-instance-patch"
    )
    assert workflow_instance_patch_command["payload"] == {
        "format": "yaml",
        "raw_option": "--raw",
        "template_command": "dsctl template workflow-instance-patch --raw",
        "target_command": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --patch FILE"
        ),
    }
    assert workflow_instance_patch_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.lines",
        "default_columns": ["line_no", "line"],
        "column_discovery": "runtime_row_keys",
    }
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
    assert topic_option["discovery_command"] == "dsctl template params"
    env_template_command = _find_command(template_group["commands"], "environment")
    assert env_template_command["action"] == "template.environment"
    assert env_template_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.lines",
        "default_columns": ["line", "purpose"],
        "column_discovery": "runtime_row_keys",
    }
    cluster_template_command = _find_command(template_group["commands"], "cluster")
    assert cluster_template_command["action"] == "template.cluster"
    assert cluster_template_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.fields",
        "default_columns": ["name", "required", "value_type", "description"],
        "column_discovery": "runtime_row_keys",
    }
    task_command = _find_command(template_group["commands"], "task")
    task_arguments = _require_list(task_command["arguments"])
    first_task_argument = _require_dict(task_arguments[0])
    task_options = _require_list(task_command["options"])
    variant_option = _find_option(task_options, "variant")
    assert task_command["action"] == "template.task"
    assert first_task_argument["choices"] == EXPECTED_TEMPLATE_TASK_TYPES
    assert first_task_argument["discovery_command"] == "dsctl template task"
    variant_choices = variant_option["choices"]
    assert isinstance(variant_choices, list)
    assert "resource" in variant_choices
    assert "post-json" in variant_choices
    variant_description = _require_str(variant_option["description"])
    assert "Known variants include" in variant_description
    assert "workflow-dependency" in variant_description
    assert "dsctl task-type get TYPE" in variant_description
    assert variant_option["discovery_command"] == "dsctl task-type get TYPE"
    raw_option = _find_option(task_options, "raw")
    assert raw_option["default"] is False
    datasource_template_command = _find_command(
        template_group["commands"],
        "datasource",
    )
    datasource_template_options = _require_list(datasource_template_command["options"])
    datasource_type_option = _find_option(datasource_template_options, "type")
    assert (
        datasource_type_option["choices"]
        == datasource_template_index_data()["supported_types"]
    )
    assert datasource_type_option["discovery_command"] == "dsctl template datasource"

    enum_group = _find_group(commands, "enum")
    enum_command_names = [
        _require_dict(item)["name"] for item in _require_list(enum_group["commands"])
    ]
    assert enum_command_names == ["names", "list"]
    enum_list = _find_command(enum_group["commands"], "list")
    enum_argument = _require_dict(_require_list(enum_list["arguments"])[0])
    assert enum_argument["discovery_command"] == "dsctl enum names"
    enum_choices = _require_list(enum_argument["choices"])
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
    assert task_type_command_names == ["list", "get", "schema"]
    assert task_type_group["summary"] == (
        "Discover DS task types and local task authoring contracts."
    )
    task_type_list = _find_command(task_type_group["commands"], "list")
    assert task_type_list["summary"] == (
        "List live DS task types, categories, favourite flags, and CLI authoring "
        "coverage."
    )
    task_type_schema = _find_command(task_type_group["commands"], "schema")
    assert task_type_schema["action"] == "task-type.schema"

    env_group = _find_group(commands, "environment")
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
    env_create = _find_command(env_group["commands"], "create")
    env_create_options = _require_list(env_create["options"])
    env_create_config = _find_option(env_create_options, "config")
    env_create_config_file = _find_option(env_create_options, "config-file")
    assert env_create_config["discovery_command"] == "dsctl template environment"
    assert env_create_config["examples"] == ["export JAVA_HOME=/opt/java"]
    assert env_create_config["required"] is False
    assert env_create_config_file["discovery_command"] == "dsctl template environment"
    env_update = _find_command(env_group["commands"], "update")
    env_update_options = _require_list(env_update["options"])
    assert (
        _find_option(env_update_options, "config-file")["discovery_command"]
        == "dsctl template environment"
    )

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
    cluster_create = _find_command(cluster_group["commands"], "create")
    cluster_create_options = _require_list(cluster_create["options"])
    cluster_create_config = _find_option(cluster_create_options, "config")
    cluster_create_config_file = _find_option(cluster_create_options, "config-file")
    assert cluster_create_config["discovery_command"] == "dsctl template cluster"
    assert cluster_create_config["required"] is False
    assert cluster_create_config_file["discovery_command"] == "dsctl template cluster"
    cluster_update = _find_command(cluster_group["commands"], "update")
    cluster_update_options = _require_list(cluster_update["options"])
    assert (
        _find_option(cluster_update_options, "config-file")["discovery_command"]
        == "dsctl template cluster"
    )

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
    datasource_create = _find_command(datasource_group["commands"], "create")
    datasource_payload = _require_dict(datasource_create["payload"])
    assert "payload_schema" not in datasource_create
    assert datasource_payload == {
        "format": "json",
        "source_option": "--file",
        "target_commands": [
            "dsctl datasource create --file FILE",
            "dsctl datasource update DATASOURCE --file FILE",
        ],
        "ds_model": "BaseDataSourceParamDTO",
        "upstream_request_shape": "DataSourceController request body String jsonStr",
        "template_command": "dsctl template datasource --type MYSQL",
        "template_command_pattern": "dsctl template datasource --type TYPE",
        "template_discovery_command": "dsctl template datasource",
        "template_json_path": "data.json",
        "template_payload_path": "data.payload",
        "type_enum": "db-type",
        "type_discovery_command": "dsctl enum list db-type",
        "rules": [
            "Create payloads must not include id; DS assigns it.",
            "Update payloads may omit id or set it to the selected datasource id.",
            "Create payloads must include the real password when the type uses one.",
            (
                "Update payloads may use the masked password ****** to preserve "
                "the stored password."
            ),
            "Use DS-native field names exactly, including userName and type.",
            "Use `dsctl datasource test DATASOURCE` after create or update.",
        ],
    }
    datasource_update = _find_command(datasource_group["commands"], "update")
    assert _require_dict(datasource_update["payload"])["template_command"] == (
        "dsctl template datasource --type MYSQL"
    )

    project_group = _find_group(commands, "project")
    project_get = _find_command(project_group["commands"], "get")
    project_get_args = _require_list(project_get["arguments"])
    assert (
        _require_dict(project_get_args[0])["discovery_command"] == "dsctl project list"
    )

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
    schedule_list = _find_command(schedule_group["commands"], "list")
    schedule_list_options = _require_list(schedule_list["options"])
    assert (
        _find_option(schedule_list_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(schedule_list_options, "workflow")["discovery_command"]
        == "dsctl workflow list"
    )
    schedule_get = _find_command(schedule_group["commands"], "get")
    schedule_get_args = _require_list(schedule_get["arguments"])
    assert (
        _require_dict(schedule_get_args[0])["discovery_command"]
        == "dsctl schedule list"
    )
    schedule_create = _find_command(schedule_group["commands"], "create")
    schedule_create_options = _require_list(schedule_create["options"])
    assert _find_option(schedule_create_options, "failure-strategy")["choices"] == [
        "CONTINUE",
        "END",
    ]
    assert (
        _find_option(schedule_create_options, "warning-group-id")["discovery_command"]
        == "dsctl alert-group list"
    )
    assert (
        _find_option(schedule_create_options, "worker-group")["discovery_command"]
        == "dsctl worker-group list"
    )
    assert (
        _find_option(schedule_create_options, "tenant-code")["discovery_command"]
        == "dsctl tenant list"
    )
    assert (
        _find_option(schedule_create_options, "environment-code")["discovery_command"]
        == "dsctl environment list"
    )

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
    project_parameter_list = _find_command(
        project_parameter_group["commands"],
        "list",
    )
    project_parameter_list_options = _require_list(project_parameter_list["options"])
    assert (
        _find_option(project_parameter_list_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(project_parameter_list_options, "data-type")["discovery_command"]
        == "dsctl enum list data-type"
    )
    project_parameter_get = _find_command(project_parameter_group["commands"], "get")
    project_parameter_get_args = _require_list(project_parameter_get["arguments"])
    assert (
        _require_dict(project_parameter_get_args[0])["discovery_command"]
        == "dsctl project-parameter list"
    )

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
    project_preference_get = _find_command(project_preference_group["commands"], "get")
    project_preference_get_options = _require_list(project_preference_get["options"])
    assert (
        _find_option(project_preference_get_options, "project")["discovery_command"]
        == "dsctl project list"
    )

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
    project_worker_group_set = _find_command(
        project_worker_group_group["commands"],
        "set",
    )
    project_worker_group_set_options = _require_list(
        project_worker_group_set["options"]
    )
    assert (
        _find_option(project_worker_group_set_options, "worker-group")[
            "discovery_command"
        ]
        == "dsctl worker-group list"
    )

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
    access_token_get = _find_command(access_token_group["commands"], "get")
    access_token_get_args = _require_list(access_token_get["arguments"])
    assert (
        _require_dict(access_token_get_args[0])["discovery_command"]
        == "dsctl access-token list"
    )
    access_token_create = _find_command(access_token_group["commands"], "create")
    access_token_create_options = _require_list(access_token_create["options"])
    assert (
        _find_option(access_token_create_options, "user")["discovery_command"]
        == "dsctl user list"
    )

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
    namespace_create = _find_command(namespace_group["commands"], "create")
    namespace_create_options = _require_list(namespace_create["options"])
    assert (
        _find_option(namespace_create_options, "cluster-code")["discovery_command"]
        == "dsctl cluster list"
    )

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
    resource_view = _find_command(resource_group["commands"], "view")
    resource_view_args = _require_list(resource_view["arguments"])
    resource_view_arg = _require_dict(resource_view_args[0])
    assert resource_view_arg["discovery_command"] == "dsctl resource list --dir DIR"
    resource_list = _find_command(resource_group["commands"], "list")
    resource_list_options = _require_list(resource_list["options"])
    assert (
        _find_option(resource_list_options, "dir")["discovery_command"]
        == "dsctl resource list"
    )

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
    queue_get = _find_command(queue_group["commands"], "get")
    queue_get_args = _require_list(queue_get["arguments"])
    assert _require_dict(queue_get_args[0])["discovery_command"] == "dsctl queue list"

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
    worker_group_create = _find_command(worker_group_group["commands"], "create")
    worker_group_create_options = _require_list(worker_group_create["options"])
    assert (
        _find_option(worker_group_create_options, "addr")["discovery_command"]
        == "dsctl monitor server worker"
    )
    worker_group_get = _find_command(worker_group_group["commands"], "get")
    worker_group_get_args = _require_list(worker_group_get["arguments"])
    assert (
        _require_dict(worker_group_get_args[0])["discovery_command"]
        == "dsctl worker-group list"
    )

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
    task_group_list = _find_command(task_group_group["commands"], "list")
    task_group_list_options = _require_list(task_group_list["options"])
    assert _find_option(task_group_list_options, "status")["choices"] == [
        "open",
        "closed",
        "1",
        "0",
    ]
    task_group_get = _find_command(task_group_group["commands"], "get")
    task_group_get_args = _require_list(task_group_get["arguments"])
    assert (
        _require_dict(task_group_get_args[0])["discovery_command"]
        == "dsctl task-group list"
    )
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
    task_group_queue_list = _find_command(task_group_queue_group["commands"], "list")
    task_group_queue_list_options = _require_list(task_group_queue_list["options"])
    assert _find_option(task_group_queue_list_options, "status")["choices"] == [
        "WAIT_QUEUE",
        "ACQUIRE_SUCCESS",
        "RELEASE",
        "-1",
        "1",
        "2",
    ]
    task_group_queue_force_start = _find_command(
        task_group_queue_group["commands"],
        "force-start",
    )
    force_start_args = _require_list(task_group_queue_force_start["arguments"])
    assert (
        _require_dict(force_start_args[0])["discovery_command"]
        == "dsctl task-group queue list TASK_GROUP"
    )

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
        "definition",
    ]
    alert_plugin_create = _find_command(alert_plugin_group["commands"], "create")
    alert_plugin_create_options = _require_list(alert_plugin_create["options"])
    assert (
        _find_option(alert_plugin_create_options, "plugin")["discovery_command"]
        == "dsctl alert-plugin definition list"
    )
    assert (
        _find_option(alert_plugin_create_options, "param")["discovery_command"]
        == "dsctl alert-plugin schema PLUGIN"
    )
    alert_plugin_get = _find_command(alert_plugin_group["commands"], "get")
    alert_plugin_get_args = _require_list(alert_plugin_get["arguments"])
    assert (
        _require_dict(alert_plugin_get_args[0])["discovery_command"]
        == "dsctl alert-plugin list"
    )
    alert_plugin_definition_group = _find_group(
        alert_plugin_group["commands"],
        "definition",
    )
    alert_plugin_definition_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(alert_plugin_definition_group["commands"])
    ]
    assert alert_plugin_definition_command_names == ["list"]

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
    alert_group_get = _find_command(alert_group_group["commands"], "get")
    alert_group_get_args = _require_list(alert_group_get["arguments"])
    assert (
        _require_dict(alert_group_get_args[0])["discovery_command"]
        == "dsctl alert-group list"
    )
    alert_group_create = _find_command(alert_group_group["commands"], "create")
    alert_group_create_options = _require_list(alert_group_create["options"])
    assert (
        _find_option(alert_group_create_options, "instance-id")["discovery_command"]
        == "dsctl alert-plugin list"
    )

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
    tenant_get = _find_command(tenant_group["commands"], "get")
    tenant_get_args = _require_list(tenant_get["arguments"])
    assert _require_dict(tenant_get_args[0])["discovery_command"] == "dsctl tenant list"
    tenant_create = _find_command(tenant_group["commands"], "create")
    tenant_create_options = _require_list(tenant_create["options"])
    assert (
        _find_option(tenant_create_options, "queue")["discovery_command"]
        == "dsctl queue list"
    )

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
    user_get = _find_command(user_group["commands"], "get")
    user_get_args = _require_list(user_get["arguments"])
    assert _require_dict(user_get_args[0])["discovery_command"] == "dsctl user list"
    user_create = _find_command(user_group["commands"], "create")
    user_create_options = _require_list(user_create["options"])
    assert (
        _find_option(user_create_options, "tenant")["discovery_command"]
        == "dsctl tenant list"
    )
    assert (
        _find_option(user_create_options, "queue")["discovery_command"]
        == "dsctl queue list"
    )
    user_grant_group = _find_group(_require_list(user_group["commands"]), "grant")
    assert [
        _require_dict(item)["name"]
        for item in _require_list(user_grant_group["commands"])
    ] == ["project", "datasource", "namespace"]
    user_grant_project = _find_command(user_grant_group["commands"], "project")
    user_grant_project_args = _require_list(user_grant_project["arguments"])
    assert (
        _require_dict(user_grant_project_args[1])["discovery_command"]
        == "dsctl project list"
    )
    user_grant_datasource = _find_command(user_grant_group["commands"], "datasource")
    user_grant_datasource_options = _require_list(user_grant_datasource["options"])
    assert (
        _find_option(user_grant_datasource_options, "datasource")["discovery_command"]
        == "dsctl datasource list"
    )
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
    audit_list = _find_command(audit_group["commands"], "list")
    audit_list_options = _require_list(audit_list["options"])
    assert (
        _find_option(audit_list_options, "model-type")["discovery_command"]
        == "dsctl audit model-types"
    )
    assert (
        _find_option(audit_list_options, "operation-type")["discovery_command"]
        == "dsctl audit operation-types"
    )

    workflow_group = _find_group(commands, "workflow")
    workflow_command_names = [
        _require_dict(item)["name"]
        for item in _require_list(workflow_group["commands"])
    ]
    assert workflow_command_names == [
        "list",
        "get",
        "export",
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
    workflow_edit_args = _require_list(workflow_edit["arguments"])
    workflow_edit_arg_description = _require_str(
        _require_dict(workflow_edit_args[0])["description"]
    )
    assert "Required with --file" in workflow_edit_arg_description
    assert workflow_edit["payload"] == {
        "format": "yaml",
        "source_options": ["--patch PATH", "--file PATH"],
        "patch_template_command": "dsctl template workflow-patch --raw",
        "file_source_command": "dsctl workflow export WORKFLOW",
        "file_template_command": "dsctl template workflow --raw",
        "target_commands": [
            "dsctl workflow edit WORKFLOW --patch FILE",
            "dsctl workflow edit WORKFLOW --file FILE",
        ],
    }
    workflow_edit_options = _require_list(workflow_edit["options"])
    assert _find_option(workflow_edit_options, "patch")["required"] is False
    assert _find_option(workflow_edit_options, "file")["required"] is False
    assert _find_option(workflow_edit_options, "dry-run")["default"] is False
    assert _find_option(workflow_edit_options, "confirm-risk")["type"] == "string"
    workflow_get = _find_command(workflow_group["commands"], "get")
    workflow_get_args = _require_list(workflow_get["arguments"])
    assert (
        _require_dict(workflow_get_args[0])["discovery_command"]
        == "dsctl workflow list"
    )
    workflow_get_options = _require_list(workflow_get["options"])
    assert [_require_dict(option)["name"] for option in workflow_get_options] == [
        "project"
    ]
    workflow_export = _find_command(workflow_group["commands"], "export")
    workflow_export_args = _require_list(workflow_export["arguments"])
    assert (
        _require_dict(workflow_export_args[0])["discovery_command"]
        == "dsctl workflow list"
    )
    workflow_export_options = _require_list(workflow_export["options"])
    assert [_require_dict(option)["name"] for option in workflow_export_options] == [
        "project"
    ]
    assert workflow_export["payload"] == {
        "format": "yaml",
        "output": "raw_document",
        "target_command": "dsctl workflow edit WORKFLOW --file FILE",
    }
    workflow_delete = _find_command(workflow_group["commands"], "delete")
    workflow_delete_options = _require_list(workflow_delete["options"])
    assert _find_option(workflow_delete_options, "force")["default"] is False
    workflow_run = _find_command(workflow_group["commands"], "run")
    workflow_run_options = _require_list(workflow_run["options"])
    assert (
        _find_option(workflow_run_options, "worker-group")["discovery_command"]
        == "dsctl worker-group list"
    )
    assert (
        _find_option(workflow_run_options, "tenant")["discovery_command"]
        == "dsctl tenant list"
    )
    assert (
        _find_option(workflow_run_options, "warning-group-id")["discovery_command"]
        == "dsctl alert-group list"
    )
    assert (
        _find_option(workflow_run_options, "environment-code")["discovery_command"]
        == "dsctl environment list"
    )
    workflow_run_task = _find_command(workflow_group["commands"], "run-task")
    workflow_run_task_options = _require_list(workflow_run_task["options"])
    assert _find_option(workflow_run_task_options, "task")["required"] is True
    assert (
        _find_option(workflow_run_task_options, "task")["discovery_command"]
        == "dsctl task list"
    )
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
    assert (
        _find_option(workflow_backfill_options, "task")["discovery_command"]
        == "dsctl task list"
    )
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
        "export",
        "parent",
        "digest",
        "edit",
        "watch",
        "stop",
        "rerun",
        "recover-failed",
        "execute-task",
    ]
    workflow_instance_list = _find_command(
        workflow_instance_group["commands"],
        "list",
    )
    workflow_instance_list_options = _require_list(workflow_instance_list["options"])
    assert (
        _find_option(workflow_instance_list_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(workflow_instance_list_options, "workflow")["discovery_command"]
        == "dsctl workflow list"
    )
    assert (
        _find_option(workflow_instance_list_options, "state")["discovery_command"]
        == "dsctl enum list workflow-execution-status"
    )
    workflow_instance_get = _find_command(workflow_instance_group["commands"], "get")
    workflow_instance_get_args = _require_list(workflow_instance_get["arguments"])
    workflow_instance_get_options = _require_list(workflow_instance_get["options"])
    assert (
        _require_dict(workflow_instance_get_args[0])["discovery_command"]
        == "dsctl workflow-instance list"
    )
    assert workflow_instance_get_options == []
    workflow_instance_export = _find_command(
        workflow_instance_group["commands"],
        "export",
    )
    workflow_instance_export_args = _require_list(workflow_instance_export["arguments"])
    assert (
        _require_dict(workflow_instance_export_args[0])["discovery_command"]
        == "dsctl workflow-instance list"
    )
    assert workflow_instance_export["payload"] == {
        "format": "yaml",
        "output": "raw_document",
        "target_command": "dsctl workflow-instance edit ID --file FILE",
    }
    workflow_instance_edit = _find_command(
        workflow_instance_group["commands"],
        "edit",
    )
    workflow_instance_edit_options = _require_list(workflow_instance_edit["options"])
    assert _find_option(workflow_instance_edit_options, "patch")["required"] is False
    assert _find_option(workflow_instance_edit_options, "file")["type"] == "path"
    assert (
        _find_option(workflow_instance_edit_options, "sync-definition")["default"]
        is False
    )
    assert _find_option(workflow_instance_edit_options, "confirm-risk")["type"] == (
        "string"
    )
    workflow_instance_execute_task = _find_command(
        workflow_instance_group["commands"],
        "execute-task",
    )
    workflow_instance_execute_task_options = _require_list(
        workflow_instance_execute_task["options"]
    )
    assert (
        _find_option(workflow_instance_execute_task_options, "task")[
            "discovery_command"
        ]
        == "dsctl task-instance list --workflow-instance WORKFLOW_INSTANCE"
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
    workflow_edit_file_description = _require_str(
        _find_option(workflow_edit_options, "file")["description"]
    )
    assert "full workflow YAML" in workflow_edit_file_description
    assert "do not infer renames" in workflow_edit_file_description

    task_group = _find_group(commands, "task")
    task_update = _find_command(task_group["commands"], "update")
    task_update_options = _require_list(task_update["options"])
    task_update_set_description = _require_str(
        _find_option(task_update_options, "set")["description"]
    )
    assert "single task" in task_update_set_description
    assert "all supported keys" in task_update_set_description
    assert task_update["payload"] == {
        "scope": "workflow_definition",
        "resource_scope": "single_existing_task",
        "input_mode": "inline_set",
        "inspect_command": "dsctl task get TASK --workflow WORKFLOW",
        "supported_keys_command": "dsctl schema --command task.update",
        "target_command": "dsctl task update TASK --set KEY=VALUE",
        "use_workflow_edit_for": [
            "create_task",
            "delete_task",
            "rename_task",
            "task_type_change",
            "multi_task_dag_edit",
        ],
        "use_workflow_instance_edit_for": [
            "finished_instance_repair",
        ],
    }

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
    task_instance_list = _find_command(task_instance_group["commands"], "list")
    task_instance_list_options = _require_list(task_instance_list["options"])
    assert (
        _find_option(task_instance_list_options, "workflow-instance")[
            "discovery_command"
        ]
        == "dsctl workflow-instance list"
    )
    assert (
        _find_option(task_instance_list_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(task_instance_list_options, "task-code")["discovery_command"]
        == "dsctl task list"
    )
    assert (
        _find_option(task_instance_list_options, "state")["discovery_command"]
        == "dsctl enum list task-execution-status"
    )
    execute_type_option = _find_option(task_instance_list_options, "execute-type")
    execute_type_description = _require_str(execute_type_option["description"])
    assert "BATCH or STREAM" in execute_type_description
    assert (
        execute_type_option["discovery_command"] == "dsctl enum list task-execute-type"
    )
    task_instance_get = _find_command(task_instance_group["commands"], "get")
    task_instance_get_args = _require_list(task_instance_get["arguments"])
    task_instance_get_options = _require_list(task_instance_get["options"])
    assert (
        _require_dict(task_instance_get_args[0])["discovery_command"]
        == "dsctl task-instance list"
    )
    assert (
        _find_option(task_instance_get_options, "workflow-instance")[
            "discovery_command"
        ]
        == "dsctl workflow-instance list"
    )
    task_instance_log = _find_command(task_instance_group["commands"], "log")
    task_instance_log_options = _require_list(task_instance_log["options"])
    assert _find_option(task_instance_log_options, "tail")["default"] == 200
    assert _find_option(task_instance_log_options, "raw")["default"] is False
    assert task_instance_log["payload"] == {
        "raw_option": "--raw",
        "raw_field": "data.text",
    }

    capabilities = data["capabilities"]
    assert capabilities == {
        "ds": EXPECTED_DS_CAPABILITIES,
        "output": {
            "standard_envelope": True,
            "formats": ["json", "table", "tsv"],
            "default_format": "json",
            "data_shape_metadata": True,
            "display_columns": True,
            "json_column_projection": True,
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
            "workflow": {
                "with_schedule_option": True,
                "raw_template_command": "dsctl template workflow --raw",
                "export_command": "dsctl workflow export WORKFLOW",
            },
            "workflow_patch": {
                "raw_template_command": "dsctl template workflow-patch --raw",
                "target_command": "dsctl workflow edit WORKFLOW --patch FILE",
            },
            "workflow_instance_patch": {
                "raw_template_command": (
                    "dsctl template workflow-instance-patch --raw"
                ),
                "target_command": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE --patch FILE"
                ),
                "file_source_command": (
                    "dsctl workflow-instance export WORKFLOW_INSTANCE"
                ),
                "file_target_command": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE --file FILE"
                ),
            },
            "parameters": EXPECTED_PARAMETER_SYNTAX,
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
                "supported_types": EXPECTED_TEMPLATE_TASK_TYPES,
                "typed_types": EXPECTED_TYPED_TASK_TYPES,
                "generic_types": EXPECTED_GENERIC_TEMPLATE_TASK_TYPES,
                "templates_by_type": EXPECTED_TASK_TEMPLATE_METADATA,
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
            "workflow_yaml_edit": True,
            "workflow_digest": True,
            "workflow_schedule_block": True,
            "workflow_dry_run": True,
            "workflow_patch_template": True,
            "workflow_instance_patch_template": True,
            "workflow_instance_yaml_edit": True,
            "environment_config_template": True,
            "cluster_config_template": True,
            "datasource_payload_templates": True,
            "task_authoring_schema": True,
            "task_authoring_schema_command_pattern": "dsctl task-type schema TYPE",
            "datasource_template_types": datasource_template_index_data()[
                "supported_types"
            ],
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
    rows = _require_list(data["rows"])
    assert rows[0] == {
        "kind": "command",
        "action": "task-instance.list",
        "name": "list",
        "summary": "List task instances with project-scoped runtime filters.",
        "schema_command": "dsctl schema --command task-instance.list",
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
    assert task_instance_list["data_shape"] == {
        "kind": "page",
        "row_path": "data.totalList",
        "default_columns": [
            "id",
            "name",
            "state",
            "taskType",
            "startTime",
            "endTime",
            "duration",
            "host",
        ],
        "column_discovery": "runtime_row_keys",
    }
    rows = _require_list(data["rows"])
    assert rows[0] == {
        "kind": "command",
        "name": "task-instance.list",
        "description": "List task instances with project-scoped runtime filters.",
    }
    assert any(
        _require_dict(row).get("kind") == "data_shape"
        and _require_dict(row).get("name") == "row_path"
        and _require_dict(row).get("value") == "data.totalList"
        for row in rows
    )

    workflow_instance_result = get_schema_result(
        command_action="workflow-instance.list"
    )
    workflow_instance_data = _require_dict(workflow_instance_result.data)
    workflow_instance_group = _require_dict(
        _require_list(workflow_instance_data["commands"])[0]
    )
    workflow_instance_command = _require_dict(
        _require_list(workflow_instance_group["commands"])[0]
    )
    assert workflow_instance_command["data_shape"] == {
        "kind": "page",
        "row_path": "data.totalList",
        "default_columns": [
            "id",
            "name",
            "state",
            "scheduleTime",
            "startTime",
            "endTime",
            "duration",
            "host",
        ],
        "column_discovery": "runtime_row_keys",
    }

    workflow_instance_get_result = get_schema_result(
        command_action="workflow-instance.get"
    )
    workflow_instance_get_data = _require_dict(workflow_instance_get_result.data)
    workflow_instance_get_group = _require_dict(
        _require_list(workflow_instance_get_data["commands"])[0]
    )
    workflow_instance_get_command = _require_dict(
        _require_list(workflow_instance_get_group["commands"])[0]
    )
    assert workflow_instance_get_command["data_shape"] == {
        "kind": "object",
        "row_path": "data",
        "default_columns": [
            "id",
            "name",
            "state",
            "scheduleTime",
            "startTime",
            "endTime",
            "duration",
            "host",
        ],
        "column_discovery": "runtime_row_keys",
    }
    workflow_instance_get_options = _require_list(
        workflow_instance_get_command["options"]
    )
    assert workflow_instance_get_options == []

    datasource_list_result = get_schema_result(command_action="datasource.list")
    datasource_list_data = _require_dict(datasource_list_result.data)
    datasource_group = _require_dict(_require_list(datasource_list_data["commands"])[0])
    datasource_list_command = _require_dict(
        _require_list(datasource_group["commands"])[0]
    )
    assert datasource_list_command["data_shape"] == {
        "kind": "page",
        "row_path": "data.totalList",
        "default_columns": ["id", "name", "type", "createTime"],
        "column_discovery": "runtime_row_keys",
    }
    datasource_get_result = get_schema_result(command_action="datasource.get")
    datasource_get_data = _require_dict(datasource_get_result.data)
    datasource_get_group = _require_dict(
        _require_list(datasource_get_data["commands"])[0]
    )
    datasource_get_command = _require_dict(
        _require_list(datasource_get_group["commands"])[0]
    )
    assert datasource_get_command["data_shape"] == {
        "kind": "object",
        "row_path": "data",
        "default_columns": ["id", "name", "type", "host", "port", "database"],
        "column_discovery": "runtime_row_keys",
    }


def test_schema_result_command_rows_expose_payload_discovery() -> None:
    result = get_schema_result(command_action="datasource.create")
    data = _require_dict(result.data)
    rows = [_require_dict(row) for row in _require_list(data["rows"])]

    file_row = next(row for row in rows if row.get("name") == "file")
    assert file_row["discovery_command"] == "dsctl template datasource"
    assert file_row["description"] == (
        "Path to one DS-native datasource JSON payload file."
    )
    assert {
        "kind": "payload",
        "name": "template_discovery_command",
        "value": "dsctl template datasource",
    } in rows
    assert {
        "kind": "payload",
        "name": "target_commands",
        "value": (
            "dsctl datasource create --file FILE, "
            "dsctl datasource update DATASOURCE --file FILE"
        ),
    } in rows

    workflow_result = get_schema_result(command_action="workflow.edit")
    workflow_data = _require_dict(workflow_result.data)
    workflow_rows = [_require_dict(row) for row in _require_list(workflow_data["rows"])]
    assert {
        "kind": "payload",
        "name": "source_options",
        "value": "--patch PATH, --file PATH",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "patch_template_command",
        "value": "dsctl template workflow-patch --raw",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "file_source_command",
        "value": "dsctl workflow export WORKFLOW",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "target_commands",
        "value": (
            "dsctl workflow edit WORKFLOW --patch FILE, "
            "dsctl workflow edit WORKFLOW --file FILE"
        ),
    } in workflow_rows


def test_schema_result_can_list_group_and_command_discovery_rows() -> None:
    groups_result = get_schema_result(list_groups=True)
    groups_data = _require_list(groups_result.data)
    first_group = _require_dict(groups_data[0])

    assert groups_result.resolved == {
        "schema": {
            "view": "groups",
            "next": "dsctl schema --group GROUP",
        }
    }
    assert first_group == {
        "name": "use",
        "summary": "Set or clear persisted CLI context.",
        "command_count": 2,
        "schema_command": "dsctl schema --group use",
    }

    commands_result = get_schema_result(list_commands=True)
    commands_data = _require_list(commands_result.data)
    version_command = next(
        _require_dict(item)
        for item in commands_data
        if _require_dict(item)["action"] == "version"
    )
    datasource_create = next(
        _require_dict(item)
        for item in commands_data
        if _require_dict(item)["action"] == "datasource.create"
    )

    assert commands_result.resolved == {
        "schema": {
            "view": "commands",
            "next": "dsctl schema --command ACTION",
        }
    }
    assert version_command == {
        "action": "version",
        "group": None,
        "name": "version",
        "summary": "Return CLI and supported DolphinScheduler version metadata.",
        "schema_command": "dsctl schema --command version",
    }
    assert datasource_create == {
        "action": "datasource.create",
        "group": "datasource",
        "name": "create",
        "summary": "Create one datasource from a JSON payload file.",
        "schema_command": "dsctl schema --command datasource.create",
    }


def test_schema_result_exposes_collection_and_nested_data_shapes() -> None:
    workflow_result = get_schema_result(command_action="workflow.list")
    workflow_data = _require_dict(workflow_result.data)
    workflow_group = _require_dict(_require_list(workflow_data["commands"])[0])
    workflow_command = _require_dict(_require_list(workflow_group["commands"])[0])
    assert workflow_command["data_shape"] == {
        "kind": "collection",
        "row_path": "data",
        "default_columns": ["code", "name", "version"],
        "column_discovery": "runtime_row_keys",
    }

    task_type_result = get_schema_result(command_action="task-type.list")
    task_type_data = _require_dict(task_type_result.data)
    task_type_group = _require_dict(_require_list(task_type_data["commands"])[0])
    task_type_command = _require_dict(_require_list(task_type_group["commands"])[0])
    assert task_type_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.taskTypes",
        "default_columns": ["taskType", "taskCategory", "isCollection"],
        "column_discovery": "runtime_row_keys",
    }

    task_type_schema_result = get_schema_result(command_action="task-type.schema")
    task_type_schema_data = _require_dict(task_type_schema_result.data)
    task_type_schema_group = _require_dict(
        _require_list(task_type_schema_data["commands"])[0]
    )
    task_type_schema_command = _require_dict(
        _require_list(task_type_schema_group["commands"])[0]
    )
    assert task_type_schema_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.fields",
        "default_columns": [
            "path",
            "type",
            "required",
            "default",
            "choice_source",
            "active_when",
        ],
        "column_discovery": "runtime_row_keys",
    }

    alert_definition_result = get_schema_result(
        command_action="alert-plugin.definition.list"
    )
    alert_definition_data = _require_dict(alert_definition_result.data)
    alert_definition_group = _require_dict(
        _require_list(alert_definition_data["commands"])[0]
    )
    alert_definition_subgroup = _require_dict(
        _require_list(alert_definition_group["commands"])[0]
    )
    alert_definition_command = _require_dict(
        _require_list(alert_definition_subgroup["commands"])[0]
    )
    assert alert_definition_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.definitions",
        "default_columns": ["id", "pluginName", "pluginType"],
        "column_discovery": "runtime_row_keys",
    }

    digest_result = get_schema_result(command_action="workflow-instance.digest")
    digest_data = _require_dict(digest_result.data)
    digest_group = _require_dict(_require_list(digest_data["commands"])[0])
    digest_command = _require_dict(_require_list(digest_group["commands"])[0])
    assert digest_command["data_shape"] == {
        "kind": "object",
        "row_path": "data",
        "default_columns": [
            "taskCount",
            "progress",
            "taskStateCounts",
            "runningTasks",
            "failedTasks",
        ],
        "column_discovery": "runtime_row_keys",
    }


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

    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_schema_result(group="workflow", list_groups=True)


def test_schema_result_rejects_unknown_group() -> None:
    with pytest.raises(UserInputError, match="Unknown schema group") as exc_info:
        get_schema_result(group="missing")

    assert exc_info.value.details["group"] == "missing"
    available_groups = exc_info.value.details["available_groups"]
    assert isinstance(available_groups, list)
    assert "task-instance" in available_groups
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --list-groups` to choose a group name."
    )


def test_schema_result_rejects_unknown_command() -> None:
    with pytest.raises(UserInputError, match="Unknown schema command") as exc_info:
        get_schema_result(command_action="missing.command")

    assert exc_info.value.details["command"] == "missing.command"
    available_commands = exc_info.value.details["available_commands"]
    assert isinstance(available_commands, list)
    assert "task-instance.list" in available_commands
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --list-commands` to choose a command action."
    )


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
    use_project = _find_command(use_group["commands"], "project")
    use_project_args = _require_list(use_project["arguments"])
    assert (
        _require_dict(use_project_args[0])["discovery_command"] == "dsctl project list"
    )
    use_workflow = _find_command(use_group["commands"], "workflow")
    use_workflow_args = _require_list(use_workflow["arguments"])
    assert (
        _require_dict(use_workflow_args[0])["discovery_command"]
        == "dsctl workflow list"
    )


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

    task_update_args = _require_list(task_update["arguments"])
    assert _require_dict(task_update_args[0])["discovery_command"] == "dsctl task list"
    assert set_option["discovery_command"] == "dsctl schema --command task.update"
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
