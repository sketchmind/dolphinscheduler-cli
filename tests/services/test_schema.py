from pathlib import Path

import pytest

from dsctl import __version__
from dsctl.data_shapes import data_shape_schema_for_action
from dsctl.errors import ConfigError, UserInputError
from dsctl.models import supported_typed_task_types
from dsctl.services import capabilities as capabilities_service
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.enums import enum_capabilities_data
from dsctl.services.schema import get_schema_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.task_instance import (
    DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS,
    DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS,
)
from dsctl.services.template import (
    cluster_config_template_capability_data,
    parameter_syntax_index_data,
    supported_task_template_types,
    task_template_metadata,
)
from dsctl.services.workflow_instance._types import (
    DEFAULT_WATCH_INTERVAL_SECONDS,
    DEFAULT_WATCH_TIMEOUT_SECONDS,
)
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    supported_version_metadata,
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)
from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE

EXPECTED_TYPED_TASK_TYPES = list(supported_typed_task_types())
EXPECTED_UPSTREAM_TASK_TYPES_BY_CATEGORY = {
    category: list(task_types)
    for category, task_types in upstream_default_task_types_by_category().items()
}
EXPECTED_UPSTREAM_TASK_TYPES = list(upstream_default_task_types())
EXPECTED_TEMPLATE_TASK_TYPES = list(supported_task_template_types())
EXPECTED_GENERIC_TEMPLATE_TASK_TYPES = [
    task_type
    for task_type in EXPECTED_TEMPLATE_TASK_TYPES
    if task_type not in EXPECTED_TYPED_TASK_TYPES
]
EXPECTED_UNTEMPLATED_UPSTREAM_TASK_TYPES = ["LINKIS"]
EXPECTED_TASK_TEMPLATE_METADATA = task_template_metadata()
EXPECTED_PARAMETER_SYNTAX = parameter_syntax_index_data()
EXPECTED_VERSION_METADATA = list(supported_version_metadata())
EXPECTED_DS_CAPABILITIES = {
    "current_version": "3.4.1",
    "selected_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "tested": True,
    "supported_version_count": len(SUPPORTED_VERSIONS),
    "catalog": {
        "selected_version": "3.4.1",
        "action_count": 181,
        "availability_counts": {
            "supported": 180,
            "limited": 1,
            "unsupported": 0,
        },
        "verification_counts": {
            "static": 29,
            "contract_tested": 145,
            "live_smoke": 7,
            "live_full": 0,
        },
    },
}


def test_schema_result_describes_current_stable_surface() -> None:
    result = get_schema_result(full=True)
    data = result.data

    assert isinstance(data, dict)
    assert data["schema_version"] == 3
    assert data["view"] == "full"
    assert data["cli"] == {"name": "dsctl", "version": __version__}
    assert data["supported_ds_versions"] == list(SUPPORTED_VERSIONS)
    assert data["ds_versions"] == EXPECTED_VERSION_METADATA
    assert data["selection"] == {
        "precedence": ["flag", "context"],
        "selector_types": {
            "opaque_name": "User-provided DS resource name.",
            "name_or_code": "Name-first selector with numeric code shortcut.",
            "name_or_native_identity": (
                "Project name or native numeric identity: id on DS 1.3.9, code on "
                "newer versions."
            ),
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
        "formats": ["json", "json-compact", "table", "tsv"],
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
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
        ],
        "optional_success_fields": ["warnings", "next_actions", "action_index"],
        "error_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "error",
        ],
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
            "max_items": 3,
            "ordered": True,
            "item_fields": ["action", "command", "mutates"],
            "command_kind": "complete_shell_invocation",
            "authorization": "advisory",
            "row_output": False,
            "preserves_env_file": True,
        },
        "action_index": {
            "field": "action_index",
            "presence": "successful_applicable_json_responses_only",
            "max_indexed_targets": 100,
            "index_fields": [
                "scope",
                "target",
                "authorization",
                "eligibility",
                "groups",
                "schema_command_pattern",
                "group_command",
                "target_count",
                "indexed_target_count",
                "truncated",
            ],
            "target_fields": ["resource", "field"],
            "group_fields": [
                "targets",
                "read",
                "read_needs_input",
                "mutate",
                "mutate_needs_input",
            ],
            "all_targets_semantics": "all_returned_rows",
            "authorization": "not_evaluated",
            "eligibility": "row_facts_only",
            "row_output": False,
        },
    }

    commands = data["commands"]
    assert isinstance(commands, list)
    assert [item["name"] for item in commands] == [
        "version",
        "doctor",
        "schema",
        "capabilities",
        "context",
        "config",
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
    assert schema_command["invocation"] == "dsctl schema [OPTIONS]"
    schema_options = _require_list(schema_command["options"])
    assert _find_option(schema_options, "group")["description"] == (
        "Return one group's action index. Discover groups with `dsctl schema` "
        "or `dsctl schema --list-groups`."
    )
    assert _find_option(schema_options, "group")["discovery_command"] == (
        "dsctl schema --list-groups"
    )
    assert _find_option(schema_options, "command")["description"] == (
        "Return one complete action-local contract. Discover actions with "
        "`dsctl schema` or `dsctl schema --group GROUP`."
    )
    assert _find_option(schema_options, "command")["discovery_command"] == (
        "dsctl schema"
    )
    assert _find_option(schema_options, "list-groups")["default"] is False
    assert _find_option(schema_options, "list-commands")["default"] is False
    assert _find_option(schema_options, "full")["default"] is False
    schema_view_shapes = _require_dict(schema_command["data_shapes_by_view"])
    assert _require_dict(schema_view_shapes["index"])["row_path"] == "data.groups"
    assert _require_dict(schema_view_shapes["group"])["row_path"] == "data.actions"
    assert _require_dict(schema_view_shapes["command"])["row_path"] == ("data.command")
    assert _require_dict(schema_view_shapes["full"])["row_path"] == "data.commands"
    assert _require_dict(schema_view_shapes["full_group"])["row_path"] == "data.rows"
    assert _require_dict(schema_view_shapes["full_command"])["row_path"] == (
        "data.rows"
    )
    global_options = _require_list(data["global_options"])
    assert _find_option(global_options, "format")["choices"] == [
        "json",
        "json-compact",
        "table",
        "tsv",
    ]
    assert _find_option(global_options, "columns")["value_name"] == "FIELDS"
    assert all(_require_dict(option)["name"] != "compact" for option in global_options)
    assert all(
        _find_option(global_options, name)["placement"] == "anywhere"
        for name in ("env-file", "format", "columns")
    )
    capabilities_command = _find_command(commands, "capabilities")
    capabilities_options = _require_list(capabilities_command["options"])
    assert _find_option(capabilities_options, "summary")["default"] is False
    full_option = _find_option(capabilities_options, "full")
    assert full_option["default"] is False
    assert full_option["description"] == (
        "Return the complete expanded capability inventory."
    )
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
    action_option = _find_option(capabilities_options, "action")
    assert action_option["discovery_command"] == "dsctl schema"
    assert capabilities_command["constraints"] == [
        {
            "kind": "at_most_one_of",
            "fields": ["--summary", "--section", "--full", "--action"],
        }
    ]

    template_group = _find_group(commands, "template")
    workflow_command = _find_command(template_group["commands"], "workflow")
    workflow_options = _require_list(workflow_command["options"])
    assert workflow_command["action"] == "template.workflow"
    assert workflow_command["payload"] == {
        "format": "yaml",
        "raw_option": "--raw",
        "template_command": "dsctl template workflow --raw",
        "target_command_pattern": "dsctl workflow create --file FILE",
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
        "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    assert workflow_patch_command["data_shape"] == {
        "kind": "document",
        "row_path": "data.lines",
        "value_path": "data.yaml",
        "line_source_path": "data.yaml",
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
        "target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
            "--patch FILE"
        ),
    }
    assert workflow_instance_patch_command["data_shape"] == {
        "kind": "document",
        "row_path": "data.lines",
        "value_path": "data.yaml",
        "line_source_path": "data.yaml",
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
    assert "main template" in variant_description
    assert "dsctl task-type get TYPE" in variant_description
    assert variant_option["discovery_command_pattern"] == "dsctl task-type get TYPE"
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
    assert lint_command_names == [
        "workflow",
        "workflow-patch",
        "workflow-instance-patch",
    ]

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
    task_type_schema_options = _require_list(task_type_schema["options"])
    assert [_require_dict(item)["name"] for item in task_type_schema_options] == [
        "field",
        "json-schema",
        "compile-mappings",
        "full",
    ]
    assert _find_option(task_type_schema_options, "field")[
        "discovery_command_pattern"
    ] == ("dsctl task-type schema TYPE")
    assert task_type_schema["constraints"] == [
        {
            "kind": "at_most_one_of",
            "fields": [
                "--field",
                "--json-schema",
                "--compile-mappings",
                "--full",
            ],
        }
    ]

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
        "runtime_contract_selection": "configured_cluster_ds_version",
        "template_default_ds_version": "3.4.1",
        "source_option": "--file",
        "target_command_patterns": [
            "dsctl datasource create --file FILE",
            "dsctl datasource update DATASOURCE --file FILE",
        ],
        "ds_model": "BaseDataSourceParamDTO",
        "upstream_request_shape": (
            "Exact-version DataSourceController form or request-body contract"
        ),
        "template_command": "dsctl template datasource --type MYSQL",
        "template_command_pattern": "dsctl template datasource --type TYPE",
        "template_discovery_command": "dsctl template datasource",
        "template_json_path": "data.json",
        "template_payload_path": "data.payload",
        "type_enum": "db-type",
        "type_discovery_command": "dsctl template datasource",
        "rules": [
            "Create payloads must not include id; DS assigns it.",
            "Update payloads may omit id or set it to the selected datasource id.",
            "Create payloads must include real values for every secret the type uses.",
            (
                "Update payloads may omit secrets or use ****** to preserve "
                "stored values when upstream exposes a safe preservation path."
            ),
            "Fields and datasource types are validated against the exact DS version.",
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
    assert "pass 0 to explicitly use no environment" in str(
        _find_option(schedule_create_options, "environment-code")["description"]
    )
    schedule_update = _find_command(schedule_group["commands"], "update")
    schedule_update_options = _require_list(schedule_update["options"])
    assert "pass 0 to clear the environment" in str(
        _find_option(schedule_update_options, "environment-code")["description"]
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
    assert {_require_dict(item)["name"] for item in namespace_create_options} == {
        "namespace",
        "cluster-code",
        "k8s",
        "limits-cpu",
        "limits-memory",
    }
    assert _find_option(namespace_create_options, "cluster-code")["required"] is False
    assert _find_option(namespace_create_options, "k8s")["required"] is False
    assert _find_option(namespace_create_options, "limits-cpu")["minimum"] == 0
    assert _find_option(namespace_create_options, "limits-memory")["minimum"] == 0
    namespace_delete = _find_command(namespace_group["commands"], "delete")
    assert namespace_delete["summary"] == (
        "Delete a namespace registration and, on DS 3.0-3.1, its K8s object."
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
    assert (
        resource_view_arg["discovery_command_pattern"]
        == "dsctl resource list --dir DIR"
    )
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
        _require_dict(force_start_args[0])["discovery_command_pattern"]
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
        _find_option(alert_plugin_create_options, "param")["discovery_command_pattern"]
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
    tenant_update = _find_command(tenant_group["commands"], "update")
    tenant_update_options = _require_list(tenant_update["options"])
    assert [_require_dict(item)["name"] for item in tenant_update_options] == [
        "queue",
        "description",
        "clear-description",
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
    workflow_create = _find_command(workflow_group["commands"], "create")
    workflow_create_options = _require_list(workflow_create["options"])
    assert "dsctl lint workflow FILE" in _require_str(
        _find_option(workflow_create_options, "file")["description"]
    )
    workflow_create_dry_run = _find_option(
        workflow_create_options,
        "dry-run",
    )
    assert "full DS request" in _require_str(workflow_create_dry_run["description"])
    assert "bounded DAG validation" in _require_str(
        workflow_create_dry_run["description"]
    )
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
    assert (
        "Pass WORKFLOW explicitly with either --file or --patch"
        in workflow_edit_arg_description
    )
    assert workflow_edit["payload"] == {
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
        "target_command_patterns": [
            "dsctl workflow create --file FILE",
            "dsctl workflow edit WORKFLOW --file FILE",
        ],
        "schedule_on_create": "desired_state",
        "schedule_on_edit": "read_only_snapshot",
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
        _find_option(workflow_run_task_options, "task")["discovery_command_pattern"]
        == "dsctl task list --project PROJECT --workflow WORKFLOW"
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
        _find_option(workflow_backfill_options, "task")["discovery_command_pattern"]
        == "dsctl task list --project PROJECT --workflow WORKFLOW"
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
        _require_dict(workflow_instance_get_args[0])["discovery_command_pattern"]
        == "dsctl workflow-instance list --project PROJECT"
    )
    assert (
        _find_option(workflow_instance_get_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    workflow_instance_export = _find_command(
        workflow_instance_group["commands"],
        "export",
    )
    workflow_instance_export_args = _require_list(workflow_instance_export["arguments"])
    assert (
        _require_dict(workflow_instance_export_args[0])["discovery_command_pattern"]
        == "dsctl workflow-instance list --project PROJECT"
    )
    assert workflow_instance_export["payload"] == {
        "format": "yaml",
        "output": "raw_document",
        "target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
            "PROJECT --file FILE"
        ),
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
    assert _find_option(workflow_instance_execute_task_options, "task")[
        "discovery_command_pattern"
    ] == (
        "dsctl task-instance list --project PROJECT --workflow-instance "
        "WORKFLOW_INSTANCE"
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
    assert _find_option(workflow_create_options, "file")["discovery_command"] == (
        "dsctl template workflow --raw"
    )

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
            "discovery_command_pattern"
        ]
        == "dsctl workflow-instance list --project PROJECT"
    )
    assert (
        _find_option(task_instance_list_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(task_instance_list_options, "task-code")[
            "discovery_command_pattern"
        ]
        == "dsctl task list --project PROJECT --workflow WORKFLOW"
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
        _require_dict(task_instance_get_args[0])["discovery_command_pattern"]
        == "dsctl task-instance list --project PROJECT"
    )
    assert (
        _find_option(task_instance_get_options, "workflow-instance")[
            "discovery_command_pattern"
        ]
        == "dsctl workflow-instance list --project PROJECT"
    )
    assert (
        _find_option(task_instance_get_options, "project")["discovery_command"]
        == "dsctl project list"
    )
    assert (
        _find_option(task_instance_get_options, "workflow-instance")["required"]
        is False
    )
    for action in ("watch", "savepoint", "stop"):
        command = _find_command(task_instance_group["commands"], action)
        options = _require_list(command["options"])
        assert _find_option(options, "workflow-instance")["required"] is False
    for action in ("sub-workflow", "force-success"):
        command = _find_command(task_instance_group["commands"], action)
        options = _require_list(command["options"])
        assert _find_option(options, "workflow-instance")["required"] is True
    task_instance_log = _find_command(task_instance_group["commands"], "log")
    task_instance_log_options = _require_list(task_instance_log["options"])
    assert not any(
        _require_dict(item)["name"] == "project" for item in task_instance_log_options
    )
    # Parse defaults stay unset so the service can distinguish tail mode from
    # an explicitly selected line window; effective defaults are documented.
    assert _find_option(task_instance_log_options, "tail").get("default") is None
    assert "default: 200" in _require_str(
        _find_option(task_instance_log_options, "tail")["description"]
    )
    for name in ("start-line", "limit"):
        option = _find_option(task_instance_log_options, name)
        assert option.get("default") is None
        assert option["minimum"] == 1
    assert _find_option(task_instance_log_options, "tail")["minimum"] == 1
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
            "formats": ["json", "json-compact", "table", "tsv"],
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
            "max_action_index_targets": 100,
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
            "surface_inventory_scope": "installed_cli_surface",
            "action_availability_command_pattern": (
                "dsctl capabilities --action ACTION"
            ),
        },
        "templates": {
            "workflow": {
                "with_schedule_option": True,
                "raw_template_command": "dsctl template workflow --raw",
                "export_command_pattern": "dsctl workflow export WORKFLOW",
            },
            "workflow_patch": {
                "raw_template_command": "dsctl template workflow-patch --raw",
                "target_command_pattern": ("dsctl workflow edit WORKFLOW --patch FILE"),
            },
            "workflow_instance_patch": {
                "raw_template_command": (
                    "dsctl template workflow-instance-patch --raw"
                ),
                "target_command_pattern": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE "
                    "--project PROJECT --patch FILE"
                ),
                "file_source_command_pattern": (
                    "dsctl workflow-instance export WORKFLOW_INSTANCE --project PROJECT"
                ),
                "file_target_command_pattern": (
                    "dsctl workflow-instance edit WORKFLOW_INSTANCE "
                    "--project PROJECT --file FILE"
                ),
            },
            "parameters": EXPECTED_PARAMETER_SYNTAX,
            "environment": {
                "command": "dsctl template environment",
                "source_options": [
                    "--config CONFIG",
                    "--config-file CONFIG_FILE",
                ],
                "target_command_patterns": [
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
            "installed_cli_inventory": {
                "task_authoring_schema_command_pattern": (
                    "dsctl task-type schema TYPE"
                ),
                "datasource_template_types": datasource_template_index_data()[
                    "supported_types"
                ],
                "typed_task_specs": EXPECTED_TYPED_TASK_TYPES,
                "generic_task_template_types": EXPECTED_GENERIC_TEMPLATE_TASK_TYPES,
                "upstream_default_task_types": EXPECTED_UPSTREAM_TASK_TYPES,
                "upstream_default_task_types_by_category": (
                    EXPECTED_UPSTREAM_TASK_TYPES_BY_CATEGORY
                ),
                "untemplated_upstream_task_types": (
                    EXPECTED_UNTEMPLATED_UPSTREAM_TASK_TYPES
                ),
            },
            "selected_version_availability": {
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
            },
        },
        "schedule": {
            "preview": True,
            "explain": True,
            "environment_inheritance": True,
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
    ds = _require_dict(data["ds"])
    assert ds["selected_version"] == "3.3.2"
    assert ds["contract_version"] == "3.3.2"
    assert ds["support_level"] == "experimental"
    assert ds["tested"] is False


@pytest.mark.parametrize(
    ("ds_version", "required"),
    [
        ("1.3.9", True),
        *[(f"2.0.{patch}", True) for patch in range(10)],
        ("3.0.0", False),
        ("3.4.3", False),
        (None, False),
    ],
)
def test_access_token_update_schema_requires_exact_native_date_expiry(
    tmp_path: Path, ds_version: str | None, *, required: bool
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        f"DS_VERSION={ds_version}\n"
        if ds_version
        else "DS_API_URL=http://example.test/dolphinscheduler\n",
        encoding="utf-8",
    )
    data = _require_dict(
        get_schema_result(
            env_file=str(env_file), command_action="access-token.update"
        ).data
    )
    command = _require_dict(data["command"])
    option = _find_option(_require_list(command["options"]), "expire-time")
    assert option["required"] is required
    assert "default" not in option
    assert option["type"] == "string"
    assert "Required on DS 1.3.9 and 2.0.x." in _require_str(option["description"])


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.4.3"])
@pytest.mark.parametrize("action", ["create", "update", "list"])
def test_project_parameter_schema_projects_exact_type_choices_and_parser_defaults(
    tmp_path: Path,
    ds_version: str,
    action: str,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
    data = _require_dict(
        get_schema_result(
            env_file=str(env_file), command_action=f"project-parameter.{action}"
        ).data
    )
    command = _require_dict(data["command"])
    option = _find_option(_require_list(command["options"]), "data-type")

    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        assert option["choices"] == ["VARCHAR"]
    else:
        assert "choices" not in option
    assert option.get("default") == ("VARCHAR" if action == "create" else None)
    assert ("default" in option) is (action == "create")
    assert option["type"] == "string"
    assert "Non-VARCHAR types require DS 3.3.1+" in _require_str(option["description"])


@pytest.mark.parametrize("ds_version", [None, "9.9.9"])
@pytest.mark.parametrize("action", ["create", "update", "list"])
def test_unresolved_project_parameter_schema_does_not_invent_type_choices(
    tmp_path: Path,
    ds_version: str | None,
    action: str,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\n"
        + (f"DS_VERSION={ds_version}\n" if ds_version else ""),
        encoding="utf-8",
    )
    if ds_version is not None:
        with pytest.raises(ConfigError, match="Unsupported DS version"):
            get_schema_result(
                env_file=str(env_file), command_action=f"project-parameter.{action}"
            )
        return
    data = _require_dict(
        get_schema_result(
            env_file=str(env_file), command_action=f"project-parameter.{action}"
        ).data
    )
    command = _require_dict(data["command"])
    option = _find_option(_require_list(command["options"]), "data-type")

    assert "choices" not in option
    assert option.get("default") == ("VARCHAR" if action == "create" else None)
    assert ("default" in option) is (action == "create")


@pytest.mark.parametrize(
    ("ds_version", "unavailable_flags"),
    [
        (
            "1.3.9",
            {
                "--timezone",
                "--tenant-code",
                "--environment-code",
                "--missed-fire-policy",
            },
        ),
        ("2.0.0", {"--tenant-code", "--missed-fire-policy"}),
        ("3.1.9", {"--tenant-code", "--missed-fire-policy"}),
        ("3.2.0", {"--missed-fire-policy"}),
    ],
)
def test_schema_projects_exact_schedule_create_fields_for_selected_version(
    tmp_path: Path,
    ds_version: str,
    unavailable_flags: set[str],
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="schedule.create",
        ).data
    )
    command = _require_dict(data["command"])
    options = _require_list(command["options"])
    option_names = {_require_str(_require_dict(option)["name"]) for option in options}
    unavailable = command.get("unavailable_options", [])
    projected_flags = {
        _require_str(_require_dict(item)["flag"]) for item in _require_list(unavailable)
    }

    assert projected_flags == unavailable_flags
    assert ("timezone" in option_names) is (ds_version != "1.3.9")
    assert ("environment-code" in option_names) is (ds_version != "1.3.9")
    assert ("tenant-code" in option_names) is (ds_version == "3.2.0")
    if "timezone" in option_names:
        assert _find_option(options, "timezone")["required"] is True
    for item in _require_list(unavailable):
        unavailable_option = _require_dict(item)
        assert unavailable_option["availability"] == "upstream_absent"
        assert unavailable_option["instruction"] == "omit"


@pytest.mark.parametrize(
    ("ds_version", "present", "absent"),
    [
        ("1.3.9", {"name", "group-type", "description"}, {"instance-id"}),
        ("2.0.0", {"name", "instance-id", "description"}, {"group-type"}),
        ("3.4.2", {"name", "instance-id", "description"}, {"group-type"}),
    ],
)
def test_schema_projects_exact_alert_group_association_for_selected_version(
    tmp_path: Path,
    ds_version: str,
    present: set[str],
    absent: set[str],
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="alert-group.create",
        ).data
    )
    command = _require_dict(data["command"])
    options = _require_list(command["options"])
    option_names = {_require_str(_require_dict(option)["name"]) for option in options}

    assert present <= option_names
    assert not (absent & option_names)
    if ds_version == "1.3.9":
        group_type = _find_option(options, "group-type")
        assert group_type["required"] is True
        assert group_type["choices"] == ["EMAIL", "SMS"]


@pytest.mark.parametrize(
    ("ds_version", "unavailable_flags"),
    [
        (
            "1.3.9",
            {
                "--workflow-instance-name",
                "--execute-type",
                "--task-code",
            },
        ),
        ("2.0.0", {"--execute-type", "--task-code"}),
        ("3.1.9", {"--task-code"}),
        ("3.2.0", set()),
        ("3.4.2", set()),
    ],
)
def test_schema_projects_exact_task_instance_list_fields_for_selected_version(
    tmp_path: Path,
    ds_version: str,
    unavailable_flags: set[str],
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="task-instance.list",
        ).data
    )
    command = _require_dict(data["command"])
    options = _require_list(command["options"])
    option_names = {_require_str(_require_dict(option)["name"]) for option in options}
    unavailable = _require_list(command.get("unavailable_options", []))
    projected_flags = {
        _require_str(_require_dict(item)["flag"]) for item in unavailable
    }

    assert projected_flags == unavailable_flags
    for flag in unavailable_flags:
        assert flag.removeprefix("--") not in option_names
    for item in unavailable:
        unavailable_option = _require_dict(item)
        assert unavailable_option["availability"] == "upstream_absent"
        assert unavailable_option["instruction"] == "omit"


def test_schema_projects_legacy_task_selector_and_update_fields(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    get_data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="task.get",
        ).data
    )
    get_command = _require_dict(get_data["command"])
    get_task = _require_dict(_require_list(get_command["arguments"])[0])

    assert _require_dict(get_command["data_shape"])["default_columns"] == [
        "id",
        "name",
    ]
    assert get_command["summary"] == "Get one task definition by exact name."
    assert get_task["selector"] == "opaque_name"
    assert get_task["description"] == (
        "Exact task name inside the selected workflow. Use `dsctl task list` "
        "to discover values."
    )

    update_data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="task.update",
        ).data
    )
    update_command = _require_dict(update_data["command"])
    update_task = _require_dict(_require_list(update_command["arguments"])[0])
    update_options = _require_list(update_command["options"])
    set_option = _find_option(update_options, "set")

    assert update_task["selector"] == "opaque_name"
    assert update_command["summary"] == (
        "Update one task by exact name; use workflow edit for other DAG changes, "
        "workflow-instance edit for repairs."
    )
    assert set_option["supported_keys"] == [
        "command",
        "depends_on",
        "description",
        "flag",
        "priority",
        "retry.interval",
        "retry.times",
        "timeout",
        "timeout_notify_strategy",
        "worker_group",
    ]
    assert set_option["examples"] == [
        "command=python v2.py",
        "retry.times=5",
        "timeout_notify_strategy=FAILED",
    ]

    list_data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="task.list",
        ).data
    )
    list_command = _require_dict(list_data["command"])
    assert _require_dict(list_command["data_shape"])["default_columns"] == [
        "id",
        "name",
    ]


@pytest.mark.parametrize(
    ("ds_version", "absent_keys"),
    [
        (
            "2.0.0",
            {
                "cpu_quota",
                "memory_max",
                "task_group_id",
                "task_group_priority",
            },
        ),
        (
            "2.0.9",
            {
                "cpu_quota",
                "depends_on",
                "memory_max",
                "task_group_id",
                "task_group_priority",
            },
        ),
        ("3.0.0", {"cpu_quota", "depends_on", "memory_max"}),
        ("3.1.0", {"depends_on"}),
        ("3.1.9", {"depends_on"}),
        ("3.2.0", {"depends_on"}),
        ("3.2.1", set()),
        ("3.4.2", set()),
    ],
)
def test_schema_projects_task_update_keys_from_the_exact_wire_epoch(
    tmp_path: Path,
    ds_version: str,
    absent_keys: set[str],
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="task.update",
        ).data
    )
    command = _require_dict(data["command"])
    set_option = _find_option(_require_list(command["options"]), "set")
    supported_keys = set(_require_list(set_option["supported_keys"]))
    stable_keys = {
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
    }

    assert supported_keys == stable_keys - absent_keys


@pytest.mark.parametrize(
    "ds_version",
    [
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.2.1",
    ],
)
def test_schema_projects_legacy_audit_filters_from_exact_controller_contract(
    tmp_path: Path,
    ds_version: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="audit.list",
        ).data
    )
    command = _require_dict(data["command"])
    options = _require_list(command["options"])
    model_type = _find_option(options, "model-type")
    operation_type = _find_option(options, "operation-type")

    assert model_type["multiple"] is False
    assert model_type["choices"] == ["USER_MODULE", "PROJECT_MODULE"]
    assert "discovery_command" not in model_type
    assert operation_type["multiple"] is False
    assert operation_type["choices"] == ["CREATE", "READ", "UPDATE", "DELETE"]
    assert "discovery_command" not in operation_type
    assert "model-name" not in {
        _require_str(_require_dict(item)["name"]) for item in options
    }
    assert _require_list(command["unavailable_options"]) == [
        {
            "flag": "--model-name",
            "availability": "upstream_absent",
            "introduced_in": "3.2.2",
            "instruction": "omit",
        }
    ]


@pytest.mark.parametrize(
    "ds_version",
    [
        "1.3.9",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.2.1",
    ],
)
def test_schema_limits_fixed_monitor_routes_to_master_and_worker(
    tmp_path: Path,
    ds_version: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(
            env_file=str(env_file),
            command_action="monitor.server",
        ).data
    )
    command = _require_dict(data["command"])
    node_type = _require_dict(_require_list(command["arguments"])[0])

    assert node_type["choices"] == ["master", "worker"]


def test_schema_projects_schedule_139_branch_constraints(tmp_path: Path) -> None:
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    preview = _require_dict(
        _require_dict(
            get_schema_result(
                env_file=str(env_file),
                command_action="schedule.preview",
            ).data
        )["command"]
    )
    assert preview["constraints"] == [
        {
            "kind": "exactly_one_of",
            "alternatives": [
                ["SCHEDULE_ID"],
                ["--cron", "--start", "--end"],
            ],
        },
        {
            "kind": "forbids",
            "if_present": "SCHEDULE_ID",
            "fields": ["--cron", "--start", "--end"],
        },
    ]

    explain = _require_dict(
        _require_dict(
            get_schema_result(
                env_file=str(env_file),
                command_action="schedule.explain",
            ).data
        )["command"]
    )
    explain_constraints = _require_list(explain["constraints"])
    serialized_explain_constraints = str(explain_constraints)
    assert "--timezone" not in serialized_explain_constraints
    assert "--environment-code" not in serialized_explain_constraints
    assert "--tenant-code" not in serialized_explain_constraints
    assert [
        _require_str(_require_dict(item)["flag"])
        for item in _require_list(explain["unavailable_options"])
    ] == ["--timezone", "--missed-fire-policy", "--tenant-code", "--environment-code"]

    update = _require_dict(
        _require_dict(
            get_schema_result(
                env_file=str(env_file),
                command_action="schedule.update",
            ).data
        )["command"]
    )
    serialized_update_constraints = str(_require_list(update["constraints"]))
    assert "--timezone" not in serialized_update_constraints
    assert "--environment-code" not in serialized_update_constraints


def test_full_schema_uses_selected_schedule_version_projection(tmp_path: Path) -> None:
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    data = _require_dict(get_schema_result(env_file=str(env_file), full=True).data)
    schedule = _find_group(data["commands"], "schedule")
    create = _find_command(schedule["commands"], "create")
    options = _require_list(create["options"])

    assert "timezone" not in {
        _require_str(_require_dict(option)["name"]) for option in options
    }
    assert [
        _require_str(_require_dict(item)["flag"])
        for item in _require_list(create["unavailable_options"])
    ] == ["--timezone", "--missed-fire-policy", "--tenant-code", "--environment-code"]


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.1.0", "3.4.2"])
def test_schema_projects_exact_local_metadata_choices(
    tmp_path: Path,
    ds_version: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
    catalog = get_task_authoring_catalog(ds_version)

    task_result = get_schema_result(
        env_file=str(env_file),
        command_action="template.task",
    )
    task_command = _require_dict(_require_dict(task_result.data)["command"])
    task_argument = _require_dict(_require_list(task_command["arguments"])[0])
    assert task_argument["choices"] == list(
        supported_task_template_types(catalog=catalog)
    )

    datasource_result = get_schema_result(
        env_file=str(env_file),
        command_action="template.datasource",
    )
    datasource_command = _require_dict(_require_dict(datasource_result.data)["command"])
    datasource_type = _find_option(
        _require_list(datasource_command["options"]),
        "type",
    )
    assert (
        datasource_type["choices"]
        == datasource_template_index_data(version=ds_version)["supported_types"]
    )


def test_schema_result_can_return_one_group() -> None:
    result = get_schema_result(group="task-instance", full=True)
    data = result.data

    assert isinstance(data, dict)
    assert "capabilities" not in data
    assert result.resolved == {
        "schema": {
            "view": "full",
            "scope": "group",
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
        "availability": "supported",
        "verification": "contract_tested",
        "effects": {"remote": "read", "local": "none"},
    }


def test_schema_result_can_return_one_command() -> None:
    result = get_schema_result(command_action="task-instance.list", full=True)
    data = result.data

    assert isinstance(data, dict)
    assert "capabilities" not in data
    assert _require_dict(data["ds"])["selected_version"] == "3.4.1"
    assert data["capability"] == {
        "action": "task-instance.list",
        "availability": "supported",
        "verification": "contract_tested",
    }
    assert result.resolved == {
        "schema": {
            "view": "full",
            "scope": "command",
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
        "compact_rows": True,
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
        "invocation": "dsctl task-instance list [OPTIONS]",
    }
    assert any(
        _require_dict(row).get("kind") == "data_shape"
        and _require_dict(row).get("name") == "row_path"
        and _require_dict(row).get("value") == "data.totalList"
        for row in rows
    )


def test_schema_group_marks_selected_version_action_availability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DS_VERSION", raising=False)
    env_file = tmp_path / "ds-3.4.2.env"
    env_file.write_text("DS_VERSION=3.4.2\n", encoding="utf-8")

    result = get_schema_result(env_file=str(env_file), group="workflow")
    data = _require_dict(result.data)
    actions = {
        _require_dict(item)["action"]: _require_dict(item)
        for item in _require_list(data["actions"])
    }

    assert _require_dict(data["group"])["available_action_count"] == len(actions) == 16
    for action in (
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
    ):
        assert actions[action]["availability"] == "supported"
        assert actions[action]["verification"] == "live_smoke"
    assert actions["workflow.create"]["availability"] == "supported"
    assert actions["workflow.create"]["verification"] == "contract_tested"
    for action in ("workflow.get", "workflow.list"):
        assert actions[action]["availability"] == "supported"
        assert actions[action]["verification"] == "live_smoke"

    schedule_data = _require_dict(
        get_schema_result(env_file=str(env_file), group="schedule").data
    )
    schedule_actions = {
        _require_str(_require_dict(item)["action"]): _require_dict(item)
        for item in _require_list(schedule_data["actions"])
    }
    assert _require_dict(schedule_data["group"])["available_action_count"] == 9
    assert tuple(sorted(schedule_actions)) == (
        "schedule.create",
        "schedule.delete",
        "schedule.explain",
        "schedule.get",
        "schedule.list",
        "schedule.offline",
        "schedule.online",
        "schedule.preview",
        "schedule.update",
    )
    schedule_claims = {
        action: (
            metadata["availability"],
            metadata["verification"],
        )
        for action, metadata in schedule_actions.items()
    }
    expected_schedule_claims = dict.fromkeys(
        schedule_actions,
        ("supported", "contract_tested"),
    )
    expected_schedule_claims["schedule.list"] = ("supported", "live_smoke")
    assert schedule_claims == expected_schedule_claims


def test_partial_profile_full_schema_keeps_unavailable_capabilities_discoverable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DS_VERSION", raising=False)
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    enum_versions: list[str | None] = []

    def record_enum_registry(*, ds_version: str | None = None) -> dict[str, object]:
        enum_versions.append(ds_version)
        return enum_capabilities_data(ds_version=ds_version)

    monkeypatch.setattr(
        capabilities_service,
        "enum_capabilities_data",
        record_enum_registry,
    )

    data = _require_dict(get_schema_result(env_file=str(env_file), full=True).data)
    capabilities = _require_dict(data["capabilities"])
    authoring = _require_dict(capabilities["authoring"])
    availability = _require_dict(authoring["selected_version_availability"])

    assert _require_dict(data["ds"])["selected_version"] == "1.3.9"
    assert availability["workflow_yaml_create"] is True
    assert availability["task_authoring_schema"] is True
    enums = _require_dict(capabilities["enums"])
    assert enums["discovery"] is True
    assert "priority" in _require_list(enums["names"])
    assert enum_versions == ["1.3.9"]


def test_partial_profile_group_schema_marks_reads_and_writes_exactly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DS_VERSION", raising=False)
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    data = _require_dict(
        get_schema_result(env_file=str(env_file), group="workflow").data
    )
    actions = {
        _require_dict(item)["action"]: _require_dict(item)
        for item in _require_list(data["actions"])
    }

    assert _require_dict(data["group"])["available_action_count"] == 13
    assert actions["workflow.get"]["availability"] == "supported"
    assert actions["workflow.get"]["verification"] == "live_smoke"
    assert actions["workflow.create"]["availability"] == "supported"
    assert actions["workflow.create"]["verification"] == "contract_tested"
    assert actions["workflow.lineage.list"]["availability"] == "unsupported"


def test_partial_profile_command_schema_exposes_unsupported_capability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DS_VERSION", raising=False)
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    constraints = {
        "task-type.list": (
            "This DolphinScheduler release predates live favourite task-type "
            "discovery introduced in 3.1.0."
        ),
        "template.cluster": (
            "This DolphinScheduler release predates the cluster-management "
            "capability introduced in 3.1.0."
        ),
        "audit.list": (
            "This DolphinScheduler release predates audit-log management "
            "introduced in 3.0.0."
        ),
    }
    for action, constraint in constraints.items():
        data = _require_dict(
            get_schema_result(
                env_file=str(env_file),
                command_action=action,
            ).data
        )

        assert data["capability"] == {
            "action": action,
            "availability": "unsupported",
            "verification": "static",
            "constraint": constraint,
        }


def test_full_action_schema_preserves_selected_version_capability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DS_VERSION", raising=False)
    env_file = tmp_path / "ds-1.3.9.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    result = get_schema_result(
        env_file=str(env_file),
        command_action="task-type.list",
        full=True,
    )
    data = _require_dict(result.data)

    assert _require_dict(data["ds"])["selected_version"] == "1.3.9"
    assert data["capability"] == {
        "action": "task-type.list",
        "availability": "unsupported",
        "verification": "static",
        "constraint": (
            "This DolphinScheduler release predates live favourite task-type "
            "discovery introduced in 3.1.0."
        ),
    }

    workflow_instance_result = get_schema_result(
        command_action="workflow-instance.list",
        full=True,
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
        "compact_rows": True,
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
        command_action="workflow-instance.get",
        full=True,
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
    assert [_require_dict(item)["name"] for item in workflow_instance_get_options] == [
        "project"
    ]

    datasource_list_result = get_schema_result(
        command_action="datasource.list",
        full=True,
    )
    datasource_list_data = _require_dict(datasource_list_result.data)
    datasource_group = _require_dict(_require_list(datasource_list_data["commands"])[0])
    datasource_list_command = _require_dict(
        _require_list(datasource_group["commands"])[0]
    )
    assert datasource_list_command["data_shape"] == {
        "kind": "page",
        "compact_rows": True,
        "row_path": "data.totalList",
        "default_columns": ["id", "name", "type", "createTime"],
        "column_discovery": "runtime_row_keys",
    }
    datasource_get_result = get_schema_result(
        command_action="datasource.get",
        full=True,
    )
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
    result = get_schema_result(command_action="datasource.create", full=True)
    data = _require_dict(result.data)
    rows = [_require_dict(row) for row in _require_list(data["rows"])]

    file_row = next(row for row in rows if row.get("name") == "file")
    assert file_row["discovery_command"] == "dsctl template datasource"
    assert file_row["description"] == (
        "Path to one DS-native datasource JSON payload file. Start with "
        "`dsctl template datasource`, then `dsctl template datasource --type TYPE` "
        "and pass the saved data.json path here."
    )
    assert {
        "kind": "payload",
        "name": "template_discovery_command",
        "value": "dsctl template datasource",
    } in rows
    assert {
        "kind": "payload",
        "name": "target_command_patterns",
        "value": (
            "dsctl datasource create --file FILE, "
            "dsctl datasource update DATASOURCE --file FILE"
        ),
    } in rows

    workflow_result = get_schema_result(command_action="workflow.edit", full=True)
    workflow_data = _require_dict(workflow_result.data)
    workflow_rows = [_require_dict(row) for row in _require_list(workflow_data["rows"])]
    assert {
        "kind": "payload",
        "name": "source_options",
        "value": "--patch PATCH, --file FILE",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "patch_template_command",
        "value": "dsctl template workflow-patch --raw",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "file_source_command_pattern",
        "value": "dsctl workflow export WORKFLOW",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "file_schedule",
        "value": "read_only_snapshot",
    } in workflow_rows
    assert {
        "kind": "payload",
        "name": "target_command_patterns",
        "value": (
            "dsctl workflow edit WORKFLOW --patch FILE, "
            "dsctl workflow edit WORKFLOW --file FILE"
        ),
    } in workflow_rows

    export_result = get_schema_result(command_action="workflow.export", full=True)
    export_data = _require_dict(export_result.data)
    export_rows = [_require_dict(row) for row in _require_list(export_data["rows"])]
    assert {
        "kind": "payload",
        "name": "target_command_patterns",
        "value": (
            "dsctl workflow create --file FILE, "
            "dsctl workflow edit WORKFLOW --file FILE"
        ),
    } in export_rows
    assert {
        "kind": "payload",
        "name": "schedule_on_edit",
        "value": "read_only_snapshot",
    } in export_rows


def test_schema_result_can_list_group_and_command_discovery_rows() -> None:
    groups_result = get_schema_result(list_groups=True)
    groups_data = _require_list(groups_result.data)
    first_group = _require_dict(groups_data[0])

    assert groups_result.resolved == {
        "schema": {
            "view": "groups",
        }
    }
    assert first_group == {
        "name": "context",
        "summary": "Manage named connection contexts and inspect selection.",
        "action_count": 6,
        "schema_command": "dsctl schema --group context",
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
        }
    }
    assert version_command == {
        "action": "version",
        "group": None,
        "name": "version",
        "summary": "Print CLI and selectable DolphinScheduler version metadata.",
        "effects": {"remote": "none", "local": "none"},
        "schema_command": "dsctl schema --command version",
    }
    assert datasource_create == {
        "action": "datasource.create",
        "group": "datasource",
        "name": "create",
        "summary": "Create one datasource from a JSON payload file.",
        "effects": {"remote": "write", "local": "none"},
        "schema_command": "dsctl schema --command datasource.create",
    }


def test_data_shapes_do_not_infer_get_metadata_for_unstable_actions() -> None:
    commands = _require_list(get_schema_result(list_commands=True).data)
    stable_actions = {
        action
        for item in commands
        if isinstance((action := _require_dict(item).get("action")), str)
    }
    inferred_get_actions = {
        f"{action.removesuffix('.list')}.get"
        for action in stable_actions
        if action.endswith(".list")
    }

    orphan_get_actions = inferred_get_actions - stable_actions

    assert {
        action
        for action in orphan_get_actions
        if data_shape_schema_for_action(action) is not None
    } == set()


def test_schema_result_exposes_collection_and_nested_data_shapes() -> None:
    workflow_result = get_schema_result(command_action="workflow.list")
    workflow_data = _require_dict(workflow_result.data)
    workflow_command = _require_dict(workflow_data["command"])
    assert workflow_command["data_shape"] == {
        "kind": "page",
        "compact_rows": True,
        "row_path": "data.totalList",
        "default_columns": [
            "code",
            "name",
            "version",
            "releaseState",
            "scheduleReleaseState",
            "scheduleId",
        ],
        "column_discovery": "runtime_row_keys",
    }

    task_type_result = get_schema_result(command_action="task-type.list")
    task_type_data = _require_dict(task_type_result.data)
    task_type_command = _require_dict(task_type_data["command"])
    assert task_type_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.taskTypes",
        "compact_rows": True,
        "default_columns": ["taskType", "taskCategory", "isCollection"],
        "column_discovery": "runtime_row_keys",
    }

    task_type_schema_result = get_schema_result(command_action="task-type.schema")
    task_type_schema_data = _require_dict(task_type_schema_result.data)
    task_type_schema_command = _require_dict(task_type_schema_data["command"])
    assert task_type_schema_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.fields",
        "default_columns": [
            "path",
            "type",
            "required",
            "default",
            "choice_source",
            "choice_value",
            "active_when",
        ],
        "column_discovery": "runtime_row_keys",
    }
    assert task_type_schema_command["data_shapes_by_view"] == {
        "fields": {
            "kind": "summary",
            "row_path": "data.fields",
            "default_columns": [
                "path",
                "type",
                "required",
                "default",
                "choice_source",
                "choice_value",
                "active_when",
            ],
            "column_discovery": "runtime_row_keys",
        },
        "field": {
            "kind": "summary",
            "row_path": "data.fields",
            "default_columns": [
                "path",
                "type",
                "required",
                "default",
                "choice_source",
                "choice_value",
                "active_when",
            ],
            "column_discovery": "runtime_row_keys",
        },
        "json_schema": {
            "kind": "document",
            "value_path": "data.schema",
            "column_discovery": "not_applicable",
            "supported_output_formats": ["json", "json-compact"],
            "column_projection": False,
        },
        "compile_mappings": {
            "kind": "summary",
            "row_path": "data.compile_mappings",
            "default_columns": [
                "authoring_path",
                "ds_payload_path",
            ],
            "column_discovery": "runtime_row_keys",
        },
        "full": {
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
        },
    }

    alert_definition_result = get_schema_result(
        command_action="alert-plugin.definition.list"
    )
    alert_definition_data = _require_dict(alert_definition_result.data)
    alert_definition_command = _require_dict(alert_definition_data["command"])
    assert alert_definition_command["data_shape"] == {
        "kind": "summary",
        "row_path": "data.definitions",
        "compact_rows": True,
        "default_columns": ["id", "pluginName", "pluginType"],
        "column_discovery": "runtime_row_keys",
    }

    digest_result = get_schema_result(command_action="workflow-instance.digest")
    digest_data = _require_dict(digest_result.data)
    digest_command = _require_dict(digest_data["command"])
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


@pytest.mark.parametrize(
    ("action", "default_columns"),
    [
        ("context", ["context", "api_url", "ds_version", "project"]),
        ("context.create", ["name", "env_file", "api_url", "project"]),
        ("context.update", ["name", "env_file", "api_url", "project"]),
        ("context.get", ["name", "env_file", "api_url", "project", "default"]),
        ("context.delete", ["name", "env_file", "api_url", "project", "deleted"]),
        ("config.get", ["key", "value"]),
        ("config.set", ["key", "value"]),
        ("config.unset", ["key", "value"]),
    ],
)
def test_schema_result_describes_context_object_data_shapes(
    action: str,
    default_columns: list[str],
) -> None:
    result = get_schema_result(command_action=action)
    data = _require_dict(result.data)
    command = _require_dict(data["command"])

    assert command["data_shape"] == {
        "kind": "object",
        "row_path": "data",
        "default_columns": default_columns,
        "column_discovery": "runtime_row_keys",
    }


@pytest.mark.parametrize(
    "action", ["context", "context.list", "context.get", "config.get"]
)
def test_context_schema_declares_read_only_local_execution(action: str) -> None:
    result = get_schema_result(command_action=action)
    data = _require_dict(result.data)
    command = _require_dict(data["command"])

    assert command["effects"] == {"remote": "none", "local": "none"}


def test_context_list_schema_describes_saved_entry_rows() -> None:
    data = _require_dict(get_schema_result(command_action="context.list").data)
    command = _require_dict(data["command"])
    assert command["data_shape"] == {
        "kind": "collection",
        "compact_rows": True,
        "row_path": "data",
        "default_columns": ["name", "env_file", "api_url", "project", "default"],
        "column_discovery": "runtime_row_keys",
    }


@pytest.mark.parametrize(
    "action",
    [
        "context.create",
        "context.update",
        "context.delete",
        "config.set",
        "config.unset",
    ],
)
def test_context_schema_declares_local_registry_mutations(action: str) -> None:
    data = _require_dict(get_schema_result(command_action=action).data)
    command = _require_dict(data["command"])
    assert command["effects"] == {
        "remote": "none",
        "local": "configuration",
    }


def test_schema_result_can_return_one_top_level_command() -> None:
    result = get_schema_result(command_action="version")
    data = result.data

    assert isinstance(data, dict)
    version_command = _require_dict(data["command"])
    assert version_command["kind"] == "command"
    assert version_command["action"] == "version"


def test_schema_result_can_return_group_action_command() -> None:
    result = get_schema_result(command_action="context")
    data = result.data

    assert isinstance(data, dict)
    command = _require_dict(data["command"])
    assert command["name"] == "context"
    assert command["action"] == "context"
    assert _require_list(command["arguments"]) == []


def test_schema_result_rejects_conflicting_scope_options() -> None:
    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_schema_result(group="workflow", command_action="workflow.run")

    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_schema_result(group="workflow", list_groups=True)


def test_schema_result_rejects_unknown_group() -> None:
    with pytest.raises(UserInputError, match="Unknown schema group") as exc_info:
        get_schema_result(group="missing")

    assert exc_info.value.details["requested"] == "missing"
    assert exc_info.value.details["available_count"] == 30
    assert isinstance(exc_info.value.details["candidates"], list)
    assert exc_info.value.details["discovery_command"] == ("dsctl schema --list-groups")
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --list-groups` to choose a group name."
    )


def test_schema_result_rejects_unknown_command() -> None:
    with pytest.raises(UserInputError, match="Unknown schema action") as exc_info:
        get_schema_result(command_action="missing.command")

    assert exc_info.value.details["requested"] == "missing.command"
    assert exc_info.value.details["available_count"] == 181
    candidates = exc_info.value.details["candidates"]
    assert isinstance(candidates, list)
    assert len(candidates) <= 3
    assert exc_info.value.suggestion == (
        "Run `dsctl schema` to browse the bounded action index."
    )


def test_schema_result_describes_context_group_action_and_named_management() -> None:
    data = _require_dict(get_schema_result(full=True).data)
    group = _find_group(data["commands"], "context")
    action = _require_dict(group["group_action"])
    assert action["action"] == "context"
    assert _require_list(action["arguments"]) == []
    assert _require_list(action["options"]) == []
    assert action["effects"] == {"remote": "none", "local": "none"}
    commands = _require_list(group["commands"])
    assert [_require_dict(item)["name"] for item in commands] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    for name in ("create", "get", "update", "delete"):
        command = _find_command(commands, name)
        argument = _require_dict(_require_list(command["arguments"])[0])
        assert argument["required"] is True
        assert argument["discovery_command"] == "dsctl context list"
        assert _require_dict(command["effects"])["remote"] == "none"
    update = _find_command(commands, "update")
    assert {
        "kind": "at_most_one_of",
        "fields": ["--project", "--clear-project"],
    } in _require_list(update["constraints"])
    assert {
        "kind": "requires_any",
        "fields": ["--file", "--project", "--clear-project"],
    } in _require_list(update["constraints"])
    file_option = _find_option(_require_list(update["options"]), "file")
    assert "Clears the project unless --project" in _require_str(
        file_option["description"]
    )


def test_schema_describes_project_native_identity_boundary() -> None:
    data = _require_dict(get_schema_result(full=True).data)
    commands = data["commands"]
    native_boundary = "native numeric identity (id on DS 1.3.9, code on newer versions)"

    project_get = _find_command(_find_group(commands, "project")["commands"], "get")
    project_argument = _require_dict(_require_list(project_get["arguments"])[0])
    context_create = _find_command(
        _find_group(commands, "context")["commands"], "create"
    )
    project_parameter_list = _find_command(
        _find_group(commands, "project-parameter")["commands"], "list"
    )
    schedule_group = _find_group(commands, "schedule")
    workflow_create = _find_command(
        _find_group(commands, "workflow")["commands"], "create"
    )
    workflow_instance_list = _find_command(
        _find_group(commands, "workflow-instance")["commands"], "list"
    )
    project_inputs = [
        project_argument,
        _find_option(_require_list(context_create["options"]), "project"),
        _find_option(_require_list(project_parameter_list["options"]), "project"),
        _find_option(
            _require_list(_find_command(schedule_group["commands"], "get")["options"]),
            "project",
        ),
        _find_option(
            _require_list(
                _find_command(schedule_group["commands"], "preview")["options"]
            ),
            "project",
        ),
        _find_option(
            _require_list(
                _find_command(schedule_group["commands"], "explain")["options"]
            ),
            "project",
        ),
        _find_option(_require_list(workflow_create["options"]), "project"),
        _find_option(_require_list(workflow_instance_list["options"]), "project"),
    ]
    for project_input in project_inputs:
        assert project_input["selector"] == "name_or_native_identity"
        assert native_boundary in _require_str(project_input["description"])
    assert native_boundary in _require_str(project_get["summary"])

    workflow_get = _find_command(_find_group(commands, "workflow")["commands"], "get")
    workflow_argument = _require_dict(_require_list(workflow_get["arguments"])[0])
    assert workflow_argument["selector"] == "name_or_code"
    assert native_boundary not in _require_str(workflow_argument["description"])


def test_schema_describes_explicit_workflow_selection() -> None:
    result = get_schema_result(full=True)
    data = _require_dict(result.data)

    workflow_group = _find_group(data["commands"], "workflow")
    workflow_get = _find_command(workflow_group["commands"], "get")
    workflow_argument = _require_dict(_require_list(workflow_get["arguments"])[0])
    assert "Pass WORKFLOW explicitly" in _require_str(workflow_argument["description"])
    assert workflow_argument["required"] is True
    assert "constraints" not in workflow_get

    task_group = _find_group(data["commands"], "task")
    task_list = _find_command(task_group["commands"], "list")
    workflow_option = _find_option(
        _require_list(task_list["options"]),
        "workflow",
    )
    assert "Pass --workflow explicitly" in _require_str(workflow_option["description"])
    assert workflow_option["required"] is True
    assert "constraints" not in task_list


def test_schema_describes_mode_specific_workflow_selectors() -> None:
    result = get_schema_result(full=True)
    data = _require_dict(result.data)

    workflow_group = _find_group(data["commands"], "workflow")
    workflow_describe = _find_command(workflow_group["commands"], "describe")
    workflow_argument = _require_dict(_require_list(workflow_describe["arguments"])[0])
    workflow_description = _require_str(workflow_argument["description"])
    assert "Pass WORKFLOW explicitly" in workflow_description
    assert "--file" not in workflow_description
    assert "--patch" not in workflow_description

    schedule_group = _find_group(data["commands"], "schedule")
    schedule_preview = _find_command(schedule_group["commands"], "preview")
    preview_options = _require_list(schedule_preview["options"])
    preview_project_option = _find_option(preview_options, "project")
    assert "constrains lookup" in _require_str(preview_project_option["description"])

    schedule_explain = _find_command(schedule_group["commands"], "explain")
    explain_options = _require_list(schedule_explain["options"])
    workflow_option = _find_option(explain_options, "workflow")
    project_option = _find_option(explain_options, "project")
    assert "create explain only" in _require_str(workflow_option["description"])
    assert "do not pass --workflow with SCHEDULE_ID" in _require_str(
        workflow_option["description"]
    )
    assert "With SCHEDULE_ID, constrains lookup" in _require_str(
        project_option["description"]
    )


def test_schema_defaults_follow_runtime_constants() -> None:
    result = get_schema_result(full=True)
    data = result.data

    assert isinstance(data, dict)
    project_group = _find_group(data["commands"], "project")
    project_list = _find_command(project_group["commands"], "list")
    project_list_options = _require_list(project_list["options"])
    project_page_size = _find_option(project_list_options, "page-size")
    assert project_page_size["default"] == DEFAULT_PAGE_SIZE

    workflow_group = _find_group(data["commands"], "workflow")
    workflow_list = _find_command(workflow_group["commands"], "list")
    workflow_list_options = _require_list(workflow_list["options"])
    assert _find_option(workflow_list_options, "page-no")["default"] == 1
    assert _find_option(workflow_list_options, "page-size")["default"] == (
        DEFAULT_PAGE_SIZE
    )
    assert _find_option(workflow_list_options, "all")["default"] is False

    workflow_instance_group = _find_group(data["commands"], "workflow-instance")
    watch_command = _find_command(workflow_instance_group["commands"], "watch")
    watch_options = _require_list(watch_command["options"])
    assert _find_option(watch_options, "interval-seconds")["default"] == (
        DEFAULT_WATCH_INTERVAL_SECONDS
    )
    assert _find_option(watch_options, "interval-seconds")["minimum"] == 1
    assert _find_option(watch_options, "timeout-seconds")["default"] == (
        DEFAULT_WATCH_TIMEOUT_SECONDS
    )
    assert _find_option(watch_options, "timeout-seconds")["minimum"] == 0

    task_instance_group = _find_group(data["commands"], "task-instance")
    task_instance_watch = _find_command(task_instance_group["commands"], "watch")
    task_instance_watch_options = _require_list(task_instance_watch["options"])
    assert _find_option(task_instance_watch_options, "interval-seconds")["default"] == (
        DEFAULT_TASK_INSTANCE_WATCH_INTERVAL_SECONDS
    )
    assert _find_option(task_instance_watch_options, "interval-seconds")["minimum"] == 1
    assert _find_option(task_instance_watch_options, "timeout-seconds")["default"] == (
        DEFAULT_TASK_INSTANCE_WATCH_TIMEOUT_SECONDS
    )
    assert _find_option(task_instance_watch_options, "timeout-seconds")["minimum"] == 0


def test_schema_task_update_set_option_exposes_supported_keys() -> None:
    result = get_schema_result(full=True)
    data = result.data

    assert isinstance(data, dict)
    task_group = _find_group(data["commands"], "task")
    task_update = _find_command(task_group["commands"], "update")
    task_update_options = _require_list(task_update["options"])
    set_option = _find_option(task_update_options, "set")

    task_update_args = _require_list(task_update["arguments"])
    assert _require_dict(task_update_args[0])["discovery_command_pattern"] == (
        "dsctl task list --project PROJECT --workflow WORKFLOW"
    )
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
    result = get_schema_result(full=True)
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


@pytest.mark.parametrize(
    "action",
    ["workflow.run", "workflow.run-task", "workflow.backfill"],
)
def test_workflow_run_schema_distinguishes_fallbacks_from_parser_defaults(
    action: str,
) -> None:
    result = get_schema_result(command_action=action)
    data = _require_dict(result.data)
    command = _require_dict(data["command"])
    options = _require_list(command["options"])

    expected_fallbacks = {
        "worker-group": "default",
        "tenant": "default",
        "priority": "medium",
        "warning-type": "none",
        "warning-group-id": None,
        "environment-code": None,
    }
    for option_name, fallback in expected_fallbacks.items():
        option = _find_option(options, option_name)
        assert option["resolution"] == {
            "precedence": ["flag", "project_preference", "default"],
            "fallback": fallback,
        }

    # Schema v3 keeps these established fields as the terminal fallback while
    # resolution makes clear that project preference is consulted first.
    assert _find_option(options, "priority")["default"] == "medium"
    assert _find_option(options, "warning-type")["default"] == "none"


@pytest.mark.parametrize(
    ("action", "effects"),
    [
        ("version", {"remote": "none", "local": "none"}),
        ("doctor", {"remote": "read", "local": "none"}),
        ("datasource.test", {"remote": "read", "local": "none"}),
        ("access-token.generate", {"remote": "read", "local": "none"}),
        ("alert-plugin.test", {"remote": "write", "local": "none"}),
        ("resource.download", {"remote": "read", "local": "file"}),
        (
            "workflow.create",
            {
                "remote": "write",
                "local": "none",
                "dry_run": {"remote": "read", "local": "none"},
            },
        ),
    ],
)
def test_schema_projects_reviewed_command_effects(
    action: str,
    effects: dict[str, object],
) -> None:
    data = _require_dict(get_schema_result(command_action=action).data)
    command = _require_dict(data["command"])

    assert command["effects"] == effects
    assert "mutates" not in command
    assert "mutation_target" not in command
    assert "remote_requests" not in command


def test_schema_command_rows_include_effects() -> None:
    data = _require_dict(
        get_schema_result(command_action="workflow.create", full=True).data
    )
    rows = [_require_dict(item) for item in _require_list(data["rows"])]

    assert {"kind": "effects", "name": "remote", "value": "write"} in rows
    assert {"kind": "effects", "name": "local", "value": "none"} in rows
    assert {
        "kind": "effects",
        "name": "dry_run",
        "value": "{'remote': 'read', 'local': 'none'}",
    } in rows


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


@pytest.mark.parametrize(
    "ds_version", ["1.3.9", "2.0.0", "2.0.1", "2.0.2", "2.0.3", "3.4.1"]
)
def test_instance_edit_schema_reports_exact_sync_and_publication_constraints(
    tmp_path: Path,
    ds_version: str,
) -> None:
    env_file = tmp_path / "profile.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
    data = _require_dict(
        get_schema_result(
            env_file=str(env_file), command_action="workflow-instance.edit"
        ).data
    )
    command = _require_dict(data["command"])
    constraints = _require_dict(command["native_edit_constraints"])
    required = ds_version in {"2.0.0", "2.0.1", "2.0.2"}
    assert constraints["dag_changes_require_sync_definition"] is required
    if required:
        assert constraints["sync_definition_requires_online"] is True
        assert (
            constraints["applies_when"] == "--sync-definition with persistent changes"
        )
        assert (
            constraints["publication_condition"] == "native definition metadata differs"
        )
