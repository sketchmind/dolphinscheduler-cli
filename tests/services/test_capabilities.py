import pytest

from dsctl.cli_surface import SURFACE_PLANES
from dsctl.errors import UserInputError
from dsctl.models import supported_typed_task_types
from dsctl.services.capabilities import get_capabilities_result
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.template import parameter_syntax_index_data, task_template_metadata
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
EXPECTED_DATASOURCE_TEMPLATE_INDEX = datasource_template_index_data()
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


def test_capabilities_result_describes_current_stable_surface() -> None:
    result = get_capabilities_result()
    data = result.data

    assert isinstance(data, dict)
    assert data["cli"] == {"name": "dsctl", "version": "0.1.0"}
    assert data["ds"] == EXPECTED_DS_CAPABILITIES
    assert data["selection"] == {
        "precedence": ["flag", "context"],
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
        "confirmation_retry_option": "--confirm-risk",
    }
    assert data["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
    }
    assert data["resources"]["top_level"] == [
        "version",
        "context",
        "doctor",
        "schema",
        "capabilities",
    ]
    assert data["resources"]["groups"]["use"]["commands"] == [
        "project",
        "workflow",
    ]
    assert data["resources"]["groups"]["enum"]["commands"] == ["list"]
    assert data["resources"]["groups"]["lint"]["commands"] == ["workflow"]
    assert data["resources"]["groups"]["task-type"]["commands"] == ["list"]
    assert data["resources"]["groups"]["template"]["commands"] == [
        "workflow",
        "params",
        "datasource",
        "task",
    ]
    assert data["resources"]["groups"]["env"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["cluster"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["datasource"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "test",
    ]
    assert data["resources"]["groups"]["namespace"]["commands"] == [
        "list",
        "get",
        "available",
        "create",
        "delete",
    ]
    assert data["resources"]["groups"]["resource"]["commands"] == [
        "list",
        "view",
        "upload",
        "create",
        "mkdir",
        "download",
        "delete",
    ]
    assert data["resources"]["groups"]["queue"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["worker-group"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["task-group"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "close",
        "start",
        "queue",
    ]
    assert data["resources"]["groups"]["alert-plugin"]["commands"] == [
        "list",
        "get",
        "schema",
        "create",
        "update",
        "delete",
        "test",
        "definition",
    ]
    assert data["resources"]["groups"]["alert-group"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["tenant"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["user"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "grant",
        "revoke",
    ]
    assert data["resources"]["groups"]["access-token"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
        "generate",
    ]
    assert data["resources"]["groups"]["monitor"]["commands"] == [
        "health",
        "server",
        "database",
    ]
    assert data["resources"]["groups"]["audit"]["commands"] == [
        "list",
        "model-types",
        "operation-types",
    ]
    assert data["resources"]["groups"]["schedule"]["commands"] == [
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
    assert data["resources"]["groups"]["project-parameter"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["project-preference"]["commands"] == [
        "get",
        "update",
        "enable",
        "disable",
    ]
    assert data["resources"]["groups"]["project-worker-group"]["commands"] == [
        "list",
        "set",
        "clear",
    ]
    assert data["resources"]["groups"]["workflow"]["commands"] == [
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
    assert data["resources"]["groups"]["workflow-instance"]["commands"] == [
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
    assert data["authoring"] == {
        "workflow_yaml_create": True,
        "workflow_yaml_export": True,
        "workflow_yaml_lint": True,
        "workflow_digest": True,
        "workflow_schedule_block": True,
        "workflow_dry_run": True,
        "parameter_syntax": EXPECTED_PARAMETER_SYNTAX,
        "datasource_payload_templates": True,
        "datasource_template_types": EXPECTED_DATASOURCE_TEMPLATE_INDEX[
            "supported_types"
        ],
        "task_template_types": EXPECTED_TEMPLATE_TASK_TYPES,
        "task_templates": EXPECTED_TASK_TEMPLATE_METADATA,
        "typed_task_specs": EXPECTED_TYPED_TASK_TYPES,
        "generic_task_template_types": EXPECTED_GENERIC_TEMPLATE_TASK_TYPES,
        "logic_task_types": [
            "SUB_WORKFLOW",
            "DEPENDENT",
            "CONDITIONS",
            "SWITCH",
        ],
        "upstream_default_task_types": EXPECTED_UPSTREAM_TASK_TYPES,
        "upstream_default_task_types_by_category": (
            EXPECTED_UPSTREAM_TASK_TYPES_BY_CATEGORY
        ),
        "untemplated_upstream_task_types": EXPECTED_UNTEMPLATED_UPSTREAM_TASK_TYPES,
    }
    assert data["schedule"] == {
        "preview": True,
        "explain": True,
        "risk_confirmation": True,
        "online_offline_lifecycle": True,
    }
    assert data["monitor"] == {
        "health": True,
        "database": True,
        "server_types": ["master", "worker", "alert-server"],
    }
    assert data["enums"]["discovery"] is True
    assert "priority" in data["enums"]["names"]
    assert "resource-type" in data["enums"]["names"]
    assert data["runtime"] == {
        "audit": {
            "commands": [
                "list",
                "model-types",
                "operation-types",
            ]
        },
        "workflow-instance": {
            "commands": [
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
        },
        "task-instance": {
            "commands": [
                "list",
                "get",
                "watch",
                "sub-workflow",
                "log",
                "force-success",
                "savepoint",
                "stop",
            ]
        },
    }
    assert data["planes"] == {
        name: list(resources) for name, resources in SURFACE_PLANES.items()
    }


def test_capabilities_result_can_return_summary() -> None:
    result = get_capabilities_result(summary=True)
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"capabilities": {"view": "summary"}}
    assert data["cli"] == {"name": "dsctl", "version": "0.1.0"}
    assert data["ds"] == EXPECTED_DS_CAPABILITIES
    assert "resources" in data
    assert "runtime" in data
    assert "authoring" in data
    authoring = data["authoring"]
    assert isinstance(authoring, dict)
    assert authoring["workflow_yaml_create"] is True
    assert authoring["task_template_types"] == EXPECTED_TEMPLATE_TASK_TYPES
    assert "parameter_syntax" not in authoring
    assert "task_templates" not in authoring


def test_capabilities_result_can_return_one_section() -> None:
    result = get_capabilities_result(section="authoring")
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {
        "capabilities": {
            "view": "section",
            "section": "authoring",
        }
    }
    assert set(data) == {"cli", "ds", "self_description", "authoring"}
    authoring = data["authoring"]
    assert isinstance(authoring, dict)
    assert authoring["parameter_syntax"] == EXPECTED_PARAMETER_SYNTAX
    assert authoring["task_templates"] == EXPECTED_TASK_TEMPLATE_METADATA


def test_capabilities_result_rejects_conflicting_scope_options() -> None:
    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_capabilities_result(summary=True, section="runtime")


def test_capabilities_result_rejects_unknown_section() -> None:
    with pytest.raises(
        UserInputError,
        match="Unknown capabilities section",
    ) as exc_info:
        get_capabilities_result(section="missing")

    assert exc_info.value.details["section"] == "missing"
    available_sections = exc_info.value.details["available_sections"]
    assert isinstance(available_sections, list)
    assert "authoring" in available_sections
