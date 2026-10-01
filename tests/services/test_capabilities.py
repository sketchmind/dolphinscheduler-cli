import pytest

from dsctl import __version__
from dsctl.cli_surface import SURFACE_PLANES
from dsctl.errors import UserInputError
from dsctl.models import supported_typed_task_types
from dsctl.services import capabilities as capabilities_service
from dsctl.services.capabilities import get_capabilities_result
from dsctl.services.datasource_payload import datasource_template_index_data
from dsctl.services.schema import get_schema_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import (
    parameter_syntax_index_data,
    supported_task_template_types,
    task_template_metadata,
)
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_version_support,
    supported_version_metadata,
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)

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
EXPECTED_DATASOURCE_TEMPLATE_INDEX = datasource_template_index_data()
EXPECTED_VERSION_METADATA = list(supported_version_metadata())
EXPECTED_DS_CAPABILITIES = {
    "current_version": "3.4.1",
    "selected_version": "3.4.1",
    "contract_version": "3.4.1",
    "family": "workflow-3.3-plus",
    "support_level": "full",
    "tested": True,
    "supported_version_count": len(SUPPORTED_VERSIONS),
    "supported_versions": list(SUPPORTED_VERSIONS),
    "versions": EXPECTED_VERSION_METADATA,
    "catalog": get_version_support("3.4.1").catalog.summary_metadata(),
}


def _require_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def test_capabilities_full_result_describes_current_stable_surface() -> None:
    result = get_capabilities_result(full=True)
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"capabilities": {"view": "full"}}
    assert data["cli"] == {"name": "dsctl", "version": __version__}
    assert data["ds"] == EXPECTED_DS_CAPABILITIES
    action_catalog = data["action_catalog"]
    assert isinstance(action_catalog, list)
    assert len(action_catalog) == 181
    schema_capability = next(
        item
        for item in action_catalog
        if isinstance(item, dict) and item.get("action") == "schema"
    )
    assert schema_capability == {
        "action": "schema",
        "availability": "supported",
        "verification": "static",
    }
    assert data["selection"] == {
        "precedence": ["flag", "context"],
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
        "confirmation_retry_option": "--confirm-risk",
    }
    assert data["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
        "command_invocation_source": "schema",
        "capabilities_scope": "feature_discovery",
        "surface_inventory_scope": "installed_cli_surface",
        "action_availability_command_pattern": ("dsctl capabilities --action ACTION"),
    }
    assert data["surface"] == {
        "inventory_scope": "installed_cli_surface",
        "selected_version_availability_source": "action_catalog",
        "action_availability_command_pattern": "dsctl capabilities --action ACTION",
    }
    assert data["resources"]["top_level"] == [
        "version",
        "doctor",
        "schema",
        "capabilities",
    ]
    assert data["resources"]["groups"]["context"]["commands"] == [
        "list",
        "get",
        "create",
        "update",
        "delete",
    ]
    assert data["resources"]["groups"]["config"]["commands"] == [
        "get",
        "set",
        "unset",
    ]
    assert data["resources"]["groups"]["enum"]["commands"] == ["names", "list"]
    assert data["resources"]["groups"]["lint"]["commands"] == [
        "workflow",
        "workflow-patch",
        "workflow-instance-patch",
    ]
    assert data["resources"]["groups"]["task-type"]["commands"] == [
        "list",
        "get",
        "schema",
    ]
    assert data["resources"]["groups"]["template"]["commands"] == [
        "workflow",
        "workflow-patch",
        "workflow-instance-patch",
        "params",
        "environment",
        "cluster",
        "datasource",
        "task",
    ]
    assert data["resources"]["groups"]["environment"]["commands"] == [
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
    assert data["resources"]["groups"]["workflow-instance"]["commands"] == [
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
    assert data["authoring"] == {
        "installed_cli_inventory": {
            "parameter_syntax": EXPECTED_PARAMETER_SYNTAX,
            "task_authoring_schema_command_pattern": "dsctl task-type schema TYPE",
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
    }
    assert data["schedule"] == {
        "preview": True,
        "explain": True,
        "environment_inheritance": True,
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
                "export",
                "parent",
                "digest",
                "edit",
                "watch",
                "stop",
                "rerun",
                "recover-failed",
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
    assert data["cli"] == {"name": "dsctl", "version": __version__}
    assert data["ds"] == {
        key: value
        for key, value in EXPECTED_DS_CAPABILITIES.items()
        if key not in {"versions", "supported_versions"}
    }
    assert "resources" in data
    assert "runtime" in data
    assert "authoring" in data
    authoring = data["authoring"]
    assert isinstance(authoring, dict)
    availability = _require_dict(authoring["selected_version_availability"])
    inventory = _require_dict(authoring["installed_cli_inventory"])
    assert availability["workflow_yaml_create"] is True
    assert inventory["task_template_types"] == EXPECTED_TEMPLATE_TASK_TYPES
    assert "parameter_syntax" not in inventory
    assert "task_templates" not in inventory


def test_capabilities_result_defaults_to_summary() -> None:
    default_result = get_capabilities_result()
    explicit_result = get_capabilities_result(summary=True)

    assert default_result == explicit_result


@pytest.mark.parametrize(
    "query",
    [
        *(
            pytest.param({"section": section}, id=f"section-{section}")
            for section in capabilities_service.CAPABILITIES_SECTION_CHOICES
            if section != "enums"
        ),
        pytest.param({"action": "workflow.create"}, id="action"),
    ],
)
def test_non_enum_capability_views_do_not_load_the_enum_registry(
    monkeypatch: pytest.MonkeyPatch,
    query: dict[str, object],
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.2.2")

    def fail_enum_registry(*, ds_version: str | None = None) -> dict[str, object]:
        del ds_version
        message = "unrequested enum registry must stay lazy"
        raise AssertionError(message)

    monkeypatch.setattr(
        capabilities_service,
        "enum_capabilities_data",
        fail_enum_registry,
    )

    section = query.get("section")
    action = query.get("action")
    result = get_capabilities_result(
        summary=query.get("summary") is True,
        section=section if isinstance(section, str) else None,
        full=query.get("full") is True,
        action=action if isinstance(action, str) else None,
    )
    data = _require_dict(result.data)

    assert _require_dict(data["ds"])["selected_version"] == "3.2.2"


@pytest.mark.parametrize(
    ("ds_version", "expected_types"),
    [
        ("1.3.9", ["master", "worker"]),
        ("3.2.1", ["master", "worker"]),
        ("3.2.2", ["master", "worker", "alert-server"]),
        ("3.4.1", ["master", "worker", "alert-server"]),
    ],
)
@pytest.mark.parametrize("view", ["summary", "section", "full"])
def test_monitor_discovery_matches_exact_schema_choices(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
    expected_types: list[str],
    view: str,
) -> None:
    monkeypatch.setenv("DS_VERSION", ds_version)
    capabilities = get_capabilities_result(
        section="monitor" if view == "section" else None,
        full=view == "full",
    )
    monitor = _require_dict(_require_dict(capabilities.data)["monitor"])
    assert monitor["server_types"] == expected_types

    schema = _require_dict(get_schema_result(command_action="monitor.server").data)
    arguments = _require_dict(schema["command"])["arguments"]
    assert isinstance(arguments, list)
    assert _require_dict(arguments[0])["choices"] == expected_types


def test_legacy_profile_capabilities_advertise_only_supported_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    catalog = get_task_authoring_catalog("1.3.9")
    exact_task_types = list(supported_task_template_types(catalog=catalog))

    authoring = _require_dict(
        _require_dict(get_capabilities_result(section="authoring").data)["authoring"]
    )
    availability = _require_dict(authoring["selected_version_availability"])
    inventory = _require_dict(authoring["installed_cli_inventory"])
    assert availability["workflow_yaml_create"] is True
    assert availability["workflow_yaml_export"] is True
    assert availability["workflow_yaml_lint"] is True
    assert availability["task_authoring_schema"] is True
    assert availability["datasource_payload_templates"] is True
    assert availability["environment_config_template"] is False
    assert availability["cluster_config_template"] is False
    assert (
        inventory["datasource_template_types"]
        == datasource_template_index_data(version="1.3.9")["supported_types"]
    )
    assert inventory["task_template_types"] == exact_task_types
    assert inventory["task_templates"] == task_template_metadata(catalog=catalog)
    assert inventory["parameter_syntax"] == EXPECTED_PARAMETER_SYNTAX
    expected_upstream_types = [
        "SUB_PROCESS" if task_type == "SUB_WORKFLOW" else task_type
        for task_type in exact_task_types
    ]
    assert inventory["upstream_default_task_types"] == expected_upstream_types

    monitor = _require_dict(
        _require_dict(get_capabilities_result(section="monitor").data)["monitor"]
    )
    assert monitor == {
        "health": False,
        "database": True,
        "server_types": ["master", "worker"],
    }

    runtime = _require_dict(
        _require_dict(get_capabilities_result(section="runtime").data)["runtime"]
    )
    assert _require_dict(runtime["audit"])["commands"] == []
    assert _require_dict(runtime["workflow-instance"])["commands"]
    assert _require_dict(runtime["task-instance"])["commands"]

    enums = _require_dict(
        _require_dict(get_capabilities_result(section="enums").data)["enums"]
    )
    assert enums["discovery"] is True
    enum_names = enums["names"]
    assert isinstance(enum_names, list)
    assert "priority" in enum_names


def test_legacy_authoring_capabilities_separate_cli_alias_from_native_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "2.0.0")

    authoring = _require_dict(
        _require_dict(get_capabilities_result(section="authoring").data)["authoring"]
    )
    inventory = _require_dict(authoring["installed_cli_inventory"])

    template_types = inventory["task_template_types"]
    upstream_types = inventory["upstream_default_task_types"]
    untemplated_types = inventory["untemplated_upstream_task_types"]
    assert isinstance(template_types, list)
    assert isinstance(upstream_types, list)
    assert isinstance(untemplated_types, list)
    assert "SUB_WORKFLOW" in template_types
    assert "SUB_PROCESS" not in template_types
    assert "SUB_PROCESS" in upstream_types
    assert "SUB_WORKFLOW" not in upstream_types
    assert "SUB_PROCESS" not in untemplated_types


def test_legacy_profile_exact_actions_distinguish_supported_and_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")

    read = _require_dict(get_capabilities_result(action="workflow.get").data)
    write = _require_dict(get_capabilities_result(action="workflow.create").data)
    enums = _require_dict(get_capabilities_result(action="enum.names").data)
    task_types = _require_dict(get_capabilities_result(action="task-type.list").data)

    assert read["capability"] == {
        "action": "workflow.get",
        "availability": "supported",
        "verification": "live_smoke",
    }
    assert write["capability"] == {
        "action": "workflow.create",
        "availability": "supported",
        "verification": "contract_tested",
    }
    assert _require_dict(enums["capability"])["availability"] == "supported"
    assert _require_dict(task_types["capability"])["availability"] == "unsupported"


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
    assert set(data) == {"cli", "ds", "surface", "self_description", "authoring"}
    authoring = data["authoring"]
    assert isinstance(authoring, dict)
    inventory = _require_dict(authoring["installed_cli_inventory"])
    assert inventory["parameter_syntax"] == EXPECTED_PARAMETER_SYNTAX
    assert inventory["task_templates"] == EXPECTED_TASK_TEMPLATE_METADATA


def test_capabilities_result_can_return_one_exact_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.2")

    result = get_capabilities_result(action="workflow.get")

    assert result.resolved == {
        "capabilities": {"view": "action", "action": "workflow.get"}
    }
    assert result.data == {
        "cli": {"name": "dsctl", "version": __version__},
        "ds": {
            "selected_version": "3.4.2",
            "contract_version": "3.4.2",
            "family": "workflow-3.3-plus",
            "support_level": "experimental",
            "tested": False,
        },
        "capability": {
            "action": "workflow.get",
            "availability": "supported",
            "verification": "live_smoke",
        },
        "links": [
            {"rel": "schema", "command": "dsctl schema --command workflow.get"},
            {"rel": "help", "command": "dsctl workflow get --help"},
        ],
    }


def test_full_capabilities_expands_selected_version_action_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.2")

    data = get_capabilities_result(full=True).data
    assert isinstance(data, dict)
    action_catalog = data["action_catalog"]
    assert isinstance(action_catalog, list)
    by_action = {
        item["action"]: item for item in action_catalog if isinstance(item, dict)
    }

    assert by_action["project.list"]["availability"] == "supported"
    assert by_action["project.list"]["verification"] == "live_smoke"
    assert by_action["workflow.create"] == {
        "action": "workflow.create",
        "availability": "supported",
        "verification": "contract_tested",
    }


@pytest.mark.parametrize(
    ("summary", "section", "full", "action"),
    [
        (True, "runtime", False, None),
        (True, None, True, None),
        (False, "runtime", True, None),
        (False, None, True, "workflow.get"),
    ],
)
def test_capabilities_result_rejects_conflicting_scope_options(
    *,
    summary: bool,
    section: str | None,
    full: bool,
    action: str | None,
) -> None:
    with pytest.raises(UserInputError, match="mutually exclusive"):
        get_capabilities_result(
            summary=summary,
            section=section,
            full=full,
            action=action,
        )


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
