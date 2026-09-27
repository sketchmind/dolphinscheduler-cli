import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.models import WorkflowPatchDocument, WorkflowSpec, supported_typed_task_types
from dsctl.services._task_templates import task_template_variants
from dsctl.services._workflow import compile as workflow_compile_service
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import (
    ParameterSyntaxIndexData,
    cluster_config_template_result,
    datasource_template_result,
    environment_config_template_result,
    generic_task_template_types,
    parameter_syntax_data,
    parameter_syntax_result,
    supported_datasource_types,
    supported_task_template_types,
    task_template_metadata,
    task_template_result,
    task_template_types_result,
    typed_task_template_types,
    workflow_instance_patch_template_result,
    workflow_patch_template_result,
    workflow_template_result,
)
from dsctl.upstream import upstream_default_task_types
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph
from dsctl.upstream.task_parameter_projection import TaskResourceRefIndex

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


# Independent visible FILE snapshots for the stable exact storage-path epoch.
_TEMPLATE_FILE_REFS = TaskResourceRefIndex.from_resolved_files(
    [
        "/jobs/daily-orders.jar",
        "/jobs/wordcount.jar",
        "/scripts/job.sh",
        "/scripts/job.py",
    ],
    id_by_full_name={},
    wire_full_name_by_full_name={
        "/jobs/daily-orders.jar": "/tenant/resources/jobs/daily-orders.jar",
        "/jobs/wordcount.jar": "/tenant/resources/jobs/wordcount.jar",
        "/scripts/job.sh": "/tenant/resources/scripts/job.sh",
        "/scripts/job.py": "/tenant/resources/scripts/job.py",
    },
)


def test_workflow_template_result_returns_valid_yaml_document() -> None:
    result = workflow_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved["with_schedule"] is False
    assert result.resolved["ds_version"] == "3.4.1"
    assert data["artifact"] == {
        "kind": "workflow-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow --raw",
        "target_command_pattern": "dsctl workflow create --file FILE",
    }
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    assert yaml_text.startswith("# Workflow YAML")
    document = yaml.safe_load(yaml_text)

    assert "Task names must be unique" in yaml_text
    assert "task-only parameters belong in task_params.localParams" in yaml_text
    assert "dsctl schema --command workflow.create" in yaml_text
    assert "# timeout: 0  # Minutes" in yaml_text
    assert "# execution_type: PARALLEL" in yaml_text
    assert document["workflow"]["name"] == "example-workflow"
    assert "execution_type" not in document["workflow"]
    assert WorkflowSpec.model_validate(document).workflow.execution_type == "PARALLEL"
    assert document["workflow"]["release_state"] == "OFFLINE"
    assert len(document["tasks"]) == 2
    assert document["tasks"][1]["depends_on"] == ["extract"]
    assert "schedule" not in document


def test_workflow_template_result_can_include_schedule_block() -> None:
    result = workflow_template_result(with_schedule=True)
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved["with_schedule"] is True
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)

    assert data["artifact"]["raw_command"] == (
        "dsctl template workflow --with-schedule --raw"
    )
    assert document["workflow"]["release_state"] == "ONLINE"
    assert document["schedule"]["cron"] == "0 0 2 * * ?"
    assert document["schedule"]["enabled"] is False


def test_workflow_patch_template_result_returns_valid_patch_document() -> None:
    result = workflow_patch_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"template": "workflow.patch"}
    assert data["artifact"] == {
        "kind": "workflow-patch-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow-patch --raw",
        "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    patch = WorkflowPatchDocument.model_validate(document).patch

    assert yaml_text.startswith("# Workflow patch YAML")
    assert patch.workflow is not None
    assert patch.workflow.set.description == "Updated workflow description"
    assert patch.workflow.set.timeout is None
    assert patch.tasks is None
    assert "tasks.create" in data["rules"][2]
    assert "dsctl task-type schema TYPE" in data["related_command_patterns"]
    assert "related_commands" not in data


def test_workflow_instance_patch_template_result_uses_instance_safe_fields() -> None:
    result = workflow_instance_patch_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"template": "workflow-instance.patch"}
    assert data["artifact"] == {
        "kind": "workflow-instance-patch-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow-instance-patch --raw",
        "target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
            "--patch FILE"
        ),
    }
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    patch = WorkflowPatchDocument.model_validate(document).patch

    assert yaml_text.startswith("# Workflow-instance patch YAML")
    assert patch.workflow is None
    assert patch.tasks is not None
    assert patch.tasks.update[0].match.name == "failed-step"
    assert patch.tasks.update[0].set.model_fields_set == {"command"}
    assert "workflow-instance edit only accepts" in data["rules"][1]
    assert "related_commands" not in data


def test_parameter_syntax_result_describes_dynamic_parameter_shape() -> None:
    result = parameter_syntax_result()
    data = result.data

    assert isinstance(data, dict)
    assert data == parameter_syntax_data()
    index_data = cast("ParameterSyntaxIndexData", data)
    assert index_data["default_topic"] == "overview"
    assert "time" in [item["topic"] for item in index_data["topics"]]
    template_variants = result.resolved["template_variants"]
    assert isinstance(template_variants, list)
    assert "SHELL" in template_variants


def test_parameter_syntax_result_can_expand_specific_topics() -> None:
    property_result = parameter_syntax_result(topic="property")
    time_result = parameter_syntax_result(topic="time")
    output_result = parameter_syntax_result(topic="output")

    assert property_result.resolved["topic"] == "property"
    property_data = property_result.data
    assert isinstance(property_data, dict)
    property_details = property_data["details"]
    assert isinstance(property_details, dict)
    assert property_details["direct_values"] == ["IN", "OUT"]
    assert "VARCHAR" in property_details["type_values"]
    property_document = yaml.safe_load(property_details["yaml"])
    assert property_document["workflow"]["global_params"]["bizdate"] == (
        "${system.biz.date}"
    )

    time_data = time_result.data
    assert isinstance(time_data, dict)
    time_details = time_data["details"]
    assert isinstance(time_details, dict)
    assert "$[yyyyMMdd-1]" in time_details["examples"]
    assert any("YYYY" in caution for caution in time_details["cautions"])
    time_document = yaml.safe_load(time_details["yaml"])
    assert time_document["workflow"]["global_params"]["bizdate"] == "$[yyyyMMdd-1]"

    output_data = output_result.data
    assert isinstance(output_data, dict)
    output_details = output_data["details"]
    assert isinstance(output_details, dict)
    assert "${setValue(name=value)}" in [
        item["syntax"] for item in output_details["output_syntax"]
    ]


@pytest.mark.parametrize(
    ("ds_version", "expected_syntaxes", "expected_task_types", "placement"),
    [
        ("1.3.9", [], [], "unsupported"),
        (
            "2.0.0",
            ["${setValue(name=value)}", "result column named like an OUT prop"],
            ["SHELL", "PYTHON"],
            "beginning of a task log line",
        ),
        (
            "3.2.0",
            [
                "${setValue(name=value)}",
                "#{setValue(name=value)}",
                "result column named like an OUT prop",
            ],
            ["SHELL", "PYTHON", "REMOTESHELL"],
            "beginning of a task log line",
        ),
        (
            "3.2.1",
            [
                "${setValue(name=value)}",
                "#{setValue(name=value)}",
                "result column named like an OUT prop",
            ],
            ["SHELL", "PYTHON", "REMOTESHELL"],
            "anywhere in a task log line",
        ),
    ],
)
def test_parameter_output_template_tracks_exact_parser_epoch(
    tmp_path: Path,
    ds_version: str,
    expected_syntaxes: list[str],
    expected_task_types: list[str],
    placement: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    result = parameter_syntax_result(topic="output", env_file=str(env_file))
    data = cast("dict[str, object]", result.data)
    details = cast("dict[str, object]", data["details"])
    output_syntax = cast("list[dict[str, object]]", details["output_syntax"])

    assert [item["syntax"] for item in output_syntax] == expected_syntaxes
    log_syntax = [
        item for item in output_syntax if str(item["syntax"]).endswith("(name=value)}")
    ]
    if placement == "unsupported":
        assert details["examples"] == {}
        assert any(
            "no varPool-backed task output publication" in rule
            for rule in cast("list[str]", details["rules"])
        )
    else:
        assert all(item["task_types"] == expected_task_types for item in log_syntax)
        assert all(placement in str(item["description"]) for item in log_syntax)


@pytest.mark.parametrize(
    ("ds_version", "present", "absent"),
    [
        ("1.3.9", "BOOLEAN", "LIST"),
        ("2.0.0", "LIST", "FILE"),
        ("3.4.2", "FILE", "missing"),
    ],
)
def test_parameter_template_uses_selected_exact_data_types(
    tmp_path: Path,
    ds_version: str,
    present: str,
    absent: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    result = parameter_syntax_result(topic="property", env_file=str(env_file))
    data = cast("dict[str, object]", result.data)
    details = cast("dict[str, object]", data["details"])
    values = cast("list[str]", details["type_values"])

    assert result.resolved["ds_version"] == ds_version
    assert present in values
    assert absent not in values


def test_parameter_context_matches_ds_341_runtime_precedence() -> None:
    result = parameter_syntax_result(topic="context")
    data = result.data

    assert isinstance(data, dict)
    details = data["details"]
    assert isinstance(details, dict)
    assert details["priority"] == [
        "Upstream Output / VarPool",
        "Startup Parameter",
        "Local Parameter",
        "Global Parameter",
        "Project Parameter",
        "Built-in Parameter",
    ]
    assert any(
        "SUB_WORKFLOW localParams do not become child inputs" in rule
        for rule in details["rules"]
    )
    assert any(
        "parent workflow globals, startup parameters, and the parent "
        "workflow-instance varPool" in rule
        for rule in details["rules"]
    )


def test_139_parameter_context_links_name_selector_to_native_identity() -> None:
    data = cast(
        "dict[str, object]",
        parameter_syntax_data("context", ds_version="1.3.9"),
    )
    details = cast("dict[str, object]", data["details"])
    rules = cast("list[str]", details["rules"])

    assert any(
        "SUB_WORKFLOW childWorkflowName" in rule
        and "native SUB_PROCESS processDefinitionId" in rule
        for rule in rules
    )
    assert any(
        "SUB_WORKFLOW does not author task localParams" in rule for rule in rules
    )


@pytest.mark.parametrize(
    (
        "ds_version",
        "priority",
        "expected_scopes",
        "downstream_rule",
        "subflow_rule",
        "absent_subflow_text",
    ),
    [
        (
            "1.3.9",
            ["Local Parameter", "Global Parameter", "Built-in Parameter"],
            {"workflow.global_params", "task_params.localParams"},
            None,
            (
                "SUB_PROCESS child defaults are filled by same-name parent "
                "workflow globals"
            ),
            "startup parameters",
        ),
        (
            "2.0.0",
            [
                "Local Parameter",
                "Startup Parameter",
                "Global Parameter",
                "Upstream Output / VarPool",
                "Built-in Parameter",
            ],
            {
                "workflow.global_params",
                "task_params.localParams",
                "startup",
                "upstream_output",
            },
            None,
            (
                "SUB_PROCESS child globals are overridden by same-name parent "
                "workflow globals"
            ),
            "workflow-instance varPool",
        ),
        (
            "3.2.0",
            [
                "Local Parameter",
                "Upstream Output / VarPool",
                "Startup Parameter",
                "Global Parameter",
                "Project Parameter",
                "Built-in Parameter",
            ],
            {
                "workflow.global_params",
                "task_params.localParams",
                "startup",
                "project",
                "upstream_output",
            },
            None,
            (
                "SUB_PROCESS receives parent workflow globals and the parent "
                "workflow-instance varPool"
            ),
            "SUB_WORKFLOW localParams",
        ),
        (
            "3.3.1",
            [
                "Upstream Output / VarPool",
                "Startup Parameter",
                "Local Parameter",
                "Global Parameter",
                "Project Parameter",
                "Built-in Parameter",
            ],
            {
                "workflow.global_params",
                "task_params.localParams",
                "startup",
                "project",
                "upstream_output",
            },
            "same prop",
            (
                "SUB_WORKFLOW receives only parent startup parameters in "
                "DolphinScheduler 3.3.1"
            ),
            "parent workflow-instance varPool",
        ),
        (
            "3.4.0",
            [
                "Upstream Output / VarPool",
                "Startup Parameter",
                "Local Parameter",
                "Global Parameter",
                "Project Parameter",
                "Built-in Parameter",
            ],
            {
                "workflow.global_params",
                "task_params.localParams",
                "startup",
                "project",
                "upstream_output",
            },
            "same prop",
            (
                "SUB_WORKFLOW receives parent workflow globals, startup parameters, "
                "and the parent workflow-instance varPool"
            ),
            "DolphinScheduler 3.3.1",
        ),
    ],
)
def test_parameter_context_template_tracks_exact_runtime_epoch(
    tmp_path: Path,
    ds_version: str,
    priority: list[str],
    expected_scopes: set[str],
    downstream_rule: str | None,
    subflow_rule: str,
    absent_subflow_text: str,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    result = parameter_syntax_result(topic="context", env_file=str(env_file))
    data = cast("dict[str, object]", result.data)
    details = cast("dict[str, object]", data["details"])
    scopes = cast("dict[str, str]", details["scopes"])
    rules = cast("list[str]", details["rules"])

    assert details["priority"] == priority
    assert set(scopes) == expected_scopes
    if downstream_rule is None:
        assert all("declare an IN" not in rule for rule in rules)
    else:
        assert any(
            downstream_rule in rule and "declare an IN" in rule for rule in rules
        )
    assert any(subflow_rule in rule for rule in rules)
    assert all(absent_subflow_text not in rule for rule in rules)


def test_datasource_template_result_returns_discovery_without_type() -> None:
    result = datasource_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"view": "list"}
    assert data["default_type"] == "MYSQL"
    assert (
        data["template_command"]
        == "dsctl template datasource --ds-version 3.4.1 --type MYSQL"
    )
    assert (
        data["template_command_pattern"]
        == "dsctl template datasource --ds-version 3.4.1 --type TYPE"
    )
    assert data["target_command_patterns"] == [
        "dsctl datasource create --file FILE",
        "dsctl datasource update DATASOURCE --file FILE",
    ]
    assert data["type_discovery_command"] == (
        "dsctl template datasource --ds-version 3.4.1"
    )
    assert data["supported_types"] == list(supported_datasource_types())
    assert {
        "type": "MYSQL",
        "template_command": "dsctl template datasource --ds-version 3.4.1 --type MYSQL",
    } in data["rows"]
    assert "fields" not in data
    assert "rules" not in data


def test_environment_config_template_result_returns_shell_template() -> None:
    result = environment_config_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"template": "environment.config"}
    assert data["filename"] == "env.sh"
    assert "export JAVA_HOME=/opt/java" in data["config"]
    assert data["target_command_patterns"] == [
        "dsctl environment create --name NAME --config-file env.sh",
        "dsctl environment update ENVIRONMENT --config-file env.sh",
    ]
    assert data["source_options"] == [
        "--config CONFIG",
        "--config-file CONFIG_FILE",
    ]
    lines = data["lines"]
    assert isinstance(lines, list)
    assert lines[0]["line"] == "export JAVA_HOME=/opt/java"


def test_cluster_config_template_result_returns_json_template() -> None:
    result = cluster_config_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"template": "cluster.config"}
    assert data["filename"] == "cluster-config.json"
    assert data["target_command_patterns"] == [
        "dsctl cluster create --name NAME --config-file cluster-config.json",
        "dsctl cluster update CLUSTER --config-file cluster-config.json",
    ]
    assert data["source_options"] == [
        "--config CONFIG",
        "--config-file CONFIG_FILE",
    ]
    payload = data["payload"]
    assert isinstance(payload, dict)
    assert json.loads(data["config"]) == payload
    assert set(payload) == {"k8s", "yarn"}
    assert "apiVersion: v1" in payload["k8s"]
    assert data["rows"] == data["fields"]


def test_datasource_template_result_returns_json_payload_template() -> None:
    result = datasource_template_result("mysql")
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {
        "view": "template",
        "datasource_type": "MYSQL",
    }
    assert data["type"] == "MYSQL"
    assert data["target_command_patterns"] == [
        "dsctl datasource create --file FILE",
        "dsctl datasource update DATASOURCE --file FILE",
    ]
    assert data["source_option"] == "--file"
    payload = data["payload"]
    assert isinstance(payload, dict)
    assert payload == json.loads(data["json"])
    assert data["rows"] == data["fields"]
    assert payload["type"] == "MYSQL"
    assert payload["port"] == 3306
    assert payload["other"] == {"serverTimezone": "UTC"}
    assert "payload_schema" not in data
    type_fields = [
        field
        for field in data["fields"]
        if isinstance(field, dict) and field.get("name") == "type"
    ]
    assert type_fields
    assert "choices" not in type_fields[0]


def test_datasource_template_result_handles_type_specific_payload() -> None:
    result = datasource_template_result("k8s")
    data = result.data

    assert isinstance(data, dict)
    payload = data["payload"]
    assert isinstance(payload, dict)
    assert payload["type"] == "K8S"
    assert payload["kubeConfig"] == "change-me"
    assert payload["namespace"] == "default"
    assert "host" not in payload
    field_names = {
        field["name"]
        for field in data["fields"]
        if isinstance(field, dict) and isinstance(field.get("name"), str)
    }
    assert "kubeConfig" in field_names
    assert "namespace" in field_names


def test_datasource_template_result_rejects_unsupported_type() -> None:
    with pytest.raises(UserInputError, match="Unsupported datasource type"):
        datasource_template_result("UNKNOWN")


def test_datasource_template_result_selects_exact_legacy_contract() -> None:
    result = datasource_template_result(ds_version="1.3.9")
    data = result.data

    assert isinstance(data, dict)
    assert data["ds_version"] == "1.3.9"
    assert data["type_discovery_command"] == (
        "dsctl template datasource --ds-version 1.3.9"
    )
    assert data["template_command"] == (
        "dsctl template datasource --ds-version 1.3.9 --type MYSQL"
    )
    assert "PRESTO" not in data["supported_types"]
    assert "K8S" not in data["supported_types"]


def test_datasource_template_uses_selected_profile_without_an_override(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "legacy.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")

    result = datasource_template_result(env_file=str(env_file))
    data = cast("dict[str, object]", result.data)

    assert data["ds_version"] == "1.3.9"
    assert "K8S" not in cast("list[str]", data["supported_types"])


def test_datasource_template_result_selects_exact_ssh_key_field() -> None:
    legacy = datasource_template_result("ssh", ds_version="3.3.2")
    modern = datasource_template_result("ssh", ds_version="3.4.0")

    assert isinstance(legacy.data, dict)
    assert isinstance(modern.data, dict)
    assert "publicKey" in legacy.data["payload"]
    assert "privateKey" not in legacy.data["payload"]
    assert "privateKey" in modern.data["payload"]
    assert "publicKey" not in modern.data["payload"]


@pytest.mark.parametrize(
    ("task_type", "expected_key", "expected_kind", "expected_category"),
    [
        ("CONDITIONS", "task_params", "typed", "Logic"),
        ("shell", "command", "typed", "Universal"),
        ("PYTHON", "command", "typed", "Universal"),
        ("SUB_WORKFLOW", "task_params", "typed", "Logic"),
        ("DEPENDENT", "task_params", "typed", "Logic"),
        ("REMOTESHELL", "task_params", "typed", "Universal"),
        ("SQL", "task_params", "typed", "Universal"),
        ("SWITCH", "task_params", "typed", "Logic"),
        ("HTTP", "task_params", "typed", "Universal"),
        ("SPARK", "task_params", "typed", "Universal"),
    ],
)
def test_task_template_result_returns_valid_yaml_for_supported_types(
    task_type: str,
    expected_key: str,
    expected_kind: str,
    expected_category: str,
) -> None:
    result = task_template_result(task_type)
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved["task_type"] == task_type.upper()
    assert result.resolved["template_kind"] == expected_kind
    assert result.resolved["task_category"] == expected_category
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    assert any(line.startswith("# Task template") for line in yaml_text.splitlines())
    document = yaml.safe_load(yaml_text)

    assert "# Optional task runtime controls:" in yaml_text
    assert "# cpu_quota: 50" in yaml_text
    assert "# memory_max: 1024" in yaml_text
    assert document["type"] == task_type.upper()
    assert expected_key in document


@pytest.mark.parametrize("ds_version", ["3.4.1", "3.4.2"])
def test_sql_template_projects_the_selected_exact_profile_catalog(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    result = task_template_result(
        "SQL",
        variant="output",
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    yaml_text = result.data["yaml"]
    metadata = result.data["template"]
    assert isinstance(yaml_text, str)
    assert isinstance(metadata, dict)
    document = yaml.safe_load(yaml_text)

    assert metadata["variants"] == ["output"]
    assert document["type"] == "SQL"
    assert document["task_params"]["sql"].startswith("select count(*)")
    assert "sqlSource" not in document["task_params"]
    assert "sqlResource" not in document["task_params"]


def test_341_and_342_inline_sql_templates_are_semantically_identical() -> None:
    ds_341 = get_task_authoring_catalog("3.4.1")
    ds_342 = get_task_authoring_catalog("3.4.2")

    for variant in (None, "output"):
        result_341 = task_template_result(
            "SQL",
            variant=variant,
            catalog=ds_341,
        )
        result_342 = task_template_result(
            "SQL",
            variant=variant,
            catalog=ds_342,
        )
        assert isinstance(result_341.data, dict)
        assert isinstance(result_342.data, dict)

        assert yaml.safe_load(result_341.data["yaml"]) == yaml.safe_load(
            result_342.data["yaml"]
        )


@pytest.mark.parametrize(
    ("ds_version", "has_http_body"),
    [
        ("2.0.0", False),
        ("3.2.0", False),
        ("3.2.1", True),
        ("3.3.1", True),
        ("3.4.0", True),
    ],
)
def test_http_templates_project_exact_fields_and_normalize_as_typed_input(
    ds_version: str,
    *,
    has_http_body: bool,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    result = task_template_result("HTTP", catalog=catalog)
    data = cast("dict[str, object]", result.data)
    metadata = cast("dict[str, object]", data["template"])
    document = cast("dict[str, object]", yaml.safe_load(cast("str", data["yaml"])))
    task_params = cast("YamlObject", document["task_params"])

    assert ("post-json" in cast("list[str]", metadata["variants"])) is has_http_body
    assert ("httpBody" in task_params) is has_http_body
    assert "socketTimeout" not in task_params
    assert (
        catalog.normalize_task_params(
            "HTTP",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )["url"]
        == "https://example.test/health"
    )


@pytest.mark.parametrize(
    ("ds_version", "has_failure_control", "has_parameter_passing"),
    [
        ("1.3.9", False, False),
        ("2.0.0", False, False),
        ("2.0.9", False, False),
        ("3.0.0", False, False),
        ("3.0.6", False, False),
        ("3.1.0", False, False),
        ("3.1.9", False, False),
        ("3.2.0", True, False),
        ("3.2.1", True, True),
        ("3.2.2", True, True),
        ("3.3.1", True, True),
        ("3.3.2", True, True),
        ("3.4.0", True, True),
        ("3.4.1", True, True),
        ("3.4.2", True, True),
    ],
)
def test_dependent_templates_project_exact_fields_and_normalize_as_typed_input(
    ds_version: str,
    *,
    has_failure_control: bool,
    has_parameter_passing: bool,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    for variant in (None, "task-dependency"):
        result = task_template_result("DEPENDENT", variant=variant, catalog=catalog)
        data = cast("dict[str, object]", result.data)
        yaml_text = cast("str", data["yaml"])
        document = cast(
            "dict[str, object]",
            yaml.safe_load(yaml_text),
        )
        task_params = cast("YamlObject", document["task_params"])
        dependence = cast("dict[str, object]", task_params["dependence"])
        branch = cast(
            "dict[str, object]",
            cast("list[object]", dependence["dependTaskList"])[0],
        )
        item = cast(
            "dict[str, object]", cast("list[object]", branch["dependItemList"])[0]
        )

        assert ("checkInterval" in dependence) is has_failure_control
        assert ("failurePolicy" in dependence) is has_failure_control
        assert ("parameterPassing" in item) is has_parameter_passing
        assert yaml_text.count("\n            parameterPassing: false\n") == int(
            has_parameter_passing
        )
        assert "dependResult" not in item
        assert "resourceList" not in task_params
        assert "${" not in cast("str", data["yaml"])
        assert "localParams" not in task_params
        assert catalog.normalize_task_params(
            "DEPENDENT",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )["dependence"]


@pytest.mark.parametrize(
    ("ds_version", "inheritance_fragment"),
    [
        ("2.0.0", "parent workflow globals"),
        ("3.2.0", "parent workflow-instance varPool"),
        ("3.2.1", "parent workflow-instance varPool"),
        ("3.3.1", "only parent startup parameters"),
        ("3.4.0", "parent workflow globals, startup parameters"),
    ],
)
def test_sub_workflow_template_keeps_canonical_input_and_reuses_nested_semantics(
    ds_version: str,
    inheritance_fragment: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    result = task_template_result(
        "SUB_WORKFLOW",
        catalog=catalog,
    )
    data = cast("dict[str, object]", result.data)
    yaml_text = cast("str", data["yaml"])
    document = cast("dict[str, object]", yaml.safe_load(yaml_text))
    task_params = cast("YamlObject", document["task_params"])

    assert document["type"] == "SUB_WORKFLOW"
    assert "workflowDefinitionCode" in task_params
    assert "processDefinitionCode" not in task_params
    assert inheritance_fragment in yaml_text
    assert (
        catalog.normalize_task_params(
            "SUB_WORKFLOW",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )["workflowDefinitionCode"]
        == 1000000000001
    )


@pytest.mark.parametrize("variant", [None])
def test_139_sub_workflow_templates_use_only_same_project_name_identity(
    variant: str,
) -> None:
    catalog = get_task_authoring_catalog("1.3.9")

    result = task_template_result(
        "SUB_WORKFLOW",
        variant=variant,
        catalog=catalog,
    )
    data = cast("dict[str, object]", result.data)
    yaml_text = cast("str", data["yaml"])
    document = cast("dict[str, object]", yaml.safe_load(yaml_text))
    task_params = cast("YamlObject", document["task_params"])

    assert task_params == {"childWorkflowName": "child-daily"}
    assert "native SUB_PROCESS processDefinitionId" in yaml_text
    assert "same project" in yaml_text
    assert "workflowDefinitionCode" not in yaml_text
    assert "\n  localParams:" not in yaml_text
    assert "\n  resourceList:" not in yaml_text
    assert "\n  varPool:" not in yaml_text
    assert catalog.normalize_task_params(
        "SUB_WORKFLOW",
        task_params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    ) == {"childWorkflowName": "child-daily"}


def test_139_sub_workflow_params_template_teaches_exact_parameter_precedence() -> None:
    catalog = get_task_authoring_catalog("1.3.9")

    result = task_template_result(
        "SUB_WORKFLOW",
        catalog=catalog,
    )
    data = cast("dict[str, object]", result.data)
    yaml_text = cast("str", data["yaml"])

    assert (
        task_template_metadata(catalog=catalog)["SUB_WORKFLOW"]["parameter_fields"]
        == []
    )
    assert "parent values do not override an already" in (yaml_text)
    assert "declared child global" in yaml_text
    assert "does not author task localParams" in yaml_text


def test_task_template_result_rejects_unsupported_type() -> None:
    with pytest.raises(UserInputError, match="Unsupported task template type") as exc:
        task_template_result("SPARK_SQL")

    assert exc.value.details == {
        "task_type": "SPARK_SQL",
        "available_task_types_count": len(supported_task_template_types()),
        "discovery_command": "dsctl template task",
    }


def test_task_template_types_result_lists_supported_types() -> None:
    result = task_template_types_result()
    data = result.data
    supported = supported_task_template_types()

    assert isinstance(data, dict)
    assert result.resolved == {"mode": "index"}
    assert data["count"] == len(supported)
    assert data["task_types"] == list(supported)
    assert data["typed_task_types"] == list(typed_task_template_types())
    assert data["generic_task_types"] == list(generic_task_template_types())
    assert "LINKIS" not in supported
    assert "Universal" in data["task_types_by_category"]
    assert "Logic" in data["task_types_by_category"]
    assert data["rows"][0]["task_type"] == "SHELL"
    assert data["rows"][0]["variants"] == ["output", "resource"]
    assert data["rows"][0]["next_command"] == "dsctl task-type get SHELL"
    assert data["default_task_type"] == "SHELL"
    assert data["next_command"] == "dsctl task-type get SHELL"
    assert "task_templates" not in data


def test_task_template_types_match_typed_task_specs() -> None:
    assert set(upstream_default_task_types()) - set(
        supported_task_template_types()
    ) == {"LINKIS"}
    assert typed_task_template_types() == supported_typed_task_types()


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_task_template_discovery_uses_the_selected_exact_profile(
    ds_version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    monkeypatch.setenv("DS_VERSION", ds_version)

    result = task_template_types_result()
    data = cast("dict[str, object]", result.data)

    assert set(cast("list[str]", data["task_types"])) == set(
        catalog.authoring_task_types
    )
    assert set(cast("list[str]", data["typed_task_types"])) == set(
        catalog.reviewed_typed_task_types
    )
    assert set(cast("list[str]", data["generic_task_types"])) == (
        set(catalog.authoring_task_types) - set(catalog.reviewed_typed_task_types)
    )
    assert data["count"] == len(catalog.authoring_task_types)


def test_139_shell_templates_validate_and_compile_to_the_canonical_subset() -> None:
    catalog = get_task_authoring_catalog("1.3.9")

    for variant in ("minimal", "params"):
        result = task_template_result("SHELL", catalog=catalog)
        data = cast("dict[str, object]", result.data)
        document = cast(
            "dict[str, object]",
            yaml.safe_load(
                parameter_example_yaml("SHELL", "1.3.9")
                if variant == "params"
                else cast("str", data["yaml"])
            ),
        )
        assert not {
            "environment_code",
            "task_group_id",
            "task_group_priority",
            "delay",
            "cpu_quota",
            "memory_max",
        }.intersection(document)

        raw_params = document.get("task_params")
        if isinstance(raw_params, dict):
            normalized_params = catalog.normalize_task_params(
                "SHELL",
                cast("YamlObject", raw_params),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )
        else:
            normalized_params = {
                "rawScript": cast("str", document["command"]),
                "resourceList": [],
                "localParams": [],
            }
        assert set(normalized_params) == {
            "rawScript",
            "resourceList",
            "localParams",
        }
        assert all(
            item["direct"] == "IN"
            for item in cast(
                "list[dict[str, object]]",
                normalized_params["localParams"],
            )
        )

        spec = WorkflowSpec.model_validate(
            {
                "workflow": {"name": f"legacy-{variant}"},
                "tasks": [document],
            }
        )
        prepared = prepare_legacy_workflow_graph(
            spec,
            task_id_factory=lambda _task_name: "task-1",
        )
        process_definition = json.loads(prepared.materialize()["processDefinitionJson"])
        assert process_definition["tasks"][0]["params"] == normalized_params

    with pytest.raises(UserInputError, match="Unsupported task template variant"):
        task_template_result("SHELL", variant="resource", catalog=catalog)

    modern_params_result = task_template_result(
        "SHELL",
        variant="output",
        catalog=get_task_authoring_catalog("3.4.1"),
    )
    modern_data = cast("dict[str, object]", modern_params_result.data)
    modern_document = cast(
        "dict[str, object]",
        yaml.safe_load(cast("str", modern_data["yaml"])),
    )
    modern_params = cast("dict[str, object]", modern_document["task_params"])
    assert "varPool" in modern_params
    assert any(
        item["direct"] == "OUT"
        for item in cast("list[dict[str, object]]", modern_params["localParams"])
    )


@pytest.mark.parametrize(
    ("ds_version", "task_type", "expected_fragment"),
    [
        ("1.3.9", "SPARK", "task_params: {}"),
        ("2.0.0", "SPARK", "task_params: {}"),
        ("2.0.9", "SPARK", "task_params: {}"),
        ("3.1.0", "FLINK", "task_params: {}"),
        ("3.2.1", "JAVA", "runType: JAVA"),
        ("3.3.1", "FLINK_STREAM", "task_params: {}"),
        ("3.3.2", "FLINK_STREAM", "task_params: {}"),
        ("3.4.0", "FLINK_STREAM", "task_params: {}"),
        ("3.4.1", "FLINK_STREAM", "task_params: {}"),
        ("3.4.2", "FLINK_STREAM", "task_params: {}"),
    ],
)
def test_exact_profiles_with_opaque_authoring_can_render_a_native_template(
    ds_version: str,
    task_type: str,
    expected_fragment: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    assert task_type in generic_task_template_types(catalog=catalog)

    result = task_template_result(task_type, catalog=catalog)
    data = cast("dict[str, object]", result.data)
    template = cast("dict[str, object]", data["template"])

    assert template["task_type"] == task_type
    assert template["kind"] == "generic"
    assert f"type: {task_type}" in cast("str", data["yaml"])
    assert expected_fragment in cast("str", data["yaml"])


@pytest.mark.parametrize(
    ("task_type", "variant", "expected_fragment"),
    [
        ("SHELL", "resource", "resourceList:"),
        ("SHELL", "output", "direct: OUT"),
        ("PYTHON", "resource", "resourceList:"),
        ("PYTHON", "output", "${setValue(row_count=42)}"),
        ("HTTP", "post-json", "httpMethod: POST"),
        ("SQL", "output", "as row_count"),
        ("DEPENDENT", "task-dependency", "depTaskCode: 1000000000002"),
        ("REMOTESHELL", "output", "direct: OUT"),
    ],
)
def test_task_template_result_renders_discoverable_variants(
    task_type: str,
    variant: str,
    expected_fragment: str,
) -> None:
    result = task_template_result(task_type, variant=variant)
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved["variant"] == variant
    template_meta = data["template"]
    assert isinstance(template_meta, dict)
    available_variants = template_meta["variants"]
    assert isinstance(available_variants, list)
    assert variant in task_template_variants(task_type)
    assert available_variants == task_template_metadata()[task_type]["variants"]
    assert expected_fragment in data["yaml"]


def test_sub_workflow_params_template_teaches_parent_parameter_inheritance() -> None:
    result = task_template_result("SUB_WORKFLOW")
    data = result.data

    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    assert "localParams" not in document["task_params"]
    assert "set values on the parent workflow" in yaml_text
    assert "child workflow.global_params" in yaml_text
    assert "parent workflow-instance varPool" in yaml_text
    assert "do not become child inputs" in yaml_text


@pytest.mark.parametrize("variant", [None])
def test_conditions_templates_use_local_task_predicates(variant: str) -> None:
    result = task_template_result("CONDITIONS", variant=variant)
    data = result.data

    assert isinstance(data, dict)
    document = yaml.safe_load(data["yaml"])
    predicate = document["task_params"]["dependence"]["dependTaskList"][0][
        "dependItemList"
    ][0]

    assert predicate == {"task": "upstream-task", "status": "SUCCESS"}
    assert "conditionSuccess" not in document["task_params"]["conditionResult"]


def test_task_template_result_rejects_unsupported_variant() -> None:
    with pytest.raises(
        UserInputError, match="Unsupported task template variant"
    ) as exc:
        task_template_result("SHELL", variant="post-json")

    assert exc.value.details == {
        "task_type": "SHELL",
        "variant": "post-json",
        "available_variants": ["output", "resource"],
        "discovery_command": "dsctl task-type get SHELL",
    }


@pytest.mark.parametrize(
    "task_type",
    [
        "SHELL",
        "PYTHON",
        "REMOTESHELL",
        "SQL",
        "HTTP",
        "SUB_WORKFLOW",
        "DEPENDENT",
        "SWITCH",
        "CONDITIONS",
        "SPARK",
        "DATAX",
    ],
)
def test_task_templates_round_trip_through_workflow_spec(task_type: str) -> None:
    template = task_template_result(task_type)
    data = template.data
    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    task_document = yaml.safe_load(yaml_text)
    workflow_document = {
        "workflow": {"name": "templated-workflow"},
        "tasks": [task_document],
    }

    spec = WorkflowSpec.model_validate(workflow_document)

    assert spec.tasks[0].type == task_type


def _compilable_workflow_document(
    task_document: dict[str, object],
) -> dict[str, object]:
    task_type = task_document["type"]
    tasks: list[dict[str, object]] = [task_document]
    if task_type == "SWITCH":
        tasks.extend(
            [
                {
                    "name": "task-a",
                    "type": "SHELL",
                    "command": "echo A",
                },
                {
                    "name": "task-b",
                    "type": "SHELL",
                    "command": "echo B",
                },
                {
                    "name": "task-default",
                    "type": "SHELL",
                    "command": "echo default",
                },
            ]
        )
    if task_type == "CONDITIONS":
        tasks.extend(
            [
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
                {
                    "name": "upstream-task",
                    "type": "SHELL",
                    "command": "echo upstream",
                },
            ]
        )
    return {
        "workflow": {"name": "templated-workflow"},
        "tasks": tasks,
    }


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_task_templates_compile_through_workflow_create_payload(
    task_type: str,
) -> None:
    codes = iter(range(7001, 7100))
    template = task_template_result(task_type)
    data = template.data
    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    task_document = yaml.safe_load(yaml_text)
    assert isinstance(task_document, dict)

    spec = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    compilation = workflow_compile_service.prepare_workflow_create_compilation(spec)
    payload = compilation.materialize(
        [next(codes) for _ in range(compilation.required_task_code_count)],
        resource_refs=_TEMPLATE_FILE_REFS,
    )
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert task_definitions[0]["taskType"] == task_type
    assert json.loads(task_definitions[0]["taskParams"]) is not None
    if task_type == "JAVA":
        params = json.loads(task_definitions[0]["taskParams"])
        assert params["mainJar"] == {
            "resourceName": "/tenant/resources/jobs/daily-orders.jar"
        }
        assert params["resourceList"] == [
            {"resourceName": "/tenant/resources/jobs/daily-orders.jar"}
        ]
    if task_type == "MR":
        params = json.loads(task_definitions[0]["taskParams"])
        assert params["mainJar"] == {
            "resourceName": "/tenant/resources/jobs/wordcount.jar"
        }
        assert params["resourceList"] == []
    if task_type == "SWITCH":
        switch_params = json.loads(task_definitions[0]["taskParams"])
        assert switch_params["switchResult"]["dependTaskList"][0]["nextNode"] == 7002
        assert switch_params["switchResult"]["dependTaskList"][1]["nextNode"] == 7003
        assert switch_params["switchResult"]["nextNode"] == 7004
    if task_type == "CONDITIONS":
        conditions_params = json.loads(task_definitions[0]["taskParams"])
        assert conditions_params["conditionResult"]["successNode"] == [7002]
        assert conditions_params["conditionResult"]["failedNode"] == [7003]


@pytest.mark.parametrize(
    ("task_type", "variant"),
    [
        (task_type, variant)
        for task_type, metadata in task_template_metadata().items()
        for variant in metadata["variants"]
    ],
)
def test_task_template_variants_compile_through_workflow_create_payload(
    task_type: str,
    variant: str,
) -> None:
    codes = iter(range(8001, 8100))
    template = task_template_result(task_type, variant=variant)
    data = template.data
    assert isinstance(data, dict)
    task_document = yaml.safe_load(data["yaml"])
    assert isinstance(task_document, dict)

    spec = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    compilation = workflow_compile_service.prepare_workflow_create_compilation(spec)
    payload = compilation.materialize(
        [next(codes) for _ in range(compilation.required_task_code_count)],
        resource_refs=_TEMPLATE_FILE_REFS,
    )
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert task_definitions[0]["taskType"] == task_type
    assert json.loads(task_definitions[0]["taskParams"]) is not None
    if task_type == "SHELL" and variant == "resource":
        params = json.loads(task_definitions[0]["taskParams"])
        assert params["resourceList"] == [
            {"resourceName": "/tenant/resources/scripts/job.sh"}
        ]
    if task_type == "PYTHON" and variant == "resource":
        params = json.loads(task_definitions[0]["taskParams"])
        assert params["resourceList"] == [
            {"resourceName": "/tenant/resources/scripts/job.py"}
        ]


def test_task_template_result_accepts_remote_shell_alias() -> None:
    result = task_template_result("REMOTE_SHELL")

    assert result.resolved["task_type"] == "REMOTESHELL"
    assert result.resolved["template_kind"] == "typed"
