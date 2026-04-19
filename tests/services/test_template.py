import json
from typing import cast

import pytest
import yaml

from dsctl.errors import UserInputError
from dsctl.models import WorkflowSpec, supported_typed_task_types
from dsctl.services import _workflow_compile as workflow_compile_service
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
    workflow_template_result,
)
from dsctl.upstream import upstream_default_task_types


def test_workflow_template_result_returns_valid_yaml_document() -> None:
    result = workflow_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved["with_schedule"] is False
    assert data["artifact"] == {
        "kind": "workflow-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow --raw",
        "target_command": "dsctl workflow create --file FILE",
    }
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    assert data["lines"][0]["line"].startswith("# Workflow YAML")
    document = yaml.safe_load(yaml_text)

    assert "# Optional task runtime controls:" in yaml_text
    assert "# timeout_notify_strategy: WARN" in yaml_text
    assert "# task_group_id: 12" in yaml_text
    assert document["workflow"]["name"] == "example-workflow"
    assert document["workflow"]["execution_type"] == "PARALLEL"
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
    assert document["schedule"]["cron"] == "0 0 2 * * ?"
    assert document["schedule"]["enabled"] is False


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


def test_datasource_template_result_returns_discovery_without_type() -> None:
    result = datasource_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"view": "list"}
    assert data["default_type"] == "MYSQL"
    assert data["template_command"] == "dsctl template datasource --type MYSQL"
    assert data["template_command_pattern"] == "dsctl template datasource --type TYPE"
    assert data["target_commands"] == [
        "dsctl datasource create --file FILE",
        "dsctl datasource update DATASOURCE --file FILE",
    ]
    assert data["type_discovery_command"] == "dsctl enum list db-type"
    assert data["supported_types"] == list(supported_datasource_types())
    assert {
        "type": "MYSQL",
        "template_command": "dsctl template datasource --type MYSQL",
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
    assert data["target_commands"] == [
        "dsctl environment create --name NAME --config-file env.sh",
        "dsctl environment update ENVIRONMENT --config-file env.sh",
    ]
    assert data["source_options"] == ["--config TEXT", "--config-file PATH"]
    lines = data["lines"]
    assert isinstance(lines, list)
    assert lines[0]["line"] == "export JAVA_HOME=/opt/java"


def test_cluster_config_template_result_returns_json_template() -> None:
    result = cluster_config_template_result()
    data = result.data

    assert isinstance(data, dict)
    assert result.resolved == {"template": "cluster.config"}
    assert data["filename"] == "cluster-config.json"
    assert data["target_commands"] == [
        "dsctl cluster create --name NAME --config-file cluster-config.json",
        "dsctl cluster update CLUSTER --config-file cluster-config.json",
    ]
    assert data["source_options"] == ["--config TEXT", "--config-file PATH"]
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
    assert data["target_commands"] == [
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
        ("SPARK", "task_params", "generic", "Universal"),
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
    assert data["rows"][0]["line"].startswith("# Task template")
    document = yaml.safe_load(yaml_text)

    assert "# Optional task runtime controls:" in yaml_text
    assert "# cpu_quota: 50" in yaml_text
    assert "# memory_max: 1024" in yaml_text
    assert document["type"] == task_type.upper()
    assert expected_key in document


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

    assert isinstance(data, dict)
    assert result.resolved == {"mode": "index"}
    assert data["count"] == len(upstream_default_task_types())
    assert data["task_types"] == list(upstream_default_task_types())
    assert data["typed_task_types"] == list(typed_task_template_types())
    assert data["generic_task_types"] == list(generic_task_template_types())
    assert "Universal" in data["task_types_by_category"]
    assert "Logic" in data["task_types_by_category"]
    assert data["rows"][0]["task_type"] == "SHELL"
    assert data["rows"][0]["variants"] == ["minimal", "params", "resource"]
    assert data["rows"][0]["next_command"] == "dsctl task-type get SHELL"
    assert data["default_task_type"] == "SHELL"
    assert data["next_command"] == "dsctl task-type get SHELL"
    assert "task_templates" not in data


def test_task_template_types_match_typed_task_specs() -> None:
    assert supported_task_template_types() == upstream_default_task_types()
    assert typed_task_template_types() == supported_typed_task_types()


@pytest.mark.parametrize(
    ("task_type", "variant", "expected_fragment"),
    [
        ("SHELL", "resource", "resourceList:"),
        ("SHELL", "params", "direct: OUT"),
        ("PYTHON", "resource", "resourceList:"),
        ("PYTHON", "params", "${setValue(row_count=42)}"),
        ("HTTP", "post-json", "httpMethod: POST"),
        ("HTTP", "params", "X-Bizdate"),
        ("SQL", "pre-post-statements", "preStatements:"),
        ("SQL", "params", "as row_count"),
        ("SWITCH", "branching", "switchResult:"),
        ("SWITCH", "params", "prop: route"),
        ("CONDITIONS", "condition-routing", "conditionResult:"),
        ("CONDITIONS", "params", "localParams:"),
        ("DEPENDENT", "workflow-dependency", "dependTaskList:"),
        ("DEPENDENT", "params", "prop: date_window"),
        ("SUB_WORKFLOW", "child-workflow", "workflowDefinitionCode:"),
        ("SUB_WORKFLOW", "params", "prop: bizdate"),
        ("REMOTESHELL", "datasource", "datasource: 1"),
        ("REMOTESHELL", "params", "direct: OUT"),
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
    assert variant in available_variants
    assert expected_fragment in data["yaml"]


def test_task_template_result_rejects_unsupported_variant() -> None:
    with pytest.raises(
        UserInputError, match="Unsupported task template variant"
    ) as exc:
        task_template_result("SHELL", variant="post-json")

    assert exc.value.details == {
        "task_type": "SHELL",
        "variant": "post-json",
        "available_variants": ["minimal", "params", "resource"],
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
            ]
        )
    return {
        "workflow": {"name": "templated-workflow"},
        "tasks": tasks,
    }


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_task_templates_compile_through_workflow_create_payload(
    monkeypatch: pytest.MonkeyPatch,
    task_type: str,
) -> None:
    codes = iter(range(7001, 7100))
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    template = task_template_result(task_type)
    data = template.data
    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    task_document = yaml.safe_load(yaml_text)
    assert isinstance(task_document, dict)

    spec = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    payload = workflow_compile_service.compile_workflow_create_payload(spec)
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert task_definitions[0]["taskType"] == task_type
    assert json.loads(task_definitions[0]["taskParams"]) is not None
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
    monkeypatch: pytest.MonkeyPatch,
    task_type: str,
    variant: str,
) -> None:
    codes = iter(range(8001, 8100))
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    template = task_template_result(task_type, variant=variant)
    data = template.data
    assert isinstance(data, dict)
    task_document = yaml.safe_load(data["yaml"])
    assert isinstance(task_document, dict)

    spec = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    payload = workflow_compile_service.compile_workflow_create_payload(spec)
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert task_definitions[0]["taskType"] == task_type
    assert json.loads(task_definitions[0]["taskParams"]) is not None


def test_task_template_result_accepts_remote_shell_alias() -> None:
    result = task_template_result("REMOTE_SHELL")

    assert result.resolved["task_type"] == "REMOTESHELL"
    assert result.resolved["template_kind"] == "typed"
