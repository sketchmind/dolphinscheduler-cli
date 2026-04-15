import json

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.upstream import upstream_default_task_types

runner = CliRunner()


def test_template_workflow_command_returns_yaml_document() -> None:
    result = runner.invoke(app, ["template", "workflow"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow"
    assert payload["resolved"]["with_schedule"] is False
    assert "workflow:" in payload["data"]["yaml"]
    assert "tasks:" in payload["data"]["yaml"]
    assert "schedule:" not in payload["data"]["yaml"]


def test_template_workflow_command_can_include_schedule_block() -> None:
    result = runner.invoke(app, ["template", "workflow", "--with-schedule"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow"
    assert payload["resolved"]["with_schedule"] is True
    assert "schedule:" in payload["data"]["yaml"]


def test_template_params_command_returns_parameter_syntax() -> None:
    result = runner.invoke(app, ["template", "params"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.params"
    assert payload["resolved"]["topic"] == "overview"
    assert payload["data"]["default_topic"] == "overview"
    assert "time" in [item["topic"] for item in payload["data"]["topics"]]


def test_template_params_command_can_expand_topic() -> None:
    result = runner.invoke(app, ["template", "params", "--topic", "time"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.params"
    assert payload["resolved"]["topic"] == "time"
    assert "$[yyyyMMdd-1]" in payload["data"]["details"]["examples"]
    assert any("YYYY" in caution for caution in payload["data"]["details"]["cautions"])
    assert "workflow:" in payload["data"]["details"]["yaml"]


def test_template_params_command_rejects_unknown_topic() -> None:
    result = runner.invoke(app, ["template", "params", "--topic", "unknown"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `template params` to inspect available topics."
    )


def test_template_task_command_normalizes_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "shell"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SHELL"
    assert payload["resolved"]["variant"] == "minimal"
    assert payload["resolved"]["available_variants"] == [
        "minimal",
        "params",
        "resource",
    ]
    assert "type: SHELL" in payload["data"]["yaml"]
    assert "# Optional task runtime controls:" in payload["data"]["yaml"]
    assert "# timeout_notify_strategy: WARN" in payload["data"]["yaml"]


def test_template_task_command_normalizes_remote_shell_alias() -> None:
    result = runner.invoke(app, ["template", "task", "remote_shell"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["resolved"]["task_type"] == "REMOTESHELL"
    assert payload["resolved"]["template_kind"] == "typed"
    assert payload["resolved"]["variant"] == "minimal"
    assert "type: REMOTESHELL" in payload["data"]["yaml"]


def test_template_task_command_renders_variant() -> None:
    result = runner.invoke(app, ["template", "task", "shell", "--variant", "resource"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SHELL"
    assert payload["resolved"]["variant"] == "resource"
    assert "resourceList:" in payload["data"]["yaml"]
    assert "resourceName: /tenant/resources/scripts/job.sh" in payload["data"]["yaml"]


def test_template_task_command_supports_generic_upstream_task_types() -> None:
    result = runner.invoke(app, ["template", "task", "spark"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SPARK"
    assert payload["resolved"]["task_category"] == "Universal"
    assert payload["resolved"]["template_kind"] == "generic"
    assert payload["resolved"]["variant"] == "minimal"
    assert "type: SPARK" in payload["data"]["yaml"]
    assert "task_params: {}" in payload["data"]["yaml"]


def test_template_task_command_rejects_unknown_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "UNKNOWN"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `template task --list` to inspect supported task types."
    )


def test_template_task_command_can_list_supported_types() -> None:
    result = runner.invoke(app, ["template", "task", "--list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task_types"
    assert payload["data"]["count"] == len(upstream_default_task_types())
    assert payload["data"]["task_types"] == list(upstream_default_task_types())
    assert payload["data"]["typed_task_types"] == list(supported_typed_task_types())
    assert "SPARK" in payload["data"]["generic_task_types"]
    assert "Logic" in payload["data"]["task_types_by_category"]
    assert payload["data"]["task_templates"]["SHELL"]["variants"] == [
        "minimal",
        "params",
        "resource",
    ]


def test_template_task_command_requires_type_without_list() -> None:
    result = runner.invoke(app, ["template", "task"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"].startswith("TASK_TYPE is required.")
    assert payload["error"]["suggestion"] == (
        "Run `template task --list` to inspect supported task types."
    )
