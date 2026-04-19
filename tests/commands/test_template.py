import json

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.upstream import upstream_default_task_types
from tests.support import normalize_cli_help

runner = CliRunner()


def test_template_workflow_command_returns_yaml_document() -> None:
    result = runner.invoke(app, ["template", "workflow"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow"
    assert payload["resolved"]["with_schedule"] is False
    assert payload["data"]["artifact"] == {
        "kind": "workflow-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow --raw",
        "target_command": "dsctl workflow create --file FILE",
    }
    assert "workflow:" in payload["data"]["yaml"]
    assert "tasks:" in payload["data"]["yaml"]
    assert payload["data"]["lines"][0]["line"].startswith("# Workflow YAML")
    assert "schedule:" not in payload["data"]["yaml"]


def test_template_workflow_command_can_include_schedule_block() -> None:
    result = runner.invoke(app, ["template", "workflow", "--with-schedule"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow"
    assert payload["resolved"]["with_schedule"] is True
    assert (
        payload["data"]["artifact"]["raw_command"]
        == "dsctl template workflow --with-schedule --raw"
    )
    assert "schedule:" in payload["data"]["yaml"]


def test_template_workflow_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "workflow", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith(
        "# Workflow YAML template for `dsctl workflow create --file ...`\n"
    )
    assert '"ok": true' not in result.stdout
    assert "workflow:" in result.stdout
    assert "tasks:" in result.stdout


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
        "Run `dsctl template params` to inspect available topics."
    )


def test_template_environment_command_returns_environment_config_template() -> None:
    result = runner.invoke(app, ["template", "environment"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.environment"
    assert payload["resolved"] == {"template": "environment.config"}
    assert payload["data"]["filename"] == "env.sh"
    assert "export JAVA_HOME=/opt/java" in payload["data"]["config"]
    assert payload["data"]["lines"][0]["line"] == "export JAVA_HOME=/opt/java"


def test_template_environment_command_can_render_table_rows() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "template", "environment"],
    )

    assert result.exit_code == 0
    assert "line" in result.stdout
    assert "purpose" in result.stdout
    assert "export JAVA_HOME=/opt/java" in result.stdout
    assert "target_commands" not in result.stdout


def test_template_cluster_command_returns_cluster_config_template() -> None:
    result = runner.invoke(app, ["template", "cluster"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.cluster"
    assert payload["resolved"] == {"template": "cluster.config"}
    assert payload["data"]["filename"] == "cluster-config.json"
    assert "apiVersion: v1" in payload["data"]["payload"]["k8s"]
    assert json.loads(payload["data"]["config"]) == payload["data"]["payload"]
    assert payload["data"]["rows"] == payload["data"]["fields"]


def test_template_cluster_command_can_render_table_rows() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "template", "cluster"],
    )

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "value_type" in result.stdout
    assert "k8s" in result.stdout
    assert "CHANGE_ME_BASE64" not in result.stdout


def test_template_datasource_command_returns_discovery() -> None:
    result = runner.invoke(app, ["template", "datasource"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.datasource"
    assert payload["resolved"] == {"view": "list"}
    assert payload["data"]["default_type"] == "MYSQL"
    assert payload["data"]["template_command"] == (
        "dsctl template datasource --type MYSQL"
    )
    assert payload["data"]["template_command_pattern"] == (
        "dsctl template datasource --type TYPE"
    )
    assert "POSTGRESQL" in payload["data"]["supported_types"]
    assert {
        "type": "MYSQL",
        "template_command": "dsctl template datasource --type MYSQL",
    } in payload["data"]["rows"]
    assert "fields" not in payload["data"]


def test_template_datasource_help_points_to_type_discovery() -> None:
    result = runner.invoke(app, ["template", "datasource", "--help"])

    assert result.exit_code == 0
    assert "dsctl template datasource" in result.stdout
    assert "enum list db-type" in result.stdout


def test_template_cluster_help_describes_json_config_template() -> None:
    result = runner.invoke(app, ["template", "cluster", "--help"])

    assert result.exit_code == 0
    assert "cluster config JSON template" in result.stdout


def test_template_datasource_command_returns_payload_for_type() -> None:
    result = runner.invoke(app, ["template", "datasource", "--type", "mysql"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.datasource"
    assert payload["resolved"]["view"] == "template"
    assert payload["resolved"]["datasource_type"] == "MYSQL"
    assert payload["data"]["type"] == "MYSQL"
    assert payload["data"]["payload"]["type"] == "MYSQL"
    assert payload["data"]["payload"]["port"] == 3306
    assert json.loads(payload["data"]["json"]) == payload["data"]["payload"]
    assert payload["data"]["rows"] == payload["data"]["fields"]
    assert "payload_schema" not in payload["data"]


def test_template_datasource_command_rejects_unknown_type() -> None:
    result = runner.invoke(app, ["template", "datasource", "--type", "unknown"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template datasource` to choose a supported datasource type, "
        "then `dsctl template datasource --type TYPE`."
    )


def test_template_task_command_normalizes_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "shell"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SHELL"
    assert payload["resolved"]["variant"] == "minimal"
    assert payload["data"]["template"]["variants"] == [
        "minimal",
        "params",
        "resource",
    ]
    assert "type: SHELL" in payload["data"]["yaml"]
    assert payload["data"]["rows"][0]["line"].startswith("# Task template")
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
    assert payload["error"]["message"] == "Unsupported task template type 'UNKNOWN'."
    assert payload["error"]["details"]["task_type"] == "UNKNOWN"
    assert payload["error"]["details"]["discovery_command"] == ("dsctl template task")
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template task` to inspect supported task types."
    )


def test_template_task_command_can_list_supported_types() -> None:
    result = runner.invoke(app, ["template", "task"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"] == {"mode": "index"}
    assert payload["data"]["count"] == len(upstream_default_task_types())
    assert payload["data"]["task_types"] == list(upstream_default_task_types())
    assert payload["data"]["typed_task_types"] == list(supported_typed_task_types())
    assert "SPARK" in payload["data"]["generic_task_types"]
    assert "Logic" in payload["data"]["task_types_by_category"]
    assert payload["data"]["rows"][0]["task_type"] == "SHELL"
    assert payload["data"]["rows"][0]["variants"] == ["minimal", "params", "resource"]
    assert payload["data"]["rows"][0]["next_command"] == "dsctl task-type get SHELL"
    assert "task_templates" not in payload["data"]


def test_template_task_command_rejects_legacy_list_option() -> None:
    result = runner.invoke(app, ["template", "task", "--list"])

    assert result.exit_code == 2
    assert "No such option: --list" in result.output


def test_template_task_help_points_to_type_and_variant_discovery() -> None:
    result = runner.invoke(app, ["template", "task", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "compact" in help_text
    assert "template catalog" in help_text
    assert "post-json" in help_text
    assert "workflow-dependency" in help_text
    assert "dsctl task-type get TYPE" in help_text


def test_template_task_command_omits_type_for_index() -> None:
    result = runner.invoke(app, ["template", "task"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"] == {"mode": "index"}
    assert payload["data"]["next_command"] == "dsctl task-type get SHELL"


def test_template_task_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "task", "SHELL", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith("# Task template for SHELL\n")
    assert '"ok": true' not in result.stdout
    assert "type: SHELL" in result.stdout


def test_template_task_raw_requires_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "--raw"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "--raw requires TASK_TYPE."


def test_template_table_outputs_use_row_shapes() -> None:
    cases = [
        (["template", "workflow"], "line_no | line"),
        (["template", "environment"], "purpose"),
        (["template", "cluster"], "name"),
        (["template", "datasource"], "type"),
        (["template", "datasource", "--type", "MYSQL"], "name"),
        (["template", "task"], "task_type"),
        (["template", "task", "SHELL"], "line_no | line"),
    ]
    for args, expected_header in cases:
        result = runner.invoke(app, ["--output-format", "table", *args])

        assert result.exit_code == 0
        assert expected_header in result.stdout.splitlines()[0]
        assert max(len(line) for line in result.stdout.splitlines()) < 220
