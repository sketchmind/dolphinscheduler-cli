import json
import shlex
from pathlib import Path

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.services.template import supported_task_template_types
from dsctl.upstream import upstream_default_task_types
from tests.support import normalize_cli_help

runner = CliRunner()


def test_template_workflow_command_returns_yaml_document() -> None:
    result = runner.invoke(app, ["template", "workflow"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow"
    assert payload["resolved"]["with_schedule"] is False
    assert payload["resolved"]["ds_version"] == "3.4.1"
    assert payload["data"]["artifact"] == {
        "kind": "workflow-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow --raw",
        "target_command_pattern": "dsctl workflow create --file FILE",
    }
    assert "workflow:" in payload["data"]["yaml"]
    assert "tasks:" in payload["data"]["yaml"]
    assert payload["data"]["yaml"].startswith("# Workflow YAML")
    document = yaml.safe_load(payload["data"]["yaml"])
    assert document["workflow"]["release_state"] == "OFFLINE"
    assert "schedule" not in document


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
    document = yaml.safe_load(payload["data"]["yaml"])
    assert document["workflow"]["release_state"] == "ONLINE"
    assert document["schedule"]["enabled"] is False


def test_template_workflow_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "workflow", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith(
        "# Workflow YAML template for `dsctl workflow create --file FILE`\n"
    )
    assert '"ok": true' not in result.stdout
    assert "workflow:" in result.stdout
    assert "tasks:" in result.stdout


def test_template_workflow_with_schedule_round_trips_through_lint(
    tmp_path: Path,
) -> None:
    template_result = runner.invoke(
        app,
        ["template", "workflow", "--with-schedule", "--raw"],
    )
    assert template_result.exit_code == 0
    workflow_file = tmp_path / "workflow.yaml"
    workflow_file.write_text(template_result.stdout, encoding="utf-8")

    lint_result = runner.invoke(app, ["lint", "workflow", str(workflow_file)])

    assert lint_result.exit_code == 0, lint_result.output
    payload = json.loads(lint_result.stdout)
    assert payload["data"]["valid"] is True
    assert payload["data"]["summary"]["releaseState"] == "ONLINE"
    assert payload["data"]["summary"]["hasSchedule"] is True


@pytest.mark.parametrize("env_file_position", ["prefix", "suffix"])
@pytest.mark.parametrize("raw", [False, True])
def test_template_workflow_env_file_is_authoritative_and_lints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    env_file_position: str,
    *,
    raw: bool,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("DS_API_URL", "https://inherited.example/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "inherited-token")
    env_file = tmp_path / "legacy target.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")
    args = ["template", "workflow", "--with-schedule"]
    if raw:
        args.append("--raw")
    profile_args = ["--env-file", str(env_file)]
    args = (
        [*profile_args, *args]
        if env_file_position == "prefix"
        else [*args, *profile_args]
    )

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    if raw:
        yaml_text = result.stdout
    else:
        payload = json.loads(result.stdout)
        assert payload["resolved"]["ds_version"] == "1.3.9"
        yaml_text = payload["data"]["yaml"]
        raw_command = shlex.split(payload["data"]["artifact"]["raw_command"])
        assert raw_command[:3] == ["dsctl", "--env-file", str(env_file)]
        replay = runner.invoke(app, raw_command[1:])
        assert replay.exit_code == 0, replay.output
        assert replay.stdout == yaml_text
        for command in payload["data"]["related_command_patterns"]:
            assert shlex.split(command)[:3] == ["dsctl", "--env-file", str(env_file)]
            assert command in yaml_text
        target_command = shlex.split(
            payload["data"]["artifact"]["target_command_pattern"]
        )
        assert target_command[:3] == ["dsctl", "--env-file", str(env_file)]
    first_line = yaml_text.splitlines()[0]
    assert first_line.startswith("# Workflow YAML template for `")
    assert first_line.endswith("`")
    assert shlex.split(first_line.split("`", 1)[1][:-1]) == [
        "dsctl",
        "--env-file",
        str(env_file),
        "workflow",
        "create",
        "--file",
        "FILE",
    ]
    for field in ("execution_type", "delay", "environment_code", "timezone"):
        assert f"{field}:" not in yaml_text
    workflow_file = tmp_path / "workflow.yaml"
    workflow_file.write_text(yaml_text, encoding="utf-8")
    lint = runner.invoke(app, [*profile_args, "lint", "workflow", str(workflow_file)])
    assert lint.exit_code == 0, lint.output
    assert json.loads(lint.stdout)["data"]["valid"] is True


@pytest.mark.parametrize("requested_version", [None, "auto"])
@pytest.mark.parametrize("raw", [False, True])
def test_template_workflow_auto_without_cache_fails_locally_like_task_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    requested_version: str | None,
    *,
    raw: bool,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("DS_API_URL", "https://uncached.example/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-token")
    if requested_version is not None:
        monkeypatch.setenv("DS_VERSION", requested_version)

    def unexpected_request(*args: object, **kwargs: object) -> None:
        pytest.fail("Local template generation must not perform HTTP requests")

    monkeypatch.setattr(httpx.Client, "send", unexpected_request)
    raw_args = ["--raw"] if raw else []
    workflow = runner.invoke(app, ["template", "workflow", *raw_args])
    task = runner.invoke(app, ["template", "task", "SHELL", *raw_args])

    assert workflow.exit_code == task.exit_code == 1
    assert workflow.stdout == task.stdout == ""
    error = json.loads(workflow.stderr)["error"]
    assert error == json.loads(task.stderr)["error"]
    assert error["type"] == "config_error"
    assert error["details"]["reason"] == "version_not_resolved"
    assert "dsctl doctor" in error["suggestion"]
    assert "DS_VERSION" in error["suggestion"]


def test_template_workflow_patch_command_returns_patch_template() -> None:
    result = runner.invoke(app, ["template", "workflow-patch"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow-patch"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "template": "workflow.patch",
    }
    assert payload["data"]["artifact"] == {
        "kind": "workflow-patch-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow-patch --raw",
        "target_command_pattern": "dsctl workflow edit WORKFLOW --patch FILE",
    }
    assert payload["data"]["yaml"].startswith("# Workflow patch YAML")
    assert "patch:" in payload["data"]["yaml"]
    assert "tasks.create" in payload["data"]["rules"][2]


def test_template_workflow_patch_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "workflow-patch", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith(
        "# Workflow patch YAML template for "
        "`dsctl workflow edit WORKFLOW --patch FILE`\n"
    )
    assert '"ok": true' not in result.stdout
    assert "patch:" in result.stdout
    assert "# tasks:" in result.stdout


def test_template_workflow_instance_patch_command_returns_patch_template() -> None:
    result = runner.invoke(app, ["template", "workflow-instance-patch"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.workflow-instance-patch"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "template": "workflow-instance.patch",
    }
    assert payload["data"]["artifact"] == {
        "kind": "workflow-instance-patch-template",
        "format": "yaml",
        "raw_command": "dsctl template workflow-instance-patch --raw",
        "target_command_pattern": (
            "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
            "--patch FILE"
        ),
    }
    assert payload["data"]["yaml"].startswith("# Workflow-instance patch YAML")
    assert "patch:" in payload["data"]["yaml"]
    assert "workflow-instance edit only accepts" in payload["data"]["rules"][1]


def test_template_workflow_instance_patch_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "workflow-instance-patch", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith(
        "# Workflow-instance patch YAML template for:\n"
        "# `dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT "
        "--patch FILE`\n"
    )
    assert '"ok": true' not in result.stdout
    assert "repair_note" in result.stdout
    assert "# tasks:" in result.stdout


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
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template params` to inspect available topics."
    )


def test_template_environment_command_returns_environment_config_template() -> None:
    result = runner.invoke(app, ["template", "environment"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.environment"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "template": "environment.config",
    }
    assert payload["data"]["filename"] == "env.sh"
    assert "export JAVA_HOME=/opt/java" in payload["data"]["config"]
    assert payload["data"]["lines"][0]["line"] == "export JAVA_HOME=/opt/java"


def test_template_environment_command_can_render_table_rows() -> None:
    result = runner.invoke(
        app,
        ["--format", "table", "template", "environment"],
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
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "template": "cluster.config",
    }
    assert payload["data"]["filename"] == "cluster-config.json"
    assert "apiVersion: v1" in payload["data"]["payload"]["k8s"]
    assert json.loads(payload["data"]["config"]) == payload["data"]["payload"]
    assert payload["data"]["rows"] == payload["data"]["fields"]


def test_template_cluster_command_can_render_table_rows() -> None:
    result = runner.invoke(
        app,
        ["--format", "table", "template", "cluster"],
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
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "view": "list",
    }
    assert payload["data"]["default_type"] == "MYSQL"
    assert payload["data"]["template_command"] == (
        "dsctl template datasource --ds-version 3.4.1 --type MYSQL"
    )
    assert payload["data"]["template_command_pattern"] == (
        "dsctl template datasource --ds-version 3.4.1 --type TYPE"
    )
    assert "POSTGRESQL" in payload["data"]["supported_types"]
    assert {
        "type": "MYSQL",
        "template_command": "dsctl template datasource --ds-version 3.4.1 --type MYSQL",
    } in payload["data"]["rows"]
    assert "fields" not in payload["data"]


def test_template_datasource_column_error_uses_its_single_data_shape() -> None:
    result = runner.invoke(
        app,
        ["--columns", "bogus", "template", "datasource"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["details"]["view"] == "list"
    suggestion = payload["error"]["suggestion"]
    assert "Do not repeat the command" in suggestion
    assert "data.command.data_shape" in suggestion
    assert "data_shapes_by_view" not in suggestion


def test_template_datasource_help_points_to_type_discovery() -> None:
    result = runner.invoke(app, ["template", "datasource", "--help"])

    assert result.exit_code == 0
    assert "dsctl template datasource" in normalize_cli_help(result.stdout)
    assert "--ds-version" in result.stdout


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
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template datasource --ds-version 3.4.1` to choose a "
        "supported datasource type, then add `--type TYPE`."
    )


def test_template_datasource_command_selects_exact_version() -> None:
    result = runner.invoke(
        app,
        [
            "template",
            "datasource",
            "--ds-version",
            "3.2.0",
            "--type",
            "ssh",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds_version"] == "3.2.0"
    assert "publicKey" in payload["data"]["payload"]
    assert "privateKey" not in payload["data"]["payload"]


@pytest.mark.parametrize("selected_version", ["3.4.1", "3.2.0"])
def test_datasource_template_suggestions_retain_version_over_process_environment(
    selected_version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.2")
    result = runner.invoke(
        app,
        ["template", "datasource", "--ds-version", selected_version],
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)["data"]
    assert data["ds_version"] == selected_version
    commands = [
        data["template_command"],
        data["template_command_pattern"].replace("TYPE", "MYSQL"),
        data["type_discovery_command"],
        next(row["template_command"] for row in data["rows"] if row["type"] == "SSH"),
    ]
    for command in commands:
        suggestion = runner.invoke(app, shlex.split(command)[1:])

        assert suggestion.exit_code == 0, suggestion.stderr
        suggestion_data = json.loads(suggestion.stdout)["data"]
        assert suggestion_data["ds_version"] == selected_version
        if suggestion_data.get("type") == "SSH":
            expected_key = "privateKey" if selected_version == "3.4.1" else "publicKey"
            assert expected_key in suggestion_data["payload"]


def test_template_task_command_normalizes_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "shell"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SHELL"
    assert "variant" not in payload["resolved"]
    assert payload["data"]["template"]["variants"] == ["output", "resource"]
    assert "type: SHELL" in payload["data"]["yaml"]
    assert "# Task template" in payload["data"]["yaml"]
    assert "# Optional task runtime controls:" in payload["data"]["yaml"]
    assert "# timeout_notify_strategy: WARN" in payload["data"]["yaml"]


def test_template_task_command_normalizes_remote_shell_alias() -> None:
    result = runner.invoke(app, ["template", "task", "remote_shell"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["resolved"]["task_type"] == "REMOTESHELL"
    assert payload["resolved"]["template_kind"] == "typed"
    assert "variant" not in payload["resolved"]
    assert "type: REMOTESHELL" in payload["data"]["yaml"]


def test_template_task_command_renders_variant() -> None:
    result = runner.invoke(app, ["template", "task", "shell", "--variant", "resource"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SHELL"
    assert payload["resolved"]["variant"] == "resource"
    assert "resourceList:" in payload["data"]["yaml"]
    assert "resourceName: /scripts/job.sh" in payload["data"]["yaml"]


def test_template_task_command_supports_typed_sqoop() -> None:
    result = runner.invoke(app, ["template", "task", "sqoop"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"]["task_type"] == "SQOOP"
    assert payload["resolved"]["task_category"] == "DataIntegration"
    assert payload["resolved"]["template_kind"] == "typed"
    assert "variant" not in payload["resolved"]
    assert "type: SQOOP" in payload["data"]["yaml"]
    assert "subcommand: import" in payload["data"]["yaml"]
    assert "--password-file" in payload["data"]["yaml"]


def test_template_task_command_rejects_unknown_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "UNKNOWN"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "mode": "index",
    }
    assert payload["data"]["count"] == len(supported_task_template_types())
    assert payload["data"]["task_types"] == list(supported_task_template_types())
    assert payload["data"]["typed_task_types"] == list(supported_typed_task_types())
    assert "LINKIS" not in payload["data"]["task_types"]
    assert "LINKIS" in upstream_default_task_types()
    assert "SQOOP" not in payload["data"]["generic_task_types"]
    assert "SQOOP" in payload["data"]["typed_task_types"]
    assert "MR" not in payload["data"]["generic_task_types"]
    assert "DATAX" not in payload["data"]["generic_task_types"]
    assert "Logic" in payload["data"]["task_types_by_category"]
    assert payload["data"]["rows"][0]["task_type"] == "SHELL"
    assert payload["data"]["rows"][0]["variants"] == ["output", "resource"]
    assert payload["data"]["rows"][0]["next_command"] == "dsctl task-type get SHELL"
    assert "task_templates" not in payload["data"]


def test_template_task_command_rejects_legacy_list_option() -> None:
    result = runner.invoke(app, ["template", "task", "--list"])

    assert result.exit_code == 2
    assert "No such option: --list" in normalize_cli_help(result.output)


def test_template_task_help_points_to_type_and_variant_discovery() -> None:
    result = runner.invoke(app, ["template", "task", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "compact" in help_text
    assert "template catalog" in help_text
    assert "main template" in help_text
    assert "dsctl task-type get TYPE" in help_text


def test_template_task_command_omits_type_for_index() -> None:
    result = runner.invoke(app, ["template", "task"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "template.task"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "mode": "index",
    }
    assert payload["data"]["next_command"] == "dsctl task-type get SHELL"


def test_template_task_command_can_emit_raw_yaml() -> None:
    result = runner.invoke(app, ["template", "task", "SHELL", "--raw"])

    assert result.exit_code == 0
    assert result.stdout.startswith("# Schema and value discovery:")
    assert "# Task template for SHELL\n" in result.stdout
    assert '"ok": true' not in result.stdout
    assert "type: SHELL" in result.stdout


def test_template_task_raw_requires_task_type() -> None:
    result = runner.invoke(app, ["template", "task", "--raw"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
        result = runner.invoke(app, ["--format", "table", *args])

        assert result.exit_code == 0
        assert expected_header in result.stdout.splitlines()[0]
        assert max(len(line) for line in result.stdout.splitlines()) < 220


def test_workflow_example_command_generates_a_complete_local_graph() -> None:
    result = runner.invoke(
        app, ["template", "workflow", "--example", "output", "--raw"]
    )

    assert result.exit_code == 0
    document = yaml.safe_load(result.stdout)
    producer, consumer = document["tasks"]
    assert consumer["depends_on"] == [producer["name"]]
    assert consumer["task_params"]["localParams"][0]["direct"] == "IN"
    assert "rawScript: |" in result.stdout
