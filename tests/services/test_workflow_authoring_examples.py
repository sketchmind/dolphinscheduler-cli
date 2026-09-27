"""Exercise local authoring journeys using the artifacts exposed to CLI users."""

import json
import shlex
from pathlib import Path
from typing import cast

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models.common import YamlObject, YamlValue
from dsctl.models.workflow_spec import load_workflow_spec
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_lint_graph

runner = CliRunner()


@pytest.fixture(autouse=True)
def forbid_http(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_request(*args: object, **kwargs: object) -> None:
        pytest.fail("Local authoring examples must not perform HTTP requests")

    monkeypatch.setattr(httpx.Client, "send", unexpected_request)


def _raw_template(env_file: Path, *args: str) -> YamlObject:
    result = runner.invoke(
        app, ["--env-file", str(env_file), "template", *args, "--raw"]
    )
    assert result.exit_code == 0, result.output
    return cast("YamlObject", yaml.safe_load(result.stdout))


def _write_profile(tmp_path: Path, ds_version: str) -> Path:
    env_file = tmp_path / "authoring target.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
    return env_file


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.1"])
def test_default_shell_input_hint_can_replace_a_command_task_and_lint(
    tmp_path: Path, ds_version: str
) -> None:
    env_file = _write_profile(tmp_path, ds_version)
    document = _raw_template(env_file, "workflow")
    task = _raw_template(env_file, "task", "SHELL")
    assert "command" in task
    parameters: YamlObject = {
        "rawScript": 'echo "bizdate=${bizdate}"\n',
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${system.biz.date}",
            }
        ],
    }
    task.pop("command")
    task["task_params"] = parameters
    assert "${bizdate}" in str(parameters["rawScript"])
    local_params = cast("list[YamlObject]", parameters["localParams"])
    assert {
        "prop": "bizdate",
        "direct": "IN",
        "type": "VARCHAR",
        "value": "${system.biz.date}",
    } in local_params
    document["tasks"] = [task]
    path = tmp_path / "local-parameters.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    lint = runner.invoke(
        app, ["--env-file", str(env_file), "lint", "workflow", str(path)]
    )

    assert lint.exit_code == 0, lint.output
    data = json.loads(lint.stdout)["data"]
    assert data["valid"] is True
    assert data["summary"]["taskTypeCounts"] == {"SHELL": 1}
    assert data["summary"]["edgeCount"] == 0

    task["command"] = 'echo "duplicate script"'
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    invalid = runner.invoke(
        app, ["--env-file", str(env_file), "lint", "workflow", str(path)]
    )
    assert invalid.exit_code == 1
    payload = json.loads(invalid.stderr)
    error = payload["error"]
    assert error["type"] == "user_input_error"
    assert any(
        "cannot define both task_params and command" in diagnostic["message"]
        for diagnostic in payload["data"]["diagnostics"]
    )


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.7", "2.0.8", "3.4.1"])
def test_conditions_placeholders_form_a_four_node_graph_without_manual_edges(
    tmp_path: Path, ds_version: str
) -> None:
    env_file = _write_profile(tmp_path, ds_version)
    document = _raw_template(env_file, "workflow")
    router = _raw_template(env_file, "task", "CONDITIONS")
    shell = _raw_template(env_file, "task", "SHELL")
    tasks = [
        {**shell, "name": "upstream-task"},
        router,
        {**shell, "name": "on-success"},
        {**shell, "name": "on-failed"},
    ]
    assert all(not task.get("depends_on") for task in tasks)
    document["tasks"] = cast("list[YamlValue]", tasks)
    path = tmp_path / "conditional-routing.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    lint = runner.invoke(
        app, ["--env-file", str(env_file), "lint", "workflow", str(path)]
    )

    assert lint.exit_code == 0, lint.output
    data = json.loads(lint.stdout)["data"]
    assert data["valid"] is True
    assert data["summary"]["taskTypeCounts"] == {"SHELL": 3, "CONDITIONS": 1}
    assert data["summary"]["edgeCount"] == 3
    assert data["summary"]["rootTasks"] == ["upstream-task"]
    assert set(data["summary"]["leafTasks"]) == {"on-success", "on-failed"}
    catalog = get_task_authoring_catalog(ds_version)
    spec = load_workflow_spec(
        path,
        authoring_context=workflow_authoring_context(
            catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
        ),
    )
    compilation = (
        prepare_legacy_workflow_lint_graph(spec)
        if ds_version == "1.3.9"
        else prepare_workflow_create_compilation(spec, catalog=catalog)
    )
    assert set(compilation.edges) == {
        ("upstream-task", "conditions-task"),
        ("conditions-task", "on-success"),
        ("conditions-task", "on-failed"),
    }


def test_tenant_is_a_runtime_or_schedule_option_and_not_workflow_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    env_file = _write_profile(tmp_path, "3.4.1")
    authoring_schema = runner.invoke(
        app,
        ["--env-file", str(env_file), "schema", "--command", "workflow.create"],
    )
    assert authoring_schema.exit_code == 0, authoring_schema.output
    payload = json.loads(authoring_schema.stdout)["data"]["command"]["payload"]
    yaml_schema = payload["yaml_schema"]
    workflow_ref = yaml_schema["properties"]["workflow"]["$ref"]
    workflow_schema = yaml_schema["$defs"][workflow_ref.removeprefix("#/$defs/")]
    assert "tenant" not in workflow_schema["properties"]
    assert "tenant_code" not in workflow_schema["properties"]
    assert workflow_schema["additionalProperties"] is False
    execution_context = payload["execution_context"]
    for command_key, option_name in (
        ("runtime_schema_command", "tenant"),
        ("schedule_schema_command", "tenant-code"),
    ):
        args = shlex.split(execution_context[command_key])
        assert args[0] == "dsctl"
        schema = runner.invoke(app, args[1:])
        assert schema.exit_code == 0, schema.output
        command = json.loads(schema.stdout)["data"]["command"]
        assert option_name in [option["name"] for option in command["options"]]

    document = _raw_template(env_file, "workflow")
    workflow = cast("YamlObject", document["workflow"])
    assert "tenant" not in workflow
    workflow["tenant"] = "analytics"
    path = tmp_path / "misplaced-tenant.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    lint = runner.invoke(
        app, ["--env-file", str(env_file), "lint", "workflow", str(path)]
    )

    assert lint.exit_code == 1
    payload = json.loads(lint.stderr)
    error = payload["error"]
    assert error["type"] == "user_input_error"
    assert any(
        diagnostic["path"] == "workflow.tenant"
        and diagnostic["message"] == "Extra inputs are not permitted"
        for diagnostic in payload["data"]["diagnostics"]
    )
