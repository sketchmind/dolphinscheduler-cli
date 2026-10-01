import json
import subprocess
import sys
from pathlib import Path

import pytest
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from dsctl import __version__
from dsctl.app import _normalize_root_options, app
from dsctl.upstream import SUPPORTED_VERSIONS
from tests.support import normalize_cli_help

runner = CliRunner()


def test_cli_composition_root_and_version_command_keep_exact_packages_lazy() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    probe = f"""
import json
import sys
sys.path.insert(0, {str(source_root)!r})
from typer.testing import CliRunner
from dsctl.app import app

def exact_modules():
    return sorted(
        name for name in sys.modules
        if name.startswith("dsctl.generated.versions.")
    )

runner = CliRunner()
before = exact_modules()
local_results = [
    runner.invoke(app, ["--help"]),
    runner.invoke(app, ["workflow", "list", "--help"]),
    runner.invoke(app, ["version"]),
]
after_local = exact_modules()
capabilities = runner.invoke(
    app,
    ["capabilities"],
    env={{"DS_VERSION": "3.2.0"}},
)
print(json.dumps({{
    "before": before,
    "after_local": after_local,
    "after_capabilities": sorted({{
        name.split(".")[3] for name in exact_modules()
    }}),
    "local_exit_codes": [result.exit_code for result in local_results],
    "capabilities_exit_code": capabilities.exit_code,
}}, sort_keys=True))
"""

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", probe],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "after_capabilities": ["ds_3_2_0"],
        "after_local": [],
        "before": [],
        "capabilities_exit_code": 0,
        "local_exit_codes": [0, 0, 0],
    }


def test_version_command_reports_cli_and_ds_versions() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "version"
    assert payload["data"] == {
        "cli": __version__,
        "ds": "3.4.1",
        "selected_ds_version": "3.4.1",
        "version_source": "offline_default",
        "version_checked_at": None,
        "version_evidence": None,
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "supported_ds_versions": list(SUPPORTED_VERSIONS),
    }


def test_version_command_can_render_tsv_columns() -> None:
    result = runner.invoke(
        app,
        ["--format", "tsv", "--columns", "cli,ds,family", "version"],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        f"cli\tds\tfamily\n{__version__}\t3.4.1\tworkflow-3.3-plus\n"
    )


def test_version_command_can_project_json_columns() -> None:
    result = runner.invoke(app, ["--columns", "cli,ds", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["data"] == {"cli": __version__, "ds": "3.4.1"}


@pytest.mark.parametrize(
    "args",
    [
        ["--format", "json-compact", "version"],
        ["version", "--format", "json-compact"],
        ["--format=json-compact", "version"],
        ["version", "--format=JSON-COMPACT"],
    ],
)
def test_version_command_accepts_compact_json_output(args: list[str]) -> None:
    result = runner.invoke(app, args)
    pretty = runner.invoke(app, ["--format", "json", "version"])

    assert result.exit_code == pretty.exit_code == 0
    assert result.stderr == pretty.stderr == ""
    assert result.stdout.count("\n") == 1
    assert pretty.stdout.count("\n") > 1
    assert json.loads(result.stdout) == json.loads(pretty.stdout)


@pytest.mark.parametrize(
    ("command", "exit_code"),
    [(["workflow", "get"], 2), (["schema", "--command", "missing.command"], 1)],
)
@pytest.mark.parametrize("suffix", [False, True])
@pytest.mark.parametrize("equals", [False, True])
def test_json_error_layouts_preserve_the_complete_error(
    command: list[str], *, exit_code: int, suffix: bool, equals: bool
) -> None:
    option = ["--format=JSON-COMPACT"] if equals else ["--format", "json-compact"]
    args = [*command, *option] if suffix else [*option, *command]
    compact = runner.invoke(app, args)
    pretty = runner.invoke(app, ["--format", "json", *command])

    assert compact.exit_code == pretty.exit_code == exit_code
    assert compact.stdout == pretty.stdout == ""
    assert compact.stderr.count("\n") == 1
    assert pretty.stderr.count("\n") > 1
    assert json.loads(compact.stderr) == json.loads(pretty.stderr)


@pytest.mark.parametrize("output_format", ["json", "json-compact", "table", "tsv"])
def test_raw_template_ignores_global_display_options(output_format: str) -> None:
    command = ["template", "workflow", "--raw"]
    default = runner.invoke(app, command)
    selected = runner.invoke(
        app, [*command, "--format", output_format, "--columns", "nonexistent"]
    )

    assert selected.exit_code == default.exit_code == 0
    assert selected.stdout == default.stdout
    assert selected.stderr == default.stderr
    assert "\nworkflow:\n" in selected.stdout


@pytest.mark.parametrize("args", [["--compact", "version"], ["version", "--compact"]])
def test_removed_compact_flag_is_rejected(args: list[str]) -> None:
    result = runner.invoke(app, args)

    assert result.exit_code == 2
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert "No such option: --compact" in payload["error"]["message"]


def test_root_help_describes_global_output_option_placement() -> None:
    result = runner.invoke(app, ["--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "json-compact" in help_text
    assert "--compact" not in help_text
    assert "Global option" in help_text
    assert "before or after the command path" in help_text


def test_root_help_gives_task_examples_and_scannable_command_sections() -> None:
    result = runner.invoke(app, ["--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "Manage Apache DolphinScheduler through its REST API." in help_text
    assert "dsctl doctor" in help_text
    assert "dsctl workflow list --project etl-prod" in help_text
    assert "dsctl workflow-instance watch 901 --project etl-prod" in help_text
    for panel in (
        "Getting started",
        "Authoring",
        "Runtime",
        "Resources",
        "Access and administration",
        "Reference",
    ):
        assert panel in help_text


def test_cli_runner_renders_parser_errors_as_structured_stderr() -> None:
    result = runner.invoke(app, ["project", "list", "--page-szie", "2"])

    assert result.exit_code == 2
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["action"] == "project.list"
    assert payload["error"]["type"] == "user_input_error"
    assert "No such option: --page-szie" in payload["error"]["message"]
    assert "--page-size" in payload["error"]["message"]


def test_workflow_create_help_separates_bounded_lint_from_full_dry_run() -> None:
    result = runner.invoke(app, ["workflow", "create", "--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "lint workflow FILE" in help_text
    assert "full DS request" in help_text
    assert "bounded DAG validation" in help_text


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["version", "--format=json-compact"], ["--format=json-compact", "version"]),
        (
            ["workflow", "list", "--format", "table", "--format=json-compact"],
            ["--format", "table", "--format=json-compact", "workflow", "list"],
        ),
        (
            ["version", "--format=tsv"],
            ["--format=tsv", "version"],
        ),
        (
            ["workflow", "get", "--", "--format=json-compact"],
            ["workflow", "get", "--", "--format=json-compact"],
        ),
        (
            ["workflow", "--format=json-compact", "list", "--all"],
            ["--format=json-compact", "workflow", "list", "--all"],
        ),
        (
            ["workflow", "list", "--all", "--format=json-compact"],
            ["--format=json-compact", "workflow", "list", "--all"],
        ),
    ],
)
def test_normalize_root_options_accepts_either_placement(
    args: list[str],
    expected: list[str],
) -> None:
    root_command = get_command(app)
    assert isinstance(root_command, TyperGroup)
    assert _normalize_root_options(root_command, args) == expected


@pytest.mark.parametrize(
    "args",
    [
        ["workflow", "list", "--search", "--format=json-compact"],
        ["workflow", "get", "--project", "--format=json-compact", "daily-sync"],
        ["schema", "--command", "--format=json-compact"],
        ["workflow", "list", "--search=--format=json-compact"],
    ],
)
def test_normalize_root_options_preserves_leaf_option_values(args: list[str]) -> None:
    root_command = get_command(app)
    assert isinstance(root_command, TyperGroup)
    assert _normalize_root_options(root_command, args) == args


def test_global_render_options_can_follow_leaf_command() -> None:
    result = runner.invoke(
        app,
        ["version", "--columns", "cli,ds", "--format=json-compact"],
    )

    assert result.exit_code == 0
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["data"] == {"cli": __version__, "ds": "3.4.1"}


@pytest.mark.parametrize("ds_version", ["3.3.2", "3.4.0"])
def test_version_command_marks_untested_versions_experimental(
    ds_version: str,
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        f"DS_VERSION={ds_version}\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["--env-file", "cluster.env", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["ds"] == ds_version
    assert payload["data"]["selected_ds_version"] == ds_version
    assert payload["data"]["contract_version"] == ds_version
    assert payload["data"]["family"] == "workflow-3.3-plus"
    assert payload["data"]["support_level"] == "experimental"


def test_version_command_prefers_explicit_env_file_over_process_environment(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.3.2\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "version"],
        env={"DS_VERSION": "3.4.2"},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["selected_ds_version"] == "3.3.2"


def test_context_command_uses_env_file_without_legacy_directory_defaults(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\nDS_API_TOKEN=secret-token\n",
        encoding="utf-8",
    )
    (isolated_cwd / ".dsctl-context.yaml").write_text(
        (
            "project: etl-prod\n"
            "workflow: daily-etl\n"
            "set_at: '2026-07-13T10:00:00+00:00'\n"
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "context"],
        env={"XDG_CONFIG_HOME": str(isolated_cwd / "xdg")},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "context"
    assert payload["data"]["api_url"] == "http://example.test/dolphinscheduler"
    assert payload["data"]["ds_version"] == "auto"
    assert payload["data"]["project"] is None
    assert payload["data"]["context"] is None
    assert "workflow" not in payload["data"]
    assert payload["resolved"] == {
        "selection": {
            "source": "flag",
            "context": None,
            "env_file": str(isolated_cwd / "cluster.env"),
            "api_url": "http://example.test/dolphinscheduler",
        },
        "remote_validation": "not_performed",
    }
    assert "default_project" not in payload["data"]
