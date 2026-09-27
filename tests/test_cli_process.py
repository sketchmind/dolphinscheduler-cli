from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from dsctl import __version__
from tests.request_assertions import first_dry_run_request
from tests.support import strip_cli_ansi

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping


ROOT = Path(__file__).resolve().parents[1]
_ENTRYPOINT = "import sys; sys.argv[0] = 'dsctl'; from dsctl.app import main; main()"
_PROCESS_CONTROL_ENV = {
    "COMP_CWORD",
    "COMP_WORDS",
    "_DSCTL_COMPLETE",
}


@dataclass(frozen=True)
class _Request:
    method: str
    path: str
    body: bytes = b""


@dataclass
class _RestServer:
    url: str
    requests: list[_Request]
    request_started: threading.Event


@contextmanager
def _serve_rest(
    responder: Callable[[_Request], tuple[int, object]],
) -> Iterator[_RestServer]:
    requests: list[_Request] = []
    request_started = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._respond()

        def do_POST(self) -> None:
            self._respond()

        def _respond(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            request = _Request(
                method=self.command,
                path=urlsplit(self.path).path,
                body=self.rfile.read(content_length),
            )
            requests.append(request)
            request_started.set()
            status, payload = responder(request)
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            with suppress(BrokenPipeError):
                self.wfile.write(body)

        def log_message(self, _format: str, *args: object) -> None:
            del _format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield _RestServer(
            url=f"http://127.0.0.1:{server.server_port}/dolphinscheduler",
            requests=requests,
            request_started=request_started,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _cli_environment(
    home: Path,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DS_", "DSCTL_")) and key not in _PROCESS_CONTROL_ENV
    }
    environment.update(
        {
            "NO_COLOR": "1",
            "PYTHONPATH": str(ROOT / "src"),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "XDG_CACHE_HOME": str(home / ".cache"),
        }
    )
    environment.update(extra or {})
    return environment


def _run_cli(
    *args: str,
    home: Path,
    environment: Mapping[str, str] | None = None,
    stdin: int | None = subprocess.DEVNULL,
    timeout: float = 10,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter and entry point
        [sys.executable, "-c", _ENTRYPOINT, *args],
        cwd=home,
        env=_cli_environment(home, environment),
        stdin=stdin,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


_MISSING_WORKFLOW_SELECTOR_CASES = (
    (("workflow", "get"), ()),
    (("workflow", "export"), ()),
    (("workflow", "describe"), ()),
    (("workflow", "digest"), ()),
    (("workflow", "edit"), ("--patch", "{patch}")),
    (("workflow", "online"), ()),
    (("workflow", "offline"), ()),
    (("workflow", "run"), ()),
    (("workflow", "run-task"), ("--task", "extract")),
    (("workflow", "backfill"), ("--date", "2026-01-01")),
    (("workflow", "delete"), ("--force",)),
    (("workflow", "lineage", "get"), ()),
    (("workflow", "lineage", "dependent-tasks"), ()),
    (("task", "list"), ()),
    (("task", "get"), ("extract",)),
    (("task", "update"), ("extract", "--set", "command=echo ok")),
    (
        ("schedule", "create"),
        (
            "--cron",
            "0 0 2 * * ?",
            "--start",
            "2026-01-01 00:00:00",
            "--end",
            "2026-12-31 23:59:59",
            "--timezone",
            "UTC",
        ),
    ),
)


@pytest.mark.parametrize(("route", "extra"), _MISSING_WORKFLOW_SELECTOR_CASES)
def test_required_workflow_selectors_fail_in_parser_without_credentials(
    tmp_path: Path,
    route: tuple[str, ...],
    extra: tuple[str, ...],
) -> None:
    patch = tmp_path / "patch.yaml"
    patch.write_text("patch: {}\n", encoding="utf-8")
    args = (*route, *(str(patch) if value == "{patch}" else value for value in extra))

    completed = _run_cli(*args, home=tmp_path)

    assert completed.returncode == 2
    expected = (
        "Missing argument 'WORKFLOW'"
        if route[0] == "workflow"
        else ("Missing option '--workflow'")
    )
    assert expected in completed.stderr


@contextmanager
def _start_cli(
    *args: str,
    home: Path,
    environment: Mapping[str, str] | None = None,
) -> Iterator[subprocess.Popen[bytes]]:
    process = subprocess.Popen(  # noqa: S603 - fixed interpreter and entry point
        [sys.executable, "-c", _ENTRYPOINT, *args],
        cwd=home,
        env=_cli_environment(home, environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_shell_completion_is_exposed_without_configuration(tmp_path: Path) -> None:
    completed = _run_cli(
        "--show-completion",
        "bash",
        home=tmp_path,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert "_DSCTL_COMPLETE=complete_bash" in completed.stdout
    assert "complete -o default -F _dsctl_completion dsctl" in completed.stdout


@pytest.mark.parametrize("option", ["--show-completion", "--install-completion"])
def test_completion_options_require_an_explicit_shell(
    tmp_path: Path, option: str
) -> None:
    completed = _run_cli(option, home=tmp_path)

    assert completed.returncode == 2
    assert completed.stdout == ""
    payload = json.loads(completed.stderr)
    assert payload["action"] == "cli"
    assert payload["error"]["type"] == "user_input_error"
    assert f"Option '{option}' requires an argument" in payload["error"]["message"]


@pytest.mark.parametrize("flag", ["--version", "-v"])
def test_version_flag_is_eager_and_reads_only_the_installed_cli_version(
    tmp_path: Path,
    flag: str,
) -> None:
    completed = _run_cli(
        flag,
        "--context",
        "missing-context",
        home=tmp_path,
        environment={
            "DSCTL_CONTEXT": "also-missing",
            "DSCTL_ENV_FILE": "/missing/credentials.env",
            "DS_API_TOKEN": "must-not-appear",
        },
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout == f"{__version__}\n"
    assert "must-not-appear" not in completed.stdout


def test_bash_completion_returns_static_root_candidates(tmp_path: Path) -> None:
    completed = _run_cli(
        home=tmp_path,
        environment={
            "COMP_CWORD": "1",
            "COMP_WORDS": "dsctl wor",
            "_DSCTL_COMPLETE": "complete_bash",
        },
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout.splitlines() == [
        "worker-group",
        "workflow",
        "workflow-instance",
    ]


def test_root_help_and_global_option_order_use_real_process(tmp_path: Path) -> None:
    help_result = _run_cli("--help", home=tmp_path)
    short_help_result = _run_cli("-h", home=tmp_path)
    compact_result = _run_cli("version", "--format", "json-compact", home=tmp_path)

    assert help_result.returncode == 0
    assert help_result.stderr == ""
    help_text = strip_cli_ansi(help_result.stdout)
    assert "Usage: dsctl [OPTIONS] COMMAND [ARGS]..." in help_text
    assert "--show-completion" in help_text
    assert "--install-completion" in help_text
    assert short_help_result.returncode == 0
    assert short_help_result.stdout == help_result.stdout
    assert short_help_result.stderr == ""
    assert compact_result.returncode == 0
    assert compact_result.stderr == ""
    assert "\n  " not in compact_result.stdout


def test_usage_errors_follow_the_selected_error_format(tmp_path: Path) -> None:
    json_error = _run_cli("workflow", "get", home=tmp_path)
    spelling_error = _run_cli("project", "list", "--page-szie", "2", home=tmp_path)
    table_error = _run_cli("--format", "table", "workflow", "get", home=tmp_path)
    tsv_error = _run_cli("workflow", "get", "--format=tsv", home=tmp_path)

    for completed in (json_error, spelling_error, table_error, tsv_error):
        assert completed.returncode == 2
        assert completed.stdout == ""

    payload = json.loads(json_error.stderr)
    assert payload["action"] == "workflow.get"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "Missing argument 'WORKFLOW'."
    assert payload["error"]["details"]["missing"] == {
        "kind": "argument",
        "parameter": "workflow",
    }
    assert "workflow get [OPTIONS] WORKFLOW" in payload["error"]["details"]["usage"]

    spelling_payload = json.loads(spelling_error.stderr)
    assert spelling_payload["action"] == "project.list"
    assert "No such option: --page-szie" in spelling_payload["error"]["message"]
    assert "--page-size" in spelling_payload["error"]["message"]

    for completed in (table_error, tsv_error):
        assert completed.stderr.startswith("Error: Missing argument 'WORKFLOW'.\n")
        assert "\nHint: " in completed.stderr
        with pytest.raises(json.JSONDecodeError):
            json.loads(completed.stderr)


def test_compact_usage_error_is_single_line_json(tmp_path: Path) -> None:
    completed = _run_cli("--format", "json-compact", "workflow", "get", home=tmp_path)

    assert completed.returncode == 2
    assert completed.stdout == ""
    assert completed.stderr.count("\n") == 1
    payload = json.loads(completed.stderr)
    assert payload["error"]["details"]["missing"]["parameter"] == "workflow"


def test_invalid_format_uses_json_without_echoing_unrelated_secret_argv(
    tmp_path: Path,
) -> None:
    completed = _run_cli(
        "--format",
        "invalid",
        "version",
        "--api-token",
        "private-secret-value",
        home=tmp_path,
    )

    assert completed.returncode == 2
    payload = json.loads(completed.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert "private-secret-value" not in completed.stderr


def test_unknown_command_uses_cli_action_and_keeps_spelling_suggestion(
    tmp_path: Path,
) -> None:
    completed = _run_cli("workflo", home=tmp_path)

    assert completed.returncode == 2
    payload = json.loads(completed.stderr)
    assert payload["action"] == "cli"
    assert "Did you mean 'workflow'" in payload["error"]["message"]


def test_configuration_query_and_remote_error_use_process_channels(
    tmp_path: Path,
) -> None:
    profile = {
        "DS_API_TOKEN": "local-test-token",
        "DS_VERSION": "3.4.1",
    }
    project = {
        "id": 7,
        "code": 101,
        "name": "example-project",
        "description": "Local fake",
        "perm": 7,
        "defCount": 1,
    }

    def successful_query(request: _Request) -> tuple[int, object]:
        assert request == _Request("GET", "/dolphinscheduler/projects")
        return 200, {
            "code": 0,
            "msg": "success",
            "data": {
                "totalList": [project],
                "total": 1,
                "totalPage": 1,
                "pageSize": 10,
                "currentPage": 1,
            },
        }

    with _serve_rest(successful_query) as rest:
        profile["DS_API_URL"] = rest.url
        completed = _run_cli(
            "--format",
            "json-compact",
            "project",
            "list",
            "--page-size",
            "10",
            home=tmp_path,
            environment=profile,
        )

    assert completed.returncode == 0
    assert completed.stderr == ""
    payload = json.loads(completed.stdout)
    assert payload["action"] == "project.list"
    table = payload["data"]["totalList"]
    assert set(table) == {"columns", "rows"}
    assert len(table["rows"]) == 1
    row = dict(zip(table["columns"], table["rows"][0], strict=True))
    assert row["name"] == "example-project"
    assert row["code"] == 101

    def permission_error(request: _Request) -> tuple[int, object]:
        assert request == _Request("GET", "/dolphinscheduler/projects")
        return 200, {
            "code": 30001,
            "msg": "current user has no operation permission",
            "data": None,
        }

    with _serve_rest(permission_error) as rest:
        profile["DS_API_URL"] = rest.url
        failed = _run_cli(
            "project",
            "list",
            home=tmp_path,
            environment=profile,
        )

    assert failed.returncode == 1
    assert failed.stdout == ""
    error_payload = json.loads(failed.stderr)
    assert error_payload["ok"] is False
    assert error_payload["error"]["type"] == "permission_denied"


def test_template_to_lint_journey_uses_real_processes(tmp_path: Path) -> None:
    template = _run_cli("template", "workflow", "--raw", home=tmp_path)
    workflow_file = tmp_path / "workflow.yaml"
    workflow_file.write_text(template.stdout, encoding="utf-8")
    linted = _run_cli(
        "lint",
        "workflow",
        str(workflow_file),
        home=tmp_path,
    )

    assert template.returncode == 0
    assert template.stderr == ""
    assert template.stdout.startswith("# Workflow YAML template")
    assert linted.returncode == 0
    assert linted.stderr == ""
    lint_payload = json.loads(linted.stdout)
    assert lint_payload["action"] == "lint.workflow"
    assert lint_payload["data"]["valid"] is True


def test_workflow_authoring_dry_run_apply_and_execution_journey(  # noqa: C901
    tmp_path: Path,
) -> None:
    project = {
        "id": 7,
        "code": 101,
        "name": "example-project",
        "description": "Local fake",
        "perm": 7,
        "defCount": 1,
    }
    workflow = {
        "id": 9,
        "code": 202,
        "name": "example-workflow",
        "version": 1,
        "releaseState": "OFFLINE",
        "projectCode": 101,
        "projectName": "example-project",
        "description": "",
        "globalParams": "[]",
        "locations": "{}",
        "timeout": 0,
        "executionType": "PARALLEL",
    }
    created = False

    def responder(request: _Request) -> tuple[int, object]:  # noqa: C901
        nonlocal created
        if request == _Request("GET", "/dolphinscheduler/projects"):
            return 200, {
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [project],
                    "total": 1,
                    "totalPage": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                },
            }
        if request == _Request("GET", "/dolphinscheduler/projects/101"):
            return 200, {"code": 0, "msg": "success", "data": project}
        if request == _Request("GET", "/dolphinscheduler/projects/101/schedules"):
            return 200, {
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [],
                    "total": 0,
                    "totalPage": 0,
                    "pageSize": 100,
                    "currentPage": 1,
                },
            }
        if request == _Request(
            "GET", "/dolphinscheduler/projects/101/workflow-definition"
        ):
            return 200, {
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [workflow] if created else [],
                    "total": int(created),
                },
            }
        if request.path.endswith("/task-definition/gen-task-codes"):
            return 200, {"code": 0, "msg": "success", "data": [301, 302]}
        if request.path.endswith("/workflow-definition/simple-list"):
            return 200, {
                "code": 0,
                "msg": "success",
                "data": [
                    {
                        "id": 9,
                        "code": 202,
                        "name": "example-workflow",
                        "projectCode": 101,
                    }
                ],
            }
        if request == _Request(
            "GET", "/dolphinscheduler/projects/101/workflow-definition/202"
        ):
            return 200, {
                "code": 0,
                "msg": "success",
                "data": {
                    "workflowDefinition": workflow,
                    "workflowTaskRelationList": [],
                    "taskDefinitionList": [],
                },
            }
        if request == _Request(
            "GET", "/dolphinscheduler/projects/101/project-preference"
        ):
            return 200, {"code": 0, "msg": "success", "data": None}
        if request.method == "POST" and request.path.endswith("/workflow-definition"):
            created = True
            return 200, {"code": 0, "msg": "success", "data": workflow}
        if request.method == "POST" and request.path.endswith(
            "/workflow-definition/202/release"
        ):
            workflow["releaseState"] = "ONLINE"
            return 200, {"code": 0, "msg": "success", "data": True}
        if request.method == "POST" and request.path.endswith(
            "/executors/start-workflow-instance"
        ):
            return 200, {"code": 0, "msg": "success", "data": [901]}
        message = f"unexpected fake REST request: {request.method} {request.path}"
        raise AssertionError(message)

    with _serve_rest(responder) as rest:
        env_file = tmp_path / "cluster.env"
        env_file.write_text(
            "\n".join(
                [
                    f"DS_API_URL={rest.url}",
                    "DS_API_TOKEN=local-test-token",
                    "DS_VERSION=3.4.1",
                ]
            ),
            encoding="utf-8",
        )
        context = _run_cli(
            "context",
            "--env-file",
            str(env_file),
            home=tmp_path,
            environment={"DS_API_URL": "http://127.0.0.1:1/inherited"},
        )
        template = _run_cli("template", "workflow", "--raw", home=tmp_path)
        workflow_file = tmp_path / "workflow.yaml"
        workflow_file.write_text(template.stdout, encoding="utf-8")
        linted = _run_cli("lint", "workflow", str(workflow_file), home=tmp_path)
        dry_run = _run_cli(
            "workflow",
            "create",
            "--file",
            str(workflow_file),
            "--dry-run",
            "--columns",
            "*",
            "--env-file",
            str(env_file),
            home=tmp_path,
        )
        dry_run_requests = list(rest.requests)
        applied = _run_cli(
            "workflow",
            "create",
            "--file",
            str(workflow_file),
            "--env-file",
            str(env_file),
            home=tmp_path,
        )
        apply_requests = rest.requests[len(dry_run_requests) :]
        online = _run_cli(
            "workflow",
            "online",
            "example-workflow",
            "--project",
            "example-project",
            "--env-file",
            str(env_file),
            home=tmp_path,
        )
        executed = _run_cli(
            "workflow",
            "run",
            "example-workflow",
            "--project",
            "example-project",
            "--worker-group",
            "default",
            "--tenant",
            "default",
            "--failure-strategy",
            "continue",
            "--priority",
            "medium",
            "--warning-type",
            "none",
            "--warning-group-id",
            "0",
            "--environment-code",
            "0",
            "--execution-dry-run",
            "--env-file",
            str(env_file),
            home=tmp_path,
        )
        execution_requests = rest.requests[
            len(dry_run_requests) + len(apply_requests) :
        ]

    for completed in (context, template, linted, dry_run, applied, online, executed):
        assert completed.returncode == 0, completed.stderr
        assert completed.stderr == ""

    context_payload = json.loads(context.stdout)
    assert context_payload["data"]["api_url"] == rest.url
    assert json.loads(linted.stdout)["data"]["valid"] is True
    preview = json.loads(dry_run.stdout)
    assert preview["data"]["dry_run"] is True
    preview_path = first_dry_run_request(preview["data"])["path"]
    assert isinstance(preview_path, str)
    assert preview_path == "/projects/101/workflow-definition"
    assert all(request.method == "GET" for request in dry_run_requests)
    assert not any(
        request.path.endswith("/task-definition/gen-task-codes")
        for request in dry_run_requests
    )
    assert json.loads(applied.stdout)["data"]["code"] == 202
    assert json.loads(executed.stdout)["data"]["workflowInstanceIds"] == [901]

    create_requests = [
        request
        for request in apply_requests
        if request.method == "POST" and request.path.endswith("/workflow-definition")
    ]
    assert len(create_requests) == 1
    assert (
        sum(
            request.path.endswith("/task-definition/gen-task-codes")
            for request in apply_requests
        )
        == 1
    )
    create_request = create_requests[0]
    assert create_request.path == "/dolphinscheduler" + preview_path
    create_form = parse_qs(create_request.body.decode(), keep_blank_values=True)
    preview_form = first_dry_run_request(preview["data"])["form"]
    assert isinstance(preview_form, dict)
    assert "description" not in create_form
    assert preview_form.get("description") is None
    for stable_field in (
        "name",
        "globalParams",
        "timeout",
        "executionType",
    ):
        assert create_form[stable_field] == [str(preview_form[stable_field])]
    applied_tasks = json.loads(create_form["taskDefinitionJson"][0])
    assert [task["code"] for task in applied_tasks] == [301, 302]
    assert [task["name"] for task in applied_tasks] == ["extract", "load"]
    assert [task["taskType"] for task in applied_tasks] == ["SHELL", "SHELL"]
    applied_scripts = [
        json.loads(task["taskParams"])["rawScript"].strip() for task in applied_tasks
    ]
    assert applied_scripts == [
        'echo "extract ${bizdate}"',
        'echo "load ${bizdate}"',
    ]
    assert json.loads(create_form["globalParams"][0]) == [
        {
            "prop": "bizdate",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "${system.biz.date}",
        }
    ]
    applied_relations = json.loads(create_form["taskRelationJson"][0])
    assert [
        (relation["preTaskCode"], relation["postTaskCode"])
        for relation in applied_relations
    ] == [(0, 301), (301, 302)]
    applied_locations = json.loads(create_form["locations"][0])
    preview_locations = json.loads(preview_form["locations"])
    assert [(location["x"], location["y"]) for location in applied_locations] == [
        (location["x"], location["y"]) for location in preview_locations
    ]
    execution_requests = [
        request
        for request in execution_requests
        if request.method == "POST"
        and request.path.endswith("/executors/start-workflow-instance")
    ]
    assert len(execution_requests) == 1
    execution_request = execution_requests[0]
    assert execution_request.path == (
        "/dolphinscheduler/projects/101/executors/start-workflow-instance"
    )
    execution_form = parse_qs(execution_request.body.decode(), keep_blank_values=True)
    assert execution_form["workflowDefinitionCode"] == ["202"]
    assert execution_form["dryRun"] == ["1"]
    assert execution_form["workerGroup"] == ["default"]
    assert execution_form["tenantCode"] == ["default"]
    assert execution_form["failureStrategy"] == ["CONTINUE"]
    assert execution_form["workflowInstancePriority"] == ["MEDIUM"]
    assert execution_form["warningType"] == ["NONE"]
    assert execution_form["warningGroupId"] == ["0"]
    assert execution_form["environmentCode"] == ["0"]


def test_non_tty_delete_requires_force_without_prompting(tmp_path: Path) -> None:
    completed = _run_cli(
        "project",
        "delete",
        "example-project",
        home=tmp_path,
        timeout=3,
    )

    assert completed.returncode == 1
    assert completed.stdout == ""
    payload = json.loads(completed.stderr)
    assert payload["error"] == {
        "message": "Project deletion requires --force",
        "suggestion": "Retry the same command with --force.",
        "type": "user_input_error",
    }


def test_closed_pipeline_consumer_is_silent_and_bounded(tmp_path: Path) -> None:
    with _start_cli("schema", "--full", home=tmp_path) as process:
        assert process.stdout is not None
        process.stdout.close()
        process.stdout = None
        _, stderr = process.communicate(timeout=5)

        assert process.returncode == 1
        assert stderr == b""


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal contract")
def test_sigint_during_rest_request_exits_cleanly(tmp_path: Path) -> None:
    release_request = threading.Event()

    def delayed_query(_request: _Request) -> tuple[int, object]:
        release_request.wait(timeout=5)
        return 200, {
            "code": 0,
            "msg": "success",
            "data": {"totalList": [], "total": 0},
        }

    with (
        _serve_rest(delayed_query) as rest,
        _start_cli(
            "project",
            "list",
            home=tmp_path,
            environment={
                "DS_API_TOKEN": "local-test-token",
                "DS_API_URL": rest.url,
                "DS_VERSION": "3.4.1",
            },
        ) as process,
    ):
        try:
            assert rest.request_started.wait(timeout=5)
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=5)
        finally:
            release_request.set()

    assert process.returncode == 130
    assert stdout == b""
    assert stderr == b""
