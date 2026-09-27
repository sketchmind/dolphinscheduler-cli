from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from tests.test_cli_process import _Request, _run_cli, _serve_rest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("version", ["3.4.1", "3.2.0"])
def test_auto_remote_selection_is_shared_and_cached_for_local_discovery(
    tmp_path: Path, version: str
) -> None:
    def responder(request: _Request) -> tuple[int, object]:
        if request.path.endswith("/ui-plugins/query-product-info"):
            if version == "3.2.0":
                return 404, {}
            return 200, {"code": 0, "data": {"version": version}}
        if request.path.endswith("/v3/api-docs"):
            return 200, {"openapi": "3.0.1", "info": {"version": version}}
        assert request == _Request("GET", "/dolphinscheduler/projects")
        return 200, {"code": 0, "msg": "success", "data": {"totalList": [], "total": 0}}

    with _serve_rest(responder) as rest:
        profile = {"DS_API_URL": rest.url, "DS_API_TOKEN": "local-test-token"}
        context = _run_cli("context", home=tmp_path, environment=profile)
        help_result = _run_cli(
            "project", "list", "--help", home=tmp_path, environment=profile
        )
        cold_version = _run_cli("version", home=tmp_path, environment=profile)
        assert rest.requests == []
        assert context.returncode == help_result.returncode == 0
        assert json.loads(context.stdout)["data"]["ds_version"] == "auto"
        assert cold_version.returncode == 1
        assert json.loads(cold_version.stderr)["error"]["type"] == "config_error"

        first = _run_cli("project", "list", home=tmp_path, environment=profile)
        first_requests = list(rest.requests)
        local = _run_cli("version", home=tmp_path, environment=profile)
        assert rest.requests == first_requests
        second = _run_cli("project", "list", home=tmp_path, environment=profile)

    assert first.returncode == second.returncode == local.returncode == 0, first.stderr
    assert json.loads(local.stdout)["data"]["selected_ds_version"] == version
    assert json.loads(local.stdout)["data"]["version_source"] == "cache"
    paths = [request.path for request in first_requests]
    expected = ["/dolphinscheduler/ui-plugins/query-product-info"]
    if version == "3.2.0":
        expected.append("/dolphinscheduler/v3/api-docs")
    expected.append("/dolphinscheduler/projects")
    assert paths == expected
    assert [request.path for request in rest.requests] == expected * 2


@pytest.mark.parametrize(
    "response", [(404, {}), (401, {}), (200, {"code": 0, "data": {"version": "9.9.9"}})]
)
def test_auto_failure_never_reaches_business_endpoint(
    tmp_path: Path, response: tuple[int, object]
) -> None:
    def responder(request: _Request) -> tuple[int, object]:
        assert request.path.endswith(("/ui-plugins/query-product-info", "/v3/api-docs"))
        return response

    with _serve_rest(responder) as rest:
        result = _run_cli(
            "project",
            "list",
            home=tmp_path,
            environment={
                "DS_API_URL": rest.url,
                "DS_API_TOKEN": "local-test-token",
                "DS_VERSION": "auto",
            },
        )
    assert result.returncode == 1
    assert result.stdout == ""
    assert "local-test-token" not in result.stderr
    error = json.loads(result.stderr)["error"]
    assert error["type"] == "config_error"
    setting = "DS_API_TOKEN" if response[0] == 401 else "DS_VERSION"
    assert setting in error["suggestion"]


def test_explicit_version_bypasses_discovery(
    tmp_path: Path,
) -> None:
    def responder(request: _Request) -> tuple[int, object]:
        assert request == _Request("GET", "/dolphinscheduler/projects")
        return 200, {"code": 0, "msg": "success", "data": {"totalList": [], "total": 0}}

    with _serve_rest(responder) as rest:
        result = _run_cli(
            "project",
            "list",
            home=tmp_path,
            environment={
                "DS_API_URL": rest.url,
                "DS_API_TOKEN": "local-test-token",
                "DS_VERSION": "3.4.1",
            },
        )
    assert result.returncode == 0, result.stderr
    assert len(rest.requests) == 1


def test_local_template_version_override_does_not_probe_automatic_target(
    tmp_path: Path,
) -> None:
    def responder(request: _Request) -> tuple[int, object]:
        message = f"local template unexpectedly contacted {request.path}"
        raise AssertionError(message)

    with _serve_rest(responder) as rest:
        result = _run_cli(
            "template",
            "datasource",
            "--type",
            "MYSQL",
            "--ds-version",
            "3.2.0",
            home=tmp_path,
            environment={
                "DS_API_URL": rest.url,
                "DS_API_TOKEN": "local-test-token",
                "DS_VERSION": "auto",
            },
        )
        assert rest.requests == []
    assert result.returncode == 0, result.stderr
