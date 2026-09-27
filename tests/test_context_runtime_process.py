"""Public CLI transport acceptance for a context changing during pagination."""

import json
from functools import partial
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.client import DolphinSchedulerClient
from dsctl.context import set_default_context
from dsctl.services import runtime as runtime_service


@pytest.mark.parametrize("explicit_file", [False, True])
def test_inflight_requests_keep_original_connection_and_report_its_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, explicit_file: bool
) -> None:
    runner = CliRunner()
    test_file = tmp_path / "test.env"
    prod_file = tmp_path / "prod.env"
    test_link = tmp_path / "active.env"
    try:
        test_link.symlink_to(test_file)
    except OSError:
        pytest.skip("Creating a symlink is unavailable in this environment")
    for name, file in [("test", test_link), ("prod", prod_file)]:
        file.write_text(
            f"DS_API_URL=https://{name}.example/dolphinscheduler\n"
            f"DS_API_TOKEN={name}-private-token\nDS_VERSION=3.4.1\n"
        )
        registered = runner.invoke(
            app, ["context", "create", name, "--file", str(file)]
        )
        assert registered.exit_code == 0, registered.output
    selected = runner.invoke(app, ["config", "set", "default-context", "test"])
    assert selected.exit_code == 0, selected.output
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers["token"]))
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects"
        page = int(request.url.params["pageNo"])
        if len(seen) == 1:
            set_default_context("prod")
            test_file.write_text(prod_file.read_text())
            test_link.unlink()
            test_link.symlink_to(prod_file)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "total": 2,
                    "totalPage": 2,
                    "pageSize": 1,
                    "pageNo": page,
                    "currentPage": page,
                    "totalList": [
                        {"id": page, "code": page, "name": f"project-{page}"}
                    ],
                },
            },
        )

    monkeypatch.setattr(
        runtime_service,
        "DolphinSchedulerClient",
        partial(DolphinSchedulerClient, transport=httpx.MockTransport(handler)),
    )
    selector = ["--env-file", str(test_link)] if explicit_file else []
    first = runner.invoke(
        app, [*selector, "project", "list", "--all", "--page-size", "1"]
    )
    assert first.exit_code == 0, first.output
    assert seen == [("test.example", "test-private-token")] * 2
    payload = json.loads(first.stdout)
    selection = payload["resolved"]["selection"]
    assert selection["context"] == (None if explicit_file else "test")
    assert selection["api_url"] == "https://test.example/dolphinscheduler"
    assert "private-token" not in first.output

    second = runner.invoke(app, ["project", "list", "--page-size", "1"])
    assert second.exit_code == 0, second.output
    assert seen[-1] == ("prod.example", "prod-private-token")
    assert json.loads(second.stdout)["resolved"]["selection"]["context"] == "prod"

    retargeted = runner.invoke(app, ["--context", "test", "project", "list"])
    assert retargeted.exit_code == 1
    assert len(seen) == 3
    assert json.loads(retargeted.stderr)["error"]["type"] == "config_error"
