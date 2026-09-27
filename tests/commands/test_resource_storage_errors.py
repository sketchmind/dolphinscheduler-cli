import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import resource as resource_service
from dsctl.upstream.resources import RESOURCE_DOMAIN, ResourceDomain
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import FakeResourceAdapter
from tests.support import make_profile

runner = CliRunner()


@pytest.mark.parametrize(
    ("command", "method", "arguments"),
    [
        ("list", "list", ["--dir", "/"]),
        ("view", "view", ["/job.sql"]),
        ("upload", "upload", ["--file", "{file}", "--dir", "/"]),
        (
            "create",
            "create_from_content",
            ["--name", "job.sql", "--content", "select 1", "--dir", "/"],
        ),
        ("mkdir", "create_directory", ["scripts", "--dir", "/"]),
        ("download", "download", ["/job.sql", "--output", "{output}"]),
        ("delete", "delete", ["/job.sql", "--force"]),
        ("list", "base_dir", []),
        ("upload", "base_dir", ["--file", "{file}"]),
        (
            "create",
            "base_dir",
            ["--name", "job.sql", "--content", "select 1"],
        ),
        ("mkdir", "base_dir", ["scripts"]),
    ],
)
@pytest.mark.parametrize("code", [60002, 10057, 10061])
def test_resource_storage_errors_preserve_native_cause_and_do_not_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command: str,
    method: str,
    arguments: list[str],
    code: int,
) -> None:
    adapter = FakeResourceAdapter(resources=[])
    calls = []

    def fail(*args: object, **kwargs: object) -> None:
        calls.append((args, kwargs))
        # A generic error must not become storage-disabled from its message alone.
        raise ApiResultError(result_code=code, result_message="storage not startup")

    monkeypatch.setattr(adapter, method, fail)
    patch_bound_domain_service_runtime(
        monkeypatch,
        resource_service,
        expected_domain=RESOURCE_DOMAIN,
        runtime_domain=ResourceDomain(resources=adapter),
        profile_factory=make_profile,
    )
    upload = tmp_path / "input.sql"
    upload.write_text("select 1")
    output = tmp_path / "download.sql"
    argv = [arg.format(file=upload, output=output) for arg in arguments]

    result = runner.invoke(app, ["resource", command, *argv])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["action"] == f"resource.{command}"
    assert payload["ok"] is False
    error = payload["error"]
    assert error["source"]["result_code"] == code
    assert len(calls) == 1
    assert not output.exists()
    if code == 60002:
        assert error["type"] == "invalid_state"
        assert error["details"]["operation"] == command
        assert error["message"] == "DolphinScheduler resource storage is not enabled."
        assert "administrator" in error["suggestion"]
        assert (
            "server's effective resource storage configuration" in error["suggestion"]
        )
    else:
        assert error["type"] == "api_result_error"
        assert error["message"] == "storage not startup"
