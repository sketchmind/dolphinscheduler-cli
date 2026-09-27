from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from tests.live.support import (
    LiveProfileConfig,
    TaskDefinitionCleanupDoNotRetryError,
    TaskDefinitionCleanupInvocation,
    TaskDefinitionCleanupPreconditionError,
    TaskDefinitionCleanupProcessStartError,
    TaskDefinitionCleanupRejectedError,
    load_live_settings,
    run_dsctl,
    run_dsctl_raw,
    run_task_definition_cleanup,
    write_profile_env,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Literal


def test_write_profile_env_materializes_only_runtime_profile_keys(
    tmp_path: Path,
) -> None:
    env_file = write_profile_env(
        tmp_path / "etl.env",
        LiveProfileConfig(
            api_url="http://example.test/dolphinscheduler",
            api_token="etl-token",
            tenant_code="tenant-for-harness",
        ),
    )

    lines = env_file.read_text(encoding="utf-8").splitlines()

    assert lines == [
        "DS_API_URL=http://example.test/dolphinscheduler",
        "DS_API_TOKEN=etl-token",
    ]


def test_write_profile_env_preserves_exact_ds_version(tmp_path: Path) -> None:
    env_file = write_profile_env(
        tmp_path / "ds-3.2.2.env",
        LiveProfileConfig(
            api_url="http://example.test/dolphinscheduler",
            api_token="admin-token",
            ds_version="3.2.2",
        ),
    )

    assert env_file.read_text(encoding="utf-8").splitlines() == [
        "DS_API_URL=http://example.test/dolphinscheduler",
        "DS_API_TOKEN=admin-token",
        "DS_VERSION=3.2.2",
    ]


def test_derived_live_profile_preserves_exact_cluster_target() -> None:
    admin_profile = LiveProfileConfig(
        api_url="http://example.test/dolphinscheduler",
        api_token="admin-token",
        tenant_code="admin-tenant",
        ds_version="3.2.2",
    )

    user_profile = admin_profile.with_credentials(
        api_token="user-token",
        tenant_code="user-tenant",
    )

    assert user_profile == LiveProfileConfig(
        api_url="http://example.test/dolphinscheduler",
        api_token="user-token",
        tenant_code="user-tenant",
        ds_version="3.2.2",
    )


def test_load_live_settings_accepts_harness_metadata_in_admin_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_env_file = tmp_path / "admin.env"
    admin_env_file.write_text(
        "\n".join(
            [
                "DS_API_URL=http://example.test/dolphinscheduler",
                "DS_API_TOKEN=admin-token",
                "DS_LIVE_TENANT_CODE=tenant-for-harness",
                "DS_WEB_UI=http://example.test/dolphinscheduler/ui",
                "DS_DEPLOY_VERSION=3.4.1",
                "DS_VERSION=3.2.2",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_LIVE_ADMIN_ENV_FILE", str(admin_env_file))

    settings = load_live_settings()

    assert settings.admin is not None
    assert settings.admin.api_url == "http://example.test/dolphinscheduler"
    assert settings.admin.api_token == "admin-token"
    assert settings.admin.tenant_code == "tenant-for-harness"
    assert settings.admin.ds_version == "3.2.2"


def test_run_dsctl_sanitizes_live_and_profile_environment_when_env_file_is_used(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_env: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert check is False
        assert cwd == tmp_path
        assert text is True
        assert timeout == 60.0
        captured_env.update(env)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"ok": true, "action": "version", "data": {}}',
            stderr="",
        )

    profile_file = tmp_path / "profile.env"
    profile_file.write_text(
        "DS_API_URL=http://file.test/dolphinscheduler\n"
        "DS_API_TOKEN=file-token\n"
        "DS_VERSION=3.2.2\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DSCTL_RUN_LIVE_TESTS", "1")
    monkeypatch.setenv("DSCTL_RUN_LIVE_ADMIN_TESTS", "1")
    monkeypatch.setenv("DS_LIVE_ADMIN_ENV_FILE", str(tmp_path / "admin.env"))
    monkeypatch.setenv("DS_LIVE_TENANT_CODE", "tenant-for-harness")
    monkeypatch.setenv("DS_API_URL", "http://ambient.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "ambient-token")
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_dsctl(tmp_path, ["version"], env_file=profile_file)

    assert result.exit_code == 0
    assert "DSCTL_RUN_LIVE_TESTS" not in captured_env
    assert "DSCTL_RUN_LIVE_ADMIN_TESTS" not in captured_env
    assert "DS_LIVE_ADMIN_ENV_FILE" not in captured_env
    assert "DS_LIVE_TENANT_CODE" not in captured_env
    assert "DS_API_URL" not in captured_env
    assert "DS_API_TOKEN" not in captured_env
    assert "DS_VERSION" not in captured_env
    assert captured_env["PYTHONPATH"].split(os.pathsep)[0] == str(tmp_path / "src")


def test_run_dsctl_parses_structured_error_from_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=(
                '{"ok":false,"action":"user.list","error":{"type":"permission_denied"}}'
            ),
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_dsctl(tmp_path, ["user", "list"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.payload == {
        "ok": False,
        "action": "user.list",
        "error": {"type": "permission_denied"},
    }


def test_run_dsctl_can_isolate_an_installed_console_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "wheel-venv" / "bin" / "dsctl"
    captured_env: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert command == [str(executable), "version"]
        captured_env.update(env)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"ok":true,"action":"version","data":{}}',
            stderr="",
        )

    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "ambient-source"))
    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_dsctl(tmp_path, ["version"], executable=executable)

    assert result.exit_code == 0
    assert "PYTHONPATH" not in captured_env


def test_run_dsctl_raw_can_isolate_an_installed_console_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "wheel-venv" / "bin" / "dsctl"
    captured_env: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert command == [str(executable), "workflow", "export", "daily-sync"]
        captured_env.update(env)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="workflow:\n  name: daily-sync\n",
            stderr="",
        )

    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "ambient-source"))
    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_dsctl_raw(
        tmp_path,
        ["workflow", "export", "daily-sync"],
        executable=executable,
    )

    assert result.exit_code == 0
    assert result.stdout == "workflow:\n  name: daily-sync\n"
    assert result.payload == {}
    assert "PYTHONPATH" not in captured_env


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("selection", ["source", "environment", "explicit"])
def test_live_executable_selection_is_shared_and_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    raw: bool,
    selection: str,
) -> None:
    configured = tmp_path / "configured-wheel" / "bin" / "dsctl"
    explicit = tmp_path / "explicit-wheel" / "bin" / "dsctl"
    monkeypatch.delenv("DSCTL_LIVE_EXECUTABLE", raising=False)
    monkeypatch.setenv("PYTHONPATH", "ambient-source")
    if selection != "source":
        monkeypatch.setenv("DSCTL_LIVE_EXECUTABLE", str(configured))

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        if selection == "source":
            assert command == [sys.executable, "-m", "dsctl", "version"]
            assert env["PYTHONPATH"].split(os.pathsep)[0] == str(tmp_path / "src")
        else:
            expected = explicit if selection == "explicit" else configured
            assert command == [str(expected), "version"]
            assert "PYTHONPATH" not in env
        assert "DSCTL_LIVE_EXECUTABLE" not in env
        return subprocess.CompletedProcess(command, 0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)
    runner = run_dsctl_raw if raw else run_dsctl
    result = runner(
        tmp_path,
        ["version"],
        executable=explicit if selection == "explicit" else None,
    )
    assert result.exit_code == 0


def test_missing_configured_live_executable_does_not_fall_back_to_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing-wheel" / "dsctl"
    monkeypatch.setenv("DSCTL_LIVE_EXECUTABLE", str(missing))

    with pytest.raises(FileNotFoundError) as error:
        run_dsctl(tmp_path, ["version"])

    assert error.value.filename == str(missing)


def test_private_task_cleanup_uses_only_installed_isolated_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "wheel-venv" / "bin" / "python"
    env_file = tmp_path / "cluster.env"
    captured_env: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert command == [
            str(python),
            "-I",
            "-m",
            "dsctl.release_gate.task_definition_cleanup",
            "cleanup",
            "--env-file",
            str(env_file),
            "--project-code",
            "1234",
            "--workflow-code",
            "5678",
            "--run-id",
            "0123456789abcdef",
        ]
        assert cwd == python.parent
        captured_env.update(env)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                '{"deleted":2,"ds_version":"2.0.9","observed":2,'
                '"ok":true,"operation":"cleanup","remaining":0,'
                '"released":0,"remote_mutations":2,"schema_version":2}\n'
            ),
            stderr="",
        )

    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "ambient-source"))
    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_task_definition_cleanup(
        tmp_path,
        python=python,
        env_file=env_file,
        ds_version="2.0.9",
        operation="cleanup",
        project_code=1234,
        workflow_code=5678,
        run_id="0123456789abcdef",
    )

    assert result == TaskDefinitionCleanupInvocation(
        operation="cleanup",
        ds_version="2.0.9",
        observed=2,
        released=0,
        deleted=2,
        remaining=0,
        remote_mutations=2,
    )
    assert "PYTHONPATH" not in captured_env


def test_private_task_cleanup_never_leaks_process_output_and_fences_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "private-project-name-and-token"

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            2,
            stdout=(
                '{"schema_version":2,"ok":false,"operation":"cleanup",'
                '"ds_version":"2.0.9","error":'
                '{"type":"mutation_ambiguity_unresolved",'
                f'"message":"{secret}","do_not_retry":true}}}}\n'
            ),
            stderr=secret,
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(TaskDefinitionCleanupDoNotRetryError) as exc_info:
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )

    assert exc_info.value.error_type == "mutation_ambiguity_unresolved"
    assert exc_info.value.result_code is None
    assert secret not in str(exc_info.value)


def test_private_cleanup_preserves_bounded_deterministic_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "task-name-and-token"
    message = (
        "DolphinScheduler rejected task-definition cleanup because a task "
        "remains online; offline the task and retry cleanup"
    )

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            2,
            stdout=(
                '{"schema_version":2,"ok":false,"operation":"cleanup",'
                '"ds_version":"2.0.2","error":'
                '{"type":"api_result_error","result_code":50050,'
                f'"message":{json.dumps(message)}}}}}\n'
            ),
            stderr=secret,
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(TaskDefinitionCleanupRejectedError) as exc_info:
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.2",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )

    assert exc_info.value.error_type == "api_result_error"
    assert exc_info.value.result_code == 50050
    assert str(exc_info.value) == message
    assert secret not in str(exc_info.value)


def test_private_cleanup_fences_unreviewed_result_code_without_leaking_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "partial-mutation-secret"

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            2,
            stdout=(
                '{"schema_version":2,"ok":false,"operation":"cleanup",'
                '"ds_version":"2.0.9","error":'
                '{"type":"api_result_error","result_code":50046,'
                '"message":"reconcile task state before any retry",'
                '"do_not_retry":true}}\n'
            ),
            stderr=secret,
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(TaskDefinitionCleanupDoNotRetryError) as exc_info:
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )

    assert exc_info.value.error_type == "api_result_error"
    assert exc_info.value.result_code == 50046
    assert "result_code=50046" in str(exc_info.value)
    assert secret not in str(exc_info.value)


def test_private_cleanup_rejects_unreviewed_code_without_retry_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            2,
            stdout=(
                '{"schema_version":2,"ok":false,"operation":"cleanup",'
                '"ds_version":"2.0.9","error":'
                '{"type":"api_result_error","result_code":50046,'
                '"message":"DolphinScheduler rejected task-definition cleanup; '
                'inspect task state and permissions before retrying"}}\n'
            ),
            stderr="",
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(
        TaskDefinitionCleanupDoNotRetryError,
        match="result is ambiguous",
    ):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )


def test_private_cleanup_preserves_precondition_failure_without_ambiguity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            2,
            stdout=(
                '{"schema_version":2,"ok":false,"operation":"cleanup",'
                '"ds_version":"2.0.2","error":'
                '{"type":"precondition_not_proven",'
                '"message":"task-definition cleanup precondition was not proven"}}\n'
            ),
            stderr="",
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(
        TaskDefinitionCleanupPreconditionError,
        match="precondition was not proven",
    ):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.2",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )


def test_private_cleanup_process_start_failure_is_not_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        message = "secret local spawn detail"
        raise OSError(message)

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(
        TaskDefinitionCleanupProcessStartError,
        match="process failed to start",
    ) as exc_info:
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )

    assert "secret local spawn detail" not in str(exc_info.value)


def test_private_cleanup_process_failure_after_start_remains_fenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.SubprocessError

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(
        TaskDefinitionCleanupDoNotRetryError,
        match="process failed",
    ):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation="cleanup",
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )


@pytest.mark.parametrize(
    ("operation", "error_type"),
    [
        pytest.param("prove", AssertionError, id="prove-remains-retryable"),
        pytest.param(
            "cleanup",
            TaskDefinitionCleanupDoNotRetryError,
            id="cleanup-is-fenced",
        ),
    ],
)
def test_private_cleanup_process_timeout_has_operation_aware_retry_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Literal["prove", "cleanup"],
    error_type: type[Exception],
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["python"], timeout=1)

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(error_type, match="private task-definition cleanup"):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation=operation,
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )


@pytest.mark.parametrize(
    ("operation", "error_type"),
    [
        pytest.param("prove", AssertionError, id="prove-remains-retryable"),
        pytest.param(
            "cleanup",
            TaskDefinitionCleanupDoNotRetryError,
            id="cleanup-is-fenced",
        ),
    ],
)
def test_private_cleanup_killed_process_has_operation_aware_retry_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Literal["prove", "cleanup"],
    error_type: type[Exception],
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            -signal.SIGKILL,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(error_type, match="private task-definition cleanup"):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation=operation,
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )


@pytest.mark.parametrize(
    ("operation", "error_type"),
    [
        pytest.param("prove", AssertionError, id="prove-remains-retryable"),
        pytest.param(
            "cleanup",
            TaskDefinitionCleanupDoNotRetryError,
            id="cleanup-is-fenced",
        ),
    ],
)
def test_private_cleanup_schema_drift_has_operation_aware_retry_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: Literal["prove", "cleanup"],
    error_type: type[Exception],
) -> None:
    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["python"],
            0,
            stdout='{"schema_version":2,"ok":true}\n',
            stderr="",
        )

    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    with pytest.raises(error_type, match="private task-definition cleanup"):
        run_task_definition_cleanup(
            tmp_path,
            python=tmp_path / "venv" / "bin" / "python",
            env_file=tmp_path / "cluster.env",
            ds_version="2.0.9",
            operation=operation,
            project_code=1234,
            workflow_code=None,
            run_id="0123456789abcdef",
        )
