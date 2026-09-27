from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal

from dsctl.client import DolphinSchedulerClient
from dsctl.config import ClusterProfile
from dsctl.errors import ApiResultError
from dsctl.release_gate import task_definition_cleanup_runner as runner
from dsctl.release_gate.task_definition_cleanup import (
    CleanupTaskDetail,
    CleanupTaskHistoryPage,
    CleanupTaskPage,
    CleanupTaskRef,
    TaskDefinitionCleanupPort,
    TaskDeleteAmbiguousError,
    canonical_task_params_fingerprint,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

_RUN_ID = "0123456789abcdef"


class _Client(DolphinSchedulerClient):
    pass


class _Port:
    ds_version: str = "2.0.9"
    cleanup_strategy: Literal["direct-delete", "workflow-cascade-proof-only"] = (
        "direct-delete"
    )
    pre_delete_release: Literal["none", "offline"] = "none"
    inventory_execute_types: tuple[str, ...] = ()

    def __init__(self, names: tuple[str, ...]) -> None:
        self.details = {
            11 + index: _detail(name, 11 + index) for index, name in enumerate(names)
        }

    def list_page(
        self,
        *,
        project_code: int,
        execute_type: str | None,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskPage:
        assert project_code == 7001
        rows = tuple(
            CleanupTaskRef(detail.code, detail.name, detail.version)
            for detail in self.details.values()
        )
        return CleanupTaskPage(
            execute_type=execute_type,
            requested_page=page_no,
            current_page=page_no,
            offset=0,
            page_size=page_size,
            total=len(rows),
            total_pages=1,
            rows=rows,
        )

    def get_detail(
        self,
        *,
        project_code: int,
        code: int,
    ) -> CleanupTaskDetail:
        assert project_code == 7001
        return self.details[code]

    def list_history_page(
        self,
        *,
        project_code: int,
        code: int,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskHistoryPage:
        del project_code, code, page_no, page_size
        message = "direct-delete fake has no task history operation"
        raise AssertionError(message)

    def delete(self, *, project_code: int, code: int) -> None:
        assert project_code == 7001
        self.details.pop(code)

    def release_offline(self, *, project_code: int, code: int) -> None:
        del project_code, code
        message = "direct-delete fake has no offline release operation"
        raise AssertionError(message)

    def prove_no_workflows(self, *, project_code: int) -> None:
        del project_code
        message = "direct-delete fake has no workflow proof operation"
        raise AssertionError(message)


def test_prove_command_emits_one_sanitized_json_document(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / "cluster.env"
    profile = _profile("2.0.9")
    expected_profile = profile
    port = _Port(("extract", "load"))

    def load_test_profile(env_file: Path) -> ClusterProfile:
        assert env_file == tmp_path / "cluster.env"
        return profile

    def bind_port(
        profile: ClusterProfile,
        client: DolphinSchedulerClient,
    ) -> TaskDefinitionCleanupPort:
        assert profile is expected_profile
        assert isinstance(client, _Client)
        assert client.profile is expected_profile
        return port

    exit_code = runner.main(
        [
            "prove",
            "--env-file",
            str(env_file),
            "--project-code",
            "7001",
            "--workflow-code",
            "8001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=load_test_profile,
        client_factory=_Client,
        port_factory=bind_port,
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.out.count("\n") == 1
    assert captured.err == ""
    assert payload == {
        "schema_version": 2,
        "ok": True,
        "operation": "prove",
        "ds_version": "2.0.9",
        "observed": 2,
        "released": 0,
        "deleted": 0,
        "remaining": 2,
        "remote_mutations": 0,
    }
    assert _RUN_ID not in captured.out
    assert "7001" not in captured.out
    assert "extract" not in captured.out
    assert "load" not in captured.out


def test_cleanup_command_reports_exact_remote_mutation_count(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / "cluster.env"
    profile = _profile("3.1.0")
    port = _Port(("load",))

    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(env_file),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: port,
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["operation"] == "cleanup"
    assert payload["observed"] == 1
    assert payload["released"] == 0
    assert payload["deleted"] == 1
    assert payload["remaining"] == 0
    assert payload["remote_mutations"] == 1


def test_runner_failure_never_echoes_unknown_exception_or_secret(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / "cluster.env"
    profile = _profile("2.0.9")

    def fail_port(
        profile: ClusterProfile,
        client: DolphinSchedulerClient,
    ) -> TaskDefinitionCleanupPort:
        del profile, client
        message = "secret-token-value"
        raise RuntimeError(message)

    exit_code = runner.main(
        [
            "prove",
            "--env-file",
            str(env_file),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=fail_port,
    )

    assert exit_code == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "secret-token-value" not in captured.out
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"] == {
        "type": "internal_error",
        "message": "task-definition cleanup failed",
    }


def test_runner_marks_retained_ambiguous_delete_as_do_not_retry(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _RetainedAmbiguousPort(_Port):
        def delete(self, *, project_code: int, code: int) -> None:
            message = "unknown transport result"
            raise TaskDeleteAmbiguousError(message)

    profile = _profile("2.0.9")
    port = _RetainedAmbiguousPort(("load",))

    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: port,
    )

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["error"] == {
        "type": "mutation_ambiguity_unresolved",
        "message": "ambiguous task delete retained its target and must not be retried",
        "do_not_retry": True,
    }


def test_runner_fences_unreconciled_delete_after_success_response(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _UnreconciledDeletePort(_Port):
        def delete(self, *, project_code: int, code: int) -> None:
            assert project_code == 7001
            assert code in self.details

    profile = _profile("2.0.9")
    port = _UnreconciledDeletePort(("load",))

    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: port,
    )

    assert exit_code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == {
        "type": "mutation_ambiguity_unresolved",
        "message": (
            "task delete reconciliation drifted after mutation and must not be retried"
        ),
        "do_not_retry": True,
    }


def test_runner_reports_precondition_failure_without_ambiguity_or_raw_detail(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _WorkflowProofFailure(_Port):
        pre_delete_release: Literal["none", "offline"] = "offline"

        def prove_no_workflows(self, *, project_code: int) -> None:
            del project_code
            message = "secret workflow inventory detail"
            raise RuntimeError(message)

        def release_offline(self, *, project_code: int, code: int) -> None:
            raise AssertionError((project_code, code))

    profile = _profile("2.0.2")
    port = _WorkflowProofFailure(("load",))

    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: port,
    )

    assert exit_code == 2
    output = capsys.readouterr().out
    assert "secret workflow inventory detail" not in output
    payload = json.loads(output)
    assert payload["error"] == {
        "type": "precondition_not_proven",
        "message": "task-definition cleanup precondition was not proven",
    }
    assert port.details


def test_runner_sanitizes_deterministic_upstream_rejection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _RejectedPort(_Port):
        def delete(self, *, project_code: int, code: int) -> None:
            del project_code, code
            raise ApiResultError(
                result_code=50050,
                result_message="task secret-name is already online: token-secret",
            )

    profile = _profile("2.0.2")

    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: _RejectedPort(("load",)),
    )

    assert exit_code == 2
    output = capsys.readouterr().out
    assert "secret-name" not in output
    assert "token-secret" not in output
    payload = json.loads(output)
    assert payload == {
        "schema_version": 2,
        "ok": False,
        "operation": "cleanup",
        "ds_version": "2.0.2",
        "error": {
            "type": "api_result_error",
            "message": (
                "DolphinScheduler rejected task-definition cleanup because a task "
                "remains online; offline the task and retry cleanup"
            ),
            "result_code": 50050,
        },
    }


def test_runner_fences_unreviewed_upstream_rejection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _RejectedPort(_Port):
        def delete(self, *, project_code: int, code: int) -> None:
            del project_code, code
            raise ApiResultError(
                result_code=50046,
                result_message="secret partial cleanup detail",
            )

    profile = _profile("2.0.9")
    exit_code = runner.main(
        [
            "cleanup",
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--project-code",
            "7001",
            "--run-id",
            _RUN_ID,
        ],
        profile_loader=lambda _path: profile,
        client_factory=_Client,
        port_factory=lambda _profile, _client: _RejectedPort(("load",)),
    )

    assert exit_code == 2
    output = capsys.readouterr().out
    assert "secret partial cleanup detail" not in output
    payload = json.loads(output)
    assert payload["error"] == {
        "type": "api_result_error",
        "message": (
            "DolphinScheduler returned an unreviewed cleanup rejection; "
            "reconcile task state before any retry"
        ),
        "do_not_retry": True,
        "result_code": 50046,
    }


def _detail(name: str, code: int) -> CleanupTaskDetail:
    return CleanupTaskDetail(
        code=code,
        name=name,
        version=1,
        project_code=7001,
        task_type="SHELL",
        description=(f"dsctl-conformance-owner:{_RUN_ID};resource=task;name={name}"),
        raw_script=f'printf "%s\\n" "{_RUN_ID}-{name}"\n',
        task_params_fingerprint=canonical_task_params_fingerprint(
            f'printf "%s\\n" "{_RUN_ID}-{name}"\n'
        ),
    )


def _profile(ds_version: str) -> ClusterProfile:
    return ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token="test-token",
        ds_version=ds_version,
    )
