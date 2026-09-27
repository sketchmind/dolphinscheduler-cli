"""Production exact workflows preserve read typing and completed release writes."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import httpx
import pytest
from tests.support import make_profile

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import NotFoundError, PermissionDeniedError
from dsctl.services import runtime as runtime_service
from dsctl.services import workflow as service
from dsctl.services.selection import ResourceDefaults
from dsctl.services.version_resolution import RuntimeSelection
from dsctl.services.workflow import edit
from dsctl.upstream.workflows import WORKFLOW_DOMAIN, WorkflowAdapter

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from dsctl.upstream.workflows import WorkflowDomain


@dataclass
class _WorkflowServer:
    result_code: int
    fail_detail_call: int | None = None
    fail_after_release: bool = False
    detail_calls: int = 0
    writes: list[httpx.Request] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path.endswith(
            "/workflow-definition/101/release"
        ):
            self.writes.append(request)
            return self._success(True)
        assert request.method == "GET", f"unexpected mutation: {request}"
        if path == "/dolphinscheduler/projects":
            return self._success(
                {
                    "totalList": [{"id": 7, "code": 7, "name": "etl-prod"}],
                    "total": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                }
            )
        if path == "/dolphinscheduler/projects/7":
            return self._success({"id": 7, "code": 7, "name": "etl-prod"})
        if path.endswith("/workflow-definition/101"):
            self.detail_calls += 1
            if self.detail_calls == self.fail_detail_call or (
                self.fail_after_release and self.writes
            ):
                return httpx.Response(
                    200,
                    json={
                        "code": self.result_code,
                        "msg": "controlled workflow detail failure",
                        "data": None,
                    },
                )
            return self._success(
                {
                    "workflowDefinition": {
                        "id": 17,
                        "code": 101,
                        "name": "daily-sync",
                        "version": 5,
                        "projectCode": 7,
                        "releaseState": "OFFLINE",
                        "executionType": "PARALLEL",
                        "globalParams": "[]",
                        "locations": "{}",
                        "timeout": 0,
                    },
                    "taskDefinitionList": [],
                    "workflowTaskRelationList": [],
                }
            )
        if path.endswith("/schedules"):
            return self._success(
                {"totalList": [], "total": 0, "pageSize": 100, "currentPage": 1}
            )
        message = f"unexpected read: {request}"
        raise AssertionError(message)

    @staticmethod
    def _success(data: object) -> httpx.Response:
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _install_runtime(monkeypatch: pytest.MonkeyPatch, server: _WorkflowServer) -> None:
    profile = make_profile(ds_version="3.4.3")

    @contextmanager
    def open_runtime(
        domain: object, *, env_file: str | None = None, selection: object = None
    ) -> Iterator[runtime_service.BoundDomainServiceRuntime[WorkflowDomain]]:
        del env_file, selection
        assert domain is WORKFLOW_DOMAIN
        with DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(server.handle)
        ) as client:
            yield runtime_service.BoundDomainServiceRuntime(
                profile=profile,
                context=ResourceDefaults(),
                http_client=client,
                domain=WorkflowAdapter.for_version("3.4.3").bind(
                    profile, http_client=client
                ),
            )

    monkeypatch.setattr(
        runtime_service, "open_bound_domain_service_runtime", open_runtime
    )
    monkeypatch.setattr(
        edit,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(profile),
    )


@pytest.mark.parametrize("action", ["online", "offline"])
@pytest.mark.parametrize(
    ("result_code", "error_type"),
    [(30002, PermissionDeniedError), (50003, NotFoundError)],
)
def test_exact_343_release_readback_keeps_typed_error_and_applied_write(
    monkeypatch: pytest.MonkeyPatch,
    action: Literal["online", "offline"],
    result_code: int,
    error_type: type[PermissionDeniedError] | type[NotFoundError],
) -> None:
    server = _WorkflowServer(result_code, fail_after_release=True)
    _install_runtime(monkeypatch, server)
    release = (
        service.online_workflow_result
        if action == "online"
        else service.offline_workflow_result
    )

    with pytest.raises(error_type) as caught:
        release("101", project="7")

    error = caught.value
    assert isinstance(error.__cause__, error_type)
    assert error.details["mutation_applied"] is True
    assert error.details["phase"] == "post_mutation_refresh"
    assert error.details["operation"] == f"workflow.{action}"
    assert error.details["project_code"] == 7
    assert error.details["code"] == 101
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion
    assert "do not retry" in error.suggestion.lower()
    assert len(server.writes) == 1


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize("detail_call", [2, 4])
@pytest.mark.parametrize(
    ("result_code", "error_type"),
    [(30002, PermissionDeniedError), (50003, NotFoundError)],
)
def test_exact_343_pre_edit_detail_error_is_typed_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    input_mode: Literal["patch", "file"],
    detail_call: int,
    result_code: int,
    error_type: type[PermissionDeniedError] | type[NotFoundError],
) -> None:
    server = _WorkflowServer(result_code, fail_detail_call=detail_call)
    _install_runtime(monkeypatch, server)
    path = tmp_path / "edit.yaml"
    path.write_text(
        "patch:\n  workflow:\n    set:\n      description: revised\n"
        if input_mode == "patch"
        else "workflow:\n  name: daily-sync\n  project: etl-prod\ntasks:\n"
        "  - name: hello\n    type: SHELL\n    command: echo hello\n"
    )

    with pytest.raises(error_type) as caught:
        service.edit_workflow_result(
            "101",
            project="7",
            patch=path if input_mode == "patch" else None,
            file=path if input_mode == "file" else None,
        )

    error = caught.value
    assert error.details["resource"] == "workflow"
    assert error.details["project_code"] == 7
    assert error.details["code"] == 101
    assert not error.details.get("mutation_applied")
    assert server.detail_calls == detail_call
    assert server.writes == []
