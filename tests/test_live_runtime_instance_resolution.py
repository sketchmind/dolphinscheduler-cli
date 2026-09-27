from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live import execution_support as runtime
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from pathlib import Path


def _page(rows: list[object], *, complete: bool = True) -> DsctlCommandResult:
    return DsctlCommandResult(
        argv=(),
        exit_code=0,
        stdout="",
        stderr="",
        payload={
            "ok": True,
            "action": "workflow-instance.list",
            "data": {
                "totalList": rows,
                "total": len(rows),
                "coverage": {"scope_complete": complete},
            },
        },
    )


def test_unavailable_run_identity_uses_exclusive_workflow_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def wait_for_result(
        root: Path, args: list[str], **kwargs: object
    ) -> DsctlCommandResult:
        assert root == tmp_path
        assert args == [
            "workflow-instance",
            "list",
            "--project",
            "owned-project",
            "--workflow",
            "fresh-workflow",
            "--all",
        ]
        assert kwargs["env_file"] == tmp_path / "etl.env"
        accept = kwargs["accept"]
        assert callable(accept)
        assert accept(_page([])) is False
        assert accept(_page([{"id": 71}])) is True
        return _page([{"id": 71}])

    monkeypatch.setattr(runtime, "wait_for_result", wait_for_result)
    assert (
        runtime._execution_instance_id(
            {
                "accepted": True,
                "workflowInstanceIds": [],
                "instanceResolution": "unavailable",
            },
            repo_root=tmp_path,
            env_file=tmp_path / "etl.env",
            project="owned-project",
            fresh_workflow="fresh-workflow",
            label="owned run",
        )
        == 71
    )


@pytest.mark.parametrize(
    "rows", [[{"id": 71}, {"id": 72}], [{"id": True}], [{"id": 0}]]
)
def test_fresh_workflow_query_rejects_ambiguous_or_invalid_identity(
    rows: list[object],
) -> None:
    with pytest.raises(AssertionError):
        runtime._fresh_workflow_instance_id(_page(rows))


def test_fresh_workflow_query_requires_complete_inventory() -> None:
    with pytest.raises(AssertionError, match="incomplete"):
        runtime._fresh_workflow_instance_id(_page([{"id": 71}], complete=False))
