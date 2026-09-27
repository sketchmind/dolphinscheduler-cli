from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("check_error_translation_governance")


def test_current_entries_match_reviewed_raw_findings() -> None:
    governance = _load_module()

    assert governance.current_entries() == [
        "except|_task_resource_refs|_merge_read_file_candidates",
        "except|_task_resource_refs|resolve_read_task_resource_refs",
        "page_hook|audit|_list_audit_logs_result",
        (
            "raw_matrix|alert_group|_translate_alert_group_api_error|"
            "CREATE_ALERT_GROUP_ERROR,LIST_PAGING_ALERT_GROUP_ERROR,"
            "UPDATE_ALERT_GROUP_ERROR,DELETE_ALERT_GROUP_ERROR"
        ),
        (
            "raw_matrix|task_instance|_task_instance_action_error|"
            "TASK_SAVEPOINT_ERROR,TASK_STOP_ERROR"
        ),
    ]


@pytest.mark.parametrize(
    ("filename", "module_name"),
    [("_errors.py", "workflow._errors"), ("__init__.py", "workflow")],
)
def test_unmapped_package_error_still_fails_governance(
    filename: str,
    module_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    governance = _load_module()
    audit = importlib.import_module("audit_dsctl_error_translation")
    services = tmp_path / "src" / "dsctl" / "services"
    workflow = services / "workflow"
    workflow.mkdir(parents=True)
    (workflow / "_types.py").write_text("DENIED = 30001\n", encoding="utf-8")
    (workflow / filename).write_text(
        """
from ._types import DENIED

def _translate_demo(error: ApiResultError) -> Exception:
    if error.result_code == DENIED:
        return error
    return UserInputError("failed")

def mutate_demo() -> None:
    try:
        pass
    except ApiResultError:
        raise
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "SERVICES_ROOT", services)
    monkeypatch.setattr(governance, "ALLOWLIST_PATH", tmp_path / "empty-allowlist.txt")
    monkeypatch.setattr(sys, "argv", ["check_error_translation_governance.py"])

    assert governance.current_entries() == [
        f"except|{module_name}|mutate_demo",
        f"raw_matrix|{module_name}|_translate_demo|DENIED",
    ]
    assert governance.main() == 1
    output = capsys.readouterr().out
    assert "unexpected error-translation findings:" in output
    assert f"+ except|{module_name}|mutate_demo" in output
    assert f"+ raw_matrix|{module_name}|_translate_demo|DENIED" in output
