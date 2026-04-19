from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

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
        "page_hook|audit|_list_audit_logs_result",
        "page_hook|project|_list_projects_result",
        "page_hook|task_instance|_list_task_instances_result",
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
