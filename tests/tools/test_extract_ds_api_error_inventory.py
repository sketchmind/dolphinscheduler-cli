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
    return importlib.import_module("extract_ds_api_error_inventory")


def test_extract_status_entries_parses_multiline_enum_entries() -> None:
    inventory = _load_module()
    source = """
    SUCCESS(0, "success", "成功"),
    SCHEDULE_ALREADY_EXISTS(
            10204,
            "workflow {0} schedule {1} already exist, please update or delete it",
            "工作流 {0} 的定时 {1} 已经存在, 请更新或删除"),
    """

    entries = inventory.extract_status_entries(source)

    assert entries == [
        inventory.StatusEntry(
            name="SUCCESS",
            code=0,
            en_msg="success",
            zh_msg="成功",
        ),
        inventory.StatusEntry(
            name="SCHEDULE_ALREADY_EXISTS",
            code=10204,
            en_msg=(
                "workflow {0} schedule {1} already exist, please update or delete it"
            ),
            zh_msg="工作流 {0} 的定时 {1} 已经存在, 请更新或删除",
        ),
    ]


def test_build_inventory_finds_known_upstream_anomalies() -> None:
    inventory = _load_module()

    report = inventory.build_inventory()

    assert len(report.status_entries) >= 400
    assert any(
        entry.name == "USER_NO_OPERATION_PERM" and entry.code == 30001
        for entry in report.status_entries
    )
    assert 50014 in report.duplicate_codes
    assert any(
        finding.path.endswith("api/controller/ExecutorController.java")
        for finding in report.bare_service_exceptions
    )
    assert any(
        finding.path.endswith("api/interceptor/LoginHandlerInterceptor.java")
        for finding in report.direct_http_status_sites
    )
