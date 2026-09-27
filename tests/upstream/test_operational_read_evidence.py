from __future__ import annotations

from dataclasses import replace

import pytest

from dsctl.errors import (
    ApiResultError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES
from dsctl.output import require_json_object
from dsctl.upstream import read_compatibility, task_logs
from dsctl.upstream.instance_time_filters import (
    InstanceListAction,
    InstanceTimeFilterContract,
    instance_time_filter_contract,
)
from dsctl.upstream.pagination import requested_page_data
from dsctl.upstream.task_logs import TaskLogChunk, window_task_log
from tests.fakes import FakeProject, FakeProjectPage


@pytest.mark.parametrize("version", tuple(RUNTIME_INSTANCE_PROFILES))
def test_all_reviewed_instance_time_filters_reject_ineffective_single_bounds(
    version: str,
) -> None:
    modern = tuple(int(part) for part in version.split(".")) >= (3, 1, 0)
    actions: tuple[InstanceListAction, ...] = (
        "task-instance.list",
        "workflow-instance.list",
    )
    for action in actions:
        contract = instance_time_filter_contract(version, action)
        contract.validate(start="2026-09-01 00:00:00", end="2026-09-02 00:00:00")
        assert contract.to_data()["state_observation"] == "current_at_read"
        for start, end in (
            ("2026-09-01 00:00:00", None),
            (None, "2026-09-02 00:00:00"),
        ):
            if modern:
                contract.validate(start=start, end=end)
            else:
                with pytest.raises(UserInputError, match="both --start and --end"):
                    contract.validate(start=start, end=end)


def test_exact_start_bound_inclusivity_is_not_normalized_away() -> None:
    assert (
        instance_time_filter_contract("3.0.0", "workflow-instance.list").to_data()[
            "interval"
        ]
        == "(start,end]"
    )
    assert (
        instance_time_filter_contract("3.0.1", "workflow-instance.list").to_data()[
            "interval"
        ]
        == "[start,end]"
    )
    assert (
        instance_time_filter_contract("3.0.6", "task-instance.list").to_data()[
            "interval"
        ]
        == "(start,end]"
    )
    assert (
        instance_time_filter_contract("3.1.0", "task-instance.list").to_data()[
            "interval"
        ]
        == "[start,end]"
    )
    with pytest.raises(ValueError, match="No reviewed"):
        instance_time_filter_contract("9.0.0", "task-instance.list")


def _page(number: int, *, total: int = 2) -> FakeProjectPage:
    return FakeProjectPage(
        total_list_value=[FakeProject(code=number, name=str(number))],
        total=total,
        total_page_value=2,
        page_size_value=1,
        current_page_value=number,
    )


def test_paging_changes_are_observed_without_claiming_atomic_snapshot() -> None:
    data = requested_page_data(
        lambda page_no, page_size: _page(page_no, total=page_no + 1),
        page_no=1,
        page_size=1,
        all_pages=True,
        serialize_item=lambda item: item.name,
        resource="project",
    )
    assert data["total"] == 2  # Existing materialized view remains compatible.
    coverage = data["coverage"]
    assert coverage["initial_total"] == 2
    assert coverage["pages_read"] == coverage["rows_read"] == 2
    assert coverage["totals_changed"] is True
    assert coverage["scope_complete"] is True
    assert coverage["atomic_snapshot"] is False
    assert coverage["pages"][1]["total"] == 3
    assert coverage["observation_started_at"] <= coverage["observation_finished_at"]


def test_partial_permission_error_preserves_read_scope_and_stable_error() -> None:
    def fetch(page_no: int, page_size: int) -> FakeProjectPage:
        del page_size
        if page_no == 2:
            raise ApiResultError(result_code=30001, result_message="denied")
        return _page(page_no)

    with pytest.raises(PermissionDeniedError) as exc:
        requested_page_data(
            fetch,
            page_no=1,
            page_size=1,
            all_pages=True,
            serialize_item=lambda item: item.name,
            resource="project",
            translate_error=lambda error: PermissionDeniedError(
                "Denied", details={"result_code": error.result_code}
            ),
        )
    coverage = require_json_object(exc.value.details["coverage"], label="coverage")
    assert coverage["rows_read"] == coverage["pages_read"] == 1
    assert coverage["scope_complete"] is False
    assert coverage["initial_total"] == 2


def test_log_window_continues_at_source_line_without_rescanning_or_header_offset() -> (
    None
):
    calls: list[tuple[int, int]] = []
    lines = [f"line-{number}" for number in range(1, 9)]

    def read(*, task_instance_id: int, skip_line_num: int, limit: int) -> TaskLogChunk:
        del task_instance_id
        calls.append((skip_line_num, limit))
        selected = lines[skip_line_num : skip_line_num + limit]
        header = ["[LOG-PATH]: ignored"] if skip_line_num == 0 else []
        return TaskLogChunk(
            "".join(line + "\n" for line in header)
            + "".join(line + "\r\n" for line in selected),
            len(selected),
            not selected,
            len(header),
        )

    first = window_task_log(task_instance_id=1, start_line=1, limit=3, read_chunk=read)
    assert first.text == "line-1\nline-2\nline-3"
    assert first.window is not None
    assert first.window["next_start_line"] == 4
    second = window_task_log(task_instance_id=1, start_line=4, limit=5, read_chunk=read)
    assert second.text == "\n".join(lines[3:])
    assert second.window is not None
    assert second.window["has_more"] is False
    assert calls == [(0, 4), (3, 6)]


def test_empty_window_retains_requested_position() -> None:
    result = window_task_log(
        task_instance_id=1,
        start_line=500,
        limit=10,
        read_chunk=lambda **kwargs: TaskLogChunk(None, 0, True),
    )
    assert result.text == ""
    assert result.window is not None
    assert result.window["start_line"] == 500
    assert result.window["end_line"] is None
    assert result.window["next_start_line"] is None


def test_log_window_preserves_long_source_line_and_control_characters() -> None:
    source = "x" * 20000 + "\vstill-the-same-source-line"
    result = window_task_log(
        task_instance_id=1,
        start_line=12,
        limit=1,
        read_chunk=lambda **kwargs: TaskLogChunk(source + "\r\n", 1, False),
    )
    assert result.text == source
    assert result.line_count == 1
    assert result.window is not None
    assert result.window["end_line"] == 12


def test_log_window_unknown_eof_at_existing_budget_does_not_claim_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(task_logs, "_MAX_LOG_CHUNKS", 1)
    result = window_task_log(
        task_instance_id=1,
        start_line=1,
        limit=1000,
        read_chunk=lambda **kwargs: TaskLogChunk("line\r\n" * 1000, 1000, False),
    )
    assert result.window is not None
    assert result.window["has_more"] is None
    assert result.window["next_start_line"] == 1001
    assert result.window["scope_complete"] is True


def test_candidate_read_requires_identical_time_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    versions = ("2.0.4", "2.0.5")
    action = "workflow-instance.list"
    review = read_compatibility._REVIEWS[action]
    observed = tuple(
        {
            program.source_operation
            for version in versions
            for program in read_compatibility._programs(version, review)
        }
    )
    read_compatibility.build_read_compatibility_plan(
        versions, action=action, compatible_operations=observed
    )
    original = instance_time_filter_contract

    def changed(version: str, action: InstanceListAction) -> InstanceTimeFilterContract:
        contract = original(version, action)
        return (
            replace(contract, lower_inclusive=True) if version == "2.0.5" else contract
        )

    monkeypatch.setattr(read_compatibility, "instance_time_filter_contract", changed)
    with pytest.raises(UnsupportedFeatureError) as caught:
        read_compatibility.build_read_compatibility_plan(
            versions, action=action, compatible_operations=observed
        )
    assert caught.value.details["reason"] == "read_contracts_differ"
