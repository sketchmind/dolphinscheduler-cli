from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal, cast

import pytest

from dsctl.release_gate.task_definition_cleanup import (
    CleanupTaskDetail,
    CleanupTaskHistoryPage,
    CleanupTaskPage,
    CleanupTaskRef,
    TaskDefinitionCleanupError,
    TaskDeleteAmbiguityUnresolvedError,
    TaskDeleteAmbiguousError,
    TaskDeleteRetainedAfterAmbiguityError,
    TaskReleaseAmbiguityUnresolvedError,
    TaskReleaseAmbiguousError,
    TaskReleaseRetainedAfterAmbiguityError,
    canonical_task_params_fingerprint,
    cleanup_owned_task_residue,
    prove_owned_task_inventory,
)

_RUN_ID = "0123456789abcdef"
_PROJECT_CODE = 7001


class _FakeCleanupPort:
    ds_version: str = "3.1.0"
    cleanup_strategy: Literal["direct-delete", "workflow-cascade-proof-only"] = (
        "direct-delete"
    )
    pre_delete_release: Literal["none", "offline"] = "none"

    def __init__(
        self,
        details: tuple[CleanupTaskDetail, ...],
        *,
        execute_types: tuple[str, ...] = (),
    ) -> None:
        self.inventory_execute_types = execute_types
        self.details = {detail.code: detail for detail in details}
        self.page_calls: list[str | None] = []
        self.detail_calls: list[int] = []
        self.delete_calls: list[int] = []
        self.release_calls: list[int] = []
        self.workflow_proof_calls = 0
        self.page_override: CleanupTaskPage | None = None

    def list_page(
        self,
        *,
        project_code: int,
        execute_type: str | None,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskPage:
        assert project_code == _PROJECT_CODE
        assert page_no == 1
        assert page_size == 100
        self.page_calls.append(execute_type)
        if self.page_override is not None:
            return self.page_override
        details = tuple(
            detail
            for detail in self.details.values()
            if not self.inventory_execute_types
            or (execute_type == "BATCH" and detail.name == "extract")
            or (execute_type == "STREAM" and detail.name == "load")
        )
        return _page(details, execute_type=execute_type)

    def get_detail(
        self,
        *,
        project_code: int,
        code: int,
    ) -> CleanupTaskDetail:
        assert project_code == _PROJECT_CODE
        self.detail_calls.append(code)
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
        assert project_code == _PROJECT_CODE
        self.delete_calls.append(code)
        self.details.pop(code)

    def release_offline(self, *, project_code: int, code: int) -> None:
        assert project_code == _PROJECT_CODE
        self.release_calls.append(code)
        self.details[code] = replace(self.details[code], flag="NO")

    def prove_no_workflows(self, *, project_code: int) -> None:
        assert project_code == _PROJECT_CODE
        self.workflow_proof_calls += 1


def test_proof_inventories_and_details_exact_owned_task_set() -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))

    inventory = prove_owned_task_inventory(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert [(task.name, task.code) for task in inventory.tasks] == [
        ("extract", 11),
        ("load", 12),
    ]
    assert port.page_calls == [None]
    assert port.detail_calls == [11, 12]
    assert port.delete_calls == []


def test_proof_rejects_foreign_task_before_any_mutation() -> None:
    port = _FakeCleanupPort(
        (
            _owned_detail("extract", 11),
            _owned_detail("load", 12),
            replace(_owned_detail("load", 13), name="foreign"),
        )
    )

    with pytest.raises(TaskDefinitionCleanupError, match="exact owned task set"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == []


def test_proof_rejects_page_metadata_drift_before_detail_or_mutation() -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))
    port.page_override = replace(
        _page(tuple(port.details.values()), execute_type=None),
        total=3,
    )

    with pytest.raises(TaskDefinitionCleanupError, match="page metadata"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.detail_calls == []
    assert port.delete_calls == []


def test_310_proof_inventories_batch_and_stream_before_details() -> None:
    port = _FakeCleanupPort(
        (_owned_detail("extract", 11), _owned_detail("load", 12)),
        execute_types=("BATCH", "STREAM"),
    )

    inventory = prove_owned_task_inventory(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert len(inventory.tasks) == 2
    assert port.page_calls == ["BATCH", "STREAM"]
    assert port.detail_calls == [11, 12]
    assert port.delete_calls == []


@pytest.mark.parametrize(
    ("load_version", "load_script_epoch", "workflow_version", "outcome"),
    [
        pytest.param(1, "original", 1, "accepted", id="pre-task-update"),
        pytest.param(2, "updated", 1, "accepted", id="historical-update-bug"),
        pytest.param(
            2,
            "updated",
            2,
            "accepted",
            id="normal-before-workflow-edit",
        ),
        pytest.param(
            2,
            "updated",
            3,
            "accepted",
            id="normal-after-workflow-edit",
        ),
        pytest.param(
            2,
            "updated",
            4,
            "rejected",
            id="unreachable-workflow-version",
        ),
    ],
)
def test_319_proof_accepts_only_exact_gate_lineage_phases_without_deleting(
    load_version: int,
    load_script_epoch: Literal["original", "updated"],
    workflow_version: int,
    outcome: Literal["accepted", "rejected"],
) -> None:
    load_updated = load_script_epoch == "updated"
    workflow_code = 8001

    class _ProofOnlyPort(_FakeCleanupPort):
        ds_version = "3.1.9"
        cleanup_strategy: Literal["direct-delete", "workflow-cascade-proof-only"] = (
            "workflow-cascade-proof-only"
        )
        inventory_execute_types: tuple[str, ...] = ("BATCH", "STREAM")

        def __init__(self) -> None:
            super().__init__(
                (
                    _owned_detail("extract", 11),
                    _owned_detail(
                        "load",
                        12,
                        version=load_version,
                        updated=load_updated,
                    ),
                ),
                execute_types=self.inventory_execute_types,
            )
            self.history_calls: list[int] = []

        def list_page(
            self,
            *,
            project_code: int,
            execute_type: str | None,
            page_no: int,
            page_size: int,
        ) -> CleanupTaskPage:
            assert project_code == _PROJECT_CODE
            assert page_no == 1
            assert page_size == 100
            self.page_calls.append(execute_type)
            details = tuple(self.details.values()) if execute_type == "BATCH" else ()
            page = _page(details, execute_type=execute_type)
            return replace(
                page,
                rows=tuple(
                    replace(
                        row,
                        workflow_code=workflow_code,
                        workflow_version=workflow_version,
                        workflow_name=(f"dsctl-full-3-1-9-{_RUN_ID}"),
                        workflow_release_state="OFFLINE",
                    )
                    for row in page.rows
                ),
            )

        def list_history_page(
            self,
            *,
            project_code: int,
            code: int,
            page_no: int,
            page_size: int,
        ) -> CleanupTaskHistoryPage:
            assert project_code == _PROJECT_CODE
            self.history_calls.append(code)
            rows: tuple[CleanupTaskDetail, ...]
            if code == 11:
                rows = (_owned_detail("extract", 11),)
            elif load_version == 1:
                rows = (_owned_detail("load", 12),)
            else:
                rows = (
                    _owned_detail("load", 12, version=2, updated=True),
                    _owned_detail("load", 12),
                )
            return CleanupTaskHistoryPage(
                requested_page=page_no,
                current_page=page_no,
                offset=0,
                page_size=page_size,
                total=len(rows),
                total_pages=1,
                rows=rows,
            )

        def delete(self, *, project_code: int, code: int) -> None:
            message = "proof-only 3.1.9 must never delete a task"
            raise AssertionError(message)

    port = _ProofOnlyPort()

    if outcome == "accepted":
        inventory = prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            workflow_code=workflow_code,
            run_id=_RUN_ID,
        )
        assert [(task.name, task.version) for task in inventory.tasks] == [
            ("extract", 1),
            ("load", load_version),
        ]
    else:
        with pytest.raises(
            TaskDefinitionCleanupError,
            match="exact gate-owned lineage",
        ):
            prove_owned_task_inventory(
                port,
                project_code=_PROJECT_CODE,
                workflow_code=workflow_code,
                run_id=_RUN_ID,
            )
    assert port.page_calls == ["BATCH", "STREAM"]
    assert port.detail_calls == [11, 12]
    assert port.history_calls == [11, 12]
    assert port.delete_calls == []


def test_319_cleanup_rejects_nonzero_inventory_without_deleting() -> None:
    class _ProofOnlyResiduePort(_FakeCleanupPort):
        ds_version = "3.1.9"
        cleanup_strategy: Literal["direct-delete", "workflow-cascade-proof-only"] = (
            "workflow-cascade-proof-only"
        )
        inventory_execute_types: tuple[str, ...] = ("BATCH", "STREAM")

        def __init__(self) -> None:
            super().__init__(
                (_owned_detail("load", 12, version=2, updated=True),),
                execute_types=self.inventory_execute_types,
            )

        def delete(self, *, project_code: int, code: int) -> None:
            self.delete_calls.append(code)
            message = "proof-only cleanup must never delete a task"
            raise AssertionError(message)

    port = _ProofOnlyResiduePort()

    with pytest.raises(
        TaskDefinitionCleanupError,
        match="workflow cascade did not remove every task",
    ):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            workflow_code=8001,
            run_id=_RUN_ID,
        )

    assert port.page_calls == ["BATCH", "STREAM"]
    assert port.detail_calls == []
    assert port.delete_calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("code", 99),
        ("name", "foreign"),
        ("version", 2),
        ("project_code", 9001),
        ("task_type", "SQL"),
        ("description", "foreign"),
        ("raw_script", "rm -rf /"),
        ("task_params_fingerprint", "sha256:" + "0" * 64),
    ],
)
def test_proof_rejects_every_detail_ownership_drift_before_mutation(
    field: str,
    value: object,
) -> None:
    class _DriftedDetailPort(_FakeCleanupPort):
        def get_detail(
            self,
            *,
            project_code: int,
            code: int,
        ) -> CleanupTaskDetail:
            detail = super().get_detail(project_code=project_code, code=code)
            if code == 11:
                return replace(detail, **cast("Any", {field: value}))
            return detail

    port = _DriftedDetailPort((_owned_detail("extract", 11), _owned_detail("load", 12)))

    with pytest.raises(TaskDefinitionCleanupError, match="exact gate ownership"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == []


def test_310_reads_both_execute_types_before_rejecting_page_drift() -> None:
    port = _FakeCleanupPort(
        (_owned_detail("extract", 11), _owned_detail("load", 12)),
        execute_types=("BATCH", "STREAM"),
    )
    port.page_override = replace(
        _page((_owned_detail("extract", 11),), execute_type="BATCH"),
        total_pages=2,
    )

    with pytest.raises(TaskDefinitionCleanupError, match="page metadata"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.page_calls == ["BATCH", "STREAM"]
    assert port.detail_calls == []
    assert port.delete_calls == []


@pytest.mark.parametrize("residue_names", [(), ("load",), ("extract", "load")])
def test_cleanup_recovers_zero_one_or_two_owned_residues(
    residue_names: tuple[str, ...],
) -> None:
    port = _FakeCleanupPort(
        tuple(
            _owned_detail(name, 11 + index) for index, name in enumerate(residue_names)
        )
    )

    report = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert report.observed == len(residue_names)
    assert report.released == 0
    assert report.deleted == len(residue_names)
    assert report.remaining == 0
    assert report.remote_mutations == len(residue_names)
    assert port.delete_calls == [
        11 + residue_names.index(name) for name in sorted(residue_names)
    ]


def test_cleanup_offlines_online_tasks_after_fresh_empty_workflow_proof() -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))
    port.pre_delete_release = "offline"

    report = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert (report.observed, report.released, report.deleted, report.remaining) == (
        2,
        2,
        2,
        0,
    )
    assert report.remote_mutations == 4
    assert port.release_calls == [11, 12]
    assert port.delete_calls == [11, 12]
    assert port.workflow_proof_calls == 2


def test_cleanup_does_not_release_task_already_offline() -> None:
    port = _FakeCleanupPort((_owned_detail("load", 11, flag="NO"),))
    port.pre_delete_release = "offline"

    report = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert (report.released, report.deleted, report.remote_mutations) == (0, 1, 1)
    assert port.release_calls == []
    assert port.workflow_proof_calls == 1


def test_cleanup_workflow_proof_failure_is_a_precondition_failure() -> None:
    class _WorkflowProofFailure(_FakeCleanupPort):
        pre_delete_release: Literal["none", "offline"] = "offline"

        def prove_no_workflows(self, *, project_code: int) -> None:
            assert project_code == _PROJECT_CODE
            self.workflow_proof_calls += 1
            message = "untrusted remote detail"
            raise RuntimeError(message)

    port = _WorkflowProofFailure((_owned_detail("load", 11),))

    with pytest.raises(
        TaskDefinitionCleanupError,
        match="workflow absence could not be freshly proven",
    ):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.workflow_proof_calls == 1
    assert port.release_calls == []
    assert port.delete_calls == []


def test_ambiguous_offline_release_applied_is_reconciled_and_counted() -> None:
    class _AppliedButAmbiguousRelease(_FakeCleanupPort):
        pre_delete_release: Literal["none", "offline"] = "offline"

        def release_offline(self, *, project_code: int, code: int) -> None:
            super().release_offline(project_code=project_code, code=code)
            message = "transport outcome unknown"
            raise TaskReleaseAmbiguousError(message)

    port = _AppliedButAmbiguousRelease((_owned_detail("load", 11),))

    report = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert (report.released, report.deleted, report.remote_mutations) == (1, 1, 2)
    assert port.release_calls == [11]
    assert port.delete_calls == [11]


def test_ambiguous_offline_release_retained_forbids_retry_and_delete() -> None:
    class _RetainedButAmbiguousRelease(_FakeCleanupPort):
        pre_delete_release: Literal["none", "offline"] = "offline"

        def release_offline(self, *, project_code: int, code: int) -> None:
            assert project_code == _PROJECT_CODE
            self.release_calls.append(code)
            message = "transport outcome unknown"
            raise TaskReleaseAmbiguousError(message)

    port = _RetainedButAmbiguousRelease((_owned_detail("load", 11),))

    with pytest.raises(
        TaskReleaseRetainedAfterAmbiguityError,
        match="must not be retried",
    ):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.release_calls == [11]
    assert port.delete_calls == []


def test_offline_release_rejects_sibling_detail_drift_before_delete() -> None:
    class _SiblingDriftAfterRelease(_FakeCleanupPort):
        pre_delete_release: Literal["none", "offline"] = "offline"

        def release_offline(self, *, project_code: int, code: int) -> None:
            super().release_offline(project_code=project_code, code=code)
            self.details[12] = replace(self.details[12], flag="NO")

    port = _SiblingDriftAfterRelease(
        (_owned_detail("extract", 11), _owned_detail("load", 12))
    )

    with pytest.raises(TaskReleaseAmbiguityUnresolvedError, match="offline release"):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.release_calls == [11]
    assert port.delete_calls == []


def test_delete_reconciliation_rejects_sibling_detail_drift() -> None:
    class _SiblingDriftAfterDelete(_FakeCleanupPort):
        def delete(self, *, project_code: int, code: int) -> None:
            super().delete(project_code=project_code, code=code)
            self.details[12] = replace(self.details[12], version=2)

    port = _SiblingDriftAfterDelete(
        (_owned_detail("extract", 11), _owned_detail("load", 12))
    )

    with pytest.raises(TaskDeleteAmbiguityUnresolvedError, match="after mutation"):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == [11]


def test_cleanup_failure_can_resume_from_one_fresh_residue() -> None:
    class _FailingSecondDelete(_FakeCleanupPort):
        fail_code: int | None = 12

        def delete(self, *, project_code: int, code: int) -> None:
            if code == self.fail_code:
                self.delete_calls.append(code)
                message = "ambiguous delete"
                raise RuntimeError(message)
            super().delete(project_code=project_code, code=code)

    port = _FailingSecondDelete(
        (_owned_detail("extract", 11), _owned_detail("load", 12))
    )

    with pytest.raises(RuntimeError, match="ambiguous delete"):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == [11, 12]
    assert tuple(port.details) == (12,)
    port.fail_code = None
    recovered = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )
    assert (recovered.observed, recovered.deleted, recovered.remaining) == (1, 1, 0)


def test_cleanup_requires_fresh_reconciliation_before_second_delete() -> None:
    class _RetainedDelete(_FakeCleanupPort):
        def delete(self, *, project_code: int, code: int) -> None:
            assert project_code == _PROJECT_CODE
            self.delete_calls.append(code)

    port = _RetainedDelete((_owned_detail("extract", 11), _owned_detail("load", 12)))

    with pytest.raises(TaskDefinitionCleanupError, match="reconciliation drifted"):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == [11]


def test_ambiguous_delete_applied_is_freshly_reconciled_and_counted() -> None:
    class _AppliedButAmbiguous(_FakeCleanupPort):
        def delete(self, *, project_code: int, code: int) -> None:
            super().delete(project_code=project_code, code=code)
            message = "transport outcome unknown"
            raise TaskDeleteAmbiguousError(message)

    port = _AppliedButAmbiguous((_owned_detail("load", 11),))

    report = cleanup_owned_task_residue(
        port,
        project_code=_PROJECT_CODE,
        run_id=_RUN_ID,
    )

    assert (report.observed, report.deleted, report.remaining) == (1, 1, 0)
    assert report.remote_mutations == 1
    assert port.delete_calls == [11]
    assert port.page_calls == [None, None]


def test_ambiguous_delete_retained_raises_do_not_retry_after_one_fresh_read() -> None:
    class _RetainedButAmbiguous(_FakeCleanupPort):
        def delete(self, *, project_code: int, code: int) -> None:
            assert project_code == _PROJECT_CODE
            self.delete_calls.append(code)
            message = "transport outcome unknown"
            raise TaskDeleteAmbiguousError(message)

    port = _RetainedButAmbiguous((_owned_detail("load", 11),))

    with pytest.raises(
        TaskDeleteRetainedAfterAmbiguityError,
        match="must not be retried",
    ):
        cleanup_owned_task_residue(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    assert port.delete_calls == [11]
    assert port.page_calls == [None, None]


@pytest.mark.parametrize("bad_integer", [True, 1.0])
def test_proof_rejects_non_exact_integer_identities_and_page_metadata(
    bad_integer: int,
) -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))
    valid_page = _page(tuple(port.details.values()), execute_type=None)
    port.page_override = replace(valid_page, current_page=bad_integer)

    with pytest.raises(TaskDefinitionCleanupError, match="page metadata"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )

    port.page_override = replace(
        valid_page,
        rows=(replace(valid_page.rows[0], code=bad_integer), valid_page.rows[1]),
    )
    with pytest.raises(TaskDefinitionCleanupError, match="exact owned task set"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            run_id=_RUN_ID,
        )


@pytest.mark.parametrize("bad_project_code", [True, 7001.0])
def test_proof_rejects_non_exact_project_code_before_inventory(
    bad_project_code: int,
) -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))

    with pytest.raises(TaskDefinitionCleanupError, match="positive project code"):
        prove_owned_task_inventory(
            port,
            project_code=bad_project_code,
            run_id=_RUN_ID,
        )

    assert port.page_calls == []
    assert port.delete_calls == []


@pytest.mark.parametrize("bad_workflow_code", [True, 0, 8001.0])
def test_direct_proof_rejects_invalid_workflow_code_before_inventory(
    bad_workflow_code: int | None,
) -> None:
    port = _FakeCleanupPort((_owned_detail("extract", 11), _owned_detail("load", 12)))

    with pytest.raises(TaskDefinitionCleanupError, match="workflow code"):
        prove_owned_task_inventory(
            port,
            project_code=_PROJECT_CODE,
            workflow_code=bad_workflow_code,
            run_id=_RUN_ID,
        )

    assert port.page_calls == []
    assert port.delete_calls == []


def _owned_detail(
    name: str,
    code: int,
    *,
    version: int = 1,
    updated: bool = False,
    flag: Literal["YES", "NO"] = "YES",
) -> CleanupTaskDetail:
    command_suffix = f"updated-{name}" if updated else name
    raw_script = f'printf "%s\\n" "{_RUN_ID}-{command_suffix}"\n'
    return CleanupTaskDetail(
        code=code,
        name=name,
        version=version,
        project_code=_PROJECT_CODE,
        task_type="SHELL",
        description=(f"dsctl-conformance-owner:{_RUN_ID};resource=task;name={name}"),
        raw_script=raw_script,
        task_params_fingerprint=canonical_task_params_fingerprint(raw_script),
        flag=flag,
    )


def _page(
    details: tuple[CleanupTaskDetail, ...],
    *,
    execute_type: str | None,
) -> CleanupTaskPage:
    return CleanupTaskPage(
        execute_type=execute_type,
        requested_page=1,
        current_page=1,
        offset=0,
        page_size=100,
        total=len(details),
        total_pages=1,
        rows=tuple(
            CleanupTaskRef(
                code=detail.code,
                name=detail.name,
                version=detail.version,
            )
            for detail in details
        ),
    )
