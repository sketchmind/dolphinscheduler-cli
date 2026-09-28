"""DS 3.1.9 relation-bound task-version cleanup regression."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

import pytest
from tests.live import conformance_bundle_gate as gate_module
from tests.live.conformance_bundle_gate import (
    ConformanceBundleGateConfig,
    ConformanceScenarioCleanupError,
    execute_conformance_bundle_scenario,
    recover_existing_full_conformance_state,
)
from tests.live.support import TaskDefinitionCleanupInvocation
from tests.tools.test_conformance_bundle_scenario import (
    _config,
    _fake_int,
    _FakeTaskCleanup,
    _FullFakeDsctl,
    _mapping,
)

from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tests.live.support import DsctlCommandResult


class _Ds319NormalVersionTraceFake(_FullFakeDsctl):
    """Record the workflow version produced by each normal mutation stage."""

    def __init__(self, config: ConformanceBundleGateConfig) -> None:
        super().__init__(config)
        self.applied_workflow_versions: list[tuple[str, int]] = []

    def _install_workflow(self, document: Mapping[str, object]) -> None:
        super()._install_workflow(document)
        workflow = self.owned_workflow
        assert workflow is not None
        self.applied_workflow_versions.append(
            ("workflow.create", _fake_int(workflow["version"]))
        )

    def _task_update(self, argv: list[str]) -> DsctlCommandResult:
        result = super()._task_update(argv)
        if "--dry-run" not in argv and result.payload.get("ok") is True:
            workflow = self.owned_workflow
            assert workflow is not None
            self.applied_workflow_versions.append(
                ("task.update", _fake_int(workflow["version"]))
            )
        return result

    def _workflow_edit(self, argv: list[str]) -> DsctlCommandResult:
        result = super()._workflow_edit(argv)
        if "--dry-run" not in argv and result.payload.get("ok") is True:
            workflow = self.owned_workflow
            assert workflow is not None
            self.applied_workflow_versions.append(
                ("workflow.edit", _fake_int(workflow["version"]))
            )
        return result


class _Ds319StaleNormalVersionFake(_Ds319NormalVersionTraceFake):
    """Attack fake: task update succeeds without advancing the workflow log."""

    def _task_update(self, argv: list[str]) -> DsctlCommandResult:
        result = super()._task_update(argv)
        if "--dry-run" not in argv and result.payload.get("ok") is True:
            workflow = self.owned_workflow
            assert workflow is not None
            workflow["version"] = _fake_int(workflow["version"]) - 1
            self.applied_workflow_versions[-1] = (
                "task.update",
                _fake_int(workflow["version"]),
            )
        return result


class _Ds319BeforeWorkflowEditFailureFake(_Ds319NormalVersionTraceFake):
    """Leave an exact post-task-update v2 residue for recovery."""

    def _workflow_edit(self, argv: list[str]) -> DsctlCommandResult:
        if "--dry-run" in argv:
            return self._error(
                argv,
                "workflow.edit",
                "api_transport_error",
                details={"mutation_applied": False},
            )
        return super()._workflow_edit(argv)


class _Ds319RejectProof:
    """Interrupt every private proof so a phase snapshot remains recoverable."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int | None, str]] = []

    def __call__(
        self,
        operation: str,
        project_code: int,
        workflow_code: int | None,
        run_id: str,
    ) -> TaskDefinitionCleanupInvocation:
        self.calls.append((operation, project_code, workflow_code, run_id))
        message = "synthetic proof interruption"
        raise AssertionError(message)


class _Ds319RelationBoundTaskFake(_FullFakeDsctl):
    """Expose the post-update detail log while the workflow stays on its old log."""

    def __init__(
        self,
        config: ConformanceBundleGateConfig,
        *,
        cleanup_drift: str | None = None,
        fail_at: str | None = None,
        retain_tasks_after_workflow_delete: bool = False,
    ) -> None:
        super().__init__(config, fail_at=fail_at)
        self.relation_bound_tasks: dict[str, dict[str, object]] = {}
        self.detail_version_after_update: int | None = None
        self.relation_bound_version_after_update: int | None = None
        self.cleanup_drift = cleanup_drift
        self.retain_tasks_after_workflow_delete = retain_tasks_after_workflow_delete

    def _install_workflow(self, document: Mapping[str, object]) -> None:
        super()._install_workflow(document)
        self.relation_bound_tasks = copy.deepcopy(self.owned_tasks)

    def _workflow_describe(self, argv: list[str]) -> DsctlCommandResult:
        result = super()._workflow_describe(argv)
        data = _mapping(result.payload["data"])
        data["tasks"] = [
            copy.deepcopy(task)
            for task in sorted(
                self.relation_bound_tasks.values(),
                key=lambda item: _fake_int(item["code"]),
            )
        ]
        return self._ok(argv, "workflow.describe", data)

    def _task_list(self, argv: list[str]) -> DsctlCommandResult:
        rows = [
            self._task_ref(task)
            for task in sorted(
                self.relation_bound_tasks.values(),
                key=lambda item: _fake_int(item["code"]),
            )
        ]
        return self._ok(argv, "task.list", rows)

    def _task_update(self, argv: list[str]) -> DsctlCommandResult:
        if "--dry-run" in argv:
            return super()._task_update(argv)
        task = self._selected_task(argv[2])
        assert task is not None
        replacement = argv[argv.index("--set") + 1].removeprefix("command=")
        _mapping(task["taskParams"])["rawScript"] = replacement
        task["version"] = _fake_int(task["version"]) + 1
        self.mutations.append("task-update")
        self.detail_version_after_update = _fake_int(task["version"])
        self.relation_bound_version_after_update = next(
            _fake_int(relation["postTaskVersion"])
            for relation in self.owned_relations
            if relation["postTaskCode"] == task["code"]
        )
        if self.cleanup_drift == "latest-version":
            task["version"] = 3
        elif self.cleanup_drift == "latest-command":
            _mapping(task["taskParams"])["rawScript"] = "foreign command"
        elif self.cleanup_drift == "dag-version":
            self.relation_bound_tasks[str(task["name"])]["version"] = 2
        elif self.cleanup_drift == "extract-version":
            self.owned_tasks["extract"]["version"] = 2
        elif self.cleanup_drift in {"workflow-version-2", "workflow-version-3"}:
            workflow = self.owned_workflow
            assert workflow is not None
            workflow["version"] = int(self.cleanup_drift.rsplit("-", 1)[1])
        elif self.cleanup_drift == "workflow-description-phase":
            workflow = self.owned_workflow
            assert workflow is not None
            workflow["description"] = str(workflow["description"]).replace(
                "phase=created",
                "phase=updated",
            )
        return self._error(
            argv,
            "task.update",
            "api_transport_error",
            details={
                "mutation_applied": True,
                "phase": "readback",
                "issues": [
                    {
                        "kind": "task_version_mismatch",
                        "detail_version": self.detail_version_after_update,
                        "dag_version": self.relation_bound_version_after_update,
                    }
                ],
            },
        )

    def _workflow_delete(self, argv: list[str]) -> DsctlCommandResult:
        retained = copy.deepcopy(self.owned_tasks)
        result = super()._workflow_delete(argv)
        if self.retain_tasks_after_workflow_delete and result.payload.get("ok") is True:
            self.owned_tasks = retained
        return result


class _Ds319CascadeProof:
    """Prove task lineage without ever deleting a task definition."""

    def __init__(
        self,
        remote: _Ds319RelationBoundTaskFake,
        *,
        reject_proof: bool = False,
        validate_lineage: bool = True,
    ) -> None:
        self._remote = remote
        self._reject_proof = reject_proof
        self._validate_lineage = validate_lineage
        self.calls: list[tuple[str, int, int | None, str]] = []

    def __call__(
        self,
        operation: str,
        project_code: int,
        workflow_code: int | None,
        run_id: str,
    ) -> TaskDefinitionCleanupInvocation:
        project = self._remote.owned_project
        assert project is not None
        assert project["code"] == project_code
        assert workflow_code == 1001
        self.calls.append((operation, project_code, workflow_code, run_id))
        if operation == "prove":
            relation_bound = self._remote.relation_bound_tasks
            latest = self._remote.owned_tasks
            if self._validate_lineage:
                assert set(relation_bound) == set(latest) == {"extract", "load"}
                assert _fake_int(relation_bound["extract"]["code"]) == 2001
                assert _fake_int(latest["extract"]["code"]) == 2001
                assert _fake_int(relation_bound["load"]["code"]) == 2002
                assert _fake_int(latest["load"]["code"]) == 2002
                original_extract = f'printf "%s\\n" "{run_id}-extract"\n'
                assert (
                    _fake_int(relation_bound["extract"]["version"]),
                    _mapping(relation_bound["extract"]["taskParams"])["rawScript"],
                    _fake_int(latest["extract"]["version"]),
                    _mapping(latest["extract"]["taskParams"])["rawScript"],
                ) == (1, original_extract, 1, original_extract)
                original_load = f'printf "%s\\n" "{run_id}-load"\n'
                updated_load = f'printf "%s\\n" "{run_id}-updated-load"\n'
                lineage = (
                    _fake_int(relation_bound["load"]["version"]),
                    _mapping(relation_bound["load"]["taskParams"])["rawScript"],
                    _fake_int(latest["load"]["version"]),
                    _mapping(latest["load"]["taskParams"])["rawScript"],
                )
                assert lineage in {
                    (1, original_load, 1, original_load),
                    (1, original_load, 2, updated_load),
                    (2, updated_load, 2, updated_load),
                }
            if self._reject_proof:
                message = "synthetic proof interruption"
                raise AssertionError(message)
            return TaskDefinitionCleanupInvocation(
                operation="prove",
                ds_version="3.1.9",
                observed=2,
                released=0,
                deleted=0,
                remaining=2,
                remote_mutations=0,
            )
        assert operation == "cleanup"
        observed = len(self._remote.owned_tasks)
        return TaskDefinitionCleanupInvocation(
            operation="cleanup",
            ds_version="3.1.9",
            observed=observed,
            released=0,
            deleted=0,
            remaining=observed,
            remote_mutations=0,
        )


def _assert_owned_residue_present(remote: _FullFakeDsctl) -> None:
    assert remote.owned_project is not None
    assert remote.owned_workflow is not None


def test_ds319_normal_mutations_advance_exact_workflow_version_trace(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319NormalVersionTraceFake(config)

    execute_conformance_bundle_scenario(
        config,
        invoke=remote,
        invoke_raw=remote.invoke_raw,
        invoke_task_cleanup=_FakeTaskCleanup(remote),
        run_id="0123456789abcdef",
    )

    assert remote.applied_workflow_versions == [
        ("workflow.create", 1),
        ("task.update", 2),
        ("workflow.edit", 3),
    ]


def test_ds319_normal_scenario_refuses_stale_workflow_version_without_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319StaleNormalVersionFake(config)
    proof = _FakeTaskCleanup(remote)

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=proof,
            run_id="0123456789abcdef",
        )

    assert remote.applied_workflow_versions == [
        ("workflow.create", 1),
        ("task.update", 1),
    ]
    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove"
    ]
    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


@pytest.mark.parametrize(
    ("residue_phase", "expected_version", "expected_description_phase"),
    [
        ("post-task-update", 2, "phase=created"),
        ("post-workflow-edit", 3, "phase=updated"),
    ],
)
def test_ds319_recovery_accepts_exact_normal_mutation_residue_phases(
    tmp_path: Path,
    residue_phase: str,
    expected_version: int,
    expected_description_phase: str,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote: _Ds319NormalVersionTraceFake
    if residue_phase == "post-task-update":
        remote = _Ds319BeforeWorkflowEditFailureFake(config)
    else:
        remote = _Ds319NormalVersionTraceFake(config)
    run_id = "0123456789abcdef"

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=_Ds319RejectProof(),
            run_id=run_id,
        )

    _assert_owned_residue_present(remote)
    workflow = remote.owned_workflow
    assert workflow is not None
    assert workflow["version"] == expected_version
    assert expected_description_phase in str(workflow["description"])

    remote.calls.clear()
    proof = _FakeTaskCleanup(remote)
    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=proof,
        run_id=run_id,
    )

    assert remote.owned_tasks == {}
    assert remote.owned_workflow is None
    assert remote.owned_project is None
    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove",
        "cleanup",
    ]
    assert "task-definition-delete" not in remote.mutations


def test_generated_reconciliation_and_cross_recovery_sets_remain_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_data = copy.deepcopy(cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA)
    profile_data["cross_process_recovery_versions"] = ["2.0.9"]
    monkeypatch.setattr(
        cleanup_profiles,
        "TASK_DEFINITION_CLEANUP_PROFILE_DATA",
        profile_data,
    )
    monkeypatch.setattr(
        cleanup_profiles,
        "CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS",
        ("2.0.9",),
    )

    reconciliation, recovery, strategies, pre_delete_release_versions = (
        gate_module._validated_generated_task_definition_guard_truth()
    )

    assert "3.1.9" in reconciliation
    assert "3.1.9" not in recovery
    assert strategies["3.1.9"] == "workflow-cascade-proof-only"
    assert pre_delete_release_versions == {"2.0.1", "2.0.2", "2.0.3"}
    assert "2.0.1" in reconciliation
    assert "2.0.1" not in recovery
    assert pre_delete_release_versions & reconciliation == {
        "2.0.1",
        "2.0.2",
        "2.0.3",
    }

    monkeypatch.setattr(
        gate_module,
        "_FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS",
        frozenset(),
    )
    monkeypatch.setattr(
        gate_module,
        "_CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS",
        frozenset({"3.1.9"}),
    )
    assert gate_module._full_task_definition_guard_strategy("3.1.9") is None
    assert (
        gate_module._full_task_definition_guard_strategy(
            "3.1.9",
            for_recovery=True,
        )
        == "workflow-cascade-proof-only"
    )


def test_ds319_full_scenario_requires_proof_callback_before_remote_io(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config)

    with pytest.raises(ValueError, match="private task-definition proof runner"):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            run_id="0123456789abcdef",
        )

    assert remote.calls == []
    assert remote.owned_project is None


def test_ds319_recovery_requires_proof_callback_before_remote_io(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config)

    with pytest.raises(ValueError, match="private task-definition proof runner"):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            run_id="0123456789abcdef",
        )

    assert remote.calls == []
    assert remote.owned_project is None


def test_ds319_relation_bound_task_update_failure_still_cleans_zero_residue(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config)
    proof = _Ds319CascadeProof(remote)

    with pytest.raises(AssertionError, match="gate-owned task update failed"):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=proof,
            run_id="0123456789abcdef",
        )

    assert remote.detail_version_after_update == 2
    assert remote.relation_bound_version_after_update == 1
    assert remote.owned_tasks == {}
    assert remote.owned_workflow is None
    assert remote.owned_project is None
    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove",
        "cleanup",
    ]
    assert "task-definition-delete" not in remote.mutations
    assert not config.evidence_path.exists()


@pytest.mark.parametrize(
    "cleanup_drift",
    [
        "latest-version",
        "latest-command",
        "dag-version",
        "extract-version",
        "workflow-version-2",
        "workflow-version-3",
        "workflow-description-phase",
    ],
)
def test_ds319_cleanup_refuses_every_non_exact_task_lineage_without_delete(
    tmp_path: Path,
    cleanup_drift: str,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config, cleanup_drift=cleanup_drift)
    proof = _Ds319CascadeProof(remote, validate_lineage=False)

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=proof,
            run_id="0123456789abcdef",
        )

    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove"
    ]
    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )
    assert "task-definition-delete" not in remote.mutations


def test_ds319_post_cascade_task_residue_blocks_project_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(
        config,
        retain_tasks_after_workflow_delete=True,
    )
    proof = _Ds319CascadeProof(remote)

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=proof,
            run_id="0123456789abcdef",
        )

    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove",
        "cleanup",
    ]
    assert remote.owned_workflow is None
    assert len(remote.owned_tasks) == 2
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)
    assert "task-definition-delete" not in remote.mutations


def test_ds319_cross_process_recovery_uses_cascade_without_task_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config)
    run_id = "0123456789abcdef"

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=_Ds319CascadeProof(remote, reject_proof=True),
            run_id=run_id,
        )
    _assert_owned_residue_present(remote)
    assert len(remote.owned_tasks) == 2

    remote.calls.clear()
    proof = _Ds319CascadeProof(remote)
    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=proof,
        run_id=run_id,
    )

    assert remote.owned_tasks == {}
    assert remote.owned_workflow is None
    assert remote.owned_project is None
    assert [operation for operation, _project, _workflow, _run in proof.calls] == [
        "prove",
        "cleanup",
    ]
    assert "task-definition-delete" not in remote.mutations


def test_ds319_recovery_accepts_exact_preupdate_task_phase(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="3.1.9", bundle_name="full_core/v1")
    remote = _Ds319RelationBoundTaskFake(config, fail_at="workflow-digest")
    run_id = "0123456789abcdef"

    with pytest.raises(ConformanceScenarioCleanupError):
        execute_conformance_bundle_scenario(
            config,
            invoke=remote,
            invoke_raw=remote.invoke_raw,
            invoke_task_cleanup=_Ds319CascadeProof(remote, reject_proof=True),
            run_id=run_id,
        )
    assert remote.detail_version_after_update is None
    _assert_owned_residue_present(remote)

    proof = _Ds319CascadeProof(remote)
    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=proof,
        run_id=run_id,
    )

    assert remote.owned_tasks == {}
    assert remote.owned_workflow is None
    assert remote.owned_project is None
    assert "task-definition-delete" not in remote.mutations
